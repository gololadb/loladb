package sql

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/gololadb/gopgsql/parser"

	"github.com/gololadb/loladb/pkg/tuple"
)

// CopyFormat identifies the serialization format for COPY.
type CopyFormat int

const (
	CopyFormatText CopyFormat = iota
	CopyFormatCSV
	CopyFormatBinary // not supported, but recognized for error messages
)

// CopyOptions holds parsed WITH (...) options from a COPY statement.
type CopyOptions struct {
	Format    CopyFormat
	Header    bool
	Delimiter string // single character; default \t for text, , for csv
	Null      string // NULL representation; default \N for text, empty for csv
	Quote     byte   // CSV quote character; default "
	Escape    byte   // CSV escape character; default same as Quote
}

// CopyResult is returned by COPY operations.
type CopyResult struct {
	// For COPY TO: formatted output lines (each ending with \n).
	Data string
	// Number of rows processed.
	Count int64
	// Column names (for pgwire CopyOutResponse).
	Columns []string
}

// parseCopyOptions extracts CopyOptions from the parser's DefElem list.
func parseCopyOptions(opts []*parser.DefElem) CopyOptions {
	co := CopyOptions{
		Format:    CopyFormatText,
		Delimiter: "\t",
		Null:      "\\N",
		Quote:     '"',
		Escape:    '"',
	}
	for _, o := range opts {
		name := strings.ToLower(o.Defname)
		val := defElemString(o)
		switch name {
		case "format":
			switch strings.ToLower(val) {
			case "csv":
				co.Format = CopyFormatCSV
				// Change defaults for CSV.
				co.Delimiter = ","
				co.Null = ""
			case "binary":
				co.Format = CopyFormatBinary
			}
		case "header":
			co.Header = defElemBool(o)
		case "delimiter":
			if val != "" {
				co.Delimiter = val
			}
		case "null":
			co.Null = val
		case "quote":
			if len(val) > 0 {
				co.Quote = val[0]
			}
		case "escape":
			if len(val) > 0 {
				co.Escape = val[0]
			}
		}
	}
	return co
}

// defElemString extracts a string value from a DefElem.
func defElemString(d *parser.DefElem) string {
	if d.Arg == nil {
		return d.Defname
	}
	switch v := d.Arg.(type) {
	case *parser.A_Const:
		return v.Val.Str
	case *parser.ColumnRef:
		if len(v.Fields) > 0 {
			if s, ok := v.Fields[0].(*parser.String); ok {
				return s.Str
			}
		}
	case *parser.String:
		return v.Str
	}
	return ""
}

// defElemBool extracts a boolean value from a DefElem.
// No arg means true (e.g., HEADER without a value).
func defElemBool(d *parser.DefElem) bool {
	if d.Arg == nil {
		return true
	}
	s := defElemString(d)
	switch strings.ToLower(s) {
	case "true", "on", "yes", "1":
		return true
	}
	return false
}

// execCopyTo handles COPY table/query TO STDOUT.
func (ex *Executor) execCopyTo(cs *parser.CopyStmt) (*Result, error) {
	opts := parseCopyOptions(cs.Options)
	if opts.Format == CopyFormatBinary {
		return nil, fmt.Errorf("COPY BINARY format is not supported")
	}

	// Get the result set: either from a sub-query or a table scan.
	var columns []string
	var rows [][]tuple.Datum

	if cs.Query != nil {
		// COPY (SELECT ...) TO STDOUT
		r, err := ex.Exec(parser.Deparse(cs.Query))
		if err != nil {
			return nil, fmt.Errorf("COPY TO query: %w", err)
		}
		columns = r.Columns
		rows = r.Rows
	} else {
		// COPY table [(cols)] TO STDOUT
		tableName := rangeVarName(cs.Relation)
		var selectSQL string
		if len(cs.Attlist) > 0 {
			selectSQL = fmt.Sprintf("SELECT %s FROM %s", strings.Join(cs.Attlist, ", "), tableName)
		} else {
			selectSQL = fmt.Sprintf("SELECT * FROM %s", tableName)
		}
		r, err := ex.Exec(selectSQL)
		if err != nil {
			return nil, fmt.Errorf("COPY TO: %w", err)
		}
		columns = r.Columns
		rows = r.Rows
	}

	// Format the output.
	var buf strings.Builder

	if opts.Header {
		if opts.Format == CopyFormatCSV {
			buf.WriteString(formatCSVLine(columns, opts))
		} else {
			buf.WriteString(strings.Join(columns, opts.Delimiter))
			buf.WriteByte('\n')
		}
	}

	for _, row := range rows {
		if opts.Format == CopyFormatCSV {
			strs := make([]string, len(row))
			for i, d := range row {
				if d.Type == tuple.TypeNull {
					strs[i] = opts.Null
				} else {
					strs[i] = datumToString(d)
				}
			}
			buf.WriteString(formatCSVLine(strs, opts))
		} else {
			for i, d := range row {
				if i > 0 {
					buf.WriteString(opts.Delimiter)
				}
				if d.Type == tuple.TypeNull {
					buf.WriteString(opts.Null)
				} else {
					buf.WriteString(escapeCopyText(datumToString(d)))
				}
			}
			buf.WriteByte('\n')
		}
	}

	return &Result{
		Columns: columns,
		Rows:    rows,
		Message: fmt.Sprintf("COPY %d", len(rows)),
		CopyData: buf.String(),
	}, nil
}

// execCopyFromFile handles COPY table FROM '/path/to/file'.
func (ex *Executor) execCopyFromFile(cs *parser.CopyStmt) (*Result, error) {
	opts := parseCopyOptions(cs.Options)
	if opts.Format == CopyFormatBinary {
		return nil, fmt.Errorf("COPY BINARY format is not supported")
	}

	f, err := os.Open(cs.Filename)
	if err != nil {
		return nil, fmt.Errorf("COPY FROM: %w", err)
	}
	defer f.Close()

	tableName := rangeVarName(cs.Relation)
	_, colTypes, err := ex.resolveCopyColumns(tableName, cs.Attlist)
	if err != nil {
		return nil, err
	}

	var count int64

	if opts.Format == CopyFormatCSV {
		reader := csv.NewReader(f)
		reader.Comma = rune(opts.Delimiter[0])
		if opts.Quote != '"' {
			// Go's csv package doesn't support custom quote chars directly,
			// but we handle the common case.
			reader.LazyQuotes = true
		}

		records, err := reader.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("COPY FROM CSV: %w", err)
		}

		startIdx := 0
		if opts.Header && len(records) > 0 {
			startIdx = 1
		}

		for _, fields := range records[startIdx:] {
			values, err := parseCopyFields(fields, colTypes, opts.Null)
			if err != nil {
				return nil, fmt.Errorf("COPY FROM CSV row %d: %w", count+1, err)
			}
			if _, err := ex.Cat.InsertInto(tableName, values); err != nil {
				return nil, fmt.Errorf("COPY FROM CSV row %d: %w", count+1, err)
			}
			count++
		}
	} else {
		// Text format: tab-separated, \. terminator.
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

		if opts.Header {
			scanner.Scan() // skip header line
		}

		for scanner.Scan() {
			line := scanner.Text()
			if line == "\\." {
				break
			}
			fields := strings.Split(line, opts.Delimiter)
			// Unescape text-format fields.
			for i := range fields {
				if fields[i] == opts.Null {
					continue // will be handled as NULL in parseCopyFields
				}
				fields[i] = unescapeCopyText(fields[i])
			}
			values, err := parseCopyFields(fields, colTypes, opts.Null)
			if err != nil {
				return nil, fmt.Errorf("COPY FROM row %d: %w", count+1, err)
			}
			if _, err := ex.Cat.InsertInto(tableName, values); err != nil {
				return nil, fmt.Errorf("COPY FROM row %d: %w", count+1, err)
			}
			count++
		}
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("COPY FROM: %w", err)
		}
	}

	return &Result{
		RowsAffected: count,
		Message:      fmt.Sprintf("COPY %d", count),
	}, nil
}

// ExecCopyFromData handles COPY FROM STDIN data that arrives as lines
// (from pgwire CopyData messages or test harnesses).
func (ex *Executor) ExecCopyFromData(cs *parser.CopyStmt, lines []string) (*Result, error) {
	opts := parseCopyOptions(cs.Options)
	if opts.Format == CopyFormatBinary {
		return nil, fmt.Errorf("COPY BINARY format is not supported")
	}

	tableName := rangeVarName(cs.Relation)
	_, colTypes, err := ex.resolveCopyColumns(tableName, cs.Attlist)
	if err != nil {
		return nil, err
	}

	var count int64
	startIdx := 0
	if opts.Header && len(lines) > 0 {
		startIdx = 1
	}

	if opts.Format == CopyFormatCSV {
		// Join lines and parse as CSV.
		joined := strings.Join(lines[startIdx:], "\n")
		reader := csv.NewReader(strings.NewReader(joined))
		reader.Comma = rune(opts.Delimiter[0])
		reader.LazyQuotes = true
		reader.FieldsPerRecord = -1 // variable

		for {
			record, err := reader.Read()
			if err != nil {
				break
			}
			values, err := parseCopyFields(record, colTypes, opts.Null)
			if err != nil {
				return nil, fmt.Errorf("COPY FROM STDIN CSV row %d: %w", count+1, err)
			}
			if _, err := ex.Cat.InsertInto(tableName, values); err != nil {
				return nil, fmt.Errorf("COPY FROM STDIN CSV row %d: %w", count+1, err)
			}
			count++
		}
	} else {
		for _, line := range lines[startIdx:] {
			if line == "\\." {
				break
			}
			fields := strings.Split(line, opts.Delimiter)
			for i := range fields {
				if fields[i] == opts.Null {
					continue
				}
				fields[i] = unescapeCopyText(fields[i])
			}
			values, err := parseCopyFields(fields, colTypes, opts.Null)
			if err != nil {
				return nil, fmt.Errorf("COPY FROM STDIN row %d: %w", count+1, err)
			}
			if _, err := ex.Cat.InsertInto(tableName, values); err != nil {
				return nil, fmt.Errorf("COPY FROM STDIN row %d: %w", count+1, err)
			}
			count++
		}
	}

	return &Result{
		RowsAffected: count,
		Message:      fmt.Sprintf("COPY %d", count),
	}, nil
}

// resolveCopyColumns resolves column names and types for a COPY target table.
// If attlist is empty, all columns are used.
func (ex *Executor) resolveCopyColumns(tableName string, attlist []string) ([]string, []tuple.DatumType, error) {
	rel, err := ex.Cat.FindRelation(tableName)
	if err != nil || rel == nil {
		return nil, nil, fmt.Errorf("COPY: relation %q does not exist", tableName)
	}
	cols, err := ex.Cat.GetColumns(rel.OID)
	if err != nil {
		return nil, nil, fmt.Errorf("COPY: cannot get columns for %q: %w", tableName, err)
	}

	if len(attlist) == 0 {
		names := make([]string, len(cols))
		types := make([]tuple.DatumType, len(cols))
		for i, c := range cols {
			names[i] = c.Name
			types[i] = tuple.DatumType(c.Type)
		}
		return names, types, nil
	}

	// Map requested columns to their types.
	colMap := make(map[string]tuple.DatumType)
	for _, c := range cols {
		colMap[strings.ToLower(c.Name)] = tuple.DatumType(c.Type)
	}
	names := make([]string, len(attlist))
	types := make([]tuple.DatumType, len(attlist))
	for i, name := range attlist {
		lower := strings.ToLower(name)
		dt, ok := colMap[lower]
		if !ok {
			return nil, nil, fmt.Errorf("COPY: column %q does not exist in %q", name, tableName)
		}
		names[i] = name
		types[i] = dt
	}
	return names, types, nil
}

// parseCopyFields converts string fields to Datum values based on column types.
func parseCopyFields(fields []string, colTypes []tuple.DatumType, nullStr string) ([]tuple.Datum, error) {
	values := make([]tuple.Datum, len(colTypes))
	for i := range colTypes {
		var raw string
		if i < len(fields) {
			raw = fields[i]
		} else {
			raw = nullStr
		}

		if raw == nullStr {
			values[i] = tuple.DNull()
			continue
		}

		switch colTypes[i] {
		case tuple.TypeInt32:
			v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
			if err != nil {
				return nil, fmt.Errorf("column %d: invalid int32 %q", i+1, raw)
			}
			values[i] = tuple.DInt32(int32(v))
		case tuple.TypeInt64:
			v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("column %d: invalid int64 %q", i+1, raw)
			}
			values[i] = tuple.DInt64(v)
		case tuple.TypeFloat64:
			v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
			if err != nil {
				return nil, fmt.Errorf("column %d: invalid float64 %q", i+1, raw)
			}
			values[i] = tuple.DFloat64(v)
		case tuple.TypeBool:
			switch strings.ToLower(strings.TrimSpace(raw)) {
			case "t", "true", "1", "yes", "on":
				values[i] = tuple.DBool(true)
			case "f", "false", "0", "no", "off":
				values[i] = tuple.DBool(false)
			default:
				return nil, fmt.Errorf("column %d: invalid bool %q", i+1, raw)
			}
		case tuple.TypeNumeric:
			values[i] = tuple.DNumeric(raw)
		case tuple.TypeJSON:
			values[i] = tuple.DJSON(raw)
		case tuple.TypeUUID:
			values[i] = tuple.DUUID(raw)
		default:
			values[i] = tuple.DText(raw)
		}
	}
	return values, nil
}

// datumToString converts a Datum to its string representation for COPY output.
func datumToString(d tuple.Datum) string {
	switch d.Type {
	case tuple.TypeNull:
		return ""
	case tuple.TypeInt32:
		return strconv.FormatInt(int64(d.I32), 10)
	case tuple.TypeInt64:
		return strconv.FormatInt(d.I64, 10)
	case tuple.TypeText:
		return d.Text
	case tuple.TypeBool:
		if d.Bool {
			return "t"
		}
		return "f"
	case tuple.TypeFloat64:
		return strconv.FormatFloat(d.F64, 'g', -1, 64)
	case tuple.TypeNumeric, tuple.TypeJSON, tuple.TypeUUID, tuple.TypeBytea, tuple.TypeArray:
		return d.Text
	default:
		return fmt.Sprintf("%v", d)
	}
}

// escapeCopyText escapes special characters for COPY text format output.
func escapeCopyText(s string) string {
	r := strings.NewReplacer(
		"\\", "\\\\",
		"\t", "\\t",
		"\n", "\\n",
		"\r", "\\r",
	)
	return r.Replace(s)
}

// unescapeCopyText reverses COPY text format escapes.
func unescapeCopyText(s string) string {
	r := strings.NewReplacer(
		"\\\\", "\\",
		"\\t", "\t",
		"\\n", "\n",
		"\\r", "\r",
	)
	return r.Replace(s)
}

// formatCSVLine formats a row as a CSV line using the configured options.
func formatCSVLine(fields []string, opts CopyOptions) string {
	var buf strings.Builder
	w := csv.NewWriter(&buf)
	w.Comma = rune(opts.Delimiter[0])
	w.Write(fields)
	w.Flush()
	return buf.String()
}

// ExecCopyFromDataRaw is the interface-typed wrapper for pgwire.
// It type-asserts the opaque copyStmt back to *parser.CopyStmt.
func (ex *Executor) ExecCopyFromDataRaw(copyStmt interface{}, lines []string) (*Result, error) {
	cs, ok := copyStmt.(*parser.CopyStmt)
	if !ok {
		return nil, fmt.Errorf("COPY FROM STDIN: invalid copy statement type")
	}
	return ex.ExecCopyFromData(cs, lines)
}

// execCopy dispatches a CopyStmt to the appropriate handler.
func (ex *Executor) execCopy(cs *parser.CopyStmt) (*Result, error) {
	if !cs.IsFrom {
		// COPY ... TO
		return ex.execCopyTo(cs)
	}

	// COPY ... FROM
	if cs.Filename != "" {
		// COPY table FROM '/path/to/file'
		return ex.execCopyFromFile(cs)
	}

	// COPY table FROM STDIN — return the parsed CopyStmt so the
	// pgwire layer can initiate the COPY sub-protocol and feed data.
	return &Result{
		Message:  "COPY_IN",
		CopyStmt: cs,
	}, nil
}

// rangeVarName extracts the table name from a RangeVar, handling schema qualification.
func rangeVarName(rv *parser.RangeVar) string {
	if rv == nil {
		return ""
	}
	name := rv.Relname
	if name == "" && rv.Alias != nil && rv.Alias.Aliasname != "" {
		name = rv.Alias.Aliasname
	}
	return name
}
