package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/jespino/loladb/pkg/catalog"
	"github.com/jespino/loladb/pkg/engine"
	"github.com/jespino/loladb/pkg/sql"
	"github.com/jespino/loladb/pkg/tuple"
)

func runImport(path string) {
	eng, err := engine.Open(path, 0)
	if err != nil {
		fatal(fmt.Sprintf("Failed to open database: %v", err))
	}
	defer eng.Close()

	cat, err := catalog.New(eng)
	if err != nil {
		fatal(fmt.Sprintf("Failed to load catalog: %v", err))
	}

	ex := sql.NewExecutor(cat)

	// Determine output format.
	format := os.Getenv("LOLADB_FORMAT")
	if format == "" {
		format = "table"
	}

	// Read stdin, accumulate lines into statements split by ';',
	// respecting dollar-quoted strings.
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024) // 1MB line buffer

	var buf strings.Builder
	stmtCount := 0
	errCount := 0
	skipCount := 0
	copyCount := 0
	lineNum := 0
	inDollarQuote := "" // tracks current dollar-quote tag (e.g. "$$", "$_$")

	// COPY state
	var copyTable string
	var copyCols []string
	var copyColTypes []int32
	inCopy := false

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// --- COPY FROM stdin handling ---
		if inCopy {
			trimmed := strings.TrimSpace(line)
			if trimmed == "\\." {
				// End of COPY data.
				if copyCount > 0 {
					fmt.Fprintf(os.Stderr, "COPY %s: %d rows\n", copyTable, copyCount)
				}
				inCopy = false
				copyTable = ""
				copyCols = nil
				copyColTypes = nil
				copyCount = 0
				stmtCount++
				continue
			}
			// Parse tab-separated row and insert.
			err := insertCopyRow(cat, copyTable, copyCols, copyColTypes, line)
			if err != nil {
				// Only report first few errors per COPY.
				if copyCount < 3 {
					fmt.Fprintf(os.Stderr, "ERROR (line %d): COPY %s: %v\n", lineNum, copyTable, err)
				}
				errCount++
			}
			copyCount++
			continue
		}

		// Skip comments and empty lines (only when not inside a dollar-quoted block).
		trimmed := strings.TrimSpace(line)
		if inDollarQuote == "" {
			if trimmed == "" || strings.HasPrefix(trimmed, "--") {
				continue
			}
		}

		buf.WriteString(line)
		buf.WriteString("\n")

		// Track dollar-quoting: scan the line for dollar-quote markers.
		inDollarQuote = trackDollarQuote(line, inDollarQuote)

		// Don't split if we're inside a dollar-quoted block.
		if inDollarQuote != "" {
			continue
		}

		// Check if we have a complete statement (ends with ;).
		full := strings.TrimSpace(buf.String())
		if !strings.HasSuffix(full, ";") {
			continue
		}

		// Remove trailing semicolon and execute.
		full = strings.TrimRight(full, ";")
		full = strings.TrimSpace(full)
		buf.Reset()

		if full == "" {
			continue
		}

		// Check for COPY ... FROM stdin.
		if tbl, cols := parseCopyStmt(full); tbl != "" {
			// Verify the table exists before starting COPY.
			rel, _ := cat.FindRelation(tbl)
			if rel == nil {
				fmt.Fprintf(os.Stderr, "SKIP COPY: table %q not found\n", tbl)
				// Consume all lines until \. to skip this COPY block.
				for scanner.Scan() {
					lineNum++
					if strings.TrimSpace(scanner.Text()) == "\\." {
						break
					}
				}
				skipCount++
				continue
			}
			inCopy = true
			copyTable = tbl
			copyCols = cols
			copyCount = 0
			copyColTypes = lookupColTypes(cat, copyTable, copyCols)
			continue
		}

		// Skip statements that are purely PL/pgSQL fragments
		// (leftover from broken dollar-quoted functions).
		if isPLpgSQLFragment(full) {
			skipCount++
			continue
		}

		// Preprocess the SQL to handle PostgreSQL-specific syntax
		// that our parser doesn't support directly.
		full = preprocessSQL(full)

		r, err := ex.Exec(full)
		if err != nil {
			// Silently skip common pg dump patterns that our parser doesn't support.
			if isExpectedParseFailure(full, err) {
				skipCount++
				continue
			}
			fmt.Fprintf(os.Stderr, "ERROR (line %d): %v\n", lineNum, err)
			fmt.Fprintf(os.Stderr, "  SQL: %s\n", truncate(full, 80))
			errCount++
			continue
		}
		stmtCount++

		// Print results for SELECT queries.
		if len(r.Rows) > 0 {
			fmt.Print(formatResult(r, format))
		} else if r.Message != "" {
			fmt.Println(r.Message)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Read error: %v\n", err)
	}

	// Execute any remaining partial statement.
	remaining := strings.TrimSpace(buf.String())
	remaining = strings.TrimRight(remaining, ";")
	remaining = strings.TrimSpace(remaining)
	if remaining != "" && !isPLpgSQLFragment(remaining) {
		remaining = preprocessSQL(remaining)
		r, err := ex.Exec(remaining)
		if err != nil {
			if !isExpectedParseFailure(remaining, err) {
				fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
				errCount++
			} else {
				skipCount++
			}
		} else {
			stmtCount++
			if len(r.Rows) > 0 {
				fmt.Print(formatResult(r, format))
			} else if r.Message != "" {
				fmt.Println(r.Message)
			}
		}
	}

	fmt.Fprintf(os.Stderr, "Import complete: %d statements executed, %d skipped", stmtCount, skipCount)
	if errCount > 0 {
		fmt.Fprintf(os.Stderr, ", %d errors", errCount)
	}
	fmt.Fprintln(os.Stderr)
}

// --- COPY FROM stdin support ---

// parseCopyStmt parses "COPY public.tablename (col1, col2, ...) FROM stdin"
// and returns the table name and column names. Returns ("", nil) if not a COPY.
func parseCopyStmt(stmt string) (string, []string) {
	upper := strings.ToUpper(strings.TrimSpace(stmt))
	if !strings.HasPrefix(upper, "COPY ") || !strings.Contains(upper, "FROM STDIN") {
		return "", nil
	}

	// Extract table name: COPY [public.]tablename (col1, col2, ...)
	rest := strings.TrimSpace(stmt[5:]) // after "COPY "

	// Find table name (possibly schema-qualified).
	spaceIdx := strings.IndexAny(rest, " (")
	if spaceIdx < 0 {
		return "", nil
	}
	tableName := rest[:spaceIdx]
	// Strip schema prefix.
	if dotIdx := strings.LastIndex(tableName, "."); dotIdx >= 0 {
		tableName = tableName[dotIdx+1:]
	}

	// Extract column list between parens.
	openParen := strings.Index(rest, "(")
	closeParen := strings.Index(rest, ")")
	if openParen < 0 || closeParen < 0 {
		return tableName, nil
	}
	colStr := rest[openParen+1 : closeParen]
	parts := strings.Split(colStr, ",")
	cols := make([]string, len(parts))
	for i, p := range parts {
		cols[i] = strings.TrimSpace(p)
	}
	return tableName, cols
}

// lookupColTypes returns the datum types for the given columns in the table.
func lookupColTypes(cat *catalog.Catalog, tableName string, colNames []string) []int32 {
	rel, err := cat.FindRelation(tableName)
	if err != nil || rel == nil {
		return nil
	}
	schemaCols, err := cat.GetColumns(rel.OID)
	if err != nil {
		return nil
	}

	// Build a map of column name → type.
	typeMap := make(map[string]int32)
	for _, c := range schemaCols {
		typeMap[strings.ToLower(c.Name)] = c.Type
	}

	types := make([]int32, len(colNames))
	for i, name := range colNames {
		if t, ok := typeMap[strings.ToLower(name)]; ok {
			types[i] = t
		} else {
			types[i] = int32(tuple.TypeText) // default
		}
	}
	return types
}

// insertCopyRow parses a tab-separated line and inserts it into the table.
func insertCopyRow(cat *catalog.Catalog, tableName string, colNames []string, colTypes []int32, line string) error {
	fields := strings.Split(line, "\t")

	// Build datum values based on column types.
	values := make([]tuple.Datum, len(colNames))
	for i := range colNames {
		var raw string
		if i < len(fields) {
			raw = fields[i]
		}

		if raw == "\\N" || raw == "" {
			values[i] = tuple.DNull()
			continue
		}

		var colType tuple.DatumType
		if i < len(colTypes) {
			colType = tuple.DatumType(colTypes[i])
		} else {
			colType = tuple.TypeText
		}

		switch colType {
		case tuple.TypeInt32:
			v, err := strconv.ParseInt(raw, 10, 32)
			if err != nil {
				values[i] = tuple.DNull()
			} else {
				values[i] = tuple.DInt32(int32(v))
			}
		case tuple.TypeInt64:
			v, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				values[i] = tuple.DNull()
			} else {
				values[i] = tuple.DInt64(v)
			}
		case tuple.TypeFloat64:
			v, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				values[i] = tuple.DNull()
			} else {
				values[i] = tuple.DFloat64(v)
			}
		case tuple.TypeBool:
			values[i] = tuple.DBool(raw == "t" || raw == "true" || raw == "TRUE" || raw == "1")
		default:
			// Text and everything else stored as text.
			// Unescape PostgreSQL COPY escapes.
			raw = unescapeCopyText(raw)
			values[i] = tuple.DText(raw)
		}
	}

	_, err := cat.InsertInto(tableName, values)
	return err
}

// unescapeCopyText handles PostgreSQL COPY format text escapes.
func unescapeCopyText(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	i := 0
	for i < len(s) {
		if i+1 < len(s) && s[i] == '\\' {
			switch s[i+1] {
			case 'n':
				b.WriteByte('\n')
				i += 2
			case 't':
				b.WriteByte('\t')
				i += 2
			case 'r':
				b.WriteByte('\r')
				i += 2
			case '\\':
				b.WriteByte('\\')
				i += 2
			default:
				b.WriteByte(s[i])
				i++
			}
		} else {
			b.WriteByte(s[i])
			i++
		}
	}
	return b.String()
}

// preprocessSQL rewrites PostgreSQL-specific syntax that our parser
// doesn't handle into equivalent forms it can parse.
func preprocessSQL(sql string) string {
	// Strip PARTITION BY clauses from CREATE TABLE.
	// e.g. "... ) PARTITION BY RANGE (payment_date)" → "... )"
	if idx := findPartitionBy(sql); idx >= 0 {
		sql = strings.TrimSpace(sql[:idx])
		// Make sure it still ends with )
		if !strings.HasSuffix(sql, ")") {
			sql += ")"
		}
	}

	// Replace schema-qualified custom types with TEXT.
	// Matches patterns like "public.typename" in column definitions.
	sql = replaceCustomTypes(sql)

	// Replace PostgreSQL-specific types the parser doesn't handle.
	sql = replaceUnsupportedTypes(sql)

	return sql
}

// findPartitionBy returns the index of "PARTITION BY" in a CREATE TABLE
// statement, or -1 if not found.
func findPartitionBy(sql string) int {
	upper := strings.ToUpper(sql)
	if !strings.Contains(upper, "CREATE TABLE") {
		return -1
	}
	idx := strings.Index(upper, "PARTITION BY")
	if idx < 0 {
		return -1
	}
	// Walk backward to find the closing paren before PARTITION BY.
	for i := idx - 1; i >= 0; i-- {
		if sql[i] == ')' {
			return i + 1 // keep the closing paren, strip from there
		}
	}
	return idx
}

// replaceCustomTypes replaces schema-qualified types (e.g., public.year,
// public.mpaa_rating) with TEXT in column definitions.
func replaceCustomTypes(sql string) string {
	upper := strings.ToUpper(sql)
	if !strings.Contains(upper, "CREATE TABLE") {
		return sql
	}

	// Find patterns like "public.<identifier>" that appear as column types.
	// We do a simple scan: replace "public.<word>" with "text" when it
	// appears after a column name in a CREATE TABLE.
	lines := strings.Split(sql, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		// Skip non-column-definition lines.
		if strings.HasPrefix(strings.ToUpper(trimmed), "CREATE") ||
			strings.HasPrefix(trimmed, ")") ||
			strings.HasPrefix(trimmed, "--") ||
			trimmed == "" {
			continue
		}

		// Replace public.<identifier> type references with TEXT.
		lines[i] = replaceSchemaQualifiedType(line)
	}
	return strings.Join(lines, "\n")
}

func replaceSchemaQualifiedType(line string) string {
	// Look for "public." followed by an identifier in the line,
	// but only when it's used as a column type (not inside quotes
	// or as a table name reference).
	result := line
	for {
		lower := strings.ToLower(result)
		idx := strings.Index(lower, "public.")
		if idx < 0 {
			break
		}

		// Check if we're inside a quoted string — don't modify.
		if isInsideQuotes(result, idx) {
			result = result[:idx] + "PUBLIC_SKIP_" + result[idx+7:]
			continue
		}

		// Extract the full "public.<identifier>" token.
		end := idx + 7 // past "public."
		for end < len(result) && (isIdentChar(result[end]) || result[end] == '"') {
			end++
		}

		// Don't replace schema-qualified table names.
		beforeToken := strings.TrimSpace(result[:idx])
		upperBefore := strings.ToUpper(beforeToken)
		if strings.HasSuffix(upperBefore, "TABLE") ||
			strings.HasSuffix(upperBefore, "ON") ||
			strings.HasSuffix(upperBefore, "FROM") ||
			strings.HasSuffix(upperBefore, "INTO") ||
			strings.HasSuffix(upperBefore, "INDEX") ||
			strings.HasSuffix(upperBefore, "VIEW") ||
			strings.HasSuffix(upperBefore, "SEQUENCE") {
			result = result[:idx] + "PUBLIC_SKIP_" + result[idx+7:]
			continue
		}

		// Replace the type with TEXT.
		result = result[:idx] + "text" + result[end:]
	}
	result = strings.ReplaceAll(result, "PUBLIC_SKIP_", "public.")
	return result
}

// isInsideQuotes checks if position idx is inside a single-quoted string.
func isInsideQuotes(s string, idx int) bool {
	inQuote := false
	for i := 0; i < idx; i++ {
		if s[i] == '\'' {
			inQuote = !inQuote
		}
	}
	return inQuote
}

// replaceUnsupportedTypes replaces PostgreSQL-specific column types
// that the parser can't handle with TEXT equivalents.
func replaceUnsupportedTypes(sql string) string {
	upper := strings.ToUpper(sql)
	if !strings.Contains(upper, "CREATE TABLE") {
		return sql
	}

	// Replace array types: "text[]", "integer[]", etc.
	// Match word followed by []
	lines := strings.Split(sql, "\n")
	for i, line := range lines {
		// Replace array types.
		for {
			idx := strings.Index(line, "[]")
			if idx < 0 {
				break
			}
			// Find the start of the type name before [].
			start := idx - 1
			for start >= 0 && (isIdentChar(line[start]) || line[start] == '.') {
				start--
			}
			start++
			line = line[:start] + "text" + line[idx+2:]
		}
		// Replace tsvector.
		line = replaceCaseInsensitive(line, "tsvector", "text")
		lines[i] = line
	}
	return strings.Join(lines, "\n")
}

func replaceCaseInsensitive(s, old, replacement string) string {
	lower := strings.ToLower(s)
	lowerOld := strings.ToLower(old)
	idx := strings.Index(lower, lowerOld)
	if idx < 0 {
		return s
	}
	return s[:idx] + replacement + s[idx+len(old):]
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}

// trackDollarQuote scans a line for dollar-quote markers and updates the
// current dollar-quote state. If we're not in a dollar-quoted block
// (current == ""), it looks for an opening marker. If we are, it looks
// for the matching closing marker.
func trackDollarQuote(line, current string) string {
	i := 0
	for i < len(line) {
		if line[i] != '$' {
			i++
			continue
		}
		// Found a '$', try to extract a dollar-quote tag: $tag$ where tag is [a-zA-Z0-9_]*
		tag := extractDollarTag(line, i)
		if tag == "" {
			i++
			continue
		}
		if current == "" {
			// Opening a dollar-quoted block.
			current = tag
			i += len(tag)
		} else if tag == current {
			// Closing the dollar-quoted block.
			current = ""
			i += len(tag)
		} else {
			i += len(tag)
		}
	}
	return current
}

// extractDollarTag tries to extract a dollar-quote tag starting at position i.
// A dollar tag is $<identifier>$ where identifier is [a-zA-Z0-9_]*.
// Returns the full tag including both $ signs, or "" if not a valid tag.
func extractDollarTag(line string, i int) string {
	if i >= len(line) || line[i] != '$' {
		return ""
	}
	j := i + 1
	for j < len(line) && (isIdentChar(line[j])) {
		j++
	}
	if j >= len(line) || line[j] != '$' {
		return ""
	}
	return line[i : j+1] // includes both $ signs
}

func isIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') || c == '_'
}

// isPLpgSQLFragment detects leftover PL/pgSQL code that isn't a valid
// top-level SQL statement (e.g., DECLARE, BEGIN, END blocks from
// functions that were split incorrectly).
func isPLpgSQLFragment(s string) bool {
	upper := strings.ToUpper(strings.TrimSpace(s))
	plFragments := []string{
		"DECLARE", "BEGIN", "END", "RETURN", "RAISE",
		"IF ", "ELSIF", "ELSE", "LOOP", "FOR ",
		"EXECUTE ", "PERFORM ",
	}
	for _, frag := range plFragments {
		if strings.HasPrefix(upper, frag) {
			return true
		}
	}
	// Also skip lines that are just variable declarations or assignments
	// typical of PL/pgSQL.
	if strings.Contains(upper, ":=") {
		return true
	}
	return false
}

// isExpectedParseFailure returns true for SQL statements that are common
// in PostgreSQL dumps but not supported by our parser library, so we
// skip them silently instead of printing noisy errors.
func isExpectedParseFailure(sql string, err error) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	// Common pg dump patterns our parser can't handle.
	skipPrefixes := []string{
		"ALTER ", // covers ALTER ... OWNER TO, ALTER TABLE ... ATTACH PARTITION
		"CREATE DOMAIN",
		"CREATE TYPE",
		"CREATE FUNCTION",
		"CREATE OR REPLACE FUNCTION",
		"CREATE TRIGGER",
		"CREATE AGGREGATE",
		"CREATE MATERIALIZED VIEW",
		"REVOKE",
		"GRANT",
		"COMMENT ON",
		"SELECT PG_CATALOG.SETVAL",
		"COPY ",
	}
	for _, prefix := range skipPrefixes {
		if strings.HasPrefix(upper, prefix) {
			errStr := err.Error()
			if strings.Contains(errStr, "parse error") {
				return true
			}
			if strings.Contains(upper, "OWNER TO") ||
				strings.Contains(upper, "PARTITION BY") ||
				strings.Contains(upper, "ATTACH PARTITION") {
				return true
			}
		}
	}

	// Also skip parse errors for specific unsupported syntax patterns
	// regardless of statement prefix (e.g., USING gist in CREATE INDEX).
	errStr := err.Error()
	if strings.Contains(errStr, "parse error") {
		if strings.Contains(upper, "USING GIST") ||
			strings.Contains(upper, "USING GIN") ||
			strings.Contains(upper, "MATERIALIZED VIEW") {
			return true
		}
	}

	return false
}
