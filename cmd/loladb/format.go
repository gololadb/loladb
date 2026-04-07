package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	qt "github.com/gololadb/loladb/pkg/querytree"
	loladbsql "github.com/gololadb/loladb/pkg/sql"
	"github.com/gololadb/loladb/pkg/tuple"
)

func formatResult(r *loladbsql.Result, format string) string {
	switch format {
	case "json":
		return formatJSON(r)
	case "csv":
		return formatCSV(r)
	default:
		return formatTable(r)
	}
}

func formatTable(r *loladbsql.Result) string {
	if len(r.Rows) == 0 && r.Message != "" {
		return r.Message
	}

	if len(r.Columns) == 0 {
		if r.RowsAffected > 0 {
			return r.Message
		}
		return "(empty)"
	}

	// Calculate column widths.
	widths := make([]int, len(r.Columns))
	for i, c := range r.Columns {
		widths[i] = len(c)
	}
	for _, row := range r.Rows {
		for i, d := range row {
			if i < len(widths) {
				s := datumString(d)
				if len(s) > widths[i] {
					widths[i] = len(s)
					if widths[i] > 40 {
						widths[i] = 40
					}
				}
			}
		}
	}
	// Fix: widths should be int
	for i := range widths {
		if widths[i] < 4 {
			widths[i] = 4
		}
	}

	var sb strings.Builder

	// Header.
	for i, c := range r.Columns {
		if i > 0 {
			sb.WriteString(" | ")
		}
		sb.WriteString(padRight(c, widths[i]))
	}
	sb.WriteString("\n")

	// Separator.
	for i := range r.Columns {
		if i > 0 {
			sb.WriteString("-+-")
		}
		sb.WriteString(strings.Repeat("-", widths[i]))
	}
	sb.WriteString("\n")

	// Rows.
	for _, row := range r.Rows {
		for i, d := range row {
			if i >= len(widths) {
				break
			}
			if i > 0 {
				sb.WriteString(" | ")
			}
			sb.WriteString(padRight(datumString(d), widths[i]))
		}
		sb.WriteString("\n")
	}

	sb.WriteString(fmt.Sprintf("(%d rows)\n", len(r.Rows)))
	return sb.String()
}

func formatCSV(r *loladbsql.Result) string {
	var sb strings.Builder
	sb.WriteString(strings.Join(r.Columns, ","))
	sb.WriteString("\n")
	for _, row := range r.Rows {
		vals := make([]string, len(row))
		for i, d := range row {
			vals[i] = datumString(d)
		}
		sb.WriteString(strings.Join(vals, ","))
		sb.WriteString("\n")
	}
	return sb.String()
}

func formatJSON(r *loladbsql.Result) string {
	var rows []map[string]interface{}
	for _, row := range r.Rows {
		m := make(map[string]interface{})
		for i, d := range row {
			if i < len(r.Columns) {
				m[r.Columns[i]] = datumToInterface(d)
			}
		}
		rows = append(rows, m)
	}
	b, _ := json.MarshalIndent(rows, "", "  ")
	return string(b)
}

func datumString(d tuple.Datum) string {
	switch d.Type {
	case tuple.TypeNull:
		return "NULL"
	case tuple.TypeInt32:
		return fmt.Sprintf("%d", d.I32)
	case tuple.TypeInt64:
		return fmt.Sprintf("%d", d.I64)
	case tuple.TypeText:
		return d.Text
	case tuple.TypeBool:
		if d.Bool {
			return "true"
		}
		return "false"
	case tuple.TypeFloat64:
		return fmt.Sprintf("%g", d.F64)
	case tuple.TypeDate:
		return time.Unix(d.I64*86400, 0).UTC().Format("2006-01-02")
	case tuple.TypeTimestamp:
		return time.Unix(0, d.I64*1000).UTC().Format("2006-01-02 15:04:05")
	case tuple.TypeNumeric, tuple.TypeJSON, tuple.TypeUUID:
		return d.Text
	case tuple.TypeInterval:
		return qt.FormatInterval(d.I32, d.I64)
	case tuple.TypeBytea:
		return d.Text
	case tuple.TypeArray:
		return d.Text
	case tuple.TypeMoney:
		dollars := d.I64 / 100
		cents := d.I64 % 100
		if cents < 0 {
			cents = -cents
		}
		return fmt.Sprintf("$%d.%02d", dollars, cents)
	default:
		return "?"
	}
}

func datumToInterface(d tuple.Datum) interface{} {
	switch d.Type {
	case tuple.TypeNull:
		return nil
	case tuple.TypeInt32:
		return d.I32
	case tuple.TypeInt64:
		return d.I64
	case tuple.TypeText:
		return d.Text
	case tuple.TypeBool:
		return d.Bool
	case tuple.TypeFloat64:
		return d.F64
	case tuple.TypeDate:
		return time.Unix(d.I64*86400, 0).UTC().Format("2006-01-02")
	case tuple.TypeTimestamp:
		return time.Unix(0, d.I64*1000).UTC().Format("2006-01-02 15:04:05")
	case tuple.TypeNumeric, tuple.TypeJSON, tuple.TypeUUID:
		return d.Text
	case tuple.TypeInterval:
		return qt.FormatInterval(d.I32, d.I64)
	case tuple.TypeBytea:
		return d.Text
	case tuple.TypeArray:
		return d.Text
	case tuple.TypeMoney:
		dollars := d.I64 / 100
		cents := d.I64 % 100
		if cents < 0 {
			cents = -cents
		}
		return fmt.Sprintf("$%d.%02d", dollars, cents)
	default:
		return nil
	}
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s[:width]
	}
	return s + strings.Repeat(" ", width-len(s))
}
