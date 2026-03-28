package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/jespino/loladb/pkg/catalog"
	"github.com/jespino/loladb/pkg/engine"
	"github.com/jespino/loladb/pkg/sql"
)

func runImport(path string) {
	eng, err := engine.Open(path, 256)
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

	// Read stdin, accumulate lines into statements split by ';'.
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024) // 1MB line buffer

	var buf strings.Builder
	stmtCount := 0
	errCount := 0
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Skip comments and empty lines.
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}

		buf.WriteString(line)
		buf.WriteString("\n")

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

		r, err := ex.Exec(full)
		if err != nil {
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
	if remaining != "" {
		r, err := ex.Exec(remaining)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			errCount++
		} else {
			stmtCount++
			if len(r.Rows) > 0 {
				fmt.Print(formatResult(r, format))
			} else if r.Message != "" {
				fmt.Println(r.Message)
			}
		}
	}

	fmt.Fprintf(os.Stderr, "Import complete: %d statements executed", stmtCount)
	if errCount > 0 {
		fmt.Fprintf(os.Stderr, ", %d errors", errCount)
	}
	fmt.Fprintln(os.Stderr)

	if errCount > 0 {
		os.Exit(1)
	}
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}
