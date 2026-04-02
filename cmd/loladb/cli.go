package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/chzyer/readline"

	"github.com/gololadb/loladb/pkg/catalog"
	"github.com/gololadb/loladb/pkg/engine"
	"github.com/gololadb/loladb/pkg/sql"
)

func runCLI(path string) {
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

	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "loladb> ",
		HistoryFile:     os.TempDir() + "/loladb_history",
		InterruptPrompt: "^C",
		EOFPrompt:       "\\q",
	})
	if err != nil {
		fatal(fmt.Sprintf("Failed to init readline: %v", err))
	}
	defer rl.Close()

	fmt.Printf("LolaDB shell — connected to %s\n", path)
	fmt.Println("Type \\q to quit, \\dt for tables, \\di for indexes, \\d <table> for schema.")
	fmt.Println()

	var buf strings.Builder
	multiLine := false
	format := "table"

	for {
		if multiLine {
			rl.SetPrompt("     -> ")
		} else {
			rl.SetPrompt("loladb> ")
		}

		line, err := rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				buf.Reset()
				multiLine = false
				continue
			}
			if err == io.EOF {
				break
			}
			break
		}

		trimmed := strings.TrimSpace(line)

		// Meta-commands.
		if !multiLine && strings.HasPrefix(trimmed, "\\") {
			handleMeta(trimmed, cat, &format)
			continue
		}

		buf.WriteString(line)
		buf.WriteString("\n")

		// Check if statement is complete (ends with ;).
		full := strings.TrimSpace(buf.String())
		if !strings.HasSuffix(full, ";") {
			multiLine = true
			continue
		}

		// Remove trailing semicolon.
		full = strings.TrimSuffix(full, ";")
		buf.Reset()
		multiLine = false

		if full == "" {
			continue
		}

		r, err := ex.Exec(full)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			continue
		}
		fmt.Print(formatResult(r, format))
	}
}

func handleMeta(cmd string, cat *catalog.Catalog, format *string) {
	parts := strings.Fields(cmd)
	switch parts[0] {
	case "\\q":
		fmt.Println("Bye!")
		os.Exit(0)
	case "\\dt":
		tables, err := cat.ListTables()
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			return
		}
		fmt.Println("Tables:")
		for _, t := range tables {
			fmt.Printf("  %-20s (OID=%d, pages=%d)\n", t.Name, t.OID, t.Pages)
		}
		if len(tables) == 0 {
			fmt.Println("  (none)")
		}
	case "\\di":
		tables, _ := cat.ListTables()
		fmt.Println("Indexes:")
		count := 0
		for _, t := range tables {
			indexes, _ := cat.ListIndexesForTable(t.OID)
			for _, idx := range indexes {
				cols, _ := cat.GetColumns(t.OID)
				colName := "?"
				if int(idx.ColNum-1) < len(cols) {
					colName = cols[idx.ColNum-1].Name
				}
				fmt.Printf("  %-20s on %s(%s)\n", idx.Name, t.Name, colName)
				count++
			}
		}
		if count == 0 {
			fmt.Println("  (none)")
		}
	case "\\d":
		if len(parts) < 2 {
			fmt.Println("Usage: \\d <table>")
			return
		}
		tableName := parts[1]
		rel, err := cat.FindRelation(tableName)
		if err != nil || rel == nil {
			fmt.Printf("Table %q not found.\n", tableName)
			return
		}
		cols, _ := cat.GetColumns(rel.OID)
		fmt.Printf("Table %q:\n", tableName)
		for _, c := range cols {
			fmt.Printf("  %-20s %s\n", c.Name, typeName(c.Type))
		}
	case "\\format":
		if len(parts) < 2 {
			fmt.Printf("Current format: %s\n", *format)
			fmt.Println("Usage: \\format table|csv|json")
			return
		}
		switch parts[1] {
		case "table", "csv", "json":
			*format = parts[1]
			fmt.Printf("Output format: %s\n", *format)
		default:
			fmt.Println("Unknown format. Use: table, csv, json")
		}
	default:
		fmt.Printf("Unknown command: %s\n", parts[0])
	}
}
