package main

import (
	"fmt"
	"os"

	"github.com/jespino/loladb/pkg/catalog"
	"github.com/jespino/loladb/pkg/engine"
	"github.com/jespino/loladb/pkg/sql"
)

func runExec(path, sqlStr string) {
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
	r, err := ex.Exec(sqlStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}

	// Determine format from env or default to table.
	format := os.Getenv("LOLADB_FORMAT")
	if format == "" {
		format = "table"
	}
	fmt.Print(formatResult(r, format))
}
