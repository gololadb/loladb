package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/gololadb/loladb/pkg/catalog"
	"github.com/gololadb/loladb/pkg/engine"
	"github.com/gololadb/loladb/pkg/sql"
)

func runExec(path, sqlStr string, opts ...string) {
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

	// Apply --role if provided.
	for _, opt := range opts {
		if strings.HasPrefix(opt, "--role=") {
			role := strings.TrimPrefix(opt, "--role=")
			ex.SetRole(role)
		}
	}

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
