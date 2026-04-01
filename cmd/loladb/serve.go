package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jespino/loladb/pkg/catalog"
	"github.com/jespino/loladb/pkg/engine"
	"github.com/jespino/loladb/pkg/pgwire"
	"github.com/jespino/loladb/pkg/sql"
)

// sqlAdapter bridges sql.Executor to pgwire.QueryExecutor.
type sqlAdapter struct {
	exec *sql.Executor
}

func (a *sqlAdapter) Execute(sqlStr string) (*pgwire.QueryResult, error) {
	r, err := a.exec.Exec(sqlStr)
	if err != nil {
		return nil, err
	}
	msg := r.Message
	// PostgreSQL sends "SELECT <count>" for queries returning rows.
	if msg == "" && len(r.Columns) > 0 {
		msg = fmt.Sprintf("SELECT %d", len(r.Rows))
	}
	return &pgwire.QueryResult{
		Columns:      r.Columns,
		Rows:         r.Rows,
		RowsAffected: r.RowsAffected,
		Message:      msg,
	}, nil
}

func runServe(path string, addr string) {
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
	adapter := &sqlAdapter{exec: ex}

	srv := &pgwire.Server{
		Addr:     addr,
		Executor: adapter,
	}

	// Graceful shutdown on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nShutting down...")
		srv.Close()
	}()

	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "serve: %v\n", err)
		os.Exit(1)
	}
}
