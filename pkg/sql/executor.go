package sql

import (
	"fmt"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"

	"github.com/jespino/loladb/pkg/catalog"
	"github.com/jespino/loladb/pkg/executor"
	"github.com/jespino/loladb/pkg/planner"
	"github.com/jespino/loladb/pkg/tuple"
)

// Result holds the result of a SQL execution.
type Result struct {
	Columns      []string
	Rows         [][]tuple.Datum
	RowsAffected int64
	Message      string
}

// Executor parses SQL, builds a logical plan, optimizes it into a
// physical plan, and executes it. This is the full pipeline:
// SQL → Parser → Analyzer → Optimizer → Executor → Result
type Executor struct {
	Cat       *catalog.Catalog
	analyzer  *planner.Analyzer
	optimizer *planner.Optimizer
	exec      *executor.Executor
}

// NewExecutor creates a SQL executor backed by the given catalog.
func NewExecutor(cat *catalog.Catalog) *Executor {
	return &Executor{
		Cat:       cat,
		analyzer:  &planner.Analyzer{Cat: cat},
		optimizer: &planner.Optimizer{Cat: cat, Costs: planner.DefaultCosts()},
		exec:      executor.NewExecutor(cat),
	}
}

// Exec parses and executes one or more SQL statements through the
// full pipeline: parse → analyze → optimize → execute.
func (ex *Executor) Exec(sql string) (*Result, error) {
	stmts, err := parser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("sql: parse error: %w", err)
	}
	if len(stmts) == 0 {
		return &Result{Message: "OK"}, nil
	}

	stmt := stmts[0].AST

	// Check for EXPLAIN.
	isExplain := false
	isAnalyze := false
	if explain, ok := stmt.(*tree.Explain); ok {
		isExplain = true
		_ = isAnalyze // EXPLAIN ANALYZE not yet supported by pg parser binding
		stmt = explain.Statement
	}

	// Analyze: AST → Logical Plan
	logical, err := ex.analyzer.Analyze(stmt)
	if err != nil {
		return nil, err
	}

	// Optimize: Logical Plan → Physical Plan
	physical, err := ex.optimizer.Optimize(logical)
	if err != nil {
		return nil, err
	}

	if isExplain {
		r, err := ex.exec.ExecuteExplain(physical, isAnalyze)
		if err != nil {
			return nil, err
		}
		return convertResult(r), nil
	}

	// Execute
	r, err := ex.exec.Execute(physical)
	if err != nil {
		return nil, err
	}

	return convertResult(r), nil
}

// ExplainPlan returns the physical plan text without executing.
func (ex *Executor) ExplainPlan(sql string) (string, error) {
	stmts, err := parser.Parse(sql)
	if err != nil {
		return "", err
	}
	if len(stmts) == 0 {
		return "", nil
	}

	stmt := stmts[0].AST
	if explain, ok := stmt.(*tree.Explain); ok {
		stmt = explain.Statement
	}

	logical, err := ex.analyzer.Analyze(stmt)
	if err != nil {
		return "", err
	}
	physical, err := ex.optimizer.Optimize(logical)
	if err != nil {
		return "", err
	}
	return planner.Explain(physical), nil
}

func convertResult(r *executor.Result) *Result {
	// Strip table qualifiers from column names for cleaner output.
	cols := make([]string, len(r.Columns))
	for i, c := range r.Columns {
		cols[i] = stripQualifier(c)
	}
	return &Result{
		Columns:      cols,
		Rows:         r.Rows,
		RowsAffected: r.RowsAffected,
		Message:      r.Message,
	}
}

func stripQualifier(name string) string {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '.' {
			return name[i+1:]
		}
	}
	return name
}
