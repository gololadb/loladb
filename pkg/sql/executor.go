package sql

import (
	"fmt"
	"strings"

	"github.com/jespino/gopgsql/parser"

	"github.com/jespino/loladb/pkg/catalog"
	"github.com/jespino/loladb/pkg/executor"
	"github.com/jespino/loladb/pkg/planner"
	"github.com/jespino/loladb/pkg/rewriter"
	"github.com/jespino/loladb/pkg/tuple"
)

// Result holds the result of a SQL execution.
type Result struct {
	Columns      []string
	Rows         [][]tuple.Datum
	RowsAffected int64
	Message      string
}

// Executor parses SQL and runs it through the full pipeline:
// SQL → Parser → Analyzer (Query tree) → Rewriter → Planner (Logical) → Optimizer (Physical) → Executor → Result
type Executor struct {
	Cat         *catalog.Catalog
	CurrentUser string // session-level current user for RLS policies
	analyzer    *planner.Analyzer
	rewriter    *rewriter.Rewriter
	optimizer   *planner.Optimizer
	exec        *executor.Executor
}

// NewExecutor creates a SQL executor backed by the given catalog.
func NewExecutor(cat *catalog.Catalog) *Executor {
	a := &planner.Analyzer{Cat: cat}
	return &Executor{
		Cat:       cat,
		analyzer:  a,
		rewriter:  rewriter.New(cat, a),
		optimizer: &planner.Optimizer{Cat: cat, Costs: planner.DefaultCosts()},
		exec:      executor.NewExecutor(cat),
	}
}

// SetRole sets the session-level current user for RLS policy evaluation.
func (ex *Executor) SetRole(role string) {
	ex.CurrentUser = role
	ex.rewriter.CurrentUser = role
	ex.exec.CurrentUser = role
}

// Exec parses and executes one or more SQL statements through the
// full pipeline: parse → analyze → plan → optimize → execute.
func (ex *Executor) Exec(sql string) (*Result, error) {
	stmts, err := parser.Parse(strings.NewReader(sql), nil)
	if err != nil {
		return nil, fmt.Errorf("sql: parse error: %w", err)
	}
	if len(stmts) == 0 {
		return &Result{Message: "OK"}, nil
	}

	stmt := stmts[0].Stmt

	// Handle SET ROLE to change the session user for RLS.
	if setVar, ok := stmt.(*parser.VariableSetStmt); ok {
		if strings.EqualFold(setVar.Name, "role") && len(setVar.Args) > 0 {
			role := extractSetValue(setVar.Args[0])
			if role != "" {
				ex.SetRole(role)
				return &Result{Message: fmt.Sprintf("SET ROLE %s", role)}, nil
			}
		}
	}

	// Check for EXPLAIN.
	isExplain := false
	isAnalyze := false
	if explain, ok := stmt.(*parser.ExplainStmt); ok {
		isExplain = true
		for _, opt := range explain.Options {
			if strings.EqualFold(opt.Defname, "analyze") {
				isAnalyze = true
			}
		}
		stmt = explain.Query
	}

	// For CREATE VIEW, extract the SELECT definition from the original SQL
	// so we can store it as the rewrite rule definition.
	var viewDefSQL string
	if _, ok := stmt.(*parser.ViewStmt); ok {
		viewDefSQL = extractViewDef(sql)
	}

	// Phase 1: Analyze — parse tree → Query tree (semantic analysis).
	query, err := ex.analyzer.Analyze(stmt)
	if err != nil {
		return nil, err
	}

	// Attach the original SELECT SQL to CREATE VIEW utility statements.
	if query.CommandType == planner.CmdUtility && query.Utility != nil &&
		query.Utility.Type == planner.UtilCreateView && viewDefSQL != "" {
		query.Utility.ViewDef = viewDefSQL
	}

	// Phase 2: Rewrite — apply rewrite rules (view expansion, DML rules).
	queries, err := ex.rewriter.Rewrite(query)
	if err != nil {
		return nil, err
	}
	if len(queries) == 0 {
		return &Result{Message: "OK"}, nil
	}

	// Execute each rewritten query. For ALSO rules there may be
	// multiple queries; return the result of the last one.
	var lastResult *Result
	for _, q := range queries {
		// Phase 3: Plan — Query tree → Logical plan.
		logical, err := planner.QueryToLogicalPlan(q)
		if err != nil {
			return nil, err
		}

		// Phase 4: Optimize — Logical plan → Physical plan.
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

		// Phase 5: Execute.
		r, err := ex.exec.Execute(physical)
		if err != nil {
			return nil, err
		}
		lastResult = convertResult(r)
	}

	return lastResult, nil
}

// ExplainPlan returns the physical plan text without executing.
func (ex *Executor) ExplainPlan(sql string) (string, error) {
	stmts, err := parser.Parse(strings.NewReader(sql), nil)
	if err != nil {
		return "", err
	}
	if len(stmts) == 0 {
		return "", nil
	}

	stmt := stmts[0].Stmt
	if explain, ok := stmt.(*parser.ExplainStmt); ok {
		stmt = explain.Query
	}

	query, err := ex.analyzer.Analyze(stmt)
	if err != nil {
		return "", err
	}
	queries, err := ex.rewriter.Rewrite(query)
	if err != nil {
		return "", err
	}
	if len(queries) == 0 {
		return "", nil
	}
	logical, err := planner.QueryToLogicalPlan(queries[0])
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

// extractSetValue extracts a string value from a SET statement argument.
func extractSetValue(expr parser.Expr) string {
	switch e := expr.(type) {
	case *parser.A_Const:
		if e.Val.Type == parser.ValStr {
			return e.Val.Str
		}
		return fmt.Sprintf("%v", e.Val.Ival)
	case *parser.ColumnRef:
		if len(e.Fields) > 0 {
			if s, ok := e.Fields[0].(*parser.String); ok {
				return s.Str
			}
		}
	}
	return ""
}

// extractViewDef extracts the SELECT portion from a CREATE VIEW statement.
// e.g., "CREATE VIEW v AS SELECT * FROM t" → "SELECT * FROM t"
func extractViewDef(sql string) string {
	upper := strings.ToUpper(sql)
	idx := strings.Index(upper, " AS ")
	if idx < 0 {
		return ""
	}
	return strings.TrimSpace(sql[idx+4:])
}
