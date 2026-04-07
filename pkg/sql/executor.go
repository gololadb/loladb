package sql

import (
	"fmt"
	"strings"

	"github.com/gololadb/gopgsql/parser"

	"github.com/gololadb/loladb/pkg/catalog"
	"github.com/gololadb/loladb/pkg/executor"
	"github.com/gololadb/loladb/pkg/pl/plpgsql"
	"github.com/gololadb/loladb/pkg/planner"
	"github.com/gololadb/loladb/pkg/rewriter"
	"github.com/gololadb/loladb/pkg/tuple"
)

// Result holds the result of a SQL execution.
type Result struct {
	Columns      []string
	Rows         [][]tuple.Datum
	RowsAffected int64
	Message      string
}

// TxState tracks the session-level transaction state.
type TxState int

const (
	TxNone   TxState = iota // auto-commit mode (no explicit transaction)
	TxActive                // inside BEGIN ... COMMIT/ROLLBACK
	TxFailed                // transaction aborted, only ROLLBACK accepted
)

// Savepoint records a named position in the mutation log.
type Savepoint struct {
	Name     string
	Position int // index into the mutation log
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

	// Transaction state
	txState    TxState
	savepoints []Savepoint
}

// NewExecutor creates a SQL executor backed by the given catalog.
func NewExecutor(cat *catalog.Catalog) *Executor {
	a := &planner.Analyzer{Cat: cat}
	ex := &Executor{
		Cat:       cat,
		analyzer:  a,
		rewriter:  rewriter.New(cat, a),
		optimizer: &planner.Optimizer{Cat: cat, Costs: planner.DefaultCosts()},
		exec:      executor.NewExecutor(cat),
	}

	// Wire subquery executor so ExprSubLink can execute sub-SELECTs.
	planner.SubqueryExecutor = func(subQuery *planner.Query, outerRow *planner.Row) ([]string, [][]tuple.Datum, error) {
		logical, err := planner.QueryToLogicalPlan(subQuery)
		if err != nil {
			return nil, nil, err
		}
		physical, err := ex.optimizer.Optimize(logical)
		if err != nil {
			return nil, nil, err
		}
		r, err := ex.exec.Execute(physical)
		if err != nil {
			return nil, nil, err
		}
		return r.Columns, r.Rows, nil
	}

	// Wire enum ordinal resolver for enum-aware comparisons.
	planner.EnumOrdinalFunc = func(val string) int {
		// Check all enum types for this value.
		// This is O(enums * values) but fine for typical catalog sizes.
		cat.Types.RLock()
		defer cat.Types.RUnlock()
		for _, ct := range cat.Types.All() {
			if ct.TypType != "e" {
				continue
			}
			for i, v := range ct.EnumVals {
				if v == val {
					return i + 1
				}
			}
		}
		return 0
	}

	// Wire PL/pgSQL interpreter for trigger execution.
	interp := plpgsql.New(func(sql string) (*plpgsql.SQLResult, error) {
		r, err := ex.Exec(sql)
		if err != nil {
			return nil, err
		}
		return &plpgsql.SQLResult{
			Columns: r.Columns,
			Rows:    r.Rows,
			Message: r.Message,
		}, nil
	})

	ex.exec.TriggerExec = func(body string, tc *executor.TriggerContext) (map[string]tuple.Datum, error) {
		td := &plpgsql.TriggerData{
			TgName:   tc.TgName,
			TgTable:  tc.TgTable,
			TgOp:     tc.TgOp,
			TgWhen:   tc.TgWhen,
			TgLevel:  tc.TgLevel,
			NewRow:   tc.NewRow,
			OldRow:   tc.OldRow,
			ColNames: tc.ColNames,
		}
		result, err := interp.ExecTrigger(body, td)
		if err != nil {
			return nil, err
		}
		return result.TriggerRow, nil
	}

	// Wire SQL executor for domain CHECK constraint evaluation.
	ex.exec.SQLExec = func(sql string) (*executor.Result, error) {
		r, err := ex.Exec(sql)
		if err != nil {
			return nil, err
		}
		return &executor.Result{
			Columns:      r.Columns,
			Rows:         r.Rows,
			RowsAffected: r.RowsAffected,
			Message:      r.Message,
		}, nil
	}

	return ex
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
		if ex.txState == TxActive {
			ex.txState = TxFailed
		}
		return nil, fmt.Errorf("sql: parse error: %w", err)
	}
	if len(stmts) == 0 {
		return &Result{Message: "OK"}, nil
	}

	stmt := stmts[0].Stmt

	// Handle transaction control statements.
	if txStmt, ok := stmt.(*parser.TransactionStmt); ok {
		return ex.execTransaction(txStmt)
	}

	// In failed transaction state, reject everything except ROLLBACK.
	if ex.txState == TxFailed {
		return nil, fmt.Errorf("current transaction is aborted, commands ignored until end of transaction block")
	}

	// Handle SET statements.
	if setVar, ok := stmt.(*parser.VariableSetStmt); ok {
		if strings.EqualFold(setVar.Name, "role") && len(setVar.Args) > 0 {
			role := extractSetValue(setVar.Args[0])
			if role != "" {
				ex.SetRole(role)
				return &Result{Message: fmt.Sprintf("SET ROLE %s", role)}, nil
			}
		}
		if strings.EqualFold(setVar.Name, "search_path") {
			var schemas []string
			for _, arg := range setVar.Args {
				v := extractSetValue(arg)
				if v != "" {
					schemas = append(schemas, v)
				}
			}
			if len(schemas) > 0 {
				if err := ex.Cat.SetSearchPath(schemas); err != nil {
					return nil, err
				}
				return &Result{Message: fmt.Sprintf("SET search_path = %s", strings.Join(schemas, ", "))}, nil
			}
		}
	}

	// Handle SHOW statements.
	if showVar, ok := stmt.(*parser.VariableShowStmt); ok {
		return ex.execShow(showVar)
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

	// For CREATE VIEW, deparse the SELECT definition from the AST
	// so we can store it as the rewrite rule definition.
	var viewDefSQL string
	if vs, ok := stmt.(*parser.ViewStmt); ok {
		viewDefSQL = parser.Deparse(vs.Query)
	}

	// Phase 1: Analyze — parse tree → Query tree (semantic analysis).
	query, err := ex.analyzer.Analyze(stmt)
	if err != nil {
		return nil, ex.txError(err)
	}

	// Attach the original SELECT SQL to CREATE VIEW utility statements.
	if query.CommandType == planner.CmdUtility && query.Utility != nil &&
		query.Utility.Type == planner.UtilCreateView && viewDefSQL != "" {
		query.Utility.ViewDef = viewDefSQL
	}

	// Phase 2: Rewrite — apply rewrite rules (view expansion, DML rules).
	queries, err := ex.rewriter.Rewrite(query)
	if err != nil {
		return nil, ex.txError(err)
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
			return nil, ex.txError(err)
		}

		// Phase 4: Optimize — Logical plan → Physical plan.
		physical, err := ex.optimizer.Optimize(logical)
		if err != nil {
			return nil, ex.txError(err)
		}

		if isExplain {
			r, err := ex.exec.ExecuteExplain(physical, isAnalyze)
			if err != nil {
				return nil, ex.txError(err)
			}
			return convertResult(r), nil
		}

		// Phase 5: Execute.
		r, err := ex.exec.Execute(physical)
		if err != nil {
			return nil, ex.txError(err)
		}
		lastResult = convertResult(r)
	}

	return lastResult, nil
}

// TxStatus returns the current transaction status indicator for pgwire.
// 'I' = idle, 'T' = in transaction, 'E' = failed transaction.
func (ex *Executor) TxStatus() byte {
	switch ex.txState {
	case TxActive:
		return 'T'
	case TxFailed:
		return 'E'
	default:
		return 'I'
	}
}

// txError marks the transaction as failed if we're inside one.
func (ex *Executor) txError(err error) error {
	if ex.txState == TxActive {
		ex.txState = TxFailed
	}
	return err
}

// execTransaction handles BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, ROLLBACK TO.
func (ex *Executor) execTransaction(ts *parser.TransactionStmt) (*Result, error) {
	switch ts.Kind {
	case parser.TRANS_STMT_BEGIN, parser.TRANS_STMT_START:
		if ex.txState == TxActive {
			return nil, fmt.Errorf("there is already a transaction in progress")
		}
		ex.txState = TxActive
		ex.exec.TrackMutations = true
		ex.exec.ClearMutations()
		ex.savepoints = nil
		return &Result{Message: "BEGIN"}, nil

	case parser.TRANS_STMT_COMMIT:
		if ex.txState == TxFailed {
			// PostgreSQL rolls back on COMMIT of a failed transaction.
			ex.exec.UndoMutationsFrom(0)
			ex.txState = TxNone
			ex.exec.TrackMutations = false
			ex.savepoints = nil
			return &Result{Message: "ROLLBACK"}, nil
		}
		// Commit: mutations are already applied, just clear state.
		ex.txState = TxNone
		ex.exec.TrackMutations = false
		ex.exec.ClearMutations()
		ex.savepoints = nil
		return &Result{Message: "COMMIT"}, nil

	case parser.TRANS_STMT_ROLLBACK:
		if ex.txState == TxNone {
			// No transaction in progress — PostgreSQL issues a WARNING but succeeds.
			return &Result{Message: "ROLLBACK"}, nil
		}
		// Undo all mutations.
		ex.exec.UndoMutationsFrom(0)
		ex.txState = TxNone
		ex.exec.TrackMutations = false
		ex.savepoints = nil
		return &Result{Message: "ROLLBACK"}, nil

	case parser.TRANS_STMT_SAVEPOINT:
		if ex.txState != TxActive {
			return nil, fmt.Errorf("SAVEPOINT can only be used in transaction blocks")
		}
		name := ""
		if len(ts.Options) > 0 {
			name = ts.Options[0]
		}
		ex.savepoints = append(ex.savepoints, Savepoint{
			Name:     name,
			Position: ex.exec.MutationLogLen(),
		})
		return &Result{Message: "SAVEPOINT"}, nil

	case parser.TRANS_STMT_ROLLBACK_TO:
		if ex.txState != TxActive && ex.txState != TxFailed {
			return nil, fmt.Errorf("ROLLBACK TO SAVEPOINT can only be used in transaction blocks")
		}
		name := ""
		if len(ts.Options) > 0 {
			name = ts.Options[0]
		}
		// Find the savepoint (search from most recent).
		found := false
		for i := len(ex.savepoints) - 1; i >= 0; i-- {
			if strings.EqualFold(ex.savepoints[i].Name, name) {
				// Undo mutations back to the savepoint position.
				ex.exec.UndoMutationsFrom(ex.savepoints[i].Position)
				// Remove savepoints created after this one (but keep this one).
				ex.savepoints = ex.savepoints[:i+1]
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("savepoint %q does not exist", name)
		}
		// ROLLBACK TO restores the transaction to active state even if it was failed.
		ex.txState = TxActive
		return &Result{Message: "ROLLBACK"}, nil

	case parser.TRANS_STMT_RELEASE:
		if ex.txState != TxActive {
			return nil, fmt.Errorf("RELEASE SAVEPOINT can only be used in transaction blocks")
		}
		name := ""
		if len(ts.Options) > 0 {
			name = ts.Options[0]
		}
		// Find and remove the savepoint.
		found := false
		for i := len(ex.savepoints) - 1; i >= 0; i-- {
			if strings.EqualFold(ex.savepoints[i].Name, name) {
				// Remove this savepoint and all after it.
				ex.savepoints = ex.savepoints[:i]
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("savepoint %q does not exist", name)
		}
		return &Result{Message: "RELEASE"}, nil

	default:
		return &Result{Message: "OK"}, nil
	}
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

func (ex *Executor) execShow(n *parser.VariableShowStmt) (*Result, error) {
	name := strings.ToLower(n.Name)
	switch name {
	case "search_path":
		val := strings.Join(ex.Cat.SearchPath, ", ")
		return &Result{
			Columns: []string{"search_path"},
			Rows:    [][]tuple.Datum{{tuple.DText(val)}},
			Message: "SHOW",
		}, nil
	default:
		return &Result{Message: fmt.Sprintf("SHOW %s", n.Name)}, nil
	}
}


