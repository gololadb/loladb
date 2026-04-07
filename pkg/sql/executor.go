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
	// CopyData holds formatted output for COPY TO STDOUT.
	CopyData string
	// CopyStmt is set when a COPY FROM STDIN is parsed but needs
	// the pgwire layer to supply data. The caller should check this
	// and initiate the COPY sub-protocol.
	CopyStmt interface{}
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

	// Server-side prepared statements (session-scoped).
	preparedStmts map[string]*PreparedStmt

	// Server-side cursors (session-scoped).
	cursors map[string]*CursorState
}

// CursorState holds the materialized result set and current position for a cursor.
type CursorState struct {
	Name   string
	Result *Result
	Pos    int // 0-based index of next row to fetch (starts before first row)
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

	// Handle RENAME statements (ALTER TABLE ... RENAME COLUMN/TO).
	if rs, ok := stmt.(*parser.RenameStmt); ok {
		return ex.execRenameStmt(rs)
	}

	// Handle ALTER TABLE column-level operations directly.
	if alt, ok := stmt.(*parser.AlterTableStmt); ok {
		if r, handled := ex.tryExecAlterTable(alt); handled {
			if strings.HasPrefix(r.Message, "catalog:") || strings.HasPrefix(r.Message, "type ") {
				return nil, fmt.Errorf("%s", r.Message)
			}
			return r, nil
		}
		// Fall through to analyzer for ADD COLUMN, DROP COLUMN, etc.
	}

	// Handle COPY statements (bypass analyzer — COPY is a utility).
	if cs, ok := stmt.(*parser.CopyStmt); ok {
		return ex.execCopy(cs)
	}

	// Handle VACUUM / ANALYZE statements.
	if vs, ok := stmt.(*parser.VacuumStmt); ok {
		return ex.execVacuumAnalyze(vs)
	}

	// Handle CREATE TABLE ... AS SELECT.
	if ctas, ok := stmt.(*parser.CreateTableAsStmt); ok {
		return ex.execCreateTableAs(ctas)
	}

	// Handle PREPARE / EXECUTE / DEALLOCATE statements.
	if ps, ok := stmt.(*parser.PrepareStmt); ok {
		return ex.execPrepare(ps)
	}
	if es, ok := stmt.(*parser.ExecuteStmt); ok {
		return ex.execExecute(es)
	}
	if ds, ok := stmt.(*parser.DeallocateStmt); ok {
		return ex.execDeallocate(ds)
	}

	// Handle DO blocks (anonymous PL/pgSQL).
	if ds, ok := stmt.(*parser.DoStmt); ok {
		return ex.execDo(ds)
	}

	// Handle LISTEN / NOTIFY / UNLISTEN (accept syntax, no-op).
	if ls, ok := stmt.(*parser.ListenStmt); ok {
		_ = ls
		return &Result{Message: "LISTEN"}, nil
	}
	if ns, ok := stmt.(*parser.NotifyStmt); ok {
		_ = ns
		return &Result{Message: "NOTIFY"}, nil
	}
	if us, ok := stmt.(*parser.UnlistenStmt); ok {
		_ = us
		return &Result{Message: "UNLISTEN"}, nil
	}

	// Handle REINDEX statements.
	if rs, ok := stmt.(*parser.ReindexStmt); ok {
		return ex.execReindex(rs)
	}

	// Handle GRANT / REVOKE statements.
	if gs, ok := stmt.(*parser.GrantStmt); ok {
		return ex.execGrant(gs)
	}

	// Handle cursor statements.
	if dc, ok := stmt.(*parser.DeclareCursorStmt); ok {
		return ex.execDeclareCursor(dc)
	}
	if fs, ok := stmt.(*parser.FetchStmt); ok {
		return ex.execFetch(fs)
	}
	if cp, ok := stmt.(*parser.ClosePortalStmt); ok {
		return ex.execCloseCursor(cp)
	}

	// Handle CREATE MATERIALIZED VIEW.
	if cmv, ok := stmt.(*parser.CreateMatViewStmt); ok {
		return ex.execCreateMatView(cmv)
	}

	// Handle REFRESH MATERIALIZED VIEW.
	if rmv, ok := stmt.(*parser.RefreshMatViewStmt); ok {
		return ex.execRefreshMatView(rmv)
	}

	// Handle COMMENT ON statements.
	if cs, ok := stmt.(*parser.CommentStmt); ok {
		return ex.execComment(cs)
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
	case "transaction_isolation", "default_transaction_isolation":
		// LolaDB uses snapshot isolation which maps to "read committed" in PG terms.
		return &Result{
			Columns: []string{name},
			Rows:    [][]tuple.Datum{{tuple.DText("read committed")}},
			Message: "SHOW",
		}, nil
	case "server_version":
		return &Result{
			Columns: []string{"server_version"},
			Rows:    [][]tuple.Datum{{tuple.DText("15.0 (LolaDB)")}},
			Message: "SHOW",
		}, nil
	case "server_encoding":
		return &Result{
			Columns: []string{"server_encoding"},
			Rows:    [][]tuple.Datum{{tuple.DText("UTF8")}},
			Message: "SHOW",
		}, nil
	case "client_encoding":
		return &Result{
			Columns: []string{"client_encoding"},
			Rows:    [][]tuple.Datum{{tuple.DText("UTF8")}},
			Message: "SHOW",
		}, nil
	case "standard_conforming_strings":
		return &Result{
			Columns: []string{"standard_conforming_strings"},
			Rows:    [][]tuple.Datum{{tuple.DText("on")}},
			Message: "SHOW",
		}, nil
	case "is_superuser":
		return &Result{
			Columns: []string{"is_superuser"},
			Rows:    [][]tuple.Datum{{tuple.DText("on")}},
			Message: "SHOW",
		}, nil
	default:
		return &Result{Message: fmt.Sprintf("SHOW %s", n.Name)}, nil
	}
}

func (ex *Executor) execCreateMatView(cmv *parser.CreateMatViewStmt) (*Result, error) {
	name := cmv.Relation.Relname
	if cmv.IfNotExists {
		if rel, _ := ex.Cat.FindRelation(name); rel != nil {
			return &Result{Message: "SELECT 0"}, nil
		}
	}

	querySQL := parser.Deparse(cmv.Query)

	// Execute the query to get column definitions and data.
	r, err := ex.Exec(querySQL)
	if err != nil {
		return nil, fmt.Errorf("CREATE MATERIALIZED VIEW: %w", err)
	}

	// Build column definitions from the result set.
	cols := make([]catalog.ColumnDef, len(r.Columns))
	for i, colName := range r.Columns {
		dt := tuple.TypeText
		if len(r.Rows) > 0 && i < len(r.Rows[0]) {
			dt = r.Rows[0][i].Type
			if dt == tuple.TypeNull {
				dt = tuple.TypeText
			}
		}
		cols[i] = catalog.ColumnDef{Name: colName, Type: dt, Typmod: -1}
	}

	// Create the backing table.
	if _, err := ex.Cat.CreateTable(name, cols); err != nil {
		return nil, fmt.Errorf("CREATE MATERIALIZED VIEW: %w", err)
	}

	// Store the query definition.
	ex.Cat.MatViews[name] = querySQL

	// Insert data if WITH DATA (default).
	count := int64(0)
	if cmv.WithData {
		for _, row := range r.Rows {
			if _, err := ex.Cat.InsertInto(name, row); err != nil {
				return nil, fmt.Errorf("CREATE MATERIALIZED VIEW: insert: %w", err)
			}
			count++
		}
	}

	return &Result{Message: fmt.Sprintf("SELECT %d", count)}, nil
}

func (ex *Executor) execRefreshMatView(rmv *parser.RefreshMatViewStmt) (*Result, error) {
	name := rmv.Relation.Relname
	querySQL, ok := ex.Cat.MatViews[name]
	if !ok {
		return nil, fmt.Errorf("materialized view %q does not exist", name)
	}

	// Truncate existing data.
	if _, err := ex.Exec("TRUNCATE " + name); err != nil {
		return nil, fmt.Errorf("REFRESH MATERIALIZED VIEW: truncate: %w", err)
	}

	if rmv.SkipData {
		return &Result{Message: "REFRESH MATERIALIZED VIEW"}, nil
	}

	// Re-execute the query and insert results.
	r, err := ex.Exec(querySQL)
	if err != nil {
		return nil, fmt.Errorf("REFRESH MATERIALIZED VIEW: %w", err)
	}

	for _, row := range r.Rows {
		if _, err := ex.Cat.InsertInto(name, row); err != nil {
			return nil, fmt.Errorf("REFRESH MATERIALIZED VIEW: insert: %w", err)
		}
	}

	return &Result{Message: "REFRESH MATERIALIZED VIEW"}, nil
}

func (ex *Executor) execComment(cs *parser.CommentStmt) (*Result, error) {
	objName := ""
	if len(cs.Object) > 0 {
		objName = cs.Object[len(cs.Object)-1]
	}

	// Build a key based on object type and name.
	var key string
	switch cs.ObjType {
	case parser.OBJECT_TABLE:
		key = "table:" + objName
	case parser.OBJECT_COLUMN:
		// Object is [table, column] or [schema, table, column].
		if len(cs.Object) >= 2 {
			key = "column:" + cs.Object[len(cs.Object)-2] + "." + cs.Object[len(cs.Object)-1]
		} else {
			key = "column:" + objName
		}
	case parser.OBJECT_INDEX:
		key = "index:" + objName
	case parser.OBJECT_SCHEMA:
		key = "schema:" + objName
	case parser.OBJECT_VIEW:
		key = "view:" + objName
	case parser.OBJECT_FUNCTION:
		key = "function:" + objName
	case parser.OBJECT_SEQUENCE:
		key = "sequence:" + objName
	default:
		key = fmt.Sprintf("object:%d:%s", cs.ObjType, objName)
	}

	comment := cs.Comment
	if cs.IsNull {
		comment = ""
	}
	ex.Cat.SetComment(key, comment)
	return &Result{Message: "COMMENT"}, nil
}

// execDo executes an anonymous PL/pgSQL block (DO $$ ... $$).
func (ex *Executor) execDo(ds *parser.DoStmt) (*Result, error) {
	body := ""
	for _, arg := range ds.Args {
		if arg.Defname == "as" {
			if s, ok := arg.Arg.(*parser.String); ok {
				body = s.Str
			}
		}
	}
	if body == "" {
		return &Result{Message: "DO"}, nil
	}

	interp := plpgsql.New(func(sql string) (*plpgsql.SQLResult, error) {
		r, err := ex.Exec(sql)
		if err != nil {
			return nil, err
		}
		return &plpgsql.SQLResult{
			Columns: r.Columns,
			Rows:    r.Rows,
		}, nil
	})

	_, err := interp.ExecFunction(body, nil)
	if err != nil {
		return nil, fmt.Errorf("DO block: %w", err)
	}
	return &Result{Message: "DO"}, nil
}

// execReindex handles REINDEX TABLE/INDEX statements.
func (ex *Executor) execReindex(rs *parser.ReindexStmt) (*Result, error) {
	if rs.Relation != nil {
		name := rs.Relation.Relname
		rel, err := ex.Cat.FindRelation(name)
		if err != nil || rel == nil {
			return nil, fmt.Errorf("relation \"%s\" does not exist", name)
		}
	}
	// In-memory indexes don't need physical rebuilding; accept the command.
	return &Result{Message: "REINDEX"}, nil
}

// execGrant handles GRANT and REVOKE statements for table-level privileges.
func (ex *Executor) execGrant(gs *parser.GrantStmt) (*Result, error) {
	// Combine all privileges.
	var privs catalog.Privilege
	for _, p := range gs.Privileges {
		privs |= catalog.ParsePrivilege(p)
	}
	if privs == 0 {
		return nil, fmt.Errorf("unrecognized privilege type")
	}

	for _, objName := range gs.Objects {
		tableName := objName[len(objName)-1]
		rel, err := ex.Cat.FindRelation(tableName)
		if err != nil || rel == nil {
			return nil, fmt.Errorf("relation \"%s\" does not exist", tableName)
		}

		for _, grantee := range gs.Grantees {
			// Resolve grantee role OID; auto-create if not found (like PG's PUBLIC).
			role, err := ex.Cat.FindRole(grantee)
			if err != nil {
				return nil, err
			}
			granteeOID := int32(0)
			if role != nil {
				granteeOID = role.OID
			}

			// Grantor is the current user (0 if not set).
			grantorOID := int32(0)
			if ex.CurrentUser != "" {
				grantor, _ := ex.Cat.FindRole(ex.CurrentUser)
				if grantor != nil {
					grantorOID = grantor.OID
				}
			}

			if gs.IsGrant {
				if len(gs.PrivCols) > 0 {
					// Column-level grant.
					for i, p := range gs.Privileges {
						priv := catalog.ParsePrivilege(p)
						var cols []string
						if i < len(gs.PrivCols) && gs.PrivCols[i] != nil {
							cols = gs.PrivCols[i]
						}
						ex.Cat.GrantObjectPrivilegeColumns(rel.OID, granteeOID, grantorOID, priv, cols)
					}
				} else {
					ex.Cat.GrantObjectPrivilege(rel.OID, granteeOID, grantorOID, privs)
				}
			} else {
				if len(gs.PrivCols) > 0 {
					for i, p := range gs.Privileges {
						priv := catalog.ParsePrivilege(p)
						var cols []string
						if i < len(gs.PrivCols) && gs.PrivCols[i] != nil {
							cols = gs.PrivCols[i]
						}
						ex.Cat.RevokeObjectPrivilegeColumns(rel.OID, granteeOID, grantorOID, priv, cols)
					}
				} else {
					ex.Cat.RevokeObjectPrivilege(rel.OID, granteeOID, grantorOID, privs)
				}
			}
		}
	}

	if gs.IsGrant {
		return &Result{Message: "GRANT"}, nil
	}
	return &Result{Message: "REVOKE"}, nil
}

// execDeclareCursor materializes the query result and stores it as a named cursor.
func (ex *Executor) execDeclareCursor(dc *parser.DeclareCursorStmt) (*Result, error) {
	name := strings.ToLower(dc.Portalname)
	if ex.cursors == nil {
		ex.cursors = map[string]*CursorState{}
	}
	if _, exists := ex.cursors[name]; exists {
		return nil, fmt.Errorf("cursor \"%s\" already exists", name)
	}

	// Execute the query to materialize the result set.
	querySQL := parser.Deparse(dc.Query)
	result, err := ex.Exec(querySQL)
	if err != nil {
		return nil, fmt.Errorf("DECLARE CURSOR: %w", err)
	}

	ex.cursors[name] = &CursorState{
		Name:   name,
		Result: result,
		Pos:    0,
	}
	return &Result{Message: "DECLARE CURSOR"}, nil
}

// execFetch returns the next N rows from a cursor.
func (ex *Executor) execFetch(fs *parser.FetchStmt) (*Result, error) {
	name := strings.ToLower(fs.Portalname)
	cur, ok := ex.cursors[name]
	if !ok {
		return nil, fmt.Errorf("cursor \"%s\" does not exist", name)
	}

	if fs.IsMove {
		// MOVE just advances the position without returning rows.
		count := int(fs.HowMany)
		if count <= 0 {
			count = 1
		}
		cur.Pos += count
		if cur.Pos > len(cur.Result.Rows) {
			cur.Pos = len(cur.Result.Rows)
		}
		return &Result{Message: fmt.Sprintf("MOVE %d", count)}, nil
	}

	count := int(fs.HowMany)
	if count <= 0 {
		count = 1
	}

	result := &Result{Columns: cur.Result.Columns}
	for i := 0; i < count && cur.Pos < len(cur.Result.Rows); i++ {
		result.Rows = append(result.Rows, cur.Result.Rows[cur.Pos])
		cur.Pos++
	}
	return result, nil
}

// execCloseCursor closes a named cursor or all cursors.
func (ex *Executor) execCloseCursor(cp *parser.ClosePortalStmt) (*Result, error) {
	if cp.Portalname == "" {
		// CLOSE ALL
		ex.cursors = nil
		return &Result{Message: "CLOSE ALL"}, nil
	}
	name := strings.ToLower(cp.Portalname)
	if _, ok := ex.cursors[name]; !ok {
		return nil, fmt.Errorf("cursor \"%s\" does not exist", name)
	}
	delete(ex.cursors, name)
	return &Result{Message: "CLOSE CURSOR"}, nil
}
