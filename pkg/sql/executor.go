package sql

import (
	"fmt"
	"sort"
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

	// Session-level GUC settings (set via SET or set_config()).
	sessionSettings map[string]string
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
		Cat:             cat,
		analyzer:        a,
		sessionSettings: map[string]string{},
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

	// Wire user-defined function executor for PL/pgSQL functions in SQL expressions.
	planner.UserFuncExecutor = func(name string, args []planner.AnalyzedExpr, row *planner.Row) (tuple.Datum, error) {
		fn := ex.Cat.FindFunction(name)
		if fn == nil {
			return tuple.DNull(), fmt.Errorf("function %s is not supported", name)
		}
		// Evaluate arguments.
		params := make(map[string]tuple.Datum)
		for i, arg := range args {
			val, err := planner.EvalAnalyzedExpr(arg, row)
			if err != nil {
				return tuple.DNull(), err
			}
			// Use $1, $2, ... as parameter names, and also positional names from function def.
			params[fmt.Sprintf("$%d", i+1)] = val
			if i < len(fn.ParamNames) && fn.ParamNames[i] != "" {
				params[fn.ParamNames[i]] = val
			}
		}
		interp := plpgsql.New(func(sql string) (*plpgsql.SQLResult, error) {
			r, err := ex.Exec(sql)
			if err != nil {
				return nil, err
			}
			return &plpgsql.SQLResult{Columns: r.Columns, Rows: r.Rows, Message: r.Message}, nil
		})
		result, err := interp.ExecFunction(fn.Body, params)
		if err != nil {
			return tuple.DNull(), err
		}
		if result.IsNull {
			return tuple.DNull(), nil
		}
		return result.Value, nil
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
		// Handle built-in trigger functions natively.
		if tc.TgFuncName == "tsvector_update_trigger" || tc.TgFuncName == "tsvector_update_trigger_column" {
			return execTsvectorUpdateTrigger(tc)
		}

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

	// Wire session settings callbacks for set_config() / current_setting().
	planner.SetConfigFunc = func(name, value string) {
		ex.SetSessionSetting(name, value)
	}
	planner.CurrentSettingFunc = func(name string) string {
		return ex.GetSessionSetting(name)
	}

	return ex
}

// SetSessionSetting stores a session-level GUC parameter.
func (ex *Executor) SetSessionSetting(name, value string) {
	name = strings.ToLower(name)
	ex.sessionSettings[name] = value
	// Keep special settings in sync.
	if name == "search_path" {
		schemas := strings.Split(value, ",")
		for i := range schemas {
			schemas[i] = strings.TrimSpace(schemas[i])
		}
		_ = ex.Cat.SetSearchPath(schemas)
	}
	if name == "role" {
		ex.SetRole(value)
	}
}

// GetSessionSetting retrieves a session-level GUC parameter.
func (ex *Executor) GetSessionSetting(name string) string {
	name = strings.ToLower(name)
	// Check session overrides first.
	if v, ok := ex.sessionSettings[name]; ok {
		return v
	}
	// Fall back to well-known defaults.
	switch name {
	case "search_path":
		return strings.Join(ex.Cat.SearchPath, ", ")
	case "server_version":
		return "15.0 (LolaDB)"
	case "server_encoding", "client_encoding":
		return "UTF8"
	case "standard_conforming_strings", "is_superuser":
		return "on"
	case "transaction_isolation", "default_transaction_isolation":
		return "read committed"
	case "datestyle":
		return "ISO, MDY"
	case "timezone":
		return "UTC"
	case "lc_messages", "lc_monetary", "lc_numeric", "lc_time":
		return "en_US.UTF-8"
	case "max_identifier_length":
		return "63"
	case "integer_datetimes":
		return "on"
	default:
		return ""
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
	// Pre-parse: handle statements the parser doesn't support.
	if r, handled := ex.tryPreParse(sql); handled {
		return r, nil
	}

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

	// Handle ALTER ... OWNER TO for non-table objects.
	if ao, ok := stmt.(*parser.AlterOwnerStmt); ok {
		return ex.execAlterOwnerStmt(ao)
	}

	// Handle CREATE EXTENSION (accept, no-op).
	if ce, ok := stmt.(*parser.CreateExtensionStmt); ok {
		return &Result{Message: fmt.Sprintf("CREATE EXTENSION %s", ce.Extname)}, nil
	}

	// Handle CREATE TABLESPACE (accept, no-op).
	if ct, ok := stmt.(*parser.CreateTableSpaceStmt); ok {
		return &Result{Message: fmt.Sprintf("CREATE TABLESPACE %s", ct.Tablespacename)}, nil
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

	// Handle CREATE AGGREGATE (DefineStmt).
	if ds, ok := stmt.(*parser.DefineStmt); ok {
		if ds.Kind == parser.OBJECT_AGGREGATE {
			return ex.execCreateAggregate(ds)
		}
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
		name := strings.ToLower(setVar.Name)
		var vals []string
		for _, arg := range setVar.Args {
			v := extractSetValue(arg)
			if v != "" {
				vals = append(vals, v)
			}
		}
		value := strings.Join(vals, ", ")
		ex.SetSessionSetting(name, value)
		return &Result{Message: fmt.Sprintf("SET %s = %s", setVar.Name, value)}, nil
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
	val := ex.GetSessionSetting(name)
	return &Result{
		Columns: []string{name},
		Rows:    [][]tuple.Datum{{tuple.DText(val)}},
		Message: "SHOW",
	}, nil
}

func (ex *Executor) execCreateMatView(cmv *parser.CreateMatViewStmt) (*Result, error) {
	name := cmv.Relation.Relname
	if cmv.IfNotExists {
		if rel, _ := ex.Cat.FindRelation(name); rel != nil {
			return &Result{Message: "SELECT 0"}, nil
		}
	}

	querySQL := parser.Deparse(cmv.Query)

	// Analyze the query through the full pipeline to get proper column types.
	selStmt, ok := cmv.Query.(*parser.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("CREATE MATERIALIZED VIEW: query must be a SELECT")
	}
	query, err := ex.analyzer.Analyze(selStmt)
	if err != nil {
		return nil, fmt.Errorf("CREATE MATERIALIZED VIEW: analyze: %w", err)
	}

	// Build the logical plan.
	logPlan, err := planner.QueryToLogicalPlan(query)
	if err != nil {
		return nil, fmt.Errorf("CREATE MATERIALIZED VIEW: plan: %w", err)
	}

	// Optimize to physical plan.
	physPlan, err := ex.optimizer.Optimize(logPlan)
	if err != nil {
		return nil, fmt.Errorf("CREATE MATERIALIZED VIEW: optimize: %w", err)
	}

	// Execute the physical plan to get the result set.
	r, err := ex.exec.Execute(physPlan)
	if err != nil {
		return nil, fmt.Errorf("CREATE MATERIALIZED VIEW: execute: %w", err)
	}

	// Build column definitions from the result set with proper types.
	cols := make([]catalog.ColumnDef, len(r.Columns))
	for i, colName := range r.Columns {
		// Strip table-qualified prefix (e.g. "src.id" → "id").
		if dot := strings.LastIndex(colName, "."); dot >= 0 {
			colName = colName[dot+1:]
		}
		dt := tuple.TypeText
		if len(r.Rows) > 0 && i < len(r.Rows[0]) {
			dt = r.Rows[0][i].Type
			if dt == tuple.TypeNull {
				dt = tuple.TypeText
			}
		}
		cols[i] = catalog.ColumnDef{Name: colName, Type: dt, Typmod: -1}
	}

	// Create the materialized view in the catalog (relkind='m', with storage).
	if _, err := ex.Cat.CreateMatView(name, cols, querySQL); err != nil {
		return nil, fmt.Errorf("CREATE MATERIALIZED VIEW: %w", err)
	}

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

// execCreateAggregate handles CREATE AGGREGATE statements.
func (ex *Executor) execCreateAggregate(ds *parser.DefineStmt) (*Result, error) {
	name := ""
	if len(ds.Defnames) > 0 {
		name = ds.Defnames[len(ds.Defnames)-1]
	}
	if name == "" {
		return nil, fmt.Errorf("CREATE AGGREGATE: missing name")
	}

	aggDef := &catalog.CustomAggregateDef{Name: name}

	// Extract argument types.
	for _, arg := range ds.Args {
		if tn, ok := arg.(*parser.TypeName); ok && len(tn.Names) > 0 {
			aggDef.ArgTypes = append(aggDef.ArgTypes, tn.Names[len(tn.Names)-1])
		}
	}

	// Extract definition options (sfunc, stype, initcond, finalfunc).
	for _, d := range ds.Definition {
		switch strings.ToLower(d.Defname) {
		case "sfunc":
			aggDef.SFunc = defElemToName(d.Arg)
		case "stype":
			aggDef.SType = defElemToName(d.Arg)
		case "initcond":
			if s, ok := d.Arg.(*parser.String); ok {
				aggDef.InitCond = s.Str
			}
		case "finalfunc":
			aggDef.FinalFunc = defElemToName(d.Arg)
		}
	}

	if aggDef.SFunc == "" {
		return nil, fmt.Errorf("CREATE AGGREGATE: sfunc is required")
	}
	if aggDef.SType == "" {
		return nil, fmt.Errorf("CREATE AGGREGATE: stype is required")
	}

	ex.Cat.CustomAggregates[strings.ToLower(name)] = aggDef
	return &Result{Message: "CREATE AGGREGATE"}, nil
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
		name := objName[len(objName)-1]

		// Resolve the target object OID based on TargetType.
		var targetOID int32
		switch gs.TargetType {
		case parser.OBJECT_SCHEMA:
			targetOID = ex.Cat.SchemaOID(name)
			if targetOID == 0 {
				return nil, fmt.Errorf("schema \"%s\" does not exist", name)
			}
		default:
			// Table / relation target (original behavior).
			rel, err := ex.Cat.FindRelation(name)
			if err != nil || rel == nil {
				return nil, fmt.Errorf("relation \"%s\" does not exist", name)
			}
			targetOID = rel.OID
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
						ex.Cat.GrantObjectPrivilegeColumns(targetOID, granteeOID, grantorOID, priv, cols)
					}
				} else {
					ex.Cat.GrantObjectPrivilege(targetOID, granteeOID, grantorOID, privs)
				}
			} else {
				if len(gs.PrivCols) > 0 {
					for i, p := range gs.Privileges {
						priv := catalog.ParsePrivilege(p)
						var cols []string
						if i < len(gs.PrivCols) && gs.PrivCols[i] != nil {
							cols = gs.PrivCols[i]
						}
						ex.Cat.RevokeObjectPrivilegeColumns(targetOID, granteeOID, grantorOID, priv, cols)
					}
				} else {
					ex.Cat.RevokeObjectPrivilege(targetOID, granteeOID, grantorOID, privs)
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

// defElemToName extracts a name string from a DefElem argument node.
// Handles *parser.String ("name") and *parser.TypeName (schema-qualified "schema.name").
func defElemToName(n parser.Node) string {
	switch v := n.(type) {
	case *parser.String:
		return v.Str
	case *parser.TypeName:
		if len(v.Names) > 0 {
			return v.Names[len(v.Names)-1]
		}
	}
	return ""
}

// execTsvectorUpdateTrigger implements the built-in tsvector_update_trigger
// function natively in Go. It expects trigger args: [tsvec_col, config, col1, col2, ...].
// It tokenizes the text from the source columns and builds a tsvector string
// that is stored in the target column of NEW.
func execTsvectorUpdateTrigger(tc *executor.TriggerContext) (map[string]tuple.Datum, error) {
	if len(tc.TgArgs) < 3 {
		return nil, fmt.Errorf("tsvector_update_trigger: requires at least 3 arguments (tsvec_col, config, source_col...)")
	}

	tsvecCol := tc.TgArgs[0]
	// tc.TgArgs[1] is the text search config (e.g. "english") — we accept but
	// don't use it for dictionary-specific stemming; we do simple tokenization.
	srcCols := tc.TgArgs[2:]

	if tc.NewRow == nil {
		return nil, fmt.Errorf("tsvector_update_trigger: NEW is null (only valid for INSERT/UPDATE)")
	}

	// Collect text from source columns.
	var parts []string
	for _, col := range srcCols {
		if d, ok := tc.NewRow[col]; ok && d.Type != tuple.TypeNull {
			parts = append(parts, d.Text)
		}
	}

	// Build a tsvector: tokenize words, deduplicate, sort, and format as
	// 'word1' 'word2' ... (simplified tsvector representation).
	tsvec := buildTsvector(strings.Join(parts, " "))

	// Set the tsvector column in NEW.
	result := make(map[string]tuple.Datum, len(tc.NewRow))
	for k, v := range tc.NewRow {
		result[k] = v
	}
	result[tsvecCol] = tuple.DText(tsvec)
	return result, nil
}

// buildTsvector tokenizes text into a simplified tsvector representation.
// Words are lowercased, deduplicated, sorted, and formatted as 'w1' 'w2' ...
func buildTsvector(text string) string {
	words := strings.Fields(strings.ToLower(text))
	seen := make(map[string]bool, len(words))
	var unique []string
	for _, w := range words {
		w = strings.Trim(w, ".,;:!?\"'()[]{}—–-")
		if w == "" || seen[w] {
			continue
		}
		seen[w] = true
		unique = append(unique, w)
	}
	sort.Strings(unique)

	var buf strings.Builder
	for i, w := range unique {
		if i > 0 {
			buf.WriteByte(' ')
		}
		buf.WriteByte('\'')
		buf.WriteString(w)
		buf.WriteByte('\'')
	}
	return buf.String()
}


