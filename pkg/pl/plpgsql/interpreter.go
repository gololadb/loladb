// Package plpgsql implements a PL/pgSQL interpreter that executes ASTs
// produced by the goplpgsql parser. Expressions and embedded SQL are
// evaluated by delegating to the host database's SQL executor.
package plpgsql

import (
	"fmt"
	"strconv"
	"strings"

	plparser "github.com/gololadb/goplpgsql/parser"
	"github.com/gololadb/loladb/pkg/tuple"
)

// SQLResult mirrors the executor result shape.
type SQLResult struct {
	Columns []string
	Rows    [][]tuple.Datum
	Message string
}

// SQLExecFunc executes a SQL string and returns the result.
type SQLExecFunc func(sql string) (*SQLResult, error)

// TriggerData holds the context passed to a trigger function.
type TriggerData struct {
	TgName    string // trigger name
	TgTable   string // table name
	TgOp      string // INSERT, UPDATE, DELETE
	TgWhen    string // BEFORE, AFTER
	TgLevel   string // ROW, STATEMENT
	NewRow    map[string]tuple.Datum
	OldRow    map[string]tuple.Datum
	ColNames  []string // column names in order
}

// FuncResult holds the return value of a PL/pgSQL function.
type FuncResult struct {
	Value   tuple.Datum
	IsNull  bool
	// For trigger functions, the modified NEW row (or nil to suppress).
	TriggerRow map[string]tuple.Datum
	// For set-returning functions (RETURN NEXT / RETURN QUERY).
	Rows    [][]tuple.Datum
	Columns []string
}

// Interpreter executes a PL/pgSQL function body.
type Interpreter struct {
	execSQL    SQLExecFunc
	returnRows [][]tuple.Datum // accumulated rows for RETURN NEXT / RETURN QUERY
	returnCols []string        // column names for RETURN QUERY results
}

// New creates an interpreter that delegates SQL to the given executor.
func New(execSQL SQLExecFunc) *Interpreter {
	return &Interpreter{execSQL: execSQL}
}

// scope holds variable bindings for a block.
type scope struct {
	vars   map[string]tuple.Datum
	parent *scope
}

func newScope(parent *scope) *scope {
	return &scope{vars: make(map[string]tuple.Datum), parent: parent}
}

func (s *scope) get(name string) (tuple.Datum, bool) {
	name = strings.ToLower(name)
	if v, ok := s.vars[name]; ok {
		return v, true
	}
	if s.parent != nil {
		return s.parent.get(name)
	}
	return tuple.Datum{}, false
}

func (s *scope) set(name string, val tuple.Datum) {
	name = strings.ToLower(name)
	// Update in the scope where it exists, or current scope.
	for cur := s; cur != nil; cur = cur.parent {
		if _, ok := cur.vars[name]; ok {
			cur.vars[name] = val
			return
		}
	}
	s.vars[name] = val
}

// errReturn is used to unwind the call stack on RETURN.
type errReturn struct {
	value  tuple.Datum
	isNull bool
}

func (e *errReturn) Error() string { return "RETURN" }

// errExit is used for EXIT/CONTINUE in loops.
type errExit struct {
	label      string
	isContinue bool
}

func (e *errExit) Error() string {
	if e.isContinue {
		return "CONTINUE"
	}
	return "EXIT"
}

// errRaise is used for RAISE EXCEPTION.
type errRaise struct {
	message string
}

func (e *errRaise) Error() string { return e.message }

// ExecFunction executes a PL/pgSQL function body with the given arguments.
func (interp *Interpreter) ExecFunction(body string, params map[string]tuple.Datum) (*FuncResult, error) {
	block, err := plparser.Parse([]byte(body), nil)
	if err != nil {
		return nil, fmt.Errorf("plpgsql: parse error: %w", err)
	}

	sc := newScope(nil)
	for k, v := range params {
		sc.set(k, v)
	}

	// Reset accumulated rows for set-returning functions.
	interp.returnRows = nil
	interp.returnCols = nil

	err = interp.execBlock(sc, block)
	if err != nil {
		if ret, ok := err.(*errReturn); ok {
			result := &FuncResult{Value: ret.value, IsNull: ret.isNull}
			if len(interp.returnRows) > 0 {
				result.Rows = interp.returnRows
				result.Columns = interp.returnCols
			}
			return result, nil
		}
		if raise, ok := err.(*errRaise); ok {
			return nil, fmt.Errorf("plpgsql: %s", raise.message)
		}
		return nil, err
	}

	// If RETURN NEXT/QUERY was used without a final RETURN, return accumulated rows.
	if len(interp.returnRows) > 0 {
		return &FuncResult{Rows: interp.returnRows, Columns: interp.returnCols, IsNull: true}, nil
	}
	return &FuncResult{IsNull: true}, nil
}

// ExecTrigger executes a trigger function body with trigger context.
func (interp *Interpreter) ExecTrigger(body string, td *TriggerData) (*FuncResult, error) {
	block, err := plparser.Parse([]byte(body), nil)
	if err != nil {
		return nil, fmt.Errorf("plpgsql: parse error: %w", err)
	}

	sc := newScope(nil)

	// Inject TG_* variables.
	sc.set("tg_name", tuple.DText(td.TgName))
	sc.set("tg_table_name", tuple.DText(td.TgTable))
	sc.set("tg_op", tuple.DText(td.TgOp))
	sc.set("tg_when", tuple.DText(td.TgWhen))
	sc.set("tg_level", tuple.DText(td.TgLevel))

	// Inject NEW/OLD as composite variables.
	// We flatten them: NEW.col becomes accessible via expression evaluation.
	// Store the row maps so we can read/write them.
	if td.NewRow != nil {
		for col, val := range td.NewRow {
			sc.set("new."+strings.ToLower(col), val)
		}
	}
	if td.OldRow != nil {
		for col, val := range td.OldRow {
			sc.set("old."+strings.ToLower(col), val)
		}
	}

	err = interp.execBlock(sc, block)
	if err != nil {
		if ret, ok := err.(*errReturn); ok {
			// For BEFORE ROW triggers, return the (possibly modified) NEW row.
			if td.TgWhen == "BEFORE" && td.TgOp != "DELETE" {
				newRow := make(map[string]tuple.Datum)
				for _, col := range td.ColNames {
					key := "new." + strings.ToLower(col)
					if v, ok := sc.get(key); ok {
						newRow[col] = v
					} else if v, ok := td.NewRow[col]; ok {
						newRow[col] = v
					}
				}
				return &FuncResult{Value: ret.value, TriggerRow: newRow}, nil
			}
			return &FuncResult{Value: ret.value, IsNull: ret.isNull}, nil
		}
		if raise, ok := err.(*errRaise); ok {
			return nil, fmt.Errorf("plpgsql: %s", raise.message)
		}
		return nil, err
	}

	// Implicit RETURN NULL for trigger functions that don't explicitly return.
	if td.TgWhen == "BEFORE" && td.TgOp != "DELETE" && td.NewRow != nil {
		newRow := make(map[string]tuple.Datum)
		for _, col := range td.ColNames {
			key := "new." + strings.ToLower(col)
			if v, ok := sc.get(key); ok {
				newRow[col] = v
			} else if v, ok := td.NewRow[col]; ok {
				newRow[col] = v
			}
		}
		return &FuncResult{TriggerRow: newRow}, nil
	}
	return &FuncResult{IsNull: true}, nil
}

func (interp *Interpreter) execBlock(sc *scope, block *plparser.StmtBlock) error {
	child := newScope(sc)

	// Process declarations.
	for _, decl := range block.Decls {
		if decl.Default != "" {
			val, err := interp.evalExpr(child, decl.Default)
			if err != nil {
				return fmt.Errorf("plpgsql: error evaluating default for %s: %w", decl.Name, err)
			}
			child.set(decl.Name, val)
		} else {
			child.set(decl.Name, tuple.DNull())
		}
	}

	err := interp.execStmts(child, block.Body)
	if err != nil {
		// Handle EXCEPTION blocks: catch any error (not just RAISE).
		if len(block.Exceptions) > 0 {
			// Don't catch EXIT/CONTINUE control flow.
			if _, isExit := err.(*errExit); isExit {
				return err
			}
			if _, isReturn := err.(*errReturn); isReturn {
				return err
			}

			errMsg := err.Error()
			sqlState := "P0001" // default: raise_exception
			if _, isRaise := err.(*errRaise); !isRaise {
				// SQL error — use a generic SQLSTATE.
				sqlState = "XX000"
			}

			for _, exc := range block.Exceptions {
				if matchesException(exc, errMsg, sqlState) {
					// Set SQLERRM and SQLSTATE in the handler scope.
					child.set("sqlerrm", tuple.DText(errMsg))
					child.set("sqlstate", tuple.DText(sqlState))
					return interp.execStmts(child, exc.Body)
				}
			}
		}
		return err
	}
	return nil
}

// matchesException checks if an exception handler matches the given error.
func matchesException(exc *plparser.Exception, errMsg, sqlState string) bool {
	// No conditions means WHEN OTHERS — catches everything.
	if len(exc.Conditions) == 0 {
		return true
	}
	for _, cond := range exc.Conditions {
		name := strings.ToLower(cond.Name)
		if name == "others" || name == "" {
			return true
		}
		// Match by SQLSTATE code.
		if cond.SQLState != "" && cond.SQLState == sqlState {
			return true
		}
		// Match by condition name (substring match for common patterns).
		if strings.Contains(strings.ToLower(errMsg), strings.ReplaceAll(name, "_", " ")) {
			return true
		}
		// Map well-known condition names to SQLSTATE classes.
		if mappedState, ok := conditionToSQLState[name]; ok && mappedState == sqlState {
			return true
		}
	}
	return false
}

// conditionToSQLState maps PL/pgSQL exception condition names to SQLSTATE codes.
var conditionToSQLState = map[string]string{
	"division_by_zero":       "22012",
	"unique_violation":       "23505",
	"not_null_violation":     "23502",
	"foreign_key_violation":  "23503",
	"check_violation":        "23514",
	"numeric_value_out_of_range": "22003",
	"string_data_right_truncation": "22001",
	"undefined_table":        "42P01",
	"undefined_column":       "42703",
	"syntax_error":           "42601",
	"raise_exception":        "P0001",
	"no_data_found":          "P0002",
	"too_many_rows":          "P0003",
}

func (interp *Interpreter) execStmts(sc *scope, stmts []plparser.Stmt) error {
	for _, stmt := range stmts {
		if err := interp.execStmt(sc, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (interp *Interpreter) execStmt(sc *scope, stmt plparser.Stmt) error {
	switch s := stmt.(type) {
	case *plparser.StmtAssign:
		return interp.execAssign(sc, s)
	case *plparser.StmtIf:
		return interp.execIf(sc, s)
	case *plparser.StmtCase:
		return interp.execCase(sc, s)
	case *plparser.StmtLoop:
		return interp.execLoop(sc, s)
	case *plparser.StmtWhile:
		return interp.execWhile(sc, s)
	case *plparser.StmtForI:
		return interp.execForI(sc, s)
	case *plparser.StmtForS:
		return interp.execForS(sc, s)
	case *plparser.StmtExit:
		return interp.execExit(sc, s)
	case *plparser.StmtReturn:
		return interp.execReturn(sc, s)
	case *plparser.StmtRaise:
		return interp.execRaise(sc, s)
	case *plparser.StmtExecSQL:
		return interp.execSQL_stmt(sc, s)
	case *plparser.StmtPerform:
		return interp.execPerform(sc, s)
	case *plparser.StmtBlock:
		return interp.execBlock(sc, s)
	case *plparser.StmtDynExecute:
		return interp.execDynExecute(sc, s)
	case *plparser.StmtForEachA:
		return interp.execForEachArray(sc, s)
	case *plparser.StmtReturnNext:
		return interp.execReturnNext(sc, s)
	case *plparser.StmtReturnQuery:
		return interp.execReturnQuery(sc, s)
	case *plparser.StmtNull:
		return nil
	default:
		return fmt.Errorf("plpgsql: unsupported statement type %T", stmt)
	}
}

func (interp *Interpreter) execAssign(sc *scope, s *plparser.StmtAssign) error {
	val, err := interp.evalExpr(sc, s.Expr)
	if err != nil {
		return fmt.Errorf("plpgsql: assignment error: %w", err)
	}
	sc.set(s.Variable, val)
	return nil
}

func (interp *Interpreter) execIf(sc *scope, s *plparser.StmtIf) error {
	cond, err := interp.evalBool(sc, s.Condition)
	if err != nil {
		return err
	}
	if cond {
		return interp.execStmts(sc, s.ThenBody)
	}
	for _, elsif := range s.ElsIfs {
		cond, err := interp.evalBool(sc, elsif.Condition)
		if err != nil {
			return err
		}
		if cond {
			return interp.execStmts(sc, elsif.Body)
		}
	}
	if len(s.ElseBody) > 0 {
		return interp.execStmts(sc, s.ElseBody)
	}
	return nil
}

func (interp *Interpreter) execCase(sc *scope, s *plparser.StmtCase) error {
	if s.Expr != "" {
		// Simple CASE: evaluate search expression once.
		searchVal, err := interp.evalExpr(sc, s.Expr)
		if err != nil {
			return err
		}
		for _, w := range s.Whens {
			whenVal, err := interp.evalExpr(sc, w.Expr)
			if err != nil {
				return err
			}
			if datumEqual(searchVal, whenVal) {
				return interp.execStmts(sc, w.Body)
			}
		}
	} else {
		// Searched CASE: each WHEN is a boolean expression.
		for _, w := range s.Whens {
			cond, err := interp.evalBool(sc, w.Expr)
			if err != nil {
				return err
			}
			if cond {
				return interp.execStmts(sc, w.Body)
			}
		}
	}
	if len(s.ElseBody) > 0 {
		return interp.execStmts(sc, s.ElseBody)
	}
	return fmt.Errorf("plpgsql: CASE not found")
}

func (interp *Interpreter) execLoop(sc *scope, s *plparser.StmtLoop) error {
	for {
		err := interp.execStmts(sc, s.Body)
		if err != nil {
			if ex, ok := err.(*errExit); ok {
				if !ex.isContinue && (ex.label == "" || ex.label == s.Label) {
					return nil
				}
				if ex.isContinue && (ex.label == "" || ex.label == s.Label) {
					continue
				}
				return err // propagate to outer loop
			}
			return err
		}
	}
}

func (interp *Interpreter) execWhile(sc *scope, s *plparser.StmtWhile) error {
	for {
		cond, err := interp.evalBool(sc, s.Condition)
		if err != nil {
			return err
		}
		if !cond {
			return nil
		}
		err = interp.execStmts(sc, s.Body)
		if err != nil {
			if ex, ok := err.(*errExit); ok {
				if !ex.isContinue && (ex.label == "" || ex.label == s.Label) {
					return nil
				}
				if ex.isContinue && (ex.label == "" || ex.label == s.Label) {
					continue
				}
				return err
			}
			return err
		}
	}
}

func (interp *Interpreter) execForI(sc *scope, s *plparser.StmtForI) error {
	lower, err := interp.evalInt(sc, s.Lower)
	if err != nil {
		return fmt.Errorf("plpgsql: FOR lower bound: %w", err)
	}
	upper, err := interp.evalInt(sc, s.Upper)
	if err != nil {
		return fmt.Errorf("plpgsql: FOR upper bound: %w", err)
	}
	step := int64(1)
	if s.Step != "" {
		step, err = interp.evalInt(sc, s.Step)
		if err != nil {
			return fmt.Errorf("plpgsql: FOR step: %w", err)
		}
	}
	if s.Reverse {
		step = -step
	}

	for i := lower; (!s.Reverse && i <= upper) || (s.Reverse && i >= upper); i += step {
		sc.set(s.Var, tuple.DInt64(i))
		err := interp.execStmts(sc, s.Body)
		if err != nil {
			if ex, ok := err.(*errExit); ok {
				if !ex.isContinue && (ex.label == "" || ex.label == s.Label) {
					return nil
				}
				if ex.isContinue && (ex.label == "" || ex.label == s.Label) {
					continue
				}
				return err
			}
			return err
		}
	}
	return nil
}

func (interp *Interpreter) execForS(sc *scope, s *plparser.StmtForS) error {
	sql := interp.substituteVars(sc, s.Query)
	result, err := interp.execSQL(sql)
	if err != nil {
		return fmt.Errorf("plpgsql: FOR query: %w", err)
	}
	for _, row := range result.Rows {
		// Bind the loop variable to the first column, or as a record.
		if len(result.Columns) == 1 {
			sc.set(s.Var, row[0])
		} else {
			// Bind each column as var.colname.
			for ci, col := range result.Columns {
				sc.set(s.Var+"."+col, row[ci])
			}
		}
		err := interp.execStmts(sc, s.Body)
		if err != nil {
			if ex, ok := err.(*errExit); ok {
				if !ex.isContinue && (ex.label == "" || ex.label == s.Label) {
					return nil
				}
				if ex.isContinue && (ex.label == "" || ex.label == s.Label) {
					continue
				}
				return err
			}
			return err
		}
	}
	return nil
}

func (interp *Interpreter) execExit(sc *scope, s *plparser.StmtExit) error {
	if s.Condition != "" {
		cond, err := interp.evalBool(sc, s.Condition)
		if err != nil {
			return err
		}
		if !cond {
			return nil
		}
	}
	return &errExit{label: s.Label, isContinue: !s.IsExit}
}

func (interp *Interpreter) execReturn(sc *scope, s *plparser.StmtReturn) error {
	if s.Expr == "" {
		return &errReturn{isNull: true}
	}
	upper := strings.ToUpper(strings.TrimSpace(s.Expr))
	// RETURN NEW / RETURN OLD / RETURN NULL are special in trigger context.
	if upper == "NEW" || upper == "OLD" || upper == "NULL" {
		if upper == "NULL" {
			return &errReturn{isNull: true}
		}
		// Return a marker value — the actual row is extracted from scope by ExecTrigger.
		return &errReturn{value: tuple.DText(upper)}
	}
	val, err := interp.evalExpr(sc, s.Expr)
	if err != nil {
		return fmt.Errorf("plpgsql: RETURN: %w", err)
	}
	return &errReturn{value: val}
}

func (interp *Interpreter) execRaise(sc *scope, s *plparser.StmtRaise) error {
	msg := s.Message
	// The parser re-quotes string literals, so strip surrounding quotes.
	if len(msg) >= 2 && msg[0] == '\'' && msg[len(msg)-1] == '\'' {
		msg = strings.ReplaceAll(msg[1:len(msg)-1], "''", "'")
	}
	// Substitute % placeholders with parameter values.
	for _, param := range s.Params {
		val, err := interp.evalExpr(sc, param)
		if err != nil {
			return err
		}
		msg = strings.Replace(msg, "%", datumToString(val), 1)
	}

	level := strings.ToUpper(s.Level)
	if level == "" {
		level = "EXCEPTION"
	}
	if level == "EXCEPTION" {
		return &errRaise{message: msg}
	}
	// For NOTICE, WARNING, etc. — just ignore (no client messaging yet).
	return nil
}

func (interp *Interpreter) execSQL_stmt(sc *scope, s *plparser.StmtExecSQL) error {
	sql := interp.substituteVars(sc, s.SQL)
	result, err := interp.execSQL(sql)
	if err != nil {
		return fmt.Errorf("plpgsql: SQL error: %w", err)
	}
	if s.Into && s.Target != "" && len(result.Rows) > 0 {
		targets := strings.Split(s.Target, ",")
		for i, t := range targets {
			t = strings.TrimSpace(t)
			if i < len(result.Rows[0]) {
				sc.set(t, result.Rows[0][i])
			}
		}
	}
	return nil
}

func (interp *Interpreter) execPerform(sc *scope, s *plparser.StmtPerform) error {
	sql := "SELECT " + interp.substituteVars(sc, s.Expr)
	_, err := interp.execSQL(sql)
	return err
}

func (interp *Interpreter) execDynExecute(sc *scope, s *plparser.StmtDynExecute) error {
	// Evaluate the query expression to get the SQL string.
	queryVal, err := interp.evalExpr(sc, s.Query)
	if err != nil {
		return fmt.Errorf("plpgsql: EXECUTE expression error: %w", err)
	}
	sql := datumToString(queryVal)

	// Substitute USING parameters: evaluate each expression and replace
	// $1, $2, ... placeholders with their literal SQL values.
	for i, param := range s.Params {
		val, err := interp.evalExpr(sc, param)
		if err != nil {
			return fmt.Errorf("plpgsql: EXECUTE USING param %d: %w", i+1, err)
		}
		placeholder := fmt.Sprintf("$%d", i+1)
		sql = strings.ReplaceAll(sql, placeholder, datumToLiteral(val))
	}

	result, err := interp.execSQL(sql)
	if err != nil {
		return fmt.Errorf("plpgsql: EXECUTE error: %w", err)
	}

	if s.Into && s.Target != "" && len(result.Rows) > 0 {
		targets := strings.Split(s.Target, ",")
		for i, t := range targets {
			t = strings.TrimSpace(t)
			if i < len(result.Rows[0]) {
				sc.set(t, result.Rows[0][i])
			}
		}
	}
	return nil
}

// execForEachArray implements FOREACH var IN ARRAY expr LOOP ... END LOOP.
func (interp *Interpreter) execForEachArray(sc *scope, s *plparser.StmtForEachA) error {
	// Evaluate the array expression.
	arrVal, err := interp.evalExpr(sc, s.Expr)
	if err != nil {
		return fmt.Errorf("plpgsql: FOREACH array expression: %w", err)
	}

	// Parse the array literal into elements.
	elements := parseArrayElements(datumToString(arrVal))

	for _, elem := range elements {
		sc.set(s.Var, elem)
		err := interp.execStmts(sc, s.Body)
		if err != nil {
			if ex, ok := err.(*errExit); ok {
				if !ex.isContinue && (ex.label == "" || ex.label == s.Label) {
					return nil
				}
				if ex.isContinue && (ex.label == "" || ex.label == s.Label) {
					continue
				}
				return err
			}
			return err
		}
	}
	return nil
}

// parseArrayElements splits a PG array literal like {1,2,3} or {a,"b c",d}
// into individual Datum values.
func parseArrayElements(arr string) []tuple.Datum {
	arr = strings.TrimSpace(arr)
	if arr == "" || arr == "{}" {
		return nil
	}
	// Strip outer braces.
	if arr[0] == '{' && arr[len(arr)-1] == '}' {
		arr = arr[1 : len(arr)-1]
	}

	var elements []tuple.Datum
	var current strings.Builder
	inQuote := false
	for i := 0; i < len(arr); i++ {
		ch := arr[i]
		switch {
		case ch == '"' && !inQuote:
			inQuote = true
		case ch == '"' && inQuote:
			inQuote = false
		case ch == ',' && !inQuote:
			elements = append(elements, parseSingleElement(current.String()))
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		elements = append(elements, parseSingleElement(current.String()))
	}
	return elements
}

func parseSingleElement(s string) tuple.Datum {
	s = strings.TrimSpace(s)
	if strings.EqualFold(s, "null") {
		return tuple.DNull()
	}
	// Try integer.
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return tuple.DInt64(v)
	}
	// Try float.
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return tuple.DFloat64(v)
	}
	return tuple.DText(s)
}

// execReturnNext implements RETURN NEXT expr — appends a row to the
// set-returning function's result set.
func (interp *Interpreter) execReturnNext(sc *scope, s *plparser.StmtReturnNext) error {
	val, err := interp.evalExpr(sc, s.Expr)
	if err != nil {
		return fmt.Errorf("plpgsql: RETURN NEXT: %w", err)
	}
	interp.returnRows = append(interp.returnRows, []tuple.Datum{val})
	return nil
}

// execReturnQuery implements RETURN QUERY query — executes a query and
// appends all result rows to the set-returning function's result set.
func (interp *Interpreter) execReturnQuery(sc *scope, s *plparser.StmtReturnQuery) error {
	sql := s.Query
	if s.DynQuery != "" {
		// RETURN QUERY EXECUTE
		queryVal, err := interp.evalExpr(sc, s.DynQuery)
		if err != nil {
			return fmt.Errorf("plpgsql: RETURN QUERY EXECUTE: %w", err)
		}
		sql = datumToString(queryVal)
	}
	sql = interp.substituteVars(sc, sql)
	result, err := interp.execSQL(sql)
	if err != nil {
		return fmt.Errorf("plpgsql: RETURN QUERY: %w", err)
	}
	if interp.returnCols == nil && len(result.Columns) > 0 {
		interp.returnCols = result.Columns
	}
	interp.returnRows = append(interp.returnRows, result.Rows...)
	return nil
}

// evalExpr evaluates a PL/pgSQL expression by wrapping it in SELECT and executing.
func (interp *Interpreter) evalExpr(sc *scope, expr string) (tuple.Datum, error) {
	expr = interp.substituteVars(sc, expr)
	sql := "SELECT " + expr
	result, err := interp.execSQL(sql)
	if err != nil {
		return tuple.Datum{}, fmt.Errorf("plpgsql: eval %q: %w", expr, err)
	}
	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		return tuple.DNull(), nil
	}
	return result.Rows[0][0], nil
}

func (interp *Interpreter) evalBool(sc *scope, expr string) (bool, error) {
	val, err := interp.evalExpr(sc, expr)
	if err != nil {
		return false, err
	}
	return datumToBool(val), nil
}

func (interp *Interpreter) evalInt(sc *scope, expr string) (int64, error) {
	val, err := interp.evalExpr(sc, expr)
	if err != nil {
		return 0, err
	}
	return datumToInt64(val), nil
}

// substituteVars replaces PL/pgSQL variable references in SQL text with
// their literal values. Handles simple identifiers and dotted references
// (e.g., NEW.column_name).
func (interp *Interpreter) substituteVars(sc *scope, sql string) string {
	// Collect all variable names from scope, longest first to avoid
	// partial matches (e.g., "new.name" before "new").
	type varEntry struct {
		name string
		val  tuple.Datum
	}
	var entries []varEntry
	for cur := sc; cur != nil; cur = cur.parent {
		for name, val := range cur.vars {
			entries = append(entries, varEntry{name, val})
		}
	}
	// Sort by length descending so longer names match first.
	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			if len(entries[j].name) > len(entries[i].name) {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}

	result := sql
	for _, e := range entries {
		result = replaceIdentifier(result, e.name, datumToLiteral(e.val))
	}
	return result
}

// replaceIdentifier replaces occurrences of an identifier in SQL text,
// only matching whole identifiers (not inside strings or other identifiers).
func replaceIdentifier(sql, name, replacement string) string {
	lower := strings.ToLower(sql)
	nameLower := strings.ToLower(name)
	var buf strings.Builder
	i := 0
	for i < len(sql) {
		// Skip string literals.
		if sql[i] == '\'' {
			j := i + 1
			for j < len(sql) {
				if sql[j] == '\'' {
					if j+1 < len(sql) && sql[j+1] == '\'' {
						j += 2
						continue
					}
					break
				}
				j++
			}
			buf.WriteString(sql[i : j+1])
			i = j + 1
			continue
		}

		// Check for identifier match.
		if i+len(nameLower) <= len(lower) && lower[i:i+len(nameLower)] == nameLower {
			// Check word boundaries.
			before := i == 0 || !isIdentChar(sql[i-1])
			after := i+len(nameLower) >= len(sql) || !isIdentChar(sql[i+len(nameLower)])
			if before && after {
				buf.WriteString(replacement)
				i += len(nameLower)
				continue
			}
		}
		buf.WriteByte(sql[i])
		i++
	}
	return buf.String()
}

func isIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') || c == '_' || c == '.'
}

// Datum helpers.

func datumToString(d tuple.Datum) string {
	switch d.Type {
	case tuple.TypeNull:
		return "NULL"
	case tuple.TypeInt32:
		return strconv.FormatInt(int64(d.I32), 10)
	case tuple.TypeInt64:
		return strconv.FormatInt(d.I64, 10)
	case tuple.TypeFloat64:
		return strconv.FormatFloat(d.F64, 'f', -1, 64)
	case tuple.TypeText:
		return d.Text
	case tuple.TypeBool:
		if d.Bool {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", d)
	}
}

func datumToLiteral(d tuple.Datum) string {
	switch d.Type {
	case tuple.TypeNull:
		return "NULL"
	case tuple.TypeText:
		return "'" + strings.ReplaceAll(d.Text, "'", "''") + "'"
	case tuple.TypeBool:
		if d.Bool {
			return "true"
		}
		return "false"
	default:
		return datumToString(d)
	}
}

func datumToBool(d tuple.Datum) bool {
	switch d.Type {
	case tuple.TypeBool:
		return d.Bool
	case tuple.TypeInt32:
		return d.I32 != 0
	case tuple.TypeInt64:
		return d.I64 != 0
	case tuple.TypeText:
		t := strings.ToLower(d.Text)
		return t == "t" || t == "true" || t == "1"
	default:
		return false
	}
}

func datumToInt64(d tuple.Datum) int64 {
	switch d.Type {
	case tuple.TypeInt32:
		return int64(d.I32)
	case tuple.TypeInt64:
		return d.I64
	case tuple.TypeFloat64:
		return int64(d.F64)
	case tuple.TypeText:
		n, _ := strconv.ParseInt(d.Text, 10, 64)
		return n
	default:
		return 0
	}
}

func datumEqual(a, b tuple.Datum) bool {
	return datumToString(a) == datumToString(b)
}
