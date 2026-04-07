package executor

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gololadb/gopgsql/parser"
	"github.com/gololadb/loladb/pkg/catalog"
	"github.com/gololadb/loladb/pkg/index"
	"github.com/gololadb/loladb/pkg/planner"
	"github.com/gololadb/loladb/pkg/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
)

// Result holds the output of plan execution.
type Result struct {
	Columns      []string
	Rows         [][]tuple.Datum
	RowsAffected int64
	Message      string
}

// TriggerExecFunc executes a trigger function body with the given trigger data.
// Returns the (possibly modified) NEW row for BEFORE ROW triggers, or nil.
type TriggerExecFunc func(body string, td *TriggerContext) (map[string]tuple.Datum, error)

// TriggerContext holds the context for a trigger invocation.
type TriggerContext struct {
	TgName     string
	TgTable    string
	TgOp       string // INSERT, UPDATE, DELETE
	TgWhen     string // BEFORE, AFTER
	TgLevel    string // ROW, STATEMENT
	TgFuncName string // trigger function name
	TgArgs     []string // trigger function arguments
	NewRow     map[string]tuple.Datum
	OldRow     map[string]tuple.Datum
	ColNames   []string
}

// SQLExecFunc executes a SQL statement and returns the result.
type SQLExecFunc func(sql string) (*Result, error)

// MutationKind identifies the type of DML mutation for undo tracking.
type MutationKind int

const (
	MutInsert MutationKind = iota // row was inserted
	MutDelete                     // row was deleted
	MutUpdate                     // row was updated
)

// Mutation records a single DML change for transaction rollback.
type Mutation struct {
	Kind      MutationKind
	Table     string
	ItemID    slottedpage.ItemID   // for insert: the inserted ID; for delete: the deleted ID
	NewItemID slottedpage.ItemID   // for update: the new row's ID after update
	OldValues []tuple.Datum        // previous row values (for update/delete undo)
}

// Executor runs physical plan trees against the catalog/engine.
type Executor struct {
	Cat         *catalog.Catalog
	CurrentUser string // session user for privilege checks
	TriggerExec TriggerExecFunc // optional PL/pgSQL trigger executor
	SQLExec     SQLExecFunc     // optional SQL executor for constraint evaluation

	// cteResults holds materialized CTE results for recursive CTE execution.
	// Keyed by CTE alias name.
	cteResults map[string]*cteResultEntry

	// TrackMutations enables mutation logging for transaction support.
	TrackMutations bool
	// Mutations is the ordered log of DML changes within a transaction.
	Mutations []Mutation
}

// RecordMutation appends a mutation to the log if tracking is enabled.
func (ex *Executor) RecordMutation(m Mutation) {
	if ex.TrackMutations {
		ex.Mutations = append(ex.Mutations, m)
	}
}

// MutationLogLen returns the current length of the mutation log.
func (ex *Executor) MutationLogLen() int {
	return len(ex.Mutations)
}

// ClearMutations resets the mutation log.
func (ex *Executor) ClearMutations() {
	ex.Mutations = ex.Mutations[:0]
}

// UndoMutationsFrom reverses mutations from index `from` to the end of the log,
// in reverse order. Used for ROLLBACK and ROLLBACK TO SAVEPOINT.
func (ex *Executor) UndoMutationsFrom(from int) {
	for i := len(ex.Mutations) - 1; i >= from; i-- {
		m := ex.Mutations[i]
		switch m.Kind {
		case MutInsert:
			// Undo insert → delete the row.
			ex.Cat.Delete(m.Table, m.ItemID)
		case MutDelete:
			// Undo delete → re-insert the old values.
			ex.Cat.InsertInto(m.Table, m.OldValues)
		case MutUpdate:
			// Undo update → delete the new row and re-insert old values.
			ex.Cat.Delete(m.Table, m.NewItemID)
			ex.Cat.InsertInto(m.Table, m.OldValues)
		}
	}
	ex.Mutations = ex.Mutations[:from]
}

type cteResultEntry struct {
	Columns []string
	Rows    [][]tuple.Datum
}

// NewExecutor creates a plan executor.
func NewExecutor(cat *catalog.Catalog) *Executor {
	return &Executor{Cat: cat}
}

func (ex *Executor) setCTEResult(name string, cols []string, rows [][]tuple.Datum) {
	if ex.cteResults == nil {
		ex.cteResults = make(map[string]*cteResultEntry)
	}
	ex.cteResults[strings.ToLower(name)] = &cteResultEntry{Columns: cols, Rows: rows}
}

func (ex *Executor) clearCTEResult(name string) {
	if ex.cteResults != nil {
		delete(ex.cteResults, strings.ToLower(name))
	}
}

func (ex *Executor) getCTEResult(name string) *cteResultEntry {
	if ex.cteResults == nil {
		return nil
	}
	return ex.cteResults[strings.ToLower(name)]
}

// checkTablePrivilege verifies the current user has the required privilege
// on the given table. Superusers and empty CurrentUser (no auth) bypass checks.
func (ex *Executor) checkTablePrivilege(tableName string, required catalog.Privilege) error {
	if ex.CurrentUser == "" {
		return nil // no auth configured
	}
	rel, err := ex.Cat.FindRelation(tableName)
	if err != nil || rel == nil {
		return nil // table not found errors handled elsewhere
	}

	ok, err := ex.Cat.CheckPrivilege(ex.CurrentUser, rel.OID, required)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("permission denied for table %s", tableName)
	}
	return nil
}

// resolveProjectNames qualifies unqualified column names using the child's
// column names (which are in table.column format).
func resolveProjectNames(projNames, childCols []string) []string {
	// Build a map from bare column name → table.column from child.
	colMap := make(map[string]string)
	for _, c := range childCols {
		parts := strings.SplitN(c, ".", 2)
		if len(parts) == 2 {
			colMap[parts[1]] = c
		}
	}

	resolved := make([]string, len(projNames))
	for i, name := range projNames {
		if strings.Contains(name, ".") {
			resolved[i] = name
		} else if qualified, ok := colMap[name]; ok {
			resolved[i] = qualified
		} else {
			resolved[i] = name
		}
	}
	return resolved
}

// checkProjectedColumns verifies column-level privileges for projected columns.
// Column names are in "table.column" format. Groups by table and checks each.
func (ex *Executor) checkProjectedColumns(colNames []string, required catalog.Privilege) error {
	// Group columns by table.
	tableCols := make(map[string][]string)
	for _, name := range colNames {
		parts := strings.SplitN(name, ".", 2)
		if len(parts) == 2 {
			tableCols[parts[0]] = append(tableCols[parts[0]], parts[1])
		}
	}

	for table, cols := range tableCols {
		if err := ex.checkColumnPrivilege(table, cols, required); err != nil {
			return err
		}
	}
	return nil
}

// hasAnyColumnGrant returns true if the current user has any column-level
// grant of the required privilege on the table.
func (ex *Executor) hasAnyColumnGrant(tableName string, required catalog.Privilege) bool {
	if ex.CurrentUser == "" {
		return false
	}
	rel, err := ex.Cat.FindRelation(tableName)
	if err != nil || rel == nil {
		return false
	}
	role, err := ex.Cat.FindRole(ex.CurrentUser)
	if err != nil || role == nil {
		return false
	}
	acls := ex.Cat.ACLs.GetACL(rel.OID)
	allRoles, _ := ex.Cat.GetAllRoleOIDs(role.OID)
	for _, item := range acls {
		if len(item.Columns) > 0 && item.Privileges&required != 0 {
			if allRoles[item.Grantee] || item.Grantee == 0 {
				return true
			}
		}
	}
	return false
}

// checkColumnPrivilege verifies the current user has the required privilege
// on specific columns. If the user has table-level privilege, column checks
// are skipped. Otherwise, each column is checked individually.
func (ex *Executor) checkColumnPrivilege(tableName string, columns []string, required catalog.Privilege) error {
	if ex.CurrentUser == "" {
		return nil
	}
	rel, err := ex.Cat.FindRelation(tableName)
	if err != nil || rel == nil {
		return nil
	}

	// First check table-level privilege.
	ok, err := ex.Cat.CheckPrivilege(ex.CurrentUser, rel.OID, required)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	// Check each column individually.
	for _, col := range columns {
		ok, err := ex.Cat.CheckColumnPrivilege(ex.CurrentUser, rel.OID, required, col)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("permission denied for column %s of table %s", col, tableName)
		}
	}
	return nil
}

// Execute runs a physical plan and returns the result.
func (ex *Executor) Execute(node planner.PhysicalNode) (*Result, error) {
	switch n := node.(type) {
	case *planner.PhysSeqScan:
		return ex.execSeqScan(n)
	case *planner.PhysIndexScan:
		return ex.execIndexScan(n)
	case *planner.PhysBitmapHeapScan:
		return ex.execBitmapHeapScan(n)
	case *planner.PhysFilter:
		return ex.execFilter(n)
	case *planner.PhysProject:
		return ex.execProject(n)
	case *planner.PhysNestedLoopJoin:
		return ex.execNestedLoopJoin(n)
	case *planner.PhysHashJoin:
		return ex.execHashJoin(n)
	case *planner.PhysLimit:
		return ex.execLimit(n)
	case *planner.PhysAggregate:
		return ex.execAggregate(n)
	case *planner.PhysSort:
		return ex.execSort(n)
	case *planner.PhysInsertSelect:
		return ex.execInsertSelect(n)
	case *planner.PhysInsert:
		return ex.execInsert(n)
	case *planner.PhysDelete:
		return ex.execDelete(n)
	case *planner.PhysUpdate:
		return ex.execUpdate(n)
	case *planner.PhysCreateTable:
		return ex.execCreateTable(n)
	case *planner.PhysCreateIndex:
		return ex.execCreateIndex(n)
	case *planner.PhysNoOp:
		return &Result{Message: n.Message}, nil
	case *planner.PhysCreateSequence:
		return &Result{Message: fmt.Sprintf("CREATE SEQUENCE %s", n.Name)}, nil
	case *planner.PhysCreateView:
		return ex.execCreateView(n)
	case *planner.PhysAlterTable:
		return &Result{Message: fmt.Sprintf("ALTER TABLE %s", n.Table)}, nil
	case *planner.PhysCreatePolicy:
		return ex.execCreatePolicy(n)
	case *planner.PhysEnableRLS:
		return ex.execEnableRLS(n)
	case *planner.PhysDisableRLS:
		return ex.execDisableRLS(n)
	case *planner.PhysCreateRole:
		return ex.execCreateRole(n)
	case *planner.PhysAlterRole:
		return ex.execAlterRole(n)
	case *planner.PhysDropRole:
		return ex.execDropRole(n)
	case *planner.PhysGrantRole:
		return ex.execGrantRole(n)
	case *planner.PhysRevokeRole:
		return ex.execRevokeRole(n)
	case *planner.PhysGrantPrivilege:
		return ex.execGrantPrivilege(n)
	case *planner.PhysRevokePrivilege:
		return ex.execRevokePrivilege(n)
	case *planner.PhysCreateFunction:
		return ex.execCreateFunction(n)
	case *planner.PhysCreateTrigger:
		return ex.execCreateTrigger(n)
	case *planner.PhysDropFunction:
		return ex.execDropFunction(n)
	case *planner.PhysDropTrigger:
		return ex.execDropTrigger(n)
	case *planner.PhysAlterFunction:
		return ex.execAlterFunction(n)
	case *planner.PhysCreateDomain:
		return ex.execCreateDomain(n)
	case *planner.PhysCreateEnum:
		return ex.execCreateEnum(n)
	case *planner.PhysDropType:
		return ex.execDropType(n)
	case *planner.PhysAlterEnum:
		return ex.execAlterEnum(n)
	case *planner.PhysCreateSchema:
		return ex.execCreateSchema(n)
	case *planner.PhysDropSchema:
		return ex.execDropSchema(n)
	case *planner.PhysTruncate:
		return ex.execTruncate(n)
	case *planner.PhysDropIndex:
		return ex.execDropIndex(n)
	case *planner.PhysDropTable:
		return ex.execDropTable(n)
	case *planner.PhysDropView:
		return ex.execDropView(n)
	case *planner.PhysAddColumn:
		return ex.execAddColumn(n)
	case *planner.PhysDropColumn:
		return ex.execDropColumn(n)
	case *planner.PhysSetOp:
		return ex.execSetOp(n)
	case *planner.PhysDistinct:
		return ex.execDistinct(n)
	case *planner.PhysResult:
		return ex.execResult(n)
	case *planner.PhysValues:
		return ex.execValues(n)
	case *planner.PhysWindowAgg:
		return ex.execWindowAgg(n)
	case *planner.PhysSubqueryScan:
		return ex.execSubqueryScan(n)
	default:
		return nil, fmt.Errorf("executor: unsupported node %T", node)
	}
}

// ExecuteExplain returns the EXPLAIN output for a plan.
func (ex *Executor) ExecuteExplain(node planner.PhysicalNode, analyze bool) (*Result, error) {
	planText := planner.Explain(node)

	if !analyze {
		return &Result{
			Columns: []string{"QUERY PLAN"},
			Rows:    textToRows(planText),
			Message: "EXPLAIN",
		}, nil
	}

	// EXPLAIN ANALYZE: execute and time it.
	start := time.Now()
	_, err := ex.Execute(node)
	elapsed := time.Since(start)
	if err != nil {
		return nil, err
	}

	planText += fmt.Sprintf("Execution time: %.3f ms\n", float64(elapsed.Microseconds())/1000.0)

	return &Result{
		Columns: []string{"QUERY PLAN"},
		Rows:    textToRows(planText),
		Message: "EXPLAIN ANALYZE",
	}, nil
}

func textToRows(text string) [][]tuple.Datum {
	lines := splitLines(text)
	rows := make([][]tuple.Datum, len(lines))
	for i, line := range lines {
		rows[i] = []tuple.Datum{tuple.DText(line)}
	}
	return rows
}

func splitLines(s string) []string {
	var lines []string
	for _, line := range splitByNewline(s) {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func splitByNewline(s string) []string {
	var result []string
	start := 0
	for i, c := range s {
		if c == '\n' {
			result = append(result, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		result = append(result, s[start:])
	}
	return result
}

// --- Scan executors ---

func (ex *Executor) execSeqScan(n *planner.PhysSeqScan) (*Result, error) {
	// Check if this scan targets a virtual catalog table.
	if vr := ex.virtualCatalogTable(n.Table, n.Alias); vr != nil {
		return vr, nil
	}

	// Check if this scan targets a CTE working table (recursive CTE self-reference).
	if cte := ex.getCTEResult(n.Table); cte != nil {
		// Remap column names to use the scan's alias.
		alias := n.Alias
		if alias == "" {
			alias = n.Table
		}
		cols := make([]string, len(n.Columns))
		for i, c := range n.Columns {
			cols[i] = alias + "." + c
		}

		// Apply pushed-down filter if present.
		var rows [][]tuple.Datum
		if n.Filter != nil {
			for _, r := range cte.Rows {
				row := &planner.Row{Columns: r, Names: cols}
				val, err := n.Filter.Eval(row)
				if err != nil {
					continue
				}
				if val.Type == tuple.TypeBool && val.Bool {
					rows = append(rows, r)
				}
			}
		} else {
			rows = cte.Rows
		}

		return &Result{
			Columns: cols,
			Rows:    rows,
			Message: fmt.Sprintf("SELECT %d", len(rows)),
		}, nil
	}

	if n.IsTerminal {
		// No Project node narrows the output (e.g. SELECT *).
		// Check every scanned column individually.
		if err := ex.checkColumnPrivilege(n.Table, n.Columns, catalog.PrivSelect); err != nil {
			return nil, err
		}
	} else {
		// A Project node will check the specific projected columns.
		// Here we only verify table-level access or existence of any column grant.
		if err := ex.checkTablePrivilege(n.Table, catalog.PrivSelect); err != nil {
			if ex.hasAnyColumnGrant(n.Table, catalog.PrivSelect) {
				err = nil
			}
			if err != nil {
				return nil, err
			}
		}
	}
	alias := n.Alias
	if alias == "" {
		alias = n.Table
	}
	colNames := make([]string, len(n.Columns))
	for i, c := range n.Columns {
		colNames[i] = alias + "." + c
	}

	result := &Result{Columns: colNames}

	// If this is a partitioned parent, scan all child partitions instead.
	if pinfo, ok := ex.Cat.Partitions[n.Table]; ok && len(pinfo.Children) > 0 {
		for _, child := range pinfo.Children {
			ex.Cat.SeqScan(child.TableName, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
				row := &planner.Row{Columns: tup.Columns, Names: colNames}
				if n.Filter != nil && !planner.EvalBool(n.Filter, row) {
					return true
				}
				rowCopy := make([]tuple.Datum, len(tup.Columns))
				copy(rowCopy, tup.Columns)
				result.Rows = append(result.Rows, rowCopy)
				return true
			})
		}
		return result, nil
	}

	ex.Cat.SeqScan(n.Table, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		row := &planner.Row{Columns: tup.Columns, Names: colNames}
		if n.Filter != nil && !planner.EvalBool(n.Filter, row) {
			return true
		}
		rowCopy := make([]tuple.Datum, len(tup.Columns))
		copy(rowCopy, tup.Columns)
		result.Rows = append(result.Rows, rowCopy)
		return true
	})
	return result, nil
}

func (ex *Executor) execIndexScan(n *planner.PhysIndexScan) (*Result, error) {
	if err := ex.checkTablePrivilege(n.Table, catalog.PrivSelect); err != nil {
		return nil, err
	}
	if n.Key == nil {
		return nil, fmt.Errorf("executor: IndexScan requires a key")
	}
	keyVal, err := n.Key.Eval(&planner.Row{})
	if err != nil {
		return nil, err
	}

	key, ok := datumToInt64(keyVal)
	if !ok {
		return nil, fmt.Errorf("executor: index key must be integer")
	}

	tuples, _, err := ex.Cat.IndexScan(n.Index, key)
	if err != nil {
		return nil, err
	}

	alias := n.Alias
	if alias == "" {
		alias = n.Table
	}
	colNames := make([]string, len(n.Columns))
	for i, c := range n.Columns {
		colNames[i] = alias + "." + c
	}

	result := &Result{Columns: colNames}
	for _, tup := range tuples {
		row := make([]tuple.Datum, len(tup.Columns))
		copy(row, tup.Columns)
		result.Rows = append(result.Rows, row)
	}
	return result, nil
}

func (ex *Executor) execBitmapHeapScan(n *planner.PhysBitmapHeapScan) (*Result, error) {
	if err := ex.checkTablePrivilege(n.Table, catalog.PrivSelect); err != nil {
		return nil, err
	}

	// The child must be a BitmapIndexScan.
	bitmapIdx, ok := n.Child.(*planner.PhysBitmapIndexScan)
	if !ok {
		return nil, fmt.Errorf("executor: BitmapHeapScan child must be BitmapIndexScan")
	}
	if bitmapIdx.Key == nil {
		return nil, fmt.Errorf("executor: BitmapIndexScan requires a key")
	}
	keyVal, err := bitmapIdx.Key.Eval(&planner.Row{})
	if err != nil {
		return nil, err
	}
	key, ok2 := datumToInt64(keyVal)
	if !ok2 {
		return nil, fmt.Errorf("executor: bitmap index key must be integer")
	}

	// Phase 1: Bitmap Index Scan — collect TIDs sorted by page.
	tids, err := ex.Cat.BitmapIndexScan(bitmapIdx.Index, key)
	if err != nil {
		return nil, err
	}

	alias := n.Alias
	if alias == "" {
		alias = n.Table
	}
	colNames := make([]string, len(n.Columns))
	for i, c := range n.Columns {
		colNames[i] = alias + "." + c
	}

	// Phase 2: Bitmap Heap Scan — fetch tuples in page order with recheck.
	result := &Result{Columns: colNames}
	for _, tid := range tids {
		tup, err := ex.Cat.FetchHeapTuple(tid)
		if err != nil || tup == nil {
			continue
		}
		// Recheck condition (lossy bitmap may include false positives).
		if n.Recheck != nil {
			row := &planner.Row{Columns: tup.Columns, Names: colNames}
			if !planner.EvalBool(n.Recheck, row) {
				continue
			}
		}
		rowCopy := make([]tuple.Datum, len(tup.Columns))
		copy(rowCopy, tup.Columns)
		result.Rows = append(result.Rows, rowCopy)
	}
	return result, nil
}

// --- Filter / Project ---

func (ex *Executor) execFilter(n *planner.PhysFilter) (*Result, error) {
	child, err := ex.Execute(n.Child)
	if err != nil {
		return nil, err
	}

	result := &Result{Columns: child.Columns}
	for _, row := range child.Rows {
		r := &planner.Row{Columns: row, Names: child.Columns}
		if planner.EvalBool(n.Predicate, r) {
			result.Rows = append(result.Rows, row)
		}
	}
	return result, nil
}

func (ex *Executor) execProject(n *planner.PhysProject) (*Result, error) {
	child, err := ex.Execute(n.Child)
	if err != nil {
		return nil, err
	}

	// Column-level privilege check: verify the user can SELECT each projected column.
	// Use the child's column names (table.col format) to resolve table context
	// for unqualified projected column names.
	if ex.CurrentUser != "" {
		projNames := resolveProjectNames(n.Names, child.Columns)
		if err := ex.checkProjectedColumns(projNames, catalog.PrivSelect); err != nil {
			return nil, err
		}
	}

	// Check if any projected expression is a set-returning function (unnest).
	srfIdx := -1
	for i, expr := range n.Exprs {
		if fn, ok := expr.(*planner.ExprFunc); ok && strings.ToLower(fn.Name) == "unnest" {
			srfIdx = i
			break
		}
	}

	result := &Result{Columns: n.Names}
	for _, row := range child.Rows {
		r := &planner.Row{Columns: row, Names: child.Columns}

		if srfIdx >= 0 {
			// Evaluate the SRF argument to get the array, then expand.
			fn := n.Exprs[srfIdx].(*planner.ExprFunc)
			arrVal, err := fn.Args[0].Eval(r)
			if err != nil {
				return nil, err
			}
			elements := expandArray(arrVal)
			// Evaluate non-SRF expressions once.
			base := make([]tuple.Datum, len(n.Exprs))
			for j, expr := range n.Exprs {
				if j == srfIdx {
					continue
				}
				val, err := expr.Eval(r)
				if err != nil {
					return nil, err
				}
				base[j] = val
			}
			for _, elem := range elements {
				projected := make([]tuple.Datum, len(n.Exprs))
				copy(projected, base)
				projected[srfIdx] = elem
				result.Rows = append(result.Rows, projected)
			}
			continue
		}

		var projected []tuple.Datum
		for _, expr := range n.Exprs {
			val, err := expr.Eval(r)
			if err != nil {
				return nil, err
			}
			projected = append(projected, val)
		}
		result.Rows = append(result.Rows, projected)
	}
	return result, nil
}

// expandArray parses a PG array literal or array datum into individual elements.
func expandArray(d tuple.Datum) []tuple.Datum {
	if d.Type == tuple.TypeNull {
		return nil
	}
	s := ""
	switch d.Type {
	case tuple.TypeText:
		s = d.Text
	default:
		return []tuple.Datum{d}
	}
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return []tuple.Datum{d}
	}
	inner := s[1 : len(s)-1]
	if inner == "" {
		return nil
	}
	parts := strings.Split(inner, ",")
	result := make([]tuple.Datum, len(parts))
	for i, p := range parts {
		result[i] = tuple.DText(strings.TrimSpace(p))
	}
	return result
}

// --- Join executors ---

func (ex *Executor) execNestedLoopJoin(n *planner.PhysNestedLoopJoin) (*Result, error) {
	// Parameterized nested loop: re-execute inner index scan per outer row.
	if n.InnerParam != nil {
		return ex.execParamNestedLoop(n)
	}

	// LATERAL subquery: re-execute inner plan per outer row with outer
	// column values visible via OuterRowContext.
	if sub, ok := n.Inner.(*planner.PhysSubqueryScan); ok && sub.Lateral {
		return ex.execLateralNestedLoop(n, sub)
	}

	outer, err := ex.Execute(n.Outer)
	if err != nil {
		return nil, err
	}
	inner, err := ex.Execute(n.Inner)
	if err != nil {
		return nil, err
	}

	colNames := append(outer.Columns, inner.Columns...)
	result := &Result{Columns: colNames}

	// Track which inner rows matched (for FULL JOIN).
	innerMatched := make([]bool, len(inner.Rows))

	for _, outerRow := range outer.Rows {
		matched := false
		for j, innerRow := range inner.Rows {
			combined := append(append([]tuple.Datum{}, outerRow...), innerRow...)
			if n.Condition != nil {
				r := &planner.Row{Columns: combined, Names: colNames}
				if !planner.EvalBool(n.Condition, r) {
					continue
				}
			}
			matched = true
			innerMatched[j] = true
			result.Rows = append(result.Rows, combined)
		}
		// LEFT/FULL JOIN: emit outer row with NULLs if no match.
		if !matched && (n.Type == planner.JoinLeft || n.Type == planner.JoinFull) {
			nulls := make([]tuple.Datum, len(inner.Columns))
			for i := range nulls {
				nulls[i] = tuple.DNull()
			}
			combined := append(append([]tuple.Datum{}, outerRow...), nulls...)
			result.Rows = append(result.Rows, combined)
		}
	}

	// RIGHT/FULL JOIN: emit unmatched inner rows with NULL outer columns.
	if n.Type == planner.JoinRight || n.Type == planner.JoinFull {
		for j, innerRow := range inner.Rows {
			if innerMatched[j] {
				continue
			}
			nulls := make([]tuple.Datum, len(outer.Columns))
			for i := range nulls {
				nulls[i] = tuple.DNull()
			}
			combined := append(append([]tuple.Datum{}, nulls...), innerRow...)
			result.Rows = append(result.Rows, combined)
		}
	}

	return result, nil
}

// execLateralNestedLoop executes a nested loop where the inner side is a
// LATERAL subquery. For each outer row, the inner plan is re-executed with
// the outer row's columns visible via OuterRowContext.
func (ex *Executor) execLateralNestedLoop(n *planner.PhysNestedLoopJoin, sub *planner.PhysSubqueryScan) (*Result, error) {
	outer, err := ex.Execute(n.Outer)
	if err != nil {
		return nil, err
	}

	// Build inner column names.
	innerColNames := make([]string, len(sub.Columns))
	for i, c := range sub.Columns {
		innerColNames[i] = sub.Alias + "." + c
	}
	colNames := append(outer.Columns, innerColNames...)
	result := &Result{Columns: colNames}

	savedOuter := planner.OuterRowContext
	defer func() { planner.OuterRowContext = savedOuter }()

	for _, outerRow := range outer.Rows {
		// Set the outer row context so ExprColumn.Eval can resolve
		// references to outer table columns.
		planner.OuterRowContext = &planner.Row{
			Columns: outerRow,
			Names:   outer.Columns,
		}

		inner, err := ex.Execute(sub.Child)
		if err != nil {
			return nil, fmt.Errorf("executor: lateral subquery %q: %w", sub.Alias, err)
		}

		matched := false
		for _, innerRow := range inner.Rows {
			combined := append(append([]tuple.Datum{}, outerRow...), innerRow...)
			if n.Condition != nil {
				r := &planner.Row{Columns: combined, Names: colNames}
				if !planner.EvalBool(n.Condition, r) {
					continue
				}
			}
			matched = true
			result.Rows = append(result.Rows, combined)
		}

		// LEFT JOIN: emit outer row with NULLs if no match.
		if !matched && (n.Type == planner.JoinLeft || n.Type == planner.JoinFull) {
			nulls := make([]tuple.Datum, len(innerColNames))
			for i := range nulls {
				nulls[i] = tuple.DNull()
			}
			combined := append(append([]tuple.Datum{}, outerRow...), nulls...)
			result.Rows = append(result.Rows, combined)
		}
	}

	return result, nil
}

// execParamNestedLoop executes a parameterized nested loop join.
// For each outer row, it extracts the join key from the outer column
// and performs an index scan on the inner side with that key.
func (ex *Executor) execParamNestedLoop(n *planner.PhysNestedLoopJoin) (*Result, error) {
	outer, err := ex.Execute(n.Outer)
	if err != nil {
		return nil, err
	}

	innerIdx, ok := n.Inner.(*planner.PhysIndexScan)
	if !ok {
		return nil, fmt.Errorf("executor: parameterized NL inner must be IndexScan")
	}

	// Build inner column names.
	innerAlias := innerIdx.Alias
	if innerAlias == "" {
		innerAlias = innerIdx.Table
	}
	innerColNames := make([]string, len(innerIdx.Columns))
	for i, c := range innerIdx.Columns {
		innerColNames[i] = innerAlias + "." + c
	}

	colNames := append(outer.Columns, innerColNames...)
	result := &Result{Columns: colNames}

	// Find the outer column index.
	paramCol := n.InnerParam.OuterCol
	outerColIdx := -1
	for i, name := range outer.Columns {
		if strings.EqualFold(name, paramCol) {
			outerColIdx = i
			break
		}
	}
	if outerColIdx < 0 {
		// Try unqualified match.
		parts := strings.SplitN(paramCol, ".", 2)
		target := paramCol
		if len(parts) == 2 {
			target = parts[1]
		}
		for i, name := range outer.Columns {
			nameParts := strings.SplitN(name, ".", 2)
			colName := name
			if len(nameParts) == 2 {
				colName = nameParts[1]
			}
			if strings.EqualFold(colName, target) {
				outerColIdx = i
				break
			}
		}
	}
	if outerColIdx < 0 {
		return nil, fmt.Errorf("executor: parameterized NL outer column %q not found in %v", paramCol, outer.Columns)
	}

	for _, outerRow := range outer.Rows {
		keyVal := outerRow[outerColIdx]
		key, ok := datumToInt64(keyVal)
		if !ok {
			continue
		}

		tuples, _, err := ex.Cat.IndexScan(innerIdx.Index, key)
		if err != nil {
			return nil, err
		}

		matched := false
		for _, tup := range tuples {
			matched = true
			combined := make([]tuple.Datum, 0, len(outerRow)+len(tup.Columns))
			combined = append(combined, outerRow...)
			combined = append(combined, tup.Columns...)
			result.Rows = append(result.Rows, combined)
		}

		if !matched && n.Type == planner.JoinLeft {
			nulls := make([]tuple.Datum, len(innerColNames))
			for i := range nulls {
				nulls[i] = tuple.DNull()
			}
			combined := append(append([]tuple.Datum{}, outerRow...), nulls...)
			result.Rows = append(result.Rows, combined)
		}
	}

	return result, nil
}

func (ex *Executor) execHashJoin(n *planner.PhysHashJoin) (*Result, error) {
	outer, err := ex.Execute(n.Outer)
	if err != nil {
		return nil, err
	}
	inner, err := ex.Execute(n.Inner)
	if err != nil {
		return nil, err
	}

	colNames := append(outer.Columns, inner.Columns...)

	// Extract the join columns from the condition.
	binOp, ok := n.Condition.(*planner.ExprBinOp)
	if !ok {
		// Fall back to nested loop.
		return ex.execNestedLoopJoin(&planner.PhysNestedLoopJoin{
			Type: n.Type, Condition: n.Condition,
			Outer: n.Outer, Inner: n.Inner, Estimate: n.Estimate,
		})
	}

	leftCol, _ := binOp.Left.(*planner.ExprColumn)
	rightCol, _ := binOp.Right.(*planner.ExprColumn)
	if leftCol == nil || rightCol == nil {
		return ex.execNestedLoopJoin(&planner.PhysNestedLoopJoin{
			Type: n.Type, Condition: n.Condition,
			Outer: n.Outer, Inner: n.Inner, Estimate: n.Estimate,
		})
	}

	// Resolve column indices within inner and outer.
	innerKeyIdx := resolveColIdx(rightCol, inner.Columns)
	outerKeyIdx := resolveColIdx(leftCol, outer.Columns)
	if innerKeyIdx < 0 {
		innerKeyIdx = resolveColIdx(leftCol, inner.Columns)
		outerKeyIdx = resolveColIdx(rightCol, outer.Columns)
	}

	// Build hash table on inner.
	type hashEntry struct {
		key string
		row []tuple.Datum
	}
	hashTable := make(map[string][]int) // key string → inner row indices
	for i, row := range inner.Rows {
		if innerKeyIdx >= 0 && innerKeyIdx < len(row) {
			k := datumHashKey(row[innerKeyIdx])
			hashTable[k] = append(hashTable[k], i)
		}
	}

	result := &Result{Columns: colNames}
	innerMatched := make([]bool, len(inner.Rows))
	for _, outerRow := range outer.Rows {
		var k string
		if outerKeyIdx >= 0 && outerKeyIdx < len(outerRow) {
			k = datumHashKey(outerRow[outerKeyIdx])
		}
		matched := false
		for _, idx := range hashTable[k] {
			combined := append(append([]tuple.Datum{}, outerRow...), inner.Rows[idx]...)
			matched = true
			innerMatched[idx] = true
			result.Rows = append(result.Rows, combined)
		}
		if !matched && (n.Type == planner.JoinLeft || n.Type == planner.JoinFull) {
			nulls := make([]tuple.Datum, len(inner.Columns))
			for i := range nulls {
				nulls[i] = tuple.DNull()
			}
			combined := append(append([]tuple.Datum{}, outerRow...), nulls...)
			result.Rows = append(result.Rows, combined)
		}
	}

	// RIGHT/FULL JOIN: emit unmatched inner rows.
	if n.Type == planner.JoinRight || n.Type == planner.JoinFull {
		for j, innerRow := range inner.Rows {
			if innerMatched[j] {
				continue
			}
			nulls := make([]tuple.Datum, len(outer.Columns))
			for i := range nulls {
				nulls[i] = tuple.DNull()
			}
			combined := append(append([]tuple.Datum{}, nulls...), innerRow...)
			result.Rows = append(result.Rows, combined)
		}
	}

	return result, nil
}

// --- Limit / Sort ---

func (ex *Executor) execLimit(n *planner.PhysLimit) (*Result, error) {
	child, err := ex.Execute(n.Child)
	if err != nil {
		return nil, err
	}

	result := &Result{Columns: child.Columns}
	start := int(n.Offset)
	if start > len(child.Rows) {
		start = len(child.Rows)
	}
	end := len(child.Rows)
	if n.Count >= 0 && start+int(n.Count) < end {
		end = start + int(n.Count)
	}
	result.Rows = child.Rows[start:end]
	return result, nil
}

// execAggregate implements hash-based aggregation. It reads all rows
// from the child, groups them by the GROUP BY expressions, and
// computes aggregate functions for each group.
func (ex *Executor) execAggregate(n *planner.PhysAggregate) (*Result, error) {
	child, err := ex.Execute(n.Child)
	if err != nil {
		return nil, err
	}

	// GROUPING SETS / CUBE / ROLLUP: run aggregation once per set.
	if len(n.GroupingSets) > 0 {
		return ex.execGroupingSets(n, child)
	}

	type aggState struct {
		count  int64
		sum    float64
		sumSq  float64 // sum of squares for variance/stddev
		hasVal bool
		min    tuple.Datum
		max    tuple.Datum
		vals   []tuple.Datum // for DISTINCT
		strBuf strings.Builder // for string_agg
		arrBuf []tuple.Datum   // for array_agg
		// Two-variable statistics (corr, covar, regr_*).
		sumX   float64
		sumY   float64
		sumXX  float64
		sumYY  float64
		sumXY  float64
		countN int64 // count of non-null pairs
	}

	type groupEntry struct {
		groupKey []tuple.Datum
		aggs     []aggState
	}

	// Use ordered slice of groups to preserve insertion order.
	var groups []*groupEntry
	groupIndex := map[string]*groupEntry{} // hash key → entry

	for _, row := range child.Rows {
		r := &planner.Row{Columns: row, Names: child.Columns}

		// Compute group key.
		groupKey := make([]tuple.Datum, len(n.GroupExprs))
		var keyBuf strings.Builder
		for i, expr := range n.GroupExprs {
			val, err := expr.Eval(r)
			if err != nil {
				return nil, err
			}
			groupKey[i] = val
			if i > 0 {
				keyBuf.WriteByte(0)
			}
			keyBuf.WriteString(datumHashKey(val))
		}
		hashKey := keyBuf.String()

		entry, ok := groupIndex[hashKey]
		if !ok {
			entry = &groupEntry{
				groupKey: groupKey,
				aggs:     make([]aggState, len(n.AggDescs)),
			}
			groups = append(groups, entry)
			groupIndex[hashKey] = entry
		}

		// Feed row into each aggregate.
		for i, ad := range n.AggDescs {
			st := &entry.aggs[i]
			if ad.Star {
				// count(*)
				st.count++
				continue
			}

			// Ordered-set aggregates: accumulate the WITHIN GROUP expression value.
			if ad.WithinGroupExpr != nil {
				wval, err := ad.WithinGroupExpr.Eval(r)
				if err != nil {
					return nil, err
				}
				if wval.Type != tuple.TypeNull {
					st.arrBuf = append(st.arrBuf, wval)
					st.count++
				}
				continue
			}

			if len(ad.ArgExprs) == 0 {
				st.count++
				continue
			}
			val, err := ad.ArgExprs[0].Eval(r)
			if err != nil {
				return nil, err
			}
			// Skip NULLs for all aggregates except count(*).
			if val.Type == tuple.TypeNull {
				continue
			}

			// DISTINCT: track values and skip duplicates.
			if ad.Distinct {
				dup := false
				for _, prev := range st.vals {
					if planner.CompareDatums(prev, val) == 0 {
						dup = true
						break
					}
				}
				if dup {
					continue
				}
				st.vals = append(st.vals, val)
			}

			st.count++
			switch ad.Func {
			case "sum", "avg":
				st.sum += datumToFloat64(val)
			case "stddev", "stddev_pop", "stddev_samp", "variance", "var_pop", "var_samp":
				v := datumToFloat64(val)
				st.sum += v
				st.sumSq += v * v
			case "corr", "covar_pop", "covar_samp",
				"regr_slope", "regr_intercept", "regr_count", "regr_r2",
				"regr_avgx", "regr_avgy", "regr_sxx", "regr_syy", "regr_sxy":
				// Two-variable: first arg = Y, second arg = X.
				y := datumToFloat64(val)
				x := 0.0
				if len(ad.ArgExprs) >= 2 {
					xv, err := ad.ArgExprs[1].Eval(r)
					if err == nil && xv.Type != tuple.TypeNull {
						x = datumToFloat64(xv)
					}
				}
				st.sumX += x
				st.sumY += y
				st.sumXX += x * x
				st.sumYY += y * y
				st.sumXY += x * y
				st.countN++
			case "min":
				if !st.hasVal || planner.CompareDatums(val, st.min) < 0 {
					st.min = val
				}
			case "max":
				if !st.hasVal || planner.CompareDatums(val, st.max) > 0 {
					st.max = val
				}
			case "string_agg":
				// Get delimiter from second argument (default ',').
				delim := ","
				if len(ad.ArgExprs) >= 2 {
					dv, err := ad.ArgExprs[1].Eval(r)
					if err == nil && dv.Type == tuple.TypeText {
						delim = dv.Text
					}
				}
				if st.hasVal {
					st.strBuf.WriteString(delim)
				}
				st.strBuf.WriteString(datumToStringVal(val))
			case "array_agg":
				st.arrBuf = append(st.arrBuf, val)
			case "json_agg", "jsonb_agg":
				st.arrBuf = append(st.arrBuf, val)
			case "json_object_agg", "jsonb_object_agg":
				// Collect key from first arg (val), value from second arg.
				st.arrBuf = append(st.arrBuf, val)
				if len(ad.ArgExprs) >= 2 {
					val2, err := ad.ArgExprs[1].Eval(r)
					if err == nil {
						st.arrBuf = append(st.arrBuf, val2)
					}
				}
			default:
				// Custom aggregates: collect all values for sfunc processing.
				st.arrBuf = append(st.arrBuf, val)
			}
			st.hasVal = true
		}
	}

	// If no rows and no GROUP BY, produce a single row with default agg values.
	if len(groups) == 0 && len(n.GroupExprs) == 0 {
		entry := &groupEntry{
			groupKey: nil,
			aggs:     make([]aggState, len(n.AggDescs)),
		}
		groups = append(groups, entry)
	}

	// Build output columns: group columns + aggregate results.
	// For group-by columns, use the child's column name if the
	// expression is a simple column reference, so the Project above
	// can resolve references by name.
	var outCols []string
	for _, expr := range n.GroupExprs {
		name := expr.String()
		// Try to match against child column names for better resolution.
		if ec, ok := expr.(*planner.ExprColumn); ok {
			for _, cn := range child.Columns {
				bare := cn
				if idx := strings.LastIndex(cn, "."); idx >= 0 {
					bare = cn[idx+1:]
				}
				if strings.EqualFold(bare, ec.Column) {
					name = cn
					break
				}
			}
		}
		outCols = append(outCols, name)
	}
	for i, ad := range n.AggDescs {
		outCols = append(outCols, fmt.Sprintf("%s_%d", ad.Func, i))
	}

	result := &Result{Columns: outCols}
	for _, entry := range groups {
		var row []tuple.Datum
		// Group-by columns first.
		row = append(row, entry.groupKey...)
		// Then aggregate results.
		for i, ad := range n.AggDescs {
			st := &entry.aggs[i]
			switch ad.Func {
			case "count":
				row = append(row, tuple.DInt64(st.count))
			case "sum":
				if st.count == 0 {
					row = append(row, tuple.DNull())
				} else {
					row = append(row, tuple.DFloat64(st.sum))
				}
			case "avg":
				if st.count == 0 {
					row = append(row, tuple.DNull())
				} else {
					row = append(row, tuple.DFloat64(st.sum/float64(st.count)))
				}
			case "min":
				if !st.hasVal {
					row = append(row, tuple.DNull())
				} else {
					row = append(row, st.min)
				}
			case "max":
				if !st.hasVal {
					row = append(row, tuple.DNull())
				} else {
					row = append(row, st.max)
				}
			case "bool_and", "every":
				if !st.hasVal {
					row = append(row, tuple.DNull())
				} else {
					row = append(row, st.min) // min of bools = AND
				}
			case "bool_or":
				if !st.hasVal {
					row = append(row, tuple.DNull())
				} else {
					row = append(row, st.max) // max of bools = OR
				}
			case "string_agg":
				if !st.hasVal {
					row = append(row, tuple.DNull())
				} else {
					row = append(row, tuple.DText(st.strBuf.String()))
				}
			case "array_agg":
				if len(st.arrBuf) == 0 {
					row = append(row, tuple.DNull())
				} else {
					// Format as PostgreSQL array literal: {val1,val2,...}
					var sb strings.Builder
					sb.WriteByte('{')
					for j, v := range st.arrBuf {
						if j > 0 {
							sb.WriteByte(',')
						}
						sb.WriteString(datumToStringVal(v))
					}
					sb.WriteByte('}')
					row = append(row, tuple.DText(sb.String()))
				}
			case "json_agg", "jsonb_agg":
				if len(st.arrBuf) == 0 {
					row = append(row, tuple.DNull())
				} else {
					var elems []interface{}
					for _, v := range st.arrBuf {
						elems = append(elems, datumToJSONValue(v))
					}
					out, _ := json.Marshal(elems)
					row = append(row, tuple.DJSON(string(out)))
				}
			case "json_object_agg", "jsonb_object_agg":
				if len(st.arrBuf) == 0 {
					row = append(row, tuple.DNull())
				} else {
					obj := map[string]interface{}{}
					for i := 0; i+1 < len(st.arrBuf); i += 2 {
						key := datumToStringVal(st.arrBuf[i])
						obj[key] = datumToJSONValue(st.arrBuf[i+1])
					}
					out, _ := json.Marshal(obj)
					row = append(row, tuple.DJSON(string(out)))
				}
			case "stddev", "stddev_samp":
				// Sample standard deviation: sqrt((sumSq - sum²/n) / (n-1))
				if st.count < 2 {
					row = append(row, tuple.DNull())
				} else {
					n := float64(st.count)
					variance := (st.sumSq - st.sum*st.sum/n) / (n - 1)
					row = append(row, tuple.DFloat64(math.Sqrt(variance)))
				}
			case "stddev_pop":
				// Population standard deviation: sqrt((sumSq - sum²/n) / n)
				if st.count == 0 {
					row = append(row, tuple.DNull())
				} else {
					n := float64(st.count)
					variance := (st.sumSq - st.sum*st.sum/n) / n
					row = append(row, tuple.DFloat64(math.Sqrt(variance)))
				}
			case "variance", "var_samp":
				// Sample variance: (sumSq - sum²/n) / (n-1)
				if st.count < 2 {
					row = append(row, tuple.DNull())
				} else {
					n := float64(st.count)
					row = append(row, tuple.DFloat64((st.sumSq-st.sum*st.sum/n)/(n-1)))
				}
			case "var_pop":
				// Population variance: (sumSq - sum²/n) / n
				if st.count == 0 {
					row = append(row, tuple.DNull())
				} else {
					n := float64(st.count)
					row = append(row, tuple.DFloat64((st.sumSq-st.sum*st.sum/n)/n))
				}
			case "corr":
				if st.countN < 2 {
					row = append(row, tuple.DNull())
				} else {
					n := float64(st.countN)
					covXY := st.sumXY - st.sumX*st.sumY/n
					varX := st.sumXX - st.sumX*st.sumX/n
					varY := st.sumYY - st.sumY*st.sumY/n
					if varX == 0 || varY == 0 {
						row = append(row, tuple.DNull())
					} else {
						row = append(row, tuple.DFloat64(covXY/math.Sqrt(varX*varY)))
					}
				}
			case "covar_pop":
				if st.countN == 0 {
					row = append(row, tuple.DNull())
				} else {
					n := float64(st.countN)
					row = append(row, tuple.DFloat64((st.sumXY-st.sumX*st.sumY/n)/n))
				}
			case "covar_samp":
				if st.countN < 2 {
					row = append(row, tuple.DNull())
				} else {
					n := float64(st.countN)
					row = append(row, tuple.DFloat64((st.sumXY-st.sumX*st.sumY/n)/(n-1)))
				}
			case "regr_slope":
				if st.countN < 2 {
					row = append(row, tuple.DNull())
				} else {
					n := float64(st.countN)
					varX := st.sumXX - st.sumX*st.sumX/n
					if varX == 0 {
						row = append(row, tuple.DNull())
					} else {
						covXY := st.sumXY - st.sumX*st.sumY/n
						row = append(row, tuple.DFloat64(covXY/varX))
					}
				}
			case "regr_intercept":
				if st.countN < 2 {
					row = append(row, tuple.DNull())
				} else {
					n := float64(st.countN)
					varX := st.sumXX - st.sumX*st.sumX/n
					if varX == 0 {
						row = append(row, tuple.DNull())
					} else {
						covXY := st.sumXY - st.sumX*st.sumY/n
						slope := covXY / varX
						row = append(row, tuple.DFloat64(st.sumY/n-slope*st.sumX/n))
					}
				}
			case "regr_count":
				row = append(row, tuple.DInt64(st.countN))
			case "regr_r2":
				if st.countN < 2 {
					row = append(row, tuple.DNull())
				} else {
					n := float64(st.countN)
					varX := st.sumXX - st.sumX*st.sumX/n
					varY := st.sumYY - st.sumY*st.sumY/n
					if varX == 0 || varY == 0 {
						row = append(row, tuple.DNull())
					} else {
						covXY := st.sumXY - st.sumX*st.sumY/n
						r := covXY / math.Sqrt(varX*varY)
						row = append(row, tuple.DFloat64(r*r))
					}
				}
			case "regr_avgx":
				if st.countN == 0 {
					row = append(row, tuple.DNull())
				} else {
					row = append(row, tuple.DFloat64(st.sumX/float64(st.countN)))
				}
			case "regr_avgy":
				if st.countN == 0 {
					row = append(row, tuple.DNull())
				} else {
					row = append(row, tuple.DFloat64(st.sumY/float64(st.countN)))
				}
			case "regr_sxx":
				if st.countN == 0 {
					row = append(row, tuple.DNull())
				} else {
					n := float64(st.countN)
					row = append(row, tuple.DFloat64(st.sumXX-st.sumX*st.sumX/n))
				}
			case "regr_syy":
				if st.countN == 0 {
					row = append(row, tuple.DNull())
				} else {
					n := float64(st.countN)
					row = append(row, tuple.DFloat64(st.sumYY-st.sumY*st.sumY/n))
				}
			case "regr_sxy":
				if st.countN == 0 {
					row = append(row, tuple.DNull())
				} else {
					n := float64(st.countN)
					row = append(row, tuple.DFloat64(st.sumXY-st.sumX*st.sumY/n))
				}
			case "percentile_cont":
				if len(st.arrBuf) == 0 {
					row = append(row, tuple.DNull())
				} else {
					// Get the fraction argument.
					frac := 0.5
					if len(ad.ArgExprs) > 0 {
						fv, err := ad.ArgExprs[0].Eval(&planner.Row{})
						if err == nil {
							frac = datumToFloat64(fv)
						}
					}
					// Sort collected values.
					sort.Slice(st.arrBuf, func(a, b int) bool {
						return planner.CompareDatums(st.arrBuf[a], st.arrBuf[b]) < 0
					})
					// Continuous interpolation.
					n := float64(len(st.arrBuf))
					idx := frac * (n - 1)
					lo := int(math.Floor(idx))
					hi := int(math.Ceil(idx))
					if lo == hi || hi >= len(st.arrBuf) {
						row = append(row, tuple.DFloat64(datumToFloat64(st.arrBuf[lo])))
					} else {
						loVal := datumToFloat64(st.arrBuf[lo])
						hiVal := datumToFloat64(st.arrBuf[hi])
						row = append(row, tuple.DFloat64(loVal+(hiVal-loVal)*(idx-float64(lo))))
					}
				}
			case "percentile_disc":
				if len(st.arrBuf) == 0 {
					row = append(row, tuple.DNull())
				} else {
					frac := 0.5
					if len(ad.ArgExprs) > 0 {
						fv, err := ad.ArgExprs[0].Eval(&planner.Row{})
						if err == nil {
							frac = datumToFloat64(fv)
						}
					}
					sort.Slice(st.arrBuf, func(a, b int) bool {
						return planner.CompareDatums(st.arrBuf[a], st.arrBuf[b]) < 0
					})
					// Discrete: return the first value whose cumulative fraction >= frac.
					idx := int(math.Ceil(frac*float64(len(st.arrBuf)))) - 1
					if idx < 0 {
						idx = 0
					}
					if idx >= len(st.arrBuf) {
						idx = len(st.arrBuf) - 1
					}
					row = append(row, st.arrBuf[idx])
				}
			case "mode":
				if len(st.arrBuf) == 0 {
					row = append(row, tuple.DNull())
				} else {
					// Find the most frequent value.
					counts := map[string]int{}
					valMap := map[string]tuple.Datum{}
					for _, v := range st.arrBuf {
						key := datumToStringVal(v)
						counts[key]++
						valMap[key] = v
					}
					bestKey := ""
					bestCount := 0
					for k, c := range counts {
						if c > bestCount {
							bestCount = c
							bestKey = k
						}
					}
					row = append(row, valMap[bestKey])
				}
			case "rank":
				// Hypothetical rank: what rank would the arg have in the sorted set?
				if len(st.arrBuf) == 0 {
					row = append(row, tuple.DInt64(1))
				} else {
					hyp := tuple.DNull()
					if len(ad.ArgExprs) > 0 {
						hyp, _ = ad.ArgExprs[0].Eval(&planner.Row{})
					}
					sort.Slice(st.arrBuf, func(a, b int) bool {
						return planner.CompareDatums(st.arrBuf[a], st.arrBuf[b]) < 0
					})
					rank := int64(1)
					for _, v := range st.arrBuf {
						if planner.CompareDatums(v, hyp) < 0 {
							rank++
						} else {
							break
						}
					}
					row = append(row, tuple.DInt64(rank))
				}
			case "dense_rank":
				if len(st.arrBuf) == 0 {
					row = append(row, tuple.DInt64(1))
				} else {
					hyp := tuple.DNull()
					if len(ad.ArgExprs) > 0 {
						hyp, _ = ad.ArgExprs[0].Eval(&planner.Row{})
					}
					sort.Slice(st.arrBuf, func(a, b int) bool {
						return planner.CompareDatums(st.arrBuf[a], st.arrBuf[b]) < 0
					})
					// Count distinct values less than hyp.
					seen := map[string]bool{}
					denseRank := int64(1)
					for _, v := range st.arrBuf {
						if planner.CompareDatums(v, hyp) < 0 {
							key := datumToStringVal(v)
							if !seen[key] {
								seen[key] = true
								denseRank++
							}
						}
					}
					row = append(row, tuple.DInt64(denseRank))
				}
			case "percent_rank":
				if len(st.arrBuf) == 0 {
					row = append(row, tuple.DFloat64(0))
				} else {
					hyp := tuple.DNull()
					if len(ad.ArgExprs) > 0 {
						hyp, _ = ad.ArgExprs[0].Eval(&planner.Row{})
					}
					sort.Slice(st.arrBuf, func(a, b int) bool {
						return planner.CompareDatums(st.arrBuf[a], st.arrBuf[b]) < 0
					})
					rank := int64(0)
					for _, v := range st.arrBuf {
						if planner.CompareDatums(v, hyp) < 0 {
							rank++
						}
					}
					// percent_rank = (rank) / (N) where N includes the hypothetical row
					n := float64(len(st.arrBuf))
					row = append(row, tuple.DFloat64(float64(rank)/n))
				}
			case "cume_dist":
				if len(st.arrBuf) == 0 {
					row = append(row, tuple.DFloat64(1))
				} else {
					hyp := tuple.DNull()
					if len(ad.ArgExprs) > 0 {
						hyp, _ = ad.ArgExprs[0].Eval(&planner.Row{})
					}
					sort.Slice(st.arrBuf, func(a, b int) bool {
						return planner.CompareDatums(st.arrBuf[a], st.arrBuf[b]) < 0
					})
					// Count values <= hyp.
					count := int64(0)
					for _, v := range st.arrBuf {
						if planner.CompareDatums(v, hyp) <= 0 {
							count++
						}
					}
					// cume_dist = (count + 1) / (N + 1) — include the hypothetical row
					n := float64(len(st.arrBuf)) + 1
					row = append(row, tuple.DFloat64(float64(count+1)/n))
				}
			default:
				// Try custom aggregate execution.
				result, err := ex.execCustomAggregate(ad.Func, st.arrBuf)
				if err != nil {
					return nil, err
				}
				row = append(row, result)
			}
		}
		// Apply HAVING filter.
		if n.HavingQual != nil {
			r := &planner.Row{Columns: row, Names: outCols}
			if !planner.EvalBool(n.HavingQual, r) {
				continue
			}
		}
		result.Rows = append(result.Rows, row)
	}
	return result, nil
}

func datumToStringVal(d tuple.Datum) string {
	switch d.Type {
	case tuple.TypeText:
		return d.Text
	case tuple.TypeInt32:
		return fmt.Sprintf("%d", d.I32)
	case tuple.TypeInt64:
		return fmt.Sprintf("%d", d.I64)
	case tuple.TypeFloat64:
		return fmt.Sprintf("%g", d.F64)
	case tuple.TypeBool:
		if d.Bool {
			return "true"
		}
		return "false"
	default:
		return "NULL"
	}
}

// datumToJSONValue converts a Datum to a Go value suitable for json.Marshal.
func datumToJSONValue(d tuple.Datum) interface{} {
	switch d.Type {
	case tuple.TypeNull:
		return nil
	case tuple.TypeInt32:
		return d.I32
	case tuple.TypeInt64:
		return d.I64
	case tuple.TypeFloat64:
		return d.F64
	case tuple.TypeBool:
		return d.Bool
	case tuple.TypeJSON:
		// Already JSON — unmarshal to preserve structure.
		var v interface{}
		if json.Unmarshal([]byte(d.Text), &v) == nil {
			return v
		}
		return d.Text
	default:
		return d.Text
	}
}

func datumToFloat64(d tuple.Datum) float64 {
	switch d.Type {
	case tuple.TypeInt32:
		return float64(d.I32)
	case tuple.TypeInt64:
		return float64(d.I64)
	case tuple.TypeFloat64:
		return d.F64
	default:
		return 0
	}
}

// execCustomAggregate runs a user-defined aggregate by iterating sfunc over
// collected values, then optionally applying finalfunc.
func (ex *Executor) execCustomAggregate(funcName string, values []tuple.Datum) (tuple.Datum, error) {
	aggDef, ok := ex.Cat.CustomAggregates[strings.ToLower(funcName)]
	if !ok {
		return tuple.DNull(), nil
	}

	// Initialize state from initcond.
	state := tuple.DText(aggDef.InitCond)

	// Apply sfunc(state, value) for each collected value.
	sfunc := strings.ToLower(aggDef.SFunc)
	for _, val := range values {
		args := []planner.AnalyzedExpr{
			&planner.Const{Value: state, ConstType: state.Type},
			&planner.Const{Value: val, ConstType: val.Type},
		}
		result, err := planner.EvalBuiltinFunc(sfunc, args, &planner.Row{})
		if err != nil {
			return tuple.DNull(), fmt.Errorf("custom aggregate %q sfunc %q: %w", funcName, sfunc, err)
		}
		state = result
	}

	// Apply finalfunc if specified.
	if aggDef.FinalFunc != "" {
		ffunc := strings.ToLower(aggDef.FinalFunc)
		args := []planner.AnalyzedExpr{
			&planner.Const{Value: state, ConstType: state.Type},
		}
		result, err := planner.EvalBuiltinFunc(ffunc, args, &planner.Row{})
		if err != nil {
			return tuple.DNull(), fmt.Errorf("custom aggregate %q finalfunc %q: %w", funcName, ffunc, err)
		}
		state = result
	}

	return state, nil
}

// execGroupingSets runs the aggregation once per grouping set, producing
// a UNION ALL of the results. Non-active group columns are set to NULL.
func (ex *Executor) execGroupingSets(n *planner.PhysAggregate, child *Result) (*Result, error) {
	// Build output column names: group columns + aggregate columns.
	var outCols []string
	for _, expr := range n.GroupExprs {
		name := expr.String()
		if ec, ok := expr.(*planner.ExprColumn); ok {
			for _, cn := range child.Columns {
				bare := cn
				if idx := strings.LastIndex(cn, "."); idx >= 0 {
					bare = cn[idx+1:]
				}
				if strings.EqualFold(bare, ec.Column) {
					name = cn
					break
				}
			}
		}
		outCols = append(outCols, name)
	}
	for i, ad := range n.AggDescs {
		if ad.Star {
			outCols = append(outCols, fmt.Sprintf("%s_%d", ad.Func, i))
		} else {
			outCols = append(outCols, fmt.Sprintf("%s_%d", ad.Func, i))
		}
	}

	result := &Result{Columns: outCols}

	for _, activeSet := range n.GroupingSets {
		// Build a set of active group expression indices for fast lookup.
		active := map[int]bool{}
		for _, idx := range activeSet {
			active[idx] = true
		}

		type aggState struct {
			count  int64
			sum    float64
			sumSq  float64
			hasVal bool
			min    tuple.Datum
			max    tuple.Datum
			vals   []tuple.Datum
			strBuf strings.Builder
			arrBuf []tuple.Datum
		}
		type groupEntry struct {
			groupKey []tuple.Datum
			aggs     []aggState
		}

		var groups []*groupEntry
		groupIndex := map[string]*groupEntry{}

		for _, row := range child.Rows {
			r := &planner.Row{Columns: row, Names: child.Columns}

			// Compute group key using only active expressions.
			groupKey := make([]tuple.Datum, len(n.GroupExprs))
			var keyBuf strings.Builder
			for i, expr := range n.GroupExprs {
				if active[i] {
					val, err := expr.Eval(r)
					if err != nil {
						return nil, err
					}
					groupKey[i] = val
					fmt.Fprintf(&keyBuf, "%v|", val)
				} else {
					groupKey[i] = tuple.DNull()
					keyBuf.WriteString("NULL|")
				}
			}
			keyStr := keyBuf.String()

			entry, exists := groupIndex[keyStr]
			if !exists {
				entry = &groupEntry{
					groupKey: groupKey,
					aggs:     make([]aggState, len(n.AggDescs)),
				}
				groups = append(groups, entry)
				groupIndex[keyStr] = entry
			}

			// Feed row into aggregates.
			for i, ad := range n.AggDescs {
				st := &entry.aggs[i]
				if ad.Star {
					st.count++
					continue
				}
				if ad.WithinGroupExpr != nil {
					wval, err := ad.WithinGroupExpr.Eval(r)
					if err != nil {
						return nil, err
					}
					if wval.Type != tuple.TypeNull {
						st.arrBuf = append(st.arrBuf, wval)
						st.count++
					}
					continue
				}
				if len(ad.ArgExprs) == 0 {
					st.count++
					continue
				}
				val, err := ad.ArgExprs[0].Eval(r)
				if err != nil {
					return nil, err
				}
				if val.Type == tuple.TypeNull {
					continue
				}
				if ad.Distinct {
					dup := false
					for _, prev := range st.vals {
						if planner.CompareDatums(prev, val) == 0 {
							dup = true
							break
						}
					}
					if dup {
						continue
					}
					st.vals = append(st.vals, val)
				}
				st.count++
				switch ad.Func {
				case "sum", "avg":
					st.sum += datumToFloat64(val)
				case "stddev", "stddev_pop", "stddev_samp", "variance", "var_pop", "var_samp":
					v := datumToFloat64(val)
					st.sum += v
					st.sumSq += v * v
				case "min":
					if !st.hasVal || planner.CompareDatums(val, st.min) < 0 {
						st.min = val
					}
				case "max":
					if !st.hasVal || planner.CompareDatums(val, st.max) > 0 {
						st.max = val
					}
				case "string_agg":
					delim := ","
					if len(ad.ArgExprs) >= 2 {
						dv, err := ad.ArgExprs[1].Eval(r)
						if err == nil && dv.Type == tuple.TypeText {
							delim = dv.Text
						}
					}
					if st.hasVal {
						st.strBuf.WriteString(delim)
					}
					st.strBuf.WriteString(datumToStringVal(val))
				case "array_agg":
					st.arrBuf = append(st.arrBuf, val)
				}
				st.hasVal = true
			}
		}

		// If no rows and this is the empty set, produce a single row.
		if len(groups) == 0 && len(activeSet) == 0 {
			entry := &groupEntry{
				groupKey: make([]tuple.Datum, len(n.GroupExprs)),
				aggs:     make([]aggState, len(n.AggDescs)),
			}
			for i := range entry.groupKey {
				entry.groupKey[i] = tuple.DNull()
			}
			groups = append(groups, entry)
		}

		// Finalize and emit rows.
		for _, entry := range groups {
			row := make([]tuple.Datum, 0, len(n.GroupExprs)+len(n.AggDescs))
			row = append(row, entry.groupKey...)
			for i, ad := range n.AggDescs {
				st := &entry.aggs[i]
				switch ad.Func {
				case "count":
					row = append(row, tuple.DInt64(st.count))
				case "sum":
					if st.count == 0 {
						row = append(row, tuple.DNull())
					} else {
						row = append(row, tuple.DFloat64(st.sum))
					}
				case "avg":
					if st.count == 0 {
						row = append(row, tuple.DNull())
					} else {
						row = append(row, tuple.DFloat64(st.sum/float64(st.count)))
					}
				case "min":
					if !st.hasVal {
						row = append(row, tuple.DNull())
					} else {
						row = append(row, st.min)
					}
				case "max":
					if !st.hasVal {
						row = append(row, tuple.DNull())
					} else {
						row = append(row, st.max)
					}
				case "bool_and", "every":
					if !st.hasVal {
						row = append(row, tuple.DNull())
					} else {
						row = append(row, st.min)
					}
				case "bool_or":
					if !st.hasVal {
						row = append(row, tuple.DNull())
					} else {
						row = append(row, st.max)
					}
				case "string_agg":
					if !st.hasVal {
						row = append(row, tuple.DNull())
					} else {
						row = append(row, tuple.DText(st.strBuf.String()))
					}
				case "array_agg":
					if len(st.arrBuf) == 0 {
						row = append(row, tuple.DNull())
					} else {
						var sb strings.Builder
						sb.WriteByte('{')
						for j, v := range st.arrBuf {
							if j > 0 {
								sb.WriteByte(',')
							}
							sb.WriteString(datumToStringVal(v))
						}
						sb.WriteByte('}')
						row = append(row, tuple.DText(sb.String()))
					}
				default:
					row = append(row, tuple.DNull())
				}
			}
			result.Rows = append(result.Rows, row)
		}
	}

	return result, nil
}

func (ex *Executor) execSort(n *planner.PhysSort) (*Result, error) {
	child, err := ex.Execute(n.Child)
	if err != nil {
		return nil, err
	}

	result := &Result{Columns: child.Columns}
	result.Rows = make([][]tuple.Datum, len(child.Rows))
	copy(result.Rows, child.Rows)

	sort.SliceStable(result.Rows, func(i, j int) bool {
		for _, key := range n.Keys {
			ri := &planner.Row{Columns: result.Rows[i], Names: child.Columns}
			rj := &planner.Row{Columns: result.Rows[j], Names: child.Columns}
			vi, _ := key.Expr.Eval(ri)
			vj, _ := key.Expr.Eval(rj)
			cmp := planner.CompareDatums(vi, vj)
			if cmp == 0 {
				continue
			}
			if key.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
	return result, nil
}

func (ex *Executor) execSetOp(n *planner.PhysSetOp) (*Result, error) {
	left, err := ex.Execute(n.Left)
	if err != nil {
		return nil, err
	}
	right, err := ex.Execute(n.Right)
	if err != nil {
		return nil, err
	}
	result := &Result{Columns: left.Columns}

	switch n.Op {
	case planner.SetOpUnion:
		result.Rows = append(result.Rows, left.Rows...)
		result.Rows = append(result.Rows, right.Rows...)
		if !n.All {
			result.Rows = deduplicateRows(result.Rows)
		}
	case planner.SetOpIntersect:
		rightSet := make(map[string]int)
		for _, row := range right.Rows {
			rightSet[rowKey(row)]++
		}
		for _, row := range left.Rows {
			key := rowKey(row)
			if rightSet[key] > 0 {
				result.Rows = append(result.Rows, row)
				if !n.All {
					rightSet[key] = 0 // only once
				} else {
					rightSet[key]--
				}
			}
		}
	case planner.SetOpExcept:
		rightSet := make(map[string]int)
		for _, row := range right.Rows {
			rightSet[rowKey(row)]++
		}
		for _, row := range left.Rows {
			key := rowKey(row)
			if rightSet[key] > 0 {
				if n.All {
					rightSet[key]--
				}
				if !n.All {
					rightSet[key] = 0
				}
				continue
			}
			result.Rows = append(result.Rows, row)
		}
	}
	return result, nil
}

func deduplicateRows(rows [][]tuple.Datum) [][]tuple.Datum {
	seen := make(map[string]bool)
	var result [][]tuple.Datum
	for _, row := range rows {
		key := rowKey(row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}
	return result
}

func (ex *Executor) execDistinct(n *planner.PhysDistinct) (*Result, error) {
	child, err := ex.Execute(n.Child)
	if err != nil {
		return nil, err
	}
	result := &Result{Columns: child.Columns}
	seen := make(map[string]bool)
	for _, row := range child.Rows {
		key := rowKey(row)
		if !seen[key] {
			seen[key] = true
			result.Rows = append(result.Rows, row)
		}
	}
	return result, nil
}

func rowKey(row []tuple.Datum) string {
	var sb strings.Builder
	for i, d := range row {
		if i > 0 {
			sb.WriteByte(0)
		}
		sb.WriteString(fmt.Sprintf("%d:%v", d.Type, datumKeyVal(d)))
	}
	return sb.String()
}

func datumKeyVal(d tuple.Datum) interface{} {
	switch d.Type {
	case tuple.TypeNull:
		return nil
	case tuple.TypeBool:
		return d.Bool
	case tuple.TypeInt32:
		return d.I32
	case tuple.TypeInt64:
		return d.I64
	case tuple.TypeFloat64:
		return d.F64
	case tuple.TypeText:
		return d.Text
	default:
		return d.Text
	}
}

// --- DML executors ---

func (ex *Executor) execInsertSelect(n *planner.PhysInsertSelect) (*Result, error) {
	// Execute the SELECT to get source rows.
	selectResult, err := ex.Execute(n.Child)
	if err != nil {
		return nil, err
	}
	// Convert to VALUES-style PhysInsert and delegate.
	var values [][]planner.Expr
	for _, row := range selectResult.Rows {
		var rowExprs []planner.Expr
		for _, d := range row {
			rowExprs = append(rowExprs, &planner.ExprLiteral{Value: d})
		}
		values = append(values, rowExprs)
	}
	return ex.execInsert(&planner.PhysInsert{
		Table:   n.Table,
		Columns: n.Columns,
		Values:  values,
	})
}

func (ex *Executor) execInsert(n *planner.PhysInsert) (*Result, error) {
	if err := ex.checkTablePrivilege(n.Table, catalog.PrivInsert); err != nil {
		return nil, err
	}

	// Partition routing: if the target is a partitioned parent, route each row
	// to the correct child partition. A partitioned parent never holds data directly.
	if pinfo, ok := ex.Cat.Partitions[n.Table]; ok {
		return ex.execInsertPartitioned(n, pinfo)
	}

	tableCols := ex.getTableColumns(n.Table)
	colNames := make([]string, len(tableCols))
	for i, c := range tableCols {
		colNames[i] = c.Name
	}
	hasTriggers := len(ex.Cat.GetTableTriggers(n.Table)) > 0 && ex.TriggerExec != nil

	if hasTriggers {
		if err := ex.fireStatementTriggers(n.Table, catalog.TrigBefore, catalog.TrigInsert); err != nil {
			return nil, err
		}
	}

	// Build column index mapping when an explicit column list is provided.
	var colIndexMap []int // colIndexMap[i] = table column index for the i-th provided value
	if len(n.Columns) > 0 {
		colIndexMap = make([]int, len(n.Columns))
		for i, name := range n.Columns {
			found := false
			for j, tc := range tableCols {
				if strings.EqualFold(name, tc.Name) {
					colIndexMap[i] = j
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("column %q of relation %q does not exist", name, n.Table)
			}
		}
	}

	var count int64
	var returningRows [][]tuple.Datum
	for _, rowExprs := range n.Values {
		// Evaluate provided expressions.
		provided := make([]tuple.Datum, len(rowExprs))
		for i, expr := range rowExprs {
			val, err := expr.Eval(&planner.Row{})
			if err != nil {
				return nil, err
			}
			provided[i] = val
		}

		// Reject explicit values for generated columns.
		if colIndexMap != nil {
			for i, j := range colIndexMap {
				if i < len(provided) && tableCols[j].GeneratedExpr != "" {
					return nil, fmt.Errorf("cannot insert a non-DEFAULT value into column %q because it is a generated column", tableCols[j].Name)
				}
			}
		}

		// Build full-width row, applying defaults for missing columns.
		values := make([]tuple.Datum, len(tableCols))
		if colIndexMap != nil {
			// Explicit column list: fill defaults first, then overlay provided values.
			for j, tc := range tableCols {
				values[j] = ex.evalDefault(tc)
			}
			for i, j := range colIndexMap {
				if i < len(provided) {
					values[j] = provided[i]
				}
			}
		} else if len(provided) < len(tableCols) {
			// Fewer values than columns: fill trailing with defaults.
			copy(values, provided)
			for j := len(provided); j < len(tableCols); j++ {
				values[j] = ex.evalDefault(tableCols[j])
			}
		} else {
			copy(values, provided)
		}

		// Compute GENERATED ALWAYS AS (expr) STORED columns.
		ex.evalGeneratedColumns(tableCols, colNames, values)

		if hasTriggers {
			newMap := rowToMap(colNames, values)
			// BEFORE ROW INSERT triggers.
			modifiedNew, err := ex.fireTriggers(n.Table, catalog.TrigBefore, catalog.TrigInsert, colNames, newMap, nil)
			if err != nil {
				return nil, err
			}
			if modifiedNew == nil {
				continue // trigger suppressed the insert
			}
			values = mapToRow(colNames, modifiedNew)
		}

		// Coerce values to match column types (e.g., text → date, text → json).
		coerceInsertValues(tableCols, values)

		// Validate NOT NULL constraints.
		if err := validateNotNull(tableCols, values); err != nil {
			return nil, err
		}

		// Validate domain/enum constraints.
		if err := ex.validateCustomTypes(tableCols, values); err != nil {
			return nil, err
		}

		// Validate CHECK constraints.
		if err := ex.validateCheckConstraints(n.Table, colNames, values); err != nil {
			return nil, err
		}

		// Validate FOREIGN KEY constraints.
		if err := ex.validateForeignKeys(n.Table, colNames, values); err != nil {
			return nil, err
		}

		insertedID, err := ex.Cat.InsertInto(n.Table, values)
		if err != nil {
			if n.OnConflict != nil && strings.Contains(err.Error(), "duplicate key value violates unique constraint") {
				if n.OnConflict.Action == planner.OnConflictNothing {
					// DO NOTHING: skip this row.
					continue
				}
				if n.OnConflict.Action == planner.OnConflictUpdate {
					// DO UPDATE: find the conflicting row and update it.
					resultRow, err := ex.execOnConflictUpdate(n, values, colNames, tableCols)
					if err != nil {
						return nil, err
					}
					count++
					if len(n.ReturningExprs) > 0 && resultRow != nil {
						row := &planner.Row{Columns: resultRow, Names: colNames}
						retRow := make([]tuple.Datum, len(n.ReturningExprs))
						for i, expr := range n.ReturningExprs {
							val, err := expr.Eval(row)
							if err != nil {
								return nil, fmt.Errorf("executor: RETURNING eval: %w", err)
							}
							retRow[i] = val
						}
						returningRows = append(returningRows, retRow)
					}
					continue
				}
			}
			return nil, err
		}
		count++
		ex.RecordMutation(Mutation{Kind: MutInsert, Table: n.Table, ItemID: insertedID})

		// Evaluate RETURNING expressions against the inserted row.
		if len(n.ReturningExprs) > 0 {
			row := &planner.Row{Columns: values, Names: colNames}
			retRow := make([]tuple.Datum, len(n.ReturningExprs))
			for i, expr := range n.ReturningExprs {
				val, err := expr.Eval(row)
				if err != nil {
					return nil, fmt.Errorf("executor: RETURNING eval: %w", err)
				}
				retRow[i] = val
			}
			returningRows = append(returningRows, retRow)
		}

		if hasTriggers {
			// AFTER ROW INSERT triggers.
			newMap := rowToMap(colNames, values)
			if _, err := ex.fireTriggers(n.Table, catalog.TrigAfter, catalog.TrigInsert, colNames, newMap, nil); err != nil {
				return nil, err
			}
		}
	}

	if hasTriggers {
		if err := ex.fireStatementTriggers(n.Table, catalog.TrigAfter, catalog.TrigInsert); err != nil {
			return nil, err
		}
	}

	if len(n.ReturningExprs) > 0 {
		return &Result{
			Columns:      n.ReturningNames,
			Rows:         returningRows,
			RowsAffected: count,
			Message:      fmt.Sprintf("INSERT 0 %d", count),
		}, nil
	}
	return &Result{RowsAffected: count, Message: fmt.Sprintf("INSERT 0 %d", count)}, nil
}

// execOnConflictUpdate handles DO UPDATE for a conflicting INSERT row.
// It finds the existing row that conflicts on the conflict columns,
// evaluates the SET assignments with both the existing row and the
// proposed "excluded" row available, and updates in place.
func (ex *Executor) execOnConflictUpdate(
	n *planner.PhysInsert,
	proposed []tuple.Datum, // the values we tried to insert
	colNames []string,
	tableCols []catalog.Column,
) ([]tuple.Datum, error) {
	oc := n.OnConflict

	// Build a map from conflict column name → index in colNames.
	conflictIdxs := make([]int, len(oc.ConflictCols))
	for i, cc := range oc.ConflictCols {
		for j, cn := range colNames {
			if strings.EqualFold(cc, cn) {
				conflictIdxs[i] = j
				break
			}
		}
	}

	// Scan for the conflicting row.
	var matchID slottedpage.ItemID
	var matchRow []tuple.Datum
	ex.Cat.SeqScan(n.Table, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		for _, ci := range conflictIdxs {
			if ci >= len(tup.Columns) || ci >= len(proposed) {
				return true
			}
			if planner.CompareDatums(tup.Columns[ci], proposed[ci]) != 0 {
				return true
			}
		}
		matchID = id
		matchRow = make([]tuple.Datum, len(tup.Columns))
		copy(matchRow, tup.Columns)
		return false // found it
	})

	if matchRow == nil {
		return nil, fmt.Errorf("executor: ON CONFLICT UPDATE could not find conflicting row")
	}

	// Build combined column names: table columns + "excluded." prefixed columns.
	combinedNames := make([]string, len(colNames)*2)
	combinedVals := make([]tuple.Datum, len(colNames)*2)
	for i, cn := range colNames {
		combinedNames[i] = n.Table + "." + cn
		combinedVals[i] = matchRow[i]
		combinedNames[len(colNames)+i] = "excluded." + cn
		combinedVals[len(colNames)+i] = proposed[i]
	}

	combinedRow := &planner.Row{Columns: combinedVals, Names: combinedNames}

	// Evaluate optional WHERE clause — if false, skip the update.
	if oc.WhereExpr != nil {
		wval, err := oc.WhereExpr.Eval(combinedRow)
		if err != nil {
			return nil, fmt.Errorf("executor: ON CONFLICT WHERE: %w", err)
		}
		if !(wval.Type == tuple.TypeBool && wval.Bool) {
			return nil, nil // WHERE not satisfied, skip
		}
	}

	// Apply SET assignments.
	newVals := make([]tuple.Datum, len(matchRow))
	copy(newVals, matchRow)
	for _, a := range oc.Assignments {
		for ci, cn := range colNames {
			if strings.EqualFold(a.Column, cn) {
				val, err := a.Value.Eval(combinedRow)
				if err != nil {
					return nil, fmt.Errorf("executor: ON CONFLICT SET %s: %w", a.Column, err)
				}
				newVals[ci] = val
				break
			}
		}
	}

	// Coerce and validate.
	coerceInsertValues(tableCols, newVals)
	if err := validateNotNull(tableCols, newVals); err != nil {
		return nil, err
	}
	if err := ex.validateCustomTypes(tableCols, newVals); err != nil {
		return nil, err
	}
	if err := ex.validateCheckConstraints(n.Table, colNames, newVals); err != nil {
		return nil, err
	}

	oldCopy := make([]tuple.Datum, len(matchRow))
	copy(oldCopy, matchRow)
	newID, _ := ex.Cat.Update(n.Table, matchID, newVals)
	ex.RecordMutation(Mutation{Kind: MutUpdate, Table: n.Table, ItemID: matchID, NewItemID: newID, OldValues: oldCopy})
	return newVals, nil
}

func (ex *Executor) execDelete(n *planner.PhysDelete) (*Result, error) {
	if err := ex.checkTablePrivilege(n.Table, catalog.PrivDelete); err != nil {
		return nil, err
	}
	child, err := ex.Execute(n.Child)
	if err != nil {
		return nil, err
	}

	rel, err := ex.Cat.FindRelation(n.Table)
	if err != nil || rel == nil {
		return nil, fmt.Errorf("executor: table %q not found", n.Table)
	}

	colNames := ex.getTableColNames(n.Table)
	hasTriggers := len(ex.Cat.GetTableTriggers(n.Table)) > 0 && ex.TriggerExec != nil

	if hasTriggers {
		if err := ex.fireStatementTriggers(n.Table, catalog.TrigBefore, catalog.TrigDelete); err != nil {
			return nil, err
		}
	}

	type deleteTarget struct {
		id  slottedpage.ItemID
		row []tuple.Datum
	}
	var toDelete []deleteTarget
	matchIdx := 0
	ex.Cat.SeqScan(n.Table, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if matchIdx < len(child.Rows) {
			if rowsMatch(tup.Columns, child.Rows[matchIdx]) {
				row := make([]tuple.Datum, len(tup.Columns))
				copy(row, tup.Columns)
				toDelete = append(toDelete, deleteTarget{id: id, row: row})
				matchIdx++
			}
		}
		return true
	})

	var returningRows [][]tuple.Datum
	for _, dt := range toDelete {
		if hasTriggers {
			oldMap := rowToMap(colNames, dt.row)
			if _, err := ex.fireTriggers(n.Table, catalog.TrigBefore, catalog.TrigDelete, colNames, nil, oldMap); err != nil {
				return nil, err
			}
		}

		// Evaluate RETURNING before delete (row still visible).
		if len(n.ReturningExprs) > 0 {
			row := &planner.Row{Columns: dt.row, Names: colNames}
			retRow := make([]tuple.Datum, len(n.ReturningExprs))
			for i, expr := range n.ReturningExprs {
				val, err := expr.Eval(row)
				if err != nil {
					return nil, fmt.Errorf("executor: RETURNING eval: %w", err)
				}
				retRow[i] = val
			}
			returningRows = append(returningRows, retRow)
		}

		// Enforce FK actions on parent side before deleting.
		if err := ex.enforceForeignKeyOnDelete(n.Table, colNames, dt.row); err != nil {
			return nil, err
		}

		ex.RecordMutation(Mutation{Kind: MutDelete, Table: n.Table, ItemID: dt.id, OldValues: dt.row})
		ex.Cat.Delete(n.Table, dt.id)

		if hasTriggers {
			oldMap := rowToMap(colNames, dt.row)
			if _, err := ex.fireTriggers(n.Table, catalog.TrigAfter, catalog.TrigDelete, colNames, nil, oldMap); err != nil {
				return nil, err
			}
		}
	}

	if hasTriggers {
		if err := ex.fireStatementTriggers(n.Table, catalog.TrigAfter, catalog.TrigDelete); err != nil {
			return nil, err
		}
	}

	if len(n.ReturningExprs) > 0 {
		return &Result{
			Columns:      n.ReturningNames,
			Rows:         returningRows,
			RowsAffected: int64(len(toDelete)),
			Message:      fmt.Sprintf("DELETE %d", len(toDelete)),
		}, nil
	}
	return &Result{RowsAffected: int64(len(toDelete)), Message: fmt.Sprintf("DELETE %d", len(toDelete))}, nil
}

func (ex *Executor) execUpdate(n *planner.PhysUpdate) (*Result, error) {
	if err := ex.checkTablePrivilege(n.Table, catalog.PrivUpdate); err != nil {
		return nil, err
	}
	child, err := ex.Execute(n.Child)
	if err != nil {
		return nil, err
	}

	rel, err := ex.Cat.FindRelation(n.Table)
	if err != nil || rel == nil {
		return nil, fmt.Errorf("executor: table %q not found", n.Table)
	}

	tableCols := ex.getTableColumns(n.Table)
	colNames := make([]string, len(tableCols))
	for i, c := range tableCols {
		colNames[i] = c.Name
	}
	hasTriggers := len(ex.Cat.GetTableTriggers(n.Table)) > 0 && ex.TriggerExec != nil

	if hasTriggers {
		if err := ex.fireStatementTriggers(n.Table, catalog.TrigBefore, catalog.TrigUpdate); err != nil {
			return nil, err
		}
	}

	type target struct {
		id       slottedpage.ItemID
		row      []tuple.Datum // target table columns only
		childRow []tuple.Datum // full child row (may include FROM columns)
	}
	nTargetCols := len(tableCols)
	var targets []target
	matchIdx := 0
	ex.Cat.SeqScan(n.Table, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if matchIdx < len(child.Rows) {
			childRow := child.Rows[matchIdx]
			// Compare only the target table columns (first nTargetCols of child row).
			if len(childRow) >= nTargetCols && rowsMatch(tup.Columns, childRow[:nTargetCols]) {
				row := make([]tuple.Datum, len(tup.Columns))
				copy(row, tup.Columns)
				targets = append(targets, target{id: id, row: row, childRow: childRow})
				matchIdx++
			}
		}
		return true
	})

	var returningRows [][]tuple.Datum
	// Reject explicit updates to generated columns.
	for _, a := range n.Assignments {
		for _, tc := range tableCols {
			if strings.EqualFold(tc.Name, a.Column) && tc.GeneratedExpr != "" {
				return nil, fmt.Errorf("column %q can only be updated to DEFAULT because it is a generated column", tc.Name)
			}
		}
	}

	childCols := child.Columns
	for _, t := range targets {
		newVals := make([]tuple.Datum, len(t.row))
		copy(newVals, t.row)
		for _, a := range n.Assignments {
			for ci, cname := range n.Columns {
				if cname == a.Column {
					// Use the full child row so FROM columns are accessible.
					r := &planner.Row{Columns: t.childRow, Names: childCols}
					val, err := a.Value.Eval(r)
					if err != nil {
						return nil, err
					}
					newVals[ci] = val
					break
				}
			}
		}

		// Recompute GENERATED ALWAYS AS (expr) STORED columns.
		ex.evalGeneratedColumns(tableCols, colNames, newVals)

		if hasTriggers {
			oldMap := rowToMap(colNames, t.row)
			newMap := rowToMap(colNames, newVals)
			modifiedNew, err := ex.fireTriggers(n.Table, catalog.TrigBefore, catalog.TrigUpdate, colNames, newMap, oldMap)
			if err != nil {
				return nil, err
			}
			if modifiedNew == nil {
				continue // trigger suppressed the update
			}
			newVals = mapToRow(colNames, modifiedNew)
		}

		// Validate NOT NULL constraints.
		if err := validateNotNull(tableCols, newVals); err != nil {
			return nil, err
		}

		// Validate domain/enum constraints.
		if err := ex.validateCustomTypes(tableCols, newVals); err != nil {
			return nil, err
		}

		// Validate CHECK constraints.
		if err := ex.validateCheckConstraints(n.Table, colNames, newVals); err != nil {
			return nil, err
		}

		// Validate FOREIGN KEY constraints (child side).
		if err := ex.validateForeignKeys(n.Table, colNames, newVals); err != nil {
			return nil, err
		}

		oldCopy := make([]tuple.Datum, len(t.row))
		copy(oldCopy, t.row)
		newID, _ := ex.Cat.Update(n.Table, t.id, newVals)
		ex.RecordMutation(Mutation{Kind: MutUpdate, Table: n.Table, ItemID: t.id, NewItemID: newID, OldValues: oldCopy})

		// Enforce FK actions on parent side (CASCADE, SET NULL, etc.).
		if err := ex.enforceForeignKeyOnUpdate(n.Table, colNames, t.row, newVals); err != nil {
			return nil, err
		}

		// Evaluate RETURNING against the new row values.
		if len(n.ReturningExprs) > 0 {
			row := &planner.Row{Columns: newVals, Names: colNames}
			retRow := make([]tuple.Datum, len(n.ReturningExprs))
			for i, expr := range n.ReturningExprs {
				val, err := expr.Eval(row)
				if err != nil {
					return nil, fmt.Errorf("executor: RETURNING eval: %w", err)
				}
				retRow[i] = val
			}
			returningRows = append(returningRows, retRow)
		}

		if hasTriggers {
			oldMap := rowToMap(colNames, t.row)
			newMap := rowToMap(colNames, newVals)
			if _, err := ex.fireTriggers(n.Table, catalog.TrigAfter, catalog.TrigUpdate, colNames, newMap, oldMap); err != nil {
				return nil, err
			}
		}
	}

	if hasTriggers {
		if err := ex.fireStatementTriggers(n.Table, catalog.TrigAfter, catalog.TrigUpdate); err != nil {
			return nil, err
		}
	}

	if len(n.ReturningExprs) > 0 {
		return &Result{
			Columns:      n.ReturningNames,
			Rows:         returningRows,
			RowsAffected: int64(len(targets)),
			Message:      fmt.Sprintf("UPDATE %d", len(targets)),
		}, nil
	}
	return &Result{RowsAffected: int64(len(targets)), Message: fmt.Sprintf("UPDATE %d", len(targets))}, nil
}

func (ex *Executor) execCreateTable(n *planner.PhysCreateTable) (*Result, error) {
	cols := make([]catalog.ColumnDef, len(n.Columns))
	for i, c := range n.Columns {
		cols[i] = catalog.ColumnDef{Name: c.Name, Type: c.Type, TypeName: c.TypeName, Typmod: c.Typmod, NotNull: c.NotNull, DefaultExpr: c.DefaultExpr, GeneratedExpr: c.GeneratedExpr}
	}
	// Set the owner to the current session user.
	var ownerOID int32
	if ex.CurrentUser != "" {
		role, _ := ex.Cat.FindRole(ex.CurrentUser)
		if role != nil {
			ownerOID = role.OID
		}
	}
	_, err := ex.Cat.CreateTableInSchema(n.Table, cols, ownerOID, n.Schema)
	if err != nil {
		return nil, err
	}

	// Auto-create unique btree indexes for PRIMARY KEY and UNIQUE columns.
	for _, c := range n.Columns {
		if c.PrimaryKey {
			idxName := fmt.Sprintf("%s_pkey", n.Table)
			ex.Cat.CreateIndex(idxName, n.Table, c.Name, "btree")
		} else if c.Unique {
			idxName := fmt.Sprintf("%s_%s_key", n.Table, c.Name)
			ex.Cat.CreateIndex(idxName, n.Table, c.Name, "btree")
		}
	}

	// Register CHECK constraints.
	for _, c := range n.Columns {
		if c.CheckExpr != "" {
			name := c.CheckName
			if name == "" {
				name = fmt.Sprintf("%s_%s_check", n.Table, c.Name)
			}
			ex.Cat.AddCheckConstraint(catalog.CheckConstraint{
				Name: name, Table: n.Table, Expr: c.CheckExpr,
			})
		}
	}

	// Register FOREIGN KEY constraints.
	for _, fk := range n.ForeignKeys {
		name := fk.Name
		if name == "" {
			name = fmt.Sprintf("%s_%s_fkey", n.Table, strings.Join(fk.Columns, "_"))
		}
		ex.Cat.AddForeignKey(catalog.ForeignKey{
			Name:       name,
			Table:      n.Table,
			Columns:    fk.Columns,
			RefTable:   fk.RefTable,
			RefColumns: fk.RefColumns,
			OnDelete:   parseFKAction(fk.OnDelete),
			OnUpdate:   parseFKAction(fk.OnUpdate),
		})
	}

	// Track temporary tables for session cleanup.
	if n.IsTemp {
		ex.Cat.TempTables[n.Table] = true
	}

	// Register partition metadata for partitioned tables.
	if n.PartitionStrategy != "" {
		ex.Cat.Partitions[n.Table] = &catalog.PartitionInfo{
			Strategy: n.PartitionStrategy,
			KeyCols:  n.PartitionKeyCols,
		}
	}

	return &Result{Message: fmt.Sprintf("CREATE TABLE %s", n.Table)}, nil
}

// execInsertPartitioned routes INSERT rows to the correct child partition.
func (ex *Executor) execInsertPartitioned(n *planner.PhysInsert, pinfo *catalog.PartitionInfo) (*Result, error) {
	parentCols := ex.getTableColumns(n.Table)
	parentColNames := make([]string, len(parentCols))
	for i, c := range parentCols {
		parentColNames[i] = c.Name
	}

	// Find the partition key column index in the parent table.
	keyColIdxs := make([]int, len(pinfo.KeyCols))
	for k, keyCol := range pinfo.KeyCols {
		found := false
		for i, cn := range parentColNames {
			if strings.EqualFold(cn, keyCol) {
				keyColIdxs[k] = i
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("partition key column %q not found in table %q", keyCol, n.Table)
		}
	}

	// If explicit column list, remap key index.
	if len(n.Columns) > 0 {
		for k, keyCol := range pinfo.KeyCols {
			found := false
			for i, cn := range n.Columns {
				if strings.EqualFold(cn, keyCol) {
					keyColIdxs[k] = i
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("partition key column %q must be specified in INSERT", keyCol)
			}
		}
	}

	var totalCount int64
	for _, rowExprs := range n.Values {
		provided := make([]tuple.Datum, len(rowExprs))
		for i, expr := range rowExprs {
			val, err := expr.Eval(&planner.Row{})
			if err != nil {
				return nil, err
			}
			provided[i] = val
		}

		// Determine partition key value(s).
		keyVals := make([]string, len(keyColIdxs))
		for k, idx := range keyColIdxs {
			if idx < len(provided) {
				keyVals[k] = datumToStringVal(provided[idx])
			}
		}

		// Find matching child partition.
		childTable := ""
		for _, child := range pinfo.Children {
			if matchesPartition(pinfo.Strategy, child, keyVals) {
				childTable = child.TableName
				break
			}
		}
		if childTable == "" {
			// Check for a default partition.
			for _, child := range pinfo.Children {
				if child.BoundType == "default" {
					childTable = child.TableName
					break
				}
			}
		}
		if childTable == "" {
			return nil, fmt.Errorf("no partition of relation %q found for row", n.Table)
		}

		// Redirect the single row to the child table.
		childInsert := &planner.PhysInsert{
			Table:   childTable,
			Columns: n.Columns,
			Values:  [][]planner.Expr{rowExprs},
		}
		r, err := ex.execInsert(childInsert)
		if err != nil {
			return nil, err
		}
		totalCount += r.RowsAffected
	}

	return &Result{
		Message:      fmt.Sprintf("INSERT 0 %d", totalCount),
		RowsAffected: totalCount,
	}, nil
}

// matchesPartition checks if key values match a child partition's bounds.
func matchesPartition(strategy string, child catalog.PartitionChild, keyVals []string) bool {
	switch strategy {
	case "list":
		if child.BoundType == "default" {
			return false // default is checked as fallback
		}
		if len(keyVals) == 0 {
			return false
		}
		for _, lv := range child.ListValues {
			if strings.EqualFold(strings.TrimSpace(lv), strings.TrimSpace(keyVals[0])) {
				return true
			}
		}
	case "range":
		if child.BoundType == "default" {
			return false
		}
		if len(keyVals) == 0 || len(child.RangeFrom) == 0 || len(child.RangeTo) == 0 {
			return false
		}
		// Simple string comparison for the first key column.
		val := strings.TrimSpace(keyVals[0])
		from := strings.TrimSpace(child.RangeFrom[0])
		to := strings.TrimSpace(child.RangeTo[0])
		return val >= from && val < to
	case "hash":
		// Hash partitioning: not commonly used, accept any child for now.
		return true
	}
	return false
}

func parseFKAction(s string) catalog.ForeignKeyAction {
	switch strings.ToUpper(s) {
	case "CASCADE":
		return catalog.FKActionCascade
	case "SET NULL":
		return catalog.FKActionSetNull
	case "SET DEFAULT":
		return catalog.FKActionSetDefault
	case "RESTRICT":
		return catalog.FKActionRestrict
	default:
		return catalog.FKActionNoAction
	}
}

func (ex *Executor) execCreateIndex(n *planner.PhysCreateIndex) (*Result, error) {
	_, err := ex.Cat.CreateIndex(n.Index, n.Table, n.Column, n.Method)
	if err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("CREATE INDEX %s", n.Index)}, nil
}

// --- Helpers ---

func datumToInt64(d tuple.Datum) (int64, bool) {
	return index.DatumToInt64(d)
}

func datumHashKey(d tuple.Datum) string {
	switch d.Type {
	case tuple.TypeInt32:
		return fmt.Sprintf("i:%d", d.I32)
	case tuple.TypeInt64:
		return fmt.Sprintf("l:%d", d.I64)
	case tuple.TypeText:
		return fmt.Sprintf("t:%s", d.Text)
	case tuple.TypeBool:
		return fmt.Sprintf("b:%v", d.Bool)
	case tuple.TypeFloat64:
		return fmt.Sprintf("f:%g", d.F64)
	default:
		return "null"
	}
}

func resolveColIdx(col *planner.ExprColumn, colNames []string) int {
	for i, name := range colNames {
		if len(name) > 0 {
			parts := splitDot(name)
			colPart := parts[len(parts)-1]
			if col.Table != "" {
				if len(parts) == 2 && equalsCI(parts[0], col.Table) && equalsCI(colPart, col.Column) {
					return i
				}
			} else if equalsCI(colPart, col.Column) {
				return i
			}
		}
	}
	return -1
}

func splitDot(s string) []string {
	for i, c := range s {
		if c == '.' {
			return []string{s[:i], s[i+1:]}
		}
	}
	return []string{s}
}

func equalsCI(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 32
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 32
		}
		if ca != cb {
			return false
		}
	}
	return true
}

func rowsMatch(a, b []tuple.Datum) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Type != b[i].Type {
			// Allow int32/int64 cross-comparison
			ai, aok := datumToInt64(a[i])
			bi, bok := datumToInt64(b[i])
			if aok && bok && ai == bi {
				continue
			}
			return false
		}
		switch a[i].Type {
		case tuple.TypeInt32:
			if a[i].I32 != b[i].I32 {
				return false
			}
		case tuple.TypeInt64:
			if a[i].I64 != b[i].I64 {
				return false
			}
		case tuple.TypeText:
			if a[i].Text != b[i].Text {
				return false
			}
		case tuple.TypeBool:
			if a[i].Bool != b[i].Bool {
				return false
			}
		case tuple.TypeFloat64:
			if a[i].F64 != b[i].F64 {
				return false
			}
		}
	}
	return true
}

func (ex *Executor) execCreateView(n *planner.PhysCreateView) (*Result, error) {
	// Convert planner.ColDef to catalog.ColumnDef.
	cols := make([]catalog.ColumnDef, len(n.Columns))
	for i, c := range n.Columns {
		cols[i] = catalog.ColumnDef{Name: c.Name, Type: c.Type, TypeName: c.TypeName}
	}

	_, err := ex.Cat.CreateView(n.Name, cols, n.Definition)
	if err != nil {
		return nil, err
	}

	return &Result{Message: fmt.Sprintf("CREATE VIEW %s", n.Name)}, nil
}

func (ex *Executor) execCreatePolicy(n *planner.PhysCreatePolicy) (*Result, error) {
	rel, err := ex.Cat.FindRelation(n.Table)
	if err != nil || rel == nil {
		return nil, fmt.Errorf("executor: table %q not found for policy", n.Table)
	}

	policy := &catalog.RLSPolicy{
		Name:       n.Name,
		RelOID:     rel.OID,
		Cmd:        catalog.PolicyCmdFromString(n.Cmd),
		Permissive: n.Permissive,
		Roles:      n.Roles,
		UsingExpr:  n.Using,
		CheckExpr:  n.Check,
	}

	if err := ex.Cat.CreatePolicy(policy); err != nil {
		return nil, err
	}

	return &Result{Message: fmt.Sprintf("CREATE POLICY %s", n.Name)}, nil
}

func (ex *Executor) execEnableRLS(n *planner.PhysEnableRLS) (*Result, error) {
	rel, err := ex.Cat.FindRelation(n.Table)
	if err != nil || rel == nil {
		return nil, fmt.Errorf("executor: table %q not found", n.Table)
	}
	if err := ex.Cat.EnableRLS(rel.OID); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("ALTER TABLE %s", n.Table)}, nil
}

func (ex *Executor) execDisableRLS(n *planner.PhysDisableRLS) (*Result, error) {
	rel, err := ex.Cat.FindRelation(n.Table)
	if err != nil || rel == nil {
		return nil, fmt.Errorf("executor: table %q not found", n.Table)
	}
	if err := ex.Cat.DisableRLS(rel.OID); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("ALTER TABLE %s", n.Table)}, nil
}

// -----------------------------------------------------------------------
// Role management
// -----------------------------------------------------------------------

func (ex *Executor) execCreateRole(n *planner.PhysCreateRole) (*Result, error) {
	role := &catalog.Role{
		Inherit:   true,
		ConnLimit: -1,
	}
	role.Name = n.RoleName

	if v, ok := n.Options["superuser"]; ok {
		role.SuperUser = v.(bool)
	}
	if v, ok := n.Options["createdb"]; ok {
		role.CreateDB = v.(bool)
	}
	if v, ok := n.Options["createrole"]; ok {
		role.CreateRole = v.(bool)
	}
	if v, ok := n.Options["inherit"]; ok {
		role.Inherit = v.(bool)
	}
	if v, ok := n.Options["login"]; ok {
		role.Login = v.(bool)
	}
	if v, ok := n.Options["bypassrls"]; ok {
		role.BypassRLS = v.(bool)
	}
	if v, ok := n.Options["connlimit"]; ok {
		role.ConnLimit = v.(int32)
	}
	if v, ok := n.Options["password"]; ok {
		role.Password = v.(string)
	}

	if err := ex.Cat.CreateRole(role); err != nil {
		return nil, fmt.Errorf("executor: %w", err)
	}

	stmtType := "ROLE"
	if n.StmtType != "" {
		stmtType = n.StmtType
	}
	return &Result{Message: fmt.Sprintf("CREATE %s", stmtType)}, nil
}

func (ex *Executor) execAlterRole(n *planner.PhysAlterRole) (*Result, error) {
	if err := ex.Cat.AlterRole(n.RoleName, n.Options); err != nil {
		return nil, fmt.Errorf("executor: %w", err)
	}
	return &Result{Message: "ALTER ROLE"}, nil
}

func (ex *Executor) execDropRole(n *planner.PhysDropRole) (*Result, error) {
	for _, name := range n.Roles {
		if err := ex.Cat.DropRole(name, n.MissingOk); err != nil {
			return nil, fmt.Errorf("executor: %w", err)
		}
	}
	return &Result{Message: "DROP ROLE"}, nil
}

func (ex *Executor) execGrantRole(n *planner.PhysGrantRole) (*Result, error) {
	for _, grantedRole := range n.GrantedRoles {
		for _, grantee := range n.Grantees {
			if err := ex.Cat.GrantRoleMembership(grantedRole, grantee, n.AdminOption); err != nil {
				return nil, fmt.Errorf("executor: %w", err)
			}
		}
	}
	return &Result{Message: "GRANT ROLE"}, nil
}

func (ex *Executor) execRevokeRole(n *planner.PhysRevokeRole) (*Result, error) {
	for _, revokedRole := range n.RevokedRoles {
		for _, grantee := range n.Grantees {
			if err := ex.Cat.RevokeRoleMembership(revokedRole, grantee); err != nil {
				return nil, fmt.Errorf("executor: %w", err)
			}
		}
	}
	return &Result{Message: "REVOKE ROLE"}, nil
}

func (ex *Executor) resolveGranteeOID(name string) (int32, error) {
	if strings.EqualFold(name, "public") {
		return 0, nil
	}
	role, err := ex.Cat.FindRole(name)
	if err != nil || role == nil {
		return 0, fmt.Errorf("executor: role %q does not exist", name)
	}
	return role.OID, nil
}

func (ex *Executor) execGrantPrivilege(n *planner.PhysGrantPrivilege) (*Result, error) {
	for _, objName := range n.Objects {
		rel, err := ex.Cat.FindRelation(objName)
		if err != nil || rel == nil {
			return nil, fmt.Errorf("executor: relation %q not found", objName)
		}

		for i, p := range n.Privileges {
			priv := catalog.ParsePrivilege(p)
			// Get column list for this privilege (if any).
			var cols []string
			if i < len(n.PrivCols) && len(n.PrivCols[i]) > 0 {
				cols = n.PrivCols[i]
			}

			for _, granteeName := range n.Grantees {
				granteeOID, err := ex.resolveGranteeOID(granteeName)
				if err != nil {
					return nil, err
				}
				ex.Cat.GrantObjectPrivilegeColumns(rel.OID, granteeOID, 0, priv, cols)
			}
		}
	}
	return &Result{Message: "GRANT"}, nil
}

func (ex *Executor) execRevokePrivilege(n *planner.PhysRevokePrivilege) (*Result, error) {
	for _, objName := range n.Objects {
		rel, err := ex.Cat.FindRelation(objName)
		if err != nil || rel == nil {
			return nil, fmt.Errorf("executor: relation %q not found", objName)
		}

		for i, p := range n.Privileges {
			priv := catalog.ParsePrivilege(p)
			var cols []string
			if i < len(n.PrivCols) && len(n.PrivCols[i]) > 0 {
				cols = n.PrivCols[i]
			}

			for _, granteeName := range n.Grantees {
				granteeOID, err := ex.resolveGranteeOID(granteeName)
				if err != nil {
					return nil, err
				}
				ex.Cat.RevokeObjectPrivilegeColumns(rel.OID, granteeOID, 0, priv, cols)
			}
		}
	}
	return &Result{Message: "REVOKE"}, nil
}


// fireTriggers fires matching ROW-level triggers for a table/event/timing combination.
// For BEFORE ROW triggers on INSERT/UPDATE, returns the (possibly modified) NEW row.
func (ex *Executor) fireTriggers(tableName string, timing int, event int, colNames []string, newRow, oldRow map[string]tuple.Datum) (map[string]tuple.Datum, error) {
	return ex.fireTriggersLevel(tableName, timing, event, "ROW", colNames, newRow, oldRow)
}

// fireStatementTriggers fires matching STATEMENT-level triggers.
func (ex *Executor) fireStatementTriggers(tableName string, timing int, event int) error {
	_, err := ex.fireTriggersLevel(tableName, timing, event, "STATEMENT", nil, nil, nil)
	return err
}

func (ex *Executor) fireTriggersLevel(tableName string, timing int, event int, level string, colNames []string, newRow, oldRow map[string]tuple.Datum) (map[string]tuple.Datum, error) {
	if ex.TriggerExec == nil {
		return newRow, nil
	}

	triggers := ex.Cat.GetTableTriggers(tableName)
	if len(triggers) == 0 {
		return newRow, nil
	}

	tgWhen := "BEFORE"
	if timing == catalog.TrigAfter {
		tgWhen = "AFTER"
	}
	tgOp := ""
	switch {
	case event&catalog.TrigInsert != 0:
		tgOp = "INSERT"
	case event&catalog.TrigUpdate != 0:
		tgOp = "UPDATE"
	case event&catalog.TrigDelete != 0:
		tgOp = "DELETE"
	}

	currentNew := newRow
	for _, trig := range triggers {
		if trig.Timing&timing == 0 || trig.Events&event == 0 {
			continue
		}
		if trig.ForEach != level {
			continue
		}
		fn := ex.Cat.FindFunctionByOID(trig.FuncOID)
		if fn == nil {
			continue
		}

		tc := &TriggerContext{
			TgName:     trig.Name,
			TgTable:    tableName,
			TgOp:       tgOp,
			TgWhen:     tgWhen,
			TgLevel:    trig.ForEach,
			TgFuncName: fn.Name,
			TgArgs:     trig.Args,
			NewRow:     currentNew,
			OldRow:     oldRow,
			ColNames:   colNames,
		}

		modifiedNew, err := ex.TriggerExec(fn.Body, tc)
		if err != nil {
			return nil, fmt.Errorf("trigger %q: %w", trig.Name, err)
		}
		// BEFORE ROW triggers can modify NEW.
		if level == "ROW" && timing == catalog.TrigBefore && modifiedNew != nil {
			currentNew = modifiedNew
		}
	}
	return currentNew, nil
}

// rowToMap converts a datum slice to a column-name-keyed map.
func rowToMap(colNames []string, values []tuple.Datum) map[string]tuple.Datum {
	m := make(map[string]tuple.Datum, len(colNames))
	for i, name := range colNames {
		if i < len(values) {
			m[name] = values[i]
		}
	}
	return m
}

// mapToRow converts a column-name-keyed map back to a datum slice.
func mapToRow(colNames []string, m map[string]tuple.Datum) []tuple.Datum {
	row := make([]tuple.Datum, len(colNames))
	for i, name := range colNames {
		if v, ok := m[name]; ok {
			row[i] = v
		} else {
			row[i] = tuple.DNull()
		}
	}
	return row
}

// getTableColumns returns the catalog columns for a table.
func (ex *Executor) getTableColumns(tableName string) []catalog.Column {
	rel, err := ex.Cat.FindRelation(tableName)
	if err != nil || rel == nil {
		return nil
	}
	cols, err := ex.Cat.GetColumns(rel.OID)
	if err != nil {
		return nil
	}
	return cols
}

// evalDefault evaluates a column's DEFAULT expression and returns the
// resulting datum. Returns NULL if the column has no default.
func (ex *Executor) evalDefault(col catalog.Column) tuple.Datum {
	if col.DefaultExpr == "" {
		return tuple.DNull()
	}
	// Parse the default expression as a SELECT expression.
	sql := "SELECT " + col.DefaultExpr
	stmts, err := parser.Parse(strings.NewReader(sql), nil)
	if err != nil || len(stmts) == 0 {
		return tuple.DNull()
	}
	sel, ok := stmts[0].Stmt.(*parser.SelectStmt)
	if !ok || len(sel.TargetList) == 0 {
		return tuple.DNull()
	}
	// Use the analyzer to transform the expression, then convert to
	// an executable Expr and evaluate it.
	a := &planner.Analyzer{Cat: ex.Cat}
	analyzed, err := a.TransformExpr(sel.TargetList[0].Val)
	if err != nil {
		return tuple.DNull()
	}
	expr := planner.AnalyzedToExpr(analyzed, nil)
	val, err := expr.Eval(&planner.Row{})
	if err != nil {
		return tuple.DNull()
	}
	return val
}

// evalGeneratedColumns computes GENERATED ALWAYS AS (expr) STORED columns.
// It sets up a temporary range table so the analyzer can resolve column
// references, then evaluates the expression against the current row.
func (ex *Executor) evalGeneratedColumns(cols []catalog.Column, colNames []string, values []tuple.Datum) {
	for i, col := range cols {
		if col.GeneratedExpr == "" || i >= len(values) {
			continue
		}
		sql := "SELECT " + col.GeneratedExpr
		stmts, err := parser.Parse(strings.NewReader(sql), nil)
		if err != nil || len(stmts) == 0 {
			continue
		}
		sel, ok := stmts[0].Stmt.(*parser.SelectStmt)
		if !ok || len(sel.TargetList) == 0 {
			continue
		}
		// Build RTE columns from the table's catalog columns so the
		// analyzer can resolve column references in the expression.
		rteCols := make([]planner.RTEColumn, len(cols))
		for j, c := range cols {
			rteCols[j] = planner.RTEColumn{
				Name:   c.Name,
				Type:   tuple.DatumType(c.Type),
				ColNum: int32(j + 1),
			}
		}
		a := planner.NewAnalyzerWithRTE(ex.Cat, rteCols)
		analyzed, err := a.TransformExpr(sel.TargetList[0].Val)
		if err != nil {
			continue
		}
		expr := planner.AnalyzedToExpr(analyzed, nil)
		row := &planner.Row{Columns: values, Names: colNames}
		val, err := expr.Eval(row)
		if err != nil {
			continue
		}
		values[i] = val
	}
}

// validateNotNull checks that NOT NULL columns do not contain NULL values.
// coerceInsertValues converts text values to the target column type
// when the column expects a typed datum (date, timestamp, numeric, json, uuid).
func coerceInsertValues(cols []catalog.Column, values []tuple.Datum) {
	for i, col := range cols {
		if i >= len(values) || values[i].Type == tuple.TypeNull {
			continue
		}
		colType := tuple.DatumType(col.Type)
		if values[i].Type == colType {
			continue // already correct type
		}
		switch colType {
		case tuple.TypeDate:
			if values[i].Type == tuple.TypeText {
				t, err := parseTimestampForCoerce(values[i].Text)
				if err == nil {
					values[i] = tuple.DDate(t.Unix() / 86400)
				}
			} else if values[i].Type == tuple.TypeTimestamp {
				// Truncate timestamp to date (days since epoch).
				values[i] = tuple.DDate(values[i].I64 / 1_000_000 / 86400)
			}
		case tuple.TypeTimestamp:
			if values[i].Type == tuple.TypeText {
				t, err := parseTimestampForCoerce(values[i].Text)
				if err == nil {
					values[i] = tuple.DTimestamp(t.UnixMicro())
				}
			} else if values[i].Type == tuple.TypeDate {
				// Promote date to timestamp (midnight).
				values[i] = tuple.DTimestamp(values[i].I64 * 86400 * 1_000_000)
			}
		case tuple.TypeNumeric:
			if values[i].Type == tuple.TypeText || values[i].Type == tuple.TypeInt32 ||
				values[i].Type == tuple.TypeInt64 || values[i].Type == tuple.TypeFloat64 {
				values[i] = tuple.DNumeric(datumToStringForCoerce(values[i]))
			}
			// Enforce NUMERIC(p,s) precision/scale if specified.
			if values[i].Type == tuple.TypeNumeric && col.Typmod >= 4 {
				values[i] = enforceNumericPrecision(values[i], col.Typmod)
			}
		case tuple.TypeJSON:
			if values[i].Type == tuple.TypeText {
				s := values[i].Text
				if !json.Valid([]byte(s)) {
					continue // leave as text; catalog type check will catch it
				}
				values[i] = tuple.DJSON(s)
			}
		case tuple.TypeUUID:
			if values[i].Type == tuple.TypeText {
				values[i] = tuple.DUUID(strings.TrimSpace(values[i].Text))
			}
		case tuple.TypeInterval:
			if values[i].Type == tuple.TypeText {
				months, us, err := parseIntervalForCoerce(values[i].Text)
				if err == nil {
					values[i] = tuple.DInterval(months, us)
				}
			}
		case tuple.TypeBytea:
			if values[i].Type == tuple.TypeText {
				s := values[i].Text
				if strings.HasPrefix(s, "\\x") {
					values[i] = tuple.DBytea(s)
				} else {
					values[i] = tuple.DBytea("\\x" + hex.EncodeToString([]byte(s)))
				}
			}
		case tuple.TypeMoney:
			if values[i].Type == tuple.TypeText {
				s := strings.TrimSpace(values[i].Text)
				s = strings.TrimPrefix(s, "$")
				s = strings.ReplaceAll(s, ",", "")
				if f, err := strconv.ParseFloat(s, 64); err == nil {
					values[i] = tuple.DMoney(int64(math.Round(f * 100)))
				}
			} else if values[i].Type == tuple.TypeInt64 || values[i].Type == tuple.TypeInt32 {
				var v int64
				if values[i].Type == tuple.TypeInt32 {
					v = int64(values[i].I32)
				} else {
					v = values[i].I64
				}
				values[i] = tuple.DMoney(v * 100)
			} else if values[i].Type == tuple.TypeFloat64 {
				values[i] = tuple.DMoney(int64(values[i].F64 * 100))
			}
		case tuple.TypeArray:
			if values[i].Type == tuple.TypeText {
				values[i] = tuple.DArray(values[i].Text)
			}
		}
	}
}

// parseTimestampForCoerce parses common date/timestamp formats.
func parseTimestampForCoerce(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	formats := []string{
		"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05",
		"2006-01-02",
		"01/02/2006",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp format: %q", s)
}

// datumToStringForCoerce converts a datum to its string representation for NUMERIC coercion.
func datumToStringForCoerce(d tuple.Datum) string {
	switch d.Type {
	case tuple.TypeInt32:
		return fmt.Sprintf("%d", d.I32)
	case tuple.TypeInt64:
		return fmt.Sprintf("%d", d.I64)
	case tuple.TypeFloat64:
		return fmt.Sprintf("%g", d.F64)
	case tuple.TypeText:
		return d.Text
	default:
		return ""
	}
}

// enforceNumericPrecision rounds a NUMERIC datum to the specified precision/scale
// encoded in the typmod. If the value exceeds the precision, it is truncated.
func enforceNumericPrecision(d tuple.Datum, typmod int32) tuple.Datum {
	if d.Type != tuple.TypeNumeric || typmod < 4 {
		return d
	}
	tm := typmod - 4
	scale := int(tm & 0xFFFF)

	// Parse the numeric string and round to the specified scale.
	f, _, err := new(big.Float).SetPrec(128).Parse(d.Text, 10)
	if err != nil {
		return d
	}

	// Multiply by 10^scale, round, divide back.
	pow := new(big.Float).SetPrec(128).SetInt64(1)
	for i := 0; i < scale; i++ {
		pow.Mul(pow, new(big.Float).SetInt64(10))
	}
	f.Mul(f, pow)

	// Round to integer.
	intVal, _ := f.Int(nil)
	f.SetInt(intVal)
	f.Quo(f, pow)

	// Format with exact scale digits.
	result := f.Text('f', scale)
	return tuple.DNumeric(result)
}

// parseIntervalForCoerce parses a PostgreSQL interval string for INSERT coercion.
func parseIntervalForCoerce(s string) (int32, int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, 0, fmt.Errorf("empty interval")
	}
	// Try time-only format.
	if strings.Contains(s, ":") && !strings.Contains(s, " ") {
		parts := strings.Split(s, ":")
		if len(parts) >= 2 {
			h, _ := strconv.Atoi(parts[0])
			m, _ := strconv.Atoi(parts[1])
			var sec float64
			if len(parts) == 3 {
				sec, _ = strconv.ParseFloat(parts[2], 64)
			}
			us := int64(h)*3600*1e6 + int64(m)*60*1e6 + int64(sec*1e6)
			return 0, us, nil
		}
	}
	// Parse "N unit" pairs.
	var months int32
	var us int64
	fields := strings.Fields(s)
	for i := 0; i+1 < len(fields); i += 2 {
		val, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			return 0, 0, err
		}
		unit := strings.ToLower(strings.TrimSuffix(fields[i+1], ","))
		switch {
		case strings.HasPrefix(unit, "year"):
			months += int32(val) * 12
		case strings.HasPrefix(unit, "mon"):
			months += int32(val)
		case strings.HasPrefix(unit, "week"):
			us += int64(val * 7 * 24 * 3600 * 1e6)
		case strings.HasPrefix(unit, "day"):
			us += int64(val * 24 * 3600 * 1e6)
		case strings.HasPrefix(unit, "hour"):
			us += int64(val * 3600 * 1e6)
		case strings.HasPrefix(unit, "min"):
			us += int64(val * 60 * 1e6)
		case strings.HasPrefix(unit, "sec"):
			us += int64(val * 1e6)
		}
	}
	return months, us, nil
}

func validateNotNull(cols []catalog.Column, values []tuple.Datum) error {
	for i, col := range cols {
		if !col.NotNull {
			continue
		}
		if i >= len(values) || values[i].Type == tuple.TypeNull {
			return fmt.Errorf("null value in column %q violates not-null constraint", col.Name)
		}
	}
	return nil
}

// validateCheckConstraints evaluates CHECK expressions against a row.
func (ex *Executor) validateCheckConstraints(table string, colNames []string, values []tuple.Datum) error {
	checks := ex.Cat.GetCheckConstraints(table)
	if len(checks) == 0 {
		return nil
	}
	for _, cc := range checks {
		// Parse the CHECK expression as a SELECT expression.
		sql := "SELECT " + cc.Expr
		stmts, err := parser.Parse(strings.NewReader(sql), nil)
		if err != nil {
			return fmt.Errorf("check constraint %q: parse error: %w", cc.Name, err)
		}
		if len(stmts) == 0 {
			continue
		}
		sel, ok := stmts[0].Stmt.(*parser.SelectStmt)
		if !ok || len(sel.TargetList) == 0 {
			continue
		}

		// Build a minimal analyzer with the table's columns in scope.
		a := &planner.Analyzer{Cat: ex.Cat}
		a.AddRangeTableEntry(table, "")

		analyzed, err := a.TransformExpr(sel.TargetList[0].Val)
		if err != nil {
			return fmt.Errorf("check constraint %q: analyze error: %w", cc.Name, err)
		}

		expr := planner.AnalyzedToExprPublic(analyzed, a.GetRangeTable())

		// Build a row for evaluation.
		names := make([]string, len(colNames))
		for i, cn := range colNames {
			names[i] = table + "." + cn
		}
		row := &planner.Row{Columns: values, Names: names}

		val, err := expr.Eval(row)
		if err != nil {
			return fmt.Errorf("check constraint %q: eval error: %w", cc.Name, err)
		}
		// PostgreSQL semantics: CHECK passes unless the result is explicitly false.
		// NULL results (e.g., from NULL column values) are treated as passing.
		if val.Type == tuple.TypeBool && !val.Bool {
			return fmt.Errorf("new row violates check constraint %q", cc.Name)
		}
	}
	return nil
}

// validateForeignKeys checks that FK column values reference existing rows in the parent table.
func (ex *Executor) validateForeignKeys(table string, colNames []string, values []tuple.Datum) error {
	fks := ex.Cat.GetForeignKeys(table)
	if len(fks) == 0 {
		return nil
	}
	for _, fk := range fks {
		// Get FK column values from the row.
		fkVals := make([]tuple.Datum, len(fk.Columns))
		allNull := true
		for i, fkCol := range fk.Columns {
			for j, cn := range colNames {
				if strings.EqualFold(fkCol, cn) && j < len(values) {
					fkVals[i] = values[j]
					if fkVals[i].Type != tuple.TypeNull {
						allNull = false
					}
					break
				}
			}
		}
		// NULL FK values are allowed (no reference to check).
		if allNull {
			continue
		}

		// Scan the parent table for a matching row.
		refColNames := ex.getTableColNames(fk.RefTable)
		found := false
		ex.Cat.SeqScan(fk.RefTable, func(_ slottedpage.ItemID, tup *tuple.Tuple) bool {
			match := true
			for i, refCol := range fk.RefColumns {
				for j, cn := range refColNames {
					if strings.EqualFold(refCol, cn) && j < len(tup.Columns) {
						if planner.CompareDatums(fkVals[i], tup.Columns[j]) != 0 {
							match = false
						}
						break
					}
				}
				if !match {
					break
				}
			}
			if match {
				found = true
				return false // stop scanning
			}
			return true
		})
		if !found {
			return fmt.Errorf("insert or update on table %q violates foreign key constraint %q", table, fk.Name)
		}
	}
	return nil
}

// enforceForeignKeyOnDelete handles FK actions when a row is deleted from a parent table.
func (ex *Executor) enforceForeignKeyOnDelete(table string, colNames []string, deletedRow []tuple.Datum) error {
	refs := ex.Cat.GetReferencingForeignKeys(table)
	if len(refs) == 0 {
		return nil
	}
	for _, fk := range refs {
		// Get the parent key values from the deleted row.
		parentVals := make([]tuple.Datum, len(fk.RefColumns))
		for i, refCol := range fk.RefColumns {
			for j, cn := range colNames {
				if strings.EqualFold(refCol, cn) && j < len(deletedRow) {
					parentVals[i] = deletedRow[j]
					break
				}
			}
		}

		// Find child rows that reference this parent row.
		childColNames := ex.getTableColNames(fk.Table)
		type childMatch struct {
			id  slottedpage.ItemID
			row []tuple.Datum
		}
		var matches []childMatch
		ex.Cat.SeqScan(fk.Table, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
			match := true
			for i, fkCol := range fk.Columns {
				for j, cn := range childColNames {
					if strings.EqualFold(fkCol, cn) && j < len(tup.Columns) {
						if planner.CompareDatums(parentVals[i], tup.Columns[j]) != 0 {
							match = false
						}
						break
					}
				}
				if !match {
					break
				}
			}
			if match {
				row := make([]tuple.Datum, len(tup.Columns))
				copy(row, tup.Columns)
				matches = append(matches, childMatch{id: id, row: row})
			}
			return true
		})

		if len(matches) == 0 {
			continue
		}

		switch fk.OnDelete {
		case catalog.FKActionCascade:
			for _, m := range matches {
				ex.RecordMutation(Mutation{Kind: MutDelete, Table: fk.Table, ItemID: m.id, OldValues: m.row})
				ex.Cat.Delete(fk.Table, m.id)
			}
		case catalog.FKActionSetNull:
			for _, m := range matches {
				newVals := make([]tuple.Datum, len(m.row))
				copy(newVals, m.row)
				for _, fkCol := range fk.Columns {
					for j, cn := range childColNames {
						if strings.EqualFold(fkCol, cn) {
							newVals[j] = tuple.DNull()
							break
						}
					}
				}
				oldCopy := make([]tuple.Datum, len(m.row))
				copy(oldCopy, m.row)
				newID, _ := ex.Cat.Update(fk.Table, m.id, newVals)
				ex.RecordMutation(Mutation{Kind: MutUpdate, Table: fk.Table, ItemID: m.id, NewItemID: newID, OldValues: oldCopy})
			}
		case catalog.FKActionSetDefault:
			childCols := ex.getTableColumns(fk.Table)
			for _, m := range matches {
				newVals := make([]tuple.Datum, len(m.row))
				copy(newVals, m.row)
				for _, fkCol := range fk.Columns {
					for j, cn := range childColNames {
						if strings.EqualFold(fkCol, cn) {
							newVals[j] = ex.evalDefault(childCols[j])
							break
						}
					}
				}
				oldCopy := make([]tuple.Datum, len(m.row))
				copy(oldCopy, m.row)
				newID, _ := ex.Cat.Update(fk.Table, m.id, newVals)
				ex.RecordMutation(Mutation{Kind: MutUpdate, Table: fk.Table, ItemID: m.id, NewItemID: newID, OldValues: oldCopy})
			}
		default: // NO ACTION / RESTRICT
			return fmt.Errorf("update or delete on table %q violates foreign key constraint %q on table %q",
				table, fk.Name, fk.Table)
		}
	}
	return nil
}

// enforceForeignKeyOnUpdate handles FK actions when a parent PK column is updated.
func (ex *Executor) enforceForeignKeyOnUpdate(table string, colNames []string, oldRow, newRow []tuple.Datum) error {
	refs := ex.Cat.GetReferencingForeignKeys(table)
	if len(refs) == 0 {
		return nil
	}
	for _, fk := range refs {
		// Check if any referenced columns actually changed.
		changed := false
		parentOldVals := make([]tuple.Datum, len(fk.RefColumns))
		parentNewVals := make([]tuple.Datum, len(fk.RefColumns))
		for i, refCol := range fk.RefColumns {
			for j, cn := range colNames {
				if strings.EqualFold(refCol, cn) && j < len(oldRow) {
					parentOldVals[i] = oldRow[j]
					parentNewVals[i] = newRow[j]
					if planner.CompareDatums(oldRow[j], newRow[j]) != 0 {
						changed = true
					}
					break
				}
			}
		}
		if !changed {
			continue
		}

		// Find child rows referencing the old parent values.
		childColNames := ex.getTableColNames(fk.Table)
		type childMatch struct {
			id  slottedpage.ItemID
			row []tuple.Datum
		}
		var matches []childMatch
		ex.Cat.SeqScan(fk.Table, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
			match := true
			for i, fkCol := range fk.Columns {
				for j, cn := range childColNames {
					if strings.EqualFold(fkCol, cn) && j < len(tup.Columns) {
						if planner.CompareDatums(parentOldVals[i], tup.Columns[j]) != 0 {
							match = false
						}
						break
					}
				}
				if !match {
					break
				}
			}
			if match {
				row := make([]tuple.Datum, len(tup.Columns))
				copy(row, tup.Columns)
				matches = append(matches, childMatch{id: id, row: row})
			}
			return true
		})

		if len(matches) == 0 {
			continue
		}

		switch fk.OnUpdate {
		case catalog.FKActionCascade:
			for _, m := range matches {
				newVals := make([]tuple.Datum, len(m.row))
				copy(newVals, m.row)
				for i, fkCol := range fk.Columns {
					for j, cn := range childColNames {
						if strings.EqualFold(fkCol, cn) {
							newVals[j] = parentNewVals[i]
							break
						}
					}
				}
				oldCopy := make([]tuple.Datum, len(m.row))
				copy(oldCopy, m.row)
				newID, _ := ex.Cat.Update(fk.Table, m.id, newVals)
				ex.RecordMutation(Mutation{Kind: MutUpdate, Table: fk.Table, ItemID: m.id, NewItemID: newID, OldValues: oldCopy})
			}
		case catalog.FKActionSetNull:
			for _, m := range matches {
				newVals := make([]tuple.Datum, len(m.row))
				copy(newVals, m.row)
				for _, fkCol := range fk.Columns {
					for j, cn := range childColNames {
						if strings.EqualFold(fkCol, cn) {
							newVals[j] = tuple.DNull()
							break
						}
					}
				}
				oldCopy := make([]tuple.Datum, len(m.row))
				copy(oldCopy, m.row)
				newID, _ := ex.Cat.Update(fk.Table, m.id, newVals)
				ex.RecordMutation(Mutation{Kind: MutUpdate, Table: fk.Table, ItemID: m.id, NewItemID: newID, OldValues: oldCopy})
			}
		default: // NO ACTION / RESTRICT
			return fmt.Errorf("update or delete on table %q violates foreign key constraint %q on table %q",
				table, fk.Name, fk.Table)
		}
	}
	return nil
}

// validateCustomTypes checks each value against domain/enum constraints.
func (ex *Executor) validateCustomTypes(cols []catalog.Column, values []tuple.Datum) error {
	for i, col := range cols {
		if i >= len(values) {
			break
		}
		ct := ex.Cat.FindTypeByOID(col.TypeOID)
		if ct == nil {
			continue
		}
		switch ct.TypType {
		case "d":
			// Domain: validate NOT NULL and CHECK constraints.
			err := ex.Cat.ValidateDomainValue(ct.Name, values[i], func(sql string) error {
				if ex.SQLExec == nil {
					return nil // no SQL executor available, skip CHECK
				}
				res, execErr := ex.SQLExec(sql)
				if execErr != nil {
					return execErr
				}
				if len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
					return fmt.Errorf("check expression returned no result")
				}
				d := res.Rows[0][0]
				if d.Type == tuple.TypeBool && !d.Bool {
					return fmt.Errorf("check constraint is not satisfied")
				}
				return nil
			})
			if err != nil {
				return err
			}
		case "e":
			// Enum: check that the value is one of the allowed values.
			if values[i].Type == tuple.TypeNull {
				continue
			}
			found := false
			for _, ev := range ct.EnumVals {
				if ev == values[i].Text {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("invalid input value for enum %q: %q", ct.Name, values[i].Text)
			}
		}
	}
	return nil
}

// getTableColNames returns the column names for a table.
func (ex *Executor) getTableColNames(tableName string) []string {
	rel, err := ex.Cat.FindRelation(tableName)
	if err != nil || rel == nil {
		return nil
	}
	cols, err := ex.Cat.GetColumns(rel.OID)
	if err != nil {
		return nil
	}
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}

func (ex *Executor) execCreateFunction(n *planner.PhysCreateFunction) (*Result, error) {
	err := ex.Cat.CreateFunction(&catalog.FuncDef{
		Name:       n.Name,
		Language:   n.Language,
		Body:       n.Body,
		ReturnType: n.ReturnType,
		ParamNames: n.ParamNames,
		ParamTypes: n.ParamTypes,
		Replace:    n.Replace,
	})
	if err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("CREATE FUNCTION %s", n.Name)}, nil
}

func (ex *Executor) execCreateTrigger(n *planner.PhysCreateTrigger) (*Result, error) {
	// Resolve the table OID.
	rel, err := ex.Cat.FindRelation(n.Table)
	if err != nil || rel == nil {
		return nil, fmt.Errorf("executor: table %q not found", n.Table)
	}

	// Resolve the function OID.
	fn := ex.Cat.FindFunction(n.FuncName)
	if fn == nil {
		return nil, fmt.Errorf("executor: function %q not found", n.FuncName)
	}

	err = ex.Cat.CreateTrigger(&catalog.TriggerDef{
		Name:     n.TrigName,
		TableOID: rel.OID,
		FuncOID:  fn.OID,
		Timing:   n.Timing,
		Events:   n.Events,
		ForEach:  n.ForEach,
		Args:     n.Args,
	})
	if err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("CREATE TRIGGER %s", n.TrigName)}, nil
}

func (ex *Executor) execDropFunction(n *planner.PhysDropFunction) (*Result, error) {
	if err := ex.Cat.DropFunction(n.Name, n.MissingOk); err != nil {
		return nil, err
	}
	return &Result{Message: "DROP FUNCTION"}, nil
}

func (ex *Executor) execAlterFunction(n *planner.PhysAlterFunction) (*Result, error) {
	if n.NewName != "" {
		if err := ex.Cat.AlterFunctionRename(n.Name, n.NewName); err != nil {
			return nil, err
		}
	}
	if n.NewOwner != "" {
		if err := ex.Cat.AlterFunctionOwner(n.Name, n.NewOwner); err != nil {
			return nil, err
		}
	}
	return &Result{Message: "ALTER FUNCTION"}, nil
}

func (ex *Executor) execCreateDomain(n *planner.PhysCreateDomain) (*Result, error) {
	baseType := planner.MapSQLType(n.BaseType)
	if err := ex.Cat.CreateDomain(n.Name, baseType, n.NotNull, n.CheckExpr); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("CREATE DOMAIN %s", n.Name)}, nil
}

func (ex *Executor) execCreateEnum(n *planner.PhysCreateEnum) (*Result, error) {
	if err := ex.Cat.CreateEnum(n.Name, n.Vals); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("CREATE TYPE %s", n.Name)}, nil
}

func (ex *Executor) execDropType(n *planner.PhysDropType) (*Result, error) {
	if err := ex.Cat.DropType(n.Name, n.MissingOk); err != nil {
		return nil, err
	}
	return &Result{Message: "DROP TYPE"}, nil
}

func (ex *Executor) execAlterEnum(n *planner.PhysAlterEnum) (*Result, error) {
	if err := ex.Cat.AlterEnumAddValue(n.Name, n.NewVal); err != nil {
		return nil, err
	}
	return &Result{Message: "ALTER TYPE"}, nil
}

func (ex *Executor) execCreateSchema(n *planner.PhysCreateSchema) (*Result, error) {
	var ownerOID int32
	if n.AuthRole != "" {
		role, _ := ex.Cat.FindRole(n.AuthRole)
		if role != nil {
			ownerOID = role.OID
		}
	} else if ex.CurrentUser != "" {
		role, _ := ex.Cat.FindRole(ex.CurrentUser)
		if role != nil {
			ownerOID = role.OID
		}
	}
	if err := ex.Cat.CreateSchema(n.Name, n.IfNotExists, ownerOID); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("CREATE SCHEMA %s", n.Name)}, nil
}

func (ex *Executor) execDropSchema(n *planner.PhysDropSchema) (*Result, error) {
	if err := ex.Cat.DropSchema(n.Name, n.MissingOk, n.Cascade); err != nil {
		return nil, err
	}
	return &Result{Message: "DROP SCHEMA"}, nil
}

func (ex *Executor) execDropTrigger(n *planner.PhysDropTrigger) (*Result, error) {
	if err := ex.Cat.DropTrigger(n.TrigName, n.Table, n.MissingOk); err != nil {
		return nil, err
	}
	return &Result{Message: "DROP TRIGGER"}, nil
}

func (ex *Executor) execTruncate(n *planner.PhysTruncate) (*Result, error) {
	if err := ex.Cat.TruncateTable(n.Table); err != nil {
		return nil, err
	}
	return &Result{Message: "TRUNCATE TABLE"}, nil
}

func (ex *Executor) execDropIndex(n *planner.PhysDropIndex) (*Result, error) {
	if err := ex.Cat.DropIndex(n.Name, n.MissingOk); err != nil {
		return nil, err
	}
	return &Result{Message: "DROP INDEX"}, nil
}

func (ex *Executor) execDropTable(n *planner.PhysDropTable) (*Result, error) {
	if err := ex.Cat.DropTable(n.Name, n.MissingOk); err != nil {
		return nil, err
	}
	return &Result{Message: "DROP TABLE"}, nil
}

func (ex *Executor) execDropView(n *planner.PhysDropView) (*Result, error) {
	if err := ex.Cat.DropView(n.Name, n.MissingOk); err != nil {
		return nil, err
	}
	return &Result{Message: "DROP VIEW"}, nil
}

func (ex *Executor) execAddColumn(n *planner.PhysAddColumn) (*Result, error) {
	if err := ex.Cat.AddColumn(n.Table, n.Col.Name, n.Col.Type, n.Col.TypeName, n.Col.NotNull, n.Col.DefaultExpr, n.IfNotExists); err != nil {
		return nil, err
	}
	return &Result{Message: "ALTER TABLE"}, nil
}

func (ex *Executor) execDropColumn(n *planner.PhysDropColumn) (*Result, error) {
	if err := ex.Cat.DropColumn(n.Table, n.ColName, n.IfExists); err != nil {
		return nil, err
	}
	return &Result{Message: "ALTER TABLE"}, nil
}

// execWindowAgg evaluates window functions over the child result.
// For each window function, it partitions the rows, sorts within partitions,
// and computes the function value for each row.
func (ex *Executor) execWindowAgg(n *planner.PhysWindowAgg) (*Result, error) {
	child, err := ex.Execute(n.Child)
	if err != nil {
		return nil, err
	}

	nRows := len(child.Rows)
	nWinFuncs := len(n.WinFuncs)

	// Pre-allocate extended rows: original columns + one column per window func.
	extRows := make([][]tuple.Datum, nRows)
	for i, row := range child.Rows {
		extRows[i] = make([]tuple.Datum, len(row)+nWinFuncs)
		copy(extRows[i], row)
	}

	// Evaluate each window function independently.
	for wIdx, wf := range n.WinFuncs {
		colOffset := len(child.Columns) + wIdx

		// Build row indices sorted by partition key then order key.
		indices := make([]int, nRows)
		for i := range indices {
			indices[i] = i
		}

		// Sort by partition keys, then order keys.
		sort.SliceStable(indices, func(a, b int) bool {
			ra := &planner.Row{Columns: child.Rows[indices[a]], Names: child.Columns}
			rb := &planner.Row{Columns: child.Rows[indices[b]], Names: child.Columns}
			// Compare partition keys.
			for _, pk := range wf.PartitionBy {
				va, _ := pk.Eval(ra)
				vb, _ := pk.Eval(rb)
				cmp := planner.CompareDatums(va, vb)
				if cmp != 0 {
					return cmp < 0
				}
			}
			// Compare order keys.
			for _, ok := range wf.OrderBy {
				va, _ := ok.Expr.Eval(ra)
				vb, _ := ok.Expr.Eval(rb)
				cmp := planner.CompareDatums(va, vb)
				if cmp != 0 {
					if ok.Desc {
						return cmp > 0
					}
					return cmp < 0
				}
			}
			return false
		})

		// Identify partition boundaries.
		partStarts := []int{0}
		for i := 1; i < nRows; i++ {
			ra := &planner.Row{Columns: child.Rows[indices[i-1]], Names: child.Columns}
			rb := &planner.Row{Columns: child.Rows[indices[i]], Names: child.Columns}
			samePartition := true
			for _, pk := range wf.PartitionBy {
				va, _ := pk.Eval(ra)
				vb, _ := pk.Eval(rb)
				if planner.CompareDatums(va, vb) != 0 {
					samePartition = false
					break
				}
			}
			if !samePartition {
				partStarts = append(partStarts, i)
			}
		}
		partStarts = append(partStarts, nRows)

		// Evaluate the window function for each partition.
		for p := 0; p < len(partStarts)-1; p++ {
			pStart := partStarts[p]
			pEnd := partStarts[p+1]
			partIndices := indices[pStart:pEnd]
			partSize := pEnd - pStart

			ex.evalWindowFunc(wf, partIndices, partSize, child, extRows, colOffset)
		}
	}

	// Reorder output rows by the last window function's partition+order keys
	// (matches PostgreSQL behavior where output order reflects the window sort).
	if nWinFuncs > 0 {
		lastWf := n.WinFuncs[nWinFuncs-1]
		sortedIndices := make([]int, nRows)
		for i := range sortedIndices {
			sortedIndices[i] = i
		}
		sort.SliceStable(sortedIndices, func(a, b int) bool {
			ra := &planner.Row{Columns: extRows[sortedIndices[a]][:len(child.Columns)], Names: child.Columns}
			rb := &planner.Row{Columns: extRows[sortedIndices[b]][:len(child.Columns)], Names: child.Columns}
			for _, pk := range lastWf.PartitionBy {
				va, _ := pk.Eval(ra)
				vb, _ := pk.Eval(rb)
				cmp := planner.CompareDatums(va, vb)
				if cmp != 0 {
					return cmp < 0
				}
			}
			for _, ok := range lastWf.OrderBy {
				va, _ := ok.Expr.Eval(ra)
				vb, _ := ok.Expr.Eval(rb)
				cmp := planner.CompareDatums(va, vb)
				if cmp != 0 {
					if ok.Desc {
						return cmp > 0
					}
					return cmp < 0
				}
			}
			return false
		})
		reordered := make([][]tuple.Datum, nRows)
		for i, idx := range sortedIndices {
			reordered[i] = extRows[idx]
		}
		extRows = reordered
	}

	// Build extended column names.
	extCols := make([]string, len(child.Columns)+nWinFuncs)
	copy(extCols, child.Columns)
	for i := range n.WinFuncs {
		extCols[len(child.Columns)+i] = fmt.Sprintf("win_%d", i)
	}

	return &Result{
		Columns: extCols,
		Rows:    extRows,
		Message: fmt.Sprintf("SELECT %d", nRows),
	}, nil
}

// evalWindowFunc computes a single window function for one partition.
func (ex *Executor) evalWindowFunc(
	wf planner.WindowFuncDesc,
	partIndices []int,
	partSize int,
	child *Result,
	extRows [][]tuple.Datum,
	colOffset int,
) {
	switch strings.ToLower(wf.FuncName) {
	case "row_number":
		for i, idx := range partIndices {
			extRows[idx][colOffset] = tuple.DInt64(int64(i + 1))
		}

	case "rank":
		rank := 1
		for i, idx := range partIndices {
			if i > 0 && !windowOrderEqual(wf, child, partIndices[i-1], idx) {
				rank = i + 1
			}
			extRows[idx][colOffset] = tuple.DInt64(int64(rank))
		}

	case "dense_rank":
		rank := 1
		for i, idx := range partIndices {
			if i > 0 && !windowOrderEqual(wf, child, partIndices[i-1], idx) {
				rank++
			}
			extRows[idx][colOffset] = tuple.DInt64(int64(rank))
		}

	case "percent_rank":
		if partSize <= 1 {
			for _, idx := range partIndices {
				extRows[idx][colOffset] = tuple.DFloat64(0)
			}
		} else {
			rank := 1
			for i, idx := range partIndices {
				if i > 0 && !windowOrderEqual(wf, child, partIndices[i-1], idx) {
					rank = i + 1
				}
				extRows[idx][colOffset] = tuple.DFloat64(float64(rank-1) / float64(partSize-1))
			}
		}

	case "cume_dist":
		for i, idx := range partIndices {
			// Find the last row with the same order key values.
			last := i
			for last+1 < partSize && windowOrderEqual(wf, child, idx, partIndices[last+1]) {
				last++
			}
			extRows[idx][colOffset] = tuple.DFloat64(float64(last+1) / float64(partSize))
		}

	case "ntile":
		n := int64(1)
		if len(wf.ArgExprs) > 0 {
			row := &planner.Row{Columns: child.Rows[partIndices[0]], Names: child.Columns}
			v, err := wf.ArgExprs[0].Eval(row)
			if err == nil && v.I64 > 0 {
				n = v.I64
			}
		}
		for i, idx := range partIndices {
			bucket := int64(i)*n/int64(partSize) + 1
			extRows[idx][colOffset] = tuple.DInt64(bucket)
		}

	case "lag":
		offset := 1
		var defaultVal tuple.Datum
		if len(wf.ArgExprs) >= 2 {
			row := &planner.Row{Columns: child.Rows[partIndices[0]], Names: child.Columns}
			v, err := wf.ArgExprs[1].Eval(row)
			if err == nil {
				offset = int(v.I64)
			}
		}
		if len(wf.ArgExprs) >= 3 {
			row := &planner.Row{Columns: child.Rows[partIndices[0]], Names: child.Columns}
			v, err := wf.ArgExprs[2].Eval(row)
			if err == nil {
				defaultVal = v
			}
		}
		for i, idx := range partIndices {
			if i-offset >= 0 {
				prevIdx := partIndices[i-offset]
				row := &planner.Row{Columns: child.Rows[prevIdx], Names: child.Columns}
				v, _ := wf.ArgExprs[0].Eval(row)
				extRows[idx][colOffset] = v
			} else if defaultVal.Type != tuple.TypeNull {
				extRows[idx][colOffset] = defaultVal
			} else {
				extRows[idx][colOffset] = tuple.DNull()
			}
		}

	case "lead":
		offset := 1
		var defaultVal tuple.Datum
		if len(wf.ArgExprs) >= 2 {
			row := &planner.Row{Columns: child.Rows[partIndices[0]], Names: child.Columns}
			v, err := wf.ArgExprs[1].Eval(row)
			if err == nil {
				offset = int(v.I64)
			}
		}
		if len(wf.ArgExprs) >= 3 {
			row := &planner.Row{Columns: child.Rows[partIndices[0]], Names: child.Columns}
			v, err := wf.ArgExprs[2].Eval(row)
			if err == nil {
				defaultVal = v
			}
		}
		for i, idx := range partIndices {
			if i+offset < partSize {
				nextIdx := partIndices[i+offset]
				row := &planner.Row{Columns: child.Rows[nextIdx], Names: child.Columns}
				v, _ := wf.ArgExprs[0].Eval(row)
				extRows[idx][colOffset] = v
			} else if defaultVal.Type != tuple.TypeNull {
				extRows[idx][colOffset] = defaultVal
			} else {
				extRows[idx][colOffset] = tuple.DNull()
			}
		}

	case "first_value":
		if len(wf.ArgExprs) > 0 && partSize > 0 {
			row := &planner.Row{Columns: child.Rows[partIndices[0]], Names: child.Columns}
			v, _ := wf.ArgExprs[0].Eval(row)
			for _, idx := range partIndices {
				extRows[idx][colOffset] = v
			}
		}

	case "last_value":
		if len(wf.ArgExprs) > 0 && partSize > 0 {
			// Default frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW,
			// so last_value returns the current row's value.
			for _, idx := range partIndices {
				row := &planner.Row{Columns: child.Rows[idx], Names: child.Columns}
				v, _ := wf.ArgExprs[0].Eval(row)
				extRows[idx][colOffset] = v
			}
		}

	case "nth_value":
		nth := 1
		if len(wf.ArgExprs) >= 2 {
			row := &planner.Row{Columns: child.Rows[partIndices[0]], Names: child.Columns}
			v, err := wf.ArgExprs[1].Eval(row)
			if err == nil {
				nth = int(v.I64)
			}
		}
		for i, idx := range partIndices {
			if nth >= 1 && nth <= i+1 && len(wf.ArgExprs) > 0 {
				nthIdx := partIndices[nth-1]
				row := &planner.Row{Columns: child.Rows[nthIdx], Names: child.Columns}
				v, _ := wf.ArgExprs[0].Eval(row)
				extRows[idx][colOffset] = v
			} else {
				extRows[idx][colOffset] = tuple.DNull()
			}
		}

	// Aggregate-as-window functions: sum, count, avg, min, max.
	case "count":
		if wf.Star {
			// count(*) OVER (...) — running count.
			for i, idx := range partIndices {
				extRows[idx][colOffset] = tuple.DInt64(int64(i + 1))
			}
		} else if len(wf.ArgExprs) > 0 {
			count := int64(0)
			for _, idx := range partIndices {
				row := &planner.Row{Columns: child.Rows[idx], Names: child.Columns}
				v, _ := wf.ArgExprs[0].Eval(row)
				if v.Type != tuple.TypeNull {
					count++
				}
				extRows[idx][colOffset] = tuple.DInt64(count)
			}
		}

	case "sum":
		if len(wf.ArgExprs) > 0 {
			var sumF float64
			var sumI int64
			isFloat := false
			for i, idx := range partIndices {
				row := &planner.Row{Columns: child.Rows[idx], Names: child.Columns}
				v, _ := wf.ArgExprs[0].Eval(row)
				if i == 0 && v.Type == tuple.TypeFloat64 {
					isFloat = true
				}
				if isFloat {
					sumF += datumToFloat64(v)
					extRows[idx][colOffset] = tuple.DFloat64(sumF)
				} else {
					sumI += v.I64
					extRows[idx][colOffset] = tuple.DInt64(sumI)
				}
			}
		}

	case "avg":
		if len(wf.ArgExprs) > 0 {
			var sum float64
			for i, idx := range partIndices {
				row := &planner.Row{Columns: child.Rows[idx], Names: child.Columns}
				v, _ := wf.ArgExprs[0].Eval(row)
				sum += datumToFloat64(v)
				extRows[idx][colOffset] = tuple.DFloat64(sum / float64(i+1))
			}
		}

	case "min":
		if len(wf.ArgExprs) > 0 {
			var minVal tuple.Datum
			for i, idx := range partIndices {
				row := &planner.Row{Columns: child.Rows[idx], Names: child.Columns}
				v, _ := wf.ArgExprs[0].Eval(row)
				if i == 0 || planner.CompareDatums(v, minVal) < 0 {
					minVal = v
				}
				extRows[idx][colOffset] = minVal
			}
		}

	case "max":
		if len(wf.ArgExprs) > 0 {
			var maxVal tuple.Datum
			for i, idx := range partIndices {
				row := &planner.Row{Columns: child.Rows[idx], Names: child.Columns}
				v, _ := wf.ArgExprs[0].Eval(row)
				if i == 0 || planner.CompareDatums(v, maxVal) > 0 {
					maxVal = v
				}
				extRows[idx][colOffset] = maxVal
			}
		}

	default:
		// Unknown window function — fill with NULL.
		for _, idx := range partIndices {
			extRows[idx][colOffset] = tuple.DNull()
		}
	}
}

// windowOrderEqual checks if two rows have equal ORDER BY key values.
func windowOrderEqual(wf planner.WindowFuncDesc, child *Result, idxA, idxB int) bool {
	ra := &planner.Row{Columns: child.Rows[idxA], Names: child.Columns}
	rb := &planner.Row{Columns: child.Rows[idxB], Names: child.Columns}
	for _, ok := range wf.OrderBy {
		va, _ := ok.Expr.Eval(ra)
		vb, _ := ok.Expr.Eval(rb)
		if planner.CompareDatums(va, vb) != 0 {
			return false
		}
	}
	return true
}

func (ex *Executor) execSubqueryScan(n *planner.PhysSubqueryScan) (*Result, error) {
	if n.IsRecursive {
		return ex.execRecursiveSubqueryScan(n)
	}

	// Materialize the child plan.
	childResult, err := ex.Execute(n.Child)
	if err != nil {
		return nil, fmt.Errorf("executor: subquery scan %q: %w", n.Alias, err)
	}

	// Build qualified column names for the subquery output.
	cols := make([]string, len(n.Columns))
	for i, c := range n.Columns {
		cols[i] = n.Alias + "." + c
	}

	return &Result{
		Columns: cols,
		Rows:    childResult.Rows,
		Message: fmt.Sprintf("SELECT %d", len(childResult.Rows)),
	}, nil
}

func (ex *Executor) execRecursiveSubqueryScan(n *planner.PhysSubqueryScan) (*Result, error) {
	// Execute the non-recursive (initial) term.
	initResult, err := ex.Execute(n.RecursiveInit)
	if err != nil {
		return nil, fmt.Errorf("executor: recursive CTE %q init: %w", n.Alias, err)
	}

	allRows := make([][]tuple.Datum, len(initResult.Rows))
	copy(allRows, initResult.Rows)
	workingTable := initResult.Rows

	// Build column names.
	cols := make([]string, len(n.Columns))
	for i, c := range n.Columns {
		cols[i] = n.Alias + "." + c
	}

	// Iteratively execute the recursive term, feeding the working table
	// as the CTE's content, until no new rows are produced.
	const maxIterations = 1000
	for iter := 0; iter < maxIterations; iter++ {
		if len(workingTable) == 0 {
			break
		}

		// The recursive term references the CTE name. We need to make
		// the working table available. We do this by temporarily
		// registering the working table in the executor's CTE store.
		ex.setCTEResult(n.Alias, cols, workingTable)

		recResult, err := ex.Execute(n.Child)
		if err != nil {
			return nil, fmt.Errorf("executor: recursive CTE %q iteration %d: %w", n.Alias, iter, err)
		}

		if len(recResult.Rows) == 0 {
			break
		}

		allRows = append(allRows, recResult.Rows...)
		workingTable = recResult.Rows
	}

	ex.clearCTEResult(n.Alias)

	return &Result{
		Columns: cols,
		Rows:    allRows,
		Message: fmt.Sprintf("SELECT %d", len(allRows)),
	}, nil
}

func (ex *Executor) execValues(n *planner.PhysValues) (*Result, error) {
	emptyRow := &planner.Row{}
	var rows [][]tuple.Datum
	for _, valRow := range n.Values {
		row := make([]tuple.Datum, len(valRow))
		for i, expr := range valRow {
			val, err := expr.Eval(emptyRow)
			if err != nil {
				return nil, fmt.Errorf("executor: Values eval: %w", err)
			}
			row[i] = val
		}
		rows = append(rows, row)
	}
	return &Result{
		Columns: n.Names,
		Rows:    rows,
		Message: fmt.Sprintf("SELECT %d", len(rows)),
	}, nil
}

func (ex *Executor) execResult(n *planner.PhysResult) (*Result, error) {
	emptyRow := &planner.Row{}

	// Check for set-returning function (unnest).
	for i, expr := range n.Exprs {
		if fn, ok := expr.(*planner.ExprFunc); ok && strings.ToLower(fn.Name) == "unnest" {
			arrVal, err := fn.Args[0].Eval(emptyRow)
			if err != nil {
				return nil, fmt.Errorf("executor: Result eval: %w", err)
			}
			elements := expandArray(arrVal)
			// Evaluate non-SRF expressions once.
			base := make([]tuple.Datum, len(n.Exprs))
			for j, e := range n.Exprs {
				if j == i {
					continue
				}
				val, err := e.Eval(emptyRow)
				if err != nil {
					return nil, fmt.Errorf("executor: Result eval: %w", err)
				}
				base[j] = val
			}
			result := &Result{Columns: n.Names}
			for _, elem := range elements {
				projected := make([]tuple.Datum, len(n.Exprs))
				copy(projected, base)
				projected[i] = elem
				result.Rows = append(result.Rows, projected)
			}
			result.Message = fmt.Sprintf("SELECT %d", len(result.Rows))
			return result, nil
		}
	}

	row := make([]tuple.Datum, len(n.Exprs))
	for i, expr := range n.Exprs {
		val, err := expr.Eval(emptyRow)
		if err != nil {
			return nil, fmt.Errorf("executor: Result eval: %w", err)
		}
		row[i] = val
	}
	return &Result{
		Columns: n.Names,
		Rows:    [][]tuple.Datum{row},
		Message: "SELECT 1",
	}, nil
}
