package executor

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jespino/loladb/pkg/catalog"
	"github.com/jespino/loladb/pkg/planner"
	"github.com/jespino/loladb/pkg/slottedpage"
	"github.com/jespino/loladb/pkg/tuple"
)

// Result holds the output of plan execution.
type Result struct {
	Columns      []string
	Rows         [][]tuple.Datum
	RowsAffected int64
	Message      string
}

// Executor runs physical plan trees against the catalog/engine.
type Executor struct {
	Cat         *catalog.Catalog
	CurrentUser string // session user for privilege checks
}

// NewExecutor creates a plan executor.
func NewExecutor(cat *catalog.Catalog) *Executor {
	return &Executor{Cat: cat}
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
	case *planner.PhysSort:
		return ex.execSort(n)
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

	result := &Result{Columns: n.Names}
	for _, row := range child.Rows {
		r := &planner.Row{Columns: row, Names: child.Columns}
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

// --- Join executors ---

func (ex *Executor) execNestedLoopJoin(n *planner.PhysNestedLoopJoin) (*Result, error) {
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

	for _, outerRow := range outer.Rows {
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
		if !matched && n.Type == planner.JoinLeft {
			nulls := make([]tuple.Datum, len(inner.Columns))
			for i := range nulls {
				nulls[i] = tuple.DNull()
			}
			combined := append(append([]tuple.Datum{}, outerRow...), nulls...)
			result.Rows = append(result.Rows, combined)
		}
	}

	// RIGHT JOIN: emit inner rows with NULLs if no match.
	if n.Type == planner.JoinRight {
		for _, innerRow := range inner.Rows {
			matched := false
			for _, outerRow := range outer.Rows {
				combined := append(append([]tuple.Datum{}, outerRow...), innerRow...)
				r := &planner.Row{Columns: combined, Names: colNames}
				if n.Condition == nil || planner.EvalBool(n.Condition, r) {
					matched = true
					break
				}
			}
			if !matched {
				nulls := make([]tuple.Datum, len(outer.Columns))
				for i := range nulls {
					nulls[i] = tuple.DNull()
				}
				combined := append(append([]tuple.Datum{}, nulls...), innerRow...)
				result.Rows = append(result.Rows, combined)
			}
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
	for _, outerRow := range outer.Rows {
		var k string
		if outerKeyIdx >= 0 && outerKeyIdx < len(outerRow) {
			k = datumHashKey(outerRow[outerKeyIdx])
		}
		matched := false
		for _, innerIdx := range hashTable[k] {
			combined := append(append([]tuple.Datum{}, outerRow...), inner.Rows[innerIdx]...)
			matched = true
			result.Rows = append(result.Rows, combined)
		}
		if !matched && n.Type == planner.JoinLeft {
			nulls := make([]tuple.Datum, len(inner.Columns))
			for i := range nulls {
				nulls[i] = tuple.DNull()
			}
			combined := append(append([]tuple.Datum{}, outerRow...), nulls...)
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

// --- DML executors ---

func (ex *Executor) execInsert(n *planner.PhysInsert) (*Result, error) {
	if err := ex.checkTablePrivilege(n.Table, catalog.PrivInsert); err != nil {
		return nil, err
	}
	var count int64
	for _, rowExprs := range n.Values {
		values := make([]tuple.Datum, len(rowExprs))
		for i, expr := range rowExprs {
			val, err := expr.Eval(&planner.Row{})
			if err != nil {
				return nil, err
			}
			values[i] = val
		}
		_, err := ex.Cat.InsertInto(n.Table, values)
		if err != nil {
			return nil, err
		}
		count++
	}
	return &Result{RowsAffected: count, Message: fmt.Sprintf("INSERT 0 %d", count)}, nil
}

func (ex *Executor) execDelete(n *planner.PhysDelete) (*Result, error) {
	if err := ex.checkTablePrivilege(n.Table, catalog.PrivDelete); err != nil {
		return nil, err
	}
	child, err := ex.Execute(n.Child)
	if err != nil {
		return nil, err
	}

	// We need the ItemIDs. Re-scan the table with the same filter to get them.
	rel, err := ex.Cat.FindRelation(n.Table)
	if err != nil || rel == nil {
		return nil, fmt.Errorf("executor: table %q not found", n.Table)
	}

	// Collect matching rows via scan
	var toDelete []slottedpage.ItemID
	matchIdx := 0
	ex.Cat.SeqScan(n.Table, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		// Match against the child result rows.
		if matchIdx < len(child.Rows) {
			// Simple approach: delete rows that match the child scan's columns.
			if rowsMatch(tup.Columns, child.Rows[matchIdx]) {
				toDelete = append(toDelete, id)
				matchIdx++
			}
		}
		return true
	})

	for _, id := range toDelete {
		ex.Cat.Delete(n.Table, id)
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

	// Collect matching ItemIDs.
	type target struct {
		id  slottedpage.ItemID
		row []tuple.Datum
	}
	var targets []target
	matchIdx := 0
	ex.Cat.SeqScan(n.Table, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if matchIdx < len(child.Rows) && rowsMatch(tup.Columns, child.Rows[matchIdx]) {
			row := make([]tuple.Datum, len(tup.Columns))
			copy(row, tup.Columns)
			targets = append(targets, target{id: id, row: row})
			matchIdx++
		}
		return true
	})

	childCols := child.Columns
	for _, t := range targets {
		newVals := make([]tuple.Datum, len(t.row))
		copy(newVals, t.row)
		for _, a := range n.Assignments {
			for ci, cname := range n.Columns {
				if cname == a.Column {
					r := &planner.Row{Columns: t.row, Names: childCols}
					val, err := a.Value.Eval(r)
					if err != nil {
						return nil, err
					}
					newVals[ci] = val
					break
				}
			}
		}
		ex.Cat.Update(n.Table, t.id, newVals)
	}

	return &Result{RowsAffected: int64(len(targets)), Message: fmt.Sprintf("UPDATE %d", len(targets))}, nil
}

func (ex *Executor) execCreateTable(n *planner.PhysCreateTable) (*Result, error) {
	cols := make([]catalog.ColumnDef, len(n.Columns))
	for i, c := range n.Columns {
		cols[i] = catalog.ColumnDef{Name: c.Name, Type: c.Type}
	}
	// Set the owner to the current session user.
	var ownerOID int32
	if ex.CurrentUser != "" {
		role, _ := ex.Cat.FindRole(ex.CurrentUser)
		if role != nil {
			ownerOID = role.OID
		}
	}
	_, err := ex.Cat.CreateTableOwned(n.Table, cols, ownerOID)
	if err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("CREATE TABLE %s", n.Table)}, nil
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
	switch d.Type {
	case tuple.TypeInt32:
		return int64(d.I32), true
	case tuple.TypeInt64:
		return d.I64, true
	default:
		return 0, false
	}
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
		cols[i] = catalog.ColumnDef{Name: c.Name, Type: c.Type}
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

