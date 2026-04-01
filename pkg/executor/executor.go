package executor

import (
	"fmt"
	"sort"
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
	Cat *catalog.Catalog
}

// NewExecutor creates a plan executor.
func NewExecutor(cat *catalog.Catalog) *Executor {
	return &Executor{Cat: cat}
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
	return &Result{RowsAffected: count, Message: fmt.Sprintf("INSERT %d", count)}, nil
}

func (ex *Executor) execDelete(n *planner.PhysDelete) (*Result, error) {
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
	_, err := ex.Cat.CreateTable(n.Table, cols)
	if err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("CREATE TABLE %s", n.Table)}, nil
}

func (ex *Executor) execCreateIndex(n *planner.PhysCreateIndex) (*Result, error) {
	_, err := ex.Cat.CreateIndex(n.Index, n.Table, n.Column)
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
