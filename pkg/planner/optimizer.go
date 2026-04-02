package planner

import (
	"fmt"
	"math"
	"strings"

	"github.com/gololadb/loladb/pkg/catalog"
	"github.com/gololadb/loladb/pkg/tuple"
)

// Optimizer converts a logical plan into a physical plan using
// cost-based decisions.
type Optimizer struct {
	Cat   *catalog.Catalog
	Costs CostConstants
}

// Optimize converts a logical plan into a physical plan.
func (o *Optimizer) Optimize(node LogicalNode) (PhysicalNode, error) {
	phys, err := o.optimize(node)
	if err != nil {
		return nil, err
	}
	markTerminalScans(phys)
	return phys, nil
}

// markTerminalScans marks SeqScan nodes that are not wrapped by a
// Project node. These scans output all table columns directly, so the
// executor must check column-level privileges for every column.
func markTerminalScans(node PhysicalNode) {
	switch n := node.(type) {
	case *PhysProject:
		// Project handles column checks itself; its child scan is not terminal.
		return
	case *PhysSeqScan:
		n.IsTerminal = true
		return
	}
	for _, child := range node.Children() {
		markTerminalScans(child)
	}
}

func (o *Optimizer) optimize(node LogicalNode) (PhysicalNode, error) {
	switch n := node.(type) {
	case *LogicalScan:
		return o.optimizeScan(n, nil)
	case *LogicalFilter:
		return o.optimizeFilter(n)
	case *LogicalProject:
		return o.optimizeProject(n)
	case *LogicalJoin:
		return o.optimizeJoin(n)
	case *LogicalLimit:
		return o.optimizeLimit(n)
	case *LogicalSort:
		return o.optimizeSort(n)
	case *LogicalInsert:
		return o.optimizeInsert(n)
	case *LogicalDelete:
		return o.optimizeDelete(n)
	case *LogicalUpdate:
		return o.optimizeUpdate(n)
	case *LogicalCreateTable:
		return &PhysCreateTable{Table: n.Table, Columns: n.Columns}, nil
	case *LogicalCreateIndex:
		return &PhysCreateIndex{Index: n.Index, Table: n.Table, Column: n.Column, Method: n.Method}, nil
	case *LogicalExplain:
		return o.optimize(n.Child)
	case *LogicalNoOp:
		return &PhysNoOp{Message: n.Message}, nil
	case *LogicalCreateSequence:
		return &PhysCreateSequence{Name: n.Name}, nil
	case *LogicalCreateView:
		return &PhysCreateView{Name: n.Name, Definition: n.Definition, Columns: n.Columns}, nil
	case *LogicalAlterTable:
		return &PhysAlterTable{Table: n.Table, Commands: n.Commands}, nil
	case *LogicalCreatePolicy:
		return &PhysCreatePolicy{
			Name: n.Name, Table: n.Table, Cmd: n.Cmd,
			Permissive: n.Permissive, Roles: n.Roles,
			Using: n.Using, Check: n.Check,
		}, nil
	case *LogicalEnableRLS:
		return &PhysEnableRLS{Table: n.Table}, nil
	case *LogicalDisableRLS:
		return &PhysDisableRLS{Table: n.Table}, nil
	case *LogicalCreateRole:
		return &PhysCreateRole{RoleName: n.RoleName, Options: n.Options, StmtType: n.StmtType}, nil
	case *LogicalAlterRole:
		return &PhysAlterRole{RoleName: n.RoleName, Options: n.Options}, nil
	case *LogicalDropRole:
		return &PhysDropRole{Roles: n.Roles, MissingOk: n.MissingOk}, nil
	case *LogicalGrantRole:
		return &PhysGrantRole{GrantedRoles: n.GrantedRoles, Grantees: n.Grantees, AdminOption: n.AdminOption}, nil
	case *LogicalRevokeRole:
		return &PhysRevokeRole{RevokedRoles: n.RevokedRoles, Grantees: n.Grantees}, nil
	case *LogicalGrantPrivilege:
		return &PhysGrantPrivilege{Privileges: n.Privileges, PrivCols: n.PrivCols, TargetType: n.TargetType, Objects: n.Objects, Grantees: n.Grantees, GrantOption: n.GrantOption}, nil
	case *LogicalRevokePrivilege:
		return &PhysRevokePrivilege{Privileges: n.Privileges, PrivCols: n.PrivCols, TargetType: n.TargetType, Objects: n.Objects, Grantees: n.Grantees}, nil
	case *LogicalCreateFunction:
		return &PhysCreateFunction{Name: n.Name, Language: n.Language, Body: n.Body, ReturnType: n.ReturnType, ParamNames: n.ParamNames, ParamTypes: n.ParamTypes, Replace: n.Replace}, nil
	case *LogicalCreateTrigger:
		return &PhysCreateTrigger{TrigName: n.TrigName, Table: n.Table, FuncName: n.FuncName, Timing: n.Timing, Events: n.Events, ForEach: n.ForEach, Replace: n.Replace}, nil
	case *LogicalDropFunction:
		return &PhysDropFunction{Name: n.Name, MissingOk: n.MissingOk}, nil
	case *LogicalDropTrigger:
		return &PhysDropTrigger{TrigName: n.TrigName, Table: n.Table, MissingOk: n.MissingOk}, nil
	case *LogicalAlterFunction:
		return &PhysAlterFunction{Name: n.Name, NewName: n.NewName, NewOwner: n.NewOwner}, nil
	case *LogicalCreateDomain:
		return &PhysCreateDomain{Name: n.Name, BaseType: n.BaseType, NotNull: n.NotNull, CheckExpr: n.CheckExpr}, nil
	case *LogicalCreateEnum:
		return &PhysCreateEnum{Name: n.Name, Vals: n.Vals}, nil
	case *LogicalDropType:
		return &PhysDropType{Name: n.Name, MissingOk: n.MissingOk}, nil
	case *LogicalAlterEnum:
		return &PhysAlterEnum{Name: n.Name, NewVal: n.NewVal}, nil
	case *LogicalResult:
		return &PhysResult{Exprs: n.Exprs, Names: n.Names}, nil
	default:
		child, err := o.optimize(node)
		return child, err
	}
}

func (o *Optimizer) optimizeScan(n *LogicalScan, filter Expr) (PhysicalNode, error) {
	rel, err := o.Cat.FindRelation(n.Table)
	if err != nil || rel == nil {
		return nil, err
	}
	stats, _ := o.Cat.Stats(n.Table)
	tupleCount := float64(100) // default
	pages := float64(1)
	var colStats map[string]*catalog.ColumnStats
	if stats != nil {
		tupleCount = float64(stats.TupleCount)
		pages = float64(stats.RelPages)
		colStats = stats.ColumnStats
		if tupleCount == 0 {
			tupleCount = 1
		}
		if pages == 0 {
			pages = 1
		}
	}

	// Try index scan if there's an equality filter on an indexed column.
	if filter != nil {
		if idxScan := o.tryIndexScan(n, filter, rel, tupleCount, pages, colStats); idxScan != nil {
			return idxScan, nil
		}
	}

	// SeqScan cost.
	seqCost := pages*o.Costs.SeqPageCost + tupleCount*o.Costs.CPUTupleCost
	node := &PhysSeqScan{
		Table:    n.Table,
		Alias:    n.Alias,
		Columns:  n.Columns,
		HeadPage: uint32(rel.HeadPage),
		Estimate: PlanCost{Total: seqCost, Rows: tupleCount, Width: 40},
		Filter:   filter,
	}
	if filter != nil {
		// Apply selectivity using column stats when available.
		sel := estimateSelectivityWithStats(filter, tupleCount, colStats)
		node.Estimate.Rows = tupleCount * sel
		node.Estimate.Total = seqCost + tupleCount*o.Costs.CPUOperatorCost
	}
	return node, nil
}

func (o *Optimizer) tryIndexScan(n *LogicalScan, filter Expr, rel *catalog.Relation, tupleCount, pages float64, colStats map[string]*catalog.ColumnStats) PhysicalNode {
	binOp, ok := filter.(*ExprBinOp)
	if !ok || binOp.Op != OpEq {
		return nil
	}

	col, lit := extractColLit(binOp)
	if col == nil || lit == nil {
		return nil
	}

	indexes, _ := o.Cat.ListIndexesForTable(rel.OID)
	for _, idx := range indexes {
		cols, _ := o.Cat.GetColumns(rel.OID)
		if int(idx.ColNum-1) >= len(cols) || !strings.EqualFold(cols[idx.ColNum-1].Name, col.Column) {
			continue
		}

		sel := estimateSelectivityWithStats(filter, tupleCount, colStats)
		estRows := tupleCount * sel
		if estRows < 1 {
			estRows = 1
		}
		if pages < 1 {
			pages = 1
		}
		height := math.Ceil(math.Log(tupleCount+1) / math.Log(200))
		if height < 1 {
			height = 1
		}

		seqCost := pages*o.Costs.SeqPageCost + tupleCount*o.Costs.CPUTupleCost

		// --- Plain index scan cost ---
		idxStartup := height * o.Costs.RandomPageCost
		heapFetchCost := estRows * o.Costs.RandomPageCost * (pages / tupleCount)
		if heapFetchCost < o.Costs.RandomPageCost*0.01 {
			heapFetchCost = o.Costs.RandomPageCost * 0.01
		}
		idxCPU := estRows * (o.Costs.CPUIndexTupleCost + o.Costs.CPUTupleCost)
		idxTotal := idxStartup + heapFetchCost + idxCPU

		// --- Bitmap scan cost ---
		// Bitmap index scan: traverse index + collect TIDs.
		bitmapIdxCost := height*o.Costs.RandomPageCost + estRows*o.Costs.CPUIndexTupleCost
		// Bitmap heap scan: fetch distinct pages sequentially.
		// Estimate distinct pages touched using Mackert-Lohman formula:
		// pages_fetched ≈ min(pages, estRows) when rows are random.
		distinctPages := pages * (1.0 - math.Pow(1.0-1.0/pages, estRows))
		if distinctPages < 1 {
			distinctPages = 1
		}
		if distinctPages > pages {
			distinctPages = pages
		}
		bitmapHeapCost := distinctPages*o.Costs.SeqPageCost + estRows*o.Costs.CPUTupleCost
		bitmapTotal := bitmapIdxCost + bitmapHeapCost
		bitmapStartup := bitmapIdxCost // must build full bitmap before fetching

		// Pick the cheapest among: plain index scan, bitmap scan, seq scan.
		// Seq scan is the fallback (caller handles it), so we only return
		// a node if index or bitmap beats seq scan.
		type candidate struct {
			cost float64
			kind string
		}
		best := candidate{seqCost, "seq"}
		if idxTotal < best.cost {
			best = candidate{idxTotal, "idx"}
		}
		if bitmapTotal < best.cost {
			best = candidate{bitmapTotal, "bitmap"}
		}

		switch best.kind {
		case "idx":
			return &PhysIndexScan{
				Table:     n.Table,
				Alias:     n.Alias,
				Index:     idx.Name,
				Columns:   n.Columns,
				HeadPage:  uint32(rel.HeadPage),
				IndexRoot: uint32(idx.HeadPage),
				Key:       lit,
				Estimate:  PlanCost{Startup: idxStartup, Total: idxTotal, Rows: estRows, Width: 40},
			}
		case "bitmap":
			bitmapIdx := &PhysBitmapIndexScan{
				Table:     n.Table,
				Index:     idx.Name,
				IndexRoot: uint32(idx.HeadPage),
				Key:       lit,
				Estimate:  PlanCost{Startup: bitmapIdxCost, Total: bitmapIdxCost, Rows: estRows, Width: 0},
			}
			return &PhysBitmapHeapScan{
				Table:    n.Table,
				Alias:    n.Alias,
				Columns:  n.Columns,
				HeadPage: uint32(rel.HeadPage),
				Recheck:  filter,
				Child:    bitmapIdx,
				Estimate: PlanCost{Startup: bitmapStartup, Total: bitmapTotal, Rows: estRows, Width: 40},
			}
		}
		// "seq" — return nil, caller will use SeqScan.
	}
	return nil
}

func (o *Optimizer) optimizeFilter(n *LogicalFilter) (PhysicalNode, error) {
	// Try to push the filter into a scan.
	if scan, ok := n.Child.(*LogicalScan); ok {
		return o.optimizeScan(scan, n.Predicate)
	}

	// Try to push filter predicates into join children.
	if join, ok := n.Child.(*LogicalJoin); ok {
		return o.pushFilterIntoJoin(n.Predicate, join)
	}

	child, err := o.optimize(n.Child)
	if err != nil {
		return nil, err
	}

	childCost := child.Cost()
	sel := estimateSelectivity(n.Predicate, childCost.Rows)
	filterCost := PlanCost{
		Startup: childCost.Startup,
		Total:   childCost.Total + childCost.Rows*o.Costs.CPUOperatorCost,
		Rows:    childCost.Rows * sel,
		Width:   childCost.Width,
	}
	return &PhysFilter{Predicate: n.Predicate, Child: child, Estimate: filterCost}, nil
}

// pushFilterIntoJoin decomposes a filter predicate into conjuncts and
// pushes each one to the appropriate join child when possible. It also
// performs transitive predicate inference: if the filter says
// "customer.id = 5" and the join condition says "rental.customer_id =
// customer.id", we infer "rental.customer_id = 5" and push it to the
// rental side. This mirrors PostgreSQL's generate_implied_equalities.
func (o *Optimizer) pushFilterIntoJoin(pred Expr, join *LogicalJoin) (PhysicalNode, error) {
	leftCols := columnSet(join.Left.OutputColumns())
	rightCols := columnSet(join.Right.OutputColumns())

	conjuncts := flattenAnd(pred)

	// Derive transitive predicates from equi-join conditions.
	// For each "col_a = literal" in the filter and "col_a = col_b" in
	// the join condition, infer "col_b = literal".
	if join.Condition != nil {
		derived := deriveTransitivePredicates(conjuncts, join.Condition)
		conjuncts = append(conjuncts, derived...)
	}

	var leftPreds, rightPreds, remaining []Expr

	for _, c := range conjuncts {
		refs := exprColumnRefs(c)
		if len(refs) == 0 {
			remaining = append(remaining, c)
			continue
		}
		allLeft := true
		allRight := true
		for _, ref := range refs {
			if !matchesColumnSet(ref, leftCols) {
				allLeft = false
			}
			if !matchesColumnSet(ref, rightCols) {
				allRight = false
			}
		}
		if allLeft {
			leftPreds = append(leftPreds, c)
		} else if allRight {
			rightPreds = append(rightPreds, c)
		} else {
			remaining = append(remaining, c)
		}
	}

	// Wrap children with pushed-down filters.
	left := join.Left
	if len(leftPreds) > 0 {
		left = &LogicalFilter{Predicate: combineAnd(leftPreds), Child: left}
	}
	right := join.Right
	if len(rightPreds) > 0 {
		right = &LogicalFilter{Predicate: combineAnd(rightPreds), Child: right}
	}

	newJoin := &LogicalJoin{
		Type:      join.Type,
		Condition: join.Condition,
		Left:      left,
		Right:     right,
	}

	child, err := o.optimize(newJoin)
	if err != nil {
		return nil, err
	}

	// If there are remaining predicates, wrap with a filter.
	if len(remaining) > 0 {
		childCost := child.Cost()
		sel := estimateSelectivity(combineAnd(remaining), childCost.Rows)
		filterCost := PlanCost{
			Startup: childCost.Startup,
			Total:   childCost.Total + childCost.Rows*o.Costs.CPUOperatorCost,
			Rows:    childCost.Rows * sel,
			Width:   childCost.Width,
		}
		return &PhysFilter{Predicate: combineAnd(remaining), Child: child, Estimate: filterCost}, nil
	}
	return child, nil
}

// deriveTransitivePredicates generates implied equality predicates.
// Given filter predicates like "A.x = 5" and a join condition "A.x = B.y",
// it derives "B.y = 5". This mirrors PostgreSQL's EquivalenceClass
// mechanism (generate_implied_equalities in equivclass.c).
func deriveTransitivePredicates(filterPreds []Expr, joinCond Expr) []Expr {
	// Collect "column = literal" facts from the filter.
	type colLitFact struct {
		col *ExprColumn
		lit *ExprLiteral
	}
	var facts []colLitFact
	for _, p := range filterPreds {
		binOp, ok := p.(*ExprBinOp)
		if !ok || binOp.Op != OpEq {
			continue
		}
		col, lit := extractColLit(binOp)
		if col != nil && lit != nil {
			facts = append(facts, colLitFact{col, lit})
		}
	}
	if len(facts) == 0 {
		return nil
	}

	// Collect "column = column" equalities from the join condition.
	type colColEq struct {
		left, right *ExprColumn
	}
	var eqs []colColEq
	for _, jc := range flattenAnd(joinCond) {
		binOp, ok := jc.(*ExprBinOp)
		if !ok || binOp.Op != OpEq {
			continue
		}
		lc, _ := binOp.Left.(*ExprColumn)
		rc, _ := binOp.Right.(*ExprColumn)
		if lc != nil && rc != nil {
			eqs = append(eqs, colColEq{lc, rc})
		}
	}

	// For each fact and each join equality, derive new predicates.
	var derived []Expr
	for _, fact := range facts {
		for _, eq := range eqs {
			if columnsEqual(fact.col, eq.left) {
				// fact: A.x = lit, join: A.x = B.y → derive B.y = lit
				derived = append(derived, &ExprBinOp{Op: OpEq, Left: eq.right, Right: fact.lit})
			} else if columnsEqual(fact.col, eq.right) {
				// fact: B.y = lit, join: A.x = B.y → derive A.x = lit
				derived = append(derived, &ExprBinOp{Op: OpEq, Left: eq.left, Right: fact.lit})
			}
		}
	}
	return derived
}

// columnsEqual checks if two ExprColumn references refer to the same column.
func columnsEqual(a, b *ExprColumn) bool {
	if a.Table != "" && b.Table != "" {
		return strings.EqualFold(a.Table, b.Table) && strings.EqualFold(a.Column, b.Column)
	}
	return strings.EqualFold(a.Column, b.Column)
}

// flattenAnd extracts conjuncts from nested AND expressions.
func flattenAnd(e Expr) []Expr {
	if binOp, ok := e.(*ExprBinOp); ok && binOp.Op == OpAnd {
		return append(flattenAnd(binOp.Left), flattenAnd(binOp.Right)...)
	}
	return []Expr{e}
}

// combineAnd combines multiple predicates with AND.
func combineAnd(preds []Expr) Expr {
	if len(preds) == 1 {
		return preds[0]
	}
	result := preds[0]
	for _, p := range preds[1:] {
		result = &ExprBinOp{Op: OpAnd, Left: result, Right: p}
	}
	return result
}

// exprColumnRefs collects all ExprColumn references in an expression.
func exprColumnRefs(e Expr) []*ExprColumn {
	var refs []*ExprColumn
	switch v := e.(type) {
	case *ExprColumn:
		refs = append(refs, v)
	case *ExprBinOp:
		refs = append(refs, exprColumnRefs(v.Left)...)
		refs = append(refs, exprColumnRefs(v.Right)...)
	case *ExprNot:
		refs = append(refs, exprColumnRefs(v.Child)...)
	case *ExprIsNull:
		refs = append(refs, exprColumnRefs(v.Child)...)
	}
	return refs
}

// columnSet builds a set of "table.column" strings from output columns.
func columnSet(cols []string) map[string]bool {
	s := make(map[string]bool, len(cols))
	for _, c := range cols {
		s[strings.ToLower(c)] = true
	}
	return s
}

// matchesColumnSet checks if a column reference matches any column in the set.
func matchesColumnSet(col *ExprColumn, set map[string]bool) bool {
	if col.Table != "" {
		// Qualified reference: must match "table.column" exactly.
		qualified := strings.ToLower(col.Table + "." + col.Column)
		if set[qualified] {
			return true
		}
		// No match — a qualified reference should not fall back to
		// unqualified matching because that would incorrectly push
		// e.g. "customer.customer_id" to the rental side when both
		// sides have a "customer_id" column.
		return false
	}
	// Unqualified reference: match if exactly one side contains it.
	colLower := strings.ToLower(col.Column)
	for k := range set {
		parts := strings.SplitN(k, ".", 2)
		if len(parts) == 2 && parts[1] == colLower {
			return true
		}
		if k == colLower {
			return true
		}
	}
	return false
}

func (o *Optimizer) optimizeProject(n *LogicalProject) (PhysicalNode, error) {
	child, err := o.optimize(n.Child)
	if err != nil {
		return nil, err
	}
	childCost := child.Cost()
	projCost := PlanCost{
		Startup: childCost.Startup,
		Total:   childCost.Total + childCost.Rows*o.Costs.CPUOperatorCost,
		Rows:    childCost.Rows,
		Width:   len(n.Names) * 8,
	}
	return &PhysProject{Exprs: n.Exprs, Names: n.Names, Child: child, Estimate: projCost}, nil
}

func (o *Optimizer) optimizeJoin(n *LogicalJoin) (PhysicalNode, error) {
	// Try DP join reordering for chains of inner joins (≤12 relations).
	if n.Type == JoinInner || n.Type == JoinCross {
		if result := o.tryDPJoinOrder(n); result != nil {
			return result, nil
		}
	}

	left, err := o.optimize(n.Left)
	if err != nil {
		return nil, err
	}
	right, err := o.optimize(n.Right)
	if err != nil {
		return nil, err
	}

	leftCost := left.Cost()
	rightCost := right.Cost()
	joinRows := leftCost.Rows * rightCost.Rows

	if n.Condition != nil {
		sel := estimateSelectivity(n.Condition, joinRows)
		joinRows *= sel
	}
	if joinRows < 1 {
		joinRows = 1
	}

	width := leftCost.Width + rightCost.Width

	type joinCandidate struct {
		node PhysicalNode
		cost float64
	}
	var best *joinCandidate

	consider := func(node PhysicalNode, cost float64) {
		if best == nil || cost < best.cost {
			best = &joinCandidate{node, cost}
		}
	}

	// Plain nested loop.
	nlTotal := leftCost.Total + leftCost.Rows*rightCost.Total
	consider(&PhysNestedLoopJoin{
		Type: n.Type, Condition: n.Condition,
		Outer: left, Inner: right,
		Estimate: PlanCost{Startup: leftCost.Startup + rightCost.Startup, Total: nlTotal, Rows: joinRows, Width: width},
	}, nlTotal)

	if n.Condition != nil && isEquiJoin(n.Condition) {
		// Hash join.
		hashTotal := rightCost.Total + rightCost.Rows*o.Costs.CPUTupleCost + leftCost.Total + leftCost.Rows*o.Costs.CPUOperatorCost
		consider(&PhysHashJoin{
			Type: n.Type, Condition: n.Condition,
			Outer: left, Inner: right,
			Estimate: PlanCost{Startup: rightCost.Total + rightCost.Rows*o.Costs.CPUTupleCost, Total: hashTotal, Rows: joinRows, Width: width},
		}, hashTotal)

		// Parameterized nested loop: try using an index on the inner
		// side's join column, re-scanned for each outer row.
		if paramNL := o.tryParamNestedLoop(n, left, right, leftCost, joinRows, width); paramNL != nil {
			consider(paramNL, paramNL.Cost().Total)
		}
		// Also try with sides swapped (inner ↔ outer) for INNER joins.
		if n.Type == JoinInner {
			if paramNL := o.tryParamNestedLoop(n, right, left, rightCost, joinRows, width); paramNL != nil {
				consider(paramNL, paramNL.Cost().Total)
			}
		}
	}

	return best.node, nil
}

// tryParamNestedLoop checks if the inner side of a join has an index
// on the join column. If so, it builds a parameterized nested loop
// where the inner index scan is re-executed per outer row.
func (o *Optimizer) tryParamNestedLoop(n *LogicalJoin, outer, inner PhysicalNode, outerCost PlanCost, joinRows float64, width int) PhysicalNode {
	binOp, ok := n.Condition.(*ExprBinOp)
	if !ok || binOp.Op != OpEq {
		return nil
	}
	lc, _ := binOp.Left.(*ExprColumn)
	rc, _ := binOp.Right.(*ExprColumn)
	if lc == nil || rc == nil {
		return nil
	}

	// Determine which column belongs to the inner side.
	innerCols := columnSet(innerOutputCols(inner))
	var innerCol, outerCol *ExprColumn
	if matchesColumnSet(lc, innerCols) {
		innerCol, outerCol = lc, rc
	} else if matchesColumnSet(rc, innerCols) {
		innerCol, outerCol = rc, lc
	} else {
		return nil
	}

	// Find the inner table and check for an index on innerCol.
	innerTable := extractTableName(inner)
	if innerTable == "" {
		return nil
	}
	rel, err := o.Cat.FindRelation(innerTable)
	if err != nil || rel == nil {
		return nil
	}
	indexes, _ := o.Cat.ListIndexesForTable(rel.OID)
	cols, _ := o.Cat.GetColumns(rel.OID)

	for _, idx := range indexes {
		if int(idx.ColNum-1) >= len(cols) {
			continue
		}
		if !strings.EqualFold(cols[idx.ColNum-1].Name, innerCol.Column) {
			continue
		}

		// Found an index. Cost: for each outer row, one index lookup.
		stats, _ := o.Cat.Stats(innerTable)
		tupleCount := float64(100)
		pages := float64(1)
		if stats != nil {
			tupleCount = float64(stats.TupleCount)
			pages = float64(stats.RelPages)
			if tupleCount == 0 {
				tupleCount = 1
			}
			if pages == 0 {
				pages = 1
			}
		}

		height := math.Ceil(math.Log(tupleCount+1) / math.Log(200))
		if height < 1 {
			height = 1
		}
		// Per-lookup cost: index traversal + heap fetch for ~1 row.
		perLookup := height*o.Costs.RandomPageCost + o.Costs.CPUIndexTupleCost + o.Costs.CPUTupleCost
		paramTotal := outerCost.Total + outerCost.Rows*perLookup

		// Build inner columns list.
		innerColNames := make([]string, len(cols))
		for i, c := range cols {
			innerColNames[i] = c.Name
		}

		// Resolve the outer column's qualified name.
		outerQual := outerCol.Column
		if outerCol.Table != "" {
			outerQual = outerCol.Table + "." + outerCol.Column
		}

		innerScan := &PhysIndexScan{
			Table:     innerTable,
			Index:     idx.Name,
			Columns:   innerColNames,
			HeadPage:  uint32(rel.HeadPage),
			IndexRoot: uint32(idx.HeadPage),
			Key:       nil, // filled at execution time
			Estimate:  PlanCost{Total: perLookup, Rows: 1, Width: 40},
		}

		return &PhysNestedLoopJoin{
			Type:  n.Type,
			Outer: outer,
			Inner: innerScan,
			Estimate: PlanCost{
				Startup: outerCost.Startup,
				Total:   paramTotal,
				Rows:    joinRows,
				Width:   width,
			},
			InnerParam: &NestLoopParam{OuterCol: outerQual},
		}
	}
	return nil
}

// innerOutputCols returns the output column names from a physical node,
// traversing through Filter nodes.
func innerOutputCols(node PhysicalNode) []string {
	switch n := node.(type) {
	case *PhysSeqScan:
		alias := n.Alias
		if alias == "" {
			alias = n.Table
		}
		cols := make([]string, len(n.Columns))
		for i, c := range n.Columns {
			cols[i] = alias + "." + c
		}
		return cols
	case *PhysIndexScan:
		alias := n.Alias
		if alias == "" {
			alias = n.Table
		}
		cols := make([]string, len(n.Columns))
		for i, c := range n.Columns {
			cols[i] = alias + "." + c
		}
		return cols
	case *PhysFilter:
		return innerOutputCols(n.Child)
	case *PhysBitmapHeapScan:
		alias := n.Alias
		if alias == "" {
			alias = n.Table
		}
		cols := make([]string, len(n.Columns))
		for i, c := range n.Columns {
			cols[i] = alias + "." + c
		}
		return cols
	}
	// Fallback: use Children.
	for _, child := range node.Children() {
		if cols := innerOutputCols(child); len(cols) > 0 {
			return cols
		}
	}
	return nil
}

// extractTableName extracts the base table name from a physical scan node.
func extractTableName(node PhysicalNode) string {
	switch n := node.(type) {
	case *PhysSeqScan:
		return n.Table
	case *PhysIndexScan:
		return n.Table
	case *PhysBitmapHeapScan:
		return n.Table
	case *PhysFilter:
		return extractTableName(n.Child)
	}
	return ""
}

// -----------------------------------------------------------------------
// DP join reordering — mirrors PostgreSQL's standard_join_search
// (optimizer/path/joinrels.c) for inner joins with ≤12 relations.
// -----------------------------------------------------------------------

// tryDPJoinOrder flattens a tree of inner joins into base relations and
// join predicates, then uses dynamic programming to find the cheapest
// join order. Returns nil if the join tree can't be flattened (e.g.
// contains outer joins or too many relations).
func (o *Optimizer) tryDPJoinOrder(root *LogicalJoin) PhysicalNode {
	var rels []LogicalNode
	var preds []Expr

	if !flattenInnerJoins(root, &rels, &preds) {
		return nil
	}
	if len(rels) < 3 || len(rels) > 12 {
		// Not worth reordering for 2 relations; too expensive for >12.
		return nil
	}

	n := len(rels)

	// Optimize each base relation.
	basePhys := make([]PhysicalNode, n)
	for i, rel := range rels {
		p, err := o.optimize(rel)
		if err != nil {
			return nil
		}
		basePhys[i] = p
	}

	// DP table: maps a bitmask of relation indices to the best physical plan.
	type dpEntry struct {
		node PhysicalNode
		cost float64
	}
	dp := make(map[uint]dpEntry)

	// Initialize singletons.
	for i := 0; i < n; i++ {
		mask := uint(1) << i
		dp[mask] = dpEntry{basePhys[i], basePhys[i].Cost().Total}
	}

	// Build up subsets of increasing size.
	for size := 2; size <= n; size++ {
		for mask := uint(0); mask < (1 << n); mask++ {
			if popcount(mask) != size {
				continue
			}
			// Try all ways to split mask into two non-empty subsets.
			for sub := (mask - 1) & mask; sub > 0; sub = (sub - 1) & mask {
				comp := mask ^ sub
				if comp == 0 || sub > comp {
					// Avoid duplicates: only consider sub < comp.
					continue
				}
				left, lok := dp[sub]
				right, rok := dp[comp]
				if !lok || !rok {
					continue
				}

				// Find applicable join predicates.
				var applicable []Expr
				leftCols := physOutputCols(left.node)
				rightCols := physOutputCols(right.node)
				for _, pred := range preds {
					refs := exprColumnRefs(pred)
					hasLeft, hasRight := false, false
					for _, ref := range refs {
						if matchesColumnSet(ref, leftCols) {
							hasLeft = true
						}
						if matchesColumnSet(ref, rightCols) {
							hasRight = true
						}
					}
					if hasLeft && hasRight {
						applicable = append(applicable, pred)
					}
				}

				var cond Expr
				joinType := JoinInner
				if len(applicable) > 0 {
					cond = combineAnd(applicable)
				} else {
					joinType = JoinCross
				}

				// Cost the join both ways (left⋈right and right⋈left).
				for _, swap := range []bool{false, true} {
					l, r := left.node, right.node
					if swap {
						l, r = r, l
					}
					lc, rc := l.Cost(), r.Cost()
					joinRows := lc.Rows * rc.Rows
					if cond != nil {
						joinRows *= estimateSelectivity(cond, joinRows)
					}
					if joinRows < 1 {
						joinRows = 1
					}
					width := lc.Width + rc.Width

					// Hash join cost.
					if cond != nil && isEquiJoin(cond) {
						hashTotal := rc.Total + rc.Rows*o.Costs.CPUTupleCost + lc.Total + lc.Rows*o.Costs.CPUOperatorCost
						if prev, ok := dp[mask]; !ok || hashTotal < prev.cost {
							dp[mask] = dpEntry{
								node: &PhysHashJoin{
									Type: joinType, Condition: cond,
									Outer: l, Inner: r,
									Estimate: PlanCost{
										Startup: rc.Total + rc.Rows*o.Costs.CPUTupleCost,
										Total:   hashTotal, Rows: joinRows, Width: width,
									},
								},
								cost: hashTotal,
							}
						}
					}

					// Nested loop cost.
					nlTotal := lc.Total + lc.Rows*rc.Total
					if prev, ok := dp[mask]; !ok || nlTotal < prev.cost {
						dp[mask] = dpEntry{
							node: &PhysNestedLoopJoin{
								Type: joinType, Condition: cond,
								Outer: l, Inner: r,
								Estimate: PlanCost{
									Startup: lc.Startup + rc.Startup,
									Total:   nlTotal, Rows: joinRows, Width: width,
								},
							},
							cost: nlTotal,
						}
					}

					// Parameterized NL (try index on inner side).
					if cond != nil && isEquiJoin(cond) {
						logJoin := &LogicalJoin{Type: joinType, Condition: cond}
						if paramNL := o.tryParamNestedLoop(logJoin, l, r, lc, joinRows, width); paramNL != nil {
							paramCost := paramNL.Cost().Total
							if prev, ok := dp[mask]; !ok || paramCost < prev.cost {
								dp[mask] = dpEntry{node: paramNL, cost: paramCost}
							}
						}
					}
				}
			}
		}
	}

	fullMask := uint((1 << n) - 1)
	if entry, ok := dp[fullMask]; ok {
		return entry.node
	}
	return nil
}

// flattenInnerJoins recursively flattens a tree of inner/cross joins
// into a list of base relations and join predicates.
func flattenInnerJoins(node LogicalNode, rels *[]LogicalNode, preds *[]Expr) bool {
	join, ok := node.(*LogicalJoin)
	if !ok {
		*rels = append(*rels, node)
		return true
	}
	if join.Type != JoinInner && join.Type != JoinCross {
		return false // can't reorder outer joins
	}
	if !flattenInnerJoins(join.Left, rels, preds) {
		return false
	}
	if !flattenInnerJoins(join.Right, rels, preds) {
		return false
	}
	if join.Condition != nil {
		*preds = append(*preds, flattenAnd(join.Condition)...)
	}
	return true
}

// physOutputCols returns the qualified output column names from a physical node.
func physOutputCols(node PhysicalNode) map[string]bool {
	var cols []string
	switch n := node.(type) {
	case *PhysSeqScan:
		alias := n.Alias
		if alias == "" {
			alias = n.Table
		}
		for _, c := range n.Columns {
			cols = append(cols, alias+"."+c)
		}
	case *PhysIndexScan:
		alias := n.Alias
		if alias == "" {
			alias = n.Table
		}
		for _, c := range n.Columns {
			cols = append(cols, alias+"."+c)
		}
	case *PhysBitmapHeapScan:
		alias := n.Alias
		if alias == "" {
			alias = n.Table
		}
		for _, c := range n.Columns {
			cols = append(cols, alias+"."+c)
		}
	case *PhysFilter:
		return physOutputCols(n.Child)
	default:
		// For join nodes, merge children.
		for _, child := range node.Children() {
			for k, v := range physOutputCols(child) {
				if cols == nil {
					cols = []string{}
				}
				_ = v
				cols = append(cols, k)
			}
		}
	}
	set := make(map[string]bool, len(cols))
	for _, c := range cols {
		set[strings.ToLower(c)] = true
	}
	return set
}

func popcount(x uint) int {
	count := 0
	for x != 0 {
		count += int(x & 1)
		x >>= 1
	}
	return count
}

func (o *Optimizer) optimizeLimit(n *LogicalLimit) (PhysicalNode, error) {
	child, err := o.optimize(n.Child)
	if err != nil {
		return nil, err
	}
	childCost := child.Cost()
	rows := childCost.Rows
	if n.Count >= 0 && float64(n.Count) < rows {
		rows = float64(n.Count)
	}
	return &PhysLimit{
		Count: n.Count, Offset: n.Offset, Child: child,
		Estimate: PlanCost{Startup: childCost.Startup, Total: childCost.Total, Rows: rows, Width: childCost.Width},
	}, nil
}

func (o *Optimizer) optimizeSort(n *LogicalSort) (PhysicalNode, error) {
	child, err := o.optimize(n.Child)
	if err != nil {
		return nil, err
	}
	childCost := child.Cost()
	rows := childCost.Rows
	sortCost := rows * math.Log2(rows+1) * o.Costs.CPUOperatorCost
	return &PhysSort{
		Keys: n.Keys, Child: child,
		Estimate: PlanCost{Startup: childCost.Total + sortCost, Total: childCost.Total + sortCost, Rows: rows, Width: childCost.Width},
	}, nil
}

func (o *Optimizer) optimizeInsert(n *LogicalInsert) (PhysicalNode, error) {
	var values [][]Expr
	values = append(values, n.Values...)
	return &PhysInsert{Table: n.Table, Values: values}, nil
}

func (o *Optimizer) optimizeDelete(n *LogicalDelete) (PhysicalNode, error) {
	child, err := o.optimize(n.Child)
	if err != nil {
		return nil, err
	}
	return &PhysDelete{Table: n.Table, Child: child}, nil
}

func (o *Optimizer) optimizeUpdate(n *LogicalUpdate) (PhysicalNode, error) {
	child, err := o.optimize(n.Child)
	if err != nil {
		return nil, err
	}
	colTypes := make([]DatumType, len(n.ColTypes))
	for i, ct := range n.ColTypes {
		colTypes[i] = DatumType(ct)
	}
	return &PhysUpdate{
		Table: n.Table, Assignments: n.Assignments,
		Columns: n.Columns, ColTypes: colTypes, Child: child,
	}, nil
}

// --- Helpers ---

// estimateSelectivity estimates the fraction of rows matching a predicate.
// colStats may be nil; when available, ndistinct values improve equality
// estimates (mirroring PostgreSQL's clausesel.c / eqsel).
func estimateSelectivity(pred Expr, rows float64) float64 {
	return estimateSelectivityWithStats(pred, rows, nil)
}

func estimateSelectivityWithStats(pred Expr, rows float64, colStats map[string]*catalog.ColumnStats) float64 {
	switch e := pred.(type) {
	case *ExprBinOp:
		switch e.Op {
		case OpEq:
			// Try to use MCV or ndistinct from column stats.
			if colStats != nil {
				col, lit := extractColLit(e)
				if col != nil {
					cs, ok := colStats[strings.ToLower(col.Column)]
					if ok {
						// Check if the literal matches an MCV.
						if lit != nil && len(cs.MCVals) > 0 {
							litKey := datumKeyForOptimizer(lit.Value)
							for i, mcv := range cs.MCVals {
								if mcv == litKey {
									return cs.MCFreqs[i]
								}
							}
							// Value not in MCV list — estimate from
							// remaining values (non-MCV fraction).
							mcvTotal := 0.0
							for _, f := range cs.MCFreqs {
								mcvTotal += f
							}
							remainingDistinct := cs.NDistinct - float64(len(cs.MCVals))
							if remainingDistinct < 1 {
								remainingDistinct = 1
							}
							return (1.0 - mcvTotal) / remainingDistinct
						}
						// No literal or no MCVs — use 1/ndistinct.
						if cs.NDistinct > 0 {
							return 1.0 / cs.NDistinct
						}
					}
				}
			}
			// Fallback: approximate ndistinct as sqrt(rows).
			if rows > 0 {
				distinct := math.Sqrt(rows)
				if distinct < 1 {
					distinct = 1
				}
				return 1.0 / distinct
			}
			return 0.01
		case OpLt, OpLte, OpGt, OpGte:
			return 0.33
		case OpNeq:
			return 0.99
		case OpAnd:
			return estimateSelectivityWithStats(e.Left, rows, colStats) * estimateSelectivityWithStats(e.Right, rows, colStats)
		case OpOr:
			sl := estimateSelectivityWithStats(e.Left, rows, colStats)
			sr := estimateSelectivityWithStats(e.Right, rows, colStats)
			return sl + sr - sl*sr
		}
	case *ExprNot:
		return 1.0 - estimateSelectivityWithStats(e.Child, rows, colStats)
	case *ExprIsNull:
		if e.Not {
			return 0.99
		}
		return 0.01
	}
	return 0.5
}

func isEquiJoin(cond Expr) bool {
	if binOp, ok := cond.(*ExprBinOp); ok {
		return binOp.Op == OpEq
	}
	return false
}

func extractColLit(e *ExprBinOp) (*ExprColumn, *ExprLiteral) {
	col, _ := e.Left.(*ExprColumn)
	lit, _ := e.Right.(*ExprLiteral)
	if col != nil && lit != nil {
		return col, lit
	}
	col, _ = e.Right.(*ExprColumn)
	lit, _ = e.Left.(*ExprLiteral)
	return col, lit
}

// datumKeyForOptimizer converts a Datum to the same string key format
// used by catalog.datumKey, so MCV lookups match.
func datumKeyForOptimizer(d tuple.Datum) string {
	switch d.Type {
	case tuple.TypeInt32:
		return fmt.Sprintf("i32:%d", d.I32)
	case tuple.TypeInt64:
		return fmt.Sprintf("i64:%d", d.I64)
	case tuple.TypeText:
		return "t:" + d.Text
	case tuple.TypeBool:
		if d.Bool {
			return "b:t"
		}
		return "b:f"
	case tuple.TypeFloat64:
		return fmt.Sprintf("f:%g", d.F64)
	default:
		return "null"
	}
}

// ListIndexesForTable is a helper to look up indexes on a catalog.
// We add this method to the catalog interface.
func init() {
	// This init is intentionally empty — the actual method is on
	// catalog.Catalog. See the note below.
}
