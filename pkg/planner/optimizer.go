package planner

import (
	"math"
	"strings"

	"github.com/jespino/loladb/pkg/catalog"
)

// Optimizer converts a logical plan into a physical plan using
// cost-based decisions.
type Optimizer struct {
	Cat   *catalog.Catalog
	Costs CostConstants
}

// Optimize converts a logical plan into a physical plan.
func (o *Optimizer) Optimize(node LogicalNode) (PhysicalNode, error) {
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
		return &PhysCreateIndex{Index: n.Index, Table: n.Table, Column: n.Column}, nil
	case *LogicalExplain:
		return o.Optimize(n.Child)
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
	default:
		child, err := o.Optimize(node)
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

	// Try index scan if there's an equality filter on an indexed column.
	if filter != nil {
		if idxScan := o.tryIndexScan(n, filter, rel, tupleCount); idxScan != nil {
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
		// Apply selectivity.
		sel := estimateSelectivity(filter, tupleCount)
		node.Estimate.Rows = tupleCount * sel
		node.Estimate.Total = seqCost + tupleCount*o.Costs.CPUOperatorCost
	}
	return node, nil
}

func (o *Optimizer) tryIndexScan(n *LogicalScan, filter Expr, rel *catalog.Relation, tupleCount float64) PhysicalNode {
	binOp, ok := filter.(*ExprBinOp)
	if !ok || binOp.Op != OpEq {
		return nil
	}

	// Check if one side is a column and the other is a literal.
	col, lit := extractColLit(binOp)
	if col == nil || lit == nil {
		return nil
	}

	// Find an index on this column.
	indexes, _ := o.Cat.ListIndexesForTable(rel.OID)
	for _, idx := range indexes {
		cols, _ := o.Cat.GetColumns(rel.OID)
		if int(idx.ColNum-1) < len(cols) && strings.EqualFold(cols[idx.ColNum-1].Name, col.Column) {
			height := math.Log2(tupleCount + 1)
			if height < 1 {
				height = 1
			}
			idxCost := height*o.Costs.RandomPageCost + o.Costs.CPUIndexTupleCost + o.Costs.CPUTupleCost
			return &PhysIndexScan{
				Table:     n.Table,
				Alias:     n.Alias,
				Index:     idx.Name,
				Columns:   n.Columns,
				HeadPage:  uint32(rel.HeadPage),
				IndexRoot: uint32(idx.HeadPage),
				Key:       lit,
				Estimate:  PlanCost{Total: idxCost, Rows: 1, Width: 40},
			}
		}
	}
	return nil
}

func (o *Optimizer) optimizeFilter(n *LogicalFilter) (PhysicalNode, error) {
	// Try to push the filter into a scan.
	if scan, ok := n.Child.(*LogicalScan); ok {
		return o.optimizeScan(scan, n.Predicate)
	}

	child, err := o.Optimize(n.Child)
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

func (o *Optimizer) optimizeProject(n *LogicalProject) (PhysicalNode, error) {
	child, err := o.Optimize(n.Child)
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
	left, err := o.Optimize(n.Left)
	if err != nil {
		return nil, err
	}
	right, err := o.Optimize(n.Right)
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

	// Try hash join for equi-joins.
	if n.Condition != nil && isEquiJoin(n.Condition) {
		hashCost := PlanCost{
			Startup: rightCost.Total + rightCost.Rows*o.Costs.CPUTupleCost,
			Total:   rightCost.Total + rightCost.Rows*o.Costs.CPUTupleCost + leftCost.Total + leftCost.Rows*o.Costs.CPUOperatorCost,
			Rows:    joinRows,
			Width:   leftCost.Width + rightCost.Width,
		}

		nlCost := PlanCost{
			Startup: leftCost.Startup + rightCost.Startup,
			Total:   leftCost.Total + leftCost.Rows*rightCost.Total,
			Rows:    joinRows,
			Width:   leftCost.Width + rightCost.Width,
		}

		if hashCost.Total < nlCost.Total {
			return &PhysHashJoin{
				Type: n.Type, Condition: n.Condition,
				Outer: left, Inner: right, Estimate: hashCost,
			}, nil
		}
	}

	// Nested loop join.
	nlCost := PlanCost{
		Startup: leftCost.Startup + rightCost.Startup,
		Total:   leftCost.Total + leftCost.Rows*rightCost.Total,
		Rows:    joinRows,
		Width:   leftCost.Width + rightCost.Width,
	}

	return &PhysNestedLoopJoin{
		Type: n.Type, Condition: n.Condition,
		Outer: left, Inner: right, Estimate: nlCost,
	}, nil
}

func (o *Optimizer) optimizeLimit(n *LogicalLimit) (PhysicalNode, error) {
	child, err := o.Optimize(n.Child)
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
	child, err := o.Optimize(n.Child)
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
	child, err := o.Optimize(n.Child)
	if err != nil {
		return nil, err
	}
	return &PhysDelete{Table: n.Table, Child: child}, nil
}

func (o *Optimizer) optimizeUpdate(n *LogicalUpdate) (PhysicalNode, error) {
	child, err := o.Optimize(n.Child)
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

func estimateSelectivity(pred Expr, rows float64) float64 {
	switch e := pred.(type) {
	case *ExprBinOp:
		switch e.Op {
		case OpEq:
			if rows > 0 {
				distinct := math.Sqrt(rows) // rough estimate
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
			return estimateSelectivity(e.Left, rows) * estimateSelectivity(e.Right, rows)
		case OpOr:
			sl := estimateSelectivity(e.Left, rows)
			sr := estimateSelectivity(e.Right, rows)
			return sl + sr - sl*sr
		}
	case *ExprNot:
		return 1.0 - estimateSelectivity(e.Child, rows)
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

// ListIndexesForTable is a helper to look up indexes on a catalog.
// We add this method to the catalog interface.
func init() {
	// This init is intentionally empty — the actual method is on
	// catalog.Catalog. See the note below.
}
