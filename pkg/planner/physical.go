package planner

import "fmt"

// PhysicalNode is a node in a physical (executable) plan tree.
type PhysicalNode interface {
	fmt.Stringer
	Cost() PlanCost
	Children() []PhysicalNode
}

// PhysSeqScan is a full table sequential scan.
type PhysSeqScan struct {
	Table    string
	Alias    string
	Columns  []string
	HeadPage uint32
	Estimate PlanCost
	Filter   Expr // optional pushed-down filter
}

func (n *PhysSeqScan) String() string {
	name := n.Table
	if n.Alias != "" && n.Alias != n.Table {
		name = n.Table + " " + n.Alias
	}
	if n.Filter != nil {
		return fmt.Sprintf("SeqScan on %s (filter: %s)", name, n.Filter)
	}
	return fmt.Sprintf("SeqScan on %s", name)
}
func (n *PhysSeqScan) Cost() PlanCost       { return n.Estimate }
func (n *PhysSeqScan) Children() []PhysicalNode { return nil }

// PhysIndexScan uses a B+Tree index for lookup.
type PhysIndexScan struct {
	Table     string
	Alias     string
	Index     string
	Columns   []string
	HeadPage  uint32
	IndexRoot uint32
	Key       Expr   // exact key for equality
	Lo, Hi    Expr   // range bounds (nil = unbounded)
	Estimate  PlanCost
}

func (n *PhysIndexScan) String() string {
	name := n.Table
	if n.Alias != "" && n.Alias != n.Table {
		name = n.Table + " " + n.Alias
	}
	if n.Key != nil {
		return fmt.Sprintf("IndexScan on %s using %s (key=%s)", name, n.Index, n.Key)
	}
	return fmt.Sprintf("IndexScan on %s using %s", name, n.Index)
}
func (n *PhysIndexScan) Cost() PlanCost       { return n.Estimate }
func (n *PhysIndexScan) Children() []PhysicalNode { return nil }

// PhysFilter evaluates a predicate per row.
type PhysFilter struct {
	Predicate Expr
	Child     PhysicalNode
	Estimate  PlanCost
}

func (n *PhysFilter) String() string {
	return fmt.Sprintf("Filter (%s)", n.Predicate)
}
func (n *PhysFilter) Cost() PlanCost       { return n.Estimate }
func (n *PhysFilter) Children() []PhysicalNode { return []PhysicalNode{n.Child} }

// PhysProject computes output columns.
type PhysProject struct {
	Exprs    []Expr
	Names    []string
	Child    PhysicalNode
	Estimate PlanCost
}

func (n *PhysProject) String() string {
	return fmt.Sprintf("Project (%s)", fmt.Sprint(n.Names))
}
func (n *PhysProject) Cost() PlanCost       { return n.Estimate }
func (n *PhysProject) Children() []PhysicalNode { return []PhysicalNode{n.Child} }

// PhysNestedLoopJoin performs a nested loop join.
type PhysNestedLoopJoin struct {
	Type      JoinType
	Condition Expr
	Outer     PhysicalNode
	Inner     PhysicalNode
	Estimate  PlanCost
}

func (n *PhysNestedLoopJoin) String() string {
	if n.Condition != nil {
		return fmt.Sprintf("NestedLoop %s Join (%s)", n.Type, n.Condition)
	}
	return fmt.Sprintf("NestedLoop %s Join", n.Type)
}
func (n *PhysNestedLoopJoin) Cost() PlanCost       { return n.Estimate }
func (n *PhysNestedLoopJoin) Children() []PhysicalNode { return []PhysicalNode{n.Outer, n.Inner} }

// PhysHashJoin performs a hash join on an equi-join condition.
type PhysHashJoin struct {
	Type      JoinType
	Condition Expr
	Outer     PhysicalNode // probe side
	Inner     PhysicalNode // build side
	Estimate  PlanCost
}

func (n *PhysHashJoin) String() string {
	return fmt.Sprintf("Hash %s Join (%s)", n.Type, n.Condition)
}
func (n *PhysHashJoin) Cost() PlanCost       { return n.Estimate }
func (n *PhysHashJoin) Children() []PhysicalNode { return []PhysicalNode{n.Outer, n.Inner} }

// PhysLimit limits output.
type PhysLimit struct {
	Count    int64
	Offset   int64
	Child    PhysicalNode
	Estimate PlanCost
}

func (n *PhysLimit) String() string {
	return fmt.Sprintf("Limit (count=%d offset=%d)", n.Count, n.Offset)
}
func (n *PhysLimit) Cost() PlanCost       { return n.Estimate }
func (n *PhysLimit) Children() []PhysicalNode { return []PhysicalNode{n.Child} }

// PhysSort sorts output.
type PhysSort struct {
	Keys     []SortKey
	Child    PhysicalNode
	Estimate PlanCost
}

func (n *PhysSort) String() string            { return "Sort" }
func (n *PhysSort) Cost() PlanCost            { return n.Estimate }
func (n *PhysSort) Children() []PhysicalNode  { return []PhysicalNode{n.Child} }

// --- DML physical nodes ---

type PhysInsert struct {
	Table    string
	Values   [][]Expr
	Estimate PlanCost
}

func (n *PhysInsert) String() string            { return fmt.Sprintf("Insert on %s", n.Table) }
func (n *PhysInsert) Cost() PlanCost            { return n.Estimate }
func (n *PhysInsert) Children() []PhysicalNode  { return nil }

type PhysDelete struct {
	Table    string
	Child    PhysicalNode
	Estimate PlanCost
}

func (n *PhysDelete) String() string            { return fmt.Sprintf("Delete on %s", n.Table) }
func (n *PhysDelete) Cost() PlanCost            { return n.Estimate }
func (n *PhysDelete) Children() []PhysicalNode  { return []PhysicalNode{n.Child} }

type PhysUpdate struct {
	Table       string
	Assignments []Assignment
	Columns     []string
	ColTypes    []DatumType
	Child       PhysicalNode
	Estimate    PlanCost
}

type DatumType = uint8

func (n *PhysUpdate) String() string            { return fmt.Sprintf("Update on %s", n.Table) }
func (n *PhysUpdate) Cost() PlanCost            { return n.Estimate }
func (n *PhysUpdate) Children() []PhysicalNode  { return []PhysicalNode{n.Child} }

type PhysCreateTable struct {
	Table   string
	Columns []ColDef
}

func (n *PhysCreateTable) String() string            { return fmt.Sprintf("CreateTable %s", n.Table) }
func (n *PhysCreateTable) Cost() PlanCost            { return PlanCost{} }
func (n *PhysCreateTable) Children() []PhysicalNode  { return nil }

type PhysCreateIndex struct {
	Index  string
	Table  string
	Column string
}

func (n *PhysCreateIndex) String() string            { return fmt.Sprintf("CreateIndex %s", n.Index) }
func (n *PhysCreateIndex) Cost() PlanCost            { return PlanCost{} }
func (n *PhysCreateIndex) Children() []PhysicalNode  { return nil }
