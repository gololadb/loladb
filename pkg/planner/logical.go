package planner

import (
	"fmt"
	"strings"

	"github.com/jespino/loladb/pkg/tuple"
)

// JoinType represents the type of join.
type JoinType int

const (
	JoinInner JoinType = iota
	JoinLeft
	JoinRight
	JoinCross
)

func (j JoinType) String() string {
	switch j {
	case JoinInner:
		return "INNER"
	case JoinLeft:
		return "LEFT"
	case JoinRight:
		return "RIGHT"
	case JoinCross:
		return "CROSS"
	default:
		return "?"
	}
}

// LogicalNode is a node in a logical plan tree.
type LogicalNode interface {
	fmt.Stringer
	// OutputColumns returns the qualified column names this node produces.
	OutputColumns() []string
}

// LogicalScan reads all rows from a table.
type LogicalScan struct {
	Table   string
	Alias   string
	Columns []string // column names from catalog
}

func (n *LogicalScan) String() string {
	if n.Alias != "" && n.Alias != n.Table {
		return fmt.Sprintf("Scan(%s AS %s)", n.Table, n.Alias)
	}
	return fmt.Sprintf("Scan(%s)", n.Table)
}

func (n *LogicalScan) OutputColumns() []string {
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

// LogicalFilter applies a predicate to its child's output.
type LogicalFilter struct {
	Predicate Expr
	Child     LogicalNode
}

func (n *LogicalFilter) String() string {
	return fmt.Sprintf("Filter(%s, %s)", n.Predicate, n.Child)
}
func (n *LogicalFilter) OutputColumns() []string { return n.Child.OutputColumns() }

// LogicalProject selects/computes output columns.
type LogicalProject struct {
	Exprs  []Expr
	Names  []string // output column names
	Child  LogicalNode
}

func (n *LogicalProject) String() string {
	names := strings.Join(n.Names, ", ")
	return fmt.Sprintf("Project(%s, %s)", names, n.Child)
}
func (n *LogicalProject) OutputColumns() []string { return n.Names }

// LogicalJoin joins two relations.
type LogicalJoin struct {
	Type      JoinType
	Condition Expr // nil for CROSS JOIN
	Left      LogicalNode
	Right     LogicalNode
}

func (n *LogicalJoin) String() string {
	if n.Condition != nil {
		return fmt.Sprintf("%s Join(%s, %s, %s)", n.Type, n.Condition, n.Left, n.Right)
	}
	return fmt.Sprintf("%s Join(%s, %s)", n.Type, n.Left, n.Right)
}

func (n *LogicalJoin) OutputColumns() []string {
	return append(n.Left.OutputColumns(), n.Right.OutputColumns()...)
}

// LogicalLimit limits output rows.
type LogicalLimit struct {
	Count  int64
	Offset int64
	Child  LogicalNode
}

func (n *LogicalLimit) String() string {
	return fmt.Sprintf("Limit(%d, offset=%d, %s)", n.Count, n.Offset, n.Child)
}
func (n *LogicalLimit) OutputColumns() []string { return n.Child.OutputColumns() }

// LogicalSort orders output rows.
type LogicalSort struct {
	Keys  []SortKey
	Child LogicalNode
}

type SortKey struct {
	Expr Expr
	Desc bool
}

func (n *LogicalSort) String() string { return fmt.Sprintf("Sort(%s)", n.Child) }
func (n *LogicalSort) OutputColumns() []string { return n.Child.OutputColumns() }

// --- DML nodes ---

type Assignment struct {
	Column string
	Value  Expr
}

type LogicalInsert struct {
	Table  string
	Values [][]Expr // each inner slice is a row
}

func (n *LogicalInsert) String() string        { return fmt.Sprintf("Insert(%s)", n.Table) }
func (n *LogicalInsert) OutputColumns() []string { return nil }

type LogicalDelete struct {
	Table     string
	Predicate Expr // nil = delete all
	Child     LogicalNode
}

func (n *LogicalDelete) String() string        { return fmt.Sprintf("Delete(%s)", n.Table) }
func (n *LogicalDelete) OutputColumns() []string { return nil }

type LogicalUpdate struct {
	Table       string
	Assignments []Assignment
	Predicate   Expr // nil = update all
	Child       LogicalNode
	Columns     []string // schema column names
	ColTypes    []tuple.DatumType
}

func (n *LogicalUpdate) String() string        { return fmt.Sprintf("Update(%s)", n.Table) }
func (n *LogicalUpdate) OutputColumns() []string { return nil }

type LogicalCreateTable struct {
	Table   string
	Columns []ColDef
}

type ColDef struct {
	Name string
	Type tuple.DatumType
}

func (n *LogicalCreateTable) String() string        { return fmt.Sprintf("CreateTable(%s)", n.Table) }
func (n *LogicalCreateTable) OutputColumns() []string { return nil }

type LogicalCreateIndex struct {
	Index  string
	Table  string
	Column string
}

func (n *LogicalCreateIndex) String() string        { return fmt.Sprintf("CreateIndex(%s)", n.Index) }
func (n *LogicalCreateIndex) OutputColumns() []string { return nil }

// LogicalExplain wraps another plan for EXPLAIN output.
type LogicalExplain struct {
	Child   LogicalNode
	Analyze bool
}

func (n *LogicalExplain) String() string        { return fmt.Sprintf("Explain(%s)", n.Child) }
func (n *LogicalExplain) OutputColumns() []string { return []string{"plan"} }
