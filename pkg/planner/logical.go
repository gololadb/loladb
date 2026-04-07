package planner

import (
	"fmt"
	"strings"

	"github.com/gololadb/loladb/pkg/tuple"
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

// LogicalSetOp represents UNION / INTERSECT / EXCEPT.
type LogicalSetOp struct {
	Op    SetOpKind
	All   bool
	Left  LogicalNode
	Right LogicalNode
}

func (n *LogicalSetOp) String() string {
	op := "Union"
	switch n.Op {
	case SetOpIntersect:
		op = "Intersect"
	case SetOpExcept:
		op = "Except"
	}
	if n.All {
		op += " All"
	}
	return fmt.Sprintf("%s(%s, %s)", op, n.Left, n.Right)
}
func (n *LogicalSetOp) OutputColumns() []string { return n.Left.OutputColumns() }

// LogicalDistinct removes duplicate rows.
type LogicalDistinct struct {
	Child LogicalNode
}

func (n *LogicalDistinct) String() string        { return fmt.Sprintf("Distinct(%s)", n.Child) }
func (n *LogicalDistinct) OutputColumns() []string { return n.Child.OutputColumns() }

// --- DML nodes ---

type Assignment struct {
	Column string
	Value  Expr
}

type LogicalInsert struct {
	Table          string
	Columns        []string // explicit column list (nil = all columns in order)
	Values         [][]Expr // each inner slice is a row
	ReturningExprs []Expr
	ReturningNames []string
	OnConflict     *OnConflictPlan // nil = no ON CONFLICT
}

// OnConflictPlan holds the plan-level ON CONFLICT information.
type OnConflictPlan struct {
	Action       OnConflictAction
	ConflictCols []string     // columns that form the conflict target
	Assignments  []Assignment // SET assignments for DO UPDATE
	WhereExpr    Expr         // optional WHERE on DO UPDATE
}

func (n *LogicalInsert) String() string        { return fmt.Sprintf("Insert(%s)", n.Table) }
func (n *LogicalInsert) OutputColumns() []string { return n.ReturningNames }

// LogicalInsertSelect represents INSERT ... SELECT.
type LogicalInsertSelect struct {
	Table      string
	Columns    []string
	SelectPlan LogicalNode
}

func (n *LogicalInsertSelect) String() string        { return fmt.Sprintf("InsertSelect(%s)", n.Table) }
func (n *LogicalInsertSelect) OutputColumns() []string { return nil }

type LogicalDelete struct {
	Table          string
	Predicate      Expr // nil = delete all
	Child          LogicalNode
	ReturningExprs []Expr
	ReturningNames []string
}

func (n *LogicalDelete) String() string        { return fmt.Sprintf("Delete(%s)", n.Table) }
func (n *LogicalDelete) OutputColumns() []string { return n.ReturningNames }

type LogicalUpdate struct {
	Table          string
	Assignments    []Assignment
	Predicate      Expr // nil = update all
	Child          LogicalNode
	Columns        []string // schema column names
	ColTypes       []tuple.DatumType
	ReturningExprs []Expr
	ReturningNames []string
}

func (n *LogicalUpdate) String() string        { return fmt.Sprintf("Update(%s)", n.Table) }
func (n *LogicalUpdate) OutputColumns() []string { return n.ReturningNames }

type LogicalCreateTable struct {
	Table       string
	Schema      string // target schema (empty = current)
	Columns     []ColDef
	ForeignKeys []ForeignKeyDef
}

type ColDef struct {
	Name        string
	Type        tuple.DatumType
	TypeName    string // original SQL type name (for domain/enum validation)
	Typmod      int32  // type modifier (-1 = unspecified; for NUMERIC: ((p<<16)|s)+4)
	NotNull     bool   // column-level NOT NULL constraint
	PrimaryKey  bool   // column-level PRIMARY KEY constraint
	Unique      bool   // column-level UNIQUE constraint
	DefaultExpr string // SQL text of DEFAULT expression (empty = no default)
	CheckExpr   string // SQL text of CHECK expression (empty = no check)
	CheckName   string // optional constraint name for CHECK
}

// ForeignKeyDef holds a foreign key definition from CREATE TABLE.
type ForeignKeyDef struct {
	Name       string   // constraint name (may be empty)
	Columns    []string // local column(s)
	RefTable   string   // referenced table
	RefColumns []string // referenced column(s)
	OnDelete   string   // action: "", "CASCADE", "SET NULL", "SET DEFAULT", "RESTRICT"
	OnUpdate   string   // action: "", "CASCADE", "SET NULL", "SET DEFAULT", "RESTRICT"
}

func (n *LogicalCreateTable) String() string        { return fmt.Sprintf("CreateTable(%s)", n.Table) }
func (n *LogicalCreateTable) OutputColumns() []string { return nil }

type LogicalCreateIndex struct {
	Index  string
	Table  string
	Column string
	Method string
}

func (n *LogicalCreateIndex) String() string        { return fmt.Sprintf("CreateIndex(%s)", n.Index) }
func (n *LogicalCreateIndex) OutputColumns() []string { return nil }

// LogicalExplain wraps another plan for EXPLAIN output.
type LogicalExplain struct {
	Child   LogicalNode
	Analyze bool
}

func (n *LogicalExplain) String() string         { return fmt.Sprintf("Explain(%s)", n.Child) }
func (n *LogicalExplain) OutputColumns() []string { return []string{"plan"} }

// LogicalNoOp represents a statement that is acknowledged but performs
// no real work (e.g., SET, unsupported DDL we want to skip gracefully).
type LogicalNoOp struct {
	Message string // human-readable description of what was acknowledged
}

func (n *LogicalNoOp) String() string         { return fmt.Sprintf("NoOp(%s)", n.Message) }
func (n *LogicalNoOp) OutputColumns() []string { return nil }

// LogicalCreateSequence represents CREATE SEQUENCE.
type LogicalCreateSequence struct {
	Name string
}

func (n *LogicalCreateSequence) String() string         { return fmt.Sprintf("CreateSequence(%s)", n.Name) }
func (n *LogicalCreateSequence) OutputColumns() []string { return nil }

// LogicalCreateView represents CREATE VIEW.
type LogicalCreateView struct {
	Name       string
	Definition string
	Columns    []ColDef
}

func (n *LogicalCreateView) String() string         { return fmt.Sprintf("CreateView(%s)", n.Name) }
func (n *LogicalCreateView) OutputColumns() []string { return nil }

// LogicalAlterTable represents ALTER TABLE operations.
type LogicalAlterTable struct {
	Table    string
	Commands []string // human-readable descriptions of each ALTER command
}

func (n *LogicalAlterTable) String() string         { return fmt.Sprintf("AlterTable(%s)", n.Table) }
func (n *LogicalAlterTable) OutputColumns() []string { return nil }

// LogicalCreatePolicy represents CREATE POLICY.
type LogicalCreatePolicy struct {
	Name       string
	Table      string
	Cmd        string
	Permissive bool
	Roles      []string
	Using      string
	Check      string
}

func (n *LogicalCreatePolicy) String() string         { return fmt.Sprintf("CreatePolicy(%s)", n.Name) }
func (n *LogicalCreatePolicy) OutputColumns() []string { return nil }

// LogicalEnableRLS represents ALTER TABLE ... ENABLE ROW LEVEL SECURITY.
type LogicalEnableRLS struct {
	Table string
}

func (n *LogicalEnableRLS) String() string         { return fmt.Sprintf("EnableRLS(%s)", n.Table) }
func (n *LogicalEnableRLS) OutputColumns() []string { return nil }

// LogicalDisableRLS represents ALTER TABLE ... DISABLE ROW LEVEL SECURITY.
type LogicalDisableRLS struct {
	Table string
}

func (n *LogicalDisableRLS) String() string         { return fmt.Sprintf("DisableRLS(%s)", n.Table) }
func (n *LogicalDisableRLS) OutputColumns() []string { return nil }

// LogicalCreateRole represents CREATE ROLE / CREATE USER.
type LogicalCreateRole struct {
	RoleName string
	Options  map[string]interface{}
	StmtType string
}

func (n *LogicalCreateRole) String() string         { return fmt.Sprintf("CreateRole(%s)", n.RoleName) }
func (n *LogicalCreateRole) OutputColumns() []string { return nil }

// LogicalAlterRole represents ALTER ROLE / ALTER USER.
type LogicalAlterRole struct {
	RoleName string
	Options  map[string]interface{}
}

func (n *LogicalAlterRole) String() string         { return fmt.Sprintf("AlterRole(%s)", n.RoleName) }
func (n *LogicalAlterRole) OutputColumns() []string { return nil }

// LogicalDropRole represents DROP ROLE / DROP USER.
type LogicalDropRole struct {
	Roles     []string
	MissingOk bool
}

func (n *LogicalDropRole) String() string         { return fmt.Sprintf("DropRole(%v)", n.Roles) }
func (n *LogicalDropRole) OutputColumns() []string { return nil }

// LogicalGrantRole represents GRANT role TO role.
type LogicalGrantRole struct {
	GrantedRoles []string
	Grantees     []string
	AdminOption  bool
}

func (n *LogicalGrantRole) String() string         { return fmt.Sprintf("GrantRole(%v TO %v)", n.GrantedRoles, n.Grantees) }
func (n *LogicalGrantRole) OutputColumns() []string { return nil }

// LogicalRevokeRole represents REVOKE role FROM role.
type LogicalRevokeRole struct {
	RevokedRoles []string
	Grantees     []string
}

func (n *LogicalRevokeRole) String() string         { return fmt.Sprintf("RevokeRole(%v FROM %v)", n.RevokedRoles, n.Grantees) }
func (n *LogicalRevokeRole) OutputColumns() []string { return nil }

// LogicalGrantPrivilege represents GRANT privileges ON object TO role.
type LogicalGrantPrivilege struct {
	Privileges  []string
	PrivCols    [][]string
	TargetType  string
	Objects     []string
	Grantees    []string
	GrantOption bool
}

func (n *LogicalGrantPrivilege) String() string         { return fmt.Sprintf("Grant(%v ON %v TO %v)", n.Privileges, n.Objects, n.Grantees) }
func (n *LogicalGrantPrivilege) OutputColumns() []string { return nil }

// LogicalRevokePrivilege represents REVOKE privileges ON object FROM role.
type LogicalRevokePrivilege struct {
	Privileges []string
	PrivCols   [][]string
	TargetType string
	Objects    []string
	Grantees   []string
}

func (n *LogicalRevokePrivilege) String() string         { return fmt.Sprintf("Revoke(%v ON %v FROM %v)", n.Privileges, n.Objects, n.Grantees) }
func (n *LogicalRevokePrivilege) OutputColumns() []string { return nil }


// LogicalCreateFunction represents CREATE FUNCTION.
type LogicalCreateFunction struct {
	Name       string
	Language   string
	Body       string
	ReturnType string
	ParamNames []string
	ParamTypes []string
	Replace    bool
}

func (n *LogicalCreateFunction) String() string         { return fmt.Sprintf("CreateFunction(%s)", n.Name) }
func (n *LogicalCreateFunction) OutputColumns() []string { return nil }

// LogicalCreateTrigger represents CREATE TRIGGER.
type LogicalCreateTrigger struct {
	TrigName string
	Table    string
	FuncName string
	Timing   int
	Events   int
	ForEach  string
	Replace  bool
}

func (n *LogicalCreateTrigger) String() string         { return fmt.Sprintf("CreateTrigger(%s ON %s)", n.TrigName, n.Table) }
func (n *LogicalCreateTrigger) OutputColumns() []string { return nil }

// LogicalDropFunction represents DROP FUNCTION.
type LogicalDropFunction struct {
	Name      string
	MissingOk bool
}

func (n *LogicalDropFunction) String() string         { return fmt.Sprintf("DropFunction(%s)", n.Name) }
func (n *LogicalDropFunction) OutputColumns() []string { return nil }

// LogicalDropTrigger represents DROP TRIGGER ... ON table.
type LogicalDropTrigger struct {
	TrigName  string
	Table     string
	MissingOk bool
}

func (n *LogicalDropTrigger) String() string         { return fmt.Sprintf("DropTrigger(%s ON %s)", n.TrigName, n.Table) }
func (n *LogicalDropTrigger) OutputColumns() []string { return nil }

// LogicalAlterFunction represents ALTER FUNCTION.
type LogicalAlterFunction struct {
	Name     string
	NewName  string
	NewOwner string
}

func (n *LogicalAlterFunction) String() string         { return fmt.Sprintf("AlterFunction(%s)", n.Name) }
func (n *LogicalAlterFunction) OutputColumns() []string { return nil }

// LogicalCreateDomain represents CREATE DOMAIN.
type LogicalCreateDomain struct {
	Name      string
	BaseType  string
	NotNull   bool
	CheckExpr string
}

func (n *LogicalCreateDomain) String() string         { return fmt.Sprintf("CreateDomain(%s)", n.Name) }
func (n *LogicalCreateDomain) OutputColumns() []string { return nil }

// LogicalCreateEnum represents CREATE TYPE ... AS ENUM.
type LogicalCreateEnum struct {
	Name string
	Vals []string
}

func (n *LogicalCreateEnum) String() string         { return fmt.Sprintf("CreateEnum(%s)", n.Name) }
func (n *LogicalCreateEnum) OutputColumns() []string { return nil }

// LogicalDropType represents DROP TYPE / DROP DOMAIN.
type LogicalDropType struct {
	Name      string
	MissingOk bool
}

func (n *LogicalDropType) String() string         { return fmt.Sprintf("DropType(%s)", n.Name) }
func (n *LogicalDropType) OutputColumns() []string { return nil }

// LogicalAlterEnum represents ALTER TYPE ... ADD VALUE.
type LogicalAlterEnum struct {
	Name   string
	NewVal string
}

func (n *LogicalAlterEnum) String() string         { return fmt.Sprintf("AlterEnum(%s)", n.Name) }
func (n *LogicalAlterEnum) OutputColumns() []string { return nil }

// LogicalCreateSchema represents CREATE SCHEMA.
type LogicalCreateSchema struct {
	Name        string
	IfNotExists bool
	AuthRole    string
}

func (n *LogicalCreateSchema) String() string         { return fmt.Sprintf("CreateSchema(%s)", n.Name) }
func (n *LogicalCreateSchema) OutputColumns() []string { return nil }

// LogicalDropSchema represents DROP SCHEMA.
type LogicalDropSchema struct {
	Name      string
	MissingOk bool
	Cascade   bool
}

func (n *LogicalDropSchema) String() string         { return fmt.Sprintf("DropSchema(%s)", n.Name) }
func (n *LogicalDropSchema) OutputColumns() []string { return nil }

// LogicalTruncate represents TRUNCATE TABLE.
type LogicalTruncate struct {
	Table string
}

func (n *LogicalTruncate) String() string         { return fmt.Sprintf("Truncate(%s)", n.Table) }
func (n *LogicalTruncate) OutputColumns() []string { return nil }

// LogicalDropIndex represents DROP INDEX.
type LogicalDropIndex struct {
	Name      string
	MissingOk bool
	Cascade   bool
}

func (n *LogicalDropIndex) String() string         { return fmt.Sprintf("DropIndex(%s)", n.Name) }
func (n *LogicalDropIndex) OutputColumns() []string { return nil }

// LogicalDropView represents DROP VIEW.
type LogicalDropView struct {
	Name      string
	MissingOk bool
	Cascade   bool
}

func (n *LogicalDropView) String() string         { return fmt.Sprintf("DropView(%s)", n.Name) }
func (n *LogicalDropView) OutputColumns() []string { return nil }

// LogicalAddColumn represents ALTER TABLE ... ADD COLUMN.
type LogicalAddColumn struct {
	Table        string
	Col          ColDef
	IfNotExists  bool
}

func (n *LogicalAddColumn) String() string         { return fmt.Sprintf("AddColumn(%s.%s)", n.Table, n.Col.Name) }
func (n *LogicalAddColumn) OutputColumns() []string { return nil }

// LogicalDropColumn represents ALTER TABLE ... DROP COLUMN.
type LogicalDropColumn struct {
	Table    string
	ColName  string
	IfExists bool
}

func (n *LogicalDropColumn) String() string         { return fmt.Sprintf("DropColumn(%s.%s)", n.Table, n.ColName) }
func (n *LogicalDropColumn) OutputColumns() []string { return nil }

// LogicalResult produces a single row by evaluating expressions (SELECT without FROM).
// LogicalAggregate groups input rows and computes aggregate functions.
type LogicalAggregate struct {
	GroupExprs []Expr    // GROUP BY expressions (empty = single group)
	AggDescs   []AggDesc // aggregate function descriptors
	HavingQual Expr      // HAVING filter (nil = no HAVING)
	Child      LogicalNode
}

// AggDesc describes a single aggregate computation.
type AggDesc struct {
	Func     string // "count", "sum", "avg", "min", "max"
	ArgExprs []Expr // argument expressions (empty for count(*))
	Star     bool   // true for count(*)
	Distinct bool
}

func (n *LogicalAggregate) String() string { return "Aggregate" }
func (n *LogicalAggregate) OutputColumns() []string {
	// Output columns: group-by columns + aggregate results.
	var cols []string
	for i := range n.GroupExprs {
		cols = append(cols, fmt.Sprintf("group%d", i))
	}
	for i, ad := range n.AggDescs {
		if ad.Star {
			cols = append(cols, fmt.Sprintf("%s_%d", ad.Func, i))
		} else {
			cols = append(cols, fmt.Sprintf("%s_%d", ad.Func, i))
		}
	}
	return cols
}

type LogicalResult struct {
	Exprs []Expr
	Names []string
}

func (n *LogicalResult) String() string         { return "Result" }
func (n *LogicalResult) OutputColumns() []string { return n.Names }

// LogicalValues produces multiple rows from literal expressions (bare VALUES clause).
type LogicalValues struct {
	Names  []string   // column names (column1, column2, ...)
	Values [][]Expr   // rows of expressions
}

func (n *LogicalValues) String() string         { return "Values" }
func (n *LogicalValues) OutputColumns() []string { return n.Names }

// LogicalWindowAgg computes window functions over the child's output.
type LogicalWindowAgg struct {
	Child    LogicalNode
	WinFuncs []WindowFuncDesc // window function descriptors
}

// WindowFuncDesc describes a single window function computation.
type WindowFuncDesc struct {
	FuncName    string
	ArgExprs    []Expr
	Star        bool
	Distinct    bool
	PartitionBy []Expr
	OrderBy     []SortExpr
	FrameMode   WindowFrameMode
	FrameStart  WindowFrameBound
	FrameEnd    WindowFrameBound
}

// SortExpr is an expression with a sort direction, used in window ORDER BY.
type SortExpr struct {
	Expr Expr
	Desc bool
}

func (n *LogicalWindowAgg) String() string { return "WindowAgg" }
func (n *LogicalWindowAgg) OutputColumns() []string {
	// Window functions append columns to the child's output.
	childCols := n.Child.OutputColumns()
	out := make([]string, len(childCols))
	copy(out, childCols)
	for i := range n.WinFuncs {
		out = append(out, fmt.Sprintf("win_%d", i))
	}
	return out
}

// LogicalSubqueryScan materializes a subquery (CTE or inline subquery)
// and scans the result as if it were a table.
type LogicalSubqueryScan struct {
	Alias      string
	Columns    []string
	ChildPlan  LogicalNode
	IsRecursive bool
	// RecursiveInit is the non-recursive (initial) part for WITH RECURSIVE.
	RecursiveInit LogicalNode
}

func (n *LogicalSubqueryScan) String() string {
	return fmt.Sprintf("SubqueryScan(%s)", n.Alias)
}

func (n *LogicalSubqueryScan) OutputColumns() []string {
	cols := make([]string, len(n.Columns))
	for i, c := range n.Columns {
		cols[i] = n.Alias + "." + c
	}
	return cols
}
