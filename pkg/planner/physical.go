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
	Table      string
	Alias      string
	Columns    []string
	HeadPage   uint32
	Estimate   PlanCost
	Filter     Expr // optional pushed-down filter
	IsTerminal bool // true when no Project node narrows the output columns
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

// PhysBitmapHeapScan fetches heap tuples identified by a child bitmap
// index scan, reading pages in physical order to reduce random I/O.
// Mirrors PostgreSQL's BitmapHeapScan node.
type PhysBitmapHeapScan struct {
	Table    string
	Alias    string
	Columns  []string
	HeadPage uint32
	Recheck  Expr // recheck condition applied to each fetched tuple
	Child    PhysicalNode // must be a PhysBitmapIndexScan
	Estimate PlanCost
}

func (n *PhysBitmapHeapScan) String() string {
	name := n.Table
	if n.Alias != "" && n.Alias != n.Table {
		name = n.Table + " " + n.Alias
	}
	if n.Recheck != nil {
		return fmt.Sprintf("Bitmap Heap Scan on %s (recheck: %s)", name, n.Recheck)
	}
	return fmt.Sprintf("Bitmap Heap Scan on %s", name)
}
func (n *PhysBitmapHeapScan) Cost() PlanCost       { return n.Estimate }
func (n *PhysBitmapHeapScan) Children() []PhysicalNode { return []PhysicalNode{n.Child} }

// PhysBitmapIndexScan scans an index and produces a set of TIDs
// (represented as a sorted list of page+slot pairs). It does not
// fetch heap tuples itself.
type PhysBitmapIndexScan struct {
	Table     string
	Index     string
	IndexRoot uint32
	Key       Expr
	Estimate  PlanCost
}

func (n *PhysBitmapIndexScan) String() string {
	if n.Key != nil {
		return fmt.Sprintf("Bitmap Index Scan on %s (key=%s)", n.Index, n.Key)
	}
	return fmt.Sprintf("Bitmap Index Scan on %s", n.Index)
}
func (n *PhysBitmapIndexScan) Cost() PlanCost       { return n.Estimate }
func (n *PhysBitmapIndexScan) Children() []PhysicalNode { return nil }

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
// When InnerParam is set, the inner side is re-executed for each outer
// row with the parameterized column value substituted. This mirrors
// PostgreSQL's parameterized path mechanism.
type PhysNestedLoopJoin struct {
	Type      JoinType
	Condition Expr
	Outer     PhysicalNode
	Inner     PhysicalNode
	Estimate  PlanCost
	// InnerParam, when non-nil, describes a parameterized inner index scan.
	// The inner PhysIndexScan's Key is replaced at execution time with the
	// value of OuterCol from each outer row.
	InnerParam *NestLoopParam
}

// NestLoopParam describes how to parameterize the inner side of a
// nested loop join. OuterCol is the column name (table.col) from the
// outer side whose value is passed as the index key for the inner scan.
type NestLoopParam struct {
	OuterCol string // qualified column name from outer side
}

func (n *PhysNestedLoopJoin) String() string {
	if n.InnerParam != nil {
		return fmt.Sprintf("NestedLoop %s Join (param: %s)", n.Type, n.InnerParam.OuterCol)
	}
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
	Columns  []string // explicit column list (nil = all columns in order)
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
	Schema  string // target schema (empty = current)
	Columns []ColDef
}

func (n *PhysCreateTable) String() string            { return fmt.Sprintf("CreateTable %s", n.Table) }
func (n *PhysCreateTable) Cost() PlanCost            { return PlanCost{} }
func (n *PhysCreateTable) Children() []PhysicalNode  { return nil }

type PhysCreateIndex struct {
	Index  string
	Table  string
	Column string
	Method string
}

func (n *PhysCreateIndex) String() string            { return fmt.Sprintf("CreateIndex %s", n.Index) }
func (n *PhysCreateIndex) Cost() PlanCost            { return PlanCost{} }
func (n *PhysCreateIndex) Children() []PhysicalNode  { return nil }

// PhysNoOp is a physical node that produces a message but does no real work.
type PhysNoOp struct {
	Message string
}

func (n *PhysNoOp) String() string            { return fmt.Sprintf("NoOp: %s", n.Message) }
func (n *PhysNoOp) Cost() PlanCost            { return PlanCost{} }
func (n *PhysNoOp) Children() []PhysicalNode  { return nil }

// PhysCreateSequence creates a sequence (currently stored as acknowledgement).
type PhysCreateSequence struct {
	Name string
}

func (n *PhysCreateSequence) String() string            { return fmt.Sprintf("CreateSequence %s", n.Name) }
func (n *PhysCreateSequence) Cost() PlanCost            { return PlanCost{} }
func (n *PhysCreateSequence) Children() []PhysicalNode  { return nil }

// PhysCreateView stores a view definition.
type PhysCreateView struct {
	Name       string
	Definition string
	Columns    []ColDef // resolved column definitions for the view
}

func (n *PhysCreateView) String() string            { return fmt.Sprintf("CreateView %s", n.Name) }
func (n *PhysCreateView) Cost() PlanCost            { return PlanCost{} }
func (n *PhysCreateView) Children() []PhysicalNode  { return nil }

// PhysAlterTable represents ALTER TABLE operations.
type PhysAlterTable struct {
	Table    string
	Commands []string
}

func (n *PhysAlterTable) String() string            { return fmt.Sprintf("AlterTable %s", n.Table) }
func (n *PhysAlterTable) Cost() PlanCost            { return PlanCost{} }
func (n *PhysAlterTable) Children() []PhysicalNode  { return nil }

// PhysCreatePolicy represents CREATE POLICY.
type PhysCreatePolicy struct {
	Name       string
	Table      string
	Cmd        string
	Permissive bool
	Roles      []string
	Using      string
	Check      string
}

func (n *PhysCreatePolicy) String() string            { return fmt.Sprintf("CreatePolicy %s ON %s", n.Name, n.Table) }
func (n *PhysCreatePolicy) Cost() PlanCost            { return PlanCost{} }
func (n *PhysCreatePolicy) Children() []PhysicalNode  { return nil }

// PhysEnableRLS represents ALTER TABLE ... ENABLE ROW LEVEL SECURITY.
type PhysEnableRLS struct {
	Table string
}

func (n *PhysEnableRLS) String() string            { return fmt.Sprintf("EnableRLS %s", n.Table) }
func (n *PhysEnableRLS) Cost() PlanCost            { return PlanCost{} }
func (n *PhysEnableRLS) Children() []PhysicalNode  { return nil }

// PhysDisableRLS represents ALTER TABLE ... DISABLE ROW LEVEL SECURITY.
type PhysDisableRLS struct {
	Table string
}

func (n *PhysDisableRLS) String() string            { return fmt.Sprintf("DisableRLS %s", n.Table) }
func (n *PhysDisableRLS) Cost() PlanCost            { return PlanCost{} }
func (n *PhysDisableRLS) Children() []PhysicalNode  { return nil }

// PhysCreateRole represents CREATE ROLE / CREATE USER.
type PhysCreateRole struct {
	RoleName    string
	Options     map[string]interface{}
	StmtType    string // "ROLE", "USER", "GROUP"
}

func (n *PhysCreateRole) String() string            { return fmt.Sprintf("CreateRole %s", n.RoleName) }
func (n *PhysCreateRole) Cost() PlanCost            { return PlanCost{} }
func (n *PhysCreateRole) Children() []PhysicalNode  { return nil }

// PhysAlterRole represents ALTER ROLE / ALTER USER.
type PhysAlterRole struct {
	RoleName string
	Options  map[string]interface{}
}

func (n *PhysAlterRole) String() string            { return fmt.Sprintf("AlterRole %s", n.RoleName) }
func (n *PhysAlterRole) Cost() PlanCost            { return PlanCost{} }
func (n *PhysAlterRole) Children() []PhysicalNode  { return nil }

// PhysDropRole represents DROP ROLE / DROP USER.
type PhysDropRole struct {
	Roles     []string
	MissingOk bool
}

func (n *PhysDropRole) String() string            { return fmt.Sprintf("DropRole %v", n.Roles) }
func (n *PhysDropRole) Cost() PlanCost            { return PlanCost{} }
func (n *PhysDropRole) Children() []PhysicalNode  { return nil }

// PhysGrantRole represents GRANT role TO role.
type PhysGrantRole struct {
	GrantedRoles []string
	Grantees     []string
	AdminOption  bool
}

func (n *PhysGrantRole) String() string            { return fmt.Sprintf("GrantRole %v TO %v", n.GrantedRoles, n.Grantees) }
func (n *PhysGrantRole) Cost() PlanCost            { return PlanCost{} }
func (n *PhysGrantRole) Children() []PhysicalNode  { return nil }

// PhysRevokeRole represents REVOKE role FROM role.
type PhysRevokeRole struct {
	RevokedRoles []string
	Grantees     []string
}

func (n *PhysRevokeRole) String() string            { return fmt.Sprintf("RevokeRole %v FROM %v", n.RevokedRoles, n.Grantees) }
func (n *PhysRevokeRole) Cost() PlanCost            { return PlanCost{} }
func (n *PhysRevokeRole) Children() []PhysicalNode  { return nil }

// PhysGrantPrivilege represents GRANT privileges ON object TO role.
type PhysGrantPrivilege struct {
	Privileges  []string
	PrivCols    [][]string // per-privilege column lists
	TargetType  string
	Objects     []string
	Grantees    []string
	GrantOption bool
}

func (n *PhysGrantPrivilege) String() string            { return fmt.Sprintf("Grant %v ON %v TO %v", n.Privileges, n.Objects, n.Grantees) }
func (n *PhysGrantPrivilege) Cost() PlanCost            { return PlanCost{} }
func (n *PhysGrantPrivilege) Children() []PhysicalNode  { return nil }

// PhysRevokePrivilege represents REVOKE privileges ON object FROM role.
type PhysRevokePrivilege struct {
	Privileges []string
	PrivCols   [][]string
	TargetType string
	Objects    []string
	Grantees   []string
}

func (n *PhysRevokePrivilege) String() string            { return fmt.Sprintf("Revoke %v ON %v FROM %v", n.Privileges, n.Objects, n.Grantees) }
func (n *PhysRevokePrivilege) Cost() PlanCost            { return PlanCost{} }
func (n *PhysRevokePrivilege) Children() []PhysicalNode  { return nil }

// PhysCreateFunction represents CREATE [OR REPLACE] FUNCTION.
type PhysCreateFunction struct {
	Name       string
	Language   string
	Body       string
	ReturnType string
	ParamNames []string
	ParamTypes []string
	Replace    bool
}

func (n *PhysCreateFunction) String() string            { return fmt.Sprintf("CreateFunction %s", n.Name) }
func (n *PhysCreateFunction) Cost() PlanCost            { return PlanCost{} }
func (n *PhysCreateFunction) Children() []PhysicalNode  { return nil }

// PhysCreateTrigger represents CREATE TRIGGER.
type PhysCreateTrigger struct {
	TrigName string
	Table    string
	FuncName string
	Timing   int
	Events   int
	ForEach  string // "ROW" or "STATEMENT"
	Replace  bool
}

func (n *PhysCreateTrigger) String() string            { return fmt.Sprintf("CreateTrigger %s ON %s", n.TrigName, n.Table) }
func (n *PhysCreateTrigger) Cost() PlanCost            { return PlanCost{} }
func (n *PhysCreateTrigger) Children() []PhysicalNode  { return nil }

// PhysDropFunction represents DROP FUNCTION.
type PhysDropFunction struct {
	Name      string
	MissingOk bool
}

func (n *PhysDropFunction) String() string            { return fmt.Sprintf("DropFunction %s", n.Name) }
func (n *PhysDropFunction) Cost() PlanCost            { return PlanCost{} }
func (n *PhysDropFunction) Children() []PhysicalNode  { return nil }

// PhysDropTrigger represents DROP TRIGGER ... ON table.
type PhysDropTrigger struct {
	TrigName  string
	Table     string
	MissingOk bool
}

func (n *PhysDropTrigger) String() string            { return fmt.Sprintf("DropTrigger %s ON %s", n.TrigName, n.Table) }
func (n *PhysDropTrigger) Cost() PlanCost            { return PlanCost{} }
func (n *PhysDropTrigger) Children() []PhysicalNode  { return nil }

// PhysAlterFunction represents ALTER FUNCTION.
type PhysAlterFunction struct {
	Name     string
	NewName  string
	NewOwner string
}

func (n *PhysAlterFunction) String() string            { return fmt.Sprintf("AlterFunction %s", n.Name) }
func (n *PhysAlterFunction) Cost() PlanCost            { return PlanCost{} }
func (n *PhysAlterFunction) Children() []PhysicalNode  { return nil }

// PhysCreateDomain represents CREATE DOMAIN.
type PhysCreateDomain struct {
	Name      string
	BaseType  string
	NotNull   bool
	CheckExpr string
}

func (n *PhysCreateDomain) String() string            { return fmt.Sprintf("CreateDomain %s", n.Name) }
func (n *PhysCreateDomain) Cost() PlanCost            { return PlanCost{} }
func (n *PhysCreateDomain) Children() []PhysicalNode  { return nil }

// PhysCreateEnum represents CREATE TYPE ... AS ENUM.
type PhysCreateEnum struct {
	Name string
	Vals []string
}

func (n *PhysCreateEnum) String() string            { return fmt.Sprintf("CreateEnum %s", n.Name) }
func (n *PhysCreateEnum) Cost() PlanCost            { return PlanCost{} }
func (n *PhysCreateEnum) Children() []PhysicalNode  { return nil }

// PhysDropType represents DROP TYPE / DROP DOMAIN.
type PhysDropType struct {
	Name      string
	MissingOk bool
}

func (n *PhysDropType) String() string            { return fmt.Sprintf("DropType %s", n.Name) }
func (n *PhysDropType) Cost() PlanCost            { return PlanCost{} }
func (n *PhysDropType) Children() []PhysicalNode  { return nil }

// PhysAlterEnum represents ALTER TYPE ... ADD VALUE.
type PhysAlterEnum struct {
	Name   string
	NewVal string
}

func (n *PhysAlterEnum) String() string            { return fmt.Sprintf("AlterEnum %s", n.Name) }
func (n *PhysAlterEnum) Cost() PlanCost            { return PlanCost{} }
func (n *PhysAlterEnum) Children() []PhysicalNode  { return nil }

// PhysCreateSchema represents CREATE SCHEMA.
type PhysCreateSchema struct {
	Name        string
	IfNotExists bool
	AuthRole    string
}

func (n *PhysCreateSchema) String() string            { return fmt.Sprintf("CreateSchema %s", n.Name) }
func (n *PhysCreateSchema) Cost() PlanCost            { return PlanCost{} }
func (n *PhysCreateSchema) Children() []PhysicalNode  { return nil }

// PhysDropSchema represents DROP SCHEMA.
type PhysDropSchema struct {
	Name      string
	MissingOk bool
	Cascade   bool
}

func (n *PhysDropSchema) String() string            { return fmt.Sprintf("DropSchema %s", n.Name) }
func (n *PhysDropSchema) Cost() PlanCost            { return PlanCost{} }
func (n *PhysDropSchema) Children() []PhysicalNode  { return nil }

// PhysResult produces a single row by evaluating expressions (SELECT without FROM).
type PhysResult struct {
	Exprs []Expr
	Names []string
}

func (n *PhysResult) String() string            { return "Result" }
func (n *PhysResult) Cost() PlanCost            { return PlanCost{Startup: 0.01, Total: 0.01, Rows: 1} }
func (n *PhysResult) Children() []PhysicalNode  { return nil }
