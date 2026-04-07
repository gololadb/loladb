package querytree

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/gololadb/loladb/pkg/tuple"
)

// CmdType identifies the type of command a Query represents,
// mirroring PostgreSQL's CmdType enum.
type CmdType int

const (
	CmdSelect CmdType = iota
	CmdInsert
	CmdUpdate
	CmdDelete
	CmdUtility
)

func (c CmdType) String() string {
	switch c {
	case CmdSelect:
		return "SELECT"
	case CmdInsert:
		return "INSERT"
	case CmdUpdate:
		return "UPDATE"
	case CmdDelete:
		return "DELETE"
	case CmdUtility:
		return "UTILITY"
	default:
		return "?"
	}
}

// Query is the output of semantic analysis (the analyzer). It is an
// enriched representation of a SQL statement where all names have been
// resolved against the catalog, all expressions carry type information,
// and table references are collected into a range table.
//
// This mirrors PostgreSQL's Query struct (src/include/nodes/parsenodes.h).
// The planner converts a Query into a plan tree; the analyzer converts
// a raw parse tree into a Query.
type Query struct {
	// CommandType identifies SELECT / INSERT / UPDATE / DELETE / UTILITY.
	CommandType CmdType

	// RangeTable is the list of relations referenced by the query.
	// Other fields reference entries by 1-based index (RangeTableRef).
	RangeTable []*RangeTblEntry

	// JoinTree represents the FROM and WHERE clauses. For simple
	// queries it is a flat list of RangeTableRef nodes; for explicit
	// JOINs it is a tree of JoinNode.
	JoinTree *FromExpr

	// TargetList is the SELECT output list, or the column assignments
	// for INSERT/UPDATE.
	TargetList []*TargetEntry

	// Qualification is the WHERE clause (already resolved).
	// Stored separately from JoinTree for clarity; the planner may
	// push it down into the join tree.
	Qual AnalyzedExpr

	// SortClause describes ORDER BY.
	SortClause []*SortClause

	// LimitCount and LimitOffset are the LIMIT/OFFSET expressions.
	LimitCount  AnalyzedExpr
	LimitOffset AnalyzedExpr

	// ResultRelation is the 1-based range table index of the target
	// table for INSERT/UPDATE/DELETE. Zero for SELECT.
	ResultRelation int

	// Values holds the rows for INSERT ... VALUES.
	Values [][]AnalyzedExpr

	// InsertColumns is the explicit column list for INSERT (nil = all).
	InsertColumns []string

	// GroupClause holds GROUP BY expressions.
	GroupClause []AnalyzedExpr

	// GroupingSets holds the expanded grouping sets (each is a list of indices
	// into GroupClause). Non-nil only for GROUPING SETS / CUBE / ROLLUP.
	GroupingSets [][]int

	// HasAggs is true when the query contains aggregate functions.
	HasAggs bool

	// HavingQual is the HAVING clause expression (may contain AggRefs).
	HavingQual AnalyzedExpr

	// AggRefs collects all AggRef nodes found in the target list,
	// in order. The planner uses this to build the aggregate node.
	AggRefs []*AggRef

	// Distinct is true when SELECT DISTINCT is used.
	Distinct bool

	// SetOp describes a UNION/INTERSECT/EXCEPT operation.
	SetOp    SetOpKind
	SetAll   bool
	SetLeft  *Query
	SetRight *Query

	// SelectSource holds the analyzed SELECT query for INSERT ... SELECT.
	SelectSource *Query

	// Assignments holds SET col = expr for UPDATE.
	Assignments []*UpdateAssignment

	// ReturningList holds the RETURNING clause expressions and names.
	ReturningList []*TargetEntry

	// Utility holds DDL/utility statement info when CommandType == CmdUtility.
	Utility *UtilityStmt

	// CTEs holds analyzed Common Table Expressions from WITH clauses.
	CTEs []*CTEDef

	// WindowFuncs collects all window function references in the query.
	WindowFuncs []*WindowFuncRef

	// OnConflict holds the analyzed ON CONFLICT clause for INSERT.
	OnConflict *OnConflictClause

	// IsValues is true for bare VALUES (...), (...) queries.
	IsValues bool
}

// CTEDef holds a single analyzed CTE definition.
type CTEDef struct {
	Name      string
	Query     *Query
	Columns   []RTEColumn // resolved output columns
	Recursive bool
}

// RangeTblEntry represents one entry in the query's range table,
// mirroring PostgreSQL's RangeTblEntry. Each entry describes a
// relation (table) that participates in the query.
type RangeTblEntry struct {
	// RTIndex is the 1-based position in Query.RangeTable.
	RTIndex int

	// RelOID is the catalog OID of the relation.
	RelOID int32

	// RelName is the physical table name.
	RelName string

	// Alias is the query-level alias (from AS clause), or RelName if none.
	Alias string

	// Columns describes every column of the relation, resolved from
	// the catalog at analysis time.
	Columns []RTEColumn

	// HeadPage is the first heap page (from catalog), used by the planner.
	HeadPage int32

	// Subquery holds the analyzed query for CTE / subquery range table entries.
	// When non-nil, this RTE represents a subquery scan rather than a heap scan.
	Subquery *Query

	// IsRecursive is true for recursive CTE entries (WITH RECURSIVE).
	IsRecursive bool

	// Lateral is true for LATERAL subqueries that can reference columns
	// from preceding FROM items.
	Lateral bool

	// TABLESAMPLE fields.
	SampleMethod  string // "bernoulli", "system", or ""
	SamplePercent string // percentage expression as text (e.g. "10")
}

// RTEColumn is a single column within a RangeTblEntry, carrying the
// catalog-resolved name and type.
type RTEColumn struct {
	Name   string
	Type   tuple.DatumType
	ColNum int32 // 1-based column number in the physical relation
}

// RangeTblRef is a leaf in the join tree that references a range table
// entry by its 1-based index.
type RangeTblRef struct {
	RTIndex int
}

// FromExpr represents the FROM clause: a list of table references
// (which may include JoinNode trees) plus an optional top-level
// qualification (WHERE). Mirrors PostgreSQL's FromExpr.
type FromExpr struct {
	FromList []JoinTreeNode // RangeTblRef or JoinNode
	Quals    AnalyzedExpr   // top-level WHERE, may be nil
}

// JoinTreeNode is implemented by nodes that can appear in the join tree.
type JoinTreeNode interface {
	joinTreeNode()
}

func (*RangeTblRef) joinTreeNode() {}
func (*JoinNode) joinTreeNode()    {}

// JoinNode represents an explicit JOIN in the FROM clause.
type JoinNode struct {
	JoinType  JoinType
	Left      JoinTreeNode
	Right     JoinTreeNode
	Quals     AnalyzedExpr // ON clause (resolved)
	LeftRTI   int          // range table index of left side (for simple refs)
	RightRTI  int          // range table index of right side
}

// TargetEntry is one item in the query's target list. For SELECT it
// is an output column; for INSERT/UPDATE it is a value to store.
// Mirrors PostgreSQL's TargetEntry.
type TargetEntry struct {
	// Expr is the resolved expression.
	Expr AnalyzedExpr

	// Name is the output column name (from AS or inferred).
	Name string

	// ResNo is the 1-based result column number.
	ResNo int
}

// SortClause describes one ORDER BY item.
type SortClause struct {
	Expr AnalyzedExpr
	Desc bool
}

// UpdateAssignment represents SET column = expr in an UPDATE.
type UpdateAssignment struct {
	ColName string
	ColNum  int32 // 1-based column number in the target relation
	ColType tuple.DatumType
	Expr    AnalyzedExpr
}

// OnConflictAction mirrors parser.OnConflictAction.
type OnConflictAction int

const (
	OnConflictNone    OnConflictAction = iota
	OnConflictNothing                  // DO NOTHING
	OnConflictUpdate                   // DO UPDATE SET ...
)

// OnConflictClause is the analyzed form of ON CONFLICT.
type OnConflictClause struct {
	Action         OnConflictAction
	ConflictCols   []string            // column names from the conflict target
	Assignments    []*UpdateAssignment // SET assignments for DO UPDATE
	WhereClause    AnalyzedExpr        // optional WHERE on the DO UPDATE
}

// UtilityStmt carries information for DDL / utility commands that
// bypass the planner (CREATE TABLE, CREATE INDEX, etc.).
type UtilityStmt struct {
	Type UtilityType

	// Fields used by various utility types.
	TableName   string
	TableSchema string // schema for CREATE TABLE / CREATE VIEW (empty = current)
	IsTemp      bool   // CREATE TEMPORARY TABLE
	Columns     []ColDef
	ForeignKeys []ForeignKeyDef // FK constraints from CREATE TABLE
	IndexName   string
	IndexTable  string
	IndexColumn string
	IndexMethod string // btree, hash, gin, gist, spgist, brin
	SeqName     string
	ViewName    string
	ViewDef     string
	ViewColumns []ColDef
	AlterCmds   []string
	Message     string

	// RLS policy fields
	PolicyName       string
	PolicyTable      string
	PolicyCmd        string // ALL, SELECT, INSERT, UPDATE, DELETE
	PolicyPermissive bool
	PolicyRoles      []string
	PolicyUsing      string
	PolicyCheck      string

	// Role management fields
	RoleName       string
	RoleOptions    map[string]interface{} // CREATE ROLE options
	RoleStmtType   string                // "ROLE", "USER", "GROUP"
	DropRoles      []string
	DropMissingOk  bool
	DropCascade    bool

	// GRANT/REVOKE role membership
	GrantedRoles   []string
	Grantees       []string
	AdminOption    bool

	// GRANT/REVOKE object privileges
	Privileges     []string
	PrivCols       [][]string // per-privilege column lists (nil = table-wide)
	TargetType     string     // "TABLE", etc.
	Objects        []string   // object names
	GrantOption    bool

	// CREATE FUNCTION fields
	FuncName       string
	FuncLanguage   string
	FuncBody       string
	FuncReturnType string
	FuncParamNames []string
	FuncParamTypes []string
	FuncReplace    bool

	// ALTER FUNCTION fields
	FuncNewName  string // RENAME TO
	FuncNewOwner string // OWNER TO

	// CREATE DOMAIN / CREATE TYPE AS ENUM fields
	DomainName     string
	DomainBaseType string // SQL type name for the base type
	DomainNotNull  bool
	DomainCheck    string // CHECK expression (raw SQL)
	EnumName       string
	EnumVals       []string

	// DROP TYPE / ALTER TYPE fields
	DropTypeName  string
	AlterEnumName string
	AlterEnumVal  string // new value to add

	// CREATE TRIGGER fields
	TrigName       string
	TrigTable      string
	TrigFuncName   string
	TrigTiming     int
	TrigEvents     int
	TrigForEach    string // "ROW" or "STATEMENT"
	TrigReplace    bool
	TrigArgs       []string // trigger function arguments

	// Schema fields
	SchemaName      string
	SchemaIfNotExists bool
	SchemaAuthRole  string

	// ALTER TABLE ADD/DROP COLUMN fields
	AlterColName    string // column name for ADD/DROP COLUMN
	AlterColDef     *ColDef // column definition for ADD COLUMN
	AlterIfNotExists bool  // IF NOT EXISTS for ADD COLUMN
	AlterIfExists   bool   // IF EXISTS for DROP COLUMN

	// Partition fields
	PartitionStrategy string   // "range", "list", or "hash"
	PartitionKeyCols  []string // partition key column names

	// Inheritance
	InheritParents []string // parent table names for INHERITS

	// ALTER TABLE ... OWNER TO
	NewOwner string
}

// PartitionBound describes the bounds for ATTACH PARTITION.
type PartitionBound struct {
	BoundType  string   // "range", "list", or "default"
	ListValues []string
	RangeFrom  []string
	RangeTo    []string
}

type UtilityType int

// SetOpKind identifies the set operation type.
type SetOpKind int

const (
	SetOpNone      SetOpKind = iota
	SetOpUnion
	SetOpIntersect
	SetOpExcept
)

const (
	UtilCreateTable UtilityType = iota
	UtilCreateIndex
	UtilCreateSequence
	UtilCreateView
	UtilAlterTable
	UtilCreatePolicy
	UtilEnableRLS
	UtilDisableRLS
	UtilCreateRole
	UtilAlterRole
	UtilDropRole
	UtilGrantRole
	UtilRevokeRole
	UtilGrantPrivilege
	UtilRevokePrivilege
	UtilCreateFunction
	UtilCreateTrigger
	UtilDropFunction
	UtilDropTrigger
	UtilAlterFunction
	UtilCreateDomain
	UtilCreateEnum
	UtilDropType
	UtilAlterEnum
	UtilCreateSchema
	UtilTruncate
	UtilDropTable
	UtilDropIndex
	UtilDropView
	UtilDropSchema
	UtilAddColumn
	UtilDropColumn
	UtilNoOp
)

// AnalyzedExpr is an expression where all column references have been
// resolved to (RTIndex, ColNum) pairs and all nodes carry type info.
// It extends the planner's Expr interface with type information.
type AnalyzedExpr interface {
	Expr // embeds the existing Expr interface (String, Eval)
	// ResultType returns the resolved datum type of this expression.
	ResultType() tuple.DatumType
}

// --- Concrete analyzed expression types ---

// ColumnVar references a column via its range table entry and column
// number, mirroring PostgreSQL's Var node. This replaces the
// name-based ExprColumn during analysis.
type ColumnVar struct {
	RTIndex  int    // 1-based range table index
	ColNum   int32  // 1-based column number within the RTE
	ColName  string // resolved column name (for display)
	Table    string // alias or table name (for display)
	VarType  tuple.DatumType
	AttIndex int // 0-based index into the flattened output columns
}

// Const is a typed literal value, mirroring PostgreSQL's Const node.
type Const struct {
	Value    tuple.Datum
	ConstType tuple.DatumType
}

// OpExpr is a resolved operator expression (comparison or logical),
// mirroring PostgreSQL's OpExpr.
type OpExpr struct {
	Op         OpKind
	Left       AnalyzedExpr
	Right      AnalyzedExpr
	ResultTyp  tuple.DatumType
}

// BoolExprNode represents AND/OR/NOT with resolved operands,
// mirroring PostgreSQL's BoolExpr.
type BoolExprNode struct {
	Op    BoolOp
	Args  []AnalyzedExpr
	// Always returns TypeBool.
}

type BoolOp int

const (
	BoolAnd BoolOp = iota
	BoolOr
	BoolNot
)

// NullTestExpr represents IS [NOT] NULL with a resolved operand.
type NullTestExpr struct {
	Arg    AnalyzedExpr
	IsNot  bool // true for IS NOT NULL
}

// AggRef represents a reference to an aggregate function in the target
// list. During execution, the aggregate executor replaces these with
// the accumulated result for each group.
type AggRef struct {
	AggFunc         string         // e.g. "count", "sum", "avg", "min", "max"
	Args            []AnalyzedExpr // arguments (empty for count(*))
	Star            bool           // true for count(*)
	Distinct        bool           // true for count(DISTINCT ...)
	AggIndex        int            // index into the aggregate list (set by planner)
	ReturnTyp       tuple.DatumType
	WithinGroupExpr AnalyzedExpr   // ORDER BY expr for ordered-set aggregates (percentile_cont, etc.)
}

// WindowFuncRef represents a window function call in the target list.
// It is the analyzed form; the planner converts it to an executable node.
type WindowFuncRef struct {
	FuncName  string         // e.g. "row_number", "rank", "sum"
	Args      []AnalyzedExpr // function arguments (empty for row_number, rank, etc.)
	Star      bool           // true for count(*) OVER (...)
	Distinct  bool           // true for count(DISTINCT ...) OVER (...)
	WinDef    *AnalyzedWindowDef
	ReturnTyp tuple.DatumType
	WinIndex  int // index into the query's window function list (set by planner)
}

func (w *WindowFuncRef) String() string {
	return w.FuncName + "() OVER (...)"
}

func (w *WindowFuncRef) Eval(row *Row) (tuple.Datum, error) {
	// Window functions are not evaluated row-by-row; the executor
	// pre-computes them and injects the values. This uses the WinIndex
	// to look up the pre-computed value from the row.
	if w.WinIndex >= 0 && w.WinIndex < len(row.Columns) {
		return row.Columns[w.WinIndex], nil
	}
	return tuple.DNull(), fmt.Errorf("window function %s: result not available (index %d)", w.FuncName, w.WinIndex)
}

func (w *WindowFuncRef) ResultType() tuple.DatumType { return w.ReturnTyp }

// AnalyzedWindowDef holds the analyzed OVER clause for a window function.
type AnalyzedWindowDef struct {
	PartitionBy []AnalyzedExpr // PARTITION BY expressions
	OrderBy     []*SortClause
	FrameMode   WindowFrameMode
	FrameStart  WindowFrameBound
	FrameEnd    WindowFrameBound
}

// WindowFrameMode identifies the frame type.
type WindowFrameMode int

const (
	FrameModeRange WindowFrameMode = iota
	FrameModeRows
	FrameModeGroups
)

// WindowFrameBound identifies a frame boundary.
type WindowFrameBound struct {
	Type   WindowBoundType
	Offset AnalyzedExpr // for PRECEDING/FOLLOWING with offset
}

// WindowBoundType identifies the kind of frame boundary.
type WindowBoundType int

const (
	BoundUnboundedPreceding WindowBoundType = iota
	BoundCurrentRow
	BoundUnboundedFollowing
	BoundOffsetPreceding
	BoundOffsetFollowing
)

// FuncCallExpr represents a resolved function call.
type FuncCallExpr struct {
	FuncName   string         // unqualified function name (lowercase)
	Args       []AnalyzedExpr
	ReturnType tuple.DatumType
}

// TypeCastExpr represents a type cast (e.g., expr::integer).
type TypeCastExpr struct {
	Arg        AnalyzedExpr
	TargetType string          // SQL type name (lowercase)
	CastType   tuple.DatumType // resolved target datum type
}

// StarExpr represents SELECT * (expanded during analysis into
// individual TargetEntry items, but kept for compatibility).
type StarExpr struct{}

// --- ColumnVar implements AnalyzedExpr ---

func (v *ColumnVar) String() string {
	if v.Table != "" {
		return v.Table + "." + v.ColName
	}
	return v.ColName
}

func (v *ColumnVar) Eval(row *Row) (tuple.Datum, error) {
	if v.AttIndex >= 0 && v.AttIndex < len(row.Columns) {
		return row.Columns[v.AttIndex], nil
	}
	return tuple.DNull(), fmt.Errorf("column %s out of range", v)
}

func (v *ColumnVar) ResultType() tuple.DatumType { return v.VarType }

// --- Const implements AnalyzedExpr ---

func (c *Const) String() string {
	switch c.Value.Type {
	case tuple.TypeNull:
		return "NULL"
	case tuple.TypeInt32:
		return fmt.Sprintf("%d", c.Value.I32)
	case tuple.TypeInt64:
		return fmt.Sprintf("%d", c.Value.I64)
	case tuple.TypeText:
		return fmt.Sprintf("'%s'", c.Value.Text)
	case tuple.TypeBool:
		if c.Value.Bool {
			return "true"
		}
		return "false"
	case tuple.TypeFloat64:
		return fmt.Sprintf("%g", c.Value.F64)
	default:
		return "?"
	}
}

func (c *Const) Eval(row *Row) (tuple.Datum, error) { return c.Value, nil }
func (c *Const) ResultType() tuple.DatumType        { return c.ConstType }

// --- OpExpr implements AnalyzedExpr ---

func (o *OpExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", o.Left, o.Op, o.Right)
}

func (o *OpExpr) Eval(row *Row) (tuple.Datum, error) {
	lv, err := o.Left.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	rv, err := o.Right.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	if o.Op >= OpAdd && o.Op <= OpMod {
		return evalArithmeticDatums(o.Op, lv, rv)
	}
	if o.Op >= OpLike && o.Op <= OpNotILike {
		if lv.Type == tuple.TypeNull || rv.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		s := datumToString(lv)
		pattern := datumToString(rv)
		icase := o.Op == OpILike || o.Op == OpNotILike
		matched := matchLikePattern(s, pattern, icase)
		if o.Op == OpNotLike || o.Op == OpNotILike {
			matched = !matched
		}
		return tuple.DBool(matched), nil
	}
	if o.Op == OpStartsWith {
		if lv.Type == tuple.TypeNull || rv.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		return tuple.DBool(strings.HasPrefix(datumToString(lv), datumToString(rv))), nil
	}
	if o.Op >= OpSimilarTo && o.Op <= OpRegexNotIMatch {
		if lv.Type == tuple.TypeNull || rv.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		s := datumToString(lv)
		pattern := datumToString(rv)
		matched, err := evalRegexOp(o.Op, s, pattern)
		if err != nil {
			return tuple.DNull(), err
		}
		return tuple.DBool(matched), nil
	}
	if o.Op == OpConcat {
		if lv.Type == tuple.TypeNull || rv.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		return tuple.DText(datumToString(lv) + datumToString(rv)), nil
	}
	if o.Op >= OpJSONArrow && o.Op <= OpJSONExistsAll {
		return evalJSONOp(o.Op, lv, rv)
	}
	if o.Op >= OpArrayContains && o.Op <= OpArrayOverlap {
		return evalArrayOp(o.Op, lv, rv)
	}
	cmp := CompareDatums(lv, rv)
	switch o.Op {
	case OpEq:
		return tuple.DBool(cmp == 0), nil
	case OpNeq:
		return tuple.DBool(cmp != 0), nil
	case OpLt:
		return tuple.DBool(cmp < 0), nil
	case OpLte:
		return tuple.DBool(cmp <= 0), nil
	case OpGt:
		return tuple.DBool(cmp > 0), nil
	case OpGte:
		return tuple.DBool(cmp >= 0), nil
	}
	return tuple.DNull(), nil
}

func evalArithmeticDatums(op OpKind, lv, rv tuple.Datum) (tuple.Datum, error) {
	// Date/timestamp ± interval arithmetic.
	if d, ok := evalDateTimeInterval(op, lv, rv); ok {
		return d, nil
	}
	lint, lisInt := toInt64(lv)
	rint, risInt := toInt64(rv)
	if lisInt && risInt {
		switch op {
		case OpAdd:
			return tuple.DInt64(lint + rint), nil
		case OpSub:
			return tuple.DInt64(lint - rint), nil
		case OpMul:
			return tuple.DInt64(lint * rint), nil
		case OpDiv:
			if rint == 0 {
				return tuple.DNull(), fmt.Errorf("division by zero")
			}
			return tuple.DInt64(lint / rint), nil
		case OpMod:
			if rint == 0 {
				return tuple.DNull(), fmt.Errorf("division by zero")
			}
			return tuple.DInt64(lint % rint), nil
		}
	}
	lf, lok := datumToFloat(lv)
	rf, rok := datumToFloat(rv)
	if !lok || !rok {
		return tuple.DNull(), fmt.Errorf("arithmetic on non-numeric types")
	}
	switch op {
	case OpAdd:
		return tuple.DFloat64(lf + rf), nil
	case OpSub:
		return tuple.DFloat64(lf - rf), nil
	case OpMul:
		return tuple.DFloat64(lf * rf), nil
	case OpDiv:
		if rf == 0 {
			return tuple.DNull(), fmt.Errorf("division by zero")
		}
		return tuple.DFloat64(lf / rf), nil
	case OpMod:
		if rf == 0 {
			return tuple.DNull(), fmt.Errorf("division by zero")
		}
		return tuple.DInt64(int64(lf) % int64(rf)), nil
	}
	return tuple.DNull(), nil
}

func (o *OpExpr) ResultType() tuple.DatumType { return o.ResultTyp }

// --- BoolExprNode implements AnalyzedExpr ---

func (b *BoolExprNode) String() string {
	switch b.Op {
	case BoolNot:
		return fmt.Sprintf("NOT %s", b.Args[0])
	case BoolAnd:
		return fmt.Sprintf("(%s AND %s)", b.Args[0], b.Args[1])
	case BoolOr:
		return fmt.Sprintf("(%s OR %s)", b.Args[0], b.Args[1])
	}
	return "?"
}

func (b *BoolExprNode) Eval(row *Row) (tuple.Datum, error) {
	switch b.Op {
	case BoolAnd:
		for _, arg := range b.Args {
			v, err := arg.Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			if !datumToBool(v) {
				return tuple.DBool(false), nil
			}
		}
		return tuple.DBool(true), nil
	case BoolOr:
		for _, arg := range b.Args {
			v, err := arg.Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			if datumToBool(v) {
				return tuple.DBool(true), nil
			}
		}
		return tuple.DBool(false), nil
	case BoolNot:
		v, err := b.Args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		return tuple.DBool(!datumToBool(v)), nil
	}
	return tuple.DNull(), nil
}

func (b *BoolExprNode) ResultType() tuple.DatumType { return tuple.TypeBool }

// --- NullTestExpr implements AnalyzedExpr ---

func (n *NullTestExpr) String() string {
	if n.IsNot {
		return fmt.Sprintf("%s IS NOT NULL", n.Arg)
	}
	return fmt.Sprintf("%s IS NULL", n.Arg)
}

func (n *NullTestExpr) Eval(row *Row) (tuple.Datum, error) {
	v, err := n.Arg.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	isNull := v.Type == tuple.TypeNull
	if n.IsNot {
		return tuple.DBool(!isNull), nil
	}
	return tuple.DBool(isNull), nil
}

func (n *NullTestExpr) ResultType() tuple.DatumType { return tuple.TypeBool }

// --- StarExpr implements AnalyzedExpr ---

func (a *AggRef) String() string {
	if a.Star {
		return a.AggFunc + "(*)"
	}
	args := make([]string, len(a.Args))
	for i, arg := range a.Args {
		args[i] = arg.String()
	}
	return a.AggFunc + "(" + strings.Join(args, ", ") + ")"
}
func (a *AggRef) Eval(row *Row) (tuple.Datum, error) {
	// AggRef is replaced by the aggregate executor; if we reach here,
	// the value is stored in the row at the aggregate's output index.
	if a.AggIndex >= 0 && a.AggIndex < len(row.Columns) {
		return row.Columns[a.AggIndex], nil
	}
	return tuple.DNull(), nil
}
func (a *AggRef) ResultType() tuple.DatumType { return a.ReturnTyp }

func (tc *TypeCastExpr) String() string {
	return tc.Arg.String() + "::" + tc.TargetType
}
func (tc *TypeCastExpr) Eval(row *Row) (tuple.Datum, error) {
	val, err := tc.Arg.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	return castDatum(val, tc.CastType, tc.TargetType)
}
func (tc *TypeCastExpr) ResultType() tuple.DatumType { return tc.CastType }

func (f *FuncCallExpr) String() string {
	args := make([]string, len(f.Args))
	for i, a := range f.Args {
		args[i] = a.String()
	}
	return f.FuncName + "(" + strings.Join(args, ", ") + ")"
}
func (f *FuncCallExpr) Eval(row *Row) (tuple.Datum, error) {
	// Delegate to ExprFunc at execution time; this path is used when
	// the analyzed expression is used directly (e.g., in DEFAULT eval).
	return evalBuiltinFunc(f.FuncName, f.Args, row)
}
func (f *FuncCallExpr) ResultType() tuple.DatumType { return f.ReturnType }

func (s *StarExpr) String() string                        { return "*" }
func (s *StarExpr) Eval(row *Row) (tuple.Datum, error)    { return tuple.DNull(), nil }
func (s *StarExpr) ResultType() tuple.DatumType            { return tuple.TypeNull }

// --- CaseExpr implements AnalyzedExpr ---

// CaseWhenClause is a single WHEN ... THEN ... pair.
type CaseWhenClause struct {
	Cond   AnalyzedExpr
	Result AnalyzedExpr
}

// CaseExprNode represents a CASE expression.
type CaseExprNode struct {
	Arg       AnalyzedExpr     // optional: simple CASE comparison value
	Whens     []CaseWhenClause // WHEN clauses
	ElseExpr  AnalyzedExpr     // optional ELSE clause (nil → NULL)
	ReturnTyp tuple.DatumType
}

func (c *CaseExprNode) String() string {
	var sb strings.Builder
	sb.WriteString("CASE")
	if c.Arg != nil {
		sb.WriteString(" ")
		sb.WriteString(c.Arg.String())
	}
	for _, w := range c.Whens {
		sb.WriteString(" WHEN ")
		sb.WriteString(w.Cond.String())
		sb.WriteString(" THEN ")
		sb.WriteString(w.Result.String())
	}
	if c.ElseExpr != nil {
		sb.WriteString(" ELSE ")
		sb.WriteString(c.ElseExpr.String())
	}
	sb.WriteString(" END")
	return sb.String()
}

func (c *CaseExprNode) Eval(row *Row) (tuple.Datum, error) {
	if c.Arg != nil {
		// Simple CASE: compare Arg against each WHEN value.
		argVal, err := c.Arg.Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		for _, w := range c.Whens {
			whenVal, err := w.Cond.Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			if CompareDatums(argVal, whenVal) == 0 {
				return w.Result.Eval(row)
			}
		}
	} else {
		// Searched CASE: each WHEN is a boolean expression.
		for _, w := range c.Whens {
			condVal, err := w.Cond.Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			if datumToBool(condVal) {
				return w.Result.Eval(row)
			}
		}
	}
	if c.ElseExpr != nil {
		return c.ElseExpr.Eval(row)
	}
	return tuple.DNull(), nil
}

func (c *CaseExprNode) ResultType() tuple.DatumType { return c.ReturnTyp }

// --- BooleanTestExpr implements AnalyzedExpr ---

// BoolTestKind mirrors parser.BoolTestType.
type BoolTestKind int

const (
	BoolTestIsTrue BoolTestKind = iota
	BoolTestIsNotTrue
	BoolTestIsFalse
	BoolTestIsNotFalse
	BoolTestIsUnknown
	BoolTestIsNotUnknown
)

// BooleanTestExpr represents IS TRUE / IS FALSE / IS UNKNOWN tests.
type BooleanTestExpr struct {
	Arg  AnalyzedExpr
	Test BoolTestKind
}

func (b *BooleanTestExpr) String() string {
	suffix := ""
	switch b.Test {
	case BoolTestIsTrue:
		suffix = " IS TRUE"
	case BoolTestIsNotTrue:
		suffix = " IS NOT TRUE"
	case BoolTestIsFalse:
		suffix = " IS FALSE"
	case BoolTestIsNotFalse:
		suffix = " IS NOT FALSE"
	case BoolTestIsUnknown:
		suffix = " IS UNKNOWN"
	case BoolTestIsNotUnknown:
		suffix = " IS NOT UNKNOWN"
	}
	return b.Arg.String() + suffix
}

func (b *BooleanTestExpr) Eval(row *Row) (tuple.Datum, error) {
	val, err := b.Arg.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	isNull := val.Type == tuple.TypeNull
	isTrue := !isNull && datumToBool(val)
	switch b.Test {
	case BoolTestIsTrue:
		return tuple.DBool(isTrue), nil
	case BoolTestIsNotTrue:
		return tuple.DBool(!isTrue), nil
	case BoolTestIsFalse:
		return tuple.DBool(!isNull && !isTrue), nil
	case BoolTestIsNotFalse:
		return tuple.DBool(isNull || isTrue), nil
	case BoolTestIsUnknown:
		return tuple.DBool(isNull), nil
	case BoolTestIsNotUnknown:
		return tuple.DBool(!isNull), nil
	}
	return tuple.DNull(), nil
}

func (b *BooleanTestExpr) ResultType() tuple.DatumType { return tuple.TypeBool }

// SubLinkType identifies the kind of subquery expression.
type SubLinkType int

const (
	SubLinkExists SubLinkType = iota // EXISTS (SELECT ...)
	SubLinkAny                       // expr IN (SELECT ...) / expr = ANY (SELECT ...)
	SubLinkAll                       // expr NOT IN (SELECT ...) / expr <> ALL (SELECT ...)
	SubLinkExprSubquery              // (SELECT ...) as a scalar value
)

// SubLinkExpr represents a subquery expression (EXISTS, IN, ANY, ALL, scalar).
// The subquery is stored as an analyzed Query and re-executed for each
// evaluation (necessary for correlated subqueries).
type SubLinkExpr struct {
	LinkType SubLinkType
	TestExpr AnalyzedExpr // outer expression for comparison (nil for EXISTS/scalar)
	OpName   string       // comparison operator ("=" for IN, "<>" for NOT IN, etc.)
	Subquery *Query       // the analyzed sub-SELECT
	// SubReturnType is the result type of the subquery's first column
	// (used for scalar subqueries).
	SubReturnType tuple.DatumType
}

func (s *SubLinkExpr) String() string {
	switch s.LinkType {
	case SubLinkExists:
		return "EXISTS(...)"
	case SubLinkAny:
		if s.TestExpr != nil {
			return s.TestExpr.String() + " IN (...)"
		}
		return "ANY(...)"
	case SubLinkAll:
		if s.TestExpr != nil {
			return s.TestExpr.String() + " NOT IN (...)"
		}
		return "ALL(...)"
	case SubLinkExprSubquery:
		return "(SELECT ...)"
	}
	return "SUBLINK"
}

func (s *SubLinkExpr) Eval(row *Row) (tuple.Datum, error) {
	// SubLinkExpr is not directly evaluated — it is converted to an
	// ExprSubLink by analyzedToExpr, which holds the execution machinery.
	return tuple.DNull(), fmt.Errorf("SubLinkExpr.Eval: should not be called directly")
}

func (s *SubLinkExpr) ResultType() tuple.DatumType {
	switch s.LinkType {
	case SubLinkExists, SubLinkAny, SubLinkAll:
		return tuple.TypeBool
	case SubLinkExprSubquery:
		return s.SubReturnType
	}
	return tuple.TypeNull
}

// ---------------------------------------------------------------------------
// JSON operator evaluation
// ---------------------------------------------------------------------------

// evalJSONOp evaluates JSON operators: ->, ->>, #>, #>>, @>, <@, ?.
func evalJSONOp(op OpKind, lv, rv tuple.Datum) (tuple.Datum, error) {
	if lv.Type == tuple.TypeNull || rv.Type == tuple.TypeNull {
		return tuple.DNull(), nil
	}

	switch op {
	case OpJSONArrow:
		return jsonArrow(lv.Text, rv, false)
	case OpJSONArrowText:
		return jsonArrow(lv.Text, rv, true)
	case OpJSONHashArrow:
		return jsonHashArrow(lv.Text, rv.Text, false)
	case OpJSONHashArrowText:
		return jsonHashArrow(lv.Text, rv.Text, true)
	case OpJSONContains:
		return jsonContains(lv.Text, rv.Text)
	case OpJSONContainedBy:
		return jsonContains(rv.Text, lv.Text)
	case OpJSONExists:
		return jsonKeyExists(lv.Text, rv.Text)
	case OpJSONExistsAny:
		return jsonKeyExistsAny(lv.Text, rv.Text)
	case OpJSONExistsAll:
		return jsonKeyExistsAll(lv.Text, rv.Text)
	case OpJSONDelete:
		return jsonDelete(lv.Text, rv)
	case OpJSONDeletePath:
		return jsonDeletePath(lv.Text, rv.Text)
	}
	return tuple.DNull(), nil
}

// jsonDelete implements jsonb - text (delete key) and jsonb - integer (delete array element).
func jsonDelete(jsonStr string, key tuple.Datum) (tuple.Datum, error) {
	// Try as object key deletion.
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(jsonStr), &obj); err == nil {
		keyStr := ""
		switch key.Type {
		case tuple.TypeText:
			keyStr = key.Text
		case tuple.TypeInt32:
			keyStr = strconv.Itoa(int(key.I32))
		case tuple.TypeInt64:
			keyStr = strconv.FormatInt(key.I64, 10)
		}
		delete(obj, keyStr)
		out, err := json.Marshal(obj)
		if err != nil {
			return tuple.DNull(), nil
		}
		return tuple.DText(string(out)), nil
	}

	// Try as array element deletion by index.
	var arr []json.RawMessage
	if err := json.Unmarshal([]byte(jsonStr), &arr); err == nil {
		idx := -1
		switch key.Type {
		case tuple.TypeInt32:
			idx = int(key.I32)
		case tuple.TypeInt64:
			idx = int(key.I64)
		}
		if idx >= 0 && idx < len(arr) {
			arr = append(arr[:idx], arr[idx+1:]...)
			out, err := json.Marshal(arr)
			if err != nil {
				return tuple.DNull(), nil
			}
			return tuple.DText(string(out)), nil
		}
	}

	return tuple.DText(jsonStr), nil
}

// parsePGArrayLiteral parses a PostgreSQL array literal like {a,b,c} into a slice of strings.
func parsePGArrayLiteral(s string) []string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '{' && s[len(s)-1] == '}' {
		s = s[1 : len(s)-1]
	}
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

// evalArrayOp evaluates array containment and overlap operators.
func evalArrayOp(op OpKind, lv, rv tuple.Datum) (tuple.Datum, error) {
	lArr := parsePGArrayLiteral(datumToString(lv))
	rArr := parsePGArrayLiteral(datumToString(rv))

	switch op {
	case OpArrayContains:
		// lArr @> rArr: every element of rArr is in lArr
		lSet := map[string]bool{}
		for _, v := range lArr {
			lSet[strings.TrimSpace(v)] = true
		}
		for _, v := range rArr {
			if !lSet[strings.TrimSpace(v)] {
				return tuple.DBool(false), nil
			}
		}
		return tuple.DBool(true), nil
	case OpArrayContainedBy:
		// lArr <@ rArr: every element of lArr is in rArr
		rSet := map[string]bool{}
		for _, v := range rArr {
			rSet[strings.TrimSpace(v)] = true
		}
		for _, v := range lArr {
			if !rSet[strings.TrimSpace(v)] {
				return tuple.DBool(false), nil
			}
		}
		return tuple.DBool(true), nil
	case OpArrayOverlap:
		// lArr && rArr: any common element
		lSet := map[string]bool{}
		for _, v := range lArr {
			lSet[strings.TrimSpace(v)] = true
		}
		for _, v := range rArr {
			if lSet[strings.TrimSpace(v)] {
				return tuple.DBool(true), nil
			}
		}
		return tuple.DBool(false), nil
	}
	return tuple.DNull(), nil
}

// jsonDeletePath implements jsonb #- text[] (delete by path).
func jsonDeletePath(jsonStr, pathStr string) (tuple.Datum, error) {
	keys := parsePGArrayLiteral(pathStr)
	if len(keys) == 0 {
		return tuple.DText(jsonStr), nil
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return tuple.DNull(), nil
	}

	data = deleteAtPath(data, keys)
	out, err := json.Marshal(data)
	if err != nil {
		return tuple.DNull(), nil
	}
	return tuple.DText(string(out)), nil
}

// deleteAtPath recursively deletes a key at the given path.
func deleteAtPath(data interface{}, keys []string) interface{} {
	if len(keys) == 0 {
		return data
	}
	key := keys[0]
	if len(keys) == 1 {
		// Last key — delete it.
		if obj, ok := data.(map[string]interface{}); ok {
			delete(obj, key)
			return obj
		}
		if arr, ok := data.([]interface{}); ok {
			idx, err := strconv.Atoi(key)
			if err == nil && idx >= 0 && idx < len(arr) {
				return append(arr[:idx], arr[idx+1:]...)
			}
		}
		return data
	}
	// Recurse into nested structure.
	if obj, ok := data.(map[string]interface{}); ok {
		if child, exists := obj[key]; exists {
			obj[key] = deleteAtPath(child, keys[1:])
		}
		return obj
	}
	if arr, ok := data.([]interface{}); ok {
		idx, err := strconv.Atoi(key)
		if err == nil && idx >= 0 && idx < len(arr) {
			arr[idx] = deleteAtPath(arr[idx], keys[1:])
		}
		return arr
	}
	return data
}

// jsonArrow implements -> and ->>. The key can be a string (object lookup)
// or an integer (array index). If asText is true, the result is returned as
// TypeText (->>) instead of TypeJSON (->).
func jsonArrow(jsonStr string, key tuple.Datum, asText bool) (tuple.Datum, error) {
	var raw json.RawMessage
	if err := json.Unmarshal([]byte(jsonStr), &raw); err != nil {
		return tuple.DNull(), nil
	}

	// Try integer index for array access.
	idx := -1
	switch key.Type {
	case tuple.TypeInt32:
		idx = int(key.I32)
	case tuple.TypeInt64:
		idx = int(key.I64)
	case tuple.TypeText:
		if n, err := strconv.Atoi(key.Text); err == nil {
			// Only use as array index if the JSON is actually an array.
			var arr []json.RawMessage
			if json.Unmarshal(raw, &arr) == nil {
				idx = n
			}
		}
	}

	if idx >= 0 {
		var arr []json.RawMessage
		if err := json.Unmarshal(raw, &arr); err == nil {
			if idx < len(arr) {
				return jsonRawToResult(arr[idx], asText)
			}
			return tuple.DNull(), nil
		}
	}

	// Object key lookup.
	keyStr := datumToString(key)
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return tuple.DNull(), nil
	}
	val, ok := obj[keyStr]
	if !ok {
		return tuple.DNull(), nil
	}
	return jsonRawToResult(val, asText)
}

// jsonHashArrow implements #> and #>>. The path is a PostgreSQL text array
// literal like '{a,b,c}'.
func jsonHashArrow(jsonStr, pathStr string, asText bool) (tuple.Datum, error) {
	path := parseTextArray(pathStr)
	if len(path) == 0 {
		return tuple.DNull(), nil
	}

	current := []byte(jsonStr)
	for _, key := range path {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(current, &obj); err == nil {
			val, ok := obj[key]
			if !ok {
				return tuple.DNull(), nil
			}
			current = val
			continue
		}
		// Try array index.
		var arr []json.RawMessage
		if err := json.Unmarshal(current, &arr); err == nil {
			idx, err := strconv.Atoi(key)
			if err != nil || idx < 0 || idx >= len(arr) {
				return tuple.DNull(), nil
			}
			current = arr[idx]
			continue
		}
		return tuple.DNull(), nil
	}
	return jsonRawToResult(json.RawMessage(current), asText)
}

// jsonContains implements @>: does the left JSON contain the right JSON?
func jsonContains(left, right string) (tuple.Datum, error) {
	var lv, rv interface{}
	if err := json.Unmarshal([]byte(left), &lv); err != nil {
		return tuple.DNull(), nil
	}
	if err := json.Unmarshal([]byte(right), &rv); err != nil {
		return tuple.DNull(), nil
	}
	return tuple.DBool(jsonValueContains(lv, rv)), nil
}

// jsonKeyExists implements ?: does the JSON object have the given top-level key?
func jsonKeyExists(jsonStr, key string) (tuple.Datum, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(jsonStr), &obj); err != nil {
		// For arrays, check if the key matches any element string value.
		var arr []interface{}
		if err2 := json.Unmarshal([]byte(jsonStr), &arr); err2 == nil {
			for _, elem := range arr {
				if s, ok := elem.(string); ok && s == key {
					return tuple.DBool(true), nil
				}
			}
		}
		return tuple.DBool(false), nil
	}
	_, ok := obj[key]
	return tuple.DBool(ok), nil
}

// jsonKeyExistsAny implements ?|: does the JSON object contain any of the given keys?
// Right operand is a text array literal like {a,b,c}.
func jsonKeyExistsAny(jsonStr, arrStr string) (tuple.Datum, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(jsonStr), &obj); err != nil {
		return tuple.DBool(false), nil
	}
	keys := parsePGArrayLiteral(arrStr)
	for _, k := range keys {
		if _, ok := obj[k]; ok {
			return tuple.DBool(true), nil
		}
	}
	return tuple.DBool(false), nil
}

// jsonKeyExistsAll implements ?&: does the JSON object contain all of the given keys?
func jsonKeyExistsAll(jsonStr, arrStr string) (tuple.Datum, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(jsonStr), &obj); err != nil {
		return tuple.DBool(false), nil
	}
	keys := parsePGArrayLiteral(arrStr)
	if len(keys) == 0 {
		return tuple.DBool(true), nil
	}
	for _, k := range keys {
		if _, ok := obj[k]; !ok {
			return tuple.DBool(false), nil
		}
	}
	return tuple.DBool(true), nil
}

// jsonRawToResult converts a json.RawMessage to a Datum. If asText is true,
// strings are unquoted and other values are returned as their JSON text
// representation (matching ->> / #>> behavior).
func jsonRawToResult(raw json.RawMessage, asText bool) (tuple.Datum, error) {
	if asText {
		// Unquote strings, return others as-is.
		s := strings.TrimSpace(string(raw))
		if len(s) > 0 && s[0] == '"' {
			var unquoted string
			if err := json.Unmarshal(raw, &unquoted); err == nil {
				return tuple.DText(unquoted), nil
			}
		}
		if s == "null" {
			return tuple.DNull(), nil
		}
		return tuple.DText(s), nil
	}
	return tuple.DJSON(string(raw)), nil
}

// jsonValueContains checks if a contains b using PostgreSQL @> semantics:
// - Objects: every key in b must exist in a with a matching value (recursive).
// - Arrays: every element in b must be contained in some element of a.
// - Scalars: must be equal.
func jsonValueContains(a, b interface{}) bool {
	switch bv := b.(type) {
	case map[string]interface{}:
		av, ok := a.(map[string]interface{})
		if !ok {
			return false
		}
		for k, bval := range bv {
			aval, exists := av[k]
			if !exists || !jsonValueContains(aval, bval) {
				return false
			}
		}
		return true
	case []interface{}:
		av, ok := a.([]interface{})
		if !ok {
			return false
		}
		for _, belem := range bv {
			found := false
			for _, aelem := range av {
				if jsonValueContains(aelem, belem) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	default:
		return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
	}
}

// parseTextArray parses a PostgreSQL text array literal like '{a,b,c}'
// into a slice of strings.
func parseTextArray(s string) []string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '{' && s[len(s)-1] == '}' {
		s = s[1 : len(s)-1]
	}
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}
