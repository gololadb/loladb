package planner

import (
	"fmt"

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

	// Assignments holds SET col = expr for UPDATE.
	Assignments []*UpdateAssignment

	// Utility holds DDL/utility statement info when CommandType == CmdUtility.
	Utility *UtilityStmt
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

// UtilityStmt carries information for DDL / utility commands that
// bypass the planner (CREATE TABLE, CREATE INDEX, etc.).
type UtilityStmt struct {
	Type UtilityType

	// Fields used by various utility types.
	TableName   string
	Columns     []ColDef
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

	// CREATE TRIGGER fields
	TrigName       string
	TrigTable      string
	TrigFuncName   string
	TrigTiming     int
	TrigEvents     int
	TrigForEach    string // "ROW" or "STATEMENT"
	TrigReplace    bool
}

type UtilityType int

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

func (s *StarExpr) String() string                        { return "*" }
func (s *StarExpr) Eval(row *Row) (tuple.Datum, error)    { return tuple.DNull(), nil }
func (s *StarExpr) ResultType() tuple.DatumType            { return tuple.TypeNull }
