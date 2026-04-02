package planner

import (
	"fmt"
	"strings"

	"github.com/gololadb/loladb/pkg/tuple"
)

// Row represents a single row during query execution. It holds
// columns from one or more tables, keyed by "table.column" or just
// "column" for unambiguous references.
type Row struct {
	Columns []tuple.Datum
	Names   []string // "table.column" qualified names
}

// Expr is a node in an expression tree.
type Expr interface {
	fmt.Stringer
	// Eval evaluates the expression against a row.
	Eval(row *Row) (tuple.Datum, error)
}

// --- Concrete expression types ---

// ExprColumn references a column by table alias and column name.
type ExprColumn struct {
	Table  string // may be empty for unqualified
	Column string
	// Resolved index into Row.Columns (set by analyzer).
	Index int
}

func (e *ExprColumn) String() string {
	if e.Table != "" {
		return e.Table + "." + e.Column
	}
	return e.Column
}

func (e *ExprColumn) Eval(row *Row) (tuple.Datum, error) {
	if e.Index >= 0 && e.Index < len(row.Columns) {
		return row.Columns[e.Index], nil
	}
	// Fallback: search by name
	target := e.Column
	if e.Table != "" {
		target = e.Table + "." + e.Column
	}
	for i, name := range row.Names {
		if strings.EqualFold(name, target) || strings.HasSuffix(strings.ToLower(name), "."+strings.ToLower(e.Column)) {
			return row.Columns[i], nil
		}
	}
	return tuple.DNull(), fmt.Errorf("column %s not found in row", e)
}

// ExprLiteral is a constant value.
type ExprLiteral struct {
	Value tuple.Datum
}

func (e *ExprLiteral) String() string {
	switch e.Value.Type {
	case tuple.TypeNull:
		return "NULL"
	case tuple.TypeInt32:
		return fmt.Sprintf("%d", e.Value.I32)
	case tuple.TypeInt64:
		return fmt.Sprintf("%d", e.Value.I64)
	case tuple.TypeText:
		return fmt.Sprintf("'%s'", e.Value.Text)
	case tuple.TypeBool:
		if e.Value.Bool {
			return "true"
		}
		return "false"
	case tuple.TypeFloat64:
		return fmt.Sprintf("%g", e.Value.F64)
	default:
		return "?"
	}
}

func (e *ExprLiteral) Eval(row *Row) (tuple.Datum, error) {
	return e.Value, nil
}

// OpKind represents comparison and logical operators.
type OpKind int

const (
	OpEq   OpKind = iota // =
	OpNeq                // <>
	OpLt                 // <
	OpLte                // <=
	OpGt                 // >
	OpGte                // >=
	OpAnd                // AND
	OpOr                 // OR
	OpAdd                // +
	OpSub                // -
	OpMul                // *
	OpDiv                // /
	OpMod                // %
)

func (op OpKind) String() string {
	switch op {
	case OpEq:
		return "="
	case OpNeq:
		return "<>"
	case OpLt:
		return "<"
	case OpLte:
		return "<="
	case OpGt:
		return ">"
	case OpGte:
		return ">="
	case OpAnd:
		return "AND"
	case OpOr:
		return "OR"
	case OpAdd:
		return "+"
	case OpSub:
		return "-"
	case OpMul:
		return "*"
	case OpDiv:
		return "/"
	case OpMod:
		return "%"
	default:
		return "?"
	}
}

// ExprBinOp is a binary operation (comparison or logical).
type ExprBinOp struct {
	Op    OpKind
	Left  Expr
	Right Expr
}

func (e *ExprBinOp) String() string {
	return fmt.Sprintf("(%s %s %s)", e.Left, e.Op, e.Right)
}

func (e *ExprBinOp) Eval(row *Row) (tuple.Datum, error) {
	if e.Op == OpAnd || e.Op == OpOr {
		return e.evalLogical(row)
	}
	if e.Op >= OpAdd && e.Op <= OpMod {
		return e.evalArithmetic(row)
	}
	return e.evalComparison(row)
}

func (e *ExprBinOp) evalLogical(row *Row) (tuple.Datum, error) {
	lv, err := e.Left.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	rv, err := e.Right.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	lb := datumToBool(lv)
	rb := datumToBool(rv)
	switch e.Op {
	case OpAnd:
		return tuple.DBool(lb && rb), nil
	case OpOr:
		return tuple.DBool(lb || rb), nil
	}
	return tuple.DNull(), nil
}

func (e *ExprBinOp) evalComparison(row *Row) (tuple.Datum, error) {
	lv, err := e.Left.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	rv, err := e.Right.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	cmp := CompareDatums(lv, rv)
	switch e.Op {
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

func (e *ExprBinOp) evalArithmetic(row *Row) (tuple.Datum, error) {
	lv, err := e.Left.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	rv, err := e.Right.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	lf, lok := datumToFloat(lv)
	rf, rok := datumToFloat(rv)
	if !lok || !rok {
		return tuple.DNull(), fmt.Errorf("arithmetic on non-numeric types")
	}
	// If both operands are integers, stay in integer domain.
	lint, lisInt := datumToInt(lv)
	rint, risInt := datumToInt(rv)
	if lisInt && risInt {
		switch e.Op {
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
	switch e.Op {
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
		li := int64(lf)
		ri := int64(rf)
		return tuple.DInt64(li % ri), nil
	}
	return tuple.DNull(), nil
}

// ExprNot negates a boolean expression.
type ExprNot struct {
	Child Expr
}

func (e *ExprNot) String() string { return fmt.Sprintf("NOT %s", e.Child) }
func (e *ExprNot) Eval(row *Row) (tuple.Datum, error) {
	v, err := e.Child.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	return tuple.DBool(!datumToBool(v)), nil
}

// ExprIsNull tests for NULL.
type ExprIsNull struct {
	Child Expr
	Not   bool // true for IS NOT NULL
}

func (e *ExprIsNull) String() string {
	if e.Not {
		return fmt.Sprintf("%s IS NOT NULL", e.Child)
	}
	return fmt.Sprintf("%s IS NULL", e.Child)
}

func (e *ExprIsNull) Eval(row *Row) (tuple.Datum, error) {
	v, err := e.Child.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	isNull := v.Type == tuple.TypeNull
	if e.Not {
		return tuple.DBool(!isNull), nil
	}
	return tuple.DBool(isNull), nil
}

// ExprAggRef references an aggregate result by index. During execution,
// the aggregate node outputs rows where group-by columns come first,
// followed by aggregate results. ExprAggRef reads the aggregate value
// at offset NumGroupExprs + AggIndex.
type ExprAggRef struct {
	AggIndex      int // index into the aggregate descriptor list
	NumGroupExprs int // number of group-by columns (set by optimizer)
}

func (e *ExprAggRef) String() string { return fmt.Sprintf("agg[%d]", e.AggIndex) }
func (e *ExprAggRef) Eval(row *Row) (tuple.Datum, error) {
	idx := e.NumGroupExprs + e.AggIndex
	if idx >= 0 && idx < len(row.Columns) {
		return row.Columns[idx], nil
	}
	return tuple.DNull(), nil
}

// ExprCast represents a type cast expression (e.g., expr::integer).
type ExprCast struct {
	Inner      Expr
	TargetType tuple.DatumType
	TypeName   string
}

func (e *ExprCast) String() string { return e.Inner.String() + "::" + e.TypeName }
func (e *ExprCast) Eval(row *Row) (tuple.Datum, error) {
	val, err := e.Inner.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	return castDatum(val, e.TargetType, e.TypeName)
}

// ExprFunc represents a function call expression.
type ExprFunc struct {
	Name string
	Args []Expr
}

func (e *ExprFunc) String() string {
	args := make([]string, len(e.Args))
	for i, a := range e.Args {
		args[i] = a.String()
	}
	return e.Name + "(" + strings.Join(args, ", ") + ")"
}

func (e *ExprFunc) Eval(row *Row) (tuple.Datum, error) {
	// Wrap Expr args as AnalyzedExpr for evalBuiltinFunc.
	analyzed := make([]AnalyzedExpr, len(e.Args))
	for i, a := range e.Args {
		analyzed[i] = &exprWrapper{inner: a}
	}
	return evalBuiltinFunc(e.Name, analyzed, row)
}

// exprWrapper adapts an Expr to the AnalyzedExpr interface.
type exprWrapper struct {
	inner Expr
}

func (w *exprWrapper) String() string                     { return w.inner.String() }
func (w *exprWrapper) Eval(row *Row) (tuple.Datum, error) { return w.inner.Eval(row) }
func (w *exprWrapper) ResultType() tuple.DatumType        { return tuple.TypeText }

// ExprStar represents SELECT * (expanded during analysis).
type ExprStar struct{}

func (e *ExprStar) String() string                        { return "*" }
func (e *ExprStar) Eval(row *Row) (tuple.Datum, error)    { return tuple.DNull(), nil }

// --- Helpers ---

func datumToBool(d tuple.Datum) bool {
	switch d.Type {
	case tuple.TypeBool:
		return d.Bool
	case tuple.TypeInt32:
		return d.I32 != 0
	case tuple.TypeInt64:
		return d.I64 != 0
	case tuple.TypeNull:
		return false
	default:
		return true
	}
}

// EnumOrdinalFunc resolves an enum value to its ordinal position.
// Returns 0 if the value is not an enum member. Set by the executor
// to enable enum-aware comparisons.
var EnumOrdinalFunc func(val string) int

// CompareDatums returns -1, 0, or 1 comparing two datums.
func CompareDatums(a, b tuple.Datum) int {
	// Coerce int32/int64
	ai, aok := toInt64(a)
	bi, bok := toInt64(b)
	if aok && bok {
		if ai < bi {
			return -1
		}
		if ai > bi {
			return 1
		}
		return 0
	}

	// Coerce int/float cross-type comparisons.
	af, afok := toFloat64(a)
	bf, bfok := toFloat64(b)
	if afok && bfok {
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}

	if a.Type != b.Type {
		return 0 // incomparable
	}

	switch a.Type {
	case tuple.TypeText:
		// Try enum-aware comparison: if both values have ordinals,
		// compare by ordinal position instead of lexicographically.
		if EnumOrdinalFunc != nil {
			ao := EnumOrdinalFunc(a.Text)
			bo := EnumOrdinalFunc(b.Text)
			if ao > 0 && bo > 0 {
				if ao < bo {
					return -1
				}
				if ao > bo {
					return 1
				}
				return 0
			}
		}
		if a.Text < b.Text {
			return -1
		}
		if a.Text > b.Text {
			return 1
		}
		return 0
	case tuple.TypeFloat64:
		if a.F64 < b.F64 {
			return -1
		}
		if a.F64 > b.F64 {
			return 1
		}
		return 0
	case tuple.TypeBool:
		if a.Bool == b.Bool {
			return 0
		}
		if !a.Bool {
			return -1
		}
		return 1
	default:
		return 0
	}
}

func toFloat64(d tuple.Datum) (float64, bool) {
	switch d.Type {
	case tuple.TypeFloat64:
		return d.F64, true
	case tuple.TypeInt32:
		return float64(d.I32), true
	case tuple.TypeInt64:
		return float64(d.I64), true
	default:
		return 0, false
	}
}

func toInt64(d tuple.Datum) (int64, bool) {
	switch d.Type {
	case tuple.TypeInt32:
		return int64(d.I32), true
	case tuple.TypeInt64:
		return d.I64, true
	default:
		return 0, false
	}
}

func datumToInt(d tuple.Datum) (int64, bool) {
	return toInt64(d)
}

func datumToFloat(d tuple.Datum) (float64, bool) {
	switch d.Type {
	case tuple.TypeInt32:
		return float64(d.I32), true
	case tuple.TypeInt64:
		return float64(d.I64), true
	case tuple.TypeFloat64:
		return d.F64, true
	default:
		return 0, false
	}
}

// EvalBool evaluates an expression and converts the result to a boolean.
func EvalBool(expr Expr, row *Row) bool {
	v, err := expr.Eval(row)
	if err != nil {
		return false
	}
	return datumToBool(v)
}

// ReferencedTables returns the set of table names referenced by column
// expressions in the tree.
func ReferencedTables(expr Expr) map[string]bool {
	tables := make(map[string]bool)
	collectTables(expr, tables)
	return tables
}

func collectTables(expr Expr, tables map[string]bool) {
	switch e := expr.(type) {
	case *ExprColumn:
		if e.Table != "" {
			tables[e.Table] = true
		}
	case *ExprBinOp:
		collectTables(e.Left, tables)
		collectTables(e.Right, tables)
	case *ExprNot:
		collectTables(e.Child, tables)
	case *ExprIsNull:
		collectTables(e.Child, tables)
	}
}
