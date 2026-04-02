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
	OpEq       OpKind = iota // =
	OpNeq                    // <>
	OpLt                     // <
	OpLte                    // <=
	OpGt                     // >
	OpGte                    // >=
	OpAnd                    // AND
	OpOr                     // OR
	OpAdd                    // +
	OpSub                    // -
	OpMul                    // *
	OpDiv                    // /
	OpMod                    // %
	OpLike                   // LIKE
	OpILike                  // ILIKE
	OpNotLike                // NOT LIKE
	OpNotILike               // NOT ILIKE
	OpConcat                 // ||
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
	case OpLike:
		return "LIKE"
	case OpILike:
		return "ILIKE"
	case OpNotLike:
		return "NOT LIKE"
	case OpNotILike:
		return "NOT ILIKE"
	case OpConcat:
		return "||"
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
	if e.Op >= OpLike && e.Op <= OpNotILike {
		return e.evalLike(row)
	}
	if e.Op == OpConcat {
		return e.evalConcat(row)
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

func (e *ExprBinOp) evalLike(row *Row) (tuple.Datum, error) {
	lv, err := e.Left.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	rv, err := e.Right.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	if lv.Type == tuple.TypeNull || rv.Type == tuple.TypeNull {
		return tuple.DNull(), nil
	}
	s := datumToString(lv)
	pattern := datumToString(rv)
	icase := e.Op == OpILike || e.Op == OpNotILike
	matched := matchLikePattern(s, pattern, icase)
	if e.Op == OpNotLike || e.Op == OpNotILike {
		matched = !matched
	}
	return tuple.DBool(matched), nil
}

func (e *ExprBinOp) evalConcat(row *Row) (tuple.Datum, error) {
	lv, err := e.Left.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	rv, err := e.Right.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	// PG: NULL || 'x' = NULL
	if lv.Type == tuple.TypeNull || rv.Type == tuple.TypeNull {
		return tuple.DNull(), nil
	}
	return tuple.DText(datumToString(lv) + datumToString(rv)), nil
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

// ExprCase represents a CASE expression at the execution level.
type ExprCase struct {
	Arg      Expr // optional: simple CASE comparison value
	Whens    []ExprCaseWhen
	ElseExpr Expr // optional ELSE clause
}

type ExprCaseWhen struct {
	Cond   Expr
	Result Expr
}

func (e *ExprCase) String() string {
	var sb strings.Builder
	sb.WriteString("CASE")
	if e.Arg != nil {
		sb.WriteString(" ")
		sb.WriteString(e.Arg.String())
	}
	for _, w := range e.Whens {
		sb.WriteString(" WHEN ")
		sb.WriteString(w.Cond.String())
		sb.WriteString(" THEN ")
		sb.WriteString(w.Result.String())
	}
	if e.ElseExpr != nil {
		sb.WriteString(" ELSE ")
		sb.WriteString(e.ElseExpr.String())
	}
	sb.WriteString(" END")
	return sb.String()
}

func (e *ExprCase) Eval(row *Row) (tuple.Datum, error) {
	if e.Arg != nil {
		argVal, err := e.Arg.Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		for _, w := range e.Whens {
			whenVal, err := w.Cond.Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			if CompareDatums(argVal, whenVal) == 0 {
				return w.Result.Eval(row)
			}
		}
	} else {
		for _, w := range e.Whens {
			condVal, err := w.Cond.Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			if datumToBool(condVal) {
				return w.Result.Eval(row)
			}
		}
	}
	if e.ElseExpr != nil {
		return e.ElseExpr.Eval(row)
	}
	return tuple.DNull(), nil
}

// ExprBoolTest represents IS TRUE / IS FALSE / IS UNKNOWN at the execution level.
type ExprBoolTest struct {
	Arg  Expr
	Test BoolTestKind
}

func (e *ExprBoolTest) String() string {
	suffix := ""
	switch e.Test {
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
	return e.Arg.String() + suffix
}

func (e *ExprBoolTest) Eval(row *Row) (tuple.Datum, error) {
	val, err := e.Arg.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	isNull := val.Type == tuple.TypeNull
	isTrue := !isNull && datumToBool(val)
	switch e.Test {
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

// --- Helpers ---

// matchLikePattern implements SQL LIKE pattern matching.
// '%' matches any sequence of characters, '_' matches any single character.
func matchLikePattern(s, pattern string, icase bool) bool {
	if icase {
		s = strings.ToLower(s)
		pattern = strings.ToLower(pattern)
	}
	sr := []rune(s)
	pr := []rune(pattern)
	return likeMatch(sr, 0, pr, 0)
}

func likeMatch(s []rune, si int, p []rune, pi int) bool {
	for pi < len(p) {
		switch p[pi] {
		case '%':
			// Skip consecutive %
			for pi < len(p) && p[pi] == '%' {
				pi++
			}
			if pi == len(p) {
				return true // trailing % matches everything
			}
			for i := si; i <= len(s); i++ {
				if likeMatch(s, i, p, pi) {
					return true
				}
			}
			return false
		case '_':
			if si >= len(s) {
				return false
			}
			si++
			pi++
		case '\\':
			// Escape: next char is literal
			pi++
			if pi >= len(p) {
				return false
			}
			if si >= len(s) || s[si] != p[pi] {
				return false
			}
			si++
			pi++
		default:
			if si >= len(s) || s[si] != p[pi] {
				return false
			}
			si++
			pi++
		}
	}
	return si == len(s)
}

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
