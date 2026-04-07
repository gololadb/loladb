package querytree

import (
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"time"

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
	// Fallback: search by name.
	if d, ok := findColumnInRow(e.Table, e.Column, row); ok {
		return d, nil
	}
	// Check the outer row context for correlated subquery references.
	if OuterRowContext != nil {
		if d, ok := findColumnInRow(e.Table, e.Column, OuterRowContext); ok {
			return d, nil
		}
	}
	return tuple.DNull(), fmt.Errorf("column %s not found in row", e)
}

// findColumnInRow searches for a column by name in a row.
// When table is non-empty, requires an exact qualified match (table.column).
// When table is empty, matches either unqualified or any table's column.
func findColumnInRow(table, column string, row *Row) (tuple.Datum, bool) {
	if table != "" {
		target := strings.ToLower(table + "." + column)
		for i, name := range row.Names {
			if strings.EqualFold(name, target) {
				return row.Columns[i], true
			}
		}
		return tuple.DNull(), false
	}
	// Unqualified: match exact or suffix.
	colLower := strings.ToLower(column)
	for i, name := range row.Names {
		nameLower := strings.ToLower(name)
		if nameLower == colLower || strings.HasSuffix(nameLower, "."+colLower) {
			return row.Columns[i], true
		}
	}
	return tuple.DNull(), false
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

// ArrayConstructExpr represents ARRAY[e1, e2, ...].
type ArrayConstructExpr struct {
	Elements []Expr
}

func (e *ArrayConstructExpr) String() string            { return "ARRAY[...]" }
func (e *ArrayConstructExpr) ResultType() tuple.DatumType { return tuple.TypeText }
func (e *ArrayConstructExpr) Eval(row *Row) (tuple.Datum, error) {
	var sb strings.Builder
	sb.WriteByte('{')
	for i, el := range e.Elements {
		if i > 0 {
			sb.WriteByte(',')
		}
		v, err := el.Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		switch v.Type {
		case tuple.TypeText:
			sb.WriteString(v.Text)
		case tuple.TypeInt32:
			sb.WriteString(fmt.Sprintf("%d", v.I32))
		case tuple.TypeInt64:
			sb.WriteString(fmt.Sprintf("%d", v.I64))
		case tuple.TypeFloat64:
			sb.WriteString(fmt.Sprintf("%g", v.F64))
		case tuple.TypeBool:
			if v.Bool {
				sb.WriteString("t")
			} else {
				sb.WriteString("f")
			}
		case tuple.TypeNull:
			sb.WriteString("NULL")
		default:
			sb.WriteString(fmt.Sprintf("%v", v))
		}
	}
	sb.WriteByte('}')
	return tuple.DText(sb.String()), nil
}

// ArraySubscriptExpr represents arr[idx].
type ArraySubscriptExpr struct {
	Array Expr
	Index Expr
}

func (e *ArraySubscriptExpr) String() string            { return "arr[idx]" }
func (e *ArraySubscriptExpr) ResultType() tuple.DatumType { return tuple.TypeText }
func (e *ArraySubscriptExpr) Eval(row *Row) (tuple.Datum, error) {
	arrVal, err := e.Array.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	idxVal, err := e.Index.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	if arrVal.Type != tuple.TypeText {
		return tuple.DNull(), nil
	}

	// Parse 1-based index.
	idx := int(idxVal.I64)
	if idxVal.Type == tuple.TypeInt32 {
		idx = int(idxVal.I32)
	}

	// Parse PostgreSQL array literal: {val1,val2,...}
	s := arrVal.Text
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return tuple.DNull(), nil
	}
	inner := s[1 : len(s)-1]
	if inner == "" {
		return tuple.DNull(), nil
	}
	elements := strings.Split(inner, ",")
	if idx < 1 || idx > len(elements) {
		return tuple.DNull(), nil
	}
	return tuple.DText(elements[idx-1]), nil
}

// ArraySliceExpr represents arr[lo:hi] (1-based, inclusive).
type ArraySliceExpr struct {
	Array Expr
	Lower Expr // nil means from start (1)
	Upper Expr // nil means to end
}

func (e *ArraySliceExpr) String() string              { return "arr[lo:hi]" }
func (e *ArraySliceExpr) ResultType() tuple.DatumType  { return tuple.TypeText }
func (e *ArraySliceExpr) Eval(row *Row) (tuple.Datum, error) {
	arrVal, err := e.Array.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	if arrVal.Type != tuple.TypeText {
		return tuple.DNull(), nil
	}
	s := arrVal.Text
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return tuple.DNull(), nil
	}
	inner := s[1 : len(s)-1]
	if inner == "" {
		return tuple.DText("{}"), nil
	}
	elements := strings.Split(inner, ",")

	lo := 1
	hi := len(elements)
	if e.Lower != nil {
		lv, err := e.Lower.Eval(row)
		if err == nil {
			if lv.Type == tuple.TypeInt32 {
				lo = int(lv.I32)
			} else if lv.Type == tuple.TypeInt64 {
				lo = int(lv.I64)
			}
		}
	}
	if e.Upper != nil {
		uv, err := e.Upper.Eval(row)
		if err == nil {
			if uv.Type == tuple.TypeInt32 {
				hi = int(uv.I32)
			} else if uv.Type == tuple.TypeInt64 {
				hi = int(uv.I64)
			}
		}
	}

	// Convert to 0-based and clamp.
	lo-- // 1-based to 0-based
	if lo < 0 {
		lo = 0
	}
	if hi > len(elements) {
		hi = len(elements)
	}
	if lo >= hi {
		return tuple.DText("{}"), nil
	}

	var sb strings.Builder
	sb.WriteByte('{')
	for i := lo; i < hi; i++ {
		if i > lo {
			sb.WriteByte(',')
		}
		sb.WriteString(elements[i])
	}
	sb.WriteByte('}')
	return tuple.DText(sb.String()), nil
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
	OpSimilarTo              // SIMILAR TO
	OpNotSimilarTo           // NOT SIMILAR TO
	OpRegexMatch             // ~
	OpRegexIMatch            // ~*
	OpRegexNotMatch          // !~
	OpRegexNotIMatch         // !~*
	OpStartsWith             // ^@
	OpConcat                 // ||
	OpJSONArrow              // ->
	OpJSONArrowText          // ->>
	OpJSONHashArrow          // #>
	OpJSONHashArrowText      // #>>
	OpJSONContains           // @>
	OpJSONContainedBy        // <@
	OpJSONExists             // ?
	OpJSONExistsAny          // ?|
	OpJSONExistsAll          // ?&
	OpJSONDelete             // - (delete key from JSON object/array)
	OpJSONDeletePath         // #- (delete by path)
	OpArrayContains          // @> (array contains)
	OpArrayContainedBy       // <@ (array contained by)
	OpArrayOverlap           // && (array overlap)
	OpTSMatch                // @@ (full-text search match)
	OpGeomDistance            // <-> (geometric distance)
	OpGeomSame               // ~= (geometric same as)
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
	case OpJSONArrow:
		return "->"
	case OpJSONArrowText:
		return "->>"
	case OpJSONHashArrow:
		return "#>"
	case OpJSONHashArrowText:
		return "#>>"
	case OpJSONContains:
		return "@>"
	case OpJSONContainedBy:
		return "<@"
	case OpJSONExists:
		return "?"
	case OpJSONExistsAny:
		return "?|"
	case OpJSONExistsAll:
		return "?&"
	case OpJSONDelete:
		return "-"
	case OpJSONDeletePath:
		return "#-"
	case OpArrayContains:
		return "@>"
	case OpArrayContainedBy:
		return "<@"
	case OpArrayOverlap:
		return "&&"
	case OpSimilarTo:
		return "SIMILAR TO"
	case OpNotSimilarTo:
		return "NOT SIMILAR TO"
	case OpRegexMatch:
		return "~"
	case OpRegexIMatch:
		return "~*"
	case OpRegexNotMatch:
		return "!~"
	case OpRegexNotIMatch:
		return "!~*"
	case OpStartsWith:
		return "^@"
	case OpTSMatch:
		return "@@"
	case OpGeomDistance:
		return "<->"
	case OpGeomSame:
		return "~="
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
	if e.Op >= OpJSONArrow && e.Op <= OpJSONDeletePath {
		return e.evalJSON(row)
	}
	if e.Op >= OpArrayContains && e.Op <= OpArrayOverlap {
		return e.evalArrayOp(row)
	}
	if e.Op >= OpSimilarTo && e.Op <= OpRegexNotIMatch {
		return e.evalRegex(row)
	}
	if e.Op == OpStartsWith {
		return e.evalStartsWith(row)
	}
	if e.Op == OpTSMatch {
		return e.evalTSMatch(row)
	}
	if e.Op == OpGeomDistance || e.Op == OpGeomSame {
		return e.evalGeom(row)
	}
	return e.evalComparison(row)
}

func (e *ExprBinOp) evalJSON(row *Row) (tuple.Datum, error) {
	lv, err := e.Left.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	rv, err := e.Right.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	return evalJSONOp(e.Op, lv, rv)
}

func (e *ExprBinOp) evalArrayOp(row *Row) (tuple.Datum, error) {
	lv, err := e.Left.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	rv, err := e.Right.Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	return evalArrayOp(e.Op, lv, rv)
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
	// SQL three-valued logic: any comparison with NULL yields NULL.
	if lv.Type == tuple.TypeNull || rv.Type == tuple.TypeNull {
		return tuple.DNull(), nil
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
	// Array concatenation: array || array, array || element, element || array.
	if isArrayDatum(lv) || isArrayDatum(rv) {
		return evalArrayConcat(lv, rv), nil
	}
	return tuple.DText(datumToString(lv) + datumToString(rv)), nil
}

// isArrayDatum returns true if the datum is an array type or looks like an
// array literal ({...}).
func isArrayDatum(d tuple.Datum) bool {
	if d.Type == tuple.TypeArray {
		return true
	}
	if d.Type == tuple.TypeText {
		s := strings.TrimSpace(d.Text)
		return len(s) >= 2 && s[0] == '{' && s[len(s)-1] == '}'
	}
	return false
}

// evalArrayConcat concatenates arrays or appends/prepends elements.
func evalArrayConcat(lv, rv tuple.Datum) tuple.Datum {
	lArr := parseArrayLiteral(datumToString(lv))
	rArr := parseArrayLiteral(datumToString(rv))

	// If left is not an array, prepend as single element.
	if !isArrayDatum(lv) {
		rArr = append([]string{datumToString(lv)}, rArr...)
		return tuple.DArray("{" + strings.Join(rArr, ",") + "}")
	}
	// If right is not an array, append as single element.
	if !isArrayDatum(rv) {
		lArr = append(lArr, datumToString(rv))
		return tuple.DArray("{" + strings.Join(lArr, ",") + "}")
	}
	// Both arrays: concatenate.
	combined := append(lArr, rArr...)
	return tuple.DArray("{" + strings.Join(combined, ",") + "}")
}

// parseArrayLiteral splits a PG array literal {a,b,c} into string elements.
func parseArrayLiteral(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" || s == "{}" {
		return nil
	}
	if s[0] == '{' && s[len(s)-1] == '}' {
		s = s[1 : len(s)-1]
	}
	if s == "" {
		return nil
	}
	var elems []string
	var cur strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch == '"' && !inQuote:
			inQuote = true
			cur.WriteByte(ch)
		case ch == '"' && inQuote:
			inQuote = false
			cur.WriteByte(ch)
		case ch == ',' && !inQuote:
			elems = append(elems, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(ch)
		}
	}
	if cur.Len() > 0 {
		elems = append(elems, cur.String())
	}
	return elems
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
	// Date/timestamp ± interval arithmetic.
	if d, ok := evalDateTimeInterval(e.Op, lv, rv); ok {
		return d, nil
	}
	// If either operand is NUMERIC, use arbitrary-precision arithmetic.
	if lv.Type == tuple.TypeNumeric || rv.Type == tuple.TypeNumeric {
		return evalNumericArith(e.Op, lv, rv)
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

// evalNumericArith performs arbitrary-precision arithmetic on NUMERIC values.
func evalNumericArith(op OpKind, lv, rv tuple.Datum) (tuple.Datum, error) {
	ls := numericToString(lv)
	rs := numericToString(rv)
	lf, _, err := new(big.Float).SetPrec(128).Parse(ls, 10)
	if err != nil {
		return tuple.DNull(), fmt.Errorf("invalid numeric: %q", ls)
	}
	rf, _, err := new(big.Float).SetPrec(128).Parse(rs, 10)
	if err != nil {
		return tuple.DNull(), fmt.Errorf("invalid numeric: %q", rs)
	}
	var result *big.Float
	switch op {
	case OpAdd:
		result = new(big.Float).SetPrec(128).Add(lf, rf)
	case OpSub:
		result = new(big.Float).SetPrec(128).Sub(lf, rf)
	case OpMul:
		result = new(big.Float).SetPrec(128).Mul(lf, rf)
	case OpDiv:
		if rf.Sign() == 0 {
			return tuple.DNull(), fmt.Errorf("division by zero")
		}
		result = new(big.Float).SetPrec(128).Quo(lf, rf)
	case OpMod:
		if rf.Sign() == 0 {
			return tuple.DNull(), fmt.Errorf("division by zero")
		}
		// big.Float doesn't have Mod; use integer truncation.
		q := new(big.Float).SetPrec(128).Quo(lf, rf)
		qi, _ := q.Int(nil)
		qf := new(big.Float).SetPrec(128).SetInt(qi)
		result = new(big.Float).SetPrec(128).Sub(lf, new(big.Float).SetPrec(128).Mul(qf, rf))
	default:
		return tuple.DNull(), fmt.Errorf("unsupported numeric operation")
	}
	return tuple.DNumeric(result.Text('f', -1)), nil
}

func numericToString(d tuple.Datum) string {
	switch d.Type {
	case tuple.TypeNumeric:
		return d.Text
	case tuple.TypeInt32:
		return fmt.Sprintf("%d", d.I32)
	case tuple.TypeInt64:
		return fmt.Sprintf("%d", d.I64)
	case tuple.TypeFloat64:
		return fmt.Sprintf("%g", d.F64)
	case tuple.TypeText:
		return d.Text
	default:
		return "0"
	}
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

// evalRegexOp evaluates SIMILAR TO and regex match operators (~, ~*, !~, !~*).
func evalRegexOp(op OpKind, s, pattern string) (bool, error) {
	var re string
	var icase bool
	var negate bool

	switch op {
	case OpSimilarTo:
		re = similarToRegexp(pattern)
	case OpNotSimilarTo:
		re = similarToRegexp(pattern)
		negate = true
	case OpRegexMatch:
		re = pattern
	case OpRegexIMatch:
		re = pattern
		icase = true
	case OpRegexNotMatch:
		re = pattern
		negate = true
	case OpRegexNotIMatch:
		re = pattern
		icase = true
		negate = true
	}

	if icase {
		re = "(?i)" + re
	}

	matched, err := regexp.MatchString(re, s)
	if err != nil {
		return false, fmt.Errorf("invalid regular expression: %w", err)
	}
	if negate {
		return !matched, nil
	}
	return matched, nil
}

// similarToRegexp converts a SQL SIMILAR TO pattern to a Go regexp.
// SIMILAR TO uses: % = .*, _ = ., | for alternation, and supports
// character classes [...] and grouping (...).
func similarToRegexp(pattern string) string {
	var buf strings.Builder
	buf.WriteString("^")
	i := 0
	for i < len(pattern) {
		ch := pattern[i]
		switch ch {
		case '%':
			buf.WriteString(".*")
		case '_':
			buf.WriteByte('.')
		case '\\':
			// Escape: next char is literal.
			buf.WriteByte('\\')
			i++
			if i < len(pattern) {
				buf.WriteByte(pattern[i])
			}
		case '[', ']', '(', ')', '|':
			// Pass through — these are valid in SIMILAR TO patterns.
			buf.WriteByte(ch)
		case '.', '^', '$', '*', '+', '?', '{', '}':
			// Escape regexp metacharacters that aren't SIMILAR TO operators.
			buf.WriteByte('\\')
			buf.WriteByte(ch)
		default:
			buf.WriteByte(ch)
		}
		i++
	}
	buf.WriteString("$")
	return buf.String()
}

func (e *ExprBinOp) evalRegex(row *Row) (tuple.Datum, error) {
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
	matched, err := evalRegexOp(e.Op, datumToString(lv), datumToString(rv))
	if err != nil {
		return tuple.DNull(), err
	}
	return tuple.DBool(matched), nil
}

func (e *ExprBinOp) evalStartsWith(row *Row) (tuple.Datum, error) {
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
	return tuple.DBool(strings.HasPrefix(datumToString(lv), datumToString(rv))), nil
}

func (e *ExprBinOp) evalTSMatch(row *Row) (tuple.Datum, error) {
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
	return tuple.DBool(tsvectorMatchesTsquery(datumToString(lv), datumToString(rv))), nil
}

func (e *ExprBinOp) evalGeom(row *Row) (tuple.Datum, error) {
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
	ls := datumToString(lv)
	rs := datumToString(rv)
	switch e.Op {
	case OpGeomDistance:
		return tuple.DFloat64(geomDistance(ls, rs)), nil
	case OpGeomSame:
		return tuple.DBool(ls == rs), nil
	default:
		return tuple.DNull(), nil
	}
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
// evalDateTimeInterval handles date/timestamp ± interval arithmetic.
// Returns (result, true) if the operation was handled, (DNull, false) otherwise.
func evalDateTimeInterval(op OpKind, lv, rv tuple.Datum) (tuple.Datum, bool) {
	if op != OpAdd && op != OpSub {
		return tuple.DNull(), false
	}

	var ts tuple.Datum
	var iv tuple.Datum
	isDate := false
	sub := false

	switch {
	case (lv.Type == tuple.TypeTimestamp || lv.Type == tuple.TypeDate) && rv.Type == tuple.TypeInterval:
		ts, iv = lv, rv
		isDate = lv.Type == tuple.TypeDate
		sub = op == OpSub
	case lv.Type == tuple.TypeInterval && (rv.Type == tuple.TypeTimestamp || rv.Type == tuple.TypeDate) && op == OpAdd:
		ts, iv = rv, lv
		isDate = rv.Type == tuple.TypeDate
	default:
		return tuple.DNull(), false
	}

	// Convert date to timestamp (microseconds) for arithmetic.
	var t time.Time
	if isDate {
		t = time.Unix(ts.I64*86400, 0).UTC()
	} else {
		t = time.UnixMicro(ts.I64).UTC()
	}

	// Apply months.
	months := int(iv.I32)
	if sub {
		months = -months
	}
	if months != 0 {
		t = t.AddDate(0, months, 0)
	}

	// Apply microseconds.
	us := iv.I64
	if sub {
		us = -us
	}
	if us != 0 {
		t = t.Add(time.Duration(us) * time.Microsecond)
	}

	if isDate {
		days := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Unix() / 86400
		return tuple.DDate(days), true
	}
	return tuple.DTimestamp(t.UnixMicro()), true
}

// parseDateText parses a text string into a TypeDate datum.
func parseDateText(s string) (tuple.Datum, error) {
	t, err := parseTimestamp(s)
	if err != nil {
		return tuple.DNull(), err
	}
	days := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Unix() / 86400
	return tuple.DDate(days), nil
}

// parseTimestampText parses a text string into a TypeTimestamp datum.
func parseTimestampText(s string) (tuple.Datum, error) {
	t, err := parseTimestamp(s)
	if err != nil {
		return tuple.DNull(), err
	}
	return tuple.DTimestamp(t.UnixMicro()), nil
}

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

	// Coerce text to DATE/TIMESTAMP for cross-type comparisons.
	if a.Type == tuple.TypeDate && b.Type == tuple.TypeText {
		if parsed, err := parseDateText(b.Text); err == nil {
			b = parsed
		}
	} else if a.Type == tuple.TypeText && b.Type == tuple.TypeDate {
		if parsed, err := parseDateText(a.Text); err == nil {
			a = parsed
		}
	} else if a.Type == tuple.TypeTimestamp && b.Type == tuple.TypeText {
		if parsed, err := parseTimestampText(b.Text); err == nil {
			b = parsed
		}
	} else if a.Type == tuple.TypeText && b.Type == tuple.TypeTimestamp {
		if parsed, err := parseTimestampText(a.Text); err == nil {
			a = parsed
		}
	}

	// Coerce text to NUMERIC for cross-type comparisons.
	if a.Type == tuple.TypeNumeric && b.Type == tuple.TypeText {
		b = tuple.DNumeric(b.Text)
	} else if a.Type == tuple.TypeText && b.Type == tuple.TypeNumeric {
		a = tuple.DNumeric(a.Text)
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
	case tuple.TypeDate, tuple.TypeTimestamp:
		// Both stored as I64 (days or microseconds since epoch).
		if a.I64 < b.I64 {
			return -1
		}
		if a.I64 > b.I64 {
			return 1
		}
		return 0
	case tuple.TypeNumeric:
		return compareNumericStrings(a.Text, b.Text)
	case tuple.TypeJSON, tuple.TypeUUID, tuple.TypeBytea, tuple.TypeArray:
		if a.Text < b.Text {
			return -1
		}
		if a.Text > b.Text {
			return 1
		}
		return 0
	case tuple.TypeInterval:
		// Compare by total duration: convert months to approximate microseconds.
		aTot := int64(a.I32)*30*24*3600*1e6 + a.I64
		bTot := int64(b.I32)*30*24*3600*1e6 + b.I64
		if aTot < bTot {
			return -1
		}
		if aTot > bTot {
			return 1
		}
		return 0
	case tuple.TypeMoney:
		if a.I64 < b.I64 {
			return -1
		}
		if a.I64 > b.I64 {
			return 1
		}
		return 0
	default:
		return 0
	}
}

// compareNumericStrings compares two decimal number strings.
func compareNumericStrings(a, b string) int {
	af, _, erra := new(big.Float).Parse(a, 10)
	bf, _, errb := new(big.Float).Parse(b, 10)
	if erra != nil || errb != nil {
		// Fallback to string comparison.
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	}
	return af.Cmp(bf)
}

func toFloat64(d tuple.Datum) (float64, bool) {
	switch d.Type {
	case tuple.TypeFloat64:
		return d.F64, true
	case tuple.TypeInt32:
		return float64(d.I32), true
	case tuple.TypeInt64:
		return float64(d.I64), true
	case tuple.TypeNumeric:
		f, _, err := new(big.Float).Parse(d.Text, 10)
		if err != nil {
			return 0, false
		}
		v, _ := f.Float64()
		return v, true
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

// SubqueryExecFunc is a callback that executes a subquery and returns
// the result rows. The outer row is provided for correlated subqueries.
// The implementation is injected by the SQL executor.
type SubqueryExecFunc func(subQuery *Query, outerRow *Row) (cols []string, rows [][]tuple.Datum, err error)

// SubqueryExecutor is set by the SQL executor to provide subquery
// execution capability. This avoids a circular dependency between
// the planner and executor packages.
var SubqueryExecutor SubqueryExecFunc

// OuterRowContext holds the current outer row for correlated subquery evaluation.
// It is set by the filter/project executor before evaluating expressions that
// may contain subqueries, and checked by ExprColumn as a fallback.
var OuterRowContext *Row

// ExprSubLink evaluates a subquery expression (EXISTS, IN, ANY, ALL, scalar).
type ExprSubLink struct {
	LinkType SubLinkType
	TestExpr Expr   // outer expression for comparison (nil for EXISTS/scalar)
	OpName   string // comparison operator
	Subquery *Query // analyzed sub-SELECT
}

func (e *ExprSubLink) String() string {
	switch e.LinkType {
	case SubLinkExists:
		return "EXISTS(...)"
	case SubLinkAny:
		return "IN(...)"
	case SubLinkAll:
		return "NOT IN(...)"
	case SubLinkExprSubquery:
		return "(SELECT ...)"
	}
	return "SUBLINK"
}

func (e *ExprSubLink) Eval(row *Row) (tuple.Datum, error) {
	if SubqueryExecutor == nil {
		return tuple.DNull(), fmt.Errorf("subquery execution not available")
	}

	// Set the outer row context so correlated column references can resolve.
	savedOuter := OuterRowContext
	OuterRowContext = row
	defer func() { OuterRowContext = savedOuter }()

	// Execute the subquery.
	_, subRows, err := SubqueryExecutor(e.Subquery, row)
	if err != nil {
		return tuple.DNull(), fmt.Errorf("subquery: %w", err)
	}

	switch e.LinkType {
	case SubLinkExists:
		return tuple.DBool(len(subRows) > 0), nil

	case SubLinkExprSubquery:
		// Scalar subquery: return the first column of the first row.
		if len(subRows) == 0 {
			return tuple.DNull(), nil
		}
		if len(subRows) > 1 {
			return tuple.DNull(), fmt.Errorf("scalar subquery returned more than one row")
		}
		if len(subRows[0]) == 0 {
			return tuple.DNull(), nil
		}
		return subRows[0][0], nil

	case SubLinkAny:
		// expr IN (SELECT ...) / expr = ANY (SELECT ...)
		// True if any row matches, false if none match (and no NULLs),
		// NULL if no match but NULLs were present.
		if e.TestExpr == nil {
			return tuple.DNull(), fmt.Errorf("ANY/IN subquery missing test expression")
		}
		testVal, err := e.TestExpr.Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if testVal.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		hasNull := false
		for _, subRow := range subRows {
			if len(subRow) == 0 {
				continue
			}
			subVal := subRow[0]
			if subVal.Type == tuple.TypeNull {
				hasNull = true
				continue
			}
			if sublinkCompare(e.OpName, testVal, subVal) {
				return tuple.DBool(true), nil
			}
		}
		if hasNull {
			return tuple.DNull(), nil
		}
		return tuple.DBool(false), nil

	case SubLinkAll:
		// NOT IN (SELECT ...) is parsed as ALL_SUBLINK with OpName "=".
		// Semantics: true if the test value does NOT match ANY subquery row.
		// This is the negation of ANY_SUBLINK with the same operator.
		if e.TestExpr == nil {
			return tuple.DNull(), fmt.Errorf("ALL subquery missing test expression")
		}
		testVal, err := e.TestExpr.Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if testVal.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		hasNull := false
		for _, subRow := range subRows {
			if len(subRow) == 0 {
				continue
			}
			subVal := subRow[0]
			if subVal.Type == tuple.TypeNull {
				hasNull = true
				continue
			}
			if sublinkCompare(e.OpName, testVal, subVal) {
				// Found a match → NOT IN is false.
				return tuple.DBool(false), nil
			}
		}
		if hasNull {
			return tuple.DNull(), nil
		}
		return tuple.DBool(true), nil
	}

	return tuple.DNull(), nil
}

// sublinkCompare applies a comparison operator between two datums.
func sublinkCompare(op string, a, b tuple.Datum) bool {
	cmp := CompareDatums(a, b)
	switch op {
	case "=":
		return cmp == 0
	case "<>", "!=":
		return cmp != 0
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	default:
		return cmp == 0
	}
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
