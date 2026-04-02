package sql

import (
	"math"
	"strings"
	"testing"

	"github.com/gololadb/loladb/pkg/tuple"
)

// helper to run a SELECT expression and return the single result datum.
func evalExpr(t *testing.T, ex *Executor, expr string) tuple.Datum {
	t.Helper()
	r, err := ex.Exec("SELECT " + expr)
	if err != nil {
		t.Fatalf("SELECT %s: %v", expr, err)
	}
	if len(r.Rows) != 1 || len(r.Rows[0]) != 1 {
		t.Fatalf("SELECT %s: expected 1x1 result, got %dx%d", expr, len(r.Rows), len(r.Rows[0]))
	}
	return r.Rows[0][0]
}

func evalText(t *testing.T, ex *Executor, expr string) string {
	t.Helper()
	d := evalExpr(t, ex, expr)
	return d.Text
}

func evalInt(t *testing.T, ex *Executor, expr string) int64 {
	t.Helper()
	d := evalExpr(t, ex, expr)
	return d.I64
}

func evalFloat(t *testing.T, ex *Executor, expr string) float64 {
	t.Helper()
	d := evalExpr(t, ex, expr)
	return d.F64
}

func evalBool(t *testing.T, ex *Executor, expr string) bool {
	t.Helper()
	d := evalExpr(t, ex, expr)
	return d.Bool
}

func evalNull(t *testing.T, ex *Executor, expr string) bool {
	t.Helper()
	d := evalExpr(t, ex, expr)
	return d.Type == tuple.TypeNull
}

// --- Math function tests ---

func TestBuiltin_Abs(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalFloat(t, ex, "abs(-3.5)"); v != 3.5 {
		t.Fatalf("abs(-3.5) = %v, want 3.5", v)
	}
	if v := evalFloat(t, ex, "abs(3.5)"); v != 3.5 {
		t.Fatalf("abs(3.5) = %v, want 3.5", v)
	}
}

func TestBuiltin_CeilFloor(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalFloat(t, ex, "ceil(2.3)"); v != 3 {
		t.Fatalf("ceil(2.3) = %v, want 3", v)
	}
	if v := evalFloat(t, ex, "floor(2.7)"); v != 2 {
		t.Fatalf("floor(2.7) = %v, want 2", v)
	}
	if v := evalFloat(t, ex, "ceiling(-1.5)"); v != -1 {
		t.Fatalf("ceiling(-1.5) = %v, want -1", v)
	}
}

func TestBuiltin_Round(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalFloat(t, ex, "round(2.5)"); v != 3 {
		t.Fatalf("round(2.5) = %v, want 3", v)
	}
	if v := evalFloat(t, ex, "round(2.4)"); v != 2 {
		t.Fatalf("round(2.4) = %v, want 2", v)
	}
}

func TestBuiltin_Trunc(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalFloat(t, ex, "trunc(2.9)"); v != 2 {
		t.Fatalf("trunc(2.9) = %v, want 2", v)
	}
	if v := evalFloat(t, ex, "truncate(-2.9)"); v != -2 {
		t.Fatalf("truncate(-2.9) = %v, want -2", v)
	}
}

func TestBuiltin_Mod(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalFloat(t, ex, "mod(10.0, 3.0)"); v != 1 {
		t.Fatalf("mod(10,3) = %v, want 1", v)
	}
}

func TestBuiltin_Power(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalFloat(t, ex, "power(2.0, 10.0)"); v != 1024 {
		t.Fatalf("power(2,10) = %v, want 1024", v)
	}
	if v := evalFloat(t, ex, "pow(3.0, 2.0)"); v != 9 {
		t.Fatalf("pow(3,2) = %v, want 9", v)
	}
}

func TestBuiltin_Sqrt(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalFloat(t, ex, "sqrt(16.0)"); v != 4 {
		t.Fatalf("sqrt(16) = %v, want 4", v)
	}
}

func TestBuiltin_Cbrt(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalFloat(t, ex, "cbrt(27.0)"); v != 3 {
		t.Fatalf("cbrt(27) = %v, want 3", v)
	}
}

func TestBuiltin_Sign(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalFloat(t, ex, "sign(-5.0)"); v != -1 {
		t.Fatalf("sign(-5) = %v, want -1", v)
	}
	if v := evalFloat(t, ex, "sign(5.0)"); v != 1 {
		t.Fatalf("sign(5) = %v, want 1", v)
	}
	if v := evalFloat(t, ex, "sign(0.0)"); v != 0 {
		t.Fatalf("sign(0) = %v, want 0", v)
	}
}

func TestBuiltin_Pi(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalFloat(t, ex, "pi()"); math.Abs(v-math.Pi) > 1e-10 {
		t.Fatalf("pi() = %v, want %v", v, math.Pi)
	}
}

func TestBuiltin_Log(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalFloat(t, ex, "ln(1.0)"); v != 0 {
		t.Fatalf("ln(1) = %v, want 0", v)
	}
	if v := evalFloat(t, ex, "log10(100.0)"); math.Abs(v-2) > 1e-10 {
		t.Fatalf("log10(100) = %v, want 2", v)
	}
}

func TestBuiltin_Exp(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalFloat(t, ex, "exp(0.0)"); v != 1 {
		t.Fatalf("exp(0) = %v, want 1", v)
	}
}

func TestBuiltin_Random(t *testing.T) {
	ex := newTestExecutor(t)
	v := evalFloat(t, ex, "random()")
	if v < 0 || v >= 1 {
		t.Fatalf("random() = %v, want [0,1)", v)
	}
}

func TestBuiltin_GreatestLeast(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalFloat(t, ex, "greatest(1.0, 3.0, 2.0)"); v != 3 {
		t.Fatalf("greatest(1,3,2) = %v, want 3", v)
	}
	if v := evalFloat(t, ex, "least(1.0, 3.0, 2.0)"); v != 1 {
		t.Fatalf("least(1,3,2) = %v, want 1", v)
	}
}

// --- String function tests ---

func TestBuiltin_Substring(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalText(t, ex, "substring('hello world', 7)"); v != "world" {
		t.Fatalf("substring('hello world', 7) = %q, want 'world'", v)
	}
	if v := evalText(t, ex, "substring('hello world', 1, 5)"); v != "hello" {
		t.Fatalf("substring('hello world', 1, 5) = %q, want 'hello'", v)
	}
}

func TestBuiltin_Trim(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalText(t, ex, "trim('  hello  ')"); v != "hello" {
		t.Fatalf("trim('  hello  ') = %q, want 'hello'", v)
	}
	if v := evalText(t, ex, "ltrim('  hello')"); v != "hello" {
		t.Fatalf("ltrim('  hello') = %q, want 'hello'", v)
	}
	if v := evalText(t, ex, "rtrim('hello  ')"); v != "hello" {
		t.Fatalf("rtrim('hello  ') = %q, want 'hello'", v)
	}
}

func TestBuiltin_Replace(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalText(t, ex, "replace('hello world', 'world', 'there')"); v != "hello there" {
		t.Fatalf("replace = %q, want 'hello there'", v)
	}
}

func TestBuiltin_Position(t *testing.T) {
	ex := newTestExecutor(t)
	// SQL standard: position(substr IN str)
	if v := evalInt(t, ex, "position('lo' IN 'hello')"); v != 4 {
		t.Fatalf("position('lo' IN 'hello') = %d, want 4", v)
	}
	if v := evalInt(t, ex, "position('xyz' IN 'hello')"); v != 0 {
		t.Fatalf("position('xyz' IN 'hello') = %d, want 0", v)
	}
}

func TestBuiltin_LeftRight(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalText(t, ex, "left('hello', 3)"); v != "hel" {
		t.Fatalf("left('hello', 3) = %q, want 'hel'", v)
	}
	if v := evalText(t, ex, "right('hello', 3)"); v != "llo" {
		t.Fatalf("right('hello', 3) = %q, want 'llo'", v)
	}
}

func TestBuiltin_LpadRpad(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalText(t, ex, "lpad('hi', 5, 'xy')"); v != "xyxhi" {
		t.Fatalf("lpad('hi', 5, 'xy') = %q, want 'xyxhi'", v)
	}
	if v := evalText(t, ex, "rpad('hi', 5, 'xy')"); v != "hixyx" {
		t.Fatalf("rpad('hi', 5, 'xy') = %q, want 'hixyx'", v)
	}
}

func TestBuiltin_Repeat(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalText(t, ex, "repeat('ab', 3)"); v != "ababab" {
		t.Fatalf("repeat('ab', 3) = %q, want 'ababab'", v)
	}
}

func TestBuiltin_Reverse(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalText(t, ex, "reverse('hello')"); v != "olleh" {
		t.Fatalf("reverse('hello') = %q, want 'olleh'", v)
	}
}

func TestBuiltin_SplitPart(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalText(t, ex, "split_part('a,b,c', ',', 2)"); v != "b" {
		t.Fatalf("split_part('a,b,c', ',', 2) = %q, want 'b'", v)
	}
}

// --- CASE expression tests ---

func TestBuiltin_CaseSearched(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE items (id INT, price FLOAT8)`)
	ex.Exec(`INSERT INTO items VALUES (1, 10.0), (2, 50.0), (3, 100.0)`)

	r, err := ex.Exec(`SELECT id, CASE WHEN price < 20.0 THEN 'cheap' WHEN price < 80.0 THEN 'mid' ELSE 'expensive' END FROM items`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
	labels := []string{r.Rows[0][1].Text, r.Rows[1][1].Text, r.Rows[2][1].Text}
	if labels[0] != "cheap" || labels[1] != "mid" || labels[2] != "expensive" {
		t.Fatalf("CASE labels = %v, want [cheap mid expensive]", labels)
	}
}

func TestBuiltin_CaseSimple(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE status (id INT, code INT)`)
	ex.Exec(`INSERT INTO status VALUES (1, 1), (2, 2), (3, 3)`)

	r, err := ex.Exec(`SELECT CASE code WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM status`)
	if err != nil {
		t.Fatal(err)
	}
	labels := []string{r.Rows[0][0].Text, r.Rows[1][0].Text, r.Rows[2][0].Text}
	if labels[0] != "one" || labels[1] != "two" || labels[2] != "other" {
		t.Fatalf("CASE labels = %v, want [one two other]", labels)
	}
}

// --- IS TRUE / IS FALSE tests ---

func TestBuiltin_BooleanTest(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE flags (id INT, val BOOL)`)
	ex.Exec(`INSERT INTO flags VALUES (1, true), (2, false)`)

	r, err := ex.Exec(`SELECT val IS TRUE FROM flags`)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Rows[0][0].Bool || r.Rows[1][0].Bool {
		t.Fatalf("IS TRUE: got %v, %v", r.Rows[0][0].Bool, r.Rows[1][0].Bool)
	}

	r, err = ex.Exec(`SELECT val IS FALSE FROM flags`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Bool || !r.Rows[1][0].Bool {
		t.Fatalf("IS FALSE: got %v, %v", r.Rows[0][0].Bool, r.Rows[1][0].Bool)
	}
}

// --- NULLIF tests ---

func TestBuiltin_NullIf(t *testing.T) {
	ex := newTestExecutor(t)
	// NULLIF(1,1) → NULL
	if !evalNull(t, ex, "nullif(1, 1)") {
		t.Fatal("nullif(1,1) should be NULL")
	}
	// NULLIF(1,2) → 1
	if v := evalInt(t, ex, "nullif(1, 2)"); v != 1 {
		t.Fatalf("nullif(1,2) = %d, want 1", v)
	}
}

// --- COALESCE tests ---

func TestBuiltin_Coalesce(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalInt(t, ex, "coalesce(1, 2)"); v != 1 {
		t.Fatalf("coalesce(1,2) = %d, want 1", v)
	}
}

// --- Null propagation ---

func TestBuiltin_NullPropagation(t *testing.T) {
	ex := newTestExecutor(t)
	// Math functions should return NULL for NULL input
	for _, fn := range []string{"abs", "ceil", "floor", "round", "sqrt"} {
		expr := fn + "(NULL)"
		if !evalNull(t, ex, expr) {
			t.Fatalf("%s should return NULL", expr)
		}
	}
	// String functions should return NULL for NULL input
	for _, fn := range []string{"trim", "reverse"} {
		expr := fn + "(NULL)"
		if !evalNull(t, ex, expr) {
			t.Fatalf("%s should return NULL", expr)
		}
	}
}

// --- Error cases ---

func TestBuiltin_SqrtNegative(t *testing.T) {
	ex := newTestExecutor(t)
	_, err := ex.Exec("SELECT sqrt(-1.0)")
	if err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("sqrt(-1) should error with 'negative', got: %v", err)
	}
}

func TestBuiltin_ModZero(t *testing.T) {
	ex := newTestExecutor(t)
	_, err := ex.Exec("SELECT mod(10.0, 0.0)")
	if err == nil || !strings.Contains(err.Error(), "zero") {
		t.Fatalf("mod(10,0) should error with 'zero', got: %v", err)
	}
}

// --- New string function tests ---

func TestBuiltin_ConcatWs(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalText(t, ex, "concat_ws(', ', 'a', 'b', 'c')"); v != "a, b, c" {
		t.Fatalf("concat_ws = %q, want 'a, b, c'", v)
	}
}

func TestBuiltin_Initcap(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalText(t, ex, "initcap('hello world')"); v != "Hello World" {
		t.Fatalf("initcap = %q, want 'Hello World'", v)
	}
}

func TestBuiltin_Translate(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalText(t, ex, "translate('12345', '143', 'ax')"); v != "a2x5" {
		t.Fatalf("translate = %q, want 'a2x5'", v)
	}
}

func TestBuiltin_Ascii(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalInt(t, ex, "ascii('A')"); v != 65 {
		t.Fatalf("ascii('A') = %d, want 65", v)
	}
}

func TestBuiltin_Chr(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalText(t, ex, "chr(65)"); v != "A" {
		t.Fatalf("chr(65) = %q, want 'A'", v)
	}
}

func TestBuiltin_OctetLength(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalInt(t, ex, "octet_length('hello')"); v != 5 {
		t.Fatalf("octet_length('hello') = %d, want 5", v)
	}
}

func TestBuiltin_BitLength(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalInt(t, ex, "bit_length('hello')"); v != 40 {
		t.Fatalf("bit_length('hello') = %d, want 40", v)
	}
}

func TestBuiltin_Md5(t *testing.T) {
	ex := newTestExecutor(t)
	v := evalText(t, ex, "md5('hello')")
	if v != "5d41402abc4b2a76b9719d911017c592" {
		t.Fatalf("md5('hello') = %q", v)
	}
}

func TestBuiltin_GenRandomUuid(t *testing.T) {
	ex := newTestExecutor(t)
	v := evalText(t, ex, "gen_random_uuid()")
	// UUID v4 format: 8-4-4-4-12 hex chars
	if len(v) != 36 || v[8] != '-' || v[13] != '-' || v[18] != '-' || v[23] != '-' {
		t.Fatalf("gen_random_uuid() = %q, not valid UUID format", v)
	}
}

// --- Overlay ---

func TestBuiltin_Overlay(t *testing.T) {
	ex := newTestExecutor(t)
	v := evalText(t, ex, "overlay('Txxxxas' PLACING 'hom' FROM 2 FOR 4)")
	if v != "Thomas" {
		t.Fatalf("overlay = %q, want 'Thomas'", v)
	}
}

// --- Extract / date_part ---

func TestBuiltin_Extract(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalFloat(t, ex, "extract(year FROM '2024-03-15 10:30:00')"); v != 2024 {
		t.Fatalf("extract year = %v, want 2024", v)
	}
	if v := evalFloat(t, ex, "extract(month FROM '2024-03-15 10:30:00')"); v != 3 {
		t.Fatalf("extract month = %v, want 3", v)
	}
	if v := evalFloat(t, ex, "extract(day FROM '2024-03-15 10:30:00')"); v != 15 {
		t.Fatalf("extract day = %v, want 15", v)
	}
	if v := evalFloat(t, ex, "extract(hour FROM '2024-03-15 10:30:00')"); v != 10 {
		t.Fatalf("extract hour = %v, want 10", v)
	}
}

func TestBuiltin_DatePart(t *testing.T) {
	ex := newTestExecutor(t)
	if v := evalFloat(t, ex, "date_part('year', '2024-03-15')"); v != 2024 {
		t.Fatalf("date_part year = %v, want 2024", v)
	}
}

// --- date_trunc ---

func TestBuiltin_DateTrunc(t *testing.T) {
	ex := newTestExecutor(t)
	v := evalText(t, ex, "date_trunc('month', '2024-03-15 10:30:00')")
	if v != "2024-03-01 00:00:00" {
		t.Fatalf("date_trunc month = %q, want '2024-03-01 00:00:00'", v)
	}
	v = evalText(t, ex, "date_trunc('year', '2024-03-15 10:30:00')")
	if v != "2024-01-01 00:00:00" {
		t.Fatalf("date_trunc year = %q, want '2024-01-01 00:00:00'", v)
	}
}

// --- age ---

func TestBuiltin_Age(t *testing.T) {
	ex := newTestExecutor(t)
	v := evalText(t, ex, "age('2024-06-15', '2024-03-15')")
	if v != "3 mons" {
		t.Fatalf("age = %q, want '3 mons'", v)
	}
}

// --- Regex ---

func TestBuiltin_RegexpReplace(t *testing.T) {
	ex := newTestExecutor(t)
	v := evalText(t, ex, "regexp_replace('hello world', 'world', 'there')")
	if v != "hello there" {
		t.Fatalf("regexp_replace = %q, want 'hello there'", v)
	}
	// With global flag
	v = evalText(t, ex, "regexp_replace('aaa', 'a', 'b', 'g')")
	if v != "bbb" {
		t.Fatalf("regexp_replace global = %q, want 'bbb'", v)
	}
}

func TestBuiltin_RegexpMatch(t *testing.T) {
	ex := newTestExecutor(t)
	v := evalText(t, ex, "regexp_match('abc123def', '[0-9]+')")
	if v != "123" {
		t.Fatalf("regexp_match = %q, want '123'", v)
	}
	// No match → NULL
	if !evalNull(t, ex, "regexp_match('abc', '[0-9]+')") {
		t.Fatal("regexp_match no match should be NULL")
	}
}

// --- Formatting ---

func TestBuiltin_ToChar_Timestamp(t *testing.T) {
	ex := newTestExecutor(t)
	v := evalText(t, ex, "to_char('2024-03-15 10:30:00', 'YYYY-MM-DD')")
	if v != "2024-03-15" {
		t.Fatalf("to_char timestamp = %q, want '2024-03-15'", v)
	}
}

func TestBuiltin_ToChar_Numeric(t *testing.T) {
	ex := newTestExecutor(t)
	v := evalText(t, ex, "to_char(1234.5, '9999.99')")
	if v != "1234.50" {
		t.Fatalf("to_char numeric = %q, want '1234.50'", v)
	}
}

func TestBuiltin_ToNumber(t *testing.T) {
	ex := newTestExecutor(t)
	v := evalFloat(t, ex, "to_number('1,234.56', '9,999.99')")
	if v != 1234.56 {
		t.Fatalf("to_number = %v, want 1234.56", v)
	}
}

func TestBuiltin_ToDate(t *testing.T) {
	ex := newTestExecutor(t)
	v := evalText(t, ex, "to_date('15 03 2024', 'DD MM YYYY')")
	if v != "2024-03-15" {
		t.Fatalf("to_date = %q, want '2024-03-15'", v)
	}
}

func TestBuiltin_ToTimestamp(t *testing.T) {
	ex := newTestExecutor(t)
	// Epoch form
	v := evalText(t, ex, "to_timestamp(0.0)")
	if v != "1970-01-01 00:00:00" {
		t.Fatalf("to_timestamp(0) = %q, want '1970-01-01 00:00:00'", v)
	}
}

// --- Encode/Decode ---

func TestBuiltin_Encode(t *testing.T) {
	ex := newTestExecutor(t)
	v := evalText(t, ex, "encode('hello', 'hex')")
	if v != "68656c6c6f" {
		t.Fatalf("encode hex = %q, want '68656c6c6f'", v)
	}
	v = evalText(t, ex, "encode('hello', 'base64')")
	if v != "aGVsbG8=" {
		t.Fatalf("encode base64 = %q, want 'aGVsbG8='", v)
	}
}

func TestBuiltin_Decode(t *testing.T) {
	ex := newTestExecutor(t)
	v := evalText(t, ex, "decode('68656c6c6f', 'hex')")
	if v != "hello" {
		t.Fatalf("decode hex = %q, want 'hello'", v)
	}
}

// --- Format ---

func TestBuiltin_Format(t *testing.T) {
	ex := newTestExecutor(t)
	v := evalText(t, ex, "format('Hello %s, you are %s', 'world', 'great')")
	if v != "Hello world, you are great" {
		t.Fatalf("format = %q", v)
	}
}

// --- Array functions ---

func TestBuiltin_StringToArray(t *testing.T) {
	ex := newTestExecutor(t)
	v := evalText(t, ex, "string_to_array('a,b,c', ',')")
	if v != "{a,b,c}" {
		t.Fatalf("string_to_array = %q, want '{a,b,c}'", v)
	}
}

func TestBuiltin_ArrayLength(t *testing.T) {
	ex := newTestExecutor(t)
	v := evalInt(t, ex, "array_length(string_to_array('a,b,c', ','), 1)")
	if v != 3 {
		t.Fatalf("array_length = %d, want 3", v)
	}
}
