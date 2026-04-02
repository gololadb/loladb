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
