package sql

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// PL/JS (JavaScript) functions
// ---------------------------------------------------------------------------

func TestPLJS_SimpleReturn(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE FUNCTION js_add(a integer, b integer) RETURNS integer AS $$
		return a + b;
	$$ LANGUAGE pljs`)

	r := mustExecR(t, ex, "SELECT js_add(3, 4)")
	if r.Rows[0][0].I64 != 7 {
		t.Fatalf("js_add(3,4) expected 7, got %v", r.Rows[0][0])
	}
}

func TestPLJS_StringConcat(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE FUNCTION js_greet(name text) RETURNS text AS $$
		return "Hello, " + name + "!";
	$$ LANGUAGE pljs`)

	r := mustExecR(t, ex, "SELECT js_greet('World')")
	if r.Rows[0][0].Text != "Hello, World!" {
		t.Fatalf("js_greet expected 'Hello, World!', got %q", r.Rows[0][0].Text)
	}
}

func TestPLJS_Conditional(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE FUNCTION js_abs(x integer) RETURNS integer AS $$
		if (x < 0) return -x;
		return x;
	$$ LANGUAGE pljs`)

	r := mustExecR(t, ex, "SELECT js_abs(-42)")
	if r.Rows[0][0].I64 != 42 {
		t.Fatalf("js_abs(-42) expected 42, got %v", r.Rows[0][0])
	}

	r = mustExecR(t, ex, "SELECT js_abs(10)")
	if r.Rows[0][0].I64 != 10 {
		t.Fatalf("js_abs(10) expected 10, got %v", r.Rows[0][0])
	}
}

func TestPLJS_SPIExecute(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE js_data (id INT, val TEXT)")
	mustExec(t, ex, "INSERT INTO js_data VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

	mustExec(t, ex, `CREATE FUNCTION js_count_rows() RETURNS integer AS $$
		var rows = plv8.execute("SELECT count(*) AS cnt FROM js_data");
		return rows[0].cnt;
	$$ LANGUAGE pljs`)

	r := mustExecR(t, ex, "SELECT js_count_rows()")
	if r.Rows[0][0].I64 != 3 {
		t.Fatalf("js_count_rows() expected 3, got %v", r.Rows[0][0])
	}
}

func TestPLJS_SPIQuery(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE js_items (name TEXT, price INT)")
	mustExec(t, ex, "INSERT INTO js_items VALUES ('apple', 100), ('banana', 50), ('cherry', 200)")

	mustExec(t, ex, `CREATE FUNCTION js_expensive() RETURNS text AS $$
		var rows = plv8.execute("SELECT name FROM js_items WHERE price > 80 ORDER BY name");
		var names = [];
		for (var i = 0; i < rows.length; i++) {
			names.push(rows[i].name);
		}
		return names.join(", ");
	$$ LANGUAGE pljs`)

	r := mustExecR(t, ex, "SELECT js_expensive()")
	if r.Rows[0][0].Text != "apple, cherry" {
		t.Fatalf("js_expensive() expected 'apple, cherry', got %q", r.Rows[0][0].Text)
	}
}

func TestPLJS_BoolReturn(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE FUNCTION js_is_even(n integer) RETURNS boolean AS $$
		return n % 2 === 0;
	$$ LANGUAGE pljs`)

	r := mustExecR(t, ex, "SELECT js_is_even(4)")
	if !r.Rows[0][0].Bool {
		t.Fatal("js_is_even(4) expected true")
	}

	r = mustExecR(t, ex, "SELECT js_is_even(3)")
	if r.Rows[0][0].Bool {
		t.Fatal("js_is_even(3) expected false")
	}
}

func TestPLJS_DoBlock(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE js_do_test (val INT)")

	_, err := ex.Exec(`DO $$ plv8.execute("INSERT INTO js_do_test VALUES (42)"); $$ LANGUAGE pljs`)
	if err != nil {
		t.Fatalf("DO pljs failed: %v", err)
	}

	r := mustExecR(t, ex, "SELECT val FROM js_do_test")
	if len(r.Rows) != 1 || r.Rows[0][0].I64 != 42 {
		t.Fatalf("expected 42 from DO block insert, got %v", r.Rows)
	}
}

// ---------------------------------------------------------------------------
// PL/Starlark functions
// ---------------------------------------------------------------------------

func TestPLStarlark_SimpleReturn(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE FUNCTION star_add(a integer, b integer) RETURNS integer AS $$
		return a + b
	$$ LANGUAGE plstarlark`)

	r := mustExecR(t, ex, "SELECT star_add(10, 20)")
	if r.Rows[0][0].I64 != 30 {
		t.Fatalf("star_add(10,20) expected 30, got %v", r.Rows[0][0])
	}
}

func TestPLStarlark_StringOps(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE FUNCTION star_upper(s text) RETURNS text AS $$
		return s.upper()
	$$ LANGUAGE plstarlark`)

	r := mustExecR(t, ex, "SELECT star_upper('hello')")
	if r.Rows[0][0].Text != "HELLO" {
		t.Fatalf("star_upper expected 'HELLO', got %q", r.Rows[0][0].Text)
	}
}

func TestPLStarlark_Conditional(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE FUNCTION star_max(a integer, b integer) RETURNS integer AS $$
		if a > b:
		    return a
		return b
	$$ LANGUAGE plstarlark`)

	r := mustExecR(t, ex, "SELECT star_max(5, 9)")
	if r.Rows[0][0].I64 != 9 {
		t.Fatalf("star_max(5,9) expected 9, got %v", r.Rows[0][0])
	}
}

func TestPLStarlark_ListComprehension(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE FUNCTION star_squares(n integer) RETURNS text AS $$
		return str([x*x for x in range(n)])
	$$ LANGUAGE plstarlark`)

	r := mustExecR(t, ex, "SELECT star_squares(5)")
	if r.Rows[0][0].Text != "[0, 1, 4, 9, 16]" {
		t.Fatalf("star_squares(5) expected '[0, 1, 4, 9, 16]', got %q", r.Rows[0][0].Text)
	}
}

func TestPLStarlark_SPIExecute(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE star_data (id INT, name TEXT)")
	mustExec(t, ex, "INSERT INTO star_data VALUES (1, 'alice'), (2, 'bob')")

	mustExec(t, ex, `CREATE FUNCTION star_count() RETURNS integer AS $$
		rows = spi.execute("SELECT count(*) AS cnt FROM star_data")
		return rows[0]["cnt"]
	$$ LANGUAGE plstarlark`)

	r := mustExecR(t, ex, "SELECT star_count()")
	if r.Rows[0][0].I64 != 2 {
		t.Fatalf("star_count() expected 2, got %v", r.Rows[0][0])
	}
}

func TestPLStarlark_SPIQuery(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE star_items (name TEXT, score INT)")
	mustExec(t, ex, "INSERT INTO star_items VALUES ('x', 10), ('y', 20), ('z', 30)")

	mustExec(t, ex, `CREATE FUNCTION star_high_scorers() RETURNS text AS $$
		rows = spi.execute("SELECT name FROM star_items WHERE score >= 20 ORDER BY name")
		return ", ".join([r["name"] for r in rows])
	$$ LANGUAGE plstarlark`)

	r := mustExecR(t, ex, "SELECT star_high_scorers()")
	if r.Rows[0][0].Text != "y, z" {
		t.Fatalf("star_high_scorers() expected 'y, z', got %q", r.Rows[0][0].Text)
	}
}

func TestPLStarlark_DoBlock(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE star_do_test (val INT)")

	_, err := ex.Exec("DO $$\nspi.execute(\"INSERT INTO star_do_test VALUES (99)\")\n$$ LANGUAGE plstarlark")
	if err != nil {
		t.Fatalf("DO plstarlark failed: %v", err)
	}

	r := mustExecR(t, ex, "SELECT val FROM star_do_test")
	if len(r.Rows) != 1 || r.Rows[0][0].I64 != 99 {
		t.Fatalf("expected 99 from DO block insert, got %v", r.Rows)
	}
}

// ---------------------------------------------------------------------------
// Language aliases
// ---------------------------------------------------------------------------

func TestPLJS_LanguageAlias(t *testing.T) {
	ex := newTestExecutor(t)
	// "javascript" alias should work.
	mustExec(t, ex, `CREATE FUNCTION js_alias_test(x integer) RETURNS integer AS $$
		return x * 2;
	$$ LANGUAGE javascript`)

	r := mustExecR(t, ex, "SELECT js_alias_test(5)")
	if r.Rows[0][0].I64 != 10 {
		t.Fatalf("expected 10, got %v", r.Rows[0][0])
	}
}

func TestPLJS_NullHandling(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE FUNCTION js_null_test() RETURNS integer AS $$
		return null;
	$$ LANGUAGE pljs`)

	r := mustExecR(t, ex, "SELECT js_null_test()")
	if r.Rows[0][0].Type != 0 { // TypeNull = 0
		t.Fatalf("expected NULL, got %v", r.Rows[0][0])
	}
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

func TestPLJS_Error(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE FUNCTION js_bad() RETURNS integer AS $$
		return undefined_var;
	$$ LANGUAGE pljs`)

	_, err := ex.Exec("SELECT js_bad()")
	if err == nil {
		t.Fatal("expected error from js_bad()")
	}
	if !strings.Contains(err.Error(), "pljs") {
		t.Fatalf("expected pljs error, got: %v", err)
	}
}

func TestPLStarlark_Error(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE FUNCTION star_bad() RETURNS integer AS $$
		return undefined_var
	$$ LANGUAGE plstarlark`)

	_, err := ex.Exec("SELECT star_bad()")
	if err == nil {
		t.Fatal("expected error from star_bad()")
	}
	if !strings.Contains(err.Error(), "plstarlark") {
		t.Fatalf("expected plstarlark error, got: %v", err)
	}
}
