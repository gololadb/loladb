package sql

import (
	"testing"

	"github.com/gololadb/loladb/pkg/tuple"
)

// ---------------------------------------------------------------------------
// BETWEEN SYMMETRIC
// ---------------------------------------------------------------------------

func TestBetweenSymmetric(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE bs1 (id INT)`)
	mustExec(t, ex, `INSERT INTO bs1 VALUES (1)`)
	mustExec(t, ex, `INSERT INTO bs1 VALUES (5)`)
	mustExec(t, ex, `INSERT INTO bs1 VALUES (10)`)

	// Normal order: 3 BETWEEN SYMMETRIC 1 AND 10 → same as BETWEEN 1 AND 10
	r, err := ex.Exec(`SELECT id FROM bs1 WHERE id BETWEEN SYMMETRIC 1 AND 10`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}

	// Reversed bounds: BETWEEN SYMMETRIC 10 AND 1 → should still match
	r, err = ex.Exec(`SELECT id FROM bs1 WHERE id BETWEEN SYMMETRIC 10 AND 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows with reversed bounds, got %d", len(r.Rows))
	}

	// NOT BETWEEN SYMMETRIC
	r, err = ex.Exec(`SELECT id FROM bs1 WHERE id NOT BETWEEN SYMMETRIC 2 AND 8`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows for NOT BETWEEN SYMMETRIC, got %d", len(r.Rows))
	}
}

// ---------------------------------------------------------------------------
// SIMILAR TO
// ---------------------------------------------------------------------------

func TestSimilarTo(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE st1 (name TEXT)`)
	mustExec(t, ex, `INSERT INTO st1 VALUES ('foo')`)
	mustExec(t, ex, `INSERT INTO st1 VALUES ('bar')`)
	mustExec(t, ex, `INSERT INTO st1 VALUES ('foobar')`)
	mustExec(t, ex, `INSERT INTO st1 VALUES ('baz')`)

	// % wildcard
	r, err := ex.Exec(`SELECT name FROM st1 WHERE name SIMILAR TO 'foo%'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows matching 'foo%%', got %d", len(r.Rows))
	}

	// | alternation
	r, err = ex.Exec(`SELECT name FROM st1 WHERE name SIMILAR TO '(foo|bar)'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows matching '(foo|bar)', got %d", len(r.Rows))
	}

	// _ single char
	r, err = ex.Exec(`SELECT name FROM st1 WHERE name SIMILAR TO 'ba_'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows matching 'ba_', got %d", len(r.Rows))
	}
}

// ---------------------------------------------------------------------------
// Regex operators: ~, ~*, !~, !~*
// ---------------------------------------------------------------------------

func TestRegexOperators(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE rx1 (val TEXT)`)
	mustExec(t, ex, `INSERT INTO rx1 VALUES ('Hello')`)
	mustExec(t, ex, `INSERT INTO rx1 VALUES ('world')`)
	mustExec(t, ex, `INSERT INTO rx1 VALUES ('HELLO')`)

	// ~ case-sensitive match
	r, err := ex.Exec(`SELECT val FROM rx1 WHERE val ~ '^H'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows for ~ '^H', got %d", len(r.Rows))
	}

	// ~* case-insensitive match
	r, err = ex.Exec(`SELECT val FROM rx1 WHERE val ~* '^h'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows for ~* '^h', got %d", len(r.Rows))
	}

	// !~ negated case-sensitive
	r, err = ex.Exec(`SELECT val FROM rx1 WHERE val !~ '^H'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row for !~ '^H', got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "world" {
		t.Fatalf("expected 'world', got %q", r.Rows[0][0].Text)
	}

	// !~* negated case-insensitive
	r, err = ex.Exec(`SELECT val FROM rx1 WHERE val !~* '^h'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row for !~* '^h', got %d", len(r.Rows))
	}
}

// ---------------------------------------------------------------------------
// starts_with() function and ^@ operator
// ---------------------------------------------------------------------------

func TestStartsWith(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE sw1 (name TEXT)`)
	mustExec(t, ex, `INSERT INTO sw1 VALUES ('apple')`)
	mustExec(t, ex, `INSERT INTO sw1 VALUES ('apricot')`)
	mustExec(t, ex, `INSERT INTO sw1 VALUES ('banana')`)

	// starts_with function
	r, err := ex.Exec(`SELECT name FROM sw1 WHERE starts_with(name, 'ap')`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows for starts_with, got %d", len(r.Rows))
	}

	// ^@ operator
	r, err = ex.Exec(`SELECT name FROM sw1 WHERE name ^@ 'ban'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row for ^@, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "banana" {
		t.Fatalf("expected 'banana', got %q", r.Rows[0][0].Text)
	}
}

// ---------------------------------------------------------------------------
// num_nonnulls() / num_nulls()
// ---------------------------------------------------------------------------

func TestNumNonnullsNumNulls(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`SELECT num_nonnulls(1, NULL, 3, NULL, 5)`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].I64 != 3 {
		t.Fatalf("expected num_nonnulls=3, got %d", r.Rows[0][0].I64)
	}

	r, err = ex.Exec(`SELECT num_nulls(1, NULL, 3, NULL, 5)`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].I64 != 2 {
		t.Fatalf("expected num_nulls=2, got %d", r.Rows[0][0].I64)
	}

	// All non-null
	r, err = ex.Exec(`SELECT num_nulls(1, 2, 3)`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].I64 != 0 {
		t.Fatalf("expected num_nulls=0, got %d", r.Rows[0][0].I64)
	}
}

// ---------------------------------------------------------------------------
// VALUES as standalone query
// ---------------------------------------------------------------------------

func TestValuesStandalone(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`VALUES (1, 'a'), (2, 'b'), (3, 'c')`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
	if len(r.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(r.Columns))
	}
	if r.Columns[0] != "column1" || r.Columns[1] != "column2" {
		t.Fatalf("expected column1/column2, got %v", r.Columns)
	}
	// Check values
	if r.Rows[1][1].Text != "b" {
		t.Fatalf("expected 'b' in row 2 col 2, got %v", r.Rows[1][1])
	}
}

func TestValuesSingleRow(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`VALUES (42)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
}

// ---------------------------------------------------------------------------
// CREATE TABLE ... AS SELECT
// ---------------------------------------------------------------------------

func TestCreateTableAs(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE src (id INT, name TEXT)`)
	mustExec(t, ex, `INSERT INTO src VALUES (1, 'Alice')`)
	mustExec(t, ex, `INSERT INTO src VALUES (2, 'Bob')`)

	r, err := ex.Exec(`CREATE TABLE dst AS SELECT id, name FROM src WHERE id > 0`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "SELECT 2" {
		t.Fatalf("expected 'SELECT 2', got %q", r.Message)
	}

	// Verify the new table.
	r, err = ex.Exec(`SELECT name FROM dst ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "Alice" {
		t.Fatalf("expected 'Alice', got %q", r.Rows[0][0].Text)
	}
}

func TestCreateTableAs_IfNotExists(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE existing (id INT)`)

	// Should not error with IF NOT EXISTS.
	r, err := ex.Exec(`CREATE TABLE IF NOT EXISTS existing AS SELECT 1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "SELECT 0" {
		t.Fatalf("expected 'SELECT 0', got %q", r.Message)
	}
}

func TestCreateTableAs_WithNoData(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE src2 (id INT, val TEXT)`)
	mustExec(t, ex, `INSERT INTO src2 VALUES (1, 'x')`)

	r, err := ex.Exec(`CREATE TABLE empty_copy AS SELECT * FROM src2 WITH NO DATA`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "SELECT 0" {
		t.Fatalf("expected 'SELECT 0', got %q", r.Message)
	}

	// Table should exist but be empty.
	r, err = ex.Exec(`SELECT count(*) FROM empty_copy`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].I64 != 0 {
		t.Fatalf("expected 0 rows, got %d", r.Rows[0][0].I64)
	}
}

// ---------------------------------------------------------------------------
// Table aliases with column lists
// ---------------------------------------------------------------------------

func TestTableAliasColumnList(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE t_alias (id INT, name TEXT)`)
	mustExec(t, ex, `INSERT INTO t_alias VALUES (1, 'Alice')`)

	r, err := ex.Exec(`SELECT x, y FROM t_alias AS t(x, y)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	// Column names should be the aliases.
	if r.Columns[0] != "x" || r.Columns[1] != "y" {
		t.Fatalf("expected columns [x, y], got %v", r.Columns)
	}
	d := r.Rows[0][0]
	var idVal int64
	switch d.Type {
	case tuple.TypeInt32:
		idVal = int64(d.I32)
	case tuple.TypeInt64:
		idVal = d.I64
	}
	if idVal != 1 {
		t.Fatalf("expected id=1, got %v", r.Rows[0][0])
	}
	if r.Rows[0][1].Text != "Alice" {
		t.Fatalf("expected name='Alice', got %v", r.Rows[0][1])
	}
}

func TestSubqueryAliasColumnList(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`SELECT a, b FROM (SELECT 1, 2) AS t(a, b)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Columns[0] != "a" || r.Columns[1] != "b" {
		t.Fatalf("expected columns [a, b], got %v", r.Columns)
	}
}

// ---------------------------------------------------------------------------
// Helpers (avoid redeclaration — mustExec is in copy_test.go)
// ---------------------------------------------------------------------------

var _ = tuple.DNull // ensure import
