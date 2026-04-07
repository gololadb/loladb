package sql

import (
	"math"
	"strings"
	"testing"

	"github.com/gololadb/loladb/pkg/tuple"
)

// ---------------------------------------------------------------------------
// Statistical aggregates
// ---------------------------------------------------------------------------

func TestStddevSamp(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE stats (val INT)`)
	mustExec(t, ex, `INSERT INTO stats VALUES (2)`)
	mustExec(t, ex, `INSERT INTO stats VALUES (4)`)
	mustExec(t, ex, `INSERT INTO stats VALUES (4)`)
	mustExec(t, ex, `INSERT INTO stats VALUES (4)`)
	mustExec(t, ex, `INSERT INTO stats VALUES (5)`)
	mustExec(t, ex, `INSERT INTO stats VALUES (5)`)
	mustExec(t, ex, `INSERT INTO stats VALUES (7)`)
	mustExec(t, ex, `INSERT INTO stats VALUES (9)`)

	r, err := ex.Exec(`SELECT stddev(val) FROM stats`)
	if err != nil {
		t.Fatal(err)
	}
	// stddev_samp of {2,4,4,4,5,5,7,9} ≈ 2.138
	got := r.Rows[0][0].F64
	if math.Abs(got-2.138) > 0.01 {
		t.Fatalf("expected stddev ≈ 2.138, got %f", got)
	}
}

func TestVariancePop(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE vpop (val INT)`)
	mustExec(t, ex, `INSERT INTO vpop VALUES (1)`)
	mustExec(t, ex, `INSERT INTO vpop VALUES (2)`)
	mustExec(t, ex, `INSERT INTO vpop VALUES (3)`)

	r, err := ex.Exec(`SELECT var_pop(val) FROM vpop`)
	if err != nil {
		t.Fatal(err)
	}
	// var_pop of {1,2,3}: mean=2, var = ((1-2)²+(2-2)²+(3-2)²)/3 = 2/3 ≈ 0.6667
	got := r.Rows[0][0].F64
	if math.Abs(got-0.6667) > 0.01 {
		t.Fatalf("expected var_pop ≈ 0.6667, got %f", got)
	}
}

func TestStddevSingleRow(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE single (val INT)`)
	mustExec(t, ex, `INSERT INTO single VALUES (42)`)

	r, err := ex.Exec(`SELECT stddev(val) FROM single`)
	if err != nil {
		t.Fatal(err)
	}
	// stddev of single value is NULL (sample stddev needs n>=2).
	if r.Rows[0][0].Type != tuple.TypeNull {
		t.Fatalf("expected NULL for single-row stddev, got %v", r.Rows[0][0])
	}
}

// ---------------------------------------------------------------------------
// Materialized views
// ---------------------------------------------------------------------------

func TestCreateMatView(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE mv_src (id INT, name TEXT)`)
	mustExec(t, ex, `INSERT INTO mv_src VALUES (1, 'Alice')`)
	mustExec(t, ex, `INSERT INTO mv_src VALUES (2, 'Bob')`)

	mustExec(t, ex, `CREATE MATERIALIZED VIEW mv_test AS SELECT id, name FROM mv_src`)

	r, err := ex.Exec(`SELECT name FROM mv_test ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "Alice" {
		t.Errorf("expected 'Alice', got %q", r.Rows[0][0].Text)
	}
}

func TestRefreshMatView(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE rmv_src (id INT, val TEXT)`)
	mustExec(t, ex, `INSERT INTO rmv_src VALUES (1, 'old')`)

	mustExec(t, ex, `CREATE MATERIALIZED VIEW rmv_test AS SELECT id, val FROM rmv_src`)

	// Verify initial data.
	r, err := ex.Exec(`SELECT val FROM rmv_test`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Text != "old" {
		t.Fatalf("expected 'old', got %v", r.Rows)
	}

	// Update source and refresh.
	mustExec(t, ex, `INSERT INTO rmv_src VALUES (2, 'new')`)
	mustExec(t, ex, `REFRESH MATERIALIZED VIEW rmv_test`)

	r, err = ex.Exec(`SELECT count(*) FROM rmv_test`)
	if err != nil {
		t.Fatal(err)
	}
	if intVal(r.Rows[0][0]) != 2 {
		t.Fatalf("expected 2 rows after refresh, got %d", intVal(r.Rows[0][0]))
	}
}

func TestDropMatView(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE dmv_src (id INT)`)
	mustExec(t, ex, `INSERT INTO dmv_src VALUES (1)`)
	mustExec(t, ex, `CREATE MATERIALIZED VIEW dmv_test AS SELECT * FROM dmv_src`)

	mustExec(t, ex, `DROP MATERIALIZED VIEW dmv_test`)

	_, err := ex.Exec(`SELECT * FROM dmv_test`)
	if err == nil {
		t.Fatal("expected error after DROP MATERIALIZED VIEW")
	}
}

// ---------------------------------------------------------------------------
// Array constructors and subscripting
// ---------------------------------------------------------------------------

func TestArrayConstructor(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`SELECT ARRAY[1, 2, 3]`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "{1,2,3}" {
		t.Fatalf("expected '{1,2,3}', got %q", r.Rows[0][0].Text)
	}
}

func TestArraySubscript(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`SELECT (ARRAY[10, 20, 30])[2]`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	// 1-based indexing: [2] = 20
	if r.Rows[0][0].Text != "20" {
		t.Fatalf("expected '20', got %q", r.Rows[0][0].Text)
	}
}

func TestArrayConstructorStrings(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`SELECT ARRAY['a', 'b', 'c']`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Text != "{a,b,c}" {
		t.Fatalf("expected '{a,b,c}', got %q", r.Rows[0][0].Text)
	}
}

// ---------------------------------------------------------------------------
// JSON delete operators
// ---------------------------------------------------------------------------

func TestJSONDeleteKey(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`SELECT '{"a":1,"b":2,"c":3}'::jsonb - 'b'`)
	if err != nil {
		t.Fatal(err)
	}
	text := r.Rows[0][0].Text
	if strings.Contains(text, `"b"`) {
		t.Fatalf("expected key 'b' to be deleted, got %s", text)
	}
	if !strings.Contains(text, `"a"`) || !strings.Contains(text, `"c"`) {
		t.Fatalf("expected keys 'a' and 'c' to remain, got %s", text)
	}
}

func TestJSONDeletePath(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`SELECT '{"a":{"b":1},"c":2}'::jsonb #- '{a,b}'`)
	if err != nil {
		t.Fatal(err)
	}
	text := r.Rows[0][0].Text
	// After deleting path {a,b}, "a" should be an empty object.
	if !strings.Contains(text, `"a"`) {
		t.Fatalf("expected key 'a' to remain, got %s", text)
	}
	if strings.Contains(text, `"b"`) {
		t.Fatalf("expected nested key 'b' to be deleted, got %s", text)
	}
}

// Suppress unused import warnings.
var _ = math.Abs
var _ = strings.Contains
var _ tuple.DatumType
