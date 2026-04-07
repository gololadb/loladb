package sql

import (
	"math"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// JSON ?| and ?& operators
// ---------------------------------------------------------------------------

func TestJSONExistsAny(t *testing.T) {
	ex := newTestExecutor(t)
	// ?| returns true if any key in the array exists in the object.
	r, err := ex.Exec(`SELECT '{"a":1,"b":2,"c":3}'::jsonb ?| array['b','d']`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "true" && !r.Rows[0][0].Bool {
		t.Fatalf("expected true, got %v", r.Rows[0][0])
	}
}

func TestJSONExistsAnyFalse(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT '{"a":1,"b":2}'::jsonb ?| array['x','y']`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "false" && r.Rows[0][0].Bool {
		t.Fatalf("expected false, got %v", r.Rows[0][0])
	}
}

func TestJSONExistsAll(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT '{"a":1,"b":2,"c":3}'::jsonb ?& array['a','b']`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "true" && !r.Rows[0][0].Bool {
		t.Fatalf("expected true, got %v", r.Rows[0][0])
	}
}

func TestJSONExistsAllFalse(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT '{"a":1,"b":2}'::jsonb ?& array['a','x']`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "false" && r.Rows[0][0].Bool {
		t.Fatalf("expected false, got %v", r.Rows[0][0])
	}
}

// ---------------------------------------------------------------------------
// Ordered-set aggregates
// ---------------------------------------------------------------------------

func TestPercentileCont(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE salaries (salary INT)`)
	mustExec(t, ex, `INSERT INTO salaries VALUES (100)`)
	mustExec(t, ex, `INSERT INTO salaries VALUES (200)`)
	mustExec(t, ex, `INSERT INTO salaries VALUES (300)`)
	mustExec(t, ex, `INSERT INTO salaries VALUES (400)`)
	mustExec(t, ex, `INSERT INTO salaries VALUES (500)`)

	r, err := ex.Exec(`SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY salary) FROM salaries`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	got := r.Rows[0][0].F64
	if math.Abs(got-300.0) > 0.01 {
		t.Fatalf("expected median ~300, got %v", got)
	}
}

func TestPercentileDisc(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE scores (score INT)`)
	mustExec(t, ex, `INSERT INTO scores VALUES (10)`)
	mustExec(t, ex, `INSERT INTO scores VALUES (20)`)
	mustExec(t, ex, `INSERT INTO scores VALUES (30)`)
	mustExec(t, ex, `INSERT INTO scores VALUES (40)`)

	r, err := ex.Exec(`SELECT percentile_disc(0.75) WITHIN GROUP (ORDER BY score) FROM scores`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	// 0.75 of 4 values = index 2 (0-based), value 30
	got := r.Rows[0][0]
	val := got.I32
	if got.I64 != 0 {
		val = int32(got.I64)
	}
	if val != 30 {
		t.Fatalf("expected 30, got %v", got)
	}
}

func TestMode(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE colors (color TEXT)`)
	mustExec(t, ex, `INSERT INTO colors VALUES ('red')`)
	mustExec(t, ex, `INSERT INTO colors VALUES ('blue')`)
	mustExec(t, ex, `INSERT INTO colors VALUES ('red')`)
	mustExec(t, ex, `INSERT INTO colors VALUES ('green')`)
	mustExec(t, ex, `INSERT INTO colors VALUES ('red')`)

	r, err := ex.Exec(`SELECT mode() WITHIN GROUP (ORDER BY color) FROM colors`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "red" {
		t.Fatalf("expected 'red', got %v", r.Rows[0][0])
	}
}

// ---------------------------------------------------------------------------
// unnest()
// ---------------------------------------------------------------------------

func TestUnnest(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT unnest(ARRAY[10,20,30])`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
	vals := []string{}
	for _, row := range r.Rows {
		vals = append(vals, row[0].Text)
	}
	expected := "10,20,30"
	got := strings.Join(vals, ",")
	if got != expected {
		t.Fatalf("expected %s, got %s", expected, got)
	}
}

func TestUnnestStrings(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT unnest(ARRAY['a','b','c'])`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
	vals := []string{}
	for _, row := range r.Rows {
		vals = append(vals, row[0].Text)
	}
	// Array elements may include quotes from the PG literal format.
	got := strings.Join(vals, ",")
	if !strings.Contains(got, "a") || !strings.Contains(got, "b") || !strings.Contains(got, "c") {
		t.Fatalf("expected a,b,c elements, got %s", got)
	}
}

// ---------------------------------------------------------------------------
// Cursors: DECLARE, FETCH, CLOSE
// ---------------------------------------------------------------------------

func TestDeclareFetchClose(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE items (id INT, name TEXT)`)
	mustExec(t, ex, `INSERT INTO items VALUES (1, 'alpha')`)
	mustExec(t, ex, `INSERT INTO items VALUES (2, 'beta')`)
	mustExec(t, ex, `INSERT INTO items VALUES (3, 'gamma')`)
	mustExec(t, ex, `INSERT INTO items VALUES (4, 'delta')`)

	mustExec(t, ex, `DECLARE cur CURSOR FOR SELECT id, name FROM items`)

	// Fetch first 2 rows.
	r, err := ex.Exec(`FETCH 2 FROM cur`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}

	// Fetch next 2 rows.
	r2, err := ex.Exec(`FETCH 2 FROM cur`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r2.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r2.Rows))
	}

	// Fetch past end — should return 0 rows.
	r3, err := ex.Exec(`FETCH 1 FROM cur`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r3.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(r3.Rows))
	}

	// Close cursor.
	_, err = ex.Exec(`CLOSE cur`)
	if err != nil {
		t.Fatal(err)
	}

	// Fetch from closed cursor should error.
	_, err = ex.Exec(`FETCH 1 FROM cur`)
	if err == nil {
		t.Fatal("expected error fetching from closed cursor")
	}
}

func TestCloseAll(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE t1 (x INT)`)
	mustExec(t, ex, `INSERT INTO t1 VALUES (1)`)
	mustExec(t, ex, `INSERT INTO t1 VALUES (2)`)

	mustExec(t, ex, `DECLARE c1 CURSOR FOR SELECT x FROM t1`)
	mustExec(t, ex, `DECLARE c2 CURSOR FOR SELECT x FROM t1`)

	mustExec(t, ex, `CLOSE ALL`)

	_, err := ex.Exec(`FETCH 1 FROM c1`)
	if err == nil {
		t.Fatal("expected error after CLOSE ALL")
	}
}

func TestCursorDuplicateName(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE t2 (x INT)`)
	mustExec(t, ex, `INSERT INTO t2 VALUES (1)`)

	mustExec(t, ex, `DECLARE dup CURSOR FOR SELECT x FROM t2`)
	_, err := ex.Exec(`DECLARE dup CURSOR FOR SELECT x FROM t2`)
	if err == nil {
		t.Fatal("expected error for duplicate cursor name")
	}
}
