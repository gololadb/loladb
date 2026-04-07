package sql

import (
	"encoding/json"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Hypothetical-set aggregates
// ---------------------------------------------------------------------------

func TestHypotheticalRank(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE emp (salary INT)`)
	mustExec(t, ex, `INSERT INTO emp VALUES (100)`)
	mustExec(t, ex, `INSERT INTO emp VALUES (200)`)
	mustExec(t, ex, `INSERT INTO emp VALUES (300)`)
	mustExec(t, ex, `INSERT INTO emp VALUES (400)`)

	r, err := ex.Exec(`SELECT rank(250) WITHIN GROUP (ORDER BY salary) FROM emp`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	// 250 would be ranked 3rd (after 100, 200)
	got := r.Rows[0][0].I64
	if got != 3 {
		t.Fatalf("expected rank 3, got %d", got)
	}
}

func TestHypotheticalDenseRank(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE scores2 (score INT)`)
	mustExec(t, ex, `INSERT INTO scores2 VALUES (10)`)
	mustExec(t, ex, `INSERT INTO scores2 VALUES (10)`)
	mustExec(t, ex, `INSERT INTO scores2 VALUES (20)`)
	mustExec(t, ex, `INSERT INTO scores2 VALUES (30)`)

	r, err := ex.Exec(`SELECT dense_rank(15) WITHIN GROUP (ORDER BY score) FROM scores2`)
	if err != nil {
		t.Fatal(err)
	}
	// Distinct values less than 15: {10} → dense_rank = 2
	got := r.Rows[0][0].I64
	if got != 2 {
		t.Fatalf("expected dense_rank 2, got %d", got)
	}
}

func TestHypotheticalPercentRank(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE pr (val INT)`)
	mustExec(t, ex, `INSERT INTO pr VALUES (10)`)
	mustExec(t, ex, `INSERT INTO pr VALUES (20)`)
	mustExec(t, ex, `INSERT INTO pr VALUES (30)`)
	mustExec(t, ex, `INSERT INTO pr VALUES (40)`)

	r, err := ex.Exec(`SELECT percent_rank(25) WITHIN GROUP (ORDER BY val) FROM pr`)
	if err != nil {
		t.Fatal(err)
	}
	// 2 values < 25 (10, 20), N=4 → percent_rank = 2/4 = 0.5
	got := r.Rows[0][0].F64
	if got != 0.5 {
		t.Fatalf("expected 0.5, got %f", got)
	}
}

func TestHypotheticalCumeDist(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE cd (val INT)`)
	mustExec(t, ex, `INSERT INTO cd VALUES (10)`)
	mustExec(t, ex, `INSERT INTO cd VALUES (20)`)
	mustExec(t, ex, `INSERT INTO cd VALUES (30)`)

	r, err := ex.Exec(`SELECT cume_dist(20) WITHIN GROUP (ORDER BY val) FROM cd`)
	if err != nil {
		t.Fatal(err)
	}
	// Values <= 20: {10, 20} = 2, plus hypothetical = 3. N+1 = 4. cume_dist = 3/4 = 0.75
	got := r.Rows[0][0].F64
	if got != 0.75 {
		t.Fatalf("expected 0.75, got %f", got)
	}
}

// ---------------------------------------------------------------------------
// JSON aggregates
// ---------------------------------------------------------------------------

func TestJsonAgg(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE names (name TEXT)`)
	mustExec(t, ex, `INSERT INTO names VALUES ('alice')`)
	mustExec(t, ex, `INSERT INTO names VALUES ('bob')`)
	mustExec(t, ex, `INSERT INTO names VALUES ('charlie')`)

	r, err := ex.Exec(`SELECT json_agg(name) FROM names`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	var arr []string
	if err := json.Unmarshal([]byte(r.Rows[0][0].Text), &arr); err != nil {
		t.Fatalf("invalid JSON array: %v (raw: %s)", err, r.Rows[0][0].Text)
	}
	if len(arr) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(arr))
	}
	if arr[0] != "alice" || arr[1] != "bob" || arr[2] != "charlie" {
		t.Fatalf("unexpected values: %v", arr)
	}
}

func TestJsonObjectAgg(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE kv (k TEXT, v TEXT)`)
	mustExec(t, ex, `INSERT INTO kv VALUES ('a', 'one')`)
	mustExec(t, ex, `INSERT INTO kv VALUES ('b', 'two')`)

	r, err := ex.Exec(`SELECT json_object_agg(k, v) FROM kv`)
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]string
	if err := json.Unmarshal([]byte(r.Rows[0][0].Text), &obj); err != nil {
		t.Fatalf("invalid JSON object: %v (raw: %s)", err, r.Rows[0][0].Text)
	}
	if obj["a"] != "one" || obj["b"] != "two" {
		t.Fatalf("unexpected object: %v", obj)
	}
}

// ---------------------------------------------------------------------------
// Array slicing
// ---------------------------------------------------------------------------

func TestArraySlice(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE arrs (arr TEXT)`)
	mustExec(t, ex, `INSERT INTO arrs VALUES ('{10,20,30,40,50}')`)

	r, err := ex.Exec(`SELECT arr[2:4] FROM arrs`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	got := r.Rows[0][0].Text
	if got != "{20,30,40}" {
		t.Fatalf("expected {20,30,40}, got %s", got)
	}
}

func TestArraySliceConstructor(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT (ARRAY[1,2,3,4,5])[2:3]`)
	if err != nil {
		t.Fatal(err)
	}
	got := r.Rows[0][0].Text
	if got != "{2,3}" {
		t.Fatalf("expected {2,3}, got %s", got)
	}
}

// ---------------------------------------------------------------------------
// LISTEN / NOTIFY
// ---------------------------------------------------------------------------

func TestListenNotify(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`LISTEN my_channel`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "LISTEN" {
		t.Fatalf("expected LISTEN, got %s", r.Message)
	}

	r2, err := ex.Exec(`NOTIFY my_channel, 'hello'`)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Message != "NOTIFY" {
		t.Fatalf("expected NOTIFY, got %s", r2.Message)
	}
}

// ---------------------------------------------------------------------------
// DO blocks
// ---------------------------------------------------------------------------

func TestDoBlock(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE do_test (id INT, val TEXT)`)

	// DO block that inserts rows.
	r, err := ex.Exec(`DO $$ BEGIN
		INSERT INTO do_test VALUES (1, 'from_do');
	END $$`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "DO" {
		t.Fatalf("expected DO, got %s", r.Message)
	}

	// Verify the insert happened.
	r2, err := ex.Exec(`SELECT val FROM do_test WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r2.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r2.Rows))
	}
	if !strings.Contains(r2.Rows[0][0].Text, "from_do") {
		t.Fatalf("expected 'from_do', got %s", r2.Rows[0][0].Text)
	}
}
