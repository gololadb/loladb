package sql

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/gololadb/loladb/pkg/tuple"
)

// datumInt extracts an integer value from a Datum regardless of storage type.
func datumInt(d tuple.Datum) int64 {
	switch d.Type {
	case tuple.TypeInt32:
		return int64(d.I32)
	case tuple.TypeInt64:
		return d.I64
	case tuple.TypeFloat64:
		return int64(d.F64)
	default:
		return 0
	}
}

// ---------------------------------------------------------------------------
// LATERAL joins
// ---------------------------------------------------------------------------

func TestLateralJoin(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE users_lat (id INT, name TEXT)`)
	mustExec(t, ex, `CREATE TABLE orders_lat (id INT, user_id INT, amount INT)`)
	mustExec(t, ex, `INSERT INTO users_lat VALUES (1, 'Alice')`)
	mustExec(t, ex, `INSERT INTO users_lat VALUES (2, 'Bob')`)
	mustExec(t, ex, `INSERT INTO orders_lat VALUES (1, 1, 100)`)
	mustExec(t, ex, `INSERT INTO orders_lat VALUES (2, 1, 200)`)
	mustExec(t, ex, `INSERT INTO orders_lat VALUES (3, 2, 50)`)

	r, err := ex.Exec(`SELECT u.name, o.amount FROM users_lat u, LATERAL (SELECT amount FROM orders_lat WHERE user_id = u.id) o ORDER BY u.name, o.amount`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
	// Alice: 100, 200; Bob: 50
	if r.Rows[0][0].Text != "Alice" || datumInt(r.Rows[0][1]) != 100 {
		t.Errorf("row 0: expected Alice/100, got %s/%d", r.Rows[0][0].Text, datumInt(r.Rows[0][1]))
	}
	if r.Rows[1][0].Text != "Alice" || datumInt(r.Rows[1][1]) != 200 {
		t.Errorf("row 1: expected Alice/200, got %s/%d", r.Rows[1][0].Text, datumInt(r.Rows[1][1]))
	}
	if r.Rows[2][0].Text != "Bob" || datumInt(r.Rows[2][1]) != 50 {
		t.Errorf("row 2: expected Bob/50, got %s/%d", r.Rows[2][0].Text, datumInt(r.Rows[2][1]))
	}
}

func TestLateralJoinWithLimit(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE ul2 (id INT, name TEXT)`)
	mustExec(t, ex, `CREATE TABLE ol2 (id INT, uid INT, amt INT)`)
	mustExec(t, ex, `INSERT INTO ul2 VALUES (1, 'Alice')`)
	mustExec(t, ex, `INSERT INTO ul2 VALUES (2, 'Bob')`)
	mustExec(t, ex, `INSERT INTO ol2 VALUES (1, 1, 100)`)
	mustExec(t, ex, `INSERT INTO ol2 VALUES (2, 1, 200)`)
	mustExec(t, ex, `INSERT INTO ol2 VALUES (3, 2, 50)`)

	// Top-1 order per user.
	r, err := ex.Exec(`SELECT ul2.name, x.amt FROM ul2, LATERAL (SELECT amt FROM ol2 WHERE uid = ul2.id ORDER BY amt DESC LIMIT 1) x ORDER BY ul2.name`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	if datumInt(r.Rows[0][1]) != 200 {
		t.Errorf("Alice top order: expected 200, got %d", datumInt(r.Rows[0][1]))
	}
	if datumInt(r.Rows[1][1]) != 50 {
		t.Errorf("Bob top order: expected 50, got %d", datumInt(r.Rows[1][1]))
	}
}

// ---------------------------------------------------------------------------
// PL/pgSQL EXCEPTION handling
// ---------------------------------------------------------------------------

func TestPlpgsqlExceptionCatchAll(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE exc_test (id INT UNIQUE)`)
	mustExec(t, ex, `INSERT INTO exc_test VALUES (1)`)

	// Function that catches any exception.
	mustExec(t, ex, `CREATE FUNCTION safe_insert(v INT) RETURNS TEXT AS $$
BEGIN
	INSERT INTO exc_test VALUES (v);
	RETURN 'ok';
EXCEPTION WHEN OTHERS THEN
	RETURN 'caught: ' || SQLERRM;
END;
$$ LANGUAGE plpgsql`)

	// Should succeed.
	r, err := ex.Exec(`SELECT safe_insert(2)`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Text != "ok" {
		t.Errorf("expected 'ok', got %q", r.Rows[0][0].Text)
	}

	// Should catch the unique violation.
	r, err = ex.Exec(`SELECT safe_insert(1)`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(r.Rows[0][0].Text, "caught:") {
		t.Errorf("expected 'caught:...', got %q", r.Rows[0][0].Text)
	}
}

func TestPlpgsqlExceptionRaise(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE FUNCTION raise_and_catch() RETURNS TEXT AS $$
BEGIN
	RAISE EXCEPTION 'boom';
EXCEPTION WHEN OTHERS THEN
	RETURN 'caught: ' || SQLERRM;
END;
$$ LANGUAGE plpgsql`)

	r, err := ex.Exec(`SELECT raise_and_catch()`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.Rows[0][0].Text, "boom") {
		t.Errorf("expected 'caught: boom', got %q", r.Rows[0][0].Text)
	}
}

// ---------------------------------------------------------------------------
// PL/pgSQL FOREACH ... IN ARRAY
// ---------------------------------------------------------------------------

func TestPlpgsqlForEachArray(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE fe_result (val INT)`)
	mustExec(t, ex, `CREATE FUNCTION foreach_test() RETURNS VOID AS $$
DECLARE
	arr INT[] := ARRAY[10, 20, 30];
	elem INT;
BEGIN
	FOREACH elem IN ARRAY arr LOOP
		INSERT INTO fe_result VALUES (elem);
	END LOOP;
END;
$$ LANGUAGE plpgsql`)

	mustExec(t, ex, `SELECT foreach_test()`)

	r, err := ex.Exec(`SELECT val FROM fe_result ORDER BY val`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
	expected := []int64{10, 20, 30}
	for i, exp := range expected {
		if datumInt(r.Rows[i][0]) != exp {
			t.Errorf("row %d: expected %d, got %d", i, exp, datumInt(r.Rows[i][0]))
		}
	}
}

// ---------------------------------------------------------------------------
// Two-variable statistics
// ---------------------------------------------------------------------------

func TestCorrelation(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE corr_test (x INT, y INT)`)
	mustExec(t, ex, `INSERT INTO corr_test VALUES (1, 2)`)
	mustExec(t, ex, `INSERT INTO corr_test VALUES (2, 4)`)
	mustExec(t, ex, `INSERT INTO corr_test VALUES (3, 6)`)
	mustExec(t, ex, `INSERT INTO corr_test VALUES (4, 8)`)

	r, err := ex.Exec(`SELECT corr(y, x) FROM corr_test`)
	if err != nil {
		t.Fatal(err)
	}
	// Perfect positive correlation.
	got := r.Rows[0][0].F64
	if math.Abs(got-1.0) > 0.001 {
		t.Errorf("expected corr ≈ 1.0, got %f", got)
	}
}

func TestCovariance(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE cov_test (x INT, y INT)`)
	mustExec(t, ex, `INSERT INTO cov_test VALUES (1, 2)`)
	mustExec(t, ex, `INSERT INTO cov_test VALUES (2, 4)`)
	mustExec(t, ex, `INSERT INTO cov_test VALUES (3, 6)`)

	r, err := ex.Exec(`SELECT covar_pop(y, x), covar_samp(y, x) FROM cov_test`)
	if err != nil {
		t.Fatal(err)
	}
	// covar_pop = sum((x-xbar)(y-ybar))/n
	// x: mean=2, y: mean=4
	// (1-2)(2-4) + (2-2)(4-4) + (3-2)(6-4) = 2+0+2 = 4; /3 = 1.333
	covPop := r.Rows[0][0].F64
	if math.Abs(covPop-4.0/3.0) > 0.01 {
		t.Errorf("expected covar_pop ≈ 1.333, got %f", covPop)
	}
	// covar_samp = sum/2 = 2.0
	covSamp := r.Rows[0][1].F64
	if math.Abs(covSamp-2.0) > 0.01 {
		t.Errorf("expected covar_samp ≈ 2.0, got %f", covSamp)
	}
}

func TestRegrSlope(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE regr_test (x INT, y INT)`)
	mustExec(t, ex, `INSERT INTO regr_test VALUES (1, 3)`)
	mustExec(t, ex, `INSERT INTO regr_test VALUES (2, 5)`)
	mustExec(t, ex, `INSERT INTO regr_test VALUES (3, 7)`)

	r, err := ex.Exec(`SELECT regr_slope(y, x), regr_intercept(y, x), regr_count(y, x) FROM regr_test`)
	if err != nil {
		t.Fatal(err)
	}
	// y = 2x + 1, so slope=2, intercept=1
	slope := r.Rows[0][0].F64
	if math.Abs(slope-2.0) > 0.01 {
		t.Errorf("expected slope ≈ 2.0, got %f", slope)
	}
	intercept := r.Rows[0][1].F64
	if math.Abs(intercept-1.0) > 0.01 {
		t.Errorf("expected intercept ≈ 1.0, got %f", intercept)
	}
	count := r.Rows[0][2].I64
	if count != 3 {
		t.Errorf("expected regr_count=3, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// FETCH FIRST / OFFSET ... ROWS
// ---------------------------------------------------------------------------

func TestFetchFirstRows(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE ff_test (id INT)`)
	for i := 1; i <= 10; i++ {
		mustExec(t, ex, `INSERT INTO ff_test VALUES (`+fmt.Sprintf("%d", i)+`)`)
	}

	r, err := ex.Exec(`SELECT id FROM ff_test ORDER BY id FETCH FIRST 3 ROWS ONLY`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
	if datumInt(r.Rows[0][0]) != 1 || datumInt(r.Rows[2][0]) != 3 {
		t.Errorf("unexpected rows: first=%d last=%d", datumInt(r.Rows[0][0]), datumInt(r.Rows[2][0]))
	}
}

func TestOffsetRows(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE or_test (id INT)`)
	for i := 1; i <= 5; i++ {
		mustExec(t, ex, `INSERT INTO or_test VALUES (`+fmt.Sprintf("%d", i)+`)`)
	}

	r, err := ex.Exec(`SELECT id FROM or_test ORDER BY id OFFSET 3 ROWS FETCH FIRST 2 ROWS ONLY`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	if datumInt(r.Rows[0][0]) != 4 || datumInt(r.Rows[1][0]) != 5 {
		t.Errorf("expected 4,5 got %d,%d", datumInt(r.Rows[0][0]), datumInt(r.Rows[1][0]))
	}
}

// ---------------------------------------------------------------------------
// Array concatenation (||)
// ---------------------------------------------------------------------------

func TestArrayConcat(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`SELECT ARRAY[1,2,3] || ARRAY[4,5]`)
	if err != nil {
		t.Fatal(err)
	}
	got := r.Rows[0][0].Text
	if got != "{1,2,3,4,5}" {
		t.Errorf("expected {1,2,3,4,5}, got %q", got)
	}
}

func TestArrayAppendElement(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`SELECT ARRAY[1,2] || 3`)
	if err != nil {
		t.Fatal(err)
	}
	got := r.Rows[0][0].Text
	if got != "{1,2,3}" {
		t.Errorf("expected {1,2,3}, got %q", got)
	}
}


