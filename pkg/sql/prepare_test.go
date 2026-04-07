package sql

import (
	"testing"
)

// ---------------------------------------------------------------------------
// PREPARE + EXECUTE — basic SELECT with parameter
// ---------------------------------------------------------------------------

func TestPrepareExecute_BasicSelect(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE users (id INT, name TEXT)`)
	mustExec(t, ex, `INSERT INTO users VALUES (1, 'Alice')`)
	mustExec(t, ex, `INSERT INTO users VALUES (2, 'Bob')`)
	mustExec(t, ex, `INSERT INTO users VALUES (3, 'Charlie')`)

	r, err := ex.Exec(`PREPARE get_user(int) AS SELECT name FROM users WHERE id = $1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "PREPARE" {
		t.Fatalf("expected 'PREPARE', got %q", r.Message)
	}

	r, err = ex.Exec(`EXECUTE get_user(2)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "Bob" {
		t.Fatalf("expected 'Bob', got %q", r.Rows[0][0].Text)
	}
}

// ---------------------------------------------------------------------------
// PREPARE + EXECUTE — multiple parameters
// ---------------------------------------------------------------------------

func TestPrepareExecute_MultipleParams(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE products (id INT, name TEXT, price INT)`)
	mustExec(t, ex, `INSERT INTO products VALUES (1, 'Widget', 100)`)
	mustExec(t, ex, `INSERT INTO products VALUES (2, 'Gadget', 200)`)
	mustExec(t, ex, `INSERT INTO products VALUES (3, 'Doohickey', 300)`)

	mustExec(t, ex, `PREPARE find_products(int, int) AS SELECT name FROM products WHERE price >= $1 AND price <= $2`)

	r, err := ex.Exec(`EXECUTE find_products(150, 250)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "Gadget" {
		t.Fatalf("expected 'Gadget', got %q", r.Rows[0][0].Text)
	}
}

// ---------------------------------------------------------------------------
// PREPARE + EXECUTE — INSERT with parameters
// ---------------------------------------------------------------------------

func TestPrepareExecute_Insert(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE items (id INT, name TEXT)`)

	mustExec(t, ex, `PREPARE add_item(int, text) AS INSERT INTO items VALUES ($1, $2)`)

	mustExec(t, ex, `EXECUTE add_item(1, 'Alpha')`)
	mustExec(t, ex, `EXECUTE add_item(2, 'Beta')`)

	r, err := ex.Exec(`SELECT name FROM items ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "Alpha" {
		t.Fatalf("expected 'Alpha', got %q", r.Rows[0][0].Text)
	}
	if r.Rows[1][0].Text != "Beta" {
		t.Fatalf("expected 'Beta', got %q", r.Rows[1][0].Text)
	}
}

// ---------------------------------------------------------------------------
// PREPARE + EXECUTE — UPDATE with parameters
// ---------------------------------------------------------------------------

func TestPrepareExecute_Update(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE kv (k INT, v TEXT)`)
	mustExec(t, ex, `INSERT INTO kv VALUES (1, 'old')`)

	mustExec(t, ex, `PREPARE update_kv(text, int) AS UPDATE kv SET v = $1 WHERE k = $2`)
	mustExec(t, ex, `EXECUTE update_kv('new', 1)`)

	r, err := ex.Exec(`SELECT v FROM kv WHERE k = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Text != "new" {
		t.Fatalf("expected 'new', got %q", r.Rows[0][0].Text)
	}
}

// ---------------------------------------------------------------------------
// PREPARE + EXECUTE — DELETE with parameters
// ---------------------------------------------------------------------------

func TestPrepareExecute_Delete(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE nums (n INT)`)
	mustExec(t, ex, `INSERT INTO nums VALUES (1)`)
	mustExec(t, ex, `INSERT INTO nums VALUES (2)`)
	mustExec(t, ex, `INSERT INTO nums VALUES (3)`)

	mustExec(t, ex, `PREPARE del_num(int) AS DELETE FROM nums WHERE n = $1`)
	mustExec(t, ex, `EXECUTE del_num(2)`)

	r, err := ex.Exec(`SELECT count(*) FROM nums`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].I64 != 2 {
		t.Fatalf("expected 2 rows, got %d", r.Rows[0][0].I64)
	}
}

// ---------------------------------------------------------------------------
// PREPARE without type list
// ---------------------------------------------------------------------------

func TestPrepareExecute_NoTypeList(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE t1 (id INT, val TEXT)`)
	mustExec(t, ex, `INSERT INTO t1 VALUES (1, 'hello')`)

	mustExec(t, ex, `PREPARE q1 AS SELECT val FROM t1 WHERE id = $1`)

	r, err := ex.Exec(`EXECUTE q1(1)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Text != "hello" {
		t.Fatalf("unexpected result: %v", r.Rows)
	}
}

// ---------------------------------------------------------------------------
// EXECUTE — nonexistent prepared statement
// ---------------------------------------------------------------------------

func TestExecute_Nonexistent(t *testing.T) {
	ex := newTestExecutor(t)
	_, err := ex.Exec(`EXECUTE no_such_stmt(1)`)
	if err == nil {
		t.Fatal("expected error for nonexistent prepared statement")
	}
}

// ---------------------------------------------------------------------------
// EXECUTE — reuse prepared statement multiple times
// ---------------------------------------------------------------------------

func TestPrepareExecute_Reuse(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE t2 (id INT, name TEXT)`)
	mustExec(t, ex, `INSERT INTO t2 VALUES (1, 'A')`)
	mustExec(t, ex, `INSERT INTO t2 VALUES (2, 'B')`)
	mustExec(t, ex, `INSERT INTO t2 VALUES (3, 'C')`)

	mustExec(t, ex, `PREPARE lookup(int) AS SELECT name FROM t2 WHERE id = $1`)

	for _, tc := range []struct {
		id   string
		want string
	}{
		{"1", "A"},
		{"2", "B"},
		{"3", "C"},
	} {
		r, err := ex.Exec(`EXECUTE lookup(` + tc.id + `)`)
		if err != nil {
			t.Fatalf("EXECUTE lookup(%s): %v", tc.id, err)
		}
		if len(r.Rows) != 1 || r.Rows[0][0].Text != tc.want {
			t.Fatalf("EXECUTE lookup(%s): expected %q, got %v", tc.id, tc.want, r.Rows)
		}
	}
}

// ---------------------------------------------------------------------------
// DEALLOCATE — specific statement
// ---------------------------------------------------------------------------

func TestDeallocate(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE t3 (id INT)`)
	mustExec(t, ex, `INSERT INTO t3 VALUES (1)`)

	mustExec(t, ex, `PREPARE q3 AS SELECT * FROM t3`)

	r, err := ex.Exec(`DEALLOCATE q3`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "DEALLOCATE" {
		t.Fatalf("expected 'DEALLOCATE', got %q", r.Message)
	}

	// Should fail now.
	_, err = ex.Exec(`EXECUTE q3`)
	if err == nil {
		t.Fatal("expected error after DEALLOCATE")
	}
}

// ---------------------------------------------------------------------------
// DEALLOCATE PREPARE — alternate syntax
// ---------------------------------------------------------------------------

func TestDeallocatePrepare(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE t4 (id INT)`)
	mustExec(t, ex, `PREPARE q4 AS SELECT * FROM t4`)

	r, err := ex.Exec(`DEALLOCATE PREPARE q4`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "DEALLOCATE" {
		t.Fatalf("expected 'DEALLOCATE', got %q", r.Message)
	}
}

// ---------------------------------------------------------------------------
// DEALLOCATE ALL
// ---------------------------------------------------------------------------

func TestDeallocateAll(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE t5 (id INT)`)
	mustExec(t, ex, `PREPARE qa AS SELECT * FROM t5`)
	mustExec(t, ex, `PREPARE qb AS SELECT * FROM t5`)

	r, err := ex.Exec(`DEALLOCATE ALL`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "DEALLOCATE ALL" {
		t.Fatalf("expected 'DEALLOCATE ALL', got %q", r.Message)
	}

	// Both should fail.
	_, err = ex.Exec(`EXECUTE qa`)
	if err == nil {
		t.Fatal("expected error after DEALLOCATE ALL")
	}
	_, err = ex.Exec(`EXECUTE qb`)
	if err == nil {
		t.Fatal("expected error after DEALLOCATE ALL")
	}
}

// ---------------------------------------------------------------------------
// DEALLOCATE — nonexistent statement
// ---------------------------------------------------------------------------

func TestDeallocate_Nonexistent(t *testing.T) {
	ex := newTestExecutor(t)
	_, err := ex.Exec(`DEALLOCATE no_such`)
	if err == nil {
		t.Fatal("expected error for nonexistent prepared statement")
	}
}

// ---------------------------------------------------------------------------
// PREPARE with string parameter containing $
// ---------------------------------------------------------------------------

func TestPrepareExecute_StringParam(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE t6 (id INT, name TEXT)`)

	mustExec(t, ex, `PREPARE ins6(int, text) AS INSERT INTO t6 VALUES ($1, $2)`)
	mustExec(t, ex, `EXECUTE ins6(1, 'hello world')`)

	r, err := ex.Exec(`SELECT name FROM t6 WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Text != "hello world" {
		t.Fatalf("expected 'hello world', got %q", r.Rows[0][0].Text)
	}
}

// ---------------------------------------------------------------------------
// PREPARE — overwrite existing prepared statement
// ---------------------------------------------------------------------------

func TestPrepare_Overwrite(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE t7 (id INT, a TEXT, b TEXT)`)
	mustExec(t, ex, `INSERT INTO t7 VALUES (1, 'x', 'y')`)

	mustExec(t, ex, `PREPARE q7 AS SELECT a FROM t7 WHERE id = $1`)

	r, err := ex.Exec(`EXECUTE q7(1)`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Text != "x" {
		t.Fatalf("expected 'x', got %q", r.Rows[0][0].Text)
	}

	// Overwrite with different query.
	mustExec(t, ex, `PREPARE q7 AS SELECT b FROM t7 WHERE id = $1`)

	r, err = ex.Exec(`EXECUTE q7(1)`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Text != "y" {
		t.Fatalf("expected 'y' after overwrite, got %q", r.Rows[0][0].Text)
	}
}

// ---------------------------------------------------------------------------
// EXECUTE without parameters (no-param prepared statement)
// ---------------------------------------------------------------------------

func TestPrepareExecute_NoParams(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE t8 (id INT)`)
	mustExec(t, ex, `INSERT INTO t8 VALUES (42)`)

	mustExec(t, ex, `PREPARE q8 AS SELECT * FROM t8`)

	r, err := ex.Exec(`EXECUTE q8`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	// The value 42 may be stored as Int32 or Int64 depending on the pipeline.
	d := r.Rows[0][0]
	var got int64
	switch d.Type {
	case 1: // TypeInt32
		got = int64(d.I32)
	case 2: // TypeInt64
		got = d.I64
	default:
		t.Fatalf("unexpected type %d for id column", d.Type)
	}
	if got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
}
