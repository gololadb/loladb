package sql

import (
	"testing"
)

// --- EXISTS ---

func TestSubquery_ExistsTrue(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)
	ex.Exec(`INSERT INTO t VALUES (1), (2)`)

	r, err := ex.Exec(`SELECT EXISTS (SELECT 1 FROM t WHERE id = 1)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || !r.Rows[0][0].Bool {
		t.Fatalf("expected true, got %v", r.Rows)
	}
}

func TestSubquery_ExistsFalse(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)
	ex.Exec(`INSERT INTO t VALUES (1)`)

	r, err := ex.Exec(`SELECT EXISTS (SELECT 1 FROM t WHERE id = 99)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Bool {
		t.Fatalf("expected false, got %v", r.Rows)
	}
}

func TestSubquery_ExistsInWhere(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT, name TEXT)`)
	ex.Exec(`CREATE TABLE orders (id INT, user_id INT)`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')`)
	ex.Exec(`INSERT INTO orders VALUES (1, 1), (2, 1), (3, 3)`)

	// Users who have at least one order.
	r, err := ex.Exec(`SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

func TestSubquery_NotExists(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT, name TEXT)`)
	ex.Exec(`CREATE TABLE orders (id INT, user_id INT)`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')`)
	ex.Exec(`INSERT INTO orders VALUES (1, 1), (2, 3)`)

	// Users with no orders.
	r, err := ex.Exec(`SELECT name FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Text != "Bob" {
		t.Fatalf("expected [Bob], got %v", r.Rows)
	}
}

// --- IN (subquery) ---

func TestSubquery_InSubquery(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT, name TEXT)`)
	ex.Exec(`CREATE TABLE vip (user_id INT)`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')`)
	ex.Exec(`INSERT INTO vip VALUES (1), (3)`)

	r, err := ex.Exec(`SELECT name FROM users WHERE id IN (SELECT user_id FROM vip)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

func TestSubquery_InSubqueryEmpty(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)
	ex.Exec(`CREATE TABLE empty (id INT)`)
	ex.Exec(`INSERT INTO t VALUES (1), (2)`)

	r, err := ex.Exec(`SELECT id FROM t WHERE id IN (SELECT id FROM empty)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(r.Rows))
	}
}

// --- NOT IN (subquery) ---

func TestSubquery_NotInSubquery(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT, name TEXT)`)
	ex.Exec(`CREATE TABLE banned (user_id INT)`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')`)
	ex.Exec(`INSERT INTO banned VALUES (2)`)

	r, err := ex.Exec(`SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM banned)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

// --- Scalar subquery ---

func TestSubquery_ScalarInSelect(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)
	ex.Exec(`INSERT INTO t VALUES (1), (2), (3)`)

	r, err := ex.Exec(`SELECT (SELECT count(*) FROM t)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].I64 != 3 {
		t.Fatalf("expected 3, got %v", r.Rows)
	}
}

func TestSubquery_ScalarInWhere(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT, val INT)`)
	ex.Exec(`INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)`)

	// Rows where val > average.
	r, err := ex.Exec(`SELECT id FROM t WHERE val > (SELECT avg(val) FROM t)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].I64 != 3 {
		t.Fatalf("expected [3], got %v", r.Rows)
	}
}

func TestSubquery_ScalarReturnsNull(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)

	r, err := ex.Exec(`SELECT (SELECT id FROM t)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Type != 0 { // TypeNull = 0
		t.Fatalf("expected NULL, got %v", r.Rows)
	}
}

// --- Correlated subquery in SELECT ---

func TestSubquery_CorrelatedInSelect(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT, name TEXT)`)
	ex.Exec(`CREATE TABLE orders (id INT, user_id INT)`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')`)
	ex.Exec(`INSERT INTO orders VALUES (1, 1), (2, 1), (3, 2)`)

	r, err := ex.Exec(`SELECT name, (SELECT count(*) FROM orders WHERE orders.user_id = users.id) FROM users`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

// --- IN with expression ---

func TestSubquery_InWithExpression(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT, category TEXT)`)
	ex.Exec(`CREATE TABLE cats (name TEXT)`)
	ex.Exec(`INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')`)
	ex.Exec(`INSERT INTO cats VALUES ('a'), ('c')`)

	r, err := ex.Exec(`SELECT id FROM t WHERE category IN (SELECT name FROM cats)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

// --- Subquery with JOIN ---

func TestSubquery_ExistsWithJoin(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE a (id INT, val TEXT)`)
	ex.Exec(`CREATE TABLE b (a_id INT, score INT)`)
	ex.Exec(`INSERT INTO a VALUES (1, 'x'), (2, 'y'), (3, 'z')`)
	ex.Exec(`INSERT INTO b VALUES (1, 100), (3, 50)`)

	r, err := ex.Exec(`SELECT val FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.a_id = a.id AND b.score > 60)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Text != "x" {
		t.Fatalf("expected [x], got %v", r.Rows)
	}
}

// --- Nested subqueries ---

func TestSubquery_NestedIn(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t1 (id INT)`)
	ex.Exec(`CREATE TABLE t2 (id INT, t1_id INT)`)
	ex.Exec(`CREATE TABLE t3 (t2_id INT)`)
	ex.Exec(`INSERT INTO t1 VALUES (1), (2), (3)`)
	ex.Exec(`INSERT INTO t2 VALUES (10, 1), (20, 2)`)
	ex.Exec(`INSERT INTO t3 VALUES (10)`)

	// t1 rows where t1.id is in t2 rows that are in t3.
	r, err := ex.Exec(`SELECT id FROM t1 WHERE id IN (SELECT t1_id FROM t2 WHERE id IN (SELECT t2_id FROM t3))`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].I64 != 1 {
		t.Fatalf("expected [1], got %v", r.Rows)
	}
}
