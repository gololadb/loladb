package sql

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// CHECK constraints
// ---------------------------------------------------------------------------

func TestCheck_InsertValid(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE products (id INT, price INT CHECK (price > 0))`)

	_, err := ex.Exec(`INSERT INTO products VALUES (1, 100)`)
	if err != nil {
		t.Fatal(err)
	}

	r, _ := ex.Exec(`SELECT price FROM products WHERE id = 1`)
	if r.Rows[0][0].I64 != 100 {
		t.Fatalf("expected 100, got %d", r.Rows[0][0].I64)
	}
}

func TestCheck_InsertViolation(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE products (id INT, price INT CHECK (price > 0))`)

	_, err := ex.Exec(`INSERT INTO products VALUES (1, -5)`)
	if err == nil {
		t.Fatal("expected CHECK violation error")
	}
	if !strings.Contains(err.Error(), "check constraint") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCheck_InsertZeroBoundary(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE products (id INT, price INT CHECK (price > 0))`)

	_, err := ex.Exec(`INSERT INTO products VALUES (1, 0)`)
	if err == nil {
		t.Fatal("expected CHECK violation for price=0")
	}
}

func TestCheck_UpdateViolation(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE products (id INT, price INT CHECK (price > 0))`)
	ex.Exec(`INSERT INTO products VALUES (1, 100)`)

	_, err := ex.Exec(`UPDATE products SET price = -1 WHERE id = 1`)
	if err == nil {
		t.Fatal("expected CHECK violation on UPDATE")
	}
	if !strings.Contains(err.Error(), "check constraint") {
		t.Fatalf("unexpected error: %v", err)
	}

	// Original value should be unchanged.
	r, _ := ex.Exec(`SELECT price FROM products WHERE id = 1`)
	if r.Rows[0][0].I64 != 100 {
		t.Fatalf("expected 100, got %d", r.Rows[0][0].I64)
	}
}

func TestCheck_NullAllowed(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE products (id INT, price INT CHECK (price > 0))`)

	// NULL should pass CHECK (PostgreSQL behavior: CHECK returns NULL → not false → passes).
	_, err := ex.Exec(`INSERT INTO products VALUES (1, NULL)`)
	if err != nil {
		t.Fatalf("NULL should pass CHECK constraint, got: %v", err)
	}
}

func TestCheck_NamedConstraint(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (val INT CONSTRAINT positive_val CHECK (val > 0))`)

	_, err := ex.Exec(`INSERT INTO t VALUES (-1)`)
	if err == nil {
		t.Fatal("expected CHECK violation")
	}
	if !strings.Contains(err.Error(), "positive_val") {
		t.Fatalf("expected constraint name in error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// FOREIGN KEY — basic referential integrity
// ---------------------------------------------------------------------------

func TestFK_InsertValid(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT PRIMARY KEY, name TEXT)`)
	ex.Exec(`CREATE TABLE orders (id INT, user_id INT REFERENCES users(id))`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice')`)

	_, err := ex.Exec(`INSERT INTO orders VALUES (100, 1)`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestFK_InsertViolation(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT PRIMARY KEY, name TEXT)`)
	ex.Exec(`CREATE TABLE orders (id INT, user_id INT REFERENCES users(id))`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice')`)

	_, err := ex.Exec(`INSERT INTO orders VALUES (100, 999)`)
	if err == nil {
		t.Fatal("expected FK violation")
	}
	if !strings.Contains(err.Error(), "foreign key constraint") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFK_InsertNullAllowed(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT PRIMARY KEY, name TEXT)`)
	ex.Exec(`CREATE TABLE orders (id INT, user_id INT REFERENCES users(id))`)

	// NULL FK value should be allowed.
	_, err := ex.Exec(`INSERT INTO orders VALUES (100, NULL)`)
	if err != nil {
		t.Fatalf("NULL FK should be allowed, got: %v", err)
	}
}

func TestFK_DeleteRestrict(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT PRIMARY KEY, name TEXT)`)
	ex.Exec(`CREATE TABLE orders (id INT, user_id INT REFERENCES users(id))`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice')`)
	ex.Exec(`INSERT INTO orders VALUES (100, 1)`)

	// Default action is NO ACTION/RESTRICT — should fail.
	_, err := ex.Exec(`DELETE FROM users WHERE id = 1`)
	if err == nil {
		t.Fatal("expected FK violation on DELETE")
	}
	if !strings.Contains(err.Error(), "foreign key constraint") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFK_DeleteNoChildren(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT PRIMARY KEY, name TEXT)`)
	ex.Exec(`CREATE TABLE orders (id INT, user_id INT REFERENCES users(id))`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')`)
	ex.Exec(`INSERT INTO orders VALUES (100, 1)`)

	// Deleting user 2 (no referencing orders) should succeed.
	_, err := ex.Exec(`DELETE FROM users WHERE id = 2`)
	if err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// FOREIGN KEY — ON DELETE CASCADE
// ---------------------------------------------------------------------------

func TestFK_DeleteCascade(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT PRIMARY KEY, name TEXT)`)
	ex.Exec(`CREATE TABLE orders (id INT, user_id INT REFERENCES users(id) ON DELETE CASCADE)`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')`)
	ex.Exec(`INSERT INTO orders VALUES (100, 1), (101, 1), (102, 2)`)

	_, err := ex.Exec(`DELETE FROM users WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}

	// Orders for user 1 should be deleted, order for user 2 remains.
	r, _ := ex.Exec(`SELECT id FROM orders ORDER BY id`)
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 order remaining, got %d", len(r.Rows))
	}
	if r.Rows[0][0].I64 != 102 {
		t.Fatalf("expected order 102, got %d", r.Rows[0][0].I64)
	}
}

// ---------------------------------------------------------------------------
// FOREIGN KEY — ON DELETE SET NULL
// ---------------------------------------------------------------------------

func TestFK_DeleteSetNull(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT PRIMARY KEY, name TEXT)`)
	ex.Exec(`CREATE TABLE orders (id INT, user_id INT REFERENCES users(id) ON DELETE SET NULL)`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice')`)
	ex.Exec(`INSERT INTO orders VALUES (100, 1)`)

	_, err := ex.Exec(`DELETE FROM users WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}

	r, _ := ex.Exec(`SELECT user_id FROM orders WHERE id = 100`)
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Type != 0 { // TypeNull = 0
		t.Fatalf("expected NULL, got %v", r.Rows[0][0])
	}
}

// ---------------------------------------------------------------------------
// FOREIGN KEY — ON UPDATE CASCADE
// ---------------------------------------------------------------------------

func TestFK_UpdateCascade(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT PRIMARY KEY, name TEXT)`)
	ex.Exec(`CREATE TABLE orders (id INT, user_id INT REFERENCES users(id) ON UPDATE CASCADE)`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice')`)
	ex.Exec(`INSERT INTO orders VALUES (100, 1)`)

	_, err := ex.Exec(`UPDATE users SET id = 10 WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}

	// Child row should be updated to reference new parent key.
	r, _ := ex.Exec(`SELECT user_id FROM orders WHERE id = 100`)
	if r.Rows[0][0].I64 != 10 {
		t.Fatalf("expected user_id=10, got %d", r.Rows[0][0].I64)
	}
}

// ---------------------------------------------------------------------------
// FOREIGN KEY — UPDATE child FK column
// ---------------------------------------------------------------------------

func TestFK_UpdateChildViolation(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT PRIMARY KEY, name TEXT)`)
	ex.Exec(`CREATE TABLE orders (id INT, user_id INT REFERENCES users(id))`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice')`)
	ex.Exec(`INSERT INTO orders VALUES (100, 1)`)

	_, err := ex.Exec(`UPDATE orders SET user_id = 999 WHERE id = 100`)
	if err == nil {
		t.Fatal("expected FK violation on UPDATE of child")
	}
	if !strings.Contains(err.Error(), "foreign key constraint") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Table-level FOREIGN KEY syntax
// ---------------------------------------------------------------------------

func TestFK_TableLevel(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT PRIMARY KEY, name TEXT)`)
	ex.Exec(`CREATE TABLE orders (id INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice')`)
	ex.Exec(`INSERT INTO orders VALUES (100, 1)`)

	_, err := ex.Exec(`DELETE FROM users WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}

	r, _ := ex.Exec(`SELECT id FROM orders`)
	if len(r.Rows) != 0 {
		t.Fatalf("expected 0 orders after cascade delete, got %d", len(r.Rows))
	}
}
