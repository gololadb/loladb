package sql

import (
	"testing"
)

// ---------------------------------------------------------------------------
// INSERT ... ON CONFLICT DO NOTHING
// ---------------------------------------------------------------------------

func TestUpsert_DoNothing(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE kv (key TEXT PRIMARY KEY, val INT)`)
	ex.Exec(`INSERT INTO kv VALUES ('a', 1)`)

	// Conflicting insert should be silently skipped.
	r, err := ex.Exec(`INSERT INTO kv VALUES ('a', 99) ON CONFLICT (key) DO NOTHING`)
	if err != nil {
		t.Fatal(err)
	}
	if r.RowsAffected != 0 {
		t.Fatalf("expected 0 rows affected, got %d", r.RowsAffected)
	}

	// Original value should be unchanged.
	r, err = ex.Exec(`SELECT val FROM kv WHERE key = 'a'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].I64 != 1 {
		t.Fatalf("expected val=1, got %v", r.Rows)
	}
}

func TestUpsert_DoNothingNoConflict(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE kv (key TEXT PRIMARY KEY, val INT)`)
	ex.Exec(`INSERT INTO kv VALUES ('a', 1)`)

	// Non-conflicting insert should succeed normally.
	r, err := ex.Exec(`INSERT INTO kv VALUES ('b', 2) ON CONFLICT (key) DO NOTHING`)
	if err != nil {
		t.Fatal(err)
	}
	if r.RowsAffected != 1 {
		t.Fatalf("expected 1 row affected, got %d", r.RowsAffected)
	}

	r, err = ex.Exec(`SELECT val FROM kv ORDER BY key`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

// ---------------------------------------------------------------------------
// INSERT ... ON CONFLICT DO UPDATE (basic upsert)
// ---------------------------------------------------------------------------

func TestUpsert_DoUpdate(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE kv (key TEXT PRIMARY KEY, val INT)`)
	ex.Exec(`INSERT INTO kv VALUES ('a', 1)`)

	// Conflicting insert should update the existing row.
	r, err := ex.Exec(`INSERT INTO kv (key, val) VALUES ('a', 42)
		ON CONFLICT (key) DO UPDATE SET val = EXCLUDED.val`)
	if err != nil {
		t.Fatal(err)
	}
	if r.RowsAffected != 1 {
		t.Fatalf("expected 1 row affected, got %d", r.RowsAffected)
	}

	r, err = ex.Exec(`SELECT val FROM kv WHERE key = 'a'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].I64 != 42 {
		t.Fatalf("expected val=42, got %v", r.Rows)
	}
}

func TestUpsert_DoUpdateNoConflict(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE kv (key TEXT PRIMARY KEY, val INT)`)
	ex.Exec(`INSERT INTO kv VALUES ('a', 1)`)

	// Non-conflicting insert should just insert.
	r, err := ex.Exec(`INSERT INTO kv (key, val) VALUES ('b', 2)
		ON CONFLICT (key) DO UPDATE SET val = EXCLUDED.val`)
	if err != nil {
		t.Fatal(err)
	}
	if r.RowsAffected != 1 {
		t.Fatalf("expected 1 row affected, got %d", r.RowsAffected)
	}

	r, err = ex.Exec(`SELECT key, val FROM kv ORDER BY key`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	if r.Rows[1][1].I64 != 2 {
		t.Fatalf("expected val=2 for key 'b', got %v", r.Rows[1][1])
	}
}

func TestUpsert_DoUpdateExpression(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE counters (name TEXT PRIMARY KEY, count INT)`)
	ex.Exec(`INSERT INTO counters VALUES ('hits', 10)`)

	// Increment: SET count = counters.count + 1
	_, err := ex.Exec(`INSERT INTO counters (name, count) VALUES ('hits', 0)
		ON CONFLICT (name) DO UPDATE SET count = counters.count + 1`)
	if err != nil {
		t.Fatal(err)
	}

	r, err := ex.Exec(`SELECT count FROM counters WHERE name = 'hits'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].I64 != 11 {
		t.Fatalf("expected count=11, got %v", r.Rows)
	}
}

func TestUpsert_DoUpdateMultipleValues(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE kv (key TEXT PRIMARY KEY, val INT)`)
	ex.Exec(`INSERT INTO kv VALUES ('a', 1)`)

	// Insert two rows: 'a' conflicts, 'c' is new.
	r, err := ex.Exec(`INSERT INTO kv VALUES ('a', 100), ('c', 3)
		ON CONFLICT (key) DO UPDATE SET val = EXCLUDED.val`)
	if err != nil {
		t.Fatal(err)
	}
	if r.RowsAffected != 2 {
		t.Fatalf("expected 2 rows affected, got %d", r.RowsAffected)
	}

	r, err = ex.Exec(`SELECT key, val FROM kv ORDER BY key`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	// 'a' should be updated to 100
	if r.Rows[0][1].I64 != 100 {
		t.Fatalf("expected val=100 for 'a', got %d", r.Rows[0][1].I64)
	}
	// 'c' should be inserted as 3
	if r.Rows[1][1].I64 != 3 {
		t.Fatalf("expected val=3 for 'c', got %d", r.Rows[1][1].I64)
	}
}

// ---------------------------------------------------------------------------
// UPDATE ... FROM
// ---------------------------------------------------------------------------

func TestUpdateFrom_Basic(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE orders (id INT PRIMARY KEY, status TEXT)`)
	ex.Exec(`CREATE TABLE shipments (order_id INT, shipped_at TEXT)`)
	ex.Exec(`INSERT INTO orders VALUES (1, 'pending'), (2, 'pending'), (3, 'pending')`)
	ex.Exec(`INSERT INTO shipments VALUES (1, '2024-01-01'), (3, '2024-01-02')`)

	r, err := ex.Exec(`UPDATE orders SET status = 'shipped'
		FROM shipments WHERE shipments.order_id = orders.id`)
	if err != nil {
		t.Fatal(err)
	}
	if r.RowsAffected != 2 {
		t.Fatalf("expected 2 rows affected, got %d", r.RowsAffected)
	}

	r, err = ex.Exec(`SELECT id, status FROM orders ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[int64]string{1: "shipped", 2: "pending", 3: "shipped"}
	for _, row := range r.Rows {
		id := row[0].I64
		status := row[1].Text
		if expected[id] != status {
			t.Fatalf("order %d: expected %q, got %q", id, expected[id], status)
		}
	}
}

func TestUpdateFrom_SetFromJoinedColumn(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE products (id INT PRIMARY KEY, price INT)`)
	ex.Exec(`CREATE TABLE price_updates (product_id INT, new_price INT)`)
	ex.Exec(`INSERT INTO products VALUES (1, 100), (2, 200), (3, 300)`)
	ex.Exec(`INSERT INTO price_updates VALUES (1, 150), (3, 350)`)

	// SET price to a value from the joined table.
	_, err := ex.Exec(`UPDATE products SET price = price_updates.new_price
		FROM price_updates WHERE price_updates.product_id = products.id`)
	if err != nil {
		t.Fatal(err)
	}

	r, err := ex.Exec(`SELECT id, price FROM products ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[int64]int64{1: 150, 2: 200, 3: 350}
	for _, row := range r.Rows {
		id := row[0].I64
		price := row[1].I64
		if expected[id] != price {
			t.Fatalf("product %d: expected price %d, got %d", id, expected[id], price)
		}
	}
}

func TestUpdateFrom_NoMatch(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)`)
	ex.Exec(`CREATE TABLE t2 (t1_id INT, info TEXT)`)
	ex.Exec(`INSERT INTO t1 VALUES (1, 'old')`)
	// t2 is empty — no rows should match.

	r, err := ex.Exec(`UPDATE t1 SET val = t2.info FROM t2 WHERE t2.t1_id = t1.id`)
	if err != nil {
		t.Fatal(err)
	}
	if r.RowsAffected != 0 {
		t.Fatalf("expected 0 rows affected, got %d", r.RowsAffected)
	}

	r, err = ex.Exec(`SELECT val FROM t1 WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Text != "old" {
		t.Fatalf("expected 'old', got %q", r.Rows[0][0].Text)
	}
}
