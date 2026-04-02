package sql

import (
	"fmt"
	"strings"
	"testing"
)

func TestSQL_InnerJoin(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT, name TEXT)`)
	ex.Exec(`CREATE TABLE orders (id INT, user_id INT, total INT)`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`)
	ex.Exec(`INSERT INTO orders VALUES (10, 1, 100), (11, 2, 200), (12, 1, 150)`)

	r, err := ex.Exec(`SELECT u.name, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}

	// Alice should appear twice (orders 10 and 12).
	aliceCount := 0
	for _, row := range r.Rows {
		if row[0].Text == "Alice" {
			aliceCount++
		}
	}
	if aliceCount != 2 {
		t.Fatalf("expected Alice 2 times, got %d", aliceCount)
	}
}

func TestSQL_LeftJoin(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT, name TEXT)`)
	ex.Exec(`CREATE TABLE orders (id INT, user_id INT)`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`)
	ex.Exec(`INSERT INTO orders VALUES (10, 1), (11, 2)`)

	r, err := ex.Exec(`SELECT u.name, o.id FROM users u LEFT JOIN orders o ON u.id = o.user_id`)
	if err != nil {
		t.Fatal(err)
	}
	// All 3 users, Charlie has NULL order.
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}

	charlieFound := false
	for _, row := range r.Rows {
		if row[0].Text == "Charlie" {
			charlieFound = true
			if row[1].Type != 0 { // TypeNull = 0
				t.Fatal("Charlie's order should be NULL")
			}
		}
	}
	if !charlieFound {
		t.Fatal("Charlie not found in LEFT JOIN result")
	}
}

func TestSQL_CrossJoin(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE colors (name TEXT)`)
	ex.Exec(`CREATE TABLE sizes (name TEXT)`)
	ex.Exec(`INSERT INTO colors VALUES ('red'), ('blue')`)
	ex.Exec(`INSERT INTO sizes VALUES ('S'), ('M'), ('L')`)

	r, err := ex.Exec(`SELECT c.name, s.name FROM colors c CROSS JOIN sizes s`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 6 {
		t.Fatalf("expected 6 rows (2×3), got %d", len(r.Rows))
	}
}

func TestSQL_ImplicitCrossJoin(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE a (x INT)`)
	ex.Exec(`CREATE TABLE b (y INT)`)
	ex.Exec(`INSERT INTO a VALUES (1), (2)`)
	ex.Exec(`INSERT INTO b VALUES (10), (20)`)

	r, err := ex.Exec(`SELECT a.x, b.y FROM a, b WHERE a.x = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

func TestSQL_MultiWayJoin(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT, name TEXT)`)
	ex.Exec(`CREATE TABLE orders (id INT, user_id INT, product_id INT)`)
	ex.Exec(`CREATE TABLE products (id INT, pname TEXT)`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice')`)
	ex.Exec(`INSERT INTO products VALUES (100, 'Widget')`)
	ex.Exec(`INSERT INTO orders VALUES (10, 1, 100)`)

	r, err := ex.Exec(`SELECT u.name, p.pname FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "Alice" || r.Rows[0][1].Text != "Widget" {
		t.Fatalf("unexpected: %v", r.Rows[0])
	}
}

func TestSQL_Explain(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT, name TEXT)`)

	r, err := ex.Exec(`EXPLAIN SELECT * FROM users WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) == 0 {
		t.Fatal("EXPLAIN should produce output")
	}
	// The plan text should mention SeqScan (no index exists).
	planText := r.Rows[0][0].Text
	if !strings.Contains(planText, "SeqScan") {
		t.Fatalf("expected SeqScan in plan, got: %s", planText)
	}
}

func TestSQL_ExplainWithIndex(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT, name TEXT)`)
	// Insert enough rows across multiple pages so the optimizer prefers
	// an index scan over a sequential scan for a point lookup.
	// With few pages a seq scan is cheaper (matching PostgreSQL behavior).
	for i := 0; i < 1000; i++ {
		ex.Exec(fmt.Sprintf(`INSERT INTO users VALUES (%d, 'user_%d_padding_to_make_rows_wider_and_span_more_pages')`, i, i))
	}
	ex.Exec(`CREATE INDEX idx_users_id ON users (id)`)

	r, err := ex.Exec(`EXPLAIN SELECT * FROM users WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	planText := ""
	for _, row := range r.Rows {
		planText += row[0].Text + "\n"
	}
	if !strings.Contains(planText, "IndexScan") && !strings.Contains(planText, "Bitmap") {
		t.Fatalf("expected IndexScan or BitmapScan in plan when index exists, got:\n%s", planText)
	}
}

func TestSQL_ExplainPlan(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)

	plan, err := ex.ExplainPlan(`SELECT * FROM t WHERE id = 5`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "SeqScan") {
		t.Fatalf("expected SeqScan in plan: %s", plan)
	}
}

func TestSQL_JoinWithWhere(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT, name TEXT, active INT)`)
	ex.Exec(`CREATE TABLE orders (id INT, user_id INT)`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice', 1), (2, 'Bob', 0), (3, 'Charlie', 1)`)
	ex.Exec(`INSERT INTO orders VALUES (10, 1), (11, 3)`)

	r, err := ex.Exec(`SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

func TestSQL_SelectStar_Join(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE a (x INT)`)
	ex.Exec(`CREATE TABLE b (y INT)`)
	ex.Exec(`INSERT INTO a VALUES (1)`)
	ex.Exec(`INSERT INTO b VALUES (2)`)

	r, err := ex.Exec(`SELECT * FROM a CROSS JOIN b`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if len(r.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(r.Columns))
	}
}

func TestSQL_Limit(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)
	for i := 0; i < 20; i++ {
		ex.Exec(fmt.Sprintf(`INSERT INTO t VALUES (%d)`, i))
	}

	r, err := ex.Exec(`SELECT * FROM t LIMIT 5`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(r.Rows))
	}
}
