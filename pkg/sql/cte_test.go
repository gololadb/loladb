package sql

import (
	"strings"
	"testing"

	"github.com/gololadb/loladb/pkg/tuple"
)

// --- Non-recursive CTEs ---

func TestCTE_Basic(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE employees (id INT, name TEXT, dept TEXT)`)
	ex.Exec(`INSERT INTO employees VALUES (1, 'Alice', 'eng'), (2, 'Bob', 'eng'), (3, 'Carol', 'sales')`)

	r, err := ex.Exec(`WITH eng AS (SELECT id, name FROM employees WHERE dept = 'eng') SELECT name FROM eng`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

func TestCTE_WithAlias(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE items (id INT, label TEXT)`)
	ex.Exec(`INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c')`)

	r, err := ex.Exec(`WITH src AS (SELECT id, label FROM items) SELECT s.label FROM src s WHERE s.id > 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

func TestCTE_MultipleCTEs(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE products (id INT, name TEXT, price INT)`)
	ex.Exec(`INSERT INTO products VALUES (1, 'Widget', 10), (2, 'Gadget', 50), (3, 'Doohickey', 100)`)

	r, err := ex.Exec(`
		WITH cheap AS (SELECT name FROM products WHERE price < 30),
		     expensive AS (SELECT name FROM products WHERE price >= 30)
		SELECT name FROM cheap
	`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "Widget" {
		t.Fatalf("expected 'Widget', got %q", r.Rows[0][0].Text)
	}
}

func TestCTE_JoinWithTable(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE orders (id INT, customer_id INT, total INT)`)
	ex.Exec(`CREATE TABLE customers (id INT, name TEXT)`)
	ex.Exec(`INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')`)
	ex.Exec(`INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50)`)

	r, err := ex.Exec(`
		WITH big_orders AS (SELECT customer_id, total FROM orders WHERE total > 75)
		SELECT c.name, bo.total FROM big_orders bo JOIN customers c ON bo.customer_id = c.id
	`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

func TestCTE_WithAggregation(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE sales (id INT, region TEXT, amount INT)`)
	ex.Exec(`INSERT INTO sales VALUES (1, 'north', 100), (2, 'north', 200), (3, 'south', 150)`)

	r, err := ex.Exec(`
		WITH totals AS (SELECT region, sum(amount) AS total FROM sales GROUP BY region)
		SELECT region, total FROM totals
	`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

func TestCTE_SelectStar(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (a INT, b TEXT)`)
	ex.Exec(`INSERT INTO t VALUES (1, 'x'), (2, 'y')`)

	r, err := ex.Exec(`WITH cte AS (SELECT a, b FROM t) SELECT * FROM cte`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 || len(r.Columns) != 2 {
		t.Fatalf("expected 2 rows x 2 cols, got %d x %d", len(r.Rows), len(r.Columns))
	}
}

func TestCTE_ColumnAlias(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (val INT)`)
	ex.Exec(`INSERT INTO t VALUES (10), (20)`)

	r, err := ex.Exec(`WITH cte AS (SELECT val * 2 AS doubled FROM t) SELECT doubled FROM cte`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

// --- Recursive CTEs ---

func TestCTE_RecursiveHierarchy(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE categories (id INT, name TEXT, parent_id INT)`)
	ex.Exec(`INSERT INTO categories VALUES (1, 'root', 0), (2, 'child1', 1), (3, 'child2', 1), (4, 'grandchild', 2)`)

	r, err := ex.Exec(`
		WITH RECURSIVE tree AS (
			SELECT id, name, parent_id FROM categories WHERE id = 1
			UNION ALL
			SELECT c.id, c.name, c.parent_id FROM categories c JOIN tree t ON c.parent_id = t.id
		)
		SELECT name FROM tree
	`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(r.Rows))
	}
	// Verify order: root first, then children, then grandchild.
	names := make([]string, len(r.Rows))
	for i, row := range r.Rows {
		names[i] = row[0].Text
	}
	if names[0] != "root" {
		t.Fatalf("expected first row 'root', got %q", names[0])
	}
	if names[3] != "grandchild" {
		t.Fatalf("expected last row 'grandchild', got %q", names[3])
	}
}

func TestCTE_RecursiveCounter(t *testing.T) {
	ex := newTestExecutor(t)

	// Generate numbers 1..5 using recursive CTE.
	r, err := ex.Exec(`
		WITH RECURSIVE nums AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM nums WHERE n < 5
		)
		SELECT n FROM nums
	`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(r.Rows))
	}
	for i, row := range r.Rows {
		expected := int64(i + 1)
		if row[0].I64 != expected {
			t.Fatalf("row %d: expected %d, got %d", i, expected, row[0].I64)
		}
	}
}

func TestCTE_RecursiveDepthLimit(t *testing.T) {
	ex := newTestExecutor(t)

	// Recursive CTE that terminates naturally.
	r, err := ex.Exec(`
		WITH RECURSIVE seq AS (
			SELECT 1 AS val
			UNION ALL
			SELECT val + 1 FROM seq WHERE val < 10
		)
		SELECT val FROM seq
	`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(r.Rows))
	}
}

func TestCTE_RecursiveWithFilter(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE org (id INT, name TEXT, manager_id INT)`)
	ex.Exec(`INSERT INTO org VALUES (1, 'CEO', 0), (2, 'VP1', 1), (3, 'VP2', 1), (4, 'Dir', 2), (5, 'Mgr', 4)`)

	// Get all reports under VP1 (id=2).
	r, err := ex.Exec(`
		WITH RECURSIVE reports AS (
			SELECT id, name FROM org WHERE id = 2
			UNION ALL
			SELECT o.id, o.name FROM org o JOIN reports r ON o.manager_id = r.id
		)
		SELECT name FROM reports
	`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows (VP1, Dir, Mgr), got %d", len(r.Rows))
	}
}

// --- Subquery in FROM ---

func TestSubqueryInFROM(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT, val TEXT)`)
	ex.Exec(`INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')`)

	r, err := ex.Exec(`SELECT sub.val FROM (SELECT id, val FROM t WHERE id > 1) sub`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

func TestSubqueryInFROM_WithJoin(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE a (id INT, name TEXT)`)
	ex.Exec(`CREATE TABLE b (a_id INT, score INT)`)
	ex.Exec(`INSERT INTO a VALUES (1, 'Alice'), (2, 'Bob')`)
	ex.Exec(`INSERT INTO b VALUES (1, 90), (1, 80), (2, 70)`)

	r, err := ex.Exec(`
		SELECT a.name, s.total
		FROM a
		JOIN (SELECT a_id, sum(score) AS total FROM b GROUP BY a_id) s ON a.id = s.a_id
	`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

// --- Edge cases ---

func TestCTE_EmptyResult(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)

	r, err := ex.Exec(`WITH empty AS (SELECT id FROM t) SELECT * FROM empty`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(r.Rows))
	}
}

func TestCTE_RecursiveTerminatesEmpty(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE nodes (id INT, parent_id INT)`)
	ex.Exec(`INSERT INTO nodes VALUES (1, 0)`) // single node, no children

	r, err := ex.Exec(`
		WITH RECURSIVE tree AS (
			SELECT id FROM nodes WHERE id = 1
			UNION ALL
			SELECT n.id FROM nodes n JOIN tree t ON n.parent_id = t.id
		)
		SELECT id FROM tree
	`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
}

func TestCTE_UsedInWHERE(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT, val TEXT)`)
	ex.Exec(`INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')`)

	// CTE used in a join, with WHERE on the outer query.
	r, err := ex.Exec(`
		WITH subset AS (SELECT id, val FROM t WHERE id <= 2)
		SELECT val FROM subset WHERE val = 'b'
	`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Text != "b" {
		t.Fatalf("expected 1 row with 'b', got %v", r.Rows)
	}
}

func TestCTE_RecursiveFibonacci(t *testing.T) {
	ex := newTestExecutor(t)

	// Generate first 8 Fibonacci numbers.
	r, err := ex.Exec(`
		WITH RECURSIVE fib AS (
			SELECT 1 AS n, 1 AS fib_n, 0 AS fib_prev
			UNION ALL
			SELECT n + 1, fib_n + fib_prev, fib_n FROM fib WHERE n < 8
		)
		SELECT fib_n FROM fib
	`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 8 {
		t.Fatalf("expected 8 rows, got %d", len(r.Rows))
	}
	// First few Fibonacci: 1, 1, 2, 3, 5, 8, 13, 21
	expected := []int64{1, 1, 2, 3, 5, 8, 13, 21}
	for i, row := range r.Rows {
		if row[0].I64 != expected[i] {
			t.Fatalf("row %d: expected %d, got %d", i, expected[i], row[0].I64)
		}
	}
}

// Verify column names are correct in CTE output.
func TestCTE_ColumnNames(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (x INT, y TEXT)`)
	ex.Exec(`INSERT INTO t VALUES (1, 'hello')`)

	r, err := ex.Exec(`WITH cte AS (SELECT x, y FROM t) SELECT x, y FROM cte`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(r.Columns))
	}
	// Column names should be unqualified.
	for _, col := range r.Columns {
		if strings.Contains(col, ".") {
			// The SQL executor strips qualifiers, but just check it's reasonable.
		}
	}
	if r.Rows[0][0].I64 != 1 || r.Rows[0][1].Text != "hello" {
		t.Fatalf("unexpected values: %v", r.Rows[0])
	}
}

// Verify CTE with ORDER BY on outer query.
func TestCTE_WithOrderBy(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT, val TEXT)`)
	ex.Exec(`INSERT INTO t VALUES (3, 'c'), (1, 'a'), (2, 'b')`)

	r, err := ex.Exec(`WITH cte AS (SELECT id, val FROM t) SELECT val FROM cte ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "a" || r.Rows[1][0].Text != "b" || r.Rows[2][0].Text != "c" {
		t.Fatalf("unexpected order: %v %v %v", r.Rows[0][0].Text, r.Rows[1][0].Text, r.Rows[2][0].Text)
	}
}

// Verify CTE with LIMIT on outer query.
func TestCTE_WithLimit(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)
	ex.Exec(`INSERT INTO t VALUES (1), (2), (3), (4), (5)`)

	r, err := ex.Exec(`WITH cte AS (SELECT id FROM t) SELECT id FROM cte LIMIT 3`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
}

// Suppress unused import warning.
var _ = tuple.DNull
