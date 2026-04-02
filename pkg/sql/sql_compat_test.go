package sql

import (
	"strings"
	"testing"

	"github.com/gololadb/loladb/pkg/tuple"
)

// --- LIKE / ILIKE ---

func TestSQL_Like(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE items (name TEXT)`)
	ex.Exec(`INSERT INTO items VALUES ('apple'), ('banana'), ('apricot'), ('APPLE')`)

	r, err := ex.Exec(`SELECT name FROM items WHERE name LIKE 'ap%' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "apple" || r.Rows[1][0].Text != "apricot" {
		t.Fatalf("unexpected rows: %v", r.Rows)
	}
}

func TestSQL_ILike(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE items (name TEXT)`)
	ex.Exec(`INSERT INTO items VALUES ('apple'), ('APPLE'), ('banana')`)

	r, err := ex.Exec(`SELECT name FROM items WHERE name ILIKE 'apple'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

func TestSQL_NotLike(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE items (name TEXT)`)
	ex.Exec(`INSERT INTO items VALUES ('apple'), ('banana'), ('apricot')`)

	r, err := ex.Exec(`SELECT name FROM items WHERE name NOT LIKE 'ap%'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Text != "banana" {
		t.Fatalf("unexpected: %v", r.Rows)
	}
}

// --- BETWEEN ---

func TestSQL_Between(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE nums (val INT)`)
	ex.Exec(`INSERT INTO nums VALUES (1), (5), (10), (15), (20)`)

	r, err := ex.Exec(`SELECT val FROM nums WHERE val BETWEEN 5 AND 15 ORDER BY val`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
}

// --- IN ---

func TestSQL_In(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE colors (name TEXT)`)
	ex.Exec(`INSERT INTO colors VALUES ('red'), ('green'), ('blue'), ('yellow')`)

	r, err := ex.Exec(`SELECT name FROM colors WHERE name IN ('red', 'blue') ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

// --- String concatenation ---

func TestSQL_Concat(t *testing.T) {
	ex := newTestExecutor(t)
	d := evalExpr(t, ex, "'hello' || ' ' || 'world'")
	if d.Text != "hello world" {
		t.Fatalf("expected 'hello world', got %q", d.Text)
	}
}

// --- IS DISTINCT FROM ---

func TestSQL_IsDistinctFrom(t *testing.T) {
	ex := newTestExecutor(t)

	d := evalExpr(t, ex, "1 IS DISTINCT FROM 2")
	if !d.Bool {
		t.Fatal("1 IS DISTINCT FROM 2 should be true")
	}

	d = evalExpr(t, ex, "1 IS DISTINCT FROM 1")
	if d.Bool {
		t.Fatal("1 IS DISTINCT FROM 1 should be false")
	}

	d = evalExpr(t, ex, "NULL IS DISTINCT FROM NULL")
	if d.Bool {
		t.Fatal("NULL IS DISTINCT FROM NULL should be false")
	}

	d = evalExpr(t, ex, "1 IS DISTINCT FROM NULL")
	if !d.Bool {
		t.Fatal("1 IS DISTINCT FROM NULL should be true")
	}
}

// --- SELECT DISTINCT ---

func TestSQL_Distinct(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE tags (tag TEXT)`)
	ex.Exec(`INSERT INTO tags VALUES ('a'), ('b'), ('a'), ('c'), ('b')`)

	r, err := ex.Exec(`SELECT DISTINCT tag FROM tags ORDER BY tag`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 distinct rows, got %d", len(r.Rows))
	}
}

// --- UNION / INTERSECT / EXCEPT ---

func TestSQL_Union(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t1 (x INT)`)
	ex.Exec(`CREATE TABLE t2 (x INT)`)
	ex.Exec(`INSERT INTO t1 VALUES (1), (2), (3)`)
	ex.Exec(`INSERT INTO t2 VALUES (2), (3), (4)`)

	r, err := ex.Exec(`SELECT x FROM t1 UNION SELECT x FROM t2`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 4 {
		t.Fatalf("UNION: expected 4 rows, got %d", len(r.Rows))
	}
}

func TestSQL_UnionAll(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t1 (x INT)`)
	ex.Exec(`CREATE TABLE t2 (x INT)`)
	ex.Exec(`INSERT INTO t1 VALUES (1), (2)`)
	ex.Exec(`INSERT INTO t2 VALUES (2), (3)`)

	r, err := ex.Exec(`SELECT x FROM t1 UNION ALL SELECT x FROM t2`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 4 {
		t.Fatalf("UNION ALL: expected 4 rows, got %d", len(r.Rows))
	}
}

func TestSQL_Intersect(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t1 (x INT)`)
	ex.Exec(`CREATE TABLE t2 (x INT)`)
	ex.Exec(`INSERT INTO t1 VALUES (1), (2), (3)`)
	ex.Exec(`INSERT INTO t2 VALUES (2), (3), (4)`)

	r, err := ex.Exec(`SELECT x FROM t1 INTERSECT SELECT x FROM t2`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("INTERSECT: expected 2 rows, got %d", len(r.Rows))
	}
}

func TestSQL_Except(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t1 (x INT)`)
	ex.Exec(`CREATE TABLE t2 (x INT)`)
	ex.Exec(`INSERT INTO t1 VALUES (1), (2), (3)`)
	ex.Exec(`INSERT INTO t2 VALUES (2), (3), (4)`)

	r, err := ex.Exec(`SELECT x FROM t1 EXCEPT SELECT x FROM t2`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("EXCEPT: expected 1 row, got %d", len(r.Rows))
	}
}

// --- INSERT ... SELECT ---

func TestSQL_InsertSelect(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE src (id INT, name TEXT)`)
	ex.Exec(`CREATE TABLE dst (id INT, name TEXT)`)
	ex.Exec(`INSERT INTO src VALUES (1, 'a'), (2, 'b'), (3, 'c')`)

	r, err := ex.Exec(`INSERT INTO dst SELECT id, name FROM src WHERE id > 1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.RowsAffected != 2 {
		t.Fatalf("expected 2 rows inserted, got %d", r.RowsAffected)
	}

	r, err = ex.Exec(`SELECT id, name FROM dst ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows in dst, got %d", len(r.Rows))
	}
}

// --- TRUNCATE TABLE ---

func TestSQL_Truncate(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE data (id INT, val TEXT)`)
	ex.Exec(`INSERT INTO data VALUES (1, 'a'), (2, 'b'), (3, 'c')`)

	r, err := ex.Exec(`TRUNCATE TABLE data`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "TRUNCATE TABLE" {
		t.Fatalf("unexpected message: %s", r.Message)
	}

	r, err = ex.Exec(`SELECT * FROM data`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 0 {
		t.Fatalf("expected 0 rows after TRUNCATE, got %d", len(r.Rows))
	}

	// Table should still be usable.
	_, err = ex.Exec(`INSERT INTO data VALUES (10, 'new')`)
	if err != nil {
		t.Fatal(err)
	}
	r, err = ex.Exec(`SELECT * FROM data`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row after re-insert, got %d", len(r.Rows))
	}
}

// --- DROP INDEX ---

func TestSQL_DropIndex(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)
	ex.Exec(`CREATE INDEX idx_t_id ON t (id)`)

	_, err := ex.Exec(`DROP INDEX idx_t_id`)
	if err != nil {
		t.Fatal(err)
	}

	// DROP IF EXISTS on non-existent index should not error.
	_, err = ex.Exec(`DROP INDEX IF EXISTS idx_t_id`)
	if err != nil {
		t.Fatal(err)
	}
}

// --- DROP VIEW ---

func TestSQL_DropView(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT, name TEXT)`)
	ex.Exec(`CREATE VIEW v AS SELECT id FROM t`)

	_, err := ex.Exec(`DROP VIEW v`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = ex.Exec(`DROP VIEW IF EXISTS v`)
	if err != nil {
		t.Fatal(err)
	}
}

// --- ALTER TABLE ADD/DROP COLUMN ---

func TestSQL_AlterTableAddColumn(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)
	ex.Exec(`INSERT INTO t VALUES (1)`)

	_, err := ex.Exec(`ALTER TABLE t ADD COLUMN name TEXT`)
	if err != nil {
		t.Fatal(err)
	}

	// New inserts should accept the new column.
	_, err = ex.Exec(`INSERT INTO t VALUES (2, 'Bob')`)
	if err != nil {
		t.Fatal(err)
	}

	r, err := ex.Exec(`SELECT id, name FROM t WHERE id = 2`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][1].Text != "Bob" {
		t.Fatalf("unexpected: %v", r.Rows)
	}
}

func TestSQL_AlterTableDropColumn(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT, name TEXT, age INT)`)

	_, err := ex.Exec(`ALTER TABLE t DROP COLUMN name`)
	if err != nil {
		t.Fatal(err)
	}

	// IF EXISTS on non-existent column should not error.
	_, err = ex.Exec(`ALTER TABLE t DROP COLUMN IF EXISTS name`)
	if err != nil {
		t.Fatal(err)
	}
}

// --- PRIMARY KEY / UNIQUE constraints ---

func TestSQL_PrimaryKey(t *testing.T) {
	ex := newTestExecutor(t)
	_, err := ex.Exec(`CREATE TABLE pk_test (id INT PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = ex.Exec(`INSERT INTO pk_test VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatal(err)
	}

	// Duplicate PK should fail.
	_, err = ex.Exec(`INSERT INTO pk_test VALUES (1, 'Bob')`)
	if err == nil {
		t.Fatal("expected duplicate key error")
	}
	if !strings.Contains(err.Error(), "duplicate key") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSQL_Unique(t *testing.T) {
	ex := newTestExecutor(t)
	_, err := ex.Exec(`CREATE TABLE uq_test (id INT, email TEXT UNIQUE)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = ex.Exec(`INSERT INTO uq_test VALUES (1, 'a@b.com')`)
	if err != nil {
		t.Fatal(err)
	}

	// Duplicate UNIQUE should fail.
	_, err = ex.Exec(`INSERT INTO uq_test VALUES (2, 'a@b.com')`)
	if err == nil {
		t.Fatal("expected duplicate key error")
	}
	if !strings.Contains(err.Error(), "duplicate key") {
		t.Fatalf("unexpected error: %v", err)
	}

	// NULL values should not conflict.
	_, err = ex.Exec(`INSERT INTO uq_test VALUES (3, NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ex.Exec(`INSERT INTO uq_test VALUES (4, NULL)`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSQL_TableLevelPrimaryKey(t *testing.T) {
	ex := newTestExecutor(t)
	_, err := ex.Exec(`CREATE TABLE tlpk (a INT, b TEXT, PRIMARY KEY (a))`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = ex.Exec(`INSERT INTO tlpk VALUES (1, 'x')`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = ex.Exec(`INSERT INTO tlpk VALUES (1, 'y')`)
	if err == nil {
		t.Fatal("expected duplicate key error for table-level PK")
	}
}

// --- INSERT ... RETURNING ---

func TestSQL_InsertReturning(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE ret (id INT, name TEXT)`)

	r, err := ex.Exec(`INSERT INTO ret VALUES (1, 'Alice'), (2, 'Bob') RETURNING id, name`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 RETURNING rows, got %d", len(r.Rows))
	}
	if len(r.Columns) != 2 || r.Columns[0] != "id" || r.Columns[1] != "name" {
		t.Fatalf("unexpected columns: %v", r.Columns)
	}
	// The id value may be stored as I32 or I64 depending on the constant type.
	id := r.Rows[0][0].I64
	if id == 0 {
		id = int64(r.Rows[0][0].I32)
	}
	if id != 1 || r.Rows[0][1].Text != "Alice" {
		t.Fatalf("unexpected first row: id=%d name=%s", id, r.Rows[0][1].Text)
	}
}

func TestSQL_InsertReturningStar(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE ret2 (id INT, val TEXT)`)

	r, err := ex.Exec(`INSERT INTO ret2 VALUES (42, 'test') RETURNING *`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || len(r.Columns) != 2 {
		t.Fatalf("expected 1 row with 2 columns, got %dx%d", len(r.Rows), len(r.Columns))
	}
}

// --- DELETE ... RETURNING ---

func TestSQL_DeleteReturning(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE dret (id INT, name TEXT)`)
	ex.Exec(`INSERT INTO dret VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`)

	r, err := ex.Exec(`DELETE FROM dret WHERE id > 1 RETURNING id, name`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 RETURNING rows, got %d", len(r.Rows))
	}
	if r.Columns[0] != "id" {
		t.Fatalf("unexpected columns: %v", r.Columns)
	}
}

// --- UPDATE ... RETURNING ---

func TestSQL_UpdateReturning(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE uret (id INT, val INT)`)
	ex.Exec(`INSERT INTO uret VALUES (1, 10), (2, 20), (3, 30)`)

	r, err := ex.Exec(`UPDATE uret SET val = val + 100 WHERE id >= 2 RETURNING id, val`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 RETURNING rows, got %d", len(r.Rows))
	}
	// RETURNING should show the new values.
	for _, row := range r.Rows {
		val := row[1].I64
		if val == 0 {
			val = int64(row[1].I32)
		}
		if val < 100 {
			t.Fatalf("RETURNING should show updated val, got %d", val)
		}
	}
}

// --- CASE expression ---

func TestSQL_CaseExpression(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE grades (score INT)`)
	ex.Exec(`INSERT INTO grades VALUES (95), (75), (55)`)

	r, err := ex.Exec(`SELECT score, CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'C' END AS grade FROM grades`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
	// Verify all three grades are present.
	grades := map[string]bool{}
	for _, row := range r.Rows {
		grades[row[1].Text] = true
	}
	if !grades["A"] || !grades["B"] || !grades["C"] {
		t.Fatalf("expected A, B, C grades, got %v", grades)
	}
}

// --- IS TRUE / IS FALSE / IS NOT NULL ---

func TestSQL_BooleanTest(t *testing.T) {
	ex := newTestExecutor(t)

	d := evalExpr(t, ex, "TRUE IS TRUE")
	if !d.Bool {
		t.Fatal("TRUE IS TRUE should be true")
	}

	d = evalExpr(t, ex, "FALSE IS TRUE")
	if d.Bool {
		t.Fatal("FALSE IS TRUE should be false")
	}

	d = evalExpr(t, ex, "NULL IS NOT NULL")
	if d.Bool {
		t.Fatal("NULL IS NOT NULL should be false")
	}
}

// Verify unused import suppression.
var _ = tuple.DNull
