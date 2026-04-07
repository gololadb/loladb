package sql

import (
	"strings"
	"testing"

	"github.com/gololadb/loladb/pkg/tuple"
)

// ---------------------------------------------------------------------------
// CREATE TEMPORARY TABLE
// ---------------------------------------------------------------------------

func TestCreateTempTable(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TEMPORARY TABLE scratch (id INT, val TEXT)`)
	mustExec(t, ex, `INSERT INTO scratch VALUES (1, 'hello')`)

	r, err := ex.Exec(`SELECT val FROM scratch WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Text != "hello" {
		t.Fatalf("expected 'hello', got %v", r.Rows)
	}

	// Verify it's tracked as temp.
	if !ex.Cat.TempTables["scratch"] {
		t.Fatal("expected scratch to be tracked as temp table")
	}
}

func TestCreateTempTableDrop(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TEMP TABLE tmp1 (id INT)`)
	mustExec(t, ex, `INSERT INTO tmp1 VALUES (1)`)

	// DropTempTables should remove it.
	ex.Cat.DropTempTables()

	_, err := ex.Exec(`SELECT * FROM tmp1`)
	if err == nil {
		t.Fatal("expected error after dropping temp tables")
	}
}

// ---------------------------------------------------------------------------
// DROP TABLE
// ---------------------------------------------------------------------------

func TestDropTable(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE drop_me (id INT, name TEXT)`)
	mustExec(t, ex, `INSERT INTO drop_me VALUES (1, 'test')`)

	mustExec(t, ex, `DROP TABLE drop_me`)

	_, err := ex.Exec(`SELECT * FROM drop_me`)
	if err == nil {
		t.Fatal("expected error after DROP TABLE")
	}
}

func TestDropTableIfExists(t *testing.T) {
	ex := newTestExecutor(t)

	// Should not error with IF EXISTS on non-existent table.
	_, err := ex.Exec(`DROP TABLE IF EXISTS nonexistent`)
	if err != nil {
		t.Fatalf("DROP TABLE IF EXISTS should not error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// CREATE TABLE ... LIKE
// ---------------------------------------------------------------------------

func TestCreateTableLike(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE source (id INT NOT NULL, name TEXT, score INT)`)
	mustExec(t, ex, `CREATE TABLE copy_of_source (LIKE source)`)

	// Insert into the copy — should have the same columns.
	mustExec(t, ex, `INSERT INTO copy_of_source VALUES (1, 'Alice', 100)`)

	r, err := ex.Exec(`SELECT name, score FROM copy_of_source WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "Alice" {
		t.Errorf("expected 'Alice', got %q", r.Rows[0][0].Text)
	}
}

func TestCreateTableLikeWithExtraCols(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE base (id INT, val TEXT)`)
	mustExec(t, ex, `CREATE TABLE extended (LIKE base, extra INT)`)

	mustExec(t, ex, `INSERT INTO extended VALUES (1, 'test', 42)`)

	r, err := ex.Exec(`SELECT extra FROM extended`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || intVal(r.Rows[0][0]) != 42 {
		t.Fatalf("expected extra=42, got %v", r.Rows)
	}
}

// ---------------------------------------------------------------------------
// COMMENT ON
// ---------------------------------------------------------------------------

func TestCommentOn(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE commented (id INT, name TEXT)`)

	mustExec(t, ex, `COMMENT ON TABLE commented IS 'This is a test table'`)
	if ex.Cat.GetComment("table:commented") != "This is a test table" {
		t.Fatalf("expected comment, got %q", ex.Cat.GetComment("table:commented"))
	}

	// Update comment.
	mustExec(t, ex, `COMMENT ON TABLE commented IS 'Updated comment'`)
	if ex.Cat.GetComment("table:commented") != "Updated comment" {
		t.Fatalf("expected updated comment, got %q", ex.Cat.GetComment("table:commented"))
	}

	// Drop comment with IS NULL.
	mustExec(t, ex, `COMMENT ON TABLE commented IS NULL`)
	if ex.Cat.GetComment("table:commented") != "" {
		t.Fatalf("expected empty comment after NULL, got %q", ex.Cat.GetComment("table:commented"))
	}
}

func TestCommentOnColumn(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE col_comment (id INT, name TEXT)`)
	mustExec(t, ex, `COMMENT ON COLUMN col_comment.name IS 'The user name'`)

	if ex.Cat.GetComment("column:col_comment.name") != "The user name" {
		t.Fatalf("expected column comment, got %q", ex.Cat.GetComment("column:col_comment.name"))
	}
}

// ---------------------------------------------------------------------------
// Row value comparisons
// ---------------------------------------------------------------------------

func TestRowValueEqual(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE rv (a INT, b INT, c TEXT)`)
	mustExec(t, ex, `INSERT INTO rv VALUES (1, 2, 'match')`)
	mustExec(t, ex, `INSERT INTO rv VALUES (1, 3, 'no')`)
	mustExec(t, ex, `INSERT INTO rv VALUES (2, 2, 'no')`)

	r, err := ex.Exec(`SELECT c FROM rv WHERE (a, b) = (1, 2)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Text != "match" {
		t.Fatalf("expected 'match', got %v", r.Rows)
	}
}

func TestRowValueNotEqual(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE rv2 (a INT, b INT)`)
	mustExec(t, ex, `INSERT INTO rv2 VALUES (1, 2)`)
	mustExec(t, ex, `INSERT INTO rv2 VALUES (1, 1)`)

	r, err := ex.Exec(`SELECT * FROM rv2 WHERE (a, b) <> (1, 1)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
}

func TestRowValueGreaterThan(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE rv3 (a INT, b INT, label TEXT)`)
	mustExec(t, ex, `INSERT INTO rv3 VALUES (1, 1, 'low')`)
	mustExec(t, ex, `INSERT INTO rv3 VALUES (1, 2, 'mid')`)
	mustExec(t, ex, `INSERT INTO rv3 VALUES (2, 0, 'high')`)

	// (a, b) > (1, 1) should match (1,2) and (2,0) — lexicographic.
	r, err := ex.Exec(`SELECT label FROM rv3 WHERE (a, b) > (1, 1) ORDER BY a, b`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	// ORDER BY a, b: (1,2)='mid' then (2,0)='high'
	if r.Rows[0][0].Text != "mid" && r.Rows[1][0].Text != "mid" {
		t.Errorf("expected 'mid' in results, got %q and %q", r.Rows[0][0].Text, r.Rows[1][0].Text)
	}
	if r.Rows[0][0].Text != "high" && r.Rows[1][0].Text != "high" {
		t.Errorf("expected 'high' in results, got %q and %q", r.Rows[0][0].Text, r.Rows[1][0].Text)
	}
}

func TestRowValueSelectExpr(t *testing.T) {
	ex := newTestExecutor(t)

	// Row comparison in SELECT expression.
	r, err := ex.Exec(`SELECT (1, 2) = (1, 2)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Bool != true {
		t.Fatalf("expected true, got %v", r.Rows)
	}

	r, err = ex.Exec(`SELECT (1, 2) = (1, 3)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Bool != false {
		t.Fatalf("expected false, got %v", r.Rows)
	}
}

// ---------------------------------------------------------------------------
// Identity columns (GENERATED ALWAYS AS IDENTITY)
// ---------------------------------------------------------------------------

func TestIdentityColumn(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE ident (id INT GENERATED ALWAYS AS IDENTITY, name TEXT)`)

	mustExec(t, ex, `INSERT INTO ident (name) VALUES ('Alice')`)
	mustExec(t, ex, `INSERT INTO ident (name) VALUES ('Bob')`)
	mustExec(t, ex, `INSERT INTO ident (name) VALUES ('Charlie')`)

	r, err := ex.Exec(`SELECT id, name FROM ident ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}

	// IDs should be auto-incrementing: 1, 2, 3.
	for i, expected := range []int64{1, 2, 3} {
		if intVal(r.Rows[i][0]) != expected {
			t.Errorf("row %d: expected id=%d, got %v", i, expected, r.Rows[i][0])
		}
	}
}

func TestSerialColumn(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE ser (id SERIAL, name TEXT)`)

	mustExec(t, ex, `INSERT INTO ser (name) VALUES ('X')`)
	mustExec(t, ex, `INSERT INTO ser (name) VALUES ('Y')`)

	r, err := ex.Exec(`SELECT id, name FROM ser ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	if intVal(r.Rows[0][0]) != 1 {
		t.Errorf("expected id=1, got %v", r.Rows[0][0])
	}
	if intVal(r.Rows[1][0]) != 2 {
		t.Errorf("expected id=2, got %v", r.Rows[1][0])
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func mustExec2(t *testing.T, ex *Executor, sql string) {
	t.Helper()
	_, err := ex.Exec(sql)
	if err != nil {
		t.Fatalf("mustExec(%q): %v", sql, err)
	}
}

// intVal2 extracts an integer value from a datum regardless of int32/int64 type.
func intVal2(d tuple.Datum) int64 {
	switch d.Type {
	case tuple.TypeInt32:
		return int64(d.I32)
	case tuple.TypeInt64:
		return d.I64
	default:
		return d.I64
	}
}

// Ensure we use the helpers from medium_test.go (intVal, mustExec).
// The above are backup definitions in case they're needed.
var _ = strings.Contains // suppress unused import
