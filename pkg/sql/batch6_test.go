package sql

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// GROUPING SETS / CUBE / ROLLUP
// ---------------------------------------------------------------------------

func TestGroupingSets(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE sales (brand TEXT, size TEXT, amount INT)`)
	mustExec(t, ex, `INSERT INTO sales VALUES ('A', 'S', 10)`)
	mustExec(t, ex, `INSERT INTO sales VALUES ('A', 'M', 20)`)
	mustExec(t, ex, `INSERT INTO sales VALUES ('B', 'S', 30)`)
	mustExec(t, ex, `INSERT INTO sales VALUES ('B', 'M', 40)`)

	r, err := ex.Exec(`SELECT brand, size, sum(amount) FROM sales GROUP BY GROUPING SETS ((brand), (size), ())`)
	if err != nil {
		t.Fatal(err)
	}
	// Expect: 2 brand groups + 2 size groups + 1 grand total = 5 rows
	if len(r.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(r.Rows))
	}
}

func TestRollup(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE r (a TEXT, b TEXT, c INT)`)
	mustExec(t, ex, `INSERT INTO r VALUES ('x', 'p', 1)`)
	mustExec(t, ex, `INSERT INTO r VALUES ('x', 'q', 2)`)
	mustExec(t, ex, `INSERT INTO r VALUES ('y', 'p', 3)`)

	r, err := ex.Exec(`SELECT a, b, sum(c) FROM r GROUP BY ROLLUP(a, b)`)
	if err != nil {
		t.Fatal(err)
	}
	// ROLLUP(a,b) → (a,b), (a), () = 3 levels
	// (x,p,1), (x,q,2), (y,p,3), (x,NULL,3), (y,NULL,3), (NULL,NULL,6) = 6 rows
	if len(r.Rows) != 6 {
		t.Fatalf("expected 6 rows, got %d", len(r.Rows))
	}
}

func TestCube(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE c (a TEXT, b TEXT, v INT)`)
	mustExec(t, ex, `INSERT INTO c VALUES ('x', 'p', 1)`)
	mustExec(t, ex, `INSERT INTO c VALUES ('x', 'q', 2)`)
	mustExec(t, ex, `INSERT INTO c VALUES ('y', 'p', 3)`)

	r, err := ex.Exec(`SELECT a, b, sum(v) FROM c GROUP BY CUBE(a, b)`)
	if err != nil {
		t.Fatal(err)
	}
	// CUBE(a,b) → (a,b), (a), (b), () = 4 levels
	// (a,b): (x,p,1),(x,q,2),(y,p,3) = 3
	// (a): (x,3),(y,3) = 2
	// (b): (p,4),(q,2) = 2
	// (): (6) = 1
	// Total = 8
	if len(r.Rows) != 8 {
		t.Fatalf("expected 8 rows, got %d", len(r.Rows))
	}
}

// ---------------------------------------------------------------------------
// GRANT / REVOKE
// ---------------------------------------------------------------------------

func TestGrantRevoke(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE secret (id INT, data TEXT)`)

	r, err := ex.Exec(`GRANT SELECT, INSERT ON secret TO testrole`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "GRANT" {
		t.Fatalf("expected GRANT message, got %s", r.Message)
	}

	r2, err := ex.Exec(`REVOKE INSERT ON secret FROM testrole`)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Message != "REVOKE" {
		t.Fatalf("expected REVOKE message, got %s", r2.Message)
	}
}

// ---------------------------------------------------------------------------
// SELECT ... FOR UPDATE / FOR SHARE
// ---------------------------------------------------------------------------

func TestForUpdateAccepted(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE locktest (id INT, val TEXT)`)
	mustExec(t, ex, `INSERT INTO locktest VALUES (1, 'a')`)
	mustExec(t, ex, `INSERT INTO locktest VALUES (2, 'b')`)

	r, err := ex.Exec(`SELECT * FROM locktest FOR UPDATE`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}

	r2, err := ex.Exec(`SELECT * FROM locktest FOR SHARE`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r2.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r2.Rows))
	}
}

// ---------------------------------------------------------------------------
// REINDEX TABLE
// ---------------------------------------------------------------------------

func TestReindexTable(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE reindex_t (id INT)`)

	r, err := ex.Exec(`REINDEX TABLE reindex_t`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "REINDEX" {
		t.Fatalf("expected REINDEX message, got %s", r.Message)
	}
}

func TestReindexNonexistent(t *testing.T) {
	ex := newTestExecutor(t)
	_, err := ex.Exec(`REINDEX TABLE no_such_table`)
	if err == nil {
		t.Fatal("expected error for nonexistent table")
	}
}

// ---------------------------------------------------------------------------
// Array containment operators
// ---------------------------------------------------------------------------

func TestArrayContains(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT ARRAY[1,2,3] @> ARRAY[2,3]`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	val := strings.ToLower(r.Rows[0][0].Text)
	if val != "true" && !r.Rows[0][0].Bool {
		t.Fatalf("expected true, got %v", r.Rows[0][0])
	}
}

func TestArrayContainsFalse(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT ARRAY[1,2] @> ARRAY[3]`)
	if err != nil {
		t.Fatal(err)
	}
	val := strings.ToLower(r.Rows[0][0].Text)
	if val != "false" && r.Rows[0][0].Bool {
		t.Fatalf("expected false, got %v", r.Rows[0][0])
	}
}

func TestArrayContainedBy(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT ARRAY[2,3] <@ ARRAY[1,2,3,4]`)
	if err != nil {
		t.Fatal(err)
	}
	val := strings.ToLower(r.Rows[0][0].Text)
	if val != "true" && !r.Rows[0][0].Bool {
		t.Fatalf("expected true, got %v", r.Rows[0][0])
	}
}

func TestArrayOverlap(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT ARRAY[1,2] && ARRAY[2,3]`)
	if err != nil {
		t.Fatal(err)
	}
	val := strings.ToLower(r.Rows[0][0].Text)
	if val != "true" && !r.Rows[0][0].Bool {
		t.Fatalf("expected true, got %v", r.Rows[0][0])
	}
}

func TestArrayOverlapFalse(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT ARRAY[1,2] && ARRAY[3,4]`)
	if err != nil {
		t.Fatal(err)
	}
	val := strings.ToLower(r.Rows[0][0].Text)
	if val != "false" && r.Rows[0][0].Bool {
		t.Fatalf("expected false, got %v", r.Rows[0][0])
	}
}
