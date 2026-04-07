package sql

import (
	"fmt"
	"testing"
)

// ---------------------------------------------------------------------------
// VACUUM — basic dead tuple reclamation
// ---------------------------------------------------------------------------

func TestVacuum_Basic(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE v1 (id INT, name TEXT)`)
	mustExec(t, ex, `INSERT INTO v1 VALUES (1, 'Alice')`)
	mustExec(t, ex, `INSERT INTO v1 VALUES (2, 'Bob')`)
	mustExec(t, ex, `INSERT INTO v1 VALUES (3, 'Charlie')`)

	// Delete two rows to create dead tuples.
	mustExec(t, ex, `DELETE FROM v1 WHERE id IN (1, 3)`)

	// VACUUM should succeed.
	r, err := ex.Exec(`VACUUM v1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "VACUUM" {
		t.Fatalf("expected 'VACUUM', got %q", r.Message)
	}

	// Only 1 row should remain.
	r, err = ex.Exec(`SELECT count(*) FROM v1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].I64 != 1 {
		t.Fatalf("expected 1 row after vacuum, got %v", r.Rows)
	}
}

// ---------------------------------------------------------------------------
// VACUUM without table name — vacuums all tables
// ---------------------------------------------------------------------------

func TestVacuum_AllTables(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE va1 (id INT)`)
	mustExec(t, ex, `CREATE TABLE va2 (id INT)`)
	mustExec(t, ex, `INSERT INTO va1 VALUES (1)`)
	mustExec(t, ex, `INSERT INTO va2 VALUES (1)`)
	mustExec(t, ex, `DELETE FROM va1 WHERE id = 1`)
	mustExec(t, ex, `DELETE FROM va2 WHERE id = 1`)

	// VACUUM with no table name should vacuum all tables.
	r, err := ex.Exec(`VACUUM`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "VACUUM" {
		t.Fatalf("expected 'VACUUM', got %q", r.Message)
	}

	// Both tables should be empty.
	r, err = ex.Exec(`SELECT count(*) FROM va1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].I64 != 0 {
		t.Fatalf("expected 0 rows in va1, got %d", r.Rows[0][0].I64)
	}
	r, err = ex.Exec(`SELECT count(*) FROM va2`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].I64 != 0 {
		t.Fatalf("expected 0 rows in va2, got %d", r.Rows[0][0].I64)
	}
}

// ---------------------------------------------------------------------------
// VACUUM FULL
// ---------------------------------------------------------------------------

func TestVacuumFull(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE vf1 (id INT, payload TEXT)`)
	for i := 0; i < 20; i++ {
		mustExec(t, ex, `INSERT INTO vf1 VALUES (`+itoa(i)+`, 'data')`)
	}
	// Delete all rows.
	mustExec(t, ex, `DELETE FROM vf1 WHERE id >= 0`)

	r, err := ex.Exec(`VACUUM FULL vf1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "VACUUM" {
		t.Fatalf("expected 'VACUUM', got %q", r.Message)
	}

	// Table should be empty.
	r, err = ex.Exec(`SELECT count(*) FROM vf1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].I64 != 0 {
		t.Fatalf("expected 0 rows, got %d", r.Rows[0][0].I64)
	}
}

// ---------------------------------------------------------------------------
// ANALYZE — standalone
// ---------------------------------------------------------------------------

func TestAnalyze_Table(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE an1 (id INT, name TEXT)`)
	mustExec(t, ex, `INSERT INTO an1 VALUES (1, 'Alice')`)
	mustExec(t, ex, `INSERT INTO an1 VALUES (2, 'Bob')`)
	mustExec(t, ex, `INSERT INTO an1 VALUES (3, 'Charlie')`)

	r, err := ex.Exec(`ANALYZE an1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "ANALYZE" {
		t.Fatalf("expected 'ANALYZE', got %q", r.Message)
	}
}

// ---------------------------------------------------------------------------
// ANALYZE — all tables
// ---------------------------------------------------------------------------

func TestAnalyze_AllTables(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE an2 (id INT)`)
	mustExec(t, ex, `CREATE TABLE an3 (id INT)`)
	mustExec(t, ex, `INSERT INTO an2 VALUES (1)`)
	mustExec(t, ex, `INSERT INTO an3 VALUES (1)`)

	r, err := ex.Exec(`ANALYZE`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "ANALYZE" {
		t.Fatalf("expected 'ANALYZE', got %q", r.Message)
	}
}

// ---------------------------------------------------------------------------
// VACUUM ANALYZE — combined
// ---------------------------------------------------------------------------

func TestVacuumAnalyze(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE vac1 (id INT, name TEXT)`)
	mustExec(t, ex, `INSERT INTO vac1 VALUES (1, 'Alice')`)
	mustExec(t, ex, `INSERT INTO vac1 VALUES (2, 'Bob')`)
	mustExec(t, ex, `DELETE FROM vac1 WHERE id = 1`)

	r, err := ex.Exec(`VACUUM ANALYZE vac1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "VACUUM" {
		t.Fatalf("expected 'VACUUM', got %q", r.Message)
	}

	// Verify vacuum worked — only 1 row.
	r, err = ex.Exec(`SELECT count(*) FROM vac1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].I64 != 1 {
		t.Fatalf("expected 1 row, got %d", r.Rows[0][0].I64)
	}
}

// ---------------------------------------------------------------------------
// VACUUM on empty table
// ---------------------------------------------------------------------------

func TestVacuum_EmptyTable(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE ve1 (id INT)`)

	r, err := ex.Exec(`VACUUM ve1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "VACUUM" {
		t.Fatalf("expected 'VACUUM', got %q", r.Message)
	}
}

// ---------------------------------------------------------------------------
// VACUUM on nonexistent table
// ---------------------------------------------------------------------------

func TestVacuum_NonexistentTable(t *testing.T) {
	ex := newTestExecutor(t)
	_, err := ex.Exec(`VACUUM nonexistent`)
	if err == nil {
		t.Fatal("expected error for nonexistent table")
	}
}

// ---------------------------------------------------------------------------
// ANALYZE on nonexistent table
// ---------------------------------------------------------------------------

func TestAnalyze_NonexistentTable(t *testing.T) {
	ex := newTestExecutor(t)
	_, err := ex.Exec(`ANALYZE nonexistent`)
	if err == nil {
		t.Fatal("expected error for nonexistent table")
	}
}

// ---------------------------------------------------------------------------
// VACUUM after UPDATE (updates create dead tuples)
// ---------------------------------------------------------------------------

func TestVacuum_AfterUpdate(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE vu1 (id INT, val TEXT)`)
	mustExec(t, ex, `INSERT INTO vu1 VALUES (1, 'old')`)
	mustExec(t, ex, `INSERT INTO vu1 VALUES (2, 'old')`)

	// UPDATE creates dead tuples (old versions).
	mustExec(t, ex, `UPDATE vu1 SET val = 'new' WHERE id = 1`)

	r, err := ex.Exec(`VACUUM vu1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "VACUUM" {
		t.Fatalf("expected 'VACUUM', got %q", r.Message)
	}

	// Both rows should still be visible with correct values.
	r, err = ex.Exec(`SELECT id, val FROM vu1 ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	if r.Rows[0][1].Text != "new" {
		t.Fatalf("expected 'new' for id=1, got %q", r.Rows[0][1].Text)
	}
}

// ---------------------------------------------------------------------------
// VACUUM FREEZE (accepted, no-op)
// ---------------------------------------------------------------------------

func TestVacuum_Freeze(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE vfr1 (id INT)`)
	mustExec(t, ex, `INSERT INTO vfr1 VALUES (1)`)

	r, err := ex.Exec(`VACUUM FREEZE vfr1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "VACUUM" {
		t.Fatalf("expected 'VACUUM', got %q", r.Message)
	}
}

// ---------------------------------------------------------------------------
// Repeated VACUUM — second pass should be a no-op
// ---------------------------------------------------------------------------

func TestVacuum_Repeated(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE vr1 (id INT)`)
	mustExec(t, ex, `INSERT INTO vr1 VALUES (1)`)
	mustExec(t, ex, `DELETE FROM vr1 WHERE id = 1`)

	mustExec(t, ex, `VACUUM vr1`)

	// Second vacuum should succeed with nothing to do.
	r, err := ex.Exec(`VACUUM vr1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "VACUUM" {
		t.Fatalf("expected 'VACUUM', got %q", r.Message)
	}
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}
