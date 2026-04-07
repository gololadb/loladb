package sql

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// BEGIN / COMMIT
// ---------------------------------------------------------------------------

func TestTx_BeginCommit(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)

	ex.Exec(`BEGIN`)
	ex.Exec(`INSERT INTO t VALUES (1)`)
	ex.Exec(`INSERT INTO t VALUES (2)`)
	ex.Exec(`COMMIT`)

	r, err := ex.Exec(`SELECT id FROM t ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	if r.Rows[0][0].I64 != 1 || r.Rows[1][0].I64 != 2 {
		t.Fatalf("unexpected rows: %v", r.Rows)
	}
}

// ---------------------------------------------------------------------------
// BEGIN / ROLLBACK
// ---------------------------------------------------------------------------

func TestTx_BeginRollback(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)
	ex.Exec(`INSERT INTO t VALUES (0)`) // pre-existing row

	ex.Exec(`BEGIN`)
	ex.Exec(`INSERT INTO t VALUES (1)`)
	ex.Exec(`INSERT INTO t VALUES (2)`)
	ex.Exec(`ROLLBACK`)

	r, err := ex.Exec(`SELECT id FROM t ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row after rollback, got %d", len(r.Rows))
	}
	if r.Rows[0][0].I64 != 0 {
		t.Fatalf("expected id=0, got %d", r.Rows[0][0].I64)
	}
}

func TestTx_RollbackUpdate(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT, val TEXT)`)
	ex.Exec(`INSERT INTO t VALUES (1, 'old')`)

	ex.Exec(`BEGIN`)
	ex.Exec(`UPDATE t SET val = 'new' WHERE id = 1`)

	// Verify the update is visible within the transaction.
	r, _ := ex.Exec(`SELECT val FROM t WHERE id = 1`)
	if r.Rows[0][0].Text != "new" {
		t.Fatalf("expected 'new' within tx, got %q", r.Rows[0][0].Text)
	}

	ex.Exec(`ROLLBACK`)

	// After rollback, the old value should be restored.
	r, err := ex.Exec(`SELECT val FROM t WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Text != "old" {
		t.Fatalf("expected 'old' after rollback, got %q", r.Rows[0][0].Text)
	}
}

func TestTx_RollbackDelete(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)
	ex.Exec(`INSERT INTO t VALUES (1)`)
	ex.Exec(`INSERT INTO t VALUES (2)`)

	ex.Exec(`BEGIN`)
	ex.Exec(`DELETE FROM t WHERE id = 1`)

	r, _ := ex.Exec(`SELECT id FROM t ORDER BY id`)
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row within tx, got %d", len(r.Rows))
	}

	ex.Exec(`ROLLBACK`)

	r, err := ex.Exec(`SELECT id FROM t ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows after rollback, got %d", len(r.Rows))
	}
}

// ---------------------------------------------------------------------------
// Failed transaction state
// ---------------------------------------------------------------------------

func TestTx_FailedState(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)

	ex.Exec(`BEGIN`)
	ex.Exec(`INSERT INTO t VALUES (1)`)

	// Cause an error (table doesn't exist).
	_, err := ex.Exec(`INSERT INTO nonexistent VALUES (2)`)
	if err == nil {
		t.Fatal("expected error for nonexistent table")
	}

	// Further commands should be rejected.
	_, err = ex.Exec(`INSERT INTO t VALUES (3)`)
	if err == nil {
		t.Fatal("expected error in failed transaction")
	}
	if !strings.Contains(err.Error(), "current transaction is aborted") {
		t.Fatalf("unexpected error: %v", err)
	}

	// ROLLBACK should work and undo the first insert.
	ex.Exec(`ROLLBACK`)

	r, err := ex.Exec(`SELECT id FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 0 {
		t.Fatalf("expected 0 rows after rollback of failed tx, got %d", len(r.Rows))
	}
}

func TestTx_CommitFailedTx(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)

	ex.Exec(`BEGIN`)
	ex.Exec(`INSERT INTO t VALUES (1)`)

	// Cause an error.
	ex.Exec(`SELECT * FROM nonexistent`)

	// COMMIT of a failed transaction should rollback (PostgreSQL behavior).
	r, err := ex.Exec(`COMMIT`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "ROLLBACK" {
		t.Fatalf("expected ROLLBACK message, got %q", r.Message)
	}

	r, err = ex.Exec(`SELECT id FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(r.Rows))
	}
}

// ---------------------------------------------------------------------------
// SAVEPOINT / ROLLBACK TO
// ---------------------------------------------------------------------------

func TestTx_Savepoint(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)

	ex.Exec(`BEGIN`)
	ex.Exec(`INSERT INTO t VALUES (1)`)
	ex.Exec(`SAVEPOINT sp1`)
	ex.Exec(`INSERT INTO t VALUES (2)`)
	ex.Exec(`ROLLBACK TO sp1`)
	ex.Exec(`COMMIT`)

	// Only row 1 should be committed.
	r, err := ex.Exec(`SELECT id FROM t ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].I64 != 1 {
		t.Fatalf("expected id=1, got %d", r.Rows[0][0].I64)
	}
}

func TestTx_NestedSavepoints(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)

	ex.Exec(`BEGIN`)
	ex.Exec(`INSERT INTO t VALUES (1)`)
	ex.Exec(`SAVEPOINT sp1`)
	ex.Exec(`INSERT INTO t VALUES (2)`)
	ex.Exec(`SAVEPOINT sp2`)
	ex.Exec(`INSERT INTO t VALUES (3)`)
	ex.Exec(`ROLLBACK TO sp2`)
	// Row 3 undone, rows 1 and 2 remain.
	ex.Exec(`COMMIT`)

	r, err := ex.Exec(`SELECT id FROM t ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	if r.Rows[0][0].I64 != 1 || r.Rows[1][0].I64 != 2 {
		t.Fatalf("unexpected rows: %v", r.Rows)
	}
}

func TestTx_RollbackToOuterSavepoint(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)

	ex.Exec(`BEGIN`)
	ex.Exec(`INSERT INTO t VALUES (1)`)
	ex.Exec(`SAVEPOINT sp1`)
	ex.Exec(`INSERT INTO t VALUES (2)`)
	ex.Exec(`SAVEPOINT sp2`)
	ex.Exec(`INSERT INTO t VALUES (3)`)
	// Roll back to sp1 — undoes rows 2 and 3.
	ex.Exec(`ROLLBACK TO sp1`)
	ex.Exec(`COMMIT`)

	r, err := ex.Exec(`SELECT id FROM t ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].I64 != 1 {
		t.Fatalf("expected id=1, got %d", r.Rows[0][0].I64)
	}
}

func TestTx_SavepointRecoverFromError(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)

	ex.Exec(`BEGIN`)
	ex.Exec(`INSERT INTO t VALUES (1)`)
	ex.Exec(`SAVEPOINT sp1`)

	// Cause an error.
	_, err := ex.Exec(`INSERT INTO nonexistent VALUES (2)`)
	if err == nil {
		t.Fatal("expected error")
	}

	// ROLLBACK TO should recover from the failed state.
	_, err = ex.Exec(`ROLLBACK TO sp1`)
	if err != nil {
		t.Fatal(err)
	}

	// Should be able to continue.
	ex.Exec(`INSERT INTO t VALUES (3)`)
	ex.Exec(`COMMIT`)

	r, err := ex.Exec(`SELECT id FROM t ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	if r.Rows[0][0].I64 != 1 || r.Rows[1][0].I64 != 3 {
		t.Fatalf("unexpected rows: %v", r.Rows)
	}
}

// ---------------------------------------------------------------------------
// RELEASE SAVEPOINT
// ---------------------------------------------------------------------------

func TestTx_ReleaseSavepoint(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)

	ex.Exec(`BEGIN`)
	ex.Exec(`INSERT INTO t VALUES (1)`)
	ex.Exec(`SAVEPOINT sp1`)
	ex.Exec(`INSERT INTO t VALUES (2)`)
	ex.Exec(`RELEASE SAVEPOINT sp1`)
	ex.Exec(`COMMIT`)

	// Both rows should be committed (RELEASE doesn't undo anything).
	r, err := ex.Exec(`SELECT id FROM t ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

// ---------------------------------------------------------------------------
// Auto-commit (no explicit transaction)
// ---------------------------------------------------------------------------

func TestTx_AutoCommit(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)

	// Without BEGIN, each statement auto-commits.
	ex.Exec(`INSERT INTO t VALUES (1)`)
	ex.Exec(`INSERT INTO t VALUES (2)`)

	r, err := ex.Exec(`SELECT id FROM t ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

func TestTx_RollbackWithoutBegin(t *testing.T) {
	ex := newTestExecutor(t)

	// ROLLBACK without BEGIN should succeed (PostgreSQL behavior).
	r, err := ex.Exec(`ROLLBACK`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "ROLLBACK" {
		t.Fatalf("expected ROLLBACK message, got %q", r.Message)
	}
}

// ---------------------------------------------------------------------------
// TxStatus
// ---------------------------------------------------------------------------

func TestTx_TxStatus(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)

	if ex.TxStatus() != 'I' {
		t.Fatalf("expected idle, got %c", ex.TxStatus())
	}

	ex.Exec(`BEGIN`)
	if ex.TxStatus() != 'T' {
		t.Fatalf("expected in-tx, got %c", ex.TxStatus())
	}

	// Cause error.
	ex.Exec(`SELECT * FROM nonexistent`)
	if ex.TxStatus() != 'E' {
		t.Fatalf("expected failed, got %c", ex.TxStatus())
	}

	ex.Exec(`ROLLBACK`)
	if ex.TxStatus() != 'I' {
		t.Fatalf("expected idle after rollback, got %c", ex.TxStatus())
	}
}
