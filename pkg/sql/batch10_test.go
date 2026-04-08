package sql

import (
	"strings"
	"testing"

	"github.com/gololadb/loladb/pkg/catalog"
	"github.com/gololadb/loladb/pkg/storage"
)

func mustExecR(t *testing.T, ex *Executor, sql string) *Result {
	t.Helper()
	r, err := ex.Exec(sql)
	if err != nil {
		t.Fatalf("mustExecR(%q): %v", sql, err)
	}
	return r
}

// --- ALTER TABLE ... OWNER TO ---

func TestAlterTableOwnerTo(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE ROLE alice")
	mustExec(t, ex, "CREATE TABLE owned_tbl (id INT, name TEXT)")
	r := mustExecR(t, ex, "ALTER TABLE owned_tbl OWNER TO alice")
	if r.Message != "ALTER TABLE" {
		t.Fatalf("expected ALTER TABLE, got %s", r.Message)
	}
}

func TestAlterTableOwnerToNonexistentRole(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE owned_tbl2 (id INT)")
	r := mustExecR(t, ex, "ALTER TABLE owned_tbl2 OWNER TO nobody")
	if r.Message != "ALTER TABLE" {
		t.Fatalf("expected ALTER TABLE, got %s", r.Message)
	}
}

// --- Table Partitioning ---

func TestPartitionByList(t *testing.T) {
	ex := newTestExecutor(t)

	// Create partitioned parent.
	mustExec(t, ex, "CREATE TABLE sales (id INT, region TEXT, amount INT) PARTITION BY LIST (region)")

	// Create child partitions as regular tables.
	mustExec(t, ex, "CREATE TABLE sales_east (id INT, region TEXT, amount INT)")
	mustExec(t, ex, "CREATE TABLE sales_west (id INT, region TEXT, amount INT)")

	// Attach partitions.
	mustExec(t, ex, "ALTER TABLE sales ATTACH PARTITION sales_east FOR VALUES IN ('east')")
	mustExec(t, ex, "ALTER TABLE sales ATTACH PARTITION sales_west FOR VALUES IN ('west')")

	// Insert into parent — should route to correct child.
	mustExec(t, ex, "INSERT INTO sales VALUES (1, 'east', 100)")
	mustExec(t, ex, "INSERT INTO sales VALUES (2, 'west', 200)")
	mustExec(t, ex, "INSERT INTO sales VALUES (3, 'east', 300)")

	// Select from parent — should scan all children.
	r := mustExecR(t, ex, "SELECT id, region, amount FROM sales ORDER BY id")
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
	if r.Rows[0][1].Text != "east" {
		t.Fatalf("expected row 1 region=east, got %s", r.Rows[0][1].Text)
	}
	if r.Rows[1][1].Text != "west" {
		t.Fatalf("expected row 2 region=west, got %s", r.Rows[1][1].Text)
	}

	// Select from child directly.
	r2 := mustExecR(t, ex, "SELECT count(*) FROM sales_east")
	if len(r2.Rows) != 1 || r2.Rows[0][0].I64 != 2 {
		t.Fatalf("expected 2 rows in sales_east, got %v", r2.Rows)
	}
}

func TestPartitionByRange(t *testing.T) {
	ex := newTestExecutor(t)

	mustExec(t, ex, "CREATE TABLE measurements (id INT, ts TEXT, val INT) PARTITION BY RANGE (ts)")
	mustExec(t, ex, "CREATE TABLE meas_2024 (id INT, ts TEXT, val INT)")
	mustExec(t, ex, "CREATE TABLE meas_2025 (id INT, ts TEXT, val INT)")

	mustExec(t, ex, "ALTER TABLE measurements ATTACH PARTITION meas_2024 FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')")
	mustExec(t, ex, "ALTER TABLE measurements ATTACH PARTITION meas_2025 FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')")

	mustExec(t, ex, "INSERT INTO measurements VALUES (1, '2024-06-15', 42)")
	mustExec(t, ex, "INSERT INTO measurements VALUES (2, '2025-03-20', 99)")

	r := mustExecR(t, ex, "SELECT id, val FROM measurements ORDER BY id")
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}

	// Verify routing: meas_2024 should have 1 row.
	r2 := mustExecR(t, ex, "SELECT count(*) FROM meas_2024")
	if r2.Rows[0][0].I64 != 1 {
		t.Fatalf("expected 1 row in meas_2024, got %d", r2.Rows[0][0].I64)
	}
}

func TestPartitionDefaultPartition(t *testing.T) {
	ex := newTestExecutor(t)

	mustExec(t, ex, "CREATE TABLE logs (id INT, level TEXT, msg TEXT) PARTITION BY LIST (level)")
	mustExec(t, ex, "CREATE TABLE logs_error (id INT, level TEXT, msg TEXT)")
	mustExec(t, ex, "CREATE TABLE logs_other (id INT, level TEXT, msg TEXT)")

	mustExec(t, ex, "ALTER TABLE logs ATTACH PARTITION logs_error FOR VALUES IN ('error')")
	mustExec(t, ex, "ALTER TABLE logs ATTACH PARTITION logs_other DEFAULT")

	mustExec(t, ex, "INSERT INTO logs VALUES (1, 'error', 'fail')")
	mustExec(t, ex, "INSERT INTO logs VALUES (2, 'info', 'ok')")
	mustExec(t, ex, "INSERT INTO logs VALUES (3, 'debug', 'trace')")

	r := mustExecR(t, ex, "SELECT count(*) FROM logs_error")
	if r.Rows[0][0].I64 != 1 {
		t.Fatalf("expected 1 row in logs_error, got %d", r.Rows[0][0].I64)
	}

	r2 := mustExecR(t, ex, "SELECT count(*) FROM logs_other")
	if r2.Rows[0][0].I64 != 2 {
		t.Fatalf("expected 2 rows in logs_other, got %d", r2.Rows[0][0].I64)
	}
}

func TestDetachPartition(t *testing.T) {
	ex := newTestExecutor(t)

	mustExec(t, ex, "CREATE TABLE parts (id INT, cat TEXT) PARTITION BY LIST (cat)")
	mustExec(t, ex, "CREATE TABLE parts_a (id INT, cat TEXT)")
	mustExec(t, ex, "ALTER TABLE parts ATTACH PARTITION parts_a FOR VALUES IN ('a')")

	mustExec(t, ex, "INSERT INTO parts VALUES (1, 'a')")

	// Detach.
	mustExec(t, ex, "ALTER TABLE parts DETACH PARTITION parts_a")

	// Insert should now fail (no matching partition).
	_, err := ex.Exec("INSERT INTO parts VALUES (2, 'a')")
	if err == nil || !strings.Contains(err.Error(), "no partition") {
		t.Fatalf("expected 'no partition' error after detach, got %v", err)
	}
}

// --- ALTER TABLE ONLY ---

func TestAlterTableOnly(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE only_tbl (id INT, name TEXT)")
	r := mustExecR(t, ex, "ALTER TABLE ONLY only_tbl ADD COLUMN age INT")
	if r.Message != "ALTER TABLE" {
		t.Fatalf("expected ALTER TABLE, got %s", r.Message)
	}
}

func TestAlterTableOnlyOwnerTo(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE ROLE bob")
	mustExec(t, ex, "CREATE TABLE only_tbl2 (id INT)")
	r := mustExecR(t, ex, "ALTER TABLE ONLY only_tbl2 OWNER TO bob")
	if r.Message != "ALTER TABLE" {
		t.Fatalf("expected ALTER TABLE, got %s", r.Message)
	}
}

// --- Partition persistence across reopen ---

func TestPartitionPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/part_persist.lodb"

	// Session 1: create partitioned table, insert data.
	{
		eng, err := storage.Open(path, 64)
		if err != nil {
			t.Fatal(err)
		}
		cat, err := catalog.New(eng)
		if err != nil {
			t.Fatal(err)
		}
		ex := NewExecutor(cat)

		mustExec(t, ex, "CREATE TABLE payments (id INT, region TEXT, amount INT) PARTITION BY LIST (region)")
		mustExec(t, ex, "CREATE TABLE payments_east (id INT, region TEXT, amount INT)")
		mustExec(t, ex, "CREATE TABLE payments_west (id INT, region TEXT, amount INT)")
		mustExec(t, ex, "ALTER TABLE payments ATTACH PARTITION payments_east FOR VALUES IN ('east')")
		mustExec(t, ex, "ALTER TABLE payments ATTACH PARTITION payments_west FOR VALUES IN ('west')")
		mustExec(t, ex, "INSERT INTO payments VALUES (1, 'east', 100)")
		mustExec(t, ex, "INSERT INTO payments VALUES (2, 'west', 200)")
		mustExec(t, ex, "INSERT INTO payments VALUES (3, 'east', 300)")

		eng.Close()
	}

	// Session 2: reopen and query.
	{
		eng, err := storage.Open(path, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer eng.Close()
		cat, err := catalog.New(eng)
		if err != nil {
			t.Fatal(err)
		}
		ex := NewExecutor(cat)

		r := mustExecR(t, ex, "SELECT id, region, amount FROM payments ORDER BY id")
		if len(r.Rows) != 3 {
			t.Fatalf("expected 3 rows from parent after reopen, got %d", len(r.Rows))
		}
		if r.Rows[0][1].Text != "east" {
			t.Fatalf("expected row 1 region=east, got %s", r.Rows[0][1].Text)
		}
	}
}

// --- CREATE TABLE ... PARTITION OF persistence ---

func TestCreatePartitionOfPersistence(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/partof_persist.lodb"

	// Session 1: create using PARTITION OF syntax.
	{
		eng, err := storage.Open(path, 64)
		if err != nil {
			t.Fatal(err)
		}
		cat, err := catalog.New(eng)
		if err != nil {
			t.Fatal(err)
		}
		ex := NewExecutor(cat)

		mustExec(t, ex, "CREATE TABLE metrics (id INT, ts TEXT, val INT) PARTITION BY RANGE (ts)")
		mustExec(t, ex, "CREATE TABLE metrics_2024 PARTITION OF metrics FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')")
		mustExec(t, ex, "CREATE TABLE metrics_2025 PARTITION OF metrics FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')")
		mustExec(t, ex, "INSERT INTO metrics VALUES (1, '2024-06-15', 42)")
		mustExec(t, ex, "INSERT INTO metrics VALUES (2, '2025-03-20', 99)")

		eng.Close()
	}

	// Session 2: reopen and verify.
	{
		eng, err := storage.Open(path, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer eng.Close()
		cat, err := catalog.New(eng)
		if err != nil {
			t.Fatal(err)
		}
		ex := NewExecutor(cat)

		r := mustExecR(t, ex, "SELECT id, val FROM metrics ORDER BY id")
		if len(r.Rows) != 2 {
			t.Fatalf("expected 2 rows after reopen, got %d", len(r.Rows))
		}
	}
}

// --- INSERT into partitioned parent with no children ---

func TestInsertPartitionedNoChildren(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE empty_part (id INT, key TEXT) PARTITION BY LIST (key)")
	_, err := ex.Exec("INSERT INTO empty_part VALUES (1, 'x')")
	if err == nil || !strings.Contains(err.Error(), "no partition") {
		t.Fatalf("expected 'no partition' error, got %v", err)
	}
}
