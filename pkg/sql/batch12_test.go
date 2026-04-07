package sql

import (
	"fmt"
	"strings"
	"testing"
)

// --- CREATE TABLE with INHERITS ---

func TestInherits(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE cities (name TEXT, population INT)")
	mustExec(t, ex, "CREATE TABLE capitals (state TEXT) INHERITS (cities)")

	// Insert into parent.
	mustExec(t, ex, "INSERT INTO cities VALUES ('Springfield', 50000)")
	// Insert into child (has parent columns + own columns).
	mustExec(t, ex, "INSERT INTO capitals VALUES ('Sacramento', 500000, 'California')")
	mustExec(t, ex, "INSERT INTO capitals VALUES ('Austin', 1000000, 'Texas')")

	// Select from parent should include parent rows + child rows (truncated to parent width).
	r, err := ex.Exec("SELECT name, population FROM cities ORDER BY name")
	if err != nil {
		t.Fatalf("SELECT from parent failed: %v", err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows (1 parent + 2 child), got %d", len(r.Rows))
	}

	// Select from child should only return child rows.
	r2, err := ex.Exec("SELECT name, state FROM capitals ORDER BY name")
	if err != nil {
		t.Fatalf("SELECT from child failed: %v", err)
	}
	if len(r2.Rows) != 2 {
		t.Fatalf("expected 2 rows from child, got %d", len(r2.Rows))
	}
}

func TestInheritsColumnInheritance(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE base_tbl (id INT, name TEXT NOT NULL)")
	mustExec(t, ex, "CREATE TABLE ext_tbl (extra TEXT) INHERITS (base_tbl)")

	// Child should have id, name, extra columns.
	mustExec(t, ex, "INSERT INTO ext_tbl VALUES (1, 'test', 'bonus')")
	r, err := ex.Exec("SELECT id, name, extra FROM ext_tbl")
	if err != nil {
		t.Fatalf("SELECT from child failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
}

// --- Event triggers ---

func TestCreateEventTrigger(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE FUNCTION log_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN NULL; END; $$")
	r, err := ex.Exec("CREATE EVENT TRIGGER audit_ddl ON ddl_command_end EXECUTE FUNCTION log_ddl()")
	if err != nil {
		t.Fatalf("CREATE EVENT TRIGGER failed: %v", err)
	}
	if r.Message != "CREATE EVENT TRIGGER" {
		t.Fatalf("expected CREATE EVENT TRIGGER, got %s", r.Message)
	}
	// Verify stored.
	if ex.Cat.EventTriggers["audit_ddl"] != "ddl_command_end" {
		t.Fatalf("event trigger not stored correctly")
	}
}

// --- EXCLUDE constraints ---

func TestExcludeConstraint(t *testing.T) {
	ex := newTestExecutor(t)
	// Parser accepts bare EXCLUDE keyword in table constraints.
	r, err := ex.Exec("CREATE TABLE reservations (room INT, period TEXT, EXCLUDE)")
	if err != nil {
		t.Fatalf("CREATE TABLE with EXCLUDE failed: %v", err)
	}
	if !strings.Contains(r.Message, "CREATE TABLE") {
		t.Fatalf("expected CREATE TABLE message, got %s", r.Message)
	}
}

// --- SECURITY LABEL ---

func TestSecurityLabel(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE sec_tbl (id INT)")
	r, err := ex.Exec("SECURITY LABEL FOR selinux ON TABLE sec_tbl IS 'system_u:object_r:sepgsql_table_t:s0'")
	if err != nil {
		t.Fatalf("SECURITY LABEL failed: %v", err)
	}
	if r.Message != "SECURITY LABEL" {
		t.Fatalf("expected SECURITY LABEL, got %s", r.Message)
	}
	// Verify stored.
	if ex.Cat.Comments["seclabel:sec_tbl"] != "system_u:object_r:sepgsql_table_t:s0" {
		t.Fatalf("security label not stored correctly, got %q", ex.Cat.Comments["seclabel:sec_tbl"])
	}
}

// --- PREPARE TRANSACTION ---

func TestPrepareTransaction(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE prep_tbl (id INT)")
	mustExec(t, ex, "BEGIN")
	mustExec(t, ex, "INSERT INTO prep_tbl VALUES (1)")
	r, err := ex.Exec("PREPARE TRANSACTION 'my_txn'")
	if err != nil {
		t.Fatalf("PREPARE TRANSACTION failed: %v", err)
	}
	if r.Message != "PREPARE TRANSACTION" {
		t.Fatalf("expected PREPARE TRANSACTION, got %s", r.Message)
	}
	// Data should be committed (treated as COMMIT).
	r2, err := ex.Exec("SELECT count(*) FROM prep_tbl")
	if err != nil {
		t.Fatalf("SELECT after PREPARE TRANSACTION failed: %v", err)
	}
	if r2.Rows[0][0].I64 != 1 {
		t.Fatalf("expected 1 row after PREPARE TRANSACTION, got %d", r2.Rows[0][0].I64)
	}
}

// --- Multiple INHERITS parents ---

func TestMultipleInherits(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE person (name TEXT)")
	mustExec(t, ex, "CREATE TABLE employee (company TEXT)")
	mustExec(t, ex, "CREATE TABLE manager (level INT) INHERITS (person, employee)")

	mustExec(t, ex, "INSERT INTO manager VALUES ('Alice', 'Acme', 3)")

	// Select from person should include manager rows.
	r, err := ex.Exec("SELECT name FROM person")
	if err != nil {
		t.Fatalf("SELECT from person failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row from person, got %d", len(r.Rows))
	}

	// Select from employee should also include manager rows.
	r2, err := ex.Exec("SELECT company FROM employee")
	if err != nil {
		t.Fatalf("SELECT from employee failed: %v", err)
	}
	if len(r2.Rows) != 1 {
		t.Fatalf("expected 1 row from employee, got %d", len(r2.Rows))
	}
}

// Suppress unused import warning.
var _ = fmt.Sprintf
