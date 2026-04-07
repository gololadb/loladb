package sql

import (
	"testing"
)

func TestCreateSequenceRegistersInPgClass(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE SEQUENCE my_seq")

	// The sequence should be findable via the catalog.
	rel, err := ex.Cat.FindRelation("my_seq")
	if err != nil {
		t.Fatalf("FindRelation error: %v", err)
	}
	if rel == nil {
		t.Fatal("expected my_seq in pg_class, got nil")
	}
	if rel.OID == 0 {
		t.Fatal("expected non-zero OID for sequence")
	}
}

func TestAlterSequenceOwnerTo(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE ROLE seq_owner")
	mustExec(t, ex, "CREATE SEQUENCE owned_seq")

	// This was the bug: ALTER TABLE ... OWNER TO failed because the
	// sequence had no pg_class entry.
	_, err := ex.Exec("ALTER TABLE owned_seq OWNER TO seq_owner")
	if err != nil {
		t.Fatalf("ALTER TABLE ... OWNER TO on sequence failed: %v", err)
	}
}

func TestSequenceNextvalAfterCreate(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE SEQUENCE counter_seq")

	r := mustExecR(t, ex, "SELECT nextval('counter_seq')")
	if r.Rows[0][0].I64 != 1 {
		t.Fatalf("first nextval expected 1, got %d", r.Rows[0][0].I64)
	}

	r = mustExecR(t, ex, "SELECT nextval('counter_seq')")
	if r.Rows[0][0].I64 != 2 {
		t.Fatalf("second nextval expected 2, got %d", r.Rows[0][0].I64)
	}
}

func TestCreateSequenceIdempotent(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE SEQUENCE idempotent_seq")
	// Creating again should not error (IF NOT EXISTS semantics).
	mustExec(t, ex, "CREATE SEQUENCE idempotent_seq")
}
