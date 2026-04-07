package sql

import (
	"testing"

	"github.com/gololadb/loladb/pkg/tuple"
)

// ---------------------------------------------------------------------------
// :: shorthand type casting
// ---------------------------------------------------------------------------

func TestCast_TextToInteger(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT '42'::integer`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].I32 != 42 {
		t.Fatalf("expected 42, got %v", r.Rows[0][0])
	}
}

func TestCast_TextToBigint(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT '9999999999'::bigint`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].I64 != 9999999999 {
		t.Fatalf("expected 9999999999, got %d", r.Rows[0][0].I64)
	}
}

func TestCast_TextToFloat(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT '3.14'::float`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].F64 < 3.13 || r.Rows[0][0].F64 > 3.15 {
		t.Fatalf("expected ~3.14, got %f", r.Rows[0][0].F64)
	}
}

func TestCast_TextToBoolean(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT 'true'::boolean`)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Rows[0][0].Bool {
		t.Fatal("expected true")
	}
}

func TestCast_IntegerToText(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)
	ex.Exec(`INSERT INTO t VALUES (42)`)

	r, err := ex.Exec(`SELECT id::text FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Text != "42" {
		t.Fatalf("expected '42', got %q", r.Rows[0][0].Text)
	}
}

func TestCast_FloatToInteger(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT 3.7::integer`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].I32 != 3 {
		t.Fatalf("expected 3 (truncated), got %d", r.Rows[0][0].I32)
	}
}

func TestCast_NullCast(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT NULL::integer`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Type != tuple.TypeNull {
		t.Fatalf("expected NULL, got %v", r.Rows[0][0])
	}
}

func TestCast_TextToDate(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT '2024-01-15'::date`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Type != tuple.TypeDate {
		t.Fatalf("expected date type, got %d", r.Rows[0][0].Type)
	}
}

func TestCast_TextToTimestamp(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT '2024-01-15 10:30:00'::timestamp`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Type != tuple.TypeTimestamp {
		t.Fatalf("expected timestamp type, got %d", r.Rows[0][0].Type)
	}
}

func TestCast_TextToJSON(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT '{"key": "value"}'::json`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Type != tuple.TypeJSON {
		t.Fatalf("expected json type, got %d", r.Rows[0][0].Type)
	}
}

func TestCast_InvalidCast(t *testing.T) {
	ex := newTestExecutor(t)
	_, err := ex.Exec(`SELECT 'not_a_number'::integer`)
	if err == nil {
		t.Fatal("expected error for invalid cast")
	}
}

func TestCast_InWhereClause(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT, val TEXT)`)
	ex.Exec(`INSERT INTO t VALUES (1, '100'), (2, '200')`)

	r, err := ex.Exec(`SELECT id FROM t WHERE val::integer > 150`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].I64 != 2 {
		t.Fatalf("expected id=2, got %v", r.Rows)
	}
}

func TestCast_ChainedCast(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT 42::text::integer`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].I32 != 42 {
		t.Fatalf("expected 42, got %v", r.Rows[0][0])
	}
}

func TestCast_BoolToInt(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`SELECT true::integer`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].I32 != 1 {
		t.Fatalf("expected 1, got %d", r.Rows[0][0].I32)
	}
}
