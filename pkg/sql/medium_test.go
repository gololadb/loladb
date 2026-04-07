package sql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gololadb/loladb/pkg/tuple"
)

// ---------------------------------------------------------------------------
// Generated columns (STORED)
// ---------------------------------------------------------------------------

func TestGeneratedColumnStored(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE products (
		name TEXT,
		price INT,
		qty INT,
		total INT GENERATED ALWAYS AS (price * qty) STORED
	)`)

	mustExec(t, ex, `INSERT INTO products (name, price, qty) VALUES ('Widget', 10, 5)`)
	mustExec(t, ex, `INSERT INTO products (name, price, qty) VALUES ('Gadget', 25, 3)`)

	r, err := ex.Exec(`SELECT name, total FROM products ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	// Gadget: 25*3=75
	if r.Rows[0][1].I64 != 75 {
		t.Errorf("expected Gadget total=75, got %d", r.Rows[0][1].I64)
	}
	// Widget: 10*5=50
	if r.Rows[1][1].I64 != 50 {
		t.Errorf("expected Widget total=50, got %d", r.Rows[1][1].I64)
	}
}

func TestGeneratedColumnUpdate(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE items (
		a INT,
		b INT,
		c INT GENERATED ALWAYS AS (a + b) STORED
	)`)
	mustExec(t, ex, `INSERT INTO items (a, b) VALUES (3, 7)`)

	r, err := ex.Exec(`SELECT c FROM items`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].I64 != 10 {
		t.Fatalf("expected c=10, got %d", r.Rows[0][0].I64)
	}

	// Update a source column — generated column should recompute.
	mustExec(t, ex, `UPDATE items SET a = 100`)
	r, err = ex.Exec(`SELECT c FROM items`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].I64 != 107 {
		t.Fatalf("expected c=107 after update, got %d", r.Rows[0][0].I64)
	}
}

func TestGeneratedColumnRejectInsert(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE gen_reject (
		a INT,
		b INT GENERATED ALWAYS AS (a * 2) STORED
	)`)

	// Explicit value for generated column should fail.
	_, err := ex.Exec(`INSERT INTO gen_reject (a, b) VALUES (5, 99)`)
	if err == nil {
		t.Fatal("expected error inserting into generated column")
	}
	if !strings.Contains(err.Error(), "generated column") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGeneratedColumnRejectUpdate(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE gen_upd (
		a INT,
		b INT GENERATED ALWAYS AS (a + 1) STORED
	)`)
	mustExec(t, ex, `INSERT INTO gen_upd (a) VALUES (10)`)

	// Explicit update of generated column should fail.
	_, err := ex.Exec(`UPDATE gen_upd SET b = 99`)
	if err == nil {
		t.Fatal("expected error updating generated column")
	}
	if !strings.Contains(err.Error(), "generated column") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// System information functions (expanded set)
// ---------------------------------------------------------------------------

func TestSystemFunctions(t *testing.T) {
	ex := newTestExecutor(t)

	tests := []struct {
		sql    string
		check  func(r *Result) error
	}{
		{
			sql: `SELECT current_database()`,
			check: func(r *Result) error {
				if r.Rows[0][0].Text != "loladb" {
					return errorf("expected 'loladb', got %q", r.Rows[0][0].Text)
				}
				return nil
			},
		},
		{
			sql: `SELECT version()`,
			check: func(r *Result) error {
				if !strings.Contains(r.Rows[0][0].Text, "LolaDB") {
					return errorf("expected version containing 'LolaDB', got %q", r.Rows[0][0].Text)
				}
				return nil
			},
		},
		{
			sql: `SELECT pg_typeof(42)`,
			check: func(r *Result) error {
				// Literal integers are parsed as int64 (bigint).
				if r.Rows[0][0].Text != "bigint" {
					return errorf("expected 'bigint', got %q", r.Rows[0][0].Text)
				}
				return nil
			},
		},
		{
			sql: `SELECT pg_typeof('hello')`,
			check: func(r *Result) error {
				if r.Rows[0][0].Text != "text" {
					return errorf("expected 'text', got %q", r.Rows[0][0].Text)
				}
				return nil
			},
		},
		{
			sql: `SELECT pg_typeof(true)`,
			check: func(r *Result) error {
				if r.Rows[0][0].Text != "boolean" {
					return errorf("expected 'boolean', got %q", r.Rows[0][0].Text)
				}
				return nil
			},
		},
		{
			sql: `SELECT pg_table_size(0)`,
			check: func(r *Result) error {
				if r.Rows[0][0].I64 != 0 {
					return errorf("expected 0, got %d", r.Rows[0][0].I64)
				}
				return nil
			},
		},
		{
			sql: `SELECT pg_table_is_visible(0)`,
			check: func(r *Result) error {
				if r.Rows[0][0].Bool != true {
					return errorf("expected true")
				}
				return nil
			},
		},
		{
			sql: `SELECT has_table_privilege('public', 'users', 'SELECT')`,
			check: func(r *Result) error {
				if r.Rows[0][0].Bool != true {
					return errorf("expected true")
				}
				return nil
			},
		},
		{
			sql: `SELECT pg_backend_pid()`,
			check: func(r *Result) error {
				if r.Rows[0][0].I32 != 1 {
					return errorf("expected 1, got %d", r.Rows[0][0].I32)
				}
				return nil
			},
		},
		{
			sql: `SELECT obj_description(0)`,
			check: func(r *Result) error {
				if r.Rows[0][0].Type != tuple.TypeNull {
					return errorf("expected NULL, got type %v", r.Rows[0][0].Type)
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		r, err := ex.Exec(tt.sql)
		if err != nil {
			t.Fatalf("%s: %v", tt.sql, err)
		}
		if len(r.Rows) != 1 {
			t.Fatalf("%s: expected 1 row, got %d", tt.sql, len(r.Rows))
		}
		if err := tt.check(r); err != nil {
			t.Fatalf("%s: %v", tt.sql, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Transaction isolation levels (SHOW)
// ---------------------------------------------------------------------------

func TestShowTransactionIsolation(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`SHOW transaction_isolation`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Text != "read committed" {
		t.Fatalf("expected 'read committed', got %v", r.Rows)
	}

	r, err = ex.Exec(`SHOW default_transaction_isolation`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Text != "read committed" {
		t.Fatalf("expected 'read committed', got %v", r.Rows)
	}
}

func TestShowServerVersion(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`SHOW server_version`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || !strings.Contains(r.Rows[0][0].Text, "LolaDB") {
		t.Fatalf("expected version containing 'LolaDB', got %v", r.Rows)
	}
}

func TestSetTransactionIsolation(t *testing.T) {
	ex := newTestExecutor(t)

	// SET TRANSACTION ISOLATION LEVEL should be silently accepted.
	_, err := ex.Exec(`SET TRANSACTION ISOLATION LEVEL SERIALIZABLE`)
	if err != nil {
		t.Fatalf("SET TRANSACTION ISOLATION LEVEL should be accepted: %v", err)
	}

	_, err = ex.Exec(`SET TRANSACTION ISOLATION LEVEL READ COMMITTED`)
	if err != nil {
		t.Fatalf("SET TRANSACTION ISOLATION LEVEL should be accepted: %v", err)
	}
}

// ---------------------------------------------------------------------------
// FULL OUTER JOIN
// ---------------------------------------------------------------------------

func TestFullOuterJoin(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE left_t (id INT, val TEXT)`)
	mustExec(t, ex, `CREATE TABLE right_t (id INT, val TEXT)`)

	mustExec(t, ex, `INSERT INTO left_t VALUES (1, 'a')`)
	mustExec(t, ex, `INSERT INTO left_t VALUES (2, 'b')`)
	mustExec(t, ex, `INSERT INTO left_t VALUES (3, 'c')`)

	mustExec(t, ex, `INSERT INTO right_t VALUES (2, 'x')`)
	mustExec(t, ex, `INSERT INTO right_t VALUES (3, 'y')`)
	mustExec(t, ex, `INSERT INTO right_t VALUES (4, 'z')`)

	r, err := ex.Exec(`SELECT left_t.id, left_t.val, right_t.id, right_t.val
		FROM left_t FULL OUTER JOIN right_t ON left_t.id = right_t.id
		ORDER BY COALESCE(left_t.id, right_t.id)`)
	if err != nil {
		t.Fatal(err)
	}

	// Expected rows:
	// 1, 'a', NULL, NULL  (left only)
	// 2, 'b', 2, 'x'     (match)
	// 3, 'c', 3, 'y'     (match)
	// NULL, NULL, 4, 'z'  (right only)
	if len(r.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(r.Rows))
	}

	// Row 0: left_t.id=1, right_t.id=NULL
	if intVal(r.Rows[0][0]) != 1 {
		t.Errorf("row 0: expected left_t.id=1, got %v", r.Rows[0][0])
	}
	if r.Rows[0][2].Type != tuple.TypeNull {
		t.Errorf("row 0: expected right_t.id=NULL, got %v", r.Rows[0][2])
	}

	// Row 1: match on id=2
	if intVal(r.Rows[1][0]) != 2 || intVal(r.Rows[1][2]) != 2 {
		t.Errorf("row 1: expected id=2 match, got left=%v right=%v", r.Rows[1][0], r.Rows[1][2])
	}

	// Row 2: match on id=3
	if intVal(r.Rows[2][0]) != 3 || intVal(r.Rows[2][2]) != 3 {
		t.Errorf("row 2: expected id=3 match, got left=%v right=%v", r.Rows[2][0], r.Rows[2][2])
	}

	// Row 3: left_t.id=NULL, right_t.id=4
	if r.Rows[3][0].Type != tuple.TypeNull {
		t.Errorf("row 3: expected left_t.id=NULL, got %v", r.Rows[3][0])
	}
	if intVal(r.Rows[3][2]) != 4 {
		t.Errorf("row 3: expected right_t.id=4, got %v", r.Rows[3][2])
	}
}

func TestFullOuterJoinNoOverlap(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE fa (id INT)`)
	mustExec(t, ex, `CREATE TABLE fb (id INT)`)
	mustExec(t, ex, `INSERT INTO fa VALUES (1)`)
	mustExec(t, ex, `INSERT INTO fb VALUES (2)`)

	r, err := ex.Exec(`SELECT fa.id, fb.id FROM fa FULL OUTER JOIN fb ON fa.id = fb.id`)
	if err != nil {
		t.Fatal(err)
	}
	// No overlap: 2 rows — (1, NULL) and (NULL, 2)
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

func TestFullOuterJoinAllMatch(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE fc (id INT)`)
	mustExec(t, ex, `CREATE TABLE fd (id INT)`)
	mustExec(t, ex, `INSERT INTO fc VALUES (1)`)
	mustExec(t, ex, `INSERT INTO fd VALUES (1)`)

	r, err := ex.Exec(`SELECT fc.id, fd.id FROM fc FULL OUTER JOIN fd ON fc.id = fd.id`)
	if err != nil {
		t.Fatal(err)
	}
	// All match: 1 row — (1, 1)
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if intVal(r.Rows[0][0]) != 1 || intVal(r.Rows[0][1]) != 1 {
		t.Errorf("expected (1, 1), got (%v, %v)", r.Rows[0][0], r.Rows[0][1])
	}
}

// ---------------------------------------------------------------------------
// SHOW session variables
// ---------------------------------------------------------------------------

func TestShowSessionVariables(t *testing.T) {
	ex := newTestExecutor(t)

	tests := []struct {
		sql      string
		expected string
	}{
		{"SHOW server_encoding", "UTF8"},
		{"SHOW client_encoding", "UTF8"},
		{"SHOW standard_conforming_strings", "on"},
		{"SHOW is_superuser", "on"},
	}

	for _, tt := range tests {
		r, err := ex.Exec(tt.sql)
		if err != nil {
			t.Fatalf("%s: %v", tt.sql, err)
		}
		if len(r.Rows) != 1 || r.Rows[0][0].Text != tt.expected {
			t.Fatalf("%s: expected %q, got %v", tt.sql, tt.expected, r.Rows)
		}
	}
}

// errorf is a helper that returns a formatted error.
func errorf(format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}

// intVal extracts an integer value from a datum regardless of int32/int64 type.
func intVal(d tuple.Datum) int64 {
	switch d.Type {
	case tuple.TypeInt32:
		return int64(d.I32)
	case tuple.TypeInt64:
		return d.I64
	default:
		return d.I64
	}
}
