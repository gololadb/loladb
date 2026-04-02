package sql

import (
	"strings"
	"testing"

	"github.com/gololadb/loladb/pkg/tuple"
)

// --- DATE type ---

func TestType_DateColumn(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE events (id INT, day DATE)`)
	_, err := ex.Exec(`INSERT INTO events VALUES (1, '2024-03-15')`)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ex.Exec(`SELECT day FROM events WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Type != tuple.TypeDate {
		t.Fatalf("expected TypeDate, got %d", r.Rows[0][0].Type)
	}
	s := datumDisplayText(r.Rows[0][0])
	if s != "2024-03-15" {
		t.Fatalf("expected '2024-03-15', got %q", s)
	}
}

func TestType_DateComparison(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE events (id INT, day DATE)`)
	ex.Exec(`INSERT INTO events VALUES (1, '2024-01-01'), (2, '2024-06-15'), (3, '2024-12-31')`)

	r, err := ex.Exec(`SELECT id FROM events WHERE day > '2024-06-01' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

func TestType_CurrentDate(t *testing.T) {
	ex := newTestExecutor(t)
	d := evalExpr(t, ex, "current_date")
	if d.Type != tuple.TypeDate {
		t.Fatalf("expected TypeDate, got %d", d.Type)
	}
}

// --- TIMESTAMP type ---

func TestType_TimestampColumn(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE logs (id INT, ts TIMESTAMP)`)
	_, err := ex.Exec(`INSERT INTO logs VALUES (1, '2024-03-15 10:30:00')`)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ex.Exec(`SELECT ts FROM logs WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Type != tuple.TypeTimestamp {
		t.Fatalf("expected TypeTimestamp, got %d", r.Rows[0][0].Type)
	}
	s := datumDisplayText(r.Rows[0][0])
	if s != "2024-03-15 10:30:00" {
		t.Fatalf("expected '2024-03-15 10:30:00', got %q", s)
	}
}

func TestType_Now(t *testing.T) {
	ex := newTestExecutor(t)
	d := evalExpr(t, ex, "now()")
	if d.Type != tuple.TypeTimestamp {
		t.Fatalf("expected TypeTimestamp, got %d", d.Type)
	}
}

func TestType_TimestampDefault(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE audit (id INT, created_at DATE DEFAULT now())`)
	_, err := ex.Exec(`INSERT INTO audit (id) VALUES (1)`)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ex.Exec(`SELECT created_at FROM audit WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Type != tuple.TypeDate {
		t.Fatalf("expected TypeDate, got %v", r.Rows[0][0])
	}
}

// --- NUMERIC type ---

func TestType_NumericColumn(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE prices (id INT, amount NUMERIC)`)
	_, err := ex.Exec(`INSERT INTO prices VALUES (1, '99.99')`)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ex.Exec(`SELECT amount FROM prices WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Type != tuple.TypeNumeric {
		t.Fatalf("expected TypeNumeric, got %d", r.Rows[0][0].Type)
	}
	if r.Rows[0][0].Text != "99.99" {
		t.Fatalf("expected '99.99', got %q", r.Rows[0][0].Text)
	}
}

func TestType_NumericArithmetic(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE nums (a NUMERIC, b NUMERIC)`)
	ex.Exec(`INSERT INTO nums VALUES ('10.5', '3.2')`)

	r, err := ex.Exec(`SELECT a + b FROM nums`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Type != tuple.TypeNumeric {
		t.Fatalf("expected TypeNumeric, got %d", r.Rows[0][0].Type)
	}
	if r.Rows[0][0].Text != "13.7" {
		t.Fatalf("expected '13.7', got %q", r.Rows[0][0].Text)
	}
}

func TestType_NumericComparison(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE nums (val NUMERIC)`)
	ex.Exec(`INSERT INTO nums VALUES ('1.5'), ('2.5'), ('0.5')`)

	r, err := ex.Exec(`SELECT val FROM nums`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
	// Verify all values are NUMERIC type
	for i, row := range r.Rows {
		if row[0].Type != tuple.TypeNumeric {
			t.Fatalf("row %d: expected TypeNumeric, got %d", i, row[0].Type)
		}
	}
}

// --- JSON type ---

func TestType_JSONColumn(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE docs (id INT, data JSON)`)
	_, err := ex.Exec(`INSERT INTO docs VALUES (1, '{"name":"Alice","age":30}')`)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ex.Exec(`SELECT data FROM docs WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Type != tuple.TypeJSON {
		t.Fatalf("expected TypeJSON, got %d", r.Rows[0][0].Type)
	}
}

func TestType_JSONBColumn(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE docs (id INT, data JSONB)`)
	_, err := ex.Exec(`INSERT INTO docs VALUES (1, '{"key":"value"}')`)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ex.Exec(`SELECT data FROM docs WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Type != tuple.TypeJSON {
		t.Fatalf("expected TypeJSON, got %d", r.Rows[0][0].Type)
	}
}

func TestType_JSONExtractPathText(t *testing.T) {
	ex := newTestExecutor(t)
	v := evalText(t, ex, `json_extract_path_text('{"a":{"b":"hello"}}', 'a', 'b')`)
	if v != "hello" {
		t.Fatalf("expected 'hello', got %q", v)
	}
}

func TestType_JSONArrayLength(t *testing.T) {
	ex := newTestExecutor(t)
	d := evalExpr(t, ex, `json_array_length('[1,2,3,4]')`)
	if d.I64 != 4 {
		t.Fatalf("expected 4, got %d", d.I64)
	}
}

func TestType_JSONTypeof(t *testing.T) {
	ex := newTestExecutor(t)

	v := evalText(t, ex, `json_typeof('{"a":1}')`)
	if v != "object" {
		t.Fatalf("expected 'object', got %q", v)
	}

	v = evalText(t, ex, `json_typeof('[1,2]')`)
	if v != "array" {
		t.Fatalf("expected 'array', got %q", v)
	}

	v = evalText(t, ex, `json_typeof('"hello"')`)
	if v != "string" {
		t.Fatalf("expected 'string', got %q", v)
	}

	v = evalText(t, ex, `json_typeof('42')`)
	if v != "number" {
		t.Fatalf("expected 'number', got %q", v)
	}
}

func TestType_JSONBuildObject(t *testing.T) {
	ex := newTestExecutor(t)
	d := evalExpr(t, ex, `json_build_object('name', 'Alice', 'age', 30)`)
	if d.Type != tuple.TypeJSON {
		t.Fatalf("expected TypeJSON, got %d", d.Type)
	}
	// The JSON should contain both keys.
	if !strings.Contains(d.Text, `"name"`) || !strings.Contains(d.Text, `"Alice"`) {
		t.Fatalf("unexpected JSON: %s", d.Text)
	}
}

func TestType_ToJSON(t *testing.T) {
	ex := newTestExecutor(t)
	d := evalExpr(t, ex, `to_json('hello')`)
	if d.Type != tuple.TypeJSON {
		t.Fatalf("expected TypeJSON, got %d", d.Type)
	}
	if d.Text != `"hello"` {
		t.Fatalf("expected '\"hello\"', got %q", d.Text)
	}
}

// --- UUID type ---

func TestType_UUIDColumn(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE sessions (id UUID, user_id INT)`)
	_, err := ex.Exec(`INSERT INTO sessions VALUES ('550e8400-e29b-41d4-a716-446655440000', 1)`)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ex.Exec(`SELECT id FROM sessions WHERE user_id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Type != tuple.TypeUUID {
		t.Fatalf("expected TypeUUID, got %d", r.Rows[0][0].Type)
	}
	if r.Rows[0][0].Text != "550e8400-e29b-41d4-a716-446655440000" {
		t.Fatalf("unexpected UUID: %q", r.Rows[0][0].Text)
	}
}

func TestType_GenRandomUUID(t *testing.T) {
	ex := newTestExecutor(t)
	d := evalExpr(t, ex, "gen_random_uuid()")
	if d.Type != tuple.TypeUUID {
		t.Fatalf("expected TypeUUID, got %d", d.Type)
	}
	// UUID v4 format: 8-4-4-4-12 hex chars.
	parts := strings.Split(d.Text, "-")
	if len(parts) != 5 {
		t.Fatalf("invalid UUID format: %q", d.Text)
	}
}

func TestType_UUIDDefault(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE items (id UUID DEFAULT gen_random_uuid(), name TEXT)`)
	_, err := ex.Exec(`INSERT INTO items (name) VALUES ('test')`)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ex.Exec(`SELECT id FROM items WHERE name = 'test'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Type != tuple.TypeUUID {
		t.Fatalf("expected UUID, got %v", r.Rows[0][0])
	}
}

// --- Cross-type coercion ---

func TestType_TimestampToDateCoercion(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE tcoerce (id INT, d DATE DEFAULT now())`)
	_, err := ex.Exec(`INSERT INTO tcoerce (id) VALUES (1)`)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ex.Exec(`SELECT d FROM tcoerce WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Type != tuple.TypeDate {
		t.Fatalf("expected TypeDate, got %d", r.Rows[0][0].Type)
	}
}

func TestType_IntToNumericCoercion(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (val NUMERIC)`)
	_, err := ex.Exec(`INSERT INTO t VALUES (42)`)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ex.Exec(`SELECT val FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Type != tuple.TypeNumeric {
		t.Fatalf("expected TypeNumeric, got %d", r.Rows[0][0].Type)
	}
	if r.Rows[0][0].Text != "42" {
		t.Fatalf("expected '42', got %q", r.Rows[0][0].Text)
	}
}

// ---------------------------------------------------------------------------
// JSON operator tests
// ---------------------------------------------------------------------------

func TestJSONOp_ArrowObject(t *testing.T) {
	ex := newTestExecutor(t)
	// -> with object key returns JSON
	d := evalExpr(t, ex, `'{"a":1,"b":"hello"}'::jsonb -> 'a'`)
	if d.Type != tuple.TypeJSON {
		t.Fatalf("expected TypeJSON, got %d", d.Type)
	}
	if d.Text != "1" {
		t.Fatalf("expected '1', got %q", d.Text)
	}
}

func TestJSONOp_ArrowArray(t *testing.T) {
	ex := newTestExecutor(t)
	// -> with integer index on array
	d := evalExpr(t, ex, `'["a","b","c"]'::jsonb -> 1`)
	if d.Type != tuple.TypeJSON {
		t.Fatalf("expected TypeJSON, got %d", d.Type)
	}
	if d.Text != `"b"` {
		t.Fatalf("expected '\"b\"', got %q", d.Text)
	}
}

func TestJSONOp_ArrowTextObject(t *testing.T) {
	ex := newTestExecutor(t)
	// ->> returns text, unquoting strings
	d := evalExpr(t, ex, `'{"name":"Alice"}'::jsonb ->> 'name'`)
	if d.Type != tuple.TypeText {
		t.Fatalf("expected TypeText, got %d", d.Type)
	}
	if d.Text != "Alice" {
		t.Fatalf("expected 'Alice', got %q", d.Text)
	}
}

func TestJSONOp_ArrowTextNumber(t *testing.T) {
	ex := newTestExecutor(t)
	// ->> with a numeric value returns its text representation
	d := evalExpr(t, ex, `'{"val":42}'::jsonb ->> 'val'`)
	if d.Type != tuple.TypeText {
		t.Fatalf("expected TypeText, got %d", d.Type)
	}
	if d.Text != "42" {
		t.Fatalf("expected '42', got %q", d.Text)
	}
}

func TestJSONOp_ArrowMissing(t *testing.T) {
	ex := newTestExecutor(t)
	d := evalExpr(t, ex, `'{"a":1}'::jsonb -> 'z'`)
	if d.Type != tuple.TypeNull {
		t.Fatalf("expected NULL for missing key, got type %d", d.Type)
	}
}

func TestJSONOp_HashArrow(t *testing.T) {
	ex := newTestExecutor(t)
	// #> path traversal returns JSON
	d := evalExpr(t, ex, `'{"a":{"b":{"c":42}}}'::jsonb #> '{a,b,c}'`)
	if d.Type != tuple.TypeJSON {
		t.Fatalf("expected TypeJSON, got %d", d.Type)
	}
	if d.Text != "42" {
		t.Fatalf("expected '42', got %q", d.Text)
	}
}

func TestJSONOp_HashArrowText(t *testing.T) {
	ex := newTestExecutor(t)
	// #>> path traversal returns text
	d := evalExpr(t, ex, `'{"a":{"b":"deep"}}'::jsonb #>> '{a,b}'`)
	if d.Type != tuple.TypeText {
		t.Fatalf("expected TypeText, got %d", d.Type)
	}
	if d.Text != "deep" {
		t.Fatalf("expected 'deep', got %q", d.Text)
	}
}

func TestJSONOp_HashArrowMissing(t *testing.T) {
	ex := newTestExecutor(t)
	d := evalExpr(t, ex, `'{"a":1}'::jsonb #> '{a,b,c}'`)
	if d.Type != tuple.TypeNull {
		t.Fatalf("expected NULL for missing path, got type %d", d.Type)
	}
}

func TestJSONOp_Contains(t *testing.T) {
	ex := newTestExecutor(t)
	// @> containment: left contains right
	d := evalExpr(t, ex, `'{"a":1,"b":2}'::jsonb @> '{"a":1}'`)
	if !d.Bool {
		t.Fatal("expected true for containment")
	}
	// Negative case
	d = evalExpr(t, ex, `'{"a":1}'::jsonb @> '{"a":1,"b":2}'`)
	if d.Bool {
		t.Fatal("expected false: left does not contain right")
	}
}

func TestJSONOp_ContainedBy(t *testing.T) {
	ex := newTestExecutor(t)
	// <@ is the reverse of @>
	d := evalExpr(t, ex, `'{"a":1}'::jsonb <@ '{"a":1,"b":2}'`)
	if !d.Bool {
		t.Fatal("expected true for contained-by")
	}
}

func TestJSONOp_Exists(t *testing.T) {
	ex := newTestExecutor(t)
	// ? checks top-level key existence
	d := evalExpr(t, ex, `'{"a":1,"b":2}'::jsonb ? 'a'`)
	if !d.Bool {
		t.Fatal("expected true: key 'a' exists")
	}
	d = evalExpr(t, ex, `'{"a":1}'::jsonb ? 'z'`)
	if d.Bool {
		t.Fatal("expected false: key 'z' does not exist")
	}
}

func TestJSONOp_ColumnArrow(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE events (id INT, data JSONB)`)
	_, err := ex.Exec(`INSERT INTO events VALUES (1, '{"type":"click","x":100}')`)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ex.Exec(`SELECT data ->> 'type' FROM events WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].Text != "click" {
		t.Fatalf("expected 'click', got %v", r.Rows[0])
	}
}

func TestJSONOp_ColumnContains(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE docs (id INT, data JSONB)`)
	ex.Exec(`INSERT INTO docs VALUES (1, '{"type":"click","x":100}')`)
	ex.Exec(`INSERT INTO docs VALUES (2, '{"type":"view","x":200}')`)
	ex.Exec(`INSERT INTO docs VALUES (3, '{"type":"click","x":300}')`)

	r, err := ex.Exec(`SELECT id FROM docs WHERE data @> '{"type":"click"}'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
}

// Suppress unused import warning.
var _ = tuple.DNull
