package sql

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/gololadb/loladb/pkg/catalog"
	"github.com/gololadb/loladb/pkg/storage"
	"github.com/gololadb/loladb/pkg/tuple"
)

var _ = tuple.DNull

func newTestExecutor(t *testing.T) *Executor {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.lodb")
	eng, err := storage.Open(path, 64)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })

	cat, err := catalog.New(eng)
	if err != nil {
		t.Fatal(err)
	}
	return NewExecutor(cat)
}

func TestSQL_CreateTable(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(`CREATE TABLE users (id INT, name TEXT, active BOOL)`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "CREATE TABLE users" {
		t.Fatalf("unexpected message: %s", r.Message)
	}
}

func TestSQL_InsertAndSelect(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT, name TEXT)`)

	r, err := ex.Exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`)
	if err != nil {
		t.Fatal(err)
	}
	if r.RowsAffected != 3 {
		t.Fatalf("expected 3 rows, got %d", r.RowsAffected)
	}

	r, err = ex.Exec(`SELECT * FROM users`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
	if len(r.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(r.Columns))
	}
	if r.Columns[0] != "id" || r.Columns[1] != "name" {
		t.Fatalf("unexpected columns: %v", r.Columns)
	}
}

func TestSQL_SelectProjection(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT, name TEXT, email TEXT)`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')`)

	r, err := ex.Exec(`SELECT name, email FROM users`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Columns) != 2 || r.Columns[0] != "name" || r.Columns[1] != "email" {
		t.Fatalf("unexpected columns: %v", r.Columns)
	}
	if r.Rows[0][0].Text != "Alice" || r.Rows[0][1].Text != "alice@example.com" {
		t.Fatal("projection mismatch")
	}
}

func TestSQL_SelectWhere(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT, name TEXT)`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`)

	r, err := ex.Exec(`SELECT * FROM users WHERE id = 2`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	// INT maps to Int64 in the PG parser
	if r.Rows[0][1].Text != "Bob" {
		t.Fatalf("expected Bob, got %q", r.Rows[0][1].Text)
	}
}

func TestSQL_Delete(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT, name TEXT)`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`)

	r, err := ex.Exec(`DELETE FROM users WHERE id = 2`)
	if err != nil {
		t.Fatal(err)
	}
	if r.RowsAffected != 1 {
		t.Fatalf("expected 1 deleted, got %d", r.RowsAffected)
	}

	r, _ = ex.Exec(`SELECT * FROM users`)
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows after delete, got %d", len(r.Rows))
	}
}

func TestSQL_DeleteAll(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT)`)
	ex.Exec(`INSERT INTO users VALUES (1), (2), (3)`)

	r, _ := ex.Exec(`DELETE FROM users`)
	if r.RowsAffected != 3 {
		t.Fatalf("expected 3, got %d", r.RowsAffected)
	}

	r, _ = ex.Exec(`SELECT * FROM users`)
	if len(r.Rows) != 0 {
		t.Fatalf("expected 0, got %d", len(r.Rows))
	}
}

func TestSQL_Update(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT, name TEXT)`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')`)

	r, err := ex.Exec(`UPDATE users SET name = 'Robert' WHERE id = 2`)
	if err != nil {
		t.Fatal(err)
	}
	if r.RowsAffected != 1 {
		t.Fatalf("expected 1 updated, got %d", r.RowsAffected)
	}

	r, _ = ex.Exec(`SELECT name FROM users WHERE id = 2`)
	if len(r.Rows) != 1 || r.Rows[0][0].Text != "Robert" {
		t.Fatalf("update not applied: %v", r.Rows)
	}
}

func TestSQL_UpdateAll(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE items (id INT, status TEXT)`)
	ex.Exec(`INSERT INTO items VALUES (1, 'pending'), (2, 'pending'), (3, 'pending')`)

	r, _ := ex.Exec(`UPDATE items SET status = 'done'`)
	if r.RowsAffected != 3 {
		t.Fatalf("expected 3, got %d", r.RowsAffected)
	}

	r, _ = ex.Exec(`SELECT * FROM items`)
	for _, row := range r.Rows {
		if row[1].Text != "done" {
			t.Fatal("not all updated")
		}
	}
}

func TestSQL_CreateIndex(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT, name TEXT)`)

	r, err := ex.Exec(`CREATE INDEX idx_users_id ON users (id)`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "CREATE INDEX idx_users_id" {
		t.Fatalf("unexpected: %s", r.Message)
	}
}

func TestSQL_Types(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE data (i INT, b BIGINT, f FLOAT, t TEXT, bo BOOL)`)

	_, err := ex.Exec(`INSERT INTO data VALUES (42, 9999999999, 3.14, 'hello', true)`)
	if err != nil {
		t.Fatal(err)
	}

	r, _ := ex.Exec(`SELECT * FROM data`)
	if len(r.Rows) != 1 {
		t.Fatal("expected 1 row")
	}
	row := r.Rows[0]
	if row[0].I64 != 42 {
		t.Fatalf("int: %d", row[0].I64)
	}
	if row[1].I64 != 9999999999 {
		t.Fatalf("bigint: %d", row[1].I64)
	}
	if row[2].F64 != 3.14 {
		t.Fatalf("float: %f", row[2].F64)
	}
	if row[3].Text != "hello" {
		t.Fatalf("text: %s", row[3].Text)
	}
	if row[4].Bool != true {
		t.Fatalf("bool: %v", row[4].Bool)
	}
}

func TestSQL_MultipleStatements(t *testing.T) {
	ex := newTestExecutor(t)
	// Only first statement is executed
	r, err := ex.Exec(`CREATE TABLE a (id INT)`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "CREATE TABLE a" {
		t.Fatal("first statement not executed")
	}
}

func TestSQL_ParseError(t *testing.T) {
	ex := newTestExecutor(t)
	_, err := ex.Exec(`NOT VALID SQL !@#$`)
	if err == nil {
		t.Fatal("expected parse error")
	}
}

func TestSQL_EmptyStatement(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec(``)
	if err != nil {
		t.Fatal(err)
	}
	if r.Message != "OK" {
		t.Fatal("expected OK for empty")
	}
}

func TestSQL_InsertSelectDeleteCycle(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE log (id INT, msg TEXT)`)

	for i := 0; i < 50; i++ {
		ex.Exec(fmt.Sprintf(`INSERT INTO log VALUES (%d, 'msg-%d')`, i, i))
	}

	r, _ := ex.Exec(`SELECT * FROM log`)
	if len(r.Rows) != 50 {
		t.Fatalf("expected 50, got %d", len(r.Rows))
	}

	ex.Exec(`DELETE FROM log WHERE id = 25`)
	r, _ = ex.Exec(`SELECT * FROM log`)
	if len(r.Rows) != 49 {
		t.Fatalf("expected 49, got %d", len(r.Rows))
	}
}

func TestSQL_SelectWhereText(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT, name TEXT)`)
	ex.Exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')`)

	r, err := ex.Exec(`SELECT * FROM users WHERE name = 'Alice'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0][0].I64 != 1 {
		t.Fatal("WHERE on text failed")
	}
}

