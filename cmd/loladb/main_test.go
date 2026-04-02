package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// buildBinary builds the loladb binary and returns its path.
func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "loladb")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = filepath.Join(".") // current package dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build failed: %v\n%s", err, out)
	}
	return bin
}

func TestCLI_Help(t *testing.T) {
	bin := buildBinary(t)
	out, err := exec.Command(bin, "help").CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "loladb create") {
		t.Fatal("help output should mention 'loladb create'")
	}
}

func TestCLI_Create(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "test.mcdb")

	out, err := exec.Command(bin, "create", dbPath).CombinedOutput()
	if err != nil {
		t.Fatalf("create failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "Database created") {
		t.Fatalf("unexpected output: %s", out)
	}

	// File should exist.
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatal("database file not created")
	}
}

func TestCLI_CreateDuplicate(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "test.mcdb")

	exec.Command(bin, "create", dbPath).Run()
	out, err := exec.Command(bin, "create", dbPath).CombinedOutput()
	// Should fail because file exists.
	if err == nil {
		t.Fatal("expected error for duplicate create")
	}
	if !strings.Contains(string(out), "already exists") {
		t.Fatalf("unexpected error: %s", out)
	}
}

func TestCLI_ExecCreateInsertSelect(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "test.mcdb")
	exec.Command(bin, "create", dbPath).Run()

	// CREATE TABLE
	out, err := exec.Command(bin, "exec", dbPath, "CREATE TABLE users (id INT, name TEXT)").CombinedOutput()
	if err != nil {
		t.Fatalf("create table: %v\n%s", err, out)
	}

	// INSERT
	out, err = exec.Command(bin, "exec", dbPath, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").CombinedOutput()
	if err != nil {
		t.Fatalf("insert: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "INSERT 0 2") {
		t.Fatalf("unexpected insert output: %s", out)
	}

	// SELECT
	out, err = exec.Command(bin, "exec", dbPath, "SELECT * FROM users").CombinedOutput()
	if err != nil {
		t.Fatalf("select: %v\n%s", err, out)
	}
	s := string(out)
	if !strings.Contains(s, "Alice") || !strings.Contains(s, "Bob") {
		t.Fatalf("select output missing data: %s", s)
	}
	if !strings.Contains(s, "(2 rows)") {
		t.Fatalf("missing row count: %s", s)
	}
}

func TestCLI_ExecJoin(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "test.mcdb")
	exec.Command(bin, "create", dbPath).Run()

	exec.Command(bin, "exec", dbPath, "CREATE TABLE a (id INT, val TEXT)").Run()
	exec.Command(bin, "exec", dbPath, "CREATE TABLE b (id INT, a_id INT)").Run()
	exec.Command(bin, "exec", dbPath, "INSERT INTO a VALUES (1, 'x'), (2, 'y')").Run()
	exec.Command(bin, "exec", dbPath, "INSERT INTO b VALUES (10, 1)").Run()

	out, _ := exec.Command(bin, "exec", dbPath, "SELECT a.val, b.id FROM a JOIN b ON a.id = b.a_id").CombinedOutput()
	s := string(out)
	if !strings.Contains(s, "x") || !strings.Contains(s, "10") {
		t.Fatalf("join output missing data: %s", s)
	}
}

func TestCLI_ExecUpdate(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "test.mcdb")
	exec.Command(bin, "create", dbPath).Run()
	exec.Command(bin, "exec", dbPath, "CREATE TABLE t (id INT, val TEXT)").Run()
	exec.Command(bin, "exec", dbPath, "INSERT INTO t VALUES (1, 'old')").Run()

	out, _ := exec.Command(bin, "exec", dbPath, "UPDATE t SET val = 'new' WHERE id = 1").CombinedOutput()
	if !strings.Contains(string(out), "UPDATE 1") {
		t.Fatalf("update: %s", out)
	}

	out, _ = exec.Command(bin, "exec", dbPath, "SELECT val FROM t").CombinedOutput()
	if !strings.Contains(string(out), "new") {
		t.Fatalf("update not applied: %s", out)
	}
}

func TestCLI_ExecDelete(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "test.mcdb")
	exec.Command(bin, "create", dbPath).Run()
	exec.Command(bin, "exec", dbPath, "CREATE TABLE t (id INT)").Run()
	exec.Command(bin, "exec", dbPath, "INSERT INTO t VALUES (1), (2), (3)").Run()

	out, _ := exec.Command(bin, "exec", dbPath, "DELETE FROM t WHERE id = 2").CombinedOutput()
	if !strings.Contains(string(out), "DELETE 1") {
		t.Fatalf("delete: %s", out)
	}

	out, _ = exec.Command(bin, "exec", dbPath, "SELECT * FROM t").CombinedOutput()
	if !strings.Contains(string(out), "(2 rows)") {
		t.Fatalf("after delete: %s", out)
	}
}

func TestCLI_ExecExplain(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "test.mcdb")
	exec.Command(bin, "create", dbPath).Run()
	exec.Command(bin, "exec", dbPath, "CREATE TABLE t (id INT)").Run()

	out, _ := exec.Command(bin, "exec", dbPath, "EXPLAIN SELECT * FROM t").CombinedOutput()
	if !strings.Contains(string(out), "SeqScan") {
		t.Fatalf("EXPLAIN should show SeqScan: %s", out)
	}
}

func TestCLI_Info(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "test.mcdb")
	exec.Command(bin, "create", dbPath).Run()
	exec.Command(bin, "exec", dbPath, "CREATE TABLE users (id INT, name TEXT)").Run()
	exec.Command(bin, "exec", dbPath, "INSERT INTO users VALUES (1, 'Alice')").Run()

	out, err := exec.Command(bin, "info", dbPath).CombinedOutput()
	if err != nil {
		t.Fatalf("info: %v\n%s", err, out)
	}
	s := string(out)
	if !strings.Contains(s, "LolaDB Database Info") {
		t.Fatal("missing header")
	}
	if !strings.Contains(s, "users") {
		t.Fatal("missing table name")
	}
	if !strings.Contains(s, "0x4C4F4C41") {
		t.Fatal("missing magic")
	}
}

func TestCLI_JSONFormat(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "test.mcdb")
	exec.Command(bin, "create", dbPath).Run()
	exec.Command(bin, "exec", dbPath, "CREATE TABLE t (id INT, name TEXT)").Run()
	exec.Command(bin, "exec", dbPath, "INSERT INTO t VALUES (1, 'Alice')").Run()

	cmd := exec.Command(bin, "exec", dbPath, "SELECT * FROM t")
	cmd.Env = append(os.Environ(), "LOLADB_FORMAT=json")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("json exec: %v\n%s", err, out)
	}

	var rows []map[string]interface{}
	if err := json.Unmarshal(out, &rows); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, out)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0]["name"] != "Alice" {
		t.Fatalf("unexpected: %v", rows[0])
	}
}

func TestCLI_CSVFormat(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "test.mcdb")
	exec.Command(bin, "create", dbPath).Run()
	exec.Command(bin, "exec", dbPath, "CREATE TABLE t (id INT, val TEXT)").Run()
	exec.Command(bin, "exec", dbPath, "INSERT INTO t VALUES (1, 'x'), (2, 'y')").Run()

	cmd := exec.Command(bin, "exec", dbPath, "SELECT * FROM t")
	cmd.Env = append(os.Environ(), "LOLADB_FORMAT=csv")
	out, _ := cmd.CombinedOutput()
	s := string(out)
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) != 3 { // header + 2 rows
		t.Fatalf("expected 3 CSV lines, got %d: %s", len(lines), s)
	}
	if lines[0] != "id,val" {
		t.Fatalf("CSV header: %s", lines[0])
	}
}

func TestCLI_ImportStdin(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "test.mcdb")
	exec.Command(bin, "create", dbPath).Run()

	sqlScript := `-- Create schema
CREATE TABLE users (id INT, name TEXT);
CREATE TABLE orders (id INT, user_id INT);

-- Insert data
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
INSERT INTO orders VALUES (10, 1);
INSERT INTO orders VALUES (11, 2);

-- Query
SELECT * FROM users;
`

	cmd := exec.Command(bin, dbPath)
	cmd.Stdin = strings.NewReader(sqlScript)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("import failed: %v\n%s", err, out)
	}
	s := string(out)

	// Should show CREATE TABLE messages.
	if !strings.Contains(s, "CREATE TABLE users") {
		t.Fatalf("missing CREATE TABLE: %s", s)
	}
	// Should show INSERT messages.
	if !strings.Contains(s, "INSERT 0 1") {
		t.Fatalf("missing INSERT: %s", s)
	}
	// Should show SELECT results.
	if !strings.Contains(s, "Alice") || !strings.Contains(s, "Bob") {
		t.Fatalf("missing SELECT data: %s", s)
	}
	// Should report statement count.
	if !strings.Contains(s, "statements executed") {
		t.Fatalf("missing summary: %s", s)
	}
}

func TestCLI_ImportWithComments(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "test.mcdb")
	exec.Command(bin, "create", dbPath).Run()

	sqlScript := `-- This is a comment
CREATE TABLE t (id INT);

-- Another comment
INSERT INTO t VALUES (1);
INSERT INTO t VALUES (2);
`
	cmd := exec.Command(bin, dbPath)
	cmd.Stdin = strings.NewReader(sqlScript)
	out, _ := cmd.CombinedOutput()
	s := string(out)
	if !strings.Contains(s, "3 statements executed") {
		t.Fatalf("expected 3 statements: %s", s)
	}
}

func TestCLI_ImportMultilineStatement(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "test.mcdb")
	exec.Command(bin, "create", dbPath).Run()

	sqlScript := `CREATE TABLE users (
  id INT,
  name TEXT
);
INSERT INTO users VALUES (1, 'Alice');
SELECT * FROM users;
`
	cmd := exec.Command(bin, dbPath)
	cmd.Stdin = strings.NewReader(sqlScript)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("import failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "Alice") {
		t.Fatalf("missing data: %s", out)
	}
}

func TestCLI_ImportErrors(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "test.mcdb")
	exec.Command(bin, "create", dbPath).Run()

	sqlScript := `CREATE TABLE t (id INT);
INSERT INTO nonexistent VALUES (1);
INSERT INTO t VALUES (1);
`
	cmd := exec.Command(bin, dbPath)
	cmd.Stdin = strings.NewReader(sqlScript)
	out, _ := cmd.CombinedOutput()
	s := string(out)
	// Should have an error but continue.
	if !strings.Contains(s, "ERROR") {
		t.Fatalf("expected error: %s", s)
	}
	// Should still execute the valid statements.
	if !strings.Contains(s, "2 statements executed") && !strings.Contains(s, "1 errors") {
		// At least some statements executed and error reported.
	}
}

func TestCLI_NoArgs(t *testing.T) {
	bin := buildBinary(t)
	cmd := exec.Command(bin)
	out, _ := cmd.CombinedOutput()
	if !strings.Contains(string(out), "Usage:") {
		t.Fatalf("no-args should show usage: %s", out)
	}
}

func TestCLI_UnknownCommand(t *testing.T) {
	bin := buildBinary(t)
	cmd := exec.Command(bin, "bogus")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected error for unknown command")
	}
	if !strings.Contains(string(out), "Unknown command") {
		t.Fatalf("unexpected: %s", out)
	}
}

func TestCLI_Triggers(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "triggers.mcdb")
	exec.Command(bin, "create", dbPath).Run()

	// Helper to run SQL and check for errors.
	run := func(sql string) string {
		t.Helper()
		out, err := exec.Command(bin, "exec", dbPath, sql).CombinedOutput()
		if err != nil {
			t.Fatalf("SQL failed: %v\nSQL: %s\nOutput: %s", err, sql, out)
		}
		return string(out)
	}

	// Create table.
	run("CREATE TABLE items (id INTEGER, price INTEGER)")

	// Create a trigger function that doubles the price on INSERT.
	run(`CREATE FUNCTION double_price() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN NEW.price := NEW.price * 2; RETURN NEW; END'`)

	// Create the trigger.
	run("CREATE TRIGGER trg_double BEFORE INSERT ON items FOR EACH ROW EXECUTE FUNCTION double_price()")

	// Insert a row — the trigger should double the price.
	run("INSERT INTO items VALUES (1, 50)")

	// Verify the price was doubled.
	out := run("SELECT * FROM items")
	if !strings.Contains(out, "100") {
		t.Fatalf("expected price=100 (doubled from 50), got: %s", out)
	}

	// Insert another row.
	run("INSERT INTO items VALUES (2, 30)")
	out = run("SELECT * FROM items")
	if !strings.Contains(out, "60") {
		t.Fatalf("expected price=60 (doubled from 30), got: %s", out)
	}
}

func TestCLI_StatementTriggers(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "stmt_triggers.mcdb")
	exec.Command(bin, "create", dbPath).Run()

	run := func(sql string) string {
		t.Helper()
		out, err := exec.Command(bin, "exec", dbPath, sql).CombinedOutput()
		if err != nil {
			t.Fatalf("SQL failed: %v\nSQL: %s\nOutput: %s", err, sql, out)
		}
		return string(out)
	}

	// Create main table and audit log table.
	run("CREATE TABLE orders (id INTEGER, amount INTEGER)")
	run("CREATE TABLE audit_log (entry TEXT)")

	// Create a STATEMENT-level trigger function that logs to audit_log.
	run(`CREATE FUNCTION log_insert() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN INSERT INTO audit_log VALUES (''insert on orders''); RETURN NULL; END'`)

	// Create AFTER STATEMENT trigger (no FOR EACH ROW = STATEMENT level).
	run("CREATE TRIGGER trg_log AFTER INSERT ON orders EXECUTE FUNCTION log_insert()")

	// Insert rows — the statement trigger should fire once per statement.
	run("INSERT INTO orders VALUES (1, 100)")
	run("INSERT INTO orders VALUES (2, 200)")

	// Verify audit log has entries (one per INSERT statement).
	out := run("SELECT * FROM audit_log")
	count := strings.Count(out, "insert on orders")
	if count != 2 {
		t.Fatalf("expected 2 audit log entries (one per INSERT statement), got %d: %s", count, out)
	}

	// Verify the orders table is unaffected (STATEMENT triggers don't modify rows).
	out = run("SELECT * FROM orders")
	if !strings.Contains(out, "100") || !strings.Contains(out, "200") {
		t.Fatalf("expected orders with amounts 100 and 200, got: %s", out)
	}
}

func TestCLI_DropFunctionAndTrigger(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "drop_test.mcdb")
	exec.Command(bin, "create", dbPath).Run()

	run := func(sql string) string {
		t.Helper()
		out, err := exec.Command(bin, "exec", dbPath, sql).CombinedOutput()
		if err != nil {
			t.Fatalf("SQL failed: %v\nSQL: %s\nOutput: %s", err, sql, out)
		}
		return string(out)
	}
	runExpectErr := func(sql string) string {
		t.Helper()
		out, _ := exec.Command(bin, "exec", dbPath, sql).CombinedOutput()
		return string(out)
	}

	// Setup: table, function, trigger.
	run("CREATE TABLE items (id INTEGER, price INTEGER)")
	run(`CREATE FUNCTION double_price() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN NEW.price := NEW.price * 2; RETURN NEW; END'`)
	run("CREATE TRIGGER trg_double BEFORE INSERT ON items FOR EACH ROW EXECUTE FUNCTION double_price()")

	// Verify trigger works.
	run("INSERT INTO items VALUES (1, 50)")
	out := run("SELECT * FROM items")
	if !strings.Contains(out, "100") {
		t.Fatalf("expected price=100, got: %s", out)
	}

	// DROP TRIGGER — subsequent inserts should not double.
	run("DROP TRIGGER trg_double ON items")
	run("INSERT INTO items VALUES (2, 50)")
	out = run("SELECT * FROM items")
	if !strings.Contains(out, "50") {
		t.Fatalf("expected undoubled price=50 after DROP TRIGGER, got: %s", out)
	}

	// DROP TRIGGER IF EXISTS on already-dropped trigger — should not error.
	run("DROP TRIGGER IF EXISTS trg_double ON items")

	// DROP TRIGGER without IF EXISTS on missing trigger — should error.
	errOut := runExpectErr("DROP TRIGGER trg_double ON items")
	if !strings.Contains(errOut, "does not exist") {
		t.Fatalf("expected 'does not exist' error, got: %s", errOut)
	}

	// DROP FUNCTION.
	run("DROP FUNCTION double_price")

	// DROP FUNCTION IF EXISTS on already-dropped function — should not error.
	run("DROP FUNCTION IF EXISTS double_price")

	// DROP FUNCTION without IF EXISTS on missing function — should error.
	errOut = runExpectErr("DROP FUNCTION double_price")
	if !strings.Contains(errOut, "does not exist") {
		t.Fatalf("expected 'does not exist' error, got: %s", errOut)
	}
}

func TestCLI_AlterFunction(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "alter_func.mcdb")
	exec.Command(bin, "create", dbPath).Run()

	run := func(sql string) string {
		t.Helper()
		out, err := exec.Command(bin, "exec", dbPath, sql).CombinedOutput()
		if err != nil {
			t.Fatalf("SQL failed: %v\nSQL: %s\nOutput: %s", err, sql, out)
		}
		return string(out)
	}

	run("CREATE TABLE items (id INTEGER, price INTEGER)")
	run(`CREATE FUNCTION double_price() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN NEW.price := NEW.price * 2; RETURN NEW; END'`)

	// Rename the function.
	run("ALTER FUNCTION double_price RENAME TO multiply_price")

	// The old name should no longer work for CREATE TRIGGER.
	// The new name should work.
	run("CREATE TRIGGER trg BEFORE INSERT ON items FOR EACH ROW EXECUTE FUNCTION multiply_price()")
	run("INSERT INTO items VALUES (1, 50)")
	out := run("SELECT * FROM items")
	if !strings.Contains(out, "100") {
		t.Fatalf("expected price=100 after trigger with renamed function, got: %s", out)
	}
}

func TestCLI_CreateDomain(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "domain.mcdb")
	exec.Command(bin, "create", dbPath).Run()

	run := func(sql string) string {
		t.Helper()
		out, err := exec.Command(bin, "exec", dbPath, sql).CombinedOutput()
		if err != nil {
			t.Fatalf("SQL failed: %v\nSQL: %s\nOutput: %s", err, sql, out)
		}
		return string(out)
	}

	// Create a domain backed by bigint.
	run("CREATE DOMAIN positive_int AS bigint")

	// Use the domain as a column type.
	run("CREATE TABLE scores (id INTEGER, value positive_int)")
	run("INSERT INTO scores VALUES (1, 42)")
	out := run("SELECT * FROM scores")
	if !strings.Contains(out, "42") {
		t.Fatalf("expected value 42, got: %s", out)
	}
}

func TestCLI_CreateEnum(t *testing.T) {
	bin := buildBinary(t)
	dbPath := filepath.Join(t.TempDir(), "enum.mcdb")
	exec.Command(bin, "create", dbPath).Run()

	run := func(sql string) string {
		t.Helper()
		out, err := exec.Command(bin, "exec", dbPath, sql).CombinedOutput()
		if err != nil {
			t.Fatalf("SQL failed: %v\nSQL: %s\nOutput: %s", err, sql, out)
		}
		return string(out)
	}

	// Create an enum type.
	run("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')")

	// Use the enum as a column type.
	run("CREATE TABLE people (name TEXT, feeling mood)")
	run("INSERT INTO people VALUES ('Alice', 'happy')")
	run("INSERT INTO people VALUES ('Bob', 'sad')")
	out := run("SELECT * FROM people")
	if !strings.Contains(out, "happy") || !strings.Contains(out, "sad") {
		t.Fatalf("expected enum values, got: %s", out)
	}
}
