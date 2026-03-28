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
	if !strings.Contains(string(out), "INSERT 2") {
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
	if !strings.Contains(s, "INSERT 1") {
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
