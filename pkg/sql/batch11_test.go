package sql

import (
	"fmt"
	"strings"
	"testing"
)

// --- CREATE EXTENSION / CREATE TABLESPACE ---

func TestCreateExtension(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec("CREATE EXTENSION IF NOT EXISTS pg_trgm")
	if err != nil {
		t.Fatalf("CREATE EXTENSION failed: %v", err)
	}
	if !strings.Contains(r.Message, "CREATE EXTENSION") {
		t.Fatalf("expected CREATE EXTENSION message, got %s", r.Message)
	}
}

func TestCreateTablespace(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec("CREATE TABLESPACE fast_disk LOCATION '/ssd/data'")
	if err != nil {
		t.Fatalf("CREATE TABLESPACE failed: %v", err)
	}
	if !strings.Contains(r.Message, "CREATE TABLESPACE") {
		t.Fatalf("expected CREATE TABLESPACE message, got %s", r.Message)
	}
}

// --- CLUSTER ---

func TestCluster(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE cluster_tbl (id INT PRIMARY KEY, name TEXT)")
	r, err := ex.Exec("CLUSTER cluster_tbl USING cluster_tbl_pkey")
	if err != nil {
		t.Fatalf("CLUSTER failed: %v", err)
	}
	if r.Message != "CLUSTER" {
		t.Fatalf("expected CLUSTER, got %s", r.Message)
	}
}

func TestClusterBare(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec("CLUSTER")
	if err != nil {
		t.Fatalf("CLUSTER failed: %v", err)
	}
	if r.Message != "CLUSTER" {
		t.Fatalf("expected CLUSTER, got %s", r.Message)
	}
}

// --- Deferrable constraints ---

func TestDeferrableConstraint(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec("CREATE TABLE defer_tbl (id INT PRIMARY KEY DEFERRABLE INITIALLY DEFERRED)")
	if err != nil {
		t.Fatalf("CREATE TABLE with DEFERRABLE failed: %v", err)
	}
	if !strings.Contains(r.Message, "CREATE TABLE") {
		t.Fatalf("expected CREATE TABLE message, got %s", r.Message)
	}
}

func TestDeferrableUnique(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec("CREATE TABLE defer_uniq (id INT UNIQUE DEFERRABLE)")
	if err != nil {
		t.Fatalf("CREATE TABLE with DEFERRABLE UNIQUE failed: %v", err)
	}
	if !strings.Contains(r.Message, "CREATE TABLE") {
		t.Fatalf("expected CREATE TABLE message, got %s", r.Message)
	}
}

// --- TABLESAMPLE ---

func TestTableSampleBernoulli(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE sample_tbl (id INT, val TEXT)")
	for i := 1; i <= 100; i++ {
		mustExec(t, ex, fmt.Sprintf("INSERT INTO sample_tbl VALUES (%d, 'row')", i))
	}

	r, err := ex.Exec("SELECT * FROM sample_tbl TABLESAMPLE BERNOULLI(50)")
	if err != nil {
		t.Fatalf("TABLESAMPLE failed: %v", err)
	}
	// With 50% sampling of 100 rows, we expect roughly 50 rows.
	// Allow wide range due to randomness: 10-90.
	if len(r.Rows) < 10 || len(r.Rows) > 90 {
		t.Fatalf("expected ~50 rows from 50%% sample, got %d", len(r.Rows))
	}
}

func TestTableSampleSystem(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE sample_sys (id INT)")
	for i := 1; i <= 100; i++ {
		mustExec(t, ex, fmt.Sprintf("INSERT INTO sample_sys VALUES (%d)", i))
	}

	r, err := ex.Exec("SELECT * FROM sample_sys TABLESAMPLE SYSTEM(10)")
	if err != nil {
		t.Fatalf("TABLESAMPLE SYSTEM failed: %v", err)
	}
	// 10% of 100 = ~10 rows. Allow 0-40 due to randomness.
	if len(r.Rows) > 40 {
		t.Fatalf("expected ~10 rows from 10%% sample, got %d", len(r.Rows))
	}
}

// --- Full-text search ---

func TestToTsvector(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec("SELECT to_tsvector('The quick brown fox jumps')")
	if err != nil {
		t.Fatalf("to_tsvector failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	tsv := r.Rows[0][0].Text
	if !strings.Contains(tsv, "quick") || !strings.Contains(tsv, "fox") {
		t.Fatalf("expected tsvector to contain 'quick' and 'fox', got %q", tsv)
	}
}

func TestToTsquery(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec("SELECT to_tsquery('english', 'quick & fox')")
	if err != nil {
		t.Fatalf("to_tsquery failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	tsq := r.Rows[0][0].Text
	if !strings.Contains(tsq, "quick") || !strings.Contains(tsq, "fox") {
		t.Fatalf("expected tsquery to contain 'quick' and 'fox', got %q", tsq)
	}
}

func TestTSMatchOperator(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE docs (id INT, body TEXT)")
	mustExec(t, ex, "INSERT INTO docs VALUES (1, 'The quick brown fox')")
	mustExec(t, ex, "INSERT INTO docs VALUES (2, 'The lazy dog sleeps')")
	mustExec(t, ex, "INSERT INTO docs VALUES (3, 'Quick fox and lazy dog')")

	r, err := ex.Exec("SELECT id FROM docs WHERE to_tsvector(body) @@ to_tsquery('fox') ORDER BY id")
	if err != nil {
		t.Fatalf("@@ query failed: %v", err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 matching rows, got %d", len(r.Rows))
	}
	id0 := r.Rows[0][0].I32 + int32(r.Rows[0][0].I64)
	id1 := r.Rows[1][0].I32 + int32(r.Rows[1][0].I64)
	if id0 != 1 || id1 != 3 {
		t.Fatalf("expected ids 1 and 3, got %d and %d", id0, id1)
	}
}

func TestTSMatchMultipleTerms(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE articles (id INT, content TEXT)")
	mustExec(t, ex, "INSERT INTO articles VALUES (1, 'PostgreSQL is a powerful database')")
	mustExec(t, ex, "INSERT INTO articles VALUES (2, 'MySQL is also a database')")
	mustExec(t, ex, "INSERT INTO articles VALUES (3, 'PostgreSQL supports full text search')")

	r, err := ex.Exec("SELECT id FROM articles WHERE to_tsvector(content) @@ to_tsquery('postgresql & database') ORDER BY id")
	if err != nil {
		t.Fatalf("@@ multi-term query failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 matching row, got %d", len(r.Rows))
	}
	id := r.Rows[0][0].I32 + int32(r.Rows[0][0].I64)
	if id != 1 {
		t.Fatalf("expected id 1, got %d", id)
	}
}

// --- Advisory locks ---

func TestAdvisoryLock(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec("SELECT pg_advisory_lock(12345)")
	if err != nil {
		t.Fatalf("pg_advisory_lock failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
}

func TestAdvisoryUnlock(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec("SELECT pg_advisory_unlock(12345)")
	if err != nil {
		t.Fatalf("pg_advisory_unlock failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if !r.Rows[0][0].Bool {
		t.Fatalf("expected true, got %v", r.Rows[0][0])
	}
}

func TestTryAdvisoryLock(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec("SELECT pg_try_advisory_lock(99)")
	if err != nil {
		t.Fatalf("pg_try_advisory_lock failed: %v", err)
	}
	if len(r.Rows) != 1 || !r.Rows[0][0].Bool {
		t.Fatalf("expected true, got %v", r.Rows)
	}
}


