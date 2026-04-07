package sql

import (
	"testing"
)

// ---------------------------------------------------------------------------
// information_schema.tables
// ---------------------------------------------------------------------------

func TestInfoSchema_Tables(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT PRIMARY KEY, name TEXT)`)
	ex.Exec(`CREATE TABLE orders (id INT, user_id INT)`)

	r, err := ex.Exec(`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`)
	if err != nil {
		t.Fatal(err)
	}
	names := make(map[string]bool)
	for _, row := range r.Rows {
		names[row[0].Text] = true
	}
	if !names["users"] {
		t.Fatal("expected 'users' in information_schema.tables")
	}
	if !names["orders"] {
		t.Fatal("expected 'orders' in information_schema.tables")
	}
}

func TestInfoSchema_TablesType(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t1 (id INT)`)
	ex.Exec(`CREATE VIEW v1 AS SELECT id FROM t1`)

	r, err := ex.Exec(`SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public'`)
	if err != nil {
		t.Fatal(err)
	}
	types := make(map[string]string)
	for _, row := range r.Rows {
		types[row[0].Text] = row[1].Text
	}
	if types["t1"] != "BASE TABLE" {
		t.Fatalf("expected t1 to be BASE TABLE, got %q", types["t1"])
	}
	if types["v1"] != "VIEW" {
		t.Fatalf("expected v1 to be VIEW, got %q", types["v1"])
	}
}

// ---------------------------------------------------------------------------
// information_schema.columns
// ---------------------------------------------------------------------------

func TestInfoSchema_Columns(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOLEAN)`)

	r, err := ex.Exec(`SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_name = 'users'
		ORDER BY ordinal_position`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(r.Rows))
	}
	// id: integer, NOT NULL (primary key)
	if r.Rows[0][0].Text != "id" {
		t.Fatalf("expected 'id', got %q", r.Rows[0][0].Text)
	}
	if r.Rows[0][1].Text != "integer" {
		t.Fatalf("expected 'integer', got %q", r.Rows[0][1].Text)
	}
	if r.Rows[0][2].Text != "NO" {
		t.Fatalf("expected 'NO' for id nullable, got %q", r.Rows[0][2].Text)
	}
	// name: text, nullable
	if r.Rows[1][0].Text != "name" {
		t.Fatalf("expected 'name', got %q", r.Rows[1][0].Text)
	}
	if r.Rows[1][2].Text != "YES" {
		t.Fatalf("expected 'YES' for name nullable, got %q", r.Rows[1][2].Text)
	}
}

// ---------------------------------------------------------------------------
// information_schema.schemata
// ---------------------------------------------------------------------------

func TestInfoSchema_Schemata(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`SELECT schema_name FROM information_schema.schemata`)
	if err != nil {
		t.Fatal(err)
	}
	names := make(map[string]bool)
	for _, row := range r.Rows {
		names[row[0].Text] = true
	}
	if !names["public"] {
		t.Fatal("expected 'public' in schemata")
	}
	if !names["pg_catalog"] {
		t.Fatal("expected 'pg_catalog' in schemata")
	}
}

// ---------------------------------------------------------------------------
// pg_tables
// ---------------------------------------------------------------------------

func TestPgTables(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE products (id INT, name TEXT)`)

	r, err := ex.Exec(`SELECT tablename FROM pg_tables WHERE schemaname = 'public'`)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range r.Rows {
		if row[0].Text == "products" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected 'products' in pg_tables")
	}
}

// ---------------------------------------------------------------------------
// pg_views
// ---------------------------------------------------------------------------

func TestPgViews(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t1 (id INT)`)
	ex.Exec(`CREATE VIEW v1 AS SELECT id FROM t1`)

	r, err := ex.Exec(`SELECT viewname FROM pg_views WHERE schemaname = 'public'`)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range r.Rows {
		if row[0].Text == "v1" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected 'v1' in pg_views")
	}
}

// ---------------------------------------------------------------------------
// pg_indexes
// ---------------------------------------------------------------------------

func TestPgIndexes(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t1 (id INT PRIMARY KEY)`)

	r, err := ex.Exec(`SELECT indexname, tablename FROM pg_indexes WHERE schemaname = 'public'`)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range r.Rows {
		if row[0].Text == "t1_pkey" && row[1].Text == "t1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected 't1_pkey' index for 't1' in pg_indexes, got %v", r.Rows)
	}
}

// ---------------------------------------------------------------------------
// pg_roles
// ---------------------------------------------------------------------------

func TestPgRoles(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`SELECT rolname FROM pg_roles`)
	if err != nil {
		t.Fatal(err)
	}
	// Should have at least one role.
	if len(r.Rows) == 0 {
		t.Fatal("expected at least one role in pg_roles")
	}
}

// ---------------------------------------------------------------------------
// pg_namespace
// ---------------------------------------------------------------------------

func TestPgNamespace(t *testing.T) {
	ex := newTestExecutor(t)

	r, err := ex.Exec(`SELECT nspname FROM pg_namespace`)
	if err != nil {
		t.Fatal(err)
	}
	names := make(map[string]bool)
	for _, row := range r.Rows {
		names[row[0].Text] = true
	}
	if !names["public"] {
		t.Fatal("expected 'public' in pg_namespace")
	}
}

// ---------------------------------------------------------------------------
// Aliased access
// ---------------------------------------------------------------------------

func TestInfoSchema_WithAlias(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t1 (id INT)`)

	r, err := ex.Exec(`SELECT t.table_name FROM information_schema.tables AS t WHERE t.table_schema = 'public'`)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range r.Rows {
		if row[0].Text == "t1" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected 't1' with aliased query")
	}
}

// ---------------------------------------------------------------------------
// pg_stat_user_tables
// ---------------------------------------------------------------------------

func TestPgStatUserTables(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t1 (id INT)`)

	r, err := ex.Exec(`SELECT relname FROM pg_stat_user_tables WHERE schemaname = 'public'`)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range r.Rows {
		if row[0].Text == "t1" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected 't1' in pg_stat_user_tables")
	}
}
