package sql

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// set_config / current_setting
// ---------------------------------------------------------------------------

func TestSetConfigCurrentSetting(t *testing.T) {
	ex := newTestExecutor(t)

	// set_config sets a session variable and returns the value.
	r, err := ex.Exec(`SELECT set_config('myapp.color', 'blue', false)`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Text != "blue" {
		t.Fatalf("expected 'blue', got %q", r.Rows[0][0].Text)
	}

	// current_setting retrieves it.
	r, err = ex.Exec(`SELECT current_setting('myapp.color')`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Text != "blue" {
		t.Fatalf("expected 'blue', got %q", r.Rows[0][0].Text)
	}
}

func TestCurrentSettingDefaults(t *testing.T) {
	ex := newTestExecutor(t)

	// Well-known settings should have defaults.
	r, err := ex.Exec(`SELECT current_setting('server_version')`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Text == "" {
		t.Fatal("expected non-empty server_version")
	}
}

func TestSetConfigOverridesShow(t *testing.T) {
	ex := newTestExecutor(t)

	mustExec(t, ex, `SET search_path TO 'myschema'`)
	r, err := ex.Exec(`SELECT current_setting('search_path')`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Rows[0][0].Text != "myschema" {
		t.Fatalf("expected 'myschema', got %q", r.Rows[0][0].Text)
	}
}

// ---------------------------------------------------------------------------
// CREATE AGGREGATE
// ---------------------------------------------------------------------------

func TestCreateAggregate(t *testing.T) {
	ex := newTestExecutor(t)

	// Create a simple sum-like aggregate using array_append as sfunc.
	mustExec(t, ex, `CREATE AGGREGATE my_count (anyelement) (
		SFUNC = array_append,
		STYPE = anyarray,
		INITCOND = '{}'
	)`)

	mustExec(t, ex, `CREATE TABLE agg_test (val INT)`)
	mustExec(t, ex, `INSERT INTO agg_test VALUES (10)`)
	mustExec(t, ex, `INSERT INTO agg_test VALUES (20)`)
	mustExec(t, ex, `INSERT INTO agg_test VALUES (30)`)

	r, err := ex.Exec(`SELECT my_count(val) FROM agg_test`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	// The result should be an array containing all values.
	got := r.Rows[0][0].Text
	if !strings.Contains(got, "10") || !strings.Contains(got, "20") || !strings.Contains(got, "30") {
		t.Fatalf("expected array with 10,20,30, got %q", got)
	}
}

func TestCreateAggregateSchemaQualified(t *testing.T) {
	ex := newTestExecutor(t)

	// Schema-qualified sfunc should be accepted (schema prefix stripped).
	mustExec(t, ex, `CREATE AGGREGATE public.group_concat(text) (
		SFUNC = public.array_append,
		STYPE = anyarray,
		INITCOND = '{}'
	)`)

	mustExec(t, ex, `CREATE TABLE gc_test (val TEXT)`)
	mustExec(t, ex, `INSERT INTO gc_test VALUES ('a')`)
	mustExec(t, ex, `INSERT INTO gc_test VALUES ('b')`)

	r, err := ex.Exec(`SELECT group_concat(val) FROM gc_test`)
	if err != nil {
		t.Fatal(err)
	}
	got := r.Rows[0][0].Text
	if !strings.Contains(got, "a") || !strings.Contains(got, "b") {
		t.Fatalf("expected array with a,b, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// CREATE MATERIALIZED VIEW (full pipeline)
// ---------------------------------------------------------------------------

func TestCreateMatViewPipeline(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE mv_pipe_src (id INT, label TEXT)`)
	mustExec(t, ex, `INSERT INTO mv_pipe_src VALUES (1, 'alpha')`)
	mustExec(t, ex, `INSERT INTO mv_pipe_src VALUES (2, 'beta')`)
	mustExec(t, ex, `INSERT INTO mv_pipe_src VALUES (3, 'gamma')`)

	mustExec(t, ex, `CREATE MATERIALIZED VIEW mv_pipe AS SELECT id, label FROM mv_pipe_src WHERE id > 1`)

	r, err := ex.Exec(`SELECT label FROM mv_pipe ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "beta" {
		t.Errorf("expected 'beta', got %q", r.Rows[0][0].Text)
	}
	if r.Rows[1][0].Text != "gamma" {
		t.Errorf("expected 'gamma', got %q", r.Rows[1][0].Text)
	}
}

func TestCreateMatViewIfNotExists(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE TABLE mv_ine_src (id INT)`)
	mustExec(t, ex, `INSERT INTO mv_ine_src VALUES (1)`)
	mustExec(t, ex, `CREATE MATERIALIZED VIEW mv_ine AS SELECT id FROM mv_ine_src`)

	// Should not error with IF NOT EXISTS.
	_, err := ex.Exec(`CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ine AS SELECT id FROM mv_ine_src`)
	if err != nil {
		t.Fatalf("IF NOT EXISTS should not error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// tsvector_update_trigger
// ---------------------------------------------------------------------------

func TestTsvectorUpdateTrigger(t *testing.T) {
	ex := newTestExecutor(t)

	mustExec(t, ex, `CREATE TABLE articles (id INT, title TEXT, body TEXT, tsv TEXT)`)

	// tsvector_update_trigger is registered as a built-in trigger function.
	mustExec(t, ex, `CREATE TRIGGER tsv_update BEFORE INSERT OR UPDATE ON articles
		FOR EACH ROW EXECUTE FUNCTION tsvector_update_trigger(tsv, 'english', title, body)`)

	mustExec(t, ex, `INSERT INTO articles VALUES (1, 'Hello World', 'This is a test', '')`)

	r, err := ex.Exec(`SELECT tsv FROM articles WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}

	tsv := r.Rows[0][0].Text
	// The tsvector should contain tokenized words from title and body.
	if !strings.Contains(tsv, "'hello'") || !strings.Contains(tsv, "'world'") {
		t.Errorf("expected tsvector to contain 'hello' and 'world', got %q", tsv)
	}
	if !strings.Contains(tsv, "'test'") {
		t.Errorf("expected tsvector to contain 'test', got %q", tsv)
	}
}

func TestTsvectorUpdateTriggerMultipleCols(t *testing.T) {
	ex := newTestExecutor(t)

	mustExec(t, ex, `CREATE TABLE docs (id INT, title TEXT, summary TEXT, content TEXT, tsv TEXT)`)
	mustExec(t, ex, `CREATE TRIGGER tsv_docs BEFORE INSERT ON docs
		FOR EACH ROW EXECUTE FUNCTION tsvector_update_trigger(tsv, 'english', title, summary, content)`)

	mustExec(t, ex, `INSERT INTO docs VALUES (1, 'Go Programming', 'A language by Google', 'Concurrency is key', '')`)

	r, err := ex.Exec(`SELECT tsv FROM docs WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	tsv := r.Rows[0][0].Text
	// Should contain words from all three source columns.
	for _, word := range []string{"'go'", "'programming'", "'google'", "'concurrency'"} {
		if !strings.Contains(tsv, word) {
			t.Errorf("expected tsvector to contain %s, got %q", word, tsv)
		}
	}
}

// ---------------------------------------------------------------------------
// GRANT/REVOKE ON SCHEMA
// ---------------------------------------------------------------------------

func TestGrantRevokeOnSchema(t *testing.T) {
	ex := newTestExecutor(t)

	// Create a role and schema.
	mustExec(t, ex, `CREATE ROLE test_user`)
	mustExec(t, ex, `CREATE SCHEMA test_schema`)

	// GRANT USAGE ON SCHEMA should succeed.
	mustExec(t, ex, `GRANT USAGE ON SCHEMA test_schema TO test_user`)

	// GRANT CREATE ON SCHEMA should succeed.
	mustExec(t, ex, `GRANT CREATE ON SCHEMA test_schema TO test_user`)

	// REVOKE should also succeed.
	mustExec(t, ex, `REVOKE USAGE ON SCHEMA test_schema FROM test_user`)
}

func TestGrantOnSchemaNotFound(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, `CREATE ROLE test_user2`)

	_, err := ex.Exec(`GRANT USAGE ON SCHEMA nonexistent TO test_user2`)
	if err == nil {
		t.Fatal("expected error for nonexistent schema")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("expected 'does not exist' error, got: %v", err)
	}
}
