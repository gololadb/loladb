package pagila_test

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jespino/loladb/pkg/catalog"
	"github.com/jespino/loladb/pkg/engine"
	"github.com/jespino/loladb/pkg/sql"
)

// newPagilaDB creates a LolaDB instance, loads the pagila schema and data.
func newPagilaDB(t *testing.T) *sql.Executor {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "pagila.lodb")
	eng, err := engine.Open(path, 4096) // larger buffer pool for bulk load
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })

	cat, err := catalog.New(eng)
	if err != nil {
		t.Fatal(err)
	}
	ex := sql.NewExecutor(cat)

	// Load schema.
	t.Log("Loading schema...")
	schemaSQL, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	// Strip comments before splitting on semicolons.
	var cleaned []string
	for _, line := range strings.Split(string(schemaSQL), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "--") {
			continue
		}
		cleaned = append(cleaned, line)
	}
	for _, stmt := range splitStatements(strings.Join(cleaned, "\n")) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := ex.Exec(stmt); err != nil {
			t.Fatalf("schema: %s: %v", stmt, err)
		}
	}

	// Load data.
	t.Log("Loading data...")
	f, err := os.Open("data.sql")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		if _, err := ex.Exec(line); err != nil {
			t.Fatalf("data line %d: %s: %v", count+1, truncate(line, 80), err)
		}
		count++
		if count%5000 == 0 {
			t.Logf("  loaded %d rows...", count)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	t.Logf("Loaded %d rows total", count)
	return ex
}

func splitStatements(s string) []string {
	var stmts []string
	for _, part := range strings.Split(s, ";") {
		part = strings.TrimSpace(part)
		if part != "" {
			stmts = append(stmts, part)
		}
	}
	return stmts
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// explain runs EXPLAIN on a query and returns the plan text.
func explain(t *testing.T, ex *sql.Executor, query string) string {
	t.Helper()
	r, err := ex.Exec("EXPLAIN " + query)
	if err != nil {
		t.Fatalf("EXPLAIN %s: %v", truncate(query, 60), err)
	}
	var lines []string
	for _, row := range r.Rows {
		if len(row) > 0 {
			lines = append(lines, row[0].Text)
		}
	}
	return strings.Join(lines, "\n")
}

func TestPagila_LoadAndExplain(t *testing.T) {
	ex := newPagilaDB(t)

	// Verify row counts.
	counts := map[string]int{
		"actor": 200, "film": 1000, "customer": 599,
		"rental": 16000, "payment": 16000,
	}
	for table, expected := range counts {
		r, err := ex.Exec(fmt.Sprintf("SELECT * FROM %s", table))
		if err != nil {
			t.Fatalf("SELECT * FROM %s: %v", table, err)
		}
		if len(r.Rows) != expected {
			t.Errorf("%s: expected %d rows, got %d", table, expected, len(r.Rows))
		}
	}

	// --- Phase 1: No indexes — all queries should use SeqScan ---
	t.Log("\n=== Phase 1: No indexes (SeqScan expected) ===")

	queries := []struct {
		name  string
		query string
	}{
		{"point lookup film_id=42", "SELECT * FROM film WHERE film_id = 42"},
		{"point lookup customer_id=100", "SELECT * FROM customer WHERE customer_id = 100"},
		{"point lookup rental_id=500", "SELECT * FROM rental WHERE rental_id = 500"},
		{"join film-film_actor", "SELECT * FROM film JOIN film_actor ON film.film_id = film_actor.film_id WHERE film.film_id = 1"},
	}

	for _, q := range queries {
		plan := explain(t, ex, q.query)
		t.Logf("\n--- %s ---\nQuery: %s\nPlan:\n%s", q.name, q.query, plan)
	}

	// --- Phase 2: Create btree indexes ---
	t.Log("\n=== Phase 2: Creating btree indexes ===")
	btreeIndexes := []struct {
		name, table, column string
	}{
		{"idx_film_film_id", "film", "film_id"},
		{"idx_actor_actor_id", "actor", "actor_id"},
		{"idx_customer_customer_id", "customer", "customer_id"},
		{"idx_rental_rental_id", "rental", "rental_id"},
		{"idx_rental_customer_id", "rental", "customer_id"},
		{"idx_inventory_film_id", "inventory", "film_id"},
		{"idx_film_actor_film_id", "film_actor", "film_id"},
		{"idx_film_actor_actor_id", "film_actor", "actor_id"},
		{"idx_payment_customer_id", "payment", "customer_id"},
		{"idx_payment_rental_id", "payment", "rental_id"},
	}
	for _, idx := range btreeIndexes {
		stmt := fmt.Sprintf("CREATE INDEX %s ON %s (%s)", idx.name, idx.table, idx.column)
		if _, err := ex.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
		t.Logf("Created: %s", stmt)
	}

	// Re-run queries — should now use IndexScan for point lookups.
	t.Log("\n=== Phase 2: With btree indexes ===")
	for _, q := range queries {
		plan := explain(t, ex, q.query)
		t.Logf("\n--- %s ---\nQuery: %s\nPlan:\n%s", q.name, q.query, plan)
	}

	// --- Phase 3: Create hash indexes (now supports TEXT columns) ---
	t.Log("\n=== Phase 3: Creating hash indexes ===")
	hashIndexes := []struct {
		name, table, column string
	}{
		{"idx_film_title_hash", "film", "title"},
		{"idx_customer_email_hash", "customer", "email"},
		{"idx_film_release_year_hash", "film", "release_year"},
		{"idx_customer_active_hash", "customer", "active"},
	}
	for _, idx := range hashIndexes {
		stmt := fmt.Sprintf("CREATE INDEX %s ON %s USING hash (%s)", idx.name, idx.table, idx.column)
		if _, err := ex.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
		t.Logf("Created: %s", stmt)
	}

	// Hash index queries — including TEXT lookups.
	hashQueries := []struct {
		name  string
		query string
	}{
		{"hash lookup title (TEXT)", "SELECT * FROM film WHERE title = 'ACE GOLDFINGER'"},
		{"hash lookup email (TEXT)", "SELECT * FROM customer WHERE email = 'MARY.SMITH@example.com'"},
		{"hash lookup release_year", "SELECT * FROM film WHERE release_year = 2005"},
		{"hash lookup active", "SELECT * FROM customer WHERE active = 1"},
	}
	t.Log("\n=== Phase 3: With hash indexes ===")
	for _, q := range hashQueries {
		plan := explain(t, ex, q.query)
		t.Logf("\n--- %s ---\nQuery: %s\nPlan:\n%s", q.name, q.query, plan)
	}

	// --- Phase 4: Additional index types (TEXT and INT columns) ---
	t.Log("\n=== Phase 4: Creating BRIN, GIN, GiST, SP-GiST indexes ===")
	otherIndexes := []struct {
		name, table, column, method string
	}{
		{"idx_rental_rental_id_brin", "rental", "rental_id", "brin"},
		{"idx_film_description_gin", "film", "description", "gin"},
		{"idx_film_rating_gist", "film", "rating", "gist"},
		{"idx_film_title_spgist", "film", "title", "spgist"},
		{"idx_payment_amount_gin", "payment", "amount", "gin"},
		{"idx_film_length_gist", "film", "length", "gist"},
		{"idx_film_rental_rate_spgist", "film", "rental_rate", "spgist"},
	}
	for _, idx := range otherIndexes {
		stmt := fmt.Sprintf("CREATE INDEX %s ON %s USING %s (%s)", idx.name, idx.table, idx.method, idx.column)
		if _, err := ex.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
		t.Logf("Created: %s", stmt)
	}

	// --- Phase 5: Complex queries ---
	t.Log("\n=== Phase 5: Complex queries ===")
	complexQueries := []struct {
		name  string
		query string
	}{
		{
			"join with filter",
			"SELECT * FROM rental JOIN customer ON rental.customer_id = customer.customer_id WHERE customer.customer_id = 5",
		},
		{
			"three-way join",
			"SELECT * FROM film JOIN film_actor ON film.film_id = film_actor.film_id JOIN actor ON film_actor.actor_id = actor.actor_id WHERE film.film_id = 1",
		},
		{
			"range-like filter (inequality)",
			"SELECT * FROM film WHERE film_id = 500",
		},
	}
	for _, q := range complexQueries {
		plan := explain(t, ex, q.query)
		t.Logf("\n--- %s ---\nQuery: %s\nPlan:\n%s", q.name, q.query, plan)
	}
}
