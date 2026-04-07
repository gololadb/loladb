package sql

import (
	"strings"
	"testing"
)

// --- Network types (inet, cidr, macaddr) stored as text ---

func TestNetworkTypes(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE hosts (id INT, addr INET, net CIDR, mac MACADDR)")
	mustExec(t, ex, "INSERT INTO hosts VALUES (1, '192.168.1.1', '10.0.0.0/8', '08:00:2b:01:02:03')")
	mustExec(t, ex, "INSERT INTO hosts VALUES (2, '::1', '192.168.0.0/16', 'aa:bb:cc:dd:ee:ff')")

	r := mustExecR(t, ex, "SELECT addr, net, mac FROM hosts ORDER BY id")
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "192.168.1.1" {
		t.Fatalf("expected '192.168.1.1', got %q", r.Rows[0][0].Text)
	}
}

func TestNetworkFunctions(t *testing.T) {
	ex := newTestExecutor(t)
	r := mustExecR(t, ex, "SELECT host('192.168.1.100/24')")
	if r.Rows[0][0].Text != "192.168.1.100" {
		t.Fatalf("host() expected '192.168.1.100', got %q", r.Rows[0][0].Text)
	}

	r = mustExecR(t, ex, "SELECT family('192.168.1.1')")
	if r.Rows[0][0].Text != "4" {
		t.Fatalf("family() expected '4', got %q", r.Rows[0][0].Text)
	}

	r = mustExecR(t, ex, "SELECT family('::1')")
	if r.Rows[0][0].Text != "6" {
		t.Fatalf("family() expected '6', got %q", r.Rows[0][0].Text)
	}

	r = mustExecR(t, ex, "SELECT masklen('192.168.1.0/24')")
	if r.Rows[0][0].I32 != 24 {
		t.Fatalf("masklen() expected 24, got %d", r.Rows[0][0].I32)
	}
}

// --- Range types ---

func TestRangeFunctions(t *testing.T) {
	ex := newTestExecutor(t)

	r := mustExecR(t, ex, "SELECT int4range(1, 10)")
	if r.Rows[0][0].Text != "[1,10)" {
		t.Fatalf("int4range expected '[1,10)', got %q", r.Rows[0][0].Text)
	}

	r = mustExecR(t, ex, "SELECT lower_range('[3,7)')")
	if r.Rows[0][0].Text != "3" {
		t.Fatalf("lower_range expected '3', got %q", r.Rows[0][0].Text)
	}

	r = mustExecR(t, ex, "SELECT upper_range('[3,7)')")
	if r.Rows[0][0].Text != "7" {
		t.Fatalf("upper_range expected '7', got %q", r.Rows[0][0].Text)
	}

	r = mustExecR(t, ex, "SELECT isempty('empty')")
	if !r.Rows[0][0].Bool {
		t.Fatalf("isempty('empty') expected true, got false")
	}

	r = mustExecR(t, ex, "SELECT isempty('[1,5)')")
	if r.Rows[0][0].Bool {
		t.Fatalf("isempty('[1,5)') expected false, got true")
	}
}

func TestRangeColumnTypes(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE schedules (id INT, period TSRANGE)")
	mustExec(t, ex, "INSERT INTO schedules VALUES (1, '[2024-01-01,2024-06-01)')")

	r := mustExecR(t, ex, "SELECT period FROM schedules WHERE id = 1")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if !strings.Contains(r.Rows[0][0].Text, "2024") {
		t.Fatalf("expected range with 2024, got %q", r.Rows[0][0].Text)
	}
}

// --- Composite types ---

func TestCompositeType(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec("CREATE TYPE address AS (street TEXT, city TEXT, zip TEXT)")
	if err != nil {
		t.Fatalf("CREATE TYPE AS failed: %v", err)
	}
	if !strings.Contains(r.Message, "CREATE TYPE") {
		t.Fatalf("expected CREATE TYPE message, got %q", r.Message)
	}
}

// --- XML functions and xmlagg ---

func TestXMLFunctions(t *testing.T) {
	ex := newTestExecutor(t)

	// xmlconcat concatenates XML fragments.
	r := mustExecR(t, ex, "SELECT xmlconcat('<a/>', '<b/>')")
	if r.Rows[0][0].Text != "<a/><b/>" {
		t.Fatalf("xmlconcat expected '<a/><b/>', got %q", r.Rows[0][0].Text)
	}

	// xmlelement produces an XML element.
	r = mustExecR(t, ex, "SELECT XMLELEMENT(NAME root, 'hello')")
	if r.Rows[0][0].Text != "<root>hello</root>" {
		t.Fatalf("xmlelement expected '<root>hello</root>', got %q", r.Rows[0][0].Text)
	}
}

func TestXMLAgg(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE items (grp INT, val TEXT)")
	mustExec(t, ex, "INSERT INTO items VALUES (1, '<a/>'), (1, '<b/>'), (2, '<c/>')")

	r := mustExecR(t, ex, "SELECT xmlagg(val) FROM items WHERE grp = 1")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	agg := r.Rows[0][0].Text
	if !strings.Contains(agg, "<a/>") || !strings.Contains(agg, "<b/>") {
		t.Fatalf("xmlagg expected to contain '<a/>' and '<b/>', got %q", agg)
	}
}

// --- FDW / replication no-ops ---

func TestCreateForeignDataWrapper(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec("CREATE FOREIGN DATA WRAPPER myfdw")
	if err != nil {
		t.Fatalf("CREATE FOREIGN DATA WRAPPER failed: %v", err)
	}
	if !strings.Contains(r.Message, "myfdw") {
		t.Fatalf("expected message with 'myfdw', got %q", r.Message)
	}
}

func TestCreateServer(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec("CREATE SERVER myserver FOREIGN DATA WRAPPER myfdw")
	if err != nil {
		t.Fatalf("CREATE SERVER failed: %v", err)
	}
	if !strings.Contains(r.Message, "myserver") {
		t.Fatalf("expected message with 'myserver', got %q", r.Message)
	}
}

func TestCreateForeignTable(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec("CREATE FOREIGN TABLE remote_t (id INT, name TEXT) SERVER myserver")
	if err != nil {
		t.Fatalf("CREATE FOREIGN TABLE failed: %v", err)
	}
	if !strings.Contains(r.Message, "remote_t") {
		t.Fatalf("expected message with 'remote_t', got %q", r.Message)
	}
}

func TestCreatePublication(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec("CREATE PUBLICATION mypub FOR ALL TABLES")
	if err != nil {
		t.Fatalf("CREATE PUBLICATION failed: %v", err)
	}
	if !strings.Contains(r.Message, "mypub") {
		t.Fatalf("expected message with 'mypub', got %q", r.Message)
	}
}

func TestCreateSubscription(t *testing.T) {
	ex := newTestExecutor(t)
	r, err := ex.Exec("CREATE SUBSCRIPTION mysub CONNECTION 'host=localhost' PUBLICATION mypub")
	if err != nil {
		t.Fatalf("CREATE SUBSCRIPTION failed: %v", err)
	}
	if !strings.Contains(r.Message, "mysub") {
		t.Fatalf("expected message with 'mysub', got %q", r.Message)
	}
}

// --- pg_stat_statements ---

func TestPgStatStatements(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE stat_test (id INT)")
	mustExec(t, ex, "INSERT INTO stat_test VALUES (1)")
	mustExec(t, ex, "INSERT INTO stat_test VALUES (1)")
	mustExec(t, ex, "INSERT INTO stat_test VALUES (1)")

	// Check that query stats are tracked in the catalog directly.
	qs := ex.Cat.QueryStats["INSERT INTO stat_test VALUES (1)"]
	if qs == nil {
		t.Fatal("expected query stats for INSERT, got nil")
	}
	if qs.Calls != 3 {
		t.Fatalf("expected 3 calls, got %d", qs.Calls)
	}

	// Also verify the virtual table is queryable.
	r := mustExecR(t, ex, "SELECT query, calls FROM pg_stat_statements")
	if len(r.Rows) < 2 {
		t.Fatalf("expected at least 2 tracked queries, got %d", len(r.Rows))
	}
}
