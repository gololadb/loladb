package catalog

import (
	"testing"

	"github.com/gololadb/loladb/pkg/engine"
	"github.com/gololadb/loladb/pkg/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
)

func openEngine(t *testing.T, path string) (*engine.Engine, error) {
	t.Helper()
	return engine.Open(path, 32)
}

func TestCreateIndex(t *testing.T) {
	cat := newTestCatalog(t)

	cat.CreateTable("users", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "name", Type: tuple.TypeText},
	})

	oid, err := cat.CreateIndex("idx_users_id", "users", "id", "btree")
	if err != nil {
		t.Fatal(err)
	}
	if oid == 0 {
		t.Fatal("expected non-zero OID")
	}
}

func TestCreateIndex_DuplicateName(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("users", []ColumnDef{{Name: "id", Type: tuple.TypeInt32}})
	cat.CreateIndex("idx_users_id", "users", "id", "btree")

	_, err := cat.CreateIndex("idx_users_id", "users", "id", "btree")
	if err == nil {
		t.Fatal("expected error for duplicate index name")
	}
}

func TestCreateIndex_TableNotFound(t *testing.T) {
	cat := newTestCatalog(t)
	_, err := cat.CreateIndex("idx_x", "nonexistent", "id", "btree")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCreateIndex_ColumnNotFound(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("users", []ColumnDef{{Name: "id", Type: tuple.TypeInt32}})

	_, err := cat.CreateIndex("idx_x", "users", "nonexistent", "btree")
	if err == nil {
		t.Fatal("expected error for nonexistent column")
	}
}

func TestIndexScan_Empty(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("users", []ColumnDef{{Name: "id", Type: tuple.TypeInt32}})
	cat.CreateIndex("idx_users_id", "users", "id", "btree")

	tuples, _, err := cat.IndexScan("idx_users_id", 42)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 0 {
		t.Fatalf("expected 0, got %d", len(tuples))
	}
}

func TestCreateIndex_PopulatesExistingData(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("users", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "name", Type: tuple.TypeText},
	})

	// Insert data BEFORE creating the index.
	cat.InsertInto("users", []tuple.Datum{tuple.DInt32(10), tuple.DText("Alice")})
	cat.InsertInto("users", []tuple.Datum{tuple.DInt32(20), tuple.DText("Bob")})
	cat.InsertInto("users", []tuple.Datum{tuple.DInt32(30), tuple.DText("Charlie")})

	cat.CreateIndex("idx_users_id", "users", "id", "btree")

	// Index should find pre-existing data.
	tuples, _, err := cat.IndexScan("idx_users_id", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 1 {
		t.Fatalf("expected 1, got %d", len(tuples))
	}
	if tuples[0].Columns[1].Text != "Bob" {
		t.Fatalf("expected Bob, got %q", tuples[0].Columns[1].Text)
	}
}

func TestInsertInto_AutoUpdatesIndex(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("users", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "name", Type: tuple.TypeText},
	})
	cat.CreateIndex("idx_users_id", "users", "id", "btree")

	// Insert AFTER creating the index.
	cat.InsertInto("users", []tuple.Datum{tuple.DInt32(42), tuple.DText("Alice")})
	cat.InsertInto("users", []tuple.Datum{tuple.DInt32(99), tuple.DText("Bob")})

	// Index should find the new data.
	tuples, _, err := cat.IndexScan("idx_users_id", 42)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 1 {
		t.Fatalf("expected 1, got %d", len(tuples))
	}
	if tuples[0].Columns[1].Text != "Alice" {
		t.Fatalf("expected Alice, got %q", tuples[0].Columns[1].Text)
	}

	tuples, _, err = cat.IndexScan("idx_users_id", 99)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 1 {
		t.Fatalf("expected 1, got %d", len(tuples))
	}
	if tuples[0].Columns[1].Text != "Bob" {
		t.Fatalf("expected Bob, got %q", tuples[0].Columns[1].Text)
	}
}

func TestIndexScan_NotFound(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("users", []ColumnDef{{Name: "id", Type: tuple.TypeInt32}})
	cat.CreateIndex("idx_users_id", "users", "id", "btree")
	cat.InsertInto("users", []tuple.Datum{tuple.DInt32(1)})

	tuples, _, err := cat.IndexScan("idx_users_id", 999)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 0 {
		t.Fatalf("expected 0, got %d", len(tuples))
	}
}

func TestIndexScan_IndexNotFound(t *testing.T) {
	cat := newTestCatalog(t)
	_, _, err := cat.IndexScan("nonexistent", 1)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestIndex_ManyRows(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("items", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "val", Type: tuple.TypeText},
	})
	cat.CreateIndex("idx_items_id", "items", "id", "btree")

	n := 500
	for i := 0; i < n; i++ {
		cat.InsertInto("items", []tuple.Datum{
			tuple.DInt32(int32(i)),
			tuple.DText("item"),
		})
	}

	// Spot-check via index.
	for _, k := range []int32{0, 1, 50, 250, 499} {
		tuples, _, err := cat.IndexScan("idx_items_id", int64(k))
		if err != nil {
			t.Fatal(err)
		}
		if len(tuples) != 1 {
			t.Fatalf("key %d: expected 1, got %d", k, len(tuples))
		}
		if tuples[0].Columns[0].I32 != k {
			t.Fatalf("key %d: got id %d", k, tuples[0].Columns[0].I32)
		}
	}

	// Missing key.
	tuples, _, _ := cat.IndexScan("idx_items_id", int64(n+1))
	if len(tuples) != 0 {
		t.Fatal("should not find nonexistent key")
	}
}

func TestIndex_SeqScanVsIndexScan(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("users", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "name", Type: tuple.TypeText},
	})
	cat.CreateIndex("idx_users_id", "users", "id", "btree")

	cat.InsertInto("users", []tuple.Datum{tuple.DInt32(1), tuple.DText("Alice")})
	cat.InsertInto("users", []tuple.Datum{tuple.DInt32(2), tuple.DText("Bob")})
	cat.InsertInto("users", []tuple.Datum{tuple.DInt32(3), tuple.DText("Charlie")})

	// SeqScan should return all 3
	seqCount := 0
	cat.SeqScan("users", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		seqCount++
		return true
	})
	if seqCount != 3 {
		t.Fatalf("seqscan: expected 3, got %d", seqCount)
	}

	// IndexScan for key 2 should return exactly Bob
	tuples, _, _ := cat.IndexScan("idx_users_id", 2)
	if len(tuples) != 1 || tuples[0].Columns[1].Text != "Bob" {
		t.Fatal("indexscan mismatch")
	}
}

func TestIndex_DuplicateKeys(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("events", []ColumnDef{
		{Name: "code", Type: tuple.TypeInt32},
		{Name: "msg", Type: tuple.TypeText},
	})
	cat.CreateIndex("idx_events_code", "events", "code", "btree")

	cat.InsertInto("events", []tuple.Datum{tuple.DInt32(100), tuple.DText("a")})
	cat.InsertInto("events", []tuple.Datum{tuple.DInt32(100), tuple.DText("b")})
	cat.InsertInto("events", []tuple.Datum{tuple.DInt32(100), tuple.DText("c")})

	tuples, _, err := cat.IndexScan("idx_events_code", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 3 {
		t.Fatalf("expected 3 for duplicate key, got %d", len(tuples))
	}
}

func TestIndex_Int64Key(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("big", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt64},
		{Name: "val", Type: tuple.TypeText},
	})
	cat.CreateIndex("idx_big_id", "big", "id", "btree")

	cat.InsertInto("big", []tuple.Datum{tuple.DInt64(9999999999), tuple.DText("large")})

	tuples, _, err := cat.IndexScan("idx_big_id", 9999999999)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 1 {
		t.Fatalf("expected 1, got %d", len(tuples))
	}
	if tuples[0].Columns[1].Text != "large" {
		t.Fatal("wrong value")
	}
}

func TestIndex_PersistAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/test.lodb"

	eng, _ := openEngine(t, path)
	cat, _ := New(eng)
	cat.CreateTable("users", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "name", Type: tuple.TypeText},
	})
	cat.CreateIndex("idx_users_id", "users", "id", "btree")
	cat.InsertInto("users", []tuple.Datum{tuple.DInt32(42), tuple.DText("Alice")})
	eng.Close()

	eng2, _ := openEngine(t, path)
	defer eng2.Close()
	cat2, _ := New(eng2)

	tuples, _, err := cat2.IndexScan("idx_users_id", 42)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 1 {
		t.Fatalf("expected 1 after restart, got %d", len(tuples))
	}
	if tuples[0].Columns[1].Text != "Alice" {
		t.Fatal("wrong value after restart")
	}
}

func TestIndex_MultipleIndexesOnTable(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("data", []ColumnDef{
		{Name: "a", Type: tuple.TypeInt32},
		{Name: "b", Type: tuple.TypeInt32},
	})
	cat.CreateIndex("idx_a", "data", "a", "btree")
	cat.CreateIndex("idx_b", "data", "b", "btree")

	cat.InsertInto("data", []tuple.Datum{tuple.DInt32(1), tuple.DInt32(10)})
	cat.InsertInto("data", []tuple.Datum{tuple.DInt32(2), tuple.DInt32(20)})

	// Search by a
	tuples, _, _ := cat.IndexScan("idx_a", 1)
	if len(tuples) != 1 || tuples[0].Columns[1].I32 != 10 {
		t.Fatal("idx_a lookup failed")
	}

	// Search by b
	tuples, _, _ = cat.IndexScan("idx_b", 20)
	if len(tuples) != 1 || tuples[0].Columns[0].I32 != 2 {
		t.Fatal("idx_b lookup failed")
	}
}
