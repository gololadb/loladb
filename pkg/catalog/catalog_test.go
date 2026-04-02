package catalog

import (
	"path/filepath"
	"testing"

	"github.com/gololadb/loladb/pkg/engine"
	"github.com/gololadb/loladb/pkg/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
)

func newTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.lodb")
	eng, err := engine.Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })

	cat, err := New(eng)
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

func TestBootstrap(t *testing.T) {
	cat := newTestCatalog(t)

	if cat.Eng.Super.PgClassPage == 0 {
		t.Fatal("PgClassPage should be set after bootstrap")
	}
	if cat.Eng.Super.PgAttrPage == 0 {
		t.Fatal("PgAttrPage should be set after bootstrap")
	}
}

func TestCreateTable(t *testing.T) {
	cat := newTestCatalog(t)

	oid, err := cat.CreateTable("users", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "name", Type: tuple.TypeText},
		{Name: "email", Type: tuple.TypeText},
	})
	if err != nil {
		t.Fatal(err)
	}
	if oid == 0 {
		t.Fatal("expected non-zero OID")
	}
}

func TestFindRelation(t *testing.T) {
	cat := newTestCatalog(t)

	cat.CreateTable("users", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "name", Type: tuple.TypeText},
	})

	rel, err := cat.FindRelation("users")
	if err != nil {
		t.Fatal(err)
	}
	if rel == nil {
		t.Fatal("expected to find 'users'")
	}
	if rel.Name != "users" {
		t.Fatalf("expected name 'users', got %q", rel.Name)
	}
	if rel.Kind != RelKindTable {
		t.Fatalf("expected kind table, got %d", rel.Kind)
	}
	if rel.HeadPage == 0 {
		t.Fatal("expected non-zero HeadPage")
	}
}

func TestFindRelation_NotFound(t *testing.T) {
	cat := newTestCatalog(t)

	rel, err := cat.FindRelation("nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if rel != nil {
		t.Fatal("expected nil for nonexistent table")
	}
}

func TestGetColumns(t *testing.T) {
	cat := newTestCatalog(t)

	oid, _ := cat.CreateTable("users", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "name", Type: tuple.TypeText},
		{Name: "active", Type: tuple.TypeBool},
	})

	cols, err := cat.GetColumns(oid)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(cols))
	}

	if cols[0].Name != "id" || cols[0].Type != int32(tuple.TypeInt32) || cols[0].Num != 1 {
		t.Fatalf("col 0 mismatch: %+v", cols[0])
	}
	if cols[1].Name != "name" || cols[1].Type != int32(tuple.TypeText) || cols[1].Num != 2 {
		t.Fatalf("col 1 mismatch: %+v", cols[1])
	}
	if cols[2].Name != "active" || cols[2].Type != int32(tuple.TypeBool) || cols[2].Num != 3 {
		t.Fatalf("col 2 mismatch: %+v", cols[2])
	}
}

func TestCreateTable_Duplicate(t *testing.T) {
	cat := newTestCatalog(t)

	cat.CreateTable("users", []ColumnDef{{Name: "id", Type: tuple.TypeInt32}})
	_, err := cat.CreateTable("users", []ColumnDef{{Name: "id", Type: tuple.TypeInt32}})
	if err == nil {
		t.Fatal("expected error for duplicate table name")
	}
}

func TestListTables(t *testing.T) {
	cat := newTestCatalog(t)

	cat.CreateTable("users", []ColumnDef{{Name: "id", Type: tuple.TypeInt32}})
	cat.CreateTable("orders", []ColumnDef{{Name: "id", Type: tuple.TypeInt32}})
	cat.CreateTable("products", []ColumnDef{{Name: "id", Type: tuple.TypeInt32}})

	tables, err := cat.ListTables()
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(tables))
	}

	names := map[string]bool{}
	for _, r := range tables {
		names[r.Name] = true
	}
	for _, n := range []string{"users", "orders", "products"} {
		if !names[n] {
			t.Fatalf("table %q not found in list", n)
		}
	}
}

func TestInsertInto(t *testing.T) {
	cat := newTestCatalog(t)

	cat.CreateTable("users", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "name", Type: tuple.TypeText},
	})

	id, err := cat.InsertInto("users", []tuple.Datum{
		tuple.DInt32(1), tuple.DText("Alice"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if id.Page == 0 && id.Slot == 0 {
		// Could be valid, but let's just check no error.
	}
}

func TestInsertInto_WrongColumnCount(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("users", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "name", Type: tuple.TypeText},
	})

	_, err := cat.InsertInto("users", []tuple.Datum{tuple.DInt32(1)})
	if err == nil {
		t.Fatal("expected error for wrong column count")
	}
}

func TestInsertInto_WrongType(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("users", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "name", Type: tuple.TypeText},
	})

	_, err := cat.InsertInto("users", []tuple.Datum{
		tuple.DText("not-an-int"), tuple.DText("Alice"),
	})
	if err == nil {
		t.Fatal("expected error for wrong type")
	}
}

func TestInsertInto_NullAllowed(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("users", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "name", Type: tuple.TypeText},
	})

	_, err := cat.InsertInto("users", []tuple.Datum{
		tuple.DInt32(1), tuple.DNull(),
	})
	if err != nil {
		t.Fatalf("null should be allowed: %v", err)
	}
}

func TestInsertInto_TableNotFound(t *testing.T) {
	cat := newTestCatalog(t)
	_, err := cat.InsertInto("nonexistent", []tuple.Datum{tuple.DInt32(1)})
	if err == nil {
		t.Fatal("expected error for nonexistent table")
	}
}

func TestSeqScan(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("users", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "name", Type: tuple.TypeText},
	})

	cat.InsertInto("users", []tuple.Datum{tuple.DInt32(1), tuple.DText("Alice")})
	cat.InsertInto("users", []tuple.Datum{tuple.DInt32(2), tuple.DText("Bob")})
	cat.InsertInto("users", []tuple.Datum{tuple.DInt32(3), tuple.DText("Charlie")})

	var names []string
	err := cat.SeqScan("users", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		names = append(names, tup.Columns[1].Text)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 3 {
		t.Fatalf("expected 3, got %d", len(names))
	}
	if names[0] != "Alice" || names[1] != "Bob" || names[2] != "Charlie" {
		t.Fatalf("unexpected: %v", names)
	}
}

func TestDelete_ViaCatalog(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("users", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "name", Type: tuple.TypeText},
	})

	cat.InsertInto("users", []tuple.Datum{tuple.DInt32(1), tuple.DText("Alice")})
	id2, _ := cat.InsertInto("users", []tuple.Datum{tuple.DInt32(2), tuple.DText("Bob")})
	cat.InsertInto("users", []tuple.Datum{tuple.DInt32(3), tuple.DText("Charlie")})

	if err := cat.Delete("users", id2); err != nil {
		t.Fatal(err)
	}

	var names []string
	cat.SeqScan("users", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		names = append(names, tup.Columns[1].Text)
		return true
	})
	if len(names) != 2 {
		t.Fatalf("expected 2 after delete, got %d: %v", len(names), names)
	}
}

func TestMultipleTables_Independent(t *testing.T) {
	cat := newTestCatalog(t)

	cat.CreateTable("users", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "name", Type: tuple.TypeText},
	})
	cat.CreateTable("products", []ColumnDef{
		{Name: "sku", Type: tuple.TypeText},
		{Name: "price", Type: tuple.TypeFloat64},
	})

	cat.InsertInto("users", []tuple.Datum{tuple.DInt32(1), tuple.DText("Alice")})
	cat.InsertInto("products", []tuple.Datum{tuple.DText("ABC"), tuple.DFloat64(9.99)})
	cat.InsertInto("products", []tuple.Datum{tuple.DText("XYZ"), tuple.DFloat64(19.99)})

	userCount := 0
	cat.SeqScan("users", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		userCount++
		return true
	})
	productCount := 0
	cat.SeqScan("products", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		productCount++
		return true
	})

	if userCount != 1 {
		t.Fatalf("users: expected 1, got %d", userCount)
	}
	if productCount != 2 {
		t.Fatalf("products: expected 2, got %d", productCount)
	}
}

func TestPersistAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.lodb")

	// Session 1: create table and insert
	eng, err := engine.Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}
	cat, err := New(eng)
	if err != nil {
		t.Fatal(err)
	}

	cat.CreateTable("users", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "name", Type: tuple.TypeText},
	})
	cat.InsertInto("users", []tuple.Datum{tuple.DInt32(1), tuple.DText("Alice")})
	cat.InsertInto("users", []tuple.Datum{tuple.DInt32(2), tuple.DText("Bob")})
	eng.Close()

	// Session 2: reopen and verify
	eng2, err := engine.Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer eng2.Close()

	cat2, err := New(eng2)
	if err != nil {
		t.Fatal(err)
	}

	tables, _ := cat2.ListTables()
	if len(tables) != 1 {
		t.Fatalf("expected 1 table after restart, got %d", len(tables))
	}
	if tables[0].Name != "users" {
		t.Fatalf("expected 'users', got %q", tables[0].Name)
	}

	cols, _ := cat2.GetColumns(tables[0].OID)
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(cols))
	}

	var names []string
	cat2.SeqScan("users", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		names = append(names, tup.Columns[1].Text)
		return true
	})
	if len(names) != 2 || names[0] != "Alice" || names[1] != "Bob" {
		t.Fatalf("data not persisted: %v", names)
	}
}

func TestCreateMultipleTablesAndVerifyCatalog(t *testing.T) {
	cat := newTestCatalog(t)

	tables := []struct {
		name string
		cols []ColumnDef
	}{
		{"t1", []ColumnDef{{Name: "a", Type: tuple.TypeInt32}}},
		{"t2", []ColumnDef{{Name: "b", Type: tuple.TypeText}, {Name: "c", Type: tuple.TypeBool}}},
		{"t3", []ColumnDef{{Name: "x", Type: tuple.TypeFloat64}, {Name: "y", Type: tuple.TypeInt64}, {Name: "z", Type: tuple.TypeText}}},
	}

	oids := make([]int32, len(tables))
	for i, tbl := range tables {
		oid, err := cat.CreateTable(tbl.name, tbl.cols)
		if err != nil {
			t.Fatal(err)
		}
		oids[i] = oid
	}

	// All OIDs should be unique
	seen := map[int32]bool{}
	for _, oid := range oids {
		if seen[oid] {
			t.Fatal("duplicate OID")
		}
		seen[oid] = true
	}

	// Verify columns for each table
	for i, tbl := range tables {
		cols, err := cat.GetColumns(oids[i])
		if err != nil {
			t.Fatal(err)
		}
		if len(cols) != len(tbl.cols) {
			t.Fatalf("table %q: expected %d cols, got %d", tbl.name, len(tbl.cols), len(cols))
		}
		for j, col := range tbl.cols {
			if cols[j].Name != col.Name {
				t.Fatalf("table %q col %d: expected name %q, got %q", tbl.name, j, col.Name, cols[j].Name)
			}
			if cols[j].Type != int32(col.Type) {
				t.Fatalf("table %q col %d: expected type %d, got %d", tbl.name, j, col.Type, cols[j].Type)
			}
		}
	}

	// Verify FindRelation returns correct head pages
	for _, tbl := range tables {
		rel, err := cat.FindRelation(tbl.name)
		if err != nil {
			t.Fatal(err)
		}
		if rel == nil {
			t.Fatalf("table %q not found", tbl.name)
		}
		if rel.HeadPage == 0 {
			t.Fatalf("table %q: HeadPage should be nonzero", tbl.name)
		}
	}
}
