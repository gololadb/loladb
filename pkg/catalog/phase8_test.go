package catalog

import (
	"testing"

	"github.com/gololadb/loladb/pkg/engine/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
)

func TestUpdate_Basic(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("users", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "name", Type: tuple.TypeText},
	})

	id, _ := cat.InsertInto("users", []tuple.Datum{tuple.DInt32(1), tuple.DText("Alice")})

	newID, err := cat.Update("users", id, []tuple.Datum{tuple.DInt32(1), tuple.DText("Alicia")})
	if err != nil {
		t.Fatal(err)
	}
	if newID == id {
		t.Fatal("update should return a new ItemID")
	}

	var names []string
	cat.SeqScan("users", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		names = append(names, tup.Columns[1].Text)
		return true
	})
	if len(names) != 1 || names[0] != "Alicia" {
		t.Fatalf("expected [Alicia], got %v", names)
	}
}

func TestUpdate_WrongColumnCount(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("t", []ColumnDef{{Name: "a", Type: tuple.TypeInt32}})
	id, _ := cat.InsertInto("t", []tuple.Datum{tuple.DInt32(1)})

	_, err := cat.Update("t", id, []tuple.Datum{tuple.DInt32(1), tuple.DText("extra")})
	if err == nil {
		t.Fatal("expected error for wrong column count")
	}
}

func TestUpdate_WrongType(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("t", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "name", Type: tuple.TypeText},
	})
	id, _ := cat.InsertInto("t", []tuple.Datum{tuple.DInt32(1), tuple.DText("x")})

	_, err := cat.Update("t", id, []tuple.Datum{tuple.DText("wrong"), tuple.DText("x")})
	if err == nil {
		t.Fatal("expected type error")
	}
}

func TestUpdate_Multiple(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("items", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "status", Type: tuple.TypeText},
	})

	ids := make([]slottedpage.ItemID, 3)
	for i := 0; i < 3; i++ {
		ids[i], _ = cat.InsertInto("items", []tuple.Datum{tuple.DInt32(int32(i)), tuple.DText("pending")})
	}

	// Update each one
	for i, id := range ids {
		cat.Update("items", id, []tuple.Datum{tuple.DInt32(int32(i)), tuple.DText("done")})
	}

	var statuses []string
	cat.SeqScan("items", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		statuses = append(statuses, tup.Columns[1].Text)
		return true
	})
	if len(statuses) != 3 {
		t.Fatalf("expected 3, got %d", len(statuses))
	}
	for i, s := range statuses {
		if s != "done" {
			t.Fatalf("row %d: expected done, got %s", i, s)
		}
	}
}

func TestRangeScan_Basic(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("data", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "val", Type: tuple.TypeText},
	})
	cat.CreateIndex("idx_data_id", "data", "id", "btree")

	for i := 0; i < 100; i++ {
		cat.InsertInto("data", []tuple.Datum{tuple.DInt32(int32(i)), tuple.DText("v")})
	}

	tuples, _, err := cat.RangeScan("idx_data_id", 10, 19)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 10 {
		t.Fatalf("expected 10, got %d", len(tuples))
	}
	for _, tup := range tuples {
		id := tup.Columns[0].I32
		if id < 10 || id > 19 {
			t.Fatalf("out of range: %d", id)
		}
	}
}

func TestRangeScan_Empty(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("data", []ColumnDef{{Name: "id", Type: tuple.TypeInt32}})
	cat.CreateIndex("idx", "data", "id", "btree")

	for i := 0; i < 10; i++ {
		cat.InsertInto("data", []tuple.Datum{tuple.DInt32(int32(i))})
	}

	tuples, _, _ := cat.RangeScan("idx", 100, 200)
	if len(tuples) != 0 {
		t.Fatalf("expected 0, got %d", len(tuples))
	}
}

func TestRangeScan_SingleValue(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("data", []ColumnDef{{Name: "id", Type: tuple.TypeInt32}})
	cat.CreateIndex("idx", "data", "id", "btree")

	for i := 0; i < 50; i++ {
		cat.InsertInto("data", []tuple.Datum{tuple.DInt32(int32(i))})
	}

	tuples, _, _ := cat.RangeScan("idx", 25, 25)
	if len(tuples) != 1 {
		t.Fatalf("expected 1, got %d", len(tuples))
	}
}

func TestStats_Basic(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("data", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
	})

	for i := 0; i < 10; i++ {
		cat.InsertInto("data", []tuple.Datum{tuple.DInt32(int32(i))})
	}

	stats, err := cat.Stats("data")
	if err != nil {
		t.Fatal(err)
	}
	if stats.TupleCount != 10 {
		t.Fatalf("expected 10 tuples, got %d", stats.TupleCount)
	}
	if stats.RelPages < 1 {
		t.Fatal("expected at least 1 page")
	}
}

func TestStats_WithDeadTuples(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("data", []ColumnDef{{Name: "id", Type: tuple.TypeInt32}})

	ids := make([]slottedpage.ItemID, 5)
	for i := 0; i < 5; i++ {
		ids[i], _ = cat.InsertInto("data", []tuple.Datum{tuple.DInt32(int32(i))})
	}
	cat.Delete("data", ids[1])
	cat.Delete("data", ids[3])

	stats, _ := cat.Stats("data")
	if stats.TupleCount != 3 {
		t.Fatalf("expected 3 live, got %d", stats.TupleCount)
	}
	if stats.DeadCount != 2 {
		t.Fatalf("expected 2 dead, got %d", stats.DeadCount)
	}
}

func TestStats_TableNotFound(t *testing.T) {
	cat := newTestCatalog(t)
	_, err := cat.Stats("nonexistent")
	if err == nil {
		t.Fatal("expected error")
	}
}
