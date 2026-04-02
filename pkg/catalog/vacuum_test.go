package catalog

import (
	"testing"

	"github.com/gololadb/loladb/pkg/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
)

func TestVacuum_Basic(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("data", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "val", Type: tuple.TypeText},
	})

	// Insert 5 rows.
	ids := make([]slottedpage.ItemID, 5)
	for i := 0; i < 5; i++ {
		id, err := cat.InsertInto("data", []tuple.Datum{
			tuple.DInt32(int32(i)), tuple.DText("test"),
		})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}

	// Delete 3 of them.
	cat.Delete("data", ids[1])
	cat.Delete("data", ids[2])
	cat.Delete("data", ids[4])

	// Vacuum.
	result, err := cat.Vacuum("data")
	if err != nil {
		t.Fatal(err)
	}

	if result.TuplesRemoved != 3 {
		t.Fatalf("expected 3 removed, got %d", result.TuplesRemoved)
	}
	if result.PagesCompacted < 1 {
		t.Fatal("expected at least 1 compacted page")
	}

	// Verify only 2 rows remain visible.
	count := 0
	cat.SeqScan("data", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		count++
		return true
	})
	if count != 2 {
		t.Fatalf("expected 2 visible after vacuum, got %d", count)
	}
}

func TestVacuum_NoDeadTuples(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("data", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
	})

	for i := 0; i < 5; i++ {
		cat.InsertInto("data", []tuple.Datum{tuple.DInt32(int32(i))})
	}

	result, err := cat.Vacuum("data")
	if err != nil {
		t.Fatal(err)
	}
	if result.TuplesRemoved != 0 {
		t.Fatalf("expected 0 removed, got %d", result.TuplesRemoved)
	}

	count := 0
	cat.SeqScan("data", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		count++
		return true
	})
	if count != 5 {
		t.Fatalf("expected 5, got %d", count)
	}
}

func TestVacuum_ReclaimsSpace(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("data", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "payload", Type: tuple.TypeText},
	})

	// Fill a page.
	var lastID slottedpage.ItemID
	for i := 0; i < 50; i++ {
		id, _ := cat.InsertInto("data", []tuple.Datum{
			tuple.DInt32(int32(i)),
			tuple.DText("some padding data here to fill the page"),
		})
		lastID = id
	}

	// Record free space before vacuum.
	rel, _ := cat.FindRelation("data")
	buf, _ := cat.Eng.Pool.FetchPage(uint32(rel.HeadPage))
	sp, _ := slottedpage.FromBytes(buf)
	fsBefore := sp.FreeSpace()
	cat.Eng.Pool.ReleasePage(uint32(rel.HeadPage))

	// Delete all but one.
	for i := 0; i < 50; i++ {
		id := slottedpage.ItemID{Page: lastID.Page, Slot: uint16(i)}
		if i == 0 {
			continue // keep the first one
		}
		cat.Delete("data", id)
	}

	cat.Vacuum("data")

	// Free space should have increased on the first page.
	buf2, _ := cat.Eng.Pool.FetchPage(uint32(rel.HeadPage))
	sp2, _ := slottedpage.FromBytes(buf2)
	fsAfter := sp2.FreeSpace()
	cat.Eng.Pool.ReleasePage(uint32(rel.HeadPage))

	if fsAfter <= fsBefore {
		t.Fatalf("vacuum should reclaim space: before=%d after=%d", fsBefore, fsAfter)
	}
}

func TestVacuum_InsertAfterVacuum(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("data", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
	})

	// Insert and delete.
	for i := 0; i < 20; i++ {
		id, _ := cat.InsertInto("data", []tuple.Datum{tuple.DInt32(int32(i))})
		cat.Delete("data", id)
	}

	cat.Vacuum("data")

	// Insert new rows — should reuse reclaimed space.
	for i := 100; i < 110; i++ {
		_, err := cat.InsertInto("data", []tuple.Datum{tuple.DInt32(int32(i))})
		if err != nil {
			t.Fatalf("insert after vacuum failed: %v", err)
		}
	}

	count := 0
	cat.SeqScan("data", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		count++
		return true
	})
	if count != 10 {
		t.Fatalf("expected 10, got %d", count)
	}
}

func TestVacuum_ActiveTxPreventsCleaning(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("data", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
	})

	id, _ := cat.InsertInto("data", []tuple.Datum{tuple.DInt32(1)})

	// Start a long-running transaction (keeps horizon high).
	longTx := cat.Eng.TxMgr.Begin()

	// Delete and commit the tuple.
	cat.Delete("data", id)

	// Vacuum should NOT remove the tuple because longTx is still active
	// and its horizon prevents cleanup.
	result, _ := cat.Vacuum("data")
	if result.TuplesRemoved != 0 {
		t.Fatalf("expected 0 removed (active tx blocks vacuum), got %d", result.TuplesRemoved)
	}

	// End the long transaction.
	cat.Eng.TxMgr.Commit(longTx)

	// Now vacuum should clean it up.
	result2, _ := cat.Vacuum("data")
	if result2.TuplesRemoved != 1 {
		t.Fatalf("expected 1 removed after tx committed, got %d", result2.TuplesRemoved)
	}
}

func TestVacuum_MultiPageFreeEmptyPages(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("data", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "payload", Type: tuple.TypeText},
	})

	// Insert enough rows to span multiple pages.
	n := 200
	allIDs := make([]slottedpage.ItemID, n)
	for i := 0; i < n; i++ {
		id, err := cat.InsertInto("data", []tuple.Datum{
			tuple.DInt32(int32(i)),
			tuple.DText("some data to fill pages faster and faster"),
		})
		if err != nil {
			t.Fatal(err)
		}
		allIDs[i] = id
	}

	rel, _ := cat.FindRelation("data")
	pagesBefore := rel.Pages

	// Delete all rows.
	for _, id := range allIDs {
		cat.Delete("data", id)
	}

	result, err := cat.Vacuum("data")
	if err != nil {
		t.Fatal(err)
	}

	if result.TuplesRemoved != n {
		t.Fatalf("expected %d removed, got %d", n, result.TuplesRemoved)
	}

	// Some pages should have been freed (all except possibly the head).
	if result.PagesFreed == 0 && pagesBefore > 1 {
		t.Fatalf("expected some pages freed (had %d pages)", pagesBefore)
	}

	// No visible rows.
	count := 0
	cat.SeqScan("data", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		count++
		return true
	})
	if count != 0 {
		t.Fatalf("expected 0 visible after deleting all, got %d", count)
	}
}

func TestVacuum_TableNotFound(t *testing.T) {
	cat := newTestCatalog(t)
	_, err := cat.Vacuum("nonexistent")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestVacuum_EmptyTable(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("empty", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
	})

	result, err := cat.Vacuum("empty")
	if err != nil {
		t.Fatal(err)
	}
	if result.TuplesScanned != 0 || result.TuplesRemoved != 0 {
		t.Fatalf("empty table: unexpected stats %+v", result)
	}
}

func TestVacuum_RepeatedVacuum(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("data", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
	})

	id, _ := cat.InsertInto("data", []tuple.Datum{tuple.DInt32(1)})
	cat.Delete("data", id)

	r1, _ := cat.Vacuum("data")
	if r1.TuplesRemoved != 1 {
		t.Fatalf("first vacuum: expected 1 removed, got %d", r1.TuplesRemoved)
	}

	// Second vacuum should find nothing to do.
	r2, _ := cat.Vacuum("data")
	if r2.TuplesRemoved != 0 {
		t.Fatalf("second vacuum: expected 0 removed, got %d", r2.TuplesRemoved)
	}
}

func TestVacuum_DeleteInsertVacuumCycle(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("cycle", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
	})

	// Repeated insert-delete-vacuum cycles.
	for cycle := 0; cycle < 5; cycle++ {
		var ids []slottedpage.ItemID
		for i := 0; i < 20; i++ {
			id, _ := cat.InsertInto("cycle", []tuple.Datum{
				tuple.DInt32(int32(cycle*100 + i)),
			})
			ids = append(ids, id)
		}
		for _, id := range ids {
			cat.Delete("cycle", id)
		}
		result, err := cat.Vacuum("cycle")
		if err != nil {
			t.Fatal(err)
		}
		if result.TuplesRemoved != 20 {
			t.Fatalf("cycle %d: expected 20 removed, got %d", cycle, result.TuplesRemoved)
		}
	}

	// Table should be empty.
	count := 0
	cat.SeqScan("cycle", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		count++
		return true
	})
	if count != 0 {
		t.Fatalf("expected 0 after all cycles, got %d", count)
	}
}
