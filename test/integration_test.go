package test

import (
	"path/filepath"
	"testing"

	"github.com/gololadb/loladb/pkg/engine/freelist"
	"github.com/gololadb/loladb/pkg/pageio"
	"github.com/gololadb/loladb/pkg/slottedpage"
	"github.com/gololadb/loladb/pkg/engine/superblock"
	"github.com/gololadb/loladb/pkg/tuple"
)

// TestPhase1_EndToEnd exercises the full Phase 1 flow:
// create file → init superblock & freelist → allocate page →
// insert tuples → read back → verify → persist → reopen → verify.
func TestPhase1_EndToEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "phase1.lodb")

	// --- Create and initialise a new database file ---
	pio, err := pageio.Open(path)
	if err != nil {
		t.Fatal(err)
	}

	// Write superblock (page 0)
	sb := superblock.New()
	if err := sb.Save(pio); err != nil {
		t.Fatal(err)
	}

	// Init freelist (page 2) and mark reserved pages
	fl := freelist.New(sb.FreeListPage)
	fl.MarkUsed(0) // superblock
	fl.MarkUsed(1) // WAL control
	fl.MarkUsed(2) // freelist itself

	// Allocate a heap page for our "users" table
	heapPageNum, err := fl.Alloc()
	if err != nil {
		t.Fatal(err)
	}
	if heapPageNum != 3 {
		t.Fatalf("expected first data page to be 3, got %d", heapPageNum)
	}
	sb.TotalPages = 4

	// Save freelist
	if err := fl.Save(pio); err != nil {
		t.Fatal(err)
	}

	// Create a heap page
	heap := slottedpage.Init(slottedpage.PageTypeHeap, heapPageNum, 0)

	// --- Insert tuples ---
	type user struct {
		id    int32
		name  string
		email string
		active bool
	}
	users := []user{
		{1, "Alice", "alice@example.com", true},
		{2, "Bob", "bob@example.com", true},
		{3, "Charlie", "charlie@example.com", false},
	}

	xid := sb.AllocXID()
	slots := make([]uint16, len(users))
	for i, u := range users {
		tup := &tuple.Tuple{
			Xmin: xid,
			Xmax: 0,
			Columns: []tuple.Datum{
				tuple.DInt32(u.id),
				tuple.DText(u.name),
				tuple.DText(u.email),
				tuple.DBool(u.active),
			},
		}
		encoded := tuple.Encode(tup)
		slot, err := heap.InsertTuple(encoded)
		if err != nil {
			t.Fatalf("insert user %d: %v", u.id, err)
		}
		slots[i] = slot
	}

	// --- Read back from in-memory page ---
	for i, u := range users {
		raw, err := heap.GetTuple(slots[i])
		if err != nil {
			t.Fatalf("get tuple slot %d: %v", slots[i], err)
		}
		tup, err := tuple.Decode(raw)
		if err != nil {
			t.Fatalf("decode tuple slot %d: %v", slots[i], err)
		}
		if tup.Xmin != xid {
			t.Fatalf("user %d: xmin %d, expected %d", u.id, tup.Xmin, xid)
		}
		if tup.Columns[0].I32 != u.id {
			t.Fatalf("user %d: id mismatch", u.id)
		}
		if tup.Columns[1].Text != u.name {
			t.Fatalf("user %d: name mismatch", u.id)
		}
		if tup.Columns[2].Text != u.email {
			t.Fatalf("user %d: email mismatch", u.id)
		}
		if tup.Columns[3].Bool != u.active {
			t.Fatalf("user %d: active mismatch", u.id)
		}
	}

	// --- Persist to disk ---
	if err := pio.WritePage(heapPageNum, heap.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := sb.Save(pio); err != nil {
		t.Fatal(err)
	}
	if err := pio.Sync(); err != nil {
		t.Fatal(err)
	}
	pio.Close()

	// --- Reopen and verify ---
	pio2, err := pageio.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer pio2.Close()

	// Load superblock
	sb2, err := superblock.Load(pio2)
	if err != nil {
		t.Fatal(err)
	}
	if sb2.TotalPages != 4 {
		t.Fatalf("superblock totalPages: expected 4, got %d", sb2.TotalPages)
	}

	// Load freelist
	fl2, err := freelist.Load(sb2.FreeListPage, pio2)
	if err != nil {
		t.Fatal(err)
	}
	if !fl2.IsUsed(0) || !fl2.IsUsed(1) || !fl2.IsUsed(2) || !fl2.IsUsed(3) {
		t.Fatal("reserved and data pages should be marked used")
	}
	if fl2.IsUsed(4) {
		t.Fatal("page 4 should be free")
	}

	// Load heap page
	pageBuf := make([]byte, pageio.PageSize)
	if err := pio2.ReadPage(heapPageNum, pageBuf); err != nil {
		t.Fatal(err)
	}
	heap2, err := slottedpage.FromBytes(pageBuf)
	if err != nil {
		t.Fatal(err)
	}

	if heap2.NumSlots() != 3 {
		t.Fatalf("expected 3 slots, got %d", heap2.NumSlots())
	}

	// Verify all tuples survived the restart
	for i, u := range users {
		raw, err := heap2.GetTuple(slots[i])
		if err != nil {
			t.Fatalf("reopen: get tuple slot %d: %v", slots[i], err)
		}
		tup, err := tuple.Decode(raw)
		if err != nil {
			t.Fatalf("reopen: decode tuple slot %d: %v", slots[i], err)
		}
		if tup.Columns[0].I32 != u.id {
			t.Fatalf("reopen: user %d: id %d", u.id, tup.Columns[0].I32)
		}
		if tup.Columns[1].Text != u.name {
			t.Fatalf("reopen: user %d: name %q", u.id, tup.Columns[1].Text)
		}
		if tup.Columns[2].Text != u.email {
			t.Fatalf("reopen: user %d: email %q", u.id, tup.Columns[2].Text)
		}
		if tup.Columns[3].Bool != u.active {
			t.Fatalf("reopen: user %d: active %v", u.id, tup.Columns[3].Bool)
		}
	}
}

// TestPhase1_MultiPageTable verifies allocating multiple heap pages
// and linking them via nextPage.
func TestPhase1_MultiPageTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "multipage.lodb")
	pio, err := pageio.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer pio.Close()

	sb := superblock.New()
	fl := freelist.New(sb.FreeListPage)
	fl.MarkUsed(0)
	fl.MarkUsed(1)
	fl.MarkUsed(2)

	// Allocate first heap page
	pg1Num, _ := fl.Alloc()
	page1 := slottedpage.Init(slottedpage.PageTypeHeap, pg1Num, 0)

	// Fill it up with tuples until it's full
	inserted := 0
	for {
		tup := &tuple.Tuple{
			Xmin:    1,
			Columns: []tuple.Datum{tuple.DInt32(int32(inserted)), tuple.DText("padding data here")},
		}
		encoded := tuple.Encode(tup)
		_, err := page1.InsertTuple(encoded)
		if err != nil {
			break
		}
		inserted++
	}

	if inserted == 0 {
		t.Fatal("should have inserted at least one tuple")
	}

	// Allocate second page and link
	pg2Num, _ := fl.Alloc()
	page1.SetNextPage(pg2Num)
	page2 := slottedpage.Init(slottedpage.PageTypeHeap, pg2Num, 0)

	// Insert one more tuple on page 2
	tup := &tuple.Tuple{
		Xmin:    1,
		Columns: []tuple.Datum{tuple.DInt32(int32(inserted)), tuple.DText("overflow tuple")},
	}
	_, err = page2.InsertTuple(tuple.Encode(tup))
	if err != nil {
		t.Fatal(err)
	}

	// Save everything
	pio.WritePage(pg1Num, page1.Bytes())
	pio.WritePage(pg2Num, page2.Bytes())
	sb.Save(pio)
	fl.Save(pio)
	pio.Sync()

	// Read back and follow the chain
	buf := make([]byte, pageio.PageSize)
	pio.ReadPage(pg1Num, buf)
	p1, _ := slottedpage.FromBytes(buf)

	if p1.NextPage() != pg2Num {
		t.Fatalf("expected nextPage=%d, got %d", pg2Num, p1.NextPage())
	}

	pio.ReadPage(p1.NextPage(), buf)
	p2, _ := slottedpage.FromBytes(buf)

	raw, _ := p2.GetTuple(0)
	decoded, _ := tuple.Decode(raw)
	if decoded.Columns[1].Text != "overflow tuple" {
		t.Fatalf("expected 'overflow tuple', got %q", decoded.Columns[1].Text)
	}
}

// TestPhase1_DeleteAndCompact verifies soft-delete + compaction.
func TestPhase1_DeleteAndCompact(t *testing.T) {
	heap := slottedpage.Init(slottedpage.PageTypeHeap, 3, 0)

	// Insert 5 tuples
	for i := 0; i < 5; i++ {
		tup := &tuple.Tuple{
			Xmin:    1,
			Columns: []tuple.Datum{tuple.DInt32(int32(i)), tuple.DText("test")},
		}
		heap.InsertTuple(tuple.Encode(tup))
	}

	fsBefore := heap.FreeSpace()

	// Delete tuples 1 and 3
	heap.DeleteTuple(1)
	heap.DeleteTuple(3)

	// Compact to reclaim space
	heap.Compact()

	if heap.FreeSpace() <= fsBefore {
		t.Fatal("compact should have reclaimed space")
	}

	// Surviving tuples still correct
	for _, slot := range []uint16{0, 2, 4} {
		raw, err := heap.GetTuple(slot)
		if err != nil {
			t.Fatalf("slot %d: %v", slot, err)
		}
		decoded, err := tuple.Decode(raw)
		if err != nil {
			t.Fatalf("decode slot %d: %v", slot, err)
		}
		if decoded.Columns[0].I32 != int32(slot) {
			t.Fatalf("slot %d: expected id %d, got %d", slot, slot, decoded.Columns[0].I32)
		}
	}

	// Dead slots remain dead
	if heap.SlotIsAlive(1) || heap.SlotIsAlive(3) {
		t.Fatal("deleted slots should be dead")
	}
}
