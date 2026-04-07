package slottedpage

import (
	"testing"

	"github.com/gololadb/loladb/pkg/engine/pageio"
)

func TestInit(t *testing.T) {
	p := Init(PageTypeHeap, 5, 0)

	if p.Type() != PageTypeHeap {
		t.Fatalf("expected type %d, got %d", PageTypeHeap, p.Type())
	}
	if p.PageNum() != 5 {
		t.Fatalf("expected pageNum 5, got %d", p.PageNum())
	}
	if p.NumSlots() != 0 {
		t.Fatalf("expected 0 slots, got %d", p.NumSlots())
	}
	if p.LSN() != 0 {
		t.Fatalf("expected LSN 0, got %d", p.LSN())
	}
	if p.NextPage() != 0 {
		t.Fatalf("expected nextPage 0, got %d", p.NextPage())
	}
	// Free space = PageSize - headerSize (no special area)
	expected := uint16(pageio.PageSize) - headerSize
	if p.FreeSpace() != expected {
		t.Fatalf("expected freeSpace %d, got %d", expected, p.FreeSpace())
	}
}

func TestInitWithSpecial(t *testing.T) {
	p := Init(PageTypeBTreeInt, 10, 16)

	special := p.Special()
	if len(special) != 16 {
		t.Fatalf("expected 16 bytes special, got %d", len(special))
	}
	expected := uint16(pageio.PageSize) - headerSize - 16
	if p.FreeSpace() != expected {
		t.Fatalf("expected freeSpace %d, got %d", expected, p.FreeSpace())
	}
}

func TestInsertAndGetTuple(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)

	data := []byte("hello world")
	slot, err := p.InsertTuple(data)
	if err != nil {
		t.Fatal(err)
	}
	if slot != 0 {
		t.Fatalf("expected slot 0, got %d", slot)
	}

	got, err := p.GetTuple(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello world" {
		t.Fatalf("expected %q, got %q", "hello world", got)
	}
}

func TestInsertMultipleTuples(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)

	tuples := []string{"alpha", "beta", "gamma", "delta"}
	for i, s := range tuples {
		slot, err := p.InsertTuple([]byte(s))
		if err != nil {
			t.Fatal(err)
		}
		if slot != uint16(i) {
			t.Fatalf("expected slot %d, got %d", i, slot)
		}
	}

	if p.NumSlots() != 4 {
		t.Fatalf("expected 4 slots, got %d", p.NumSlots())
	}

	for i, s := range tuples {
		got, err := p.GetTuple(uint16(i))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != s {
			t.Fatalf("slot %d: expected %q, got %q", i, s, got)
		}
	}
}

func TestInsertFull(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)

	// Fill the page with tuples
	for {
		_, err := p.InsertTuple(make([]byte, 100))
		if err != nil {
			break
		}
	}

	// Should have some tuples
	if p.NumSlots() == 0 {
		t.Fatal("should have inserted at least one tuple")
	}

	// Verify all are readable
	for i := uint16(0); i < p.NumSlots(); i++ {
		_, err := p.GetTuple(i)
		if err != nil {
			t.Fatalf("slot %d: %v", i, err)
		}
	}
}

func TestGetTuple_OutOfRange(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)

	_, err := p.GetTuple(0)
	if err == nil {
		t.Fatal("expected error for empty page")
	}

	p.InsertTuple([]byte("x"))
	_, err = p.GetTuple(1)
	if err == nil {
		t.Fatal("expected error for out-of-range slot")
	}
}

func TestDeleteTuple(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)

	p.InsertTuple([]byte("first"))
	p.InsertTuple([]byte("second"))
	p.InsertTuple([]byte("third"))

	if err := p.DeleteTuple(1); err != nil {
		t.Fatal(err)
	}

	if p.SlotIsAlive(1) {
		t.Fatal("slot 1 should be dead after delete")
	}

	// Slot 0 and 2 still alive
	if !p.SlotIsAlive(0) || !p.SlotIsAlive(2) {
		t.Fatal("slots 0 and 2 should still be alive")
	}

	_, err := p.GetTuple(1)
	if err == nil {
		t.Fatal("expected error reading dead slot")
	}
}

func TestDeleteTuple_OutOfRange(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)
	if err := p.DeleteTuple(0); err == nil {
		t.Fatal("expected error for out-of-range delete")
	}
}

func TestCompact(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)

	p.InsertTuple([]byte("aaa"))
	p.InsertTuple([]byte("bbb"))
	p.InsertTuple([]byte("ccc"))
	p.InsertTuple([]byte("ddd"))

	fsBefore := p.FreeSpace()

	// Delete middle tuples
	p.DeleteTuple(1)
	p.DeleteTuple(2)

	// Free space hasn't changed yet (dead tuples still occupy space)
	if p.FreeSpace() != fsBefore {
		t.Fatal("free space should not change after delete (before compact)")
	}

	p.Compact()

	// Free space should have increased by the size of the two dead tuples
	fsAfter := p.FreeSpace()
	if fsAfter <= fsBefore {
		t.Fatalf("compact should reclaim space: before=%d after=%d", fsBefore, fsAfter)
	}

	// Live tuples still readable
	got0, err := p.GetTuple(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(got0) != "aaa" {
		t.Fatalf("slot 0: expected %q, got %q", "aaa", got0)
	}

	got3, err := p.GetTuple(3)
	if err != nil {
		t.Fatal(err)
	}
	if string(got3) != "ddd" {
		t.Fatalf("slot 3: expected %q, got %q", "ddd", got3)
	}

	// Dead slots remain dead
	if p.SlotIsAlive(1) || p.SlotIsAlive(2) {
		t.Fatal("dead slots should remain dead after compact")
	}
}

func TestCompactPreservesSlotNumbering(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)

	for i := 0; i < 10; i++ {
		p.InsertTuple([]byte{byte(i), byte(i), byte(i)})
	}

	// Delete even slots
	for i := uint16(0); i < 10; i += 2 {
		p.DeleteTuple(i)
	}

	p.Compact()

	// NumSlots unchanged
	if p.NumSlots() != 10 {
		t.Fatalf("expected 10 slots, got %d", p.NumSlots())
	}

	// Odd slots still have correct data
	for i := uint16(1); i < 10; i += 2 {
		got, err := p.GetTuple(i)
		if err != nil {
			t.Fatalf("slot %d: %v", i, err)
		}
		if got[0] != byte(i) {
			t.Fatalf("slot %d: expected data %d, got %d", i, i, got[0])
		}
	}
}

func TestCompactEmpty(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)
	fsBefore := p.FreeSpace()
	p.Compact()
	if p.FreeSpace() != fsBefore {
		t.Fatal("compact on empty page should not change free space")
	}
}

func TestCompactNoDeadTuples(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)
	p.InsertTuple([]byte("x"))
	p.InsertTuple([]byte("y"))
	fsBefore := p.FreeSpace()
	p.Compact()
	if p.FreeSpace() != fsBefore {
		t.Fatal("compact with no dead tuples should not change free space")
	}
	got, _ := p.GetTuple(0)
	if string(got) != "x" {
		t.Fatal("data corrupted after compact")
	}
}

func TestInsertAfterCompact(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)

	// Fill up
	for i := 0; i < 20; i++ {
		p.InsertTuple(make([]byte, 100))
	}

	// Delete all and compact
	for i := uint16(0); i < 20; i++ {
		p.DeleteTuple(i)
	}
	p.Compact()

	// Should be able to insert again
	_, err := p.InsertTuple(make([]byte, 100))
	if err != nil {
		t.Fatalf("insert after compact failed: %v", err)
	}
}

func TestFromBytesRoundtrip(t *testing.T) {
	p := Init(PageTypeHeap, 42, 0)
	p.InsertTuple([]byte("hello"))
	p.InsertTuple([]byte("world"))

	buf := p.Bytes()
	p2, err := FromBytes(buf)
	if err != nil {
		t.Fatal(err)
	}

	if p2.PageNum() != 42 {
		t.Fatalf("pageNum mismatch: got %d", p2.PageNum())
	}
	if p2.NumSlots() != 2 {
		t.Fatalf("numSlots mismatch: got %d", p2.NumSlots())
	}

	got, _ := p2.GetTuple(0)
	if string(got) != "hello" {
		t.Fatalf("expected %q, got %q", "hello", got)
	}
	got, _ = p2.GetTuple(1)
	if string(got) != "world" {
		t.Fatalf("expected %q, got %q", "world", got)
	}
}

func TestFromBytes_WrongSize(t *testing.T) {
	_, err := FromBytes(make([]byte, 100))
	if err == nil {
		t.Fatal("expected error for wrong buffer size")
	}
}

func TestSetLSN(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)
	p.SetLSN(999)
	if p.LSN() != 999 {
		t.Fatalf("expected LSN 999, got %d", p.LSN())
	}
}

func TestSetNextPage(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)
	p.SetNextPage(77)
	if p.NextPage() != 77 {
		t.Fatalf("expected nextPage 77, got %d", p.NextPage())
	}
}

func TestSpecialArea(t *testing.T) {
	p := Init(PageTypeBTreeInt, 1, 12)

	special := p.Special()
	if len(special) != 12 {
		t.Fatalf("expected 12 bytes special, got %d", len(special))
	}

	// Write to special area
	special[0] = 0xAA
	special[11] = 0xBB

	// Read back through Bytes roundtrip
	p2, _ := FromBytes(p.Bytes())
	s2 := p2.Special()
	if s2[0] != 0xAA || s2[11] != 0xBB {
		t.Fatal("special area data not preserved")
	}
}

func TestSpecialAreaDoesNotOverlapTuples(t *testing.T) {
	p := Init(PageTypeHeap, 1, 32)

	// Free space should account for special area
	expectedFree := uint16(pageio.PageSize) - headerSize - 32
	if p.FreeSpace() != expectedFree {
		t.Fatalf("expected free %d, got %d", expectedFree, p.FreeSpace())
	}

	// Insert a large tuple that would overlap the special area if
	// it wasn't accounted for
	bigTuple := make([]byte, expectedFree-linePointerSize)
	_, err := p.InsertTuple(bigTuple)
	if err != nil {
		t.Fatal("should fit exactly")
	}

	// One more byte should not fit
	_, err = p.InsertTuple([]byte{0x01})
	if err == nil {
		t.Fatal("should not fit — would overlap special area")
	}
}

func TestSlotIsAlive(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)
	p.InsertTuple([]byte("a"))
	p.InsertTuple([]byte("b"))

	if !p.SlotIsAlive(0) || !p.SlotIsAlive(1) {
		t.Fatal("both slots should be alive")
	}

	p.DeleteTuple(0)
	if p.SlotIsAlive(0) {
		t.Fatal("slot 0 should be dead")
	}
	if !p.SlotIsAlive(1) {
		t.Fatal("slot 1 should still be alive")
	}

	// Out of range
	if p.SlotIsAlive(99) {
		t.Fatal("out-of-range slot should return false")
	}
}

func TestLargeTuples(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)

	// Insert a tuple that takes most of the page
	maxData := int(p.FreeSpace()) - linePointerSize
	data := make([]byte, maxData)
	for i := range data {
		data[i] = byte(i % 251)
	}

	slot, err := p.InsertTuple(data)
	if err != nil {
		t.Fatal(err)
	}

	got, err := p.GetTuple(slot)
	if err != nil {
		t.Fatal(err)
	}
	for i := range got {
		if got[i] != data[i] {
			t.Fatalf("mismatch at byte %d", i)
		}
	}
}

func TestFreeSpaceDecreasesOnInsert(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)
	initial := p.FreeSpace()

	p.InsertTuple([]byte("test"))

	after := p.FreeSpace()
	// Should have decreased by len("test") + linePointerSize
	expected := initial - 4 - linePointerSize
	if after != expected {
		t.Fatalf("expected freeSpace %d, got %d", expected, after)
	}
}

func TestManySmallTuples(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)

	count := 0
	for {
		_, err := p.InsertTuple([]byte{byte(count)})
		if err != nil {
			break
		}
		count++
	}

	// Each tuple is 1 byte data + 4 bytes line pointer = 5 bytes
	// Available: 4096 - 24 = 4072 bytes. 4072/5 = 814.4 → 814 tuples
	if count < 800 || count > 820 {
		t.Fatalf("expected ~814 single-byte tuples, got %d", count)
	}

	// All readable
	for i := 0; i < count; i++ {
		got, err := p.GetTuple(uint16(i))
		if err != nil {
			t.Fatalf("slot %d: %v", i, err)
		}
		if got[0] != byte(i) {
			t.Fatalf("slot %d: expected %d, got %d", i, byte(i), got[0])
		}
	}
}

func TestGetTuple_ReturnsCopy(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)
	p.InsertTuple([]byte("original"))

	got, _ := p.GetTuple(0)
	got[0] = 'X' // mutate the copy

	got2, _ := p.GetTuple(0)
	if got2[0] != 'o' {
		t.Fatal("GetTuple should return a copy, not a reference")
	}
}

func TestBytes_ReturnsCopy(t *testing.T) {
	p := Init(PageTypeHeap, 1, 0)
	buf := p.Bytes()
	buf[0] = 0xFF

	if p.Type() == 0xFF {
		t.Fatal("Bytes should return a copy")
	}
}
