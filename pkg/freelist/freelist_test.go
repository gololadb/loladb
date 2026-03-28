package freelist

import (
	"path/filepath"
	"testing"

	"github.com/jespino/loladb/pkg/pageio"
)

func TestNew_AllFree(t *testing.T) {
	fl := New(2)
	if fl.FreeCount() != BitsPerPage {
		t.Fatalf("expected all %d pages free, got %d", BitsPerPage, fl.FreeCount())
	}
	if fl.UsedCount() != 0 {
		t.Fatalf("expected 0 used, got %d", fl.UsedCount())
	}
}

func TestAllocSequential(t *testing.T) {
	fl := New(2)

	for i := uint32(0); i < 20; i++ {
		pg, err := fl.Alloc()
		if err != nil {
			t.Fatal(err)
		}
		if pg != i {
			t.Fatalf("expected page %d, got %d", i, pg)
		}
	}

	if fl.UsedCount() != 20 {
		t.Fatalf("expected 20 used, got %d", fl.UsedCount())
	}
}

func TestFreeAndRealloc(t *testing.T) {
	fl := New(2)

	for i := 0; i < 5; i++ {
		fl.Alloc()
	}

	if err := fl.Free(2); err != nil {
		t.Fatal(err)
	}
	if fl.IsUsed(2) {
		t.Fatal("page 2 should be free after Free()")
	}

	pg, err := fl.Alloc()
	if err != nil {
		t.Fatal(err)
	}
	if pg != 2 {
		t.Fatalf("expected reuse of page 2, got %d", pg)
	}
}

func TestMarkUsed(t *testing.T) {
	fl := New(2)

	if err := fl.MarkUsed(100); err != nil {
		t.Fatal(err)
	}
	if !fl.IsUsed(100) {
		t.Fatal("page 100 should be used")
	}
	if fl.IsUsed(99) {
		t.Fatal("page 99 should be free")
	}
	if fl.UsedCount() != 1 {
		t.Fatalf("expected 1 used, got %d", fl.UsedCount())
	}
}

func TestMarkUsedIdempotent(t *testing.T) {
	fl := New(2)
	fl.MarkUsed(10)
	fl.MarkUsed(10)
	if fl.UsedCount() != 1 {
		t.Fatalf("double MarkUsed should still be 1 used, got %d", fl.UsedCount())
	}
}

func TestFreeIdempotent(t *testing.T) {
	fl := New(2)
	fl.MarkUsed(10)
	fl.Free(10)
	fl.Free(10)
	if fl.IsUsed(10) {
		t.Fatal("double Free should leave page free")
	}
}

func TestFreeAlreadyFree(t *testing.T) {
	fl := New(2)
	if err := fl.Free(50); err != nil {
		t.Fatal(err)
	}
	if fl.IsUsed(50) {
		t.Fatal("page 50 should still be free")
	}
}

func TestIsUsed_OutOfRange(t *testing.T) {
	fl := New(2)
	if fl.IsUsed(BitsPerPage) {
		t.Fatal("out-of-range page should return false")
	}
	if fl.IsUsed(BitsPerPage + 1000) {
		t.Fatal("out-of-range page should return false")
	}
}

func TestMarkUsed_OutOfRange(t *testing.T) {
	fl := New(2)
	if err := fl.MarkUsed(BitsPerPage); err == nil {
		t.Fatal("expected error for out-of-range MarkUsed")
	}
}

func TestFree_OutOfRange(t *testing.T) {
	fl := New(2)
	if err := fl.Free(BitsPerPage); err == nil {
		t.Fatal("expected error for out-of-range Free")
	}
}

func TestAllocExhaustion(t *testing.T) {
	fl := New(2)

	// Mark all bits as used
	for i := 0; i < bitmapBytes; i++ {
		fl.pages[0].bits[i] = 0xFF
	}

	_, err := fl.Alloc()
	if err == nil {
		t.Fatal("expected error when all pages used")
	}
}

func TestSaveAndLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.lodb")
	pio, err := pageio.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer pio.Close()

	fl := New(2)
	fl.MarkUsed(0)
	fl.MarkUsed(1)
	fl.MarkUsed(2)
	fl.MarkUsed(50)
	fl.MarkUsed(1000)

	if err := fl.Save(pio); err != nil {
		t.Fatal(err)
	}
	if err := pio.Sync(); err != nil {
		t.Fatal(err)
	}

	fl2, err := Load(2, pio)
	if err != nil {
		t.Fatal(err)
	}

	if fl2.UsedCount() != 5 {
		t.Fatalf("expected 5 used after load, got %d", fl2.UsedCount())
	}
	for _, pg := range []uint32{0, 1, 2, 50, 1000} {
		if !fl2.IsUsed(pg) {
			t.Fatalf("page %d should be used after load", pg)
		}
	}
	if fl2.IsUsed(3) {
		t.Fatal("page 3 should be free after load")
	}
}

func TestLoadPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.lodb")

	pio, err := pageio.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	fl := New(2)
	fl.MarkUsed(0)
	fl.MarkUsed(1)
	fl.MarkUsed(2)
	fl.MarkUsed(7)
	fl.MarkUsed(255)
	fl.Save(pio)
	pio.Sync()
	pio.Close()

	pio2, err := pageio.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer pio2.Close()

	fl2, err := Load(2, pio2)
	if err != nil {
		t.Fatal(err)
	}

	for _, pg := range []uint32{0, 1, 2, 7, 255} {
		if !fl2.IsUsed(pg) {
			t.Fatalf("page %d should be used after reopen", pg)
		}
	}
	if fl2.IsUsed(3) {
		t.Fatal("page 3 should be free after reopen")
	}
}

func TestAllocRespectsReservedPages(t *testing.T) {
	fl := New(2)

	fl.MarkUsed(0)
	fl.MarkUsed(1)
	fl.MarkUsed(2)

	pg, err := fl.Alloc()
	if err != nil {
		t.Fatal(err)
	}
	if pg != 3 {
		t.Fatalf("expected page 3, got %d", pg)
	}
}

func TestAllocAfterFreeGap(t *testing.T) {
	fl := New(2)

	for i := 0; i < 10; i++ {
		fl.Alloc()
	}

	fl.Free(3)
	fl.Free(7)

	pg1, _ := fl.Alloc()
	pg2, _ := fl.Alloc()
	pg3, _ := fl.Alloc()

	if pg1 != 3 {
		t.Fatalf("expected 3, got %d", pg1)
	}
	if pg2 != 7 {
		t.Fatalf("expected 7, got %d", pg2)
	}
	if pg3 != 10 {
		t.Fatalf("expected 10, got %d", pg3)
	}
}

func TestByteBoundary(t *testing.T) {
	fl := New(2)

	fl.MarkUsed(7)
	fl.MarkUsed(8)

	if !fl.IsUsed(7) || !fl.IsUsed(8) {
		t.Fatal("pages at byte boundary should be marked")
	}
	if fl.IsUsed(6) || fl.IsUsed(9) {
		t.Fatal("neighboring pages should be free")
	}
}

func TestAllocMany(t *testing.T) {
	fl := New(2)

	for i := uint32(0); i < 1000; i++ {
		pg, err := fl.Alloc()
		if err != nil {
			t.Fatalf("alloc %d: %v", i, err)
		}
		if pg != i {
			t.Fatalf("expected page %d, got %d", i, pg)
		}
	}

	if fl.UsedCount() != 1000 {
		t.Fatalf("expected 1000 used, got %d", fl.UsedCount())
	}
}

func TestFreeCountAfterMixedOps(t *testing.T) {
	fl := New(2)

	fl.MarkUsed(0)
	fl.MarkUsed(1)
	fl.MarkUsed(2)
	fl.Alloc() // gets page 3 (0,1,2 taken)
	fl.Free(1)

	// Used: 0, 2, 3 = 3 pages
	if fl.UsedCount() != 3 {
		t.Fatalf("expected 3 used, got %d", fl.UsedCount())
	}
	if fl.FreeCount() != BitsPerPage-3 {
		t.Fatalf("expected %d free, got %d", BitsPerPage-3, fl.FreeCount())
	}
}

// --- Multi-page chain tests ---

func TestGrow_ExpandsCapacity(t *testing.T) {
	fl := New(2)

	if fl.Capacity() != BitsPerPage {
		t.Fatalf("expected capacity %d, got %d", BitsPerPage, fl.Capacity())
	}
	if fl.PageCount() != 1 {
		t.Fatalf("expected 1 page, got %d", fl.PageCount())
	}

	fl.Grow(100) // add a second bitmap page at data page 100

	if fl.Capacity() != 2*BitsPerPage {
		t.Fatalf("expected capacity %d, got %d", 2*BitsPerPage, fl.Capacity())
	}
	if fl.PageCount() != 2 {
		t.Fatalf("expected 2 pages, got %d", fl.PageCount())
	}
}

func TestGrow_AllocAcrossPages(t *testing.T) {
	fl := New(2)

	// Fill the first bitmap page completely
	for i := 0; i < bitmapBytes; i++ {
		fl.pages[0].bits[i] = 0xFF
	}

	// Alloc should fail — single page exhausted
	_, err := fl.Alloc()
	if err == nil {
		t.Fatal("expected error before Grow")
	}

	// Grow and alloc again
	fl.Grow(100)

	pg, err := fl.Alloc()
	if err != nil {
		t.Fatal(err)
	}
	// First free bit on second page = BitsPerPage + 0
	if pg != BitsPerPage {
		t.Fatalf("expected page %d, got %d", BitsPerPage, pg)
	}
}

func TestGrow_MarkUsedAcrossPages(t *testing.T) {
	fl := New(2)
	fl.Grow(100)

	target := uint32(BitsPerPage + 42)
	if err := fl.MarkUsed(target); err != nil {
		t.Fatal(err)
	}
	if !fl.IsUsed(target) {
		t.Fatalf("page %d should be used", target)
	}
	if fl.IsUsed(42) {
		t.Fatal("page 42 on first bitmap page should be free")
	}
}

func TestGrow_FreeAcrossPages(t *testing.T) {
	fl := New(2)
	fl.Grow(100)

	target := uint32(BitsPerPage + 10)
	fl.MarkUsed(target)
	fl.Free(target)
	if fl.IsUsed(target) {
		t.Fatal("page should be free after Free")
	}
}

func TestGrow_ThreePages(t *testing.T) {
	fl := New(2)
	fl.Grow(100)
	fl.Grow(200)

	if fl.PageCount() != 3 {
		t.Fatalf("expected 3 pages, got %d", fl.PageCount())
	}
	if fl.Capacity() != 3*BitsPerPage {
		t.Fatalf("expected capacity %d, got %d", 3*BitsPerPage, fl.Capacity())
	}

	// Mark a page on each bitmap page
	fl.MarkUsed(5)
	fl.MarkUsed(BitsPerPage + 5)
	fl.MarkUsed(2*BitsPerPage + 5)

	if fl.UsedCount() != 3 {
		t.Fatalf("expected 3 used, got %d", fl.UsedCount())
	}
	if !fl.IsUsed(5) || !fl.IsUsed(BitsPerPage+5) || !fl.IsUsed(2*BitsPerPage+5) {
		t.Fatal("pages should be marked across all three bitmap pages")
	}
}

func TestGrow_SaveAndLoadChain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.lodb")
	pio, err := pageio.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer pio.Close()

	fl := New(2)
	fl.Grow(50)

	// Mark pages on both bitmap pages
	fl.MarkUsed(0)
	fl.MarkUsed(1)
	fl.MarkUsed(2)
	fl.MarkUsed(BitsPerPage + 100)

	if err := fl.Save(pio); err != nil {
		t.Fatal(err)
	}
	pio.Sync()

	fl2, err := Load(2, pio)
	if err != nil {
		t.Fatal(err)
	}

	if fl2.PageCount() != 2 {
		t.Fatalf("expected 2 pages after load, got %d", fl2.PageCount())
	}
	if fl2.UsedCount() != 4 {
		t.Fatalf("expected 4 used after load, got %d", fl2.UsedCount())
	}
	if !fl2.IsUsed(BitsPerPage + 100) {
		t.Fatal("page on second bitmap page should be used after load")
	}
}

func TestGrow_SaveLoadChainAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.lodb")

	pio, err := pageio.Open(path)
	if err != nil {
		t.Fatal(err)
	}

	fl := New(2)
	fl.Grow(50)
	fl.Grow(80)
	fl.MarkUsed(10)
	fl.MarkUsed(BitsPerPage + 20)
	fl.MarkUsed(2*BitsPerPage + 30)
	fl.Save(pio)
	pio.Sync()
	pio.Close()

	pio2, err := pageio.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer pio2.Close()

	fl2, err := Load(2, pio2)
	if err != nil {
		t.Fatal(err)
	}

	if fl2.PageCount() != 3 {
		t.Fatalf("expected 3 pages, got %d", fl2.PageCount())
	}
	if !fl2.IsUsed(10) || !fl2.IsUsed(BitsPerPage+20) || !fl2.IsUsed(2*BitsPerPage+30) {
		t.Fatal("pages should survive reopen across chain")
	}
}

func TestGrow_AllocFillsFirstPageGapBeforeSecond(t *testing.T) {
	fl := New(2)
	fl.Grow(100)

	// Alloc pages 0-4 on the first bitmap page
	for i := 0; i < 5; i++ {
		fl.Alloc()
	}

	// Free page 2 on first bitmap page
	fl.Free(2)

	// Next alloc should reuse page 2, not jump to second bitmap page
	pg, err := fl.Alloc()
	if err != nil {
		t.Fatal(err)
	}
	if pg != 2 {
		t.Fatalf("expected reuse of page 2, got %d", pg)
	}
}

func TestGrow_OutOfRangeStillErrors(t *testing.T) {
	fl := New(2)
	// Only 1 bitmap page — page BitsPerPage is out of range
	if err := fl.MarkUsed(BitsPerPage); err == nil {
		t.Fatal("expected out-of-range error")
	}
	if err := fl.Free(BitsPerPage); err == nil {
		t.Fatal("expected out-of-range error")
	}

	// After Grow, it should work
	fl.Grow(100)
	if err := fl.MarkUsed(BitsPerPage); err != nil {
		t.Fatalf("should succeed after Grow: %v", err)
	}
}
