package brin

import (
	"testing"

	"github.com/gololadb/loladb/pkg/engine/index"
	"github.com/gololadb/loladb/pkg/engine/pageio"
	"github.com/gololadb/loladb/pkg/engine/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
)

type memAllocator struct {
	pages    map[uint32][]byte
	nextPage uint32
}

func newMemAllocator() *memAllocator {
	return &memAllocator{pages: make(map[uint32][]byte), nextPage: 10}
}

func (m *memAllocator) AllocPage() (uint32, error) {
	pn := m.nextPage
	m.nextPage++
	m.pages[pn] = make([]byte, pageio.PageSize)
	return pn, nil
}

func (m *memAllocator) FetchPage(pageNum uint32) ([]byte, error) {
	buf, ok := m.pages[pageNum]
	if !ok {
		m.pages[pageNum] = make([]byte, pageio.PageSize)
		buf = m.pages[pageNum]
	}
	return buf, nil
}

func (m *memAllocator) ReleasePage(pageNum uint32) {}
func (m *memAllocator) MarkDirty(pageNum uint32)   {}

func setupAM(t *testing.T) (*AM, uint32) {
	t.Helper()
	alloc := newMemAllocator()
	am := NewAM(alloc)
	root, err := am.InitRootPage()
	if err != nil {
		t.Fatal(err)
	}
	return am, root
}

func searchAll(t *testing.T, am *AM, root uint32, keys []index.ScanKey) []slottedpage.ItemID {
	t.Helper()
	scan := am.BeginScan(root)
	defer scan.End()
	scan.Rescan(keys)
	var results []slottedpage.ItemID
	for {
		tid, ok, err := scan.Next(index.ForwardScan)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		results = append(results, tid)
	}
	return results
}

func TestInsertAndSearch_Single(t *testing.T) {
	am, root := setupAM(t)
	root, err := am.Insert(root, tuple.DInt64(42), slottedpage.ItemID{Page: 3, Slot: 0})
	if err != nil {
		t.Fatal(err)
	}
	results := searchAll(t, am, root, []index.ScanKey{
		{AttrNum: 1, Strategy: index.StrategyEqual, Value: tuple.DInt64(42)},
	})
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func TestSearch_NotFound(t *testing.T) {
	am, root := setupAM(t)
	root, _ = am.Insert(root, tuple.DInt64(10), slottedpage.ItemID{Page: 1, Slot: 0})
	root, _ = am.Insert(root, tuple.DInt64(20), slottedpage.ItemID{Page: 2, Slot: 0})
	results := searchAll(t, am, root, []index.ScanKey{
		{AttrNum: 1, Strategy: index.StrategyEqual, Value: tuple.DInt64(15)},
	})
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestInsertAndSearch_Multiple(t *testing.T) {
	am, root := setupAM(t)
	for i := int64(0); i < 50; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(i), slottedpage.ItemID{Page: uint32(i), Slot: 0})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := int64(0); i < 50; i++ {
		results := searchAll(t, am, root, []index.ScanKey{
			{AttrNum: 1, Strategy: index.StrategyEqual, Value: tuple.DInt64(i)},
		})
		if len(results) != 1 {
			t.Fatalf("key %d: expected 1, got %d", i, len(results))
		}
	}
}

func TestRangeScan(t *testing.T) {
	am, root := setupAM(t)
	for i := int64(0); i < 50; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(i), slottedpage.ItemID{Page: uint32(i), Slot: 0})
		if err != nil {
			t.Fatal(err)
		}
	}
	// Range [10, 20].
	results := searchAll(t, am, root, []index.ScanKey{
		{AttrNum: 1, Strategy: index.StrategyGreaterEqual, Value: tuple.DInt64(10)},
		{AttrNum: 1, Strategy: index.StrategyLessEqual, Value: tuple.DInt64(20)},
	})
	if len(results) != 11 {
		t.Fatalf("expected 11 results for [10,20], got %d", len(results))
	}
}

func TestFullScan(t *testing.T) {
	am, root := setupAM(t)
	n := 30
	for i := 0; i < n; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(int64(i)), slottedpage.ItemID{Page: uint32(i), Slot: 0})
		if err != nil {
			t.Fatal(err)
		}
	}
	results := searchAll(t, am, root, nil)
	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
}

func TestMultipleRanges(t *testing.T) {
	am, root := setupAM(t)
	// Insert more than PagesPerRange to create multiple ranges.
	n := PagesPerRange + 50
	for i := 0; i < n; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(int64(i)), slottedpage.ItemID{Page: uint32(i), Slot: 0})
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	// Verify all findable.
	for i := 0; i < n; i++ {
		results := searchAll(t, am, root, []index.ScanKey{
			{AttrNum: 1, Strategy: index.StrategyEqual, Value: tuple.DInt64(int64(i))},
		})
		if len(results) != 1 {
			t.Fatalf("key %d: expected 1, got %d", i, len(results))
		}
	}
}

func TestCapabilities(t *testing.T) {
	am := NewAM(newMemAllocator())
	if am.CanOrder() {
		t.Fatal("brin should not support ordering")
	}
	if am.CanUnique() {
		t.Fatal("brin should not support unique")
	}
}
