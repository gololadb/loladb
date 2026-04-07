package hash

import (
	"math/rand"
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

func searchAll(t *testing.T, am *AM, root uint32, key int64) []slottedpage.ItemID {
	t.Helper()
	scan := am.BeginScan(root)
	defer scan.End()
	scan.Rescan([]index.ScanKey{{AttrNum: 1, Strategy: index.StrategyEqual, Value: tuple.DInt64(key)}})
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
	results := searchAll(t, am, root, 42)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Page != 3 || results[0].Slot != 0 {
		t.Fatalf("wrong result: %+v", results[0])
	}
}

func TestSearch_NotFound(t *testing.T) {
	am, root := setupAM(t)
	root, _ = am.Insert(root, tuple.DInt64(10), slottedpage.ItemID{Page: 1, Slot: 0})
	root, _ = am.Insert(root, tuple.DInt64(20), slottedpage.ItemID{Page: 2, Slot: 0})
	results := searchAll(t, am, root, 15)
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestInsertAndSearch_Multiple(t *testing.T) {
	am, root := setupAM(t)
	for i := int64(0); i < 100; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(i), slottedpage.ItemID{Page: uint32(i + 10), Slot: uint16(i)})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := int64(0); i < 100; i++ {
		results := searchAll(t, am, root, i)
		if len(results) != 1 {
			t.Fatalf("key %d: expected 1 result, got %d", i, len(results))
		}
		if results[0].Page != uint32(i+10) {
			t.Fatalf("key %d: wrong Page %d", i, results[0].Page)
		}
	}
}

func TestInsert_DuplicateKeys(t *testing.T) {
	am, root := setupAM(t)
	for i := 0; i < 5; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(42), slottedpage.ItemID{Page: uint32(i), Slot: uint16(i)})
		if err != nil {
			t.Fatal(err)
		}
	}
	results := searchAll(t, am, root, 42)
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
}

func TestInsert_TriggersSplit(t *testing.T) {
	am, root := setupAM(t)
	// Insert enough to trigger splits (fillFactor=10, initialBuckets=4 → split at 41).
	n := 200
	for i := 0; i < n; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(int64(i)), slottedpage.ItemID{Page: uint32(i), Slot: 0})
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	// Verify all keys findable.
	for i := 0; i < n; i++ {
		results := searchAll(t, am, root, int64(i))
		if len(results) != 1 {
			t.Fatalf("key %d: expected 1, got %d", i, len(results))
		}
	}
}

func TestInsert_RandomOrder(t *testing.T) {
	am, root := setupAM(t)
	n := 500
	keys := rand.Perm(n)
	for _, k := range keys {
		var err error
		root, err = am.Insert(root, tuple.DInt64(int64(k)), slottedpage.ItemID{Page: uint32(k), Slot: 0})
		if err != nil {
			t.Fatalf("insert %d: %v", k, err)
		}
	}
	for i := 0; i < n; i++ {
		results := searchAll(t, am, root, int64(i))
		if len(results) != 1 {
			t.Fatalf("key %d: expected 1, got %d", i, len(results))
		}
	}
}

func TestInsert_NegativeKeys(t *testing.T) {
	am, root := setupAM(t)
	for i := int64(-50); i <= 50; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(i), slottedpage.ItemID{Page: uint32(i + 100), Slot: 0})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := int64(-50); i <= 50; i++ {
		results := searchAll(t, am, root, i)
		if len(results) != 1 {
			t.Fatalf("key %d: expected 1, got %d", i, len(results))
		}
	}
}

func TestScan_NoEqualKey(t *testing.T) {
	am, root := setupAM(t)
	root, _ = am.Insert(root, tuple.DInt64(1), slottedpage.ItemID{Page: 1, Slot: 0})

	// Range scan should return nothing (hash only supports equality).
	scan := am.BeginScan(root)
	defer scan.End()
	scan.Rescan([]index.ScanKey{
		{AttrNum: 1, Strategy: index.StrategyGreaterEqual, Value: tuple.DInt64(0)},
	})
	_, ok, err := scan.Next(index.ForwardScan)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("hash scan should not return results for non-equality keys")
	}
}

func TestCapabilities(t *testing.T) {
	am := NewAM(newMemAllocator())
	if am.CanOrder() {
		t.Fatal("hash should not support ordering")
	}
	if am.CanUnique() {
		t.Fatal("hash should not support unique")
	}
}

func TestBuild(t *testing.T) {
	alloc := newMemAllocator()
	am := NewAM(alloc)
	root, _ := am.InitRootPage()

	type kv struct {
		key int64
		tid slottedpage.ItemID
	}
	data := make([]kv, 100)
	for i := range data {
		data[i] = kv{key: int64(i), tid: slottedpage.ItemID{Page: uint32(i), Slot: 0}}
	}

	newRoot, err := am.Build(root, func(yield func(key tuple.Datum, tid slottedpage.ItemID) bool) {
		for _, d := range data {
			if !yield(tuple.DInt64(d.key), d.tid) {
				return
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, d := range data {
		results := searchAll(t, am, newRoot, d.key)
		if len(results) != 1 {
			t.Fatalf("key %d: expected 1, got %d", d.key, len(results))
		}
	}
}
