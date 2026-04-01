package gin

import (
	"math/rand"
	"testing"

	"github.com/jespino/loladb/pkg/index"
	"github.com/jespino/loladb/pkg/pageio"
	"github.com/jespino/loladb/pkg/slottedpage"
	"github.com/jespino/loladb/pkg/tuple"
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
	results := searchAll(t, am, root, 99)
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestInsert_DuplicateKeys(t *testing.T) {
	am, root := setupAM(t)
	// Same key, different TIDs — should all go to the same posting list.
	for i := 0; i < 10; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(42), slottedpage.ItemID{Page: uint32(i), Slot: uint16(i)})
		if err != nil {
			t.Fatal(err)
		}
	}
	results := searchAll(t, am, root, 42)
	if len(results) != 10 {
		t.Fatalf("expected 10 results, got %d", len(results))
	}
}

func TestInsertAndSearch_Multiple(t *testing.T) {
	am, root := setupAM(t)
	for i := int64(0); i < 100; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(i), slottedpage.ItemID{Page: uint32(i), Slot: 0})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := int64(0); i < 100; i++ {
		results := searchAll(t, am, root, i)
		if len(results) != 1 {
			t.Fatalf("key %d: expected 1, got %d", i, len(results))
		}
	}
}

func TestInsert_RandomOrder(t *testing.T) {
	am, root := setupAM(t)
	n := 200
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

func TestScan_NoKey(t *testing.T) {
	am, root := setupAM(t)
	root, _ = am.Insert(root, tuple.DInt64(1), slottedpage.ItemID{Page: 1, Slot: 0})

	// GIN only supports equality — no key means no results.
	scan := am.BeginScan(root)
	defer scan.End()
	scan.Rescan(nil)
	_, ok, err := scan.Next(index.ForwardScan)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected no results for keyless scan")
	}
}

func TestCapabilities(t *testing.T) {
	am := NewAM(newMemAllocator())
	if am.CanOrder() {
		t.Fatal("gin should not support ordering")
	}
	if am.CanUnique() {
		t.Fatal("gin should not support unique")
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
	data := make([]kv, 50)
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

// TestInvertedIndex verifies the inverted-index property: multiple TIDs per key.
func TestInvertedIndex(t *testing.T) {
	am, root := setupAM(t)

	// Simulate: documents 0-9 each contain tags [0, doc_id%3].
	// So tag 0 appears in all 10 docs, tag 1 in docs 1,4,7, tag 2 in docs 2,5,8.
	for doc := 0; doc < 10; doc++ {
		tid := slottedpage.ItemID{Page: uint32(doc), Slot: 0}
		root, _ = am.Insert(root, tuple.DInt64(0), tid) // tag 0
		root, _ = am.Insert(root, tuple.DInt64(int64(doc%3)), tid)
	}

	// Tag 0 should have 10 + 4 (docs 0,3,6,9 also insert tag 0 twice) = 14? No:
	// doc%3==0 for docs 0,3,6,9 → those insert tag 0 twice.
	// So tag 0: 10 (from first insert) + 4 (from doc%3==0) = 14 TIDs.
	results := searchAll(t, am, root, 0)
	if len(results) != 14 {
		t.Fatalf("tag 0: expected 14 TIDs, got %d", len(results))
	}

	// Tag 1: docs 1,4,7 → 3 TIDs.
	results = searchAll(t, am, root, 1)
	if len(results) != 3 {
		t.Fatalf("tag 1: expected 3 TIDs, got %d", len(results))
	}

	// Tag 2: docs 2,5,8 → 3 TIDs.
	results = searchAll(t, am, root, 2)
	if len(results) != 3 {
		t.Fatalf("tag 2: expected 3 TIDs, got %d", len(results))
	}
}
