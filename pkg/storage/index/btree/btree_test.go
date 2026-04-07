package btree

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/gololadb/loladb/pkg/storage/index"
	"github.com/gololadb/loladb/pkg/storage/pageio"
	"github.com/gololadb/loladb/pkg/storage/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
)

// memAllocator is an in-memory page allocator for testing.
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

// searchAll performs an equality scan and collects all matching TIDs.
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

	newRoot, err := am.Insert(root, tuple.DInt64(42), slottedpage.ItemID{Page: 3, Slot: 0})
	if err != nil {
		t.Fatal(err)
	}

	results := searchAll(t, am, newRoot, 42)
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

	for i := int64(0); i < 20; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(i), slottedpage.ItemID{Page: uint32(i + 10), Slot: uint16(i)})
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := int64(0); i < 20; i++ {
		results := searchAll(t, am, root, i)
		if len(results) != 1 {
			t.Fatalf("key %d: expected 1 result, got %d", i, len(results))
		}
		if results[0].Page != uint32(i+10) {
			t.Fatalf("key %d: wrong Page %d", i, results[0].Page)
		}
	}
}

func TestInsert_CausesSplit(t *testing.T) {
	am, root := setupAM(t)

	n := 300
	for i := 0; i < n; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(int64(i)), slottedpage.ItemID{Page: uint32(i), Slot: uint16(i % 65535)})
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	for i := 0; i < n; i++ {
		results := searchAll(t, am, root, int64(i))
		if len(results) != 1 {
			t.Fatalf("key %d: expected 1 result, got %d", i, len(results))
		}
		if results[0].Page != uint32(i) {
			t.Fatalf("key %d: wrong Page %d, want %d", i, results[0].Page, i)
		}
	}
}

func TestInsert_ReverseOrder(t *testing.T) {
	am, root := setupAM(t)

	n := 300
	for i := n - 1; i >= 0; i-- {
		var err error
		root, err = am.Insert(root, tuple.DInt64(int64(i)), slottedpage.ItemID{Page: uint32(i), Slot: 0})
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

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
	keys := make([]int64, n)
	for i := range keys {
		keys[i] = int64(i)
	}
	rand.Shuffle(n, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	for _, k := range keys {
		var err error
		root, err = am.Insert(root, tuple.DInt64(k), slottedpage.ItemID{Page: uint32(k), Slot: 0})
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
		t.Fatalf("expected 5 results for duplicate key, got %d", len(results))
	}
}

func TestInsert_LargeScale(t *testing.T) {
	am, root := setupAM(t)

	n := 5000
	for i := 0; i < n; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(int64(i)), slottedpage.ItemID{Page: uint32(i), Slot: 0})
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	for _, k := range []int64{0, 1, 100, 999, 2500, 4999} {
		results := searchAll(t, am, root, k)
		if len(results) != 1 {
			t.Fatalf("key %d: expected 1, got %d", k, len(results))
		}
	}

	results := searchAll(t, am, root, int64(n+100))
	if len(results) != 0 {
		t.Fatal("should not find nonexistent key")
	}
}

func TestInsert_NegativeKeys(t *testing.T) {
	am, root := setupAM(t)

	for i := int64(-100); i <= 100; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(i), slottedpage.ItemID{Page: uint32(i + 200), Slot: 0})
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := int64(-100); i <= 100; i++ {
		results := searchAll(t, am, root, i)
		if len(results) != 1 {
			t.Fatalf("key %d: expected 1, got %d", i, len(results))
		}
	}
}

func TestInsert_Int32Key(t *testing.T) {
	am, root := setupAM(t)

	root, err := am.Insert(root, tuple.DInt32(99), slottedpage.ItemID{Page: 7, Slot: 3})
	if err != nil {
		t.Fatal(err)
	}

	results := searchAll(t, am, root, 99)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func TestEntryEncoding(t *testing.T) {
	e := Entry{Key: -42, PageNum: 12345, SlotNum: 789}
	buf := encodeEntry(&e)
	got := decodeEntry(buf)
	if got != e {
		t.Fatalf("roundtrip failed: %+v vs %+v", got, e)
	}
}

func TestInitLeafPage(t *testing.T) {
	p := InitLeafPage(5)
	if p.Type() != slottedpage.PageTypeBTreeLeaf {
		t.Fatal("wrong page type")
	}
	sp := p.Special()
	if getLevel(sp) != 0 {
		t.Fatal("leaf level should be 0")
	}
	if getNumKeys(sp) != 0 {
		t.Fatal("new leaf should have 0 keys")
	}
}

// --- Volcano-style scan tests ---

func TestScan_RangeInclusive(t *testing.T) {
	am, root := setupAM(t)

	for i := int64(0); i < 100; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(i), slottedpage.ItemID{Page: uint32(i), Slot: 0})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Scan [10, 20] inclusive.
	scan := am.BeginScan(root)
	defer scan.End()
	scan.Rescan([]index.ScanKey{
		{AttrNum: 1, Strategy: index.StrategyGreaterEqual, Value: tuple.DInt64(10)},
		{AttrNum: 1, Strategy: index.StrategyLessEqual, Value: tuple.DInt64(20)},
	})

	var keys []int64
	for {
		tid, ok, err := scan.Next(index.ForwardScan)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		keys = append(keys, int64(tid.Page))
	}

	if len(keys) != 11 {
		t.Fatalf("expected 11 results for [10,20], got %d", len(keys))
	}
	for i, k := range keys {
		if k != int64(i+10) {
			t.Fatalf("result[%d] = %d, want %d", i, k, i+10)
		}
	}
}

func TestScan_RangeExclusive(t *testing.T) {
	am, root := setupAM(t)

	for i := int64(0); i < 50; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(i), slottedpage.ItemID{Page: uint32(i), Slot: 0})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Scan (10, 20) exclusive.
	scan := am.BeginScan(root)
	defer scan.End()
	scan.Rescan([]index.ScanKey{
		{AttrNum: 1, Strategy: index.StrategyGreater, Value: tuple.DInt64(10)},
		{AttrNum: 1, Strategy: index.StrategyLess, Value: tuple.DInt64(20)},
	})

	var keys []int64
	for {
		tid, ok, err := scan.Next(index.ForwardScan)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		keys = append(keys, int64(tid.Page))
	}

	if len(keys) != 9 {
		t.Fatalf("expected 9 results for (10,20), got %d: %v", len(keys), keys)
	}
	for i, k := range keys {
		if k != int64(i+11) {
			t.Fatalf("result[%d] = %d, want %d", i, k, i+11)
		}
	}
}

func TestScan_GreaterThan(t *testing.T) {
	am, root := setupAM(t)

	for i := int64(0); i < 20; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(i), slottedpage.ItemID{Page: uint32(i), Slot: 0})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Scan key > 15.
	scan := am.BeginScan(root)
	defer scan.End()
	scan.Rescan([]index.ScanKey{
		{AttrNum: 1, Strategy: index.StrategyGreater, Value: tuple.DInt64(15)},
	})

	var keys []int64
	for {
		tid, ok, err := scan.Next(index.ForwardScan)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		keys = append(keys, int64(tid.Page))
	}

	if len(keys) != 4 {
		t.Fatalf("expected 4 results for >15, got %d: %v", len(keys), keys)
	}
}

func TestScan_LessThan(t *testing.T) {
	am, root := setupAM(t)

	for i := int64(0); i < 20; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(i), slottedpage.ItemID{Page: uint32(i), Slot: 0})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Scan key < 5.
	scan := am.BeginScan(root)
	defer scan.End()
	scan.Rescan([]index.ScanKey{
		{AttrNum: 1, Strategy: index.StrategyLess, Value: tuple.DInt64(5)},
	})

	var keys []int64
	for {
		tid, ok, err := scan.Next(index.ForwardScan)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		keys = append(keys, int64(tid.Page))
	}

	if len(keys) != 5 {
		t.Fatalf("expected 5 results for <5, got %d: %v", len(keys), keys)
	}
}

func TestScan_NoKeys_FullScan(t *testing.T) {
	am, root := setupAM(t)

	n := 50
	for i := 0; i < n; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(int64(i)), slottedpage.ItemID{Page: uint32(i), Slot: 0})
		if err != nil {
			t.Fatal(err)
		}
	}

	// No scan keys — full index scan.
	scan := am.BeginScan(root)
	defer scan.End()
	scan.Rescan(nil)

	count := 0
	for {
		_, ok, err := scan.Next(index.ForwardScan)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		count++
	}

	if count != n {
		t.Fatalf("expected %d results for full scan, got %d", n, count)
	}
}

func TestScan_Rescan(t *testing.T) {
	am, root := setupAM(t)

	for i := int64(0); i < 30; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(i), slottedpage.ItemID{Page: uint32(i), Slot: 0})
		if err != nil {
			t.Fatal(err)
		}
	}

	scan := am.BeginScan(root)
	defer scan.End()

	// First scan: key = 10.
	scan.Rescan([]index.ScanKey{{AttrNum: 1, Strategy: index.StrategyEqual, Value: tuple.DInt64(10)}})
	r1 := 0
	for {
		_, ok, err := scan.Next(index.ForwardScan)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		r1++
	}
	if r1 != 1 {
		t.Fatalf("first scan: expected 1, got %d", r1)
	}

	// Rescan: key = 20.
	scan.Rescan([]index.ScanKey{{AttrNum: 1, Strategy: index.StrategyEqual, Value: tuple.DInt64(20)}})
	r2 := 0
	for {
		_, ok, err := scan.Next(index.ForwardScan)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		r2++
	}
	if r2 != 1 {
		t.Fatalf("rescan: expected 1, got %d", r2)
	}
}

func TestBuild(t *testing.T) {
	alloc := newMemAllocator()
	am := NewAM(alloc)
	root, err := am.InitRootPage()
	if err != nil {
		t.Fatal(err)
	}

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

func TestCapabilities(t *testing.T) {
	am := NewAM(newMemAllocator())
	if !am.CanOrder() {
		t.Fatal("btree should support ordering")
	}
	if !am.CanUnique() {
		t.Fatal("btree should support unique")
	}
	if am.CanBackward() {
		t.Fatal("btree should not support backward scan yet")
	}
}

// --- Leaf linking test (via full scan order) ---

func TestLeafLinking_SortedOrder(t *testing.T) {
	am, root := setupAM(t)

	n := 500
	for i := 0; i < n; i++ {
		var err error
		root, err = am.Insert(root, tuple.DInt64(int64(i)), slottedpage.ItemID{Page: uint32(i), Slot: 0})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Full scan should return keys in sorted order.
	scan := am.BeginScan(root)
	defer scan.End()
	scan.Rescan(nil)

	var allKeys []int64
	for {
		tid, ok, err := scan.Next(index.ForwardScan)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		allKeys = append(allKeys, int64(tid.Page))
	}

	if len(allKeys) != n {
		t.Fatalf("expected %d keys via full scan, got %d", n, len(allKeys))
	}

	if !sort.SliceIsSorted(allKeys, func(i, j int) bool { return allKeys[i] < allKeys[j] }) {
		t.Fatal("full scan keys not sorted")
	}
}

// --- Benchmarks ---

func BenchmarkInsert_Sequential(b *testing.B) {
	for range b.N {
		alloc := newMemAllocator()
		am := NewAM(alloc)
		root, _ := am.InitRootPage()

		for i := 0; i < 10000; i++ {
			root, _ = am.Insert(root, tuple.DInt64(int64(i)), slottedpage.ItemID{Page: uint32(i), Slot: 0})
		}
	}
}

func BenchmarkSearch(b *testing.B) {
	alloc := newMemAllocator()
	am := NewAM(alloc)
	root, _ := am.InitRootPage()

	for i := 0; i < 10000; i++ {
		root, _ = am.Insert(root, tuple.DInt64(int64(i)), slottedpage.ItemID{Page: uint32(i), Slot: 0})
	}

	b.ResetTimer()
	for range b.N {
		k := int64(rand.Intn(10000))
		scan := am.BeginScan(root)
		scan.Rescan([]index.ScanKey{{AttrNum: 1, Strategy: index.StrategyEqual, Value: tuple.DInt64(k)}})
		for {
			_, ok, err := scan.Next(index.ForwardScan)
			if err != nil || !ok {
				break
			}
		}
		scan.End()
	}
}

func TestStress_InsertAndSearchAll(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test")
	}

	am, root := setupAM(t)
	n := 10000

	perm := rand.Perm(n)
	for _, i := range perm {
		var err error
		root, err = am.Insert(root, tuple.DInt64(int64(i)), slottedpage.ItemID{Page: uint32(i), Slot: uint16(i % 65535)})
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	missing := 0
	for i := 0; i < n; i++ {
		results := searchAll(t, am, root, int64(i))
		if len(results) != 1 {
			missing++
			if missing <= 5 {
				t.Logf("key %d: got %d results", i, len(results))
			}
		}
	}
	if missing > 0 {
		t.Fatalf("%d/%d keys missing", missing, n)
	}

	fmt.Printf("B+Tree: %d keys\n", n)
}
