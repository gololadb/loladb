package btree

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/jespino/loladb/pkg/pageio"
	"github.com/jespino/loladb/pkg/slottedpage"
)

// memAllocator is an in-memory page allocator for testing.
type memAllocator struct {
	pages   map[uint32][]byte
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

func setupTree(t *testing.T) (*BTree, *memAllocator) {
	t.Helper()
	alloc := newMemAllocator()
	rootNum, _ := alloc.AllocPage()
	root := InitLeafPage(rootNum)
	buf := alloc.pages[rootNum]
	copy(buf, root.Bytes())
	return New(rootNum, alloc), alloc
}

func TestInsertAndSearch_Single(t *testing.T) {
	bt, _ := setupTree(t)

	if err := bt.Insert(42, 3, 0); err != nil {
		t.Fatal(err)
	}

	results, err := bt.Search(42)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].PageNum != 3 || results[0].SlotNum != 0 {
		t.Fatalf("wrong result: %+v", results[0])
	}
}

func TestSearch_NotFound(t *testing.T) {
	bt, _ := setupTree(t)

	bt.Insert(10, 1, 0)
	bt.Insert(20, 2, 0)

	results, err := bt.Search(15)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestInsertAndSearch_Multiple(t *testing.T) {
	bt, _ := setupTree(t)

	for i := int64(0); i < 20; i++ {
		if err := bt.Insert(i, uint32(i+10), uint16(i)); err != nil {
			t.Fatal(err)
		}
	}

	for i := int64(0); i < 20; i++ {
		results, err := bt.Search(i)
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Fatalf("key %d: expected 1 result, got %d", i, len(results))
		}
		if results[0].PageNum != uint32(i+10) {
			t.Fatalf("key %d: wrong PageNum %d", i, results[0].PageNum)
		}
	}
}

func TestInsert_CausesSplit(t *testing.T) {
	bt, _ := setupTree(t)

	// Insert enough entries to cause at least one leaf split.
	n := 300
	for i := 0; i < n; i++ {
		if err := bt.Insert(int64(i), uint32(i), uint16(i%65535)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// Verify all keys are findable.
	for i := 0; i < n; i++ {
		results, err := bt.Search(int64(i))
		if err != nil {
			t.Fatalf("search %d: %v", i, err)
		}
		if len(results) != 1 {
			t.Fatalf("key %d: expected 1 result, got %d", i, len(results))
		}
		if results[0].PageNum != uint32(i) {
			t.Fatalf("key %d: wrong PageNum %d, want %d", i, results[0].PageNum, i)
		}
	}
}

func TestInsert_ReverseOrder(t *testing.T) {
	bt, _ := setupTree(t)

	n := 300
	for i := n - 1; i >= 0; i-- {
		if err := bt.Insert(int64(i), uint32(i), 0); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	for i := 0; i < n; i++ {
		results, err := bt.Search(int64(i))
		if err != nil {
			t.Fatalf("search %d: %v", i, err)
		}
		if len(results) != 1 {
			t.Fatalf("key %d: expected 1, got %d", i, len(results))
		}
	}
}

func TestInsert_RandomOrder(t *testing.T) {
	bt, _ := setupTree(t)

	n := 500
	keys := make([]int64, n)
	for i := range keys {
		keys[i] = int64(i)
	}
	rand.Shuffle(n, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	for _, k := range keys {
		if err := bt.Insert(k, uint32(k), 0); err != nil {
			t.Fatalf("insert %d: %v", k, err)
		}
	}

	for i := 0; i < n; i++ {
		results, err := bt.Search(int64(i))
		if err != nil {
			t.Fatalf("search %d: %v", i, err)
		}
		if len(results) != 1 {
			t.Fatalf("key %d: expected 1, got %d", i, len(results))
		}
	}
}

func TestInsert_DuplicateKeys(t *testing.T) {
	bt, _ := setupTree(t)

	// Insert the same key 5 times with different ItemIDs.
	for i := 0; i < 5; i++ {
		if err := bt.Insert(42, uint32(i), uint16(i)); err != nil {
			t.Fatal(err)
		}
	}

	results, err := bt.Search(42)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 results for duplicate key, got %d", len(results))
	}
}

func TestInsert_LargeScale(t *testing.T) {
	bt, _ := setupTree(t)

	n := 5000
	for i := 0; i < n; i++ {
		if err := bt.Insert(int64(i), uint32(i), 0); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// Spot-check
	for _, k := range []int64{0, 1, 100, 999, 2500, 4999} {
		results, err := bt.Search(k)
		if err != nil {
			t.Fatalf("search %d: %v", k, err)
		}
		if len(results) != 1 {
			t.Fatalf("key %d: expected 1, got %d", k, len(results))
		}
	}

	// Search for nonexistent key.
	results, err := bt.Search(int64(n + 100))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatal("should not find nonexistent key")
	}
}

func TestInsert_NegativeKeys(t *testing.T) {
	bt, _ := setupTree(t)

	for i := int64(-100); i <= 100; i++ {
		if err := bt.Insert(i, uint32(i+200), 0); err != nil {
			t.Fatal(err)
		}
	}

	for i := int64(-100); i <= 100; i++ {
		results, err := bt.Search(i)
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Fatalf("key %d: expected 1, got %d", i, len(results))
		}
	}
}

func TestRootPageChangesOnSplit(t *testing.T) {
	bt, _ := setupTree(t)
	originalRoot := bt.RootPage()

	// Insert enough to cause a root split.
	for i := 0; i < 500; i++ {
		bt.Insert(int64(i), uint32(i), 0)
	}

	if bt.RootPage() == originalRoot {
		t.Fatal("root page should have changed after splits")
	}
}

func TestLeafLinking(t *testing.T) {
	bt, alloc := setupTree(t)

	// Insert enough to create multiple leaves.
	for i := 0; i < 500; i++ {
		bt.Insert(int64(i), uint32(i), 0)
	}

	// Find the leftmost leaf by descending to key 0.
	results, _ := bt.Search(0)
	if len(results) != 1 {
		t.Fatal("key 0 not found")
	}

	// Walk the root to find a leaf.
	pageNum := bt.RootPage()
	for {
		buf := alloc.pages[pageNum]
		page, _ := slottedpage.FromBytes(buf)
		sp := page.Special()
		if getLevel(sp) == 0 {
			break
		}
		// Descend to leftmost child.
		raw, _ := page.GetTuple(0)
		e := decodeEntry(raw)
		pageNum = e.PageNum
	}

	// Walk leaves via rightPtr and collect all keys.
	var allKeys []int64
	for pageNum != 0 {
		buf := alloc.pages[pageNum]
		page, _ := slottedpage.FromBytes(buf)
		sp := page.Special()
		nkeys := getNumKeys(sp)
		for i := uint16(0); i < nkeys; i++ {
			raw, _ := page.GetTuple(i)
			e := decodeEntry(raw)
			allKeys = append(allKeys, e.Key)
		}
		pageNum = getRightPtr(sp)
	}

	if len(allKeys) != 500 {
		t.Fatalf("expected 500 keys via leaf walk, got %d", len(allKeys))
	}

	// Keys should be sorted.
	if !sort.SliceIsSorted(allKeys, func(i, j int) bool { return allKeys[i] < allKeys[j] }) {
		t.Fatal("leaf keys not sorted")
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

// Benchmark: insert 10000 sequential keys.
func BenchmarkInsert_Sequential(b *testing.B) {
	for range b.N {
		alloc := newMemAllocator()
		rootNum, _ := alloc.AllocPage()
		root := InitLeafPage(rootNum)
		copy(alloc.pages[rootNum], root.Bytes())
		bt := New(rootNum, alloc)

		for i := 0; i < 10000; i++ {
			bt.Insert(int64(i), uint32(i), 0)
		}
	}
}

// Benchmark: search in a tree with 10000 keys.
func BenchmarkSearch(b *testing.B) {
	alloc := newMemAllocator()
	rootNum, _ := alloc.AllocPage()
	root := InitLeafPage(rootNum)
	copy(alloc.pages[rootNum], root.Bytes())
	bt := New(rootNum, alloc)

	for i := 0; i < 10000; i++ {
		bt.Insert(int64(i), uint32(i), 0)
	}

	b.ResetTimer()
	for range b.N {
		bt.Search(int64(rand.Intn(10000)))
	}
}

// Verify search finds correct result after many inserts (stress test).
func TestStress_InsertAndSearchAll(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test")
	}

	bt, _ := setupTree(t)
	n := 10000

	perm := rand.Perm(n)
	for _, i := range perm {
		if err := bt.Insert(int64(i), uint32(i), uint16(i%65535)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	missing := 0
	for i := 0; i < n; i++ {
		results, err := bt.Search(int64(i))
		if err != nil {
			t.Fatal(err)
		}
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

	fmt.Printf("B+Tree: %d keys, root page: %d\n", n, bt.RootPage())
}
