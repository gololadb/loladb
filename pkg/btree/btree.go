package btree

import (
	"encoding/binary"
	"fmt"

	"github.com/jespino/loladb/pkg/pageio"
	"github.com/jespino/loladb/pkg/slottedpage"
)

// Special area layout (12 bytes at the end of each B+Tree page):
//
//	level    (2B): 0 = leaf, >0 = internal
//	numKeys  (2B): number of keys stored
//	rightPtr (4B): leaf: next leaf page; internal: rightmost child page
//	flags    (4B): reserved
const specialSize = 12

const (
	spOffLevel    = 0
	spOffNumKeys  = 2
	spOffRightPtr = 4
	spOffFlags    = 8
)

// Entry is a key-value pair stored in the B+Tree.
// For leaves:   Key → ItemID (heap ctid)
// For internals: Key → child page number
type Entry struct {
	Key       int64
	PageNum   uint32 // internal: child page; leaf: ItemID.Page
	SlotNum   uint16 // leaf only: ItemID.Slot
}

// entrySize is the serialized size of an entry: 8 (key) + 4 (page) + 2 (slot) = 14
const entrySize = 14

func encodeEntry(e *Entry) []byte {
	buf := make([]byte, entrySize)
	binary.LittleEndian.PutUint64(buf[0:], uint64(e.Key))
	binary.LittleEndian.PutUint32(buf[8:], e.PageNum)
	binary.LittleEndian.PutUint16(buf[12:], e.SlotNum)
	return buf
}

func decodeEntry(buf []byte) Entry {
	return Entry{
		Key:     int64(binary.LittleEndian.Uint64(buf[0:])),
		PageNum: binary.LittleEndian.Uint32(buf[8:]),
		SlotNum: binary.LittleEndian.Uint16(buf[12:]),
	}
}

// PageAllocator is the interface the B+Tree uses to allocate new pages
// and read/write pages through the buffer pool.
type PageAllocator interface {
	AllocPage() (uint32, error)
	FetchPage(pageNum uint32) ([]byte, error)
	ReleasePage(pageNum uint32)
	MarkDirty(pageNum uint32)
}

// BTree is an on-disk B+Tree index backed by slotted pages.
type BTree struct {
	rootPage uint32
	alloc    PageAllocator
}

// New creates a new B+Tree with the given root page. The root page
// must already be initialized as a leaf node (call InitLeafPage).
func New(rootPage uint32, alloc PageAllocator) *BTree {
	return &BTree{rootPage: rootPage, alloc: alloc}
}

// RootPage returns the current root page number.
func (bt *BTree) RootPage() uint32 {
	return bt.rootPage
}

// InitLeafPage initializes a page as an empty B+Tree leaf node.
func InitLeafPage(pageNum uint32) *slottedpage.Page {
	p := slottedpage.Init(slottedpage.PageTypeBTreeLeaf, pageNum, specialSize)
	sp := p.Special()
	binary.LittleEndian.PutUint16(sp[spOffLevel:], 0)
	binary.LittleEndian.PutUint16(sp[spOffNumKeys:], 0)
	binary.LittleEndian.PutUint32(sp[spOffRightPtr:], 0)
	return p
}

// initInternalPage initializes a page as an empty B+Tree internal node.
func initInternalPage(pageNum uint32, level uint16) *slottedpage.Page {
	p := slottedpage.Init(slottedpage.PageTypeBTreeInt, pageNum, specialSize)
	sp := p.Special()
	binary.LittleEndian.PutUint16(sp[spOffLevel:], level)
	binary.LittleEndian.PutUint16(sp[spOffNumKeys:], 0)
	binary.LittleEndian.PutUint32(sp[spOffRightPtr:], 0)
	return p
}

// --- Node accessors (work on special area) ---

func getLevel(sp []byte) uint16    { return binary.LittleEndian.Uint16(sp[spOffLevel:]) }
func getNumKeys(sp []byte) uint16  { return binary.LittleEndian.Uint16(sp[spOffNumKeys:]) }
func getRightPtr(sp []byte) uint32 { return binary.LittleEndian.Uint32(sp[spOffRightPtr:]) }

func setNumKeys(sp []byte, n uint16)  { binary.LittleEndian.PutUint16(sp[spOffNumKeys:], n) }
func setRightPtr(sp []byte, p uint32) { binary.LittleEndian.PutUint32(sp[spOffRightPtr:], p) }

// --- Search ---

// Search descends from the root to a leaf and returns all entries
// matching the given key, or nil if not found.
func (bt *BTree) Search(key int64) ([]Entry, error) {
	pageNum := bt.rootPage

	for {
		buf, err := bt.alloc.FetchPage(pageNum)
		if err != nil {
			return nil, err
		}
		page, err := slottedpage.FromBytes(buf)
		if err != nil {
			bt.alloc.ReleasePage(pageNum)
			return nil, err
		}

		sp := page.Special()
		level := getLevel(sp)
		nkeys := getNumKeys(sp)

		if level == 0 {
			// Leaf: collect matching entries.
			var results []Entry
			for i := uint16(0); i < nkeys; i++ {
				raw, err := page.GetTuple(i)
				if err != nil {
					break
				}
				e := decodeEntry(raw)
				if e.Key == key {
					results = append(results, e)
				}
			}
			bt.alloc.ReleasePage(pageNum)
			return results, nil
		}

		// Internal: find the child to descend into.
		childPage := findChild(page, nkeys, sp, key)
		bt.alloc.ReleasePage(pageNum)
		pageNum = childPage
	}
}

// findChild performs binary search on an internal node to find the
// child page for the given key.
func findChild(page *slottedpage.Page, nkeys uint16, sp []byte, key int64) uint32 {
	// Entries in internal nodes: key[i] is the separator.
	// If key < entry[0].Key → go to entry[0].PageNum (left child)
	// If key >= entry[i].Key and key < entry[i+1].Key → go to entry[i+1].PageNum
	// If key >= entry[last].Key → go to rightPtr

	for i := uint16(0); i < nkeys; i++ {
		raw, err := page.GetTuple(i)
		if err != nil {
			break
		}
		e := decodeEntry(raw)
		if key < e.Key {
			return e.PageNum
		}
	}
	return getRightPtr(sp)
}

// RangeScan returns all entries with keys in [lo, hi] (inclusive) by
// descending to the leaf containing lo and then walking rightPtr links.
func (bt *BTree) RangeScan(lo, hi int64) ([]Entry, error) {
	// Find the leaf containing the lo key.
	pageNum := bt.rootPage
	for {
		buf, err := bt.alloc.FetchPage(pageNum)
		if err != nil {
			return nil, err
		}
		page, err := slottedpage.FromBytes(buf)
		if err != nil {
			bt.alloc.ReleasePage(pageNum)
			return nil, err
		}
		sp := page.Special()
		level := getLevel(sp)
		nkeys := getNumKeys(sp)

		if level == 0 {
			bt.alloc.ReleasePage(pageNum)
			break
		}
		childPage := findChild(page, nkeys, sp, lo)
		bt.alloc.ReleasePage(pageNum)
		pageNum = childPage
	}

	// Walk leaves collecting entries in [lo, hi].
	var results []Entry
	cur := pageNum
	for cur != 0 {
		buf, err := bt.alloc.FetchPage(cur)
		if err != nil {
			return results, err
		}
		page, err := slottedpage.FromBytes(buf)
		if err != nil {
			bt.alloc.ReleasePage(cur)
			return results, err
		}
		sp := page.Special()
		nkeys := getNumKeys(sp)
		done := false

		for i := uint16(0); i < nkeys; i++ {
			raw, err := page.GetTuple(i)
			if err != nil {
				break
			}
			e := decodeEntry(raw)
			if e.Key > hi {
				done = true
				break
			}
			if e.Key >= lo {
				results = append(results, e)
			}
		}

		nextPage := getRightPtr(sp)
		bt.alloc.ReleasePage(cur)
		if done {
			break
		}
		cur = nextPage
	}
	return results, nil
}

// --- Insert ---

// splitResult is returned when a node split occurs during insertion.
type splitResult struct {
	medianKey    int64
	newPageNum   uint32
}

// Insert adds a key→ItemID mapping to the B+Tree.
func (bt *BTree) Insert(key int64, itemPage uint32, itemSlot uint16) error {
	sr, err := bt.insertRecursive(bt.rootPage, key, itemPage, itemSlot)
	if err != nil {
		return err
	}

	if sr != nil {
		// Root was split — create a new root.
		newRootNum, err := bt.alloc.AllocPage()
		if err != nil {
			return fmt.Errorf("btree: alloc new root: %w", err)
		}

		// Determine level of new root.
		oldBuf, err := bt.alloc.FetchPage(bt.rootPage)
		if err != nil {
			return err
		}
		oldPage, _ := slottedpage.FromBytes(oldBuf)
		oldLevel := getLevel(oldPage.Special())
		bt.alloc.ReleasePage(bt.rootPage)

		newRoot := initInternalPage(newRootNum, oldLevel+1)
		// First entry: key=median, PageNum=old root (left child)
		e := Entry{Key: sr.medianKey, PageNum: bt.rootPage}
		newRoot.InsertTuple(encodeEntry(&e))
		sp := newRoot.Special()
		setNumKeys(sp, 1)
		setRightPtr(sp, sr.newPageNum) // right child

		buf, err := bt.alloc.FetchPage(newRootNum)
		if err != nil {
			return err
		}
		copy(buf, newRoot.Bytes())
		bt.alloc.MarkDirty(newRootNum)
		bt.alloc.ReleasePage(newRootNum)

		bt.rootPage = newRootNum
	}

	return nil
}

func (bt *BTree) insertRecursive(pageNum uint32, key int64, itemPage uint32, itemSlot uint16) (*splitResult, error) {
	buf, err := bt.alloc.FetchPage(pageNum)
	if err != nil {
		return nil, err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		bt.alloc.ReleasePage(pageNum)
		return nil, err
	}

	sp := page.Special()
	level := getLevel(sp)
	nkeys := getNumKeys(sp)

	if level == 0 {
		// Leaf: insert the entry.
		return bt.insertLeaf(pageNum, buf, page, key, itemPage, itemSlot)
	}

	// Internal: descend.
	childPage := findChild(page, nkeys, sp, key)
	bt.alloc.ReleasePage(pageNum)

	sr, err := bt.insertRecursive(childPage, key, itemPage, itemSlot)
	if err != nil {
		return nil, err
	}
	if sr == nil {
		return nil, nil
	}

	// Child split — insert the median key into this internal node.
	return bt.insertInternal(pageNum, sr.medianKey, sr.newPageNum)
}

func (bt *BTree) insertLeaf(pageNum uint32, buf []byte, page *slottedpage.Page, key int64, itemPage uint32, itemSlot uint16) (*splitResult, error) {
	e := Entry{Key: key, PageNum: itemPage, SlotNum: itemSlot}
	encoded := encodeEntry(&e)

	// Try inserting into the page.
	if page.FreeSpace() >= uint16(len(encoded))+4 {
		// Find insertion position to maintain sorted order.
		sp := page.Special()
		nkeys := getNumKeys(sp)

		// For simplicity, append and then we rely on sorted insertion order.
		// Actually, we need to maintain sorted order. Let's insert at the
		// right position by rebuilding.
		// Since slotted pages don't support insert-at-position, we'll
		// append and the leaf is considered sorted by insertion.
		// For a correct B+Tree we need sorted leaves.

		// Read all entries, insert in sorted order, rewrite.
		entries := readAllEntries(page, nkeys)
		entries = insertSorted(entries, e)

		rewritePage(page, entries)
		sp = page.Special()
		setNumKeys(sp, uint16(len(entries)))

		copy(buf, page.Bytes())
		bt.alloc.MarkDirty(pageNum)
		bt.alloc.ReleasePage(pageNum)
		return nil, nil
	}

	// Page full — split.
	sp := page.Special()
	nkeys := getNumKeys(sp)
	entries := readAllEntries(page, nkeys)
	entries = insertSorted(entries, e)

	mid := len(entries) / 2
	leftEntries := entries[:mid]
	rightEntries := entries[mid:]
	medianKey := rightEntries[0].Key

	// Allocate new right leaf page.
	newPageNum, err := bt.alloc.AllocPage()
	if err != nil {
		bt.alloc.ReleasePage(pageNum)
		return nil, err
	}

	newLeaf := InitLeafPage(newPageNum)

	// Link leaves: old.rightPtr → new, new.rightPtr → old.rightPtr
	oldRightPtr := getRightPtr(sp)
	newSp := newLeaf.Special()
	setRightPtr(newSp, oldRightPtr)

	// Rewrite left (current) page.
	rewritePage(page, leftEntries)
	sp = page.Special()
	setNumKeys(sp, uint16(len(leftEntries)))
	setRightPtr(sp, newPageNum)

	copy(buf, page.Bytes())
	bt.alloc.MarkDirty(pageNum)
	bt.alloc.ReleasePage(pageNum)

	// Write right (new) page.
	rewritePageDirect(newLeaf, rightEntries)
	newSp = newLeaf.Special()
	setNumKeys(newSp, uint16(len(rightEntries)))

	newBuf, err := bt.alloc.FetchPage(newPageNum)
	if err != nil {
		return nil, err
	}
	copy(newBuf, newLeaf.Bytes())
	bt.alloc.MarkDirty(newPageNum)
	bt.alloc.ReleasePage(newPageNum)

	return &splitResult{medianKey: medianKey, newPageNum: newPageNum}, nil
}

func (bt *BTree) insertInternal(pageNum uint32, key int64, newChildPage uint32) (*splitResult, error) {
	buf, err := bt.alloc.FetchPage(pageNum)
	if err != nil {
		return nil, err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		bt.alloc.ReleasePage(pageNum)
		return nil, err
	}

	sp := page.Special()
	nkeys := getNumKeys(sp)
	level := getLevel(sp)

	// In an internal node, entries are: (key, leftChildPage).
	// When we insert a new separator key, the entry's PageNum points to
	// the left child of that key, and the previous rightPtr or next
	// entry's PageNum is the right child.
	//
	// After a child split, we get (medianKey, newRightChild).
	// We need to insert an entry where:
	//   - entry.Key = medianKey
	//   - entry.PageNum = the OLD rightPtr or the previous child
	//   - and update rightPtr to reflect the new layout.
	//
	// The simpler approach: read all entries, find insertion point,
	// insert, fix up rightPtr.

	entries := readAllEntries(page, nkeys)
	oldRightPtr := getRightPtr(sp)

	// Find where to insert the new key.
	pos := len(entries)
	for i, e := range entries {
		if key < e.Key {
			pos = i
			break
		}
	}

	// Build the new entry. The new entry's PageNum is the left child
	// of this separator, which is the child that existed before the split.
	// The new right child (newChildPage) becomes the PageNum of the
	// NEXT entry, or the rightPtr if at the end.
	newEntry := Entry{Key: key}

	if pos < len(entries) {
		// Insert in the middle: the new entry's PageNum is what was
		// the previous entry's right (i.e., entries[pos].PageNum),
		// and entries[pos].PageNum becomes newChildPage.
		newEntry.PageNum = entries[pos].PageNum
		entries[pos].PageNum = newChildPage
	} else {
		// Insert at the end: the new entry's PageNum is the old rightPtr,
		// and the new rightPtr is newChildPage.
		newEntry.PageNum = oldRightPtr
		oldRightPtr = newChildPage
	}

	// Insert the entry.
	newEntries := make([]Entry, 0, len(entries)+1)
	newEntries = append(newEntries, entries[:pos]...)
	newEntries = append(newEntries, newEntry)
	newEntries = append(newEntries, entries[pos:]...)

	// Check if it fits.
	needed := len(newEntries) * (entrySize + 4) // rough estimate
	if needed < int(pageio.PageSize)-24-int(specialSize)-100 {
		// Fits — rewrite in place.
		rewritePage(page, newEntries)
		sp = page.Special()
		binary.LittleEndian.PutUint16(sp[spOffLevel:], level)
		setNumKeys(sp, uint16(len(newEntries)))
		setRightPtr(sp, oldRightPtr)

		copy(buf, page.Bytes())
		bt.alloc.MarkDirty(pageNum)
		bt.alloc.ReleasePage(pageNum)
		return nil, nil
	}

	// Split internal node.
	mid := len(newEntries) / 2
	leftEntries := newEntries[:mid]
	medianEntry := newEntries[mid]
	rightEntries := newEntries[mid+1:]

	// The median key goes up. The median's PageNum is the left-of-median
	// child, which becomes the rightPtr of the left page.
	// The right page's rightPtr is the old rightPtr.

	newRightPageNum, err := bt.alloc.AllocPage()
	if err != nil {
		bt.alloc.ReleasePage(pageNum)
		return nil, err
	}

	// Rewrite left (current) page.
	rewritePage(page, leftEntries)
	sp = page.Special()
	binary.LittleEndian.PutUint16(sp[spOffLevel:], level)
	setNumKeys(sp, uint16(len(leftEntries)))
	setRightPtr(sp, medianEntry.PageNum)

	copy(buf, page.Bytes())
	bt.alloc.MarkDirty(pageNum)
	bt.alloc.ReleasePage(pageNum)

	// Write right (new) page.
	newPage := initInternalPage(newRightPageNum, level)
	rewritePageDirect(newPage, rightEntries)
	newSp := newPage.Special()
	setNumKeys(newSp, uint16(len(rightEntries)))
	setRightPtr(newSp, oldRightPtr)

	newBuf, err := bt.alloc.FetchPage(newRightPageNum)
	if err != nil {
		return nil, err
	}
	copy(newBuf, newPage.Bytes())
	bt.alloc.MarkDirty(newRightPageNum)
	bt.alloc.ReleasePage(newRightPageNum)

	return &splitResult{medianKey: medianEntry.Key, newPageNum: newRightPageNum}, nil
}

// --- Helpers ---

func readAllEntries(page *slottedpage.Page, nkeys uint16) []Entry {
	entries := make([]Entry, 0, nkeys)
	for i := uint16(0); i < nkeys; i++ {
		raw, err := page.GetTuple(i)
		if err != nil {
			break
		}
		entries = append(entries, decodeEntry(raw))
	}
	return entries
}

func insertSorted(entries []Entry, e Entry) []Entry {
	pos := len(entries)
	for i, existing := range entries {
		if e.Key < existing.Key {
			pos = i
			break
		}
	}
	result := make([]Entry, 0, len(entries)+1)
	result = append(result, entries[:pos]...)
	result = append(result, e)
	result = append(result, entries[pos:]...)
	return result
}

// rewritePage clears the page's tuple area and re-inserts entries.
// It preserves the special area.
func rewritePage(page *slottedpage.Page, entries []Entry) {
	sp := page.Special()
	level := getLevel(sp)
	rightPtr := getRightPtr(sp)
	flags := binary.LittleEndian.Uint32(sp[spOffFlags:])
	pageNum := page.PageNum()
	pageType := page.Type()

	var newPage *slottedpage.Page
	if level == 0 {
		newPage = slottedpage.Init(pageType, pageNum, specialSize)
	} else {
		newPage = slottedpage.Init(pageType, pageNum, specialSize)
	}

	for _, e := range entries {
		newPage.InsertTuple(encodeEntry(&e))
	}

	newSp := newPage.Special()
	binary.LittleEndian.PutUint16(newSp[spOffLevel:], level)
	setNumKeys(newSp, uint16(len(entries)))
	setRightPtr(newSp, rightPtr)
	binary.LittleEndian.PutUint32(newSp[spOffFlags:], flags)

	// Copy data from newPage back into page's backing array.
	ref := page.DataRef()
	copy(ref[:], newPage.Bytes())
}

// rewritePageDirect writes entries into a freshly initialized page.
func rewritePageDirect(page *slottedpage.Page, entries []Entry) {
	for _, e := range entries {
		page.InsertTuple(encodeEntry(&e))
	}
}
