// Package btree implements a B+Tree index access method for LolaDB,
// mirroring PostgreSQL's nbtree (src/backend/access/nbtree).
//
// On-disk format: each B+Tree page is a slotted page with a 12-byte
// special area (level, numKeys, rightPtr). Entries are 14-byte
// (key int64, pageNum uint32, slotNum uint16). Internal nodes use
// pageNum as the child pointer; leaves use (pageNum, slotNum) as the
// heap TID (ItemID).
//
// The AM implements index.IndexAM and provides volcano-style scanning
// via index.IndexScan.
package btree

import (
	"encoding/binary"
	"fmt"

	"github.com/jespino/loladb/pkg/index"
	"github.com/jespino/loladb/pkg/pageio"
	"github.com/jespino/loladb/pkg/slottedpage"
	"github.com/jespino/loladb/pkg/tuple"
)

// -----------------------------------------------------------------------
// On-disk constants
// -----------------------------------------------------------------------

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
	Key     int64
	PageNum uint32 // internal: child page; leaf: ItemID.Page
	SlotNum uint16 // leaf only: ItemID.Slot
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

// -----------------------------------------------------------------------
// Node accessors (work on special area)
// -----------------------------------------------------------------------

func getLevel(sp []byte) uint16    { return binary.LittleEndian.Uint16(sp[spOffLevel:]) }
func getNumKeys(sp []byte) uint16  { return binary.LittleEndian.Uint16(sp[spOffNumKeys:]) }
func getRightPtr(sp []byte) uint32 { return binary.LittleEndian.Uint32(sp[spOffRightPtr:]) }

func setNumKeys(sp []byte, n uint16)  { binary.LittleEndian.PutUint16(sp[spOffNumKeys:], n) }
func setRightPtr(sp []byte, p uint32) { binary.LittleEndian.PutUint32(sp[spOffRightPtr:], p) }

// -----------------------------------------------------------------------
// Page initialization
// -----------------------------------------------------------------------

// InitLeafPage initializes a page as an empty B+Tree leaf node.
func InitLeafPage(pageNum uint32) *slottedpage.Page {
	p := slottedpage.Init(slottedpage.PageTypeBTreeLeaf, pageNum, specialSize)
	sp := p.Special()
	binary.LittleEndian.PutUint16(sp[spOffLevel:], 0)
	binary.LittleEndian.PutUint16(sp[spOffNumKeys:], 0)
	binary.LittleEndian.PutUint32(sp[spOffRightPtr:], 0)
	return p
}

func initInternalPage(pageNum uint32, level uint16) *slottedpage.Page {
	p := slottedpage.Init(slottedpage.PageTypeBTreeInt, pageNum, specialSize)
	sp := p.Special()
	binary.LittleEndian.PutUint16(sp[spOffLevel:], level)
	binary.LittleEndian.PutUint16(sp[spOffNumKeys:], 0)
	binary.LittleEndian.PutUint32(sp[spOffRightPtr:], 0)
	return p
}

// -----------------------------------------------------------------------
// AM — implements index.IndexAM
// -----------------------------------------------------------------------

// AM is the B+Tree index access method. Create one per page allocator
// (i.e. per database engine instance).
type AM struct {
	alloc index.PageAllocator
}

// NewAM creates a B+Tree access method backed by the given allocator.
func NewAM(alloc index.PageAllocator) *AM {
	return &AM{alloc: alloc}
}

// Capability flags.
func (am *AM) CanOrder() bool    { return true }
func (am *AM) CanUnique() bool   { return true }
func (am *AM) CanBackward() bool { return false }

// InitRootPage allocates and initializes a fresh leaf page.
func (am *AM) InitRootPage() (uint32, error) {
	pageNum, err := am.alloc.AllocPage()
	if err != nil {
		return 0, err
	}
	leaf := InitLeafPage(pageNum)
	buf, err := am.alloc.FetchPage(pageNum)
	if err != nil {
		return 0, err
	}
	copy(buf, leaf.Bytes())
	am.alloc.MarkDirty(pageNum)
	am.alloc.ReleasePage(pageNum)
	return pageNum, nil
}

// Insert adds a single (key, TID) entry. Returns the (possibly new)
// root page number.
func (am *AM) Insert(rootPage uint32, key tuple.Datum, tid slottedpage.ItemID) (uint32, error) {
	k, ok := index.DatumToInt64Sortable(key)
	if !ok {
		return rootPage, fmt.Errorf("btree: non-indexable datum type %d", key.Type)
	}
	bt := &tree{rootPage: rootPage, alloc: am.alloc}
	if err := bt.insert(k, tid.Page, tid.Slot); err != nil {
		return rootPage, err
	}
	return bt.rootPage, nil
}

// Build bulk-loads the index from an iterator.
func (am *AM) Build(rootPage uint32, iter func(yield func(key tuple.Datum, tid slottedpage.ItemID) bool)) (uint32, error) {
	bt := &tree{rootPage: rootPage, alloc: am.alloc}
	var buildErr error
	iter(func(key tuple.Datum, tid slottedpage.ItemID) bool {
		k, ok := index.DatumToInt64Sortable(key)
		if !ok {
			return true // skip non-indexable
		}
		if err := bt.insert(k, tid.Page, tid.Slot); err != nil {
			buildErr = err
			return false
		}
		return true
	})
	if buildErr != nil {
		return rootPage, buildErr
	}
	return bt.rootPage, nil
}

// BeginScan creates a new volcano-style scan on the index.
func (am *AM) BeginScan(rootPage uint32) index.IndexScan {
	return &scan{
		alloc:    am.alloc,
		rootPage: rootPage,
	}
}

// Compile-time interface check.
var _ index.IndexAM = (*AM)(nil)

// -----------------------------------------------------------------------
// Datum → int64 conversion
// -----------------------------------------------------------------------


// -----------------------------------------------------------------------
// scan — implements index.IndexScan (volcano-style iterator)
// -----------------------------------------------------------------------

// scan holds the state for an in-progress B+Tree index scan.
// It mirrors PostgreSQL's BTScanOpaqueData.
type scan struct {
	alloc    index.PageAllocator
	rootPage uint32

	// Scan keys set by Rescan.
	keys []index.ScanKey

	// Derived bounds from scan keys.
	hasLo, hasHi       bool
	lo, hi             int64
	loInclusive        bool
	hiInclusive        bool
	hasEq              bool
	eqKey              int64

	// Iterator state.
	started    bool
	curPage    uint32
	curSlot    uint16
	curNKeys   uint16
	curPageBuf []byte // nil when no page is pinned
}

// Rescan (re)starts the scan with the given keys.
func (s *scan) Rescan(keys []index.ScanKey) error {
	s.keys = keys
	s.started = false
	s.curPageBuf = nil
	s.hasLo = false
	s.hasHi = false
	s.hasEq = false

	// Derive lo/hi bounds from scan keys.
	for _, sk := range keys {
		v, ok := index.DatumToInt64Sortable(sk.Value)
		if !ok {
			continue
		}
		switch sk.Strategy {
		case index.StrategyEqual:
			s.hasEq = true
			s.eqKey = v
		case index.StrategyLess:
			s.hasHi = true
			s.hi = v
			s.hiInclusive = false
		case index.StrategyLessEqual:
			s.hasHi = true
			s.hi = v
			s.hiInclusive = true
		case index.StrategyGreater:
			s.hasLo = true
			s.lo = v
			s.loInclusive = false
		case index.StrategyGreaterEqual:
			s.hasLo = true
			s.lo = v
			s.loInclusive = true
		}
	}

	// Equality overrides range bounds.
	if s.hasEq {
		s.hasLo = true
		s.lo = s.eqKey
		s.loInclusive = true
		s.hasHi = true
		s.hi = s.eqKey
		s.hiInclusive = true
	}

	return nil
}

// Next returns the next matching heap TID. Mirrors btgettuple.
func (s *scan) Next(dir index.ScanDirection) (tid slottedpage.ItemID, ok bool, err error) {
	if dir != index.ForwardScan {
		return slottedpage.ItemID{}, false, fmt.Errorf("btree: only forward scan supported")
	}

	if !s.started {
		if err := s.positionFirst(); err != nil {
			return slottedpage.ItemID{}, false, err
		}
		s.started = true
	} else {
		s.curSlot++
	}

	// Walk through leaf pages.
	for {
		if s.curPage == 0 {
			return slottedpage.ItemID{}, false, nil
		}

		if s.curPageBuf == nil {
			buf, err := s.alloc.FetchPage(s.curPage)
			if err != nil {
				return slottedpage.ItemID{}, false, err
			}
			s.curPageBuf = buf
			page, err := slottedpage.FromBytes(buf)
			if err != nil {
				s.alloc.ReleasePage(s.curPage)
				s.curPageBuf = nil
				return slottedpage.ItemID{}, false, err
			}
			sp := page.Special()
			s.curNKeys = getNumKeys(sp)
		}

		// Scan entries on current page.
		page, err := slottedpage.FromBytes(s.curPageBuf)
		if err != nil {
			s.releaseCurPage()
			return slottedpage.ItemID{}, false, err
		}

		for s.curSlot < s.curNKeys {
			raw, err := page.GetTuple(s.curSlot)
			if err != nil {
				s.curSlot++
				continue
			}
			e := decodeEntry(raw)

			// Check upper bound — if exceeded, scan is done.
			if s.hasHi {
				if s.hiInclusive && e.Key > s.hi {
					s.releaseCurPage()
					s.curPage = 0
					return slottedpage.ItemID{}, false, nil
				}
				if !s.hiInclusive && e.Key >= s.hi {
					s.releaseCurPage()
					s.curPage = 0
					return slottedpage.ItemID{}, false, nil
				}
			}

			// Check lower bound — skip entries below it.
			if s.hasLo {
				if s.loInclusive && e.Key < s.lo {
					s.curSlot++
					continue
				}
				if !s.loInclusive && e.Key <= s.lo {
					s.curSlot++
					continue
				}
			}

			// Match — return this TID.
			s.curSlot++ // advance past this entry for next call
			// But we incremented at the top of Next, so undo the double-advance:
			s.curSlot--
			return slottedpage.ItemID{Page: e.PageNum, Slot: e.SlotNum}, true, nil
		}

		// Move to next leaf page.
		sp := page.Special()
		nextPage := getRightPtr(sp)
		s.releaseCurPage()
		s.curPage = nextPage
		s.curPageBuf = nil
		s.curSlot = 0
	}
}

// End releases all resources held by the scan.
func (s *scan) End() {
	s.releaseCurPage()
	s.curPage = 0
	s.started = false
}

// positionFirst descends from the root to the leftmost leaf that could
// contain a matching entry. Mirrors _bt_first.
func (s *scan) positionFirst() error {
	// Determine the descent key. If we have a lower bound, descend
	// to that key; otherwise start at the leftmost leaf.
	var descendKey int64
	hasDescendKey := false
	if s.hasLo {
		descendKey = s.lo
		hasDescendKey = true
	}

	pageNum := s.rootPage
	for {
		buf, err := s.alloc.FetchPage(pageNum)
		if err != nil {
			return err
		}
		page, err := slottedpage.FromBytes(buf)
		if err != nil {
			s.alloc.ReleasePage(pageNum)
			return err
		}
		sp := page.Special()
		if len(sp) < specialSize {
			s.alloc.ReleasePage(pageNum)
			return fmt.Errorf("btree: page %d has invalid special area", pageNum)
		}
		level := getLevel(sp)
		if level == 0 {
			// Reached a leaf — this is our starting page.
			s.alloc.ReleasePage(pageNum)
			s.curPage = pageNum
			s.curSlot = 0
			s.curPageBuf = nil
			return nil
		}

		// Internal node — descend.
		nkeys := getNumKeys(sp)
		var childPage uint32
		if hasDescendKey {
			childPage = findChild(page, nkeys, sp, descendKey)
		} else {
			// No lower bound — go to leftmost child.
			if nkeys > 0 {
				raw, err := page.GetTuple(0)
				if err != nil {
					s.alloc.ReleasePage(pageNum)
					return err
				}
				e := decodeEntry(raw)
				childPage = e.PageNum
			} else {
				childPage = getRightPtr(sp)
			}
		}
		s.alloc.ReleasePage(pageNum)
		pageNum = childPage
	}
}

func (s *scan) releaseCurPage() {
	if s.curPageBuf != nil {
		s.alloc.ReleasePage(s.curPage)
		s.curPageBuf = nil
	}
}

// Compile-time interface check.
var _ index.IndexScan = (*scan)(nil)

// -----------------------------------------------------------------------
// findChild — internal node descent
// -----------------------------------------------------------------------

func findChild(page *slottedpage.Page, nkeys uint16, sp []byte, key int64) uint32 {
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

// -----------------------------------------------------------------------
// tree — internal B+Tree operations (insert, split)
// -----------------------------------------------------------------------

// tree holds the mutable root page and allocator for insert operations.
type tree struct {
	rootPage uint32
	alloc    index.PageAllocator
}

// splitResult is returned when a node split occurs during insertion.
type splitResult struct {
	medianKey  int64
	newPageNum uint32
}

func (bt *tree) insert(key int64, itemPage uint32, itemSlot uint16) error {
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

		oldBuf, err := bt.alloc.FetchPage(bt.rootPage)
		if err != nil {
			return err
		}
		oldPage, _ := slottedpage.FromBytes(oldBuf)
		oldLevel := getLevel(oldPage.Special())
		bt.alloc.ReleasePage(bt.rootPage)

		newRoot := initInternalPage(newRootNum, oldLevel+1)
		e := Entry{Key: sr.medianKey, PageNum: bt.rootPage}
		newRoot.InsertTuple(encodeEntry(&e))
		sp := newRoot.Special()
		setNumKeys(sp, 1)
		setRightPtr(sp, sr.newPageNum)

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

func (bt *tree) insertRecursive(pageNum uint32, key int64, itemPage uint32, itemSlot uint16) (*splitResult, error) {
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
	if len(sp) < specialSize {
		bt.alloc.ReleasePage(pageNum)
		return nil, fmt.Errorf("btree: page %d has invalid special area (len=%d, need=%d)", pageNum, len(sp), specialSize)
	}
	level := getLevel(sp)
	nkeys := getNumKeys(sp)

	if level == 0 {
		return bt.insertLeaf(pageNum, buf, page, key, itemPage, itemSlot)
	}

	childPage := findChild(page, nkeys, sp, key)
	bt.alloc.ReleasePage(pageNum)

	sr, err := bt.insertRecursive(childPage, key, itemPage, itemSlot)
	if err != nil {
		return nil, err
	}
	if sr == nil {
		return nil, nil
	}

	return bt.insertInternal(pageNum, sr.medianKey, sr.newPageNum)
}

func (bt *tree) insertLeaf(pageNum uint32, buf []byte, page *slottedpage.Page, key int64, itemPage uint32, itemSlot uint16) (*splitResult, error) {
	e := Entry{Key: key, PageNum: itemPage, SlotNum: itemSlot}
	encoded := encodeEntry(&e)

	if page.FreeSpace() >= uint16(len(encoded))+4 {
		sp := page.Special()
		nkeys := getNumKeys(sp)

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

	newPageNum, err := bt.alloc.AllocPage()
	if err != nil {
		bt.alloc.ReleasePage(pageNum)
		return nil, err
	}

	newLeaf := InitLeafPage(newPageNum)

	oldRightPtr := getRightPtr(sp)
	newSp := newLeaf.Special()
	setRightPtr(newSp, oldRightPtr)

	rewritePage(page, leftEntries)
	sp = page.Special()
	setNumKeys(sp, uint16(len(leftEntries)))
	setRightPtr(sp, newPageNum)

	copy(buf, page.Bytes())
	bt.alloc.MarkDirty(pageNum)
	bt.alloc.ReleasePage(pageNum)

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

func (bt *tree) insertInternal(pageNum uint32, key int64, newChildPage uint32) (*splitResult, error) {
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

	entries := readAllEntries(page, nkeys)
	oldRightPtr := getRightPtr(sp)

	pos := len(entries)
	for i, e := range entries {
		if key < e.Key {
			pos = i
			break
		}
	}

	newEntry := Entry{Key: key}

	if pos < len(entries) {
		newEntry.PageNum = entries[pos].PageNum
		entries[pos].PageNum = newChildPage
	} else {
		newEntry.PageNum = oldRightPtr
		oldRightPtr = newChildPage
	}

	newEntries := make([]Entry, 0, len(entries)+1)
	newEntries = append(newEntries, entries[:pos]...)
	newEntries = append(newEntries, newEntry)
	newEntries = append(newEntries, entries[pos:]...)

	needed := len(newEntries) * (entrySize + 4)
	if needed < int(pageio.PageSize)-24-int(specialSize)-100 {
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

	newRightPageNum, err := bt.alloc.AllocPage()
	if err != nil {
		bt.alloc.ReleasePage(pageNum)
		return nil, err
	}

	rewritePage(page, leftEntries)
	sp = page.Special()
	binary.LittleEndian.PutUint16(sp[spOffLevel:], level)
	setNumKeys(sp, uint16(len(leftEntries)))
	setRightPtr(sp, medianEntry.PageNum)

	copy(buf, page.Bytes())
	bt.alloc.MarkDirty(pageNum)
	bt.alloc.ReleasePage(pageNum)

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

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

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

func rewritePage(page *slottedpage.Page, entries []Entry) {
	sp := page.Special()
	level := getLevel(sp)
	rightPtr := getRightPtr(sp)
	flags := binary.LittleEndian.Uint32(sp[spOffFlags:])
	pageNum := page.PageNum()
	pageType := page.Type()

	newPage := slottedpage.Init(pageType, pageNum, specialSize)

	for _, e := range entries {
		newPage.InsertTuple(encodeEntry(&e))
	}

	newSp := newPage.Special()
	binary.LittleEndian.PutUint16(newSp[spOffLevel:], level)
	setNumKeys(newSp, uint16(len(entries)))
	setRightPtr(newSp, rightPtr)
	binary.LittleEndian.PutUint32(newSp[spOffFlags:], flags)

	ref := page.DataRef()
	copy(ref[:], newPage.Bytes())
}

func rewritePageDirect(page *slottedpage.Page, entries []Entry) {
	for _, e := range entries {
		page.InsertTuple(encodeEntry(&e))
	}
}
