// Package gist implements a Generalized Search Tree index access method
// for LolaDB, mirroring PostgreSQL's GiST (src/backend/access/gist).
//
// GiST is a balanced tree where each internal node stores a bounding
// predicate that covers all keys in its subtree. The tree supports
// extensible key types via four operator-class callbacks:
//
//   - Consistent: does a subtree's bounding predicate overlap the query?
//   - Union:      compute the bounding predicate covering a set of keys.
//   - Penalty:    cost of inserting a key into a given subtree.
//   - PickSplit:  divide an overfull node into two groups.
//
// For LolaDB's int64 keys, the bounding predicate is a [min, max] range.
// Consistent checks range overlap, Union merges ranges, Penalty is the
// range enlargement, and PickSplit divides by median.
//
// On-disk layout:
//   - Each page is a slotted page with a 4-byte special area (level).
//   - Internal entries: min(8B) + max(8B) + childPage(4B) = 20B.
//   - Leaf entries: key(8B) + page(4B) + slot(2B) = 14B.
package gist

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/gololadb/loladb/pkg/engine/index"
	"github.com/gololadb/loladb/pkg/engine/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
)

// -----------------------------------------------------------------------
// On-disk constants
// -----------------------------------------------------------------------

const specialSize = 4 // level (2B) + flags (2B)

const (
	spOffLevel = 0
	spOffFlags = 2
)

// Internal entry: min(8B) + max(8B) + childPage(4B) = 20B
const internalEntrySize = 20

// Leaf entry: key(8B) + page(4B) + slot(2B) = 14B
const leafEntrySize = 14

// Maximum entries per node before split.
const maxEntriesPerNode = 200

// -----------------------------------------------------------------------
// Entry types
// -----------------------------------------------------------------------

type internalEntry struct {
	Min       int64
	Max       int64
	ChildPage uint32
}

func encodeInternal(e *internalEntry) []byte {
	buf := make([]byte, internalEntrySize)
	binary.LittleEndian.PutUint64(buf[0:], uint64(e.Min))
	binary.LittleEndian.PutUint64(buf[8:], uint64(e.Max))
	binary.LittleEndian.PutUint32(buf[16:], e.ChildPage)
	return buf
}

func decodeInternal(buf []byte) internalEntry {
	return internalEntry{
		Min:       int64(binary.LittleEndian.Uint64(buf[0:])),
		Max:       int64(binary.LittleEndian.Uint64(buf[8:])),
		ChildPage: binary.LittleEndian.Uint32(buf[16:]),
	}
}

type leafEntry struct {
	Key     int64
	PageNum uint32
	SlotNum uint16
}

func encodeLeaf(e *leafEntry) []byte {
	buf := make([]byte, leafEntrySize)
	binary.LittleEndian.PutUint64(buf[0:], uint64(e.Key))
	binary.LittleEndian.PutUint32(buf[8:], e.PageNum)
	binary.LittleEndian.PutUint16(buf[12:], e.SlotNum)
	return buf
}

func decodeLeaf(buf []byte) leafEntry {
	return leafEntry{
		Key:     int64(binary.LittleEndian.Uint64(buf[0:])),
		PageNum: binary.LittleEndian.Uint32(buf[8:]),
		SlotNum: binary.LittleEndian.Uint16(buf[12:]),
	}
}

// -----------------------------------------------------------------------
// Page helpers
// -----------------------------------------------------------------------

func getLevel(sp []byte) uint16 { return binary.LittleEndian.Uint16(sp[spOffLevel:]) }

func initPage(pageNum uint32, level uint16) *slottedpage.Page {
	p := slottedpage.Init(slottedpage.PageTypeGiST, pageNum, specialSize)
	sp := p.Special()
	binary.LittleEndian.PutUint16(sp[spOffLevel:], level)
	binary.LittleEndian.PutUint16(sp[spOffFlags:], 0)
	return p
}

func writePage(alloc index.PageAllocator, pageNum uint32, page *slottedpage.Page) error {
	buf, err := alloc.FetchPage(pageNum)
	if err != nil {
		return err
	}
	copy(buf, page.Bytes())
	alloc.MarkDirty(pageNum)
	alloc.ReleasePage(pageNum)
	return nil
}

// -----------------------------------------------------------------------
// GiST operator class for int64 ranges
// -----------------------------------------------------------------------

// consistent checks if a bounding range [min, max] is consistent with
// the scan predicates (could contain matching keys).
func consistent(min, max int64, keys []index.ScanKey) bool {
	for _, sk := range keys {
		v, ok := index.DatumToInt64(sk.Value)
		if !ok {
			continue
		}
		switch sk.Strategy {
		case index.StrategyEqual:
			if v < min || v > max {
				return false
			}
		case index.StrategyLess:
			if min >= v {
				return false
			}
		case index.StrategyLessEqual:
			if min > v {
				return false
			}
		case index.StrategyGreater:
			if max <= v {
				return false
			}
		case index.StrategyGreaterEqual:
			if max < v {
				return false
			}
		}
	}
	return true
}

// penalty computes the cost of inserting key into a subtree with
// bounding range [min, max]. Lower is better.
func penalty(min, max, key int64) float64 {
	newMin := min
	newMax := max
	if key < newMin {
		newMin = key
	}
	if key > newMax {
		newMax = key
	}
	oldSize := float64(max - min)
	newSize := float64(newMax - newMin)
	return newSize - oldSize
}

// -----------------------------------------------------------------------
// AM — implements index.IndexAM
// -----------------------------------------------------------------------

// AM is the GiST index access method.
type AM struct {
	alloc index.PageAllocator
}

// NewAM creates a GiST access method.
func NewAM(alloc index.PageAllocator) *AM {
	return &AM{alloc: alloc}
}

func (am *AM) CanOrder() bool    { return false }
func (am *AM) CanUnique() bool   { return false }
func (am *AM) CanBackward() bool { return false }

// InitRootPage allocates an empty leaf page.
func (am *AM) InitRootPage() (uint32, error) {
	pageNum, err := am.alloc.AllocPage()
	if err != nil {
		return 0, err
	}
	page := initPage(pageNum, 0) // level 0 = leaf
	return pageNum, writePage(am.alloc, pageNum, page)
}

// Insert adds a (key, TID) entry, splitting nodes as needed.
func (am *AM) Insert(rootPage uint32, key tuple.Datum, tid slottedpage.ItemID) (uint32, error) {
	k, ok := index.DatumToInt64(key)
	if !ok {
		return rootPage, fmt.Errorf("gist: non-indexable datum type %d", key.Type)
	}

	split, err := am.insertRecursive(rootPage, k, tid)
	if err != nil {
		return rootPage, err
	}

	if split == nil {
		return rootPage, nil
	}

	// Root was split — create a new root.
	newRootNum, err := am.alloc.AllocPage()
	if err != nil {
		return rootPage, err
	}

	// Determine level of old root.
	oldLevel, err := am.pageLevel(rootPage)
	if err != nil {
		return rootPage, err
	}

	newRoot := initPage(newRootNum, oldLevel+1)

	// Compute bounding range for old root.
	oldMin, oldMax, err := am.computeBounds(rootPage)
	if err != nil {
		return rootPage, err
	}
	e1 := internalEntry{Min: oldMin, Max: oldMax, ChildPage: rootPage}
	e2 := internalEntry{Min: split.min, Max: split.max, ChildPage: split.pageNum}
	newRoot.InsertTuple(encodeInternal(&e1))
	newRoot.InsertTuple(encodeInternal(&e2))

	return newRootNum, writePage(am.alloc, newRootNum, newRoot)
}

// Build bulk-loads the index.
func (am *AM) Build(rootPage uint32, iter func(yield func(key tuple.Datum, tid slottedpage.ItemID) bool)) (uint32, error) {
	root := rootPage
	var buildErr error
	iter(func(key tuple.Datum, tid slottedpage.ItemID) bool {
		var err error
		root, err = am.Insert(root, key, tid)
		if err != nil {
			buildErr = err
			return false
		}
		return true
	})
	return root, buildErr
}

// BeginScan creates a new scan.
func (am *AM) BeginScan(rootPage uint32) index.IndexScan {
	return &scan{am: am, rootPage: rootPage}
}

var _ index.IndexAM = (*AM)(nil)

// -----------------------------------------------------------------------
// splitResult
// -----------------------------------------------------------------------

type splitResult struct {
	pageNum uint32
	min     int64
	max     int64
}

// -----------------------------------------------------------------------
// Insert internals
// -----------------------------------------------------------------------

func (am *AM) insertRecursive(pageNum uint32, key int64, tid slottedpage.ItemID) (*splitResult, error) {
	level, err := am.pageLevel(pageNum)
	if err != nil {
		return nil, err
	}

	if level == 0 {
		return am.insertLeaf(pageNum, key, tid)
	}

	return am.insertInternal(pageNum, key, tid)
}

func (am *AM) insertLeaf(pageNum uint32, key int64, tid slottedpage.ItemID) (*splitResult, error) {
	entries, err := am.readLeafEntries(pageNum)
	if err != nil {
		return nil, err
	}

	entries = append(entries, leafEntry{Key: key, PageNum: tid.Page, SlotNum: tid.Slot})

	if len(entries) <= maxEntriesPerNode {
		return nil, am.rewriteLeaf(pageNum, entries)
	}

	// Split: sort by key, divide at median.
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	mid := len(entries) / 2
	leftEntries := entries[:mid]
	rightEntries := entries[mid:]

	// Rewrite current page with left entries.
	if err := am.rewriteLeaf(pageNum, leftEntries); err != nil {
		return nil, err
	}

	// Allocate new page for right entries.
	newPageNum, err := am.alloc.AllocPage()
	if err != nil {
		return nil, err
	}
	newPage := initPage(newPageNum, 0)
	for _, e := range rightEntries {
		newPage.InsertTuple(encodeLeaf(&e))
	}
	if err := writePage(am.alloc, newPageNum, newPage); err != nil {
		return nil, err
	}

	rMin := rightEntries[0].Key
	rMax := rightEntries[len(rightEntries)-1].Key
	return &splitResult{pageNum: newPageNum, min: rMin, max: rMax}, nil
}

func (am *AM) insertInternal(pageNum uint32, key int64, tid slottedpage.ItemID) (*splitResult, error) {
	entries, err := am.readInternalEntries(pageNum)
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		return nil, fmt.Errorf("gist: internal node has no entries")
	}

	// Choose subtree with minimum penalty.
	bestIdx := 0
	bestPenalty := math.MaxFloat64
	for i, e := range entries {
		p := penalty(e.Min, e.Max, key)
		if p < bestPenalty {
			bestPenalty = p
			bestIdx = i
		}
	}

	childSplit, err := am.insertRecursive(entries[bestIdx].ChildPage, key, tid)
	if err != nil {
		return nil, err
	}

	// Update the bounding range of the chosen child.
	childMin, childMax, err := am.computeBounds(entries[bestIdx].ChildPage)
	if err != nil {
		return nil, err
	}
	entries[bestIdx].Min = childMin
	entries[bestIdx].Max = childMax

	if childSplit != nil {
		// Add the new child from the split.
		entries = append(entries, internalEntry{
			Min:       childSplit.min,
			Max:       childSplit.max,
			ChildPage: childSplit.pageNum,
		})
	}

	if len(entries) <= maxEntriesPerNode {
		return nil, am.rewriteInternal(pageNum, entries)
	}

	// Split this internal node.
	sort.Slice(entries, func(i, j int) bool { return entries[i].Min < entries[j].Min })
	mid := len(entries) / 2
	leftEntries := entries[:mid]
	rightEntries := entries[mid:]

	if err := am.rewriteInternal(pageNum, leftEntries); err != nil {
		return nil, err
	}

	newPageNum, err := am.alloc.AllocPage()
	if err != nil {
		return nil, err
	}
	level, _ := am.pageLevel(pageNum)
	newPage := initPage(newPageNum, level)
	for _, e := range rightEntries {
		newPage.InsertTuple(encodeInternal(&e))
	}
	if err := writePage(am.alloc, newPageNum, newPage); err != nil {
		return nil, err
	}

	rMin := rightEntries[0].Min
	rMax := rightEntries[0].Max
	for _, e := range rightEntries[1:] {
		if e.Min < rMin {
			rMin = e.Min
		}
		if e.Max > rMax {
			rMax = e.Max
		}
	}
	return &splitResult{pageNum: newPageNum, min: rMin, max: rMax}, nil
}

// -----------------------------------------------------------------------
// Page I/O helpers
// -----------------------------------------------------------------------

func (am *AM) pageLevel(pageNum uint32) (uint16, error) {
	buf, err := am.alloc.FetchPage(pageNum)
	if err != nil {
		return 0, err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		am.alloc.ReleasePage(pageNum)
		return 0, err
	}
	sp := page.Special()
	level := getLevel(sp)
	am.alloc.ReleasePage(pageNum)
	return level, nil
}

func (am *AM) readLeafEntries(pageNum uint32) ([]leafEntry, error) {
	buf, err := am.alloc.FetchPage(pageNum)
	if err != nil {
		return nil, err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		am.alloc.ReleasePage(pageNum)
		return nil, err
	}
	var entries []leafEntry
	for i := uint16(0); ; i++ {
		raw, err := page.GetTuple(i)
		if err != nil {
			break
		}
		if len(raw) == leafEntrySize {
			entries = append(entries, decodeLeaf(raw))
		}
	}
	am.alloc.ReleasePage(pageNum)
	return entries, nil
}

func (am *AM) readInternalEntries(pageNum uint32) ([]internalEntry, error) {
	buf, err := am.alloc.FetchPage(pageNum)
	if err != nil {
		return nil, err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		am.alloc.ReleasePage(pageNum)
		return nil, err
	}
	var entries []internalEntry
	for i := uint16(0); ; i++ {
		raw, err := page.GetTuple(i)
		if err != nil {
			break
		}
		if len(raw) == internalEntrySize {
			entries = append(entries, decodeInternal(raw))
		}
	}
	am.alloc.ReleasePage(pageNum)
	return entries, nil
}

func (am *AM) rewriteLeaf(pageNum uint32, entries []leafEntry) error {
	page := initPage(pageNum, 0)
	for _, e := range entries {
		page.InsertTuple(encodeLeaf(&e))
	}
	return writePage(am.alloc, pageNum, page)
}

func (am *AM) rewriteInternal(pageNum uint32, entries []internalEntry) error {
	level, err := am.pageLevel(pageNum)
	if err != nil {
		return err
	}
	page := initPage(pageNum, level)
	for _, e := range entries {
		page.InsertTuple(encodeInternal(&e))
	}
	return writePage(am.alloc, pageNum, page)
}

// computeBounds computes the bounding [min, max] range for all keys
// in the subtree rooted at pageNum.
func (am *AM) computeBounds(pageNum uint32) (int64, int64, error) {
	level, err := am.pageLevel(pageNum)
	if err != nil {
		return 0, 0, err
	}

	if level == 0 {
		entries, err := am.readLeafEntries(pageNum)
		if err != nil {
			return 0, 0, err
		}
		if len(entries) == 0 {
			return 0, 0, nil
		}
		min, max := entries[0].Key, entries[0].Key
		for _, e := range entries[1:] {
			if e.Key < min {
				min = e.Key
			}
			if e.Key > max {
				max = e.Key
			}
		}
		return min, max, nil
	}

	entries, err := am.readInternalEntries(pageNum)
	if err != nil {
		return 0, 0, err
	}
	if len(entries) == 0 {
		return 0, 0, nil
	}
	min, max := entries[0].Min, entries[0].Max
	for _, e := range entries[1:] {
		if e.Min < min {
			min = e.Min
		}
		if e.Max > max {
			max = e.Max
		}
	}
	return min, max, nil
}

// -----------------------------------------------------------------------
// scan — implements index.IndexScan (depth-first tree traversal)
// -----------------------------------------------------------------------

type scan struct {
	am       *AM
	rootPage uint32

	keys []index.ScanKey

	// Iterator state: stack of (pageNum, entryIndex) for DFS.
	started bool
	stack   []stackFrame
	results []slottedpage.ItemID
	resIdx  int
	done    bool
}

type stackFrame struct {
	pageNum  uint32
	entryIdx int
}

func (s *scan) Rescan(keys []index.ScanKey) error {
	s.keys = keys
	s.started = false
	s.done = false
	s.stack = nil
	s.results = nil
	s.resIdx = 0
	return nil
}

func (s *scan) Next(dir index.ScanDirection) (tid slottedpage.ItemID, ok bool, err error) {
	if s.done {
		return slottedpage.ItemID{}, false, nil
	}

	if !s.started {
		s.started = true
		// Collect all matching TIDs via DFS.
		if err := s.collectMatches(s.rootPage); err != nil {
			return slottedpage.ItemID{}, false, err
		}
		s.resIdx = 0
	}

	if s.resIdx >= len(s.results) {
		s.done = true
		return slottedpage.ItemID{}, false, nil
	}

	result := s.results[s.resIdx]
	s.resIdx++
	return result, true, nil
}

func (s *scan) End() {
	s.done = true
	s.started = false
	s.results = nil
}

// collectMatches performs a depth-first traversal, pruning subtrees
// whose bounding ranges are inconsistent with the scan keys.
func (s *scan) collectMatches(pageNum uint32) error {
	level, err := s.am.pageLevel(pageNum)
	if err != nil {
		return err
	}

	if level == 0 {
		entries, err := s.am.readLeafEntries(pageNum)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if consistent(e.Key, e.Key, s.keys) {
				s.results = append(s.results, slottedpage.ItemID{Page: e.PageNum, Slot: e.SlotNum})
			}
		}
		return nil
	}

	entries, err := s.am.readInternalEntries(pageNum)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if consistent(e.Min, e.Max, s.keys) {
			if err := s.collectMatches(e.ChildPage); err != nil {
				return err
			}
		}
	}
	return nil
}

var _ index.IndexScan = (*scan)(nil)

// -----------------------------------------------------------------------
// Datum conversion
// -----------------------------------------------------------------------

