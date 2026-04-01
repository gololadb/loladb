// Package spgist implements a Space-Partitioned GiST index access method
// for LolaDB, mirroring PostgreSQL's SP-GiST (src/backend/access/spgist).
//
// SP-GiST is an unbalanced partitioned search tree. Unlike GiST (which
// uses overlapping bounding predicates), SP-GiST partitions the key
// space so that each inner node divides its domain into non-overlapping
// regions. This makes it suitable for radix trees, quad-trees, and
// k-d trees.
//
// For LolaDB's int64 keys, we implement a radix tree that partitions
// by the most-significant differing bit. Each inner node stores a
// prefix value and a split bit position; keys whose bit at that
// position is 0 go left, 1 goes right.
//
// On-disk layout:
//   - Each page is a slotted page with a 4-byte special area.
//   - Inner node entry: prefix(8B) + splitBit(1B) + leftPage(4B) + rightPage(4B) = 17B
//   - Leaf entry: key(8B) + page(4B) + slot(2B) = 14B
//
// Since SP-GiST trees are unbalanced, a single page can hold a mix
// of inner and leaf entries. We simplify by using separate pages for
// inner nodes and leaf buckets.
package spgist

import (
	"encoding/binary"
	"fmt"

	"github.com/jespino/loladb/pkg/index"
	"github.com/jespino/loladb/pkg/slottedpage"
	"github.com/jespino/loladb/pkg/tuple"
)

// -----------------------------------------------------------------------
// On-disk constants
// -----------------------------------------------------------------------

const specialSize = 4

const (
	spOffFlags = 0
)

// Node types stored in flags.
const (
	nodeTypeLeaf  = 0
	nodeTypeInner = 1
)

// Leaf entry: key(8B) + page(4B) + slot(2B) = 14B
const leafEntrySize = 14

// Inner node is stored as a single "tuple" on its page:
// prefix(8B) + splitBit(1B) + leftPage(4B) + rightPage(4B) = 17B
const innerNodeSize = 21

// Maximum leaf entries before splitting into a radix subtree.
const maxLeafEntries = 200

// -----------------------------------------------------------------------
// Entry types
// -----------------------------------------------------------------------

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

type innerNode struct {
	Prefix    int64  // common prefix value (masked to splitBit)
	SplitBit  uint8  // bit position to branch on (63 = MSB, 0 = LSB)
	LeftPage  uint32 // child page for bit=0
	RightPage uint32 // child page for bit=1
}

func encodeInner(n *innerNode) []byte {
	buf := make([]byte, innerNodeSize)
	binary.LittleEndian.PutUint64(buf[0:], uint64(n.Prefix))
	buf[8] = n.SplitBit
	binary.LittleEndian.PutUint32(buf[9:], n.LeftPage)
	binary.LittleEndian.PutUint32(buf[13:], n.RightPage)
	// Pad to 21 bytes for unique size detection.
	binary.LittleEndian.PutUint32(buf[17:], 0)
	return buf
}

func decodeInner(buf []byte) innerNode {
	return innerNode{
		Prefix:    int64(binary.LittleEndian.Uint64(buf[0:])),
		SplitBit:  buf[8],
		LeftPage:  binary.LittleEndian.Uint32(buf[9:]),
		RightPage: binary.LittleEndian.Uint32(buf[13:]),
	}
}

// getBit returns the value of bit at position pos (63=MSB, 0=LSB)
// in the unsigned representation of key.
func getBit(key int64, pos uint8) uint8 {
	// Flip the sign bit so that negative numbers sort correctly
	// in the unsigned bit space.
	u := uint64(key) ^ (1 << 63)
	if u&(1<<pos) != 0 {
		return 1
	}
	return 0
}

// findSplitBit finds the highest bit position where any two keys in
// the set differ. Returns the bit position.
func findSplitBit(keys []int64) uint8 {
	if len(keys) < 2 {
		return 0
	}
	// XOR all pairs to find differing bits, then pick the highest.
	var diff uint64
	u0 := uint64(keys[0]) ^ (1 << 63)
	for _, k := range keys[1:] {
		u := uint64(k) ^ (1 << 63)
		diff |= u0 ^ u
	}
	if diff == 0 {
		return 0
	}
	// Find highest set bit.
	pos := uint8(63)
	for diff&(1<<pos) == 0 {
		pos--
	}
	return pos
}

// -----------------------------------------------------------------------
// Page helpers
// -----------------------------------------------------------------------

func initLeafPage(alloc index.PageAllocator, pageNum uint32) *slottedpage.Page {
	p := slottedpage.Init(slottedpage.PageTypeSPGiST, pageNum, specialSize)
	sp := p.Special()
	binary.LittleEndian.PutUint32(sp[spOffFlags:], nodeTypeLeaf)
	return p
}

func initInnerPage(alloc index.PageAllocator, pageNum uint32) *slottedpage.Page {
	p := slottedpage.Init(slottedpage.PageTypeSPGiST, pageNum, specialSize)
	sp := p.Special()
	binary.LittleEndian.PutUint32(sp[spOffFlags:], nodeTypeInner)
	return p
}

func pageNodeType(alloc index.PageAllocator, pageNum uint32) (uint32, error) {
	buf, err := alloc.FetchPage(pageNum)
	if err != nil {
		return 0, err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		alloc.ReleasePage(pageNum)
		return 0, err
	}
	sp := page.Special()
	nt := binary.LittleEndian.Uint32(sp[spOffFlags:])
	alloc.ReleasePage(pageNum)
	return nt, nil
}

func writePageBuf(alloc index.PageAllocator, pageNum uint32, page *slottedpage.Page) error {
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
// AM — implements index.IndexAM
// -----------------------------------------------------------------------

// AM is the SP-GiST index access method.
type AM struct {
	alloc index.PageAllocator
}

// NewAM creates an SP-GiST access method.
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
	page := initLeafPage(am.alloc, pageNum)
	return pageNum, writePageBuf(am.alloc, pageNum, page)
}

// Insert adds a (key, TID) entry.
func (am *AM) Insert(rootPage uint32, key tuple.Datum, tid slottedpage.ItemID) (uint32, error) {
	k, ok := datumToInt64(key)
	if !ok {
		return rootPage, fmt.Errorf("spgist: non-indexable datum type %d", key.Type)
	}

	newRoot, err := am.insertRecursive(rootPage, k, tid)
	if err != nil {
		return rootPage, err
	}
	return newRoot, nil
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
// Insert internals
// -----------------------------------------------------------------------

func (am *AM) insertRecursive(pageNum uint32, key int64, tid slottedpage.ItemID) (uint32, error) {
	nt, err := pageNodeType(am.alloc, pageNum)
	if err != nil {
		return pageNum, err
	}

	if nt == nodeTypeLeaf {
		return am.insertLeaf(pageNum, key, tid)
	}

	return am.insertInner(pageNum, key, tid)
}

func (am *AM) insertLeaf(pageNum uint32, key int64, tid slottedpage.ItemID) (uint32, error) {
	entries, err := am.readLeafEntries(pageNum)
	if err != nil {
		return pageNum, err
	}

	entries = append(entries, leafEntry{Key: key, PageNum: tid.Page, SlotNum: tid.Slot})

	if len(entries) <= maxLeafEntries {
		// Rewrite leaf page.
		page := initLeafPage(am.alloc, pageNum)
		for _, e := range entries {
			page.InsertTuple(encodeLeaf(&e))
		}
		return pageNum, writePageBuf(am.alloc, pageNum, page)
	}

	// Split: partition by the highest differing bit.
	keys := make([]int64, len(entries))
	for i, e := range entries {
		keys[i] = e.Key
	}
	splitBit := findSplitBit(keys)

	var leftEntries, rightEntries []leafEntry
	for _, e := range entries {
		if getBit(e.Key, splitBit) == 0 {
			leftEntries = append(leftEntries, e)
		} else {
			rightEntries = append(rightEntries, e)
		}
	}

	// Handle degenerate case: all keys have the same bit value.
	if len(leftEntries) == 0 {
		leftEntries = rightEntries[:len(rightEntries)/2]
		rightEntries = rightEntries[len(rightEntries)/2:]
	} else if len(rightEntries) == 0 {
		rightEntries = leftEntries[len(leftEntries)/2:]
		leftEntries = leftEntries[:len(leftEntries)/2]
	}

	// Allocate left and right leaf pages.
	leftPageNum, err := am.alloc.AllocPage()
	if err != nil {
		return pageNum, err
	}
	leftPage := initLeafPage(am.alloc, leftPageNum)
	for _, e := range leftEntries {
		leftPage.InsertTuple(encodeLeaf(&e))
	}
	if err := writePageBuf(am.alloc, leftPageNum, leftPage); err != nil {
		return pageNum, err
	}

	rightPageNum, err := am.alloc.AllocPage()
	if err != nil {
		return pageNum, err
	}
	rightPage := initLeafPage(am.alloc, rightPageNum)
	for _, e := range rightEntries {
		rightPage.InsertTuple(encodeLeaf(&e))
	}
	if err := writePageBuf(am.alloc, rightPageNum, rightPage); err != nil {
		return pageNum, err
	}

	// Convert current page to inner node.
	inner := innerNode{
		Prefix:    0,
		SplitBit:  splitBit,
		LeftPage:  leftPageNum,
		RightPage: rightPageNum,
	}
	innerPage := initInnerPage(am.alloc, pageNum)
	innerPage.InsertTuple(encodeInner(&inner))
	return pageNum, writePageBuf(am.alloc, pageNum, innerPage)
}

func (am *AM) insertInner(pageNum uint32, key int64, tid slottedpage.ItemID) (uint32, error) {
	node, err := am.readInnerNode(pageNum)
	if err != nil {
		return pageNum, err
	}

	var childPage uint32
	if getBit(key, node.SplitBit) == 0 {
		childPage = node.LeftPage
	} else {
		childPage = node.RightPage
	}

	newChild, err := am.insertRecursive(childPage, key, tid)
	if err != nil {
		return pageNum, err
	}

	// Update child pointer if it changed.
	if newChild != childPage {
		if getBit(key, node.SplitBit) == 0 {
			node.LeftPage = newChild
		} else {
			node.RightPage = newChild
		}
		innerPage := initInnerPage(am.alloc, pageNum)
		innerPage.InsertTuple(encodeInner(&node))
		if err := writePageBuf(am.alloc, pageNum, innerPage); err != nil {
			return pageNum, err
		}
	}

	return pageNum, nil
}

// -----------------------------------------------------------------------
// Page I/O helpers
// -----------------------------------------------------------------------

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

func (am *AM) readInnerNode(pageNum uint32) (innerNode, error) {
	buf, err := am.alloc.FetchPage(pageNum)
	if err != nil {
		return innerNode{}, err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		am.alloc.ReleasePage(pageNum)
		return innerNode{}, err
	}
	raw, err := page.GetTuple(0)
	am.alloc.ReleasePage(pageNum)
	if err != nil {
		return innerNode{}, fmt.Errorf("spgist: inner node has no entry")
	}
	if len(raw) != innerNodeSize {
		return innerNode{}, fmt.Errorf("spgist: unexpected inner entry size %d", len(raw))
	}
	return decodeInner(raw), nil
}

// -----------------------------------------------------------------------
// scan — implements index.IndexScan
// -----------------------------------------------------------------------

type scan struct {
	am       *AM
	rootPage uint32

	keys []index.ScanKey

	// Derived bounds.
	hasLo, hasHi       bool
	lo, hi             int64
	loInclusive        bool
	hiInclusive        bool

	// Iterator state.
	started bool
	results []slottedpage.ItemID
	resIdx  int
	done    bool
}

func (s *scan) Rescan(keys []index.ScanKey) error {
	s.keys = keys
	s.started = false
	s.done = false
	s.results = nil
	s.resIdx = 0
	s.hasLo = false
	s.hasHi = false

	for _, sk := range keys {
		v, ok := datumToInt64(sk.Value)
		if !ok {
			continue
		}
		switch sk.Strategy {
		case index.StrategyEqual:
			s.hasLo = true
			s.lo = v
			s.loInclusive = true
			s.hasHi = true
			s.hi = v
			s.hiInclusive = true
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
	return nil
}

func (s *scan) Next(dir index.ScanDirection) (tid slottedpage.ItemID, ok bool, err error) {
	if s.done {
		return slottedpage.ItemID{}, false, nil
	}

	if !s.started {
		s.started = true
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

func (s *scan) collectMatches(pageNum uint32) error {
	nt, err := pageNodeType(s.am.alloc, pageNum)
	if err != nil {
		return err
	}

	if nt == nodeTypeLeaf {
		entries, err := s.am.readLeafEntries(pageNum)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if s.matchesKey(e.Key) {
				s.results = append(s.results, slottedpage.ItemID{Page: e.PageNum, Slot: e.SlotNum})
			}
		}
		return nil
	}

	// Inner node — descend into children that could contain matches.
	node, err := s.am.readInnerNode(pageNum)
	if err != nil {
		return err
	}

	// Both children could contain matches unless we can prune.
	// For simplicity, always visit both (SP-GiST pruning with bit
	// prefixes is complex; the leaf-level filter is correct).
	if err := s.collectMatches(node.LeftPage); err != nil {
		return err
	}
	return s.collectMatches(node.RightPage)
}

func (s *scan) matchesKey(key int64) bool {
	if s.hasLo {
		if s.loInclusive && key < s.lo {
			return false
		}
		if !s.loInclusive && key <= s.lo {
			return false
		}
	}
	if s.hasHi {
		if s.hiInclusive && key > s.hi {
			return false
		}
		if !s.hiInclusive && key >= s.hi {
			return false
		}
	}
	return true
}

var _ index.IndexScan = (*scan)(nil)

// -----------------------------------------------------------------------
// Datum conversion
// -----------------------------------------------------------------------

func datumToInt64(d tuple.Datum) (int64, bool) {
	switch d.Type {
	case tuple.TypeInt64:
		return d.I64, true
	case tuple.TypeInt32:
		return int64(d.I32), true
	default:
		return 0, false
	}
}
