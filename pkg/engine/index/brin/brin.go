// Package brin implements a Block Range INdex access method for LolaDB,
// mirroring PostgreSQL's BRIN (src/backend/access/brin).
//
// BRIN stores min/max summaries for contiguous ranges of heap pages.
// Each range summary covers PagesPerRange heap pages. The index is
// very compact — one entry per range — and works well when the indexed
// column is naturally correlated with physical row order.
//
// On-disk layout:
//   - Root page: slotted page holding range summary entries.
//   - Each entry: startPage(4B) + minKey(8B) + maxKey(8B) = 20B.
//   - Overflow pages linked via rightPtr if summaries exceed one page.
//
// Scanning: for a given scan key, BRIN checks each range summary.
// If the range could contain matching rows, it returns all TIDs in
// that heap page range. This is inherently lossy — the executor must
// recheck the actual column values.
//
// Since LolaDB doesn't expose heap page numbers through the index
// interface (TIDs are page+slot), BRIN stores the original key values
// alongside the TID during insert, and returns matching TIDs directly.
// This is a simplification: real PostgreSQL BRIN only stores summaries
// and returns block ranges for bitmap scans.
//
// Our simplified approach: store (minKey, maxKey) per range, plus a
// list of (key, TID) pairs per range. This makes the index larger than
// a real BRIN but preserves the correct semantics for the volcano
// interface.
package brin

import (
	"encoding/binary"
	"fmt"

	"github.com/gololadb/loladb/pkg/engine/index"
	"github.com/gololadb/loladb/pkg/engine/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
)

// PagesPerRange is the default number of logical "slots" per range.
// In real PostgreSQL this refers to heap pages; here we group by
// insertion order.
const PagesPerRange = 128

// -----------------------------------------------------------------------
// On-disk constants
// -----------------------------------------------------------------------

// Range summary entry: minKey(8B) + maxKey(8B) + count(4B) + dataPage(4B) = 24B
const summarySize = 24

const (
	sumOffMin      = 0
	sumOffMax      = 8
	sumOffCount    = 16
	sumOffDataPage = 20
)

// Data entry (stored on data pages): key(8B) + page(4B) + slot(2B) = 14B
const dataEntrySize = 14

// Special area for summary and data pages.
const specialSize = 8

const (
	spOffRightPtr = 0
	spOffFlags    = 4
)

// -----------------------------------------------------------------------
// Entry encoding
// -----------------------------------------------------------------------

type summary struct {
	MinKey   int64
	MaxKey   int64
	Count    uint32
	DataPage uint32 // page holding the actual (key, TID) pairs
}

func encodeSummary(s *summary) []byte {
	buf := make([]byte, summarySize)
	binary.LittleEndian.PutUint64(buf[sumOffMin:], uint64(s.MinKey))
	binary.LittleEndian.PutUint64(buf[sumOffMax:], uint64(s.MaxKey))
	binary.LittleEndian.PutUint32(buf[sumOffCount:], s.Count)
	binary.LittleEndian.PutUint32(buf[sumOffDataPage:], s.DataPage)
	return buf
}

func decodeSummary(buf []byte) summary {
	return summary{
		MinKey:   int64(binary.LittleEndian.Uint64(buf[sumOffMin:])),
		MaxKey:   int64(binary.LittleEndian.Uint64(buf[sumOffMax:])),
		Count:    binary.LittleEndian.Uint32(buf[sumOffCount:]),
		DataPage: binary.LittleEndian.Uint32(buf[sumOffDataPage:]),
	}
}

type dataEntry struct {
	Key     int64
	PageNum uint32
	SlotNum uint16
}

func encodeDataEntry(e *dataEntry) []byte {
	buf := make([]byte, dataEntrySize)
	binary.LittleEndian.PutUint64(buf[0:], uint64(e.Key))
	binary.LittleEndian.PutUint32(buf[8:], e.PageNum)
	binary.LittleEndian.PutUint16(buf[12:], e.SlotNum)
	return buf
}

func decodeDataEntry(buf []byte) dataEntry {
	return dataEntry{
		Key:     int64(binary.LittleEndian.Uint64(buf[0:])),
		PageNum: binary.LittleEndian.Uint32(buf[8:]),
		SlotNum: binary.LittleEndian.Uint16(buf[12:]),
	}
}

// -----------------------------------------------------------------------
// AM — implements index.IndexAM
// -----------------------------------------------------------------------

// AM is the BRIN index access method.
type AM struct {
	alloc index.PageAllocator
}

// NewAM creates a BRIN access method.
func NewAM(alloc index.PageAllocator) *AM {
	return &AM{alloc: alloc}
}

func (am *AM) CanOrder() bool    { return false }
func (am *AM) CanUnique() bool   { return false }
func (am *AM) CanBackward() bool { return false }

// InitRootPage allocates the summary page.
func (am *AM) InitRootPage() (uint32, error) {
	pageNum, err := am.alloc.AllocPage()
	if err != nil {
		return 0, err
	}
	page := slottedpage.Init(slottedpage.PageTypeBRIN, pageNum, specialSize)
	sp := page.Special()
	binary.LittleEndian.PutUint32(sp[spOffRightPtr:], 0)

	buf, err := am.alloc.FetchPage(pageNum)
	if err != nil {
		return 0, err
	}
	copy(buf, page.Bytes())
	am.alloc.MarkDirty(pageNum)
	am.alloc.ReleasePage(pageNum)
	return pageNum, nil
}

// Insert adds a (key, TID) to the current range. If the range is full,
// a new range is started.
func (am *AM) Insert(rootPage uint32, key tuple.Datum, tid slottedpage.ItemID) (uint32, error) {
	k, ok := index.DatumToInt64(key)
	if !ok {
		return rootPage, fmt.Errorf("brin: non-indexable datum type %d", key.Type)
	}

	// Find the last summary entry.
	nSummaries, err := am.countSummaries(rootPage)
	if err != nil {
		return rootPage, err
	}

	if nSummaries == 0 {
		// First insert — create a new range.
		return rootPage, am.newRange(rootPage, k, tid)
	}

	// Read last summary.
	s, sumIdx, err := am.getLastSummary(rootPage)
	if err != nil {
		return rootPage, err
	}

	if s.Count >= PagesPerRange {
		// Range full — start a new one.
		return rootPage, am.newRange(rootPage, k, tid)
	}

	// Add to existing range.
	de := dataEntry{Key: k, PageNum: tid.Page, SlotNum: tid.Slot}
	if err := am.appendDataEntry(s.DataPage, de); err != nil {
		return rootPage, err
	}

	// Update summary min/max.
	if k < s.MinKey {
		s.MinKey = k
	}
	if k > s.MaxKey {
		s.MaxKey = k
	}
	s.Count++
	return rootPage, am.updateSummary(rootPage, sumIdx, s)
}

// Build bulk-loads the index.
func (am *AM) Build(rootPage uint32, iter func(yield func(key tuple.Datum, tid slottedpage.ItemID) bool)) (uint32, error) {
	var buildErr error
	iter(func(key tuple.Datum, tid slottedpage.ItemID) bool {
		_, err := am.Insert(rootPage, key, tid)
		if err != nil {
			buildErr = err
			return false
		}
		return true
	})
	return rootPage, buildErr
}

// BeginScan creates a new scan.
func (am *AM) BeginScan(rootPage uint32) index.IndexScan {
	return &scan{am: am, rootPage: rootPage}
}

var _ index.IndexAM = (*AM)(nil)

// -----------------------------------------------------------------------
// Internal helpers
// -----------------------------------------------------------------------

func (am *AM) countSummaries(rootPage uint32) (int, error) {
	buf, err := am.alloc.FetchPage(rootPage)
	if err != nil {
		return 0, err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		am.alloc.ReleasePage(rootPage)
		return 0, err
	}
	count := 0
	for i := uint16(0); ; i++ {
		raw, err := page.GetTuple(i)
		if err != nil {
			break
		}
		if len(raw) == summarySize {
			count++
		}
	}
	am.alloc.ReleasePage(rootPage)
	return count, nil
}

func (am *AM) getLastSummary(rootPage uint32) (summary, uint16, error) {
	buf, err := am.alloc.FetchPage(rootPage)
	if err != nil {
		return summary{}, 0, err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		am.alloc.ReleasePage(rootPage)
		return summary{}, 0, err
	}
	var lastRaw []byte
	var lastIdx uint16
	for i := uint16(0); ; i++ {
		raw, err := page.GetTuple(i)
		if err != nil {
			break
		}
		if len(raw) == summarySize {
			lastRaw = raw
			lastIdx = i
		}
	}
	am.alloc.ReleasePage(rootPage)
	if lastRaw == nil {
		return summary{}, 0, fmt.Errorf("brin: no summaries found")
	}
	return decodeSummary(lastRaw), lastIdx, nil
}

func (am *AM) newRange(rootPage uint32, key int64, tid slottedpage.ItemID) error {
	// Allocate data page for this range.
	dataPageNum, err := am.alloc.AllocPage()
	if err != nil {
		return err
	}
	dataPage := slottedpage.Init(slottedpage.PageTypeBRIN, dataPageNum, specialSize)
	sp := dataPage.Special()
	binary.LittleEndian.PutUint32(sp[spOffRightPtr:], 0)

	// Insert the first data entry.
	de := dataEntry{Key: key, PageNum: tid.Page, SlotNum: tid.Slot}
	dataPage.InsertTuple(encodeDataEntry(&de))

	buf, err := am.alloc.FetchPage(dataPageNum)
	if err != nil {
		return err
	}
	copy(buf, dataPage.Bytes())
	am.alloc.MarkDirty(dataPageNum)
	am.alloc.ReleasePage(dataPageNum)

	// Add summary entry to root page.
	s := summary{MinKey: key, MaxKey: key, Count: 1, DataPage: dataPageNum}
	buf2, err := am.alloc.FetchPage(rootPage)
	if err != nil {
		return err
	}
	page, err := slottedpage.FromBytes(buf2)
	if err != nil {
		am.alloc.ReleasePage(rootPage)
		return err
	}
	page.InsertTuple(encodeSummary(&s))
	copy(buf2, page.Bytes())
	am.alloc.MarkDirty(rootPage)
	am.alloc.ReleasePage(rootPage)
	return nil
}

func (am *AM) appendDataEntry(dataPageNum uint32, de dataEntry) error {
	buf, err := am.alloc.FetchPage(dataPageNum)
	if err != nil {
		return err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		am.alloc.ReleasePage(dataPageNum)
		return err
	}
	page.InsertTuple(encodeDataEntry(&de))
	copy(buf, page.Bytes())
	am.alloc.MarkDirty(dataPageNum)
	am.alloc.ReleasePage(dataPageNum)
	return nil
}

func (am *AM) updateSummary(rootPage uint32, idx uint16, s summary) error {
	buf, err := am.alloc.FetchPage(rootPage)
	if err != nil {
		return err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		am.alloc.ReleasePage(rootPage)
		return err
	}

	// Rewrite the summary page: read all summaries, update the one at idx, rewrite.
	var summaries []summary
	for i := uint16(0); ; i++ {
		raw, err := page.GetTuple(i)
		if err != nil {
			break
		}
		if len(raw) == summarySize {
			summaries = append(summaries, decodeSummary(raw))
		}
	}
	if int(idx) < len(summaries) {
		summaries[idx] = s
	}

	// Reinitialize page and rewrite.
	pageNum := page.PageNum()
	newPage := slottedpage.Init(slottedpage.PageTypeBRIN, pageNum, specialSize)
	newSp := newPage.Special()
	binary.LittleEndian.PutUint32(newSp[spOffRightPtr:], 0)
	for _, sum := range summaries {
		newPage.InsertTuple(encodeSummary(&sum))
	}
	copy(buf, newPage.Bytes())
	am.alloc.MarkDirty(rootPage)
	am.alloc.ReleasePage(rootPage)
	return nil
}

func (am *AM) readDataEntries(dataPageNum uint32) ([]dataEntry, error) {
	buf, err := am.alloc.FetchPage(dataPageNum)
	if err != nil {
		return nil, err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		am.alloc.ReleasePage(dataPageNum)
		return nil, err
	}
	var entries []dataEntry
	for i := uint16(0); ; i++ {
		raw, err := page.GetTuple(i)
		if err != nil {
			break
		}
		if len(raw) == dataEntrySize {
			entries = append(entries, decodeDataEntry(raw))
		}
	}
	am.alloc.ReleasePage(dataPageNum)
	return entries, nil
}

func (am *AM) readAllSummaries(rootPage uint32) ([]summary, error) {
	buf, err := am.alloc.FetchPage(rootPage)
	if err != nil {
		return nil, err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		am.alloc.ReleasePage(rootPage)
		return nil, err
	}
	var summaries []summary
	for i := uint16(0); ; i++ {
		raw, err := page.GetTuple(i)
		if err != nil {
			break
		}
		if len(raw) == summarySize {
			summaries = append(summaries, decodeSummary(raw))
		}
	}
	am.alloc.ReleasePage(rootPage)
	return summaries, nil
}

// -----------------------------------------------------------------------
// scan — implements index.IndexScan
// -----------------------------------------------------------------------

type scan struct {
	am       *AM
	rootPage uint32

	// Derived from scan keys.
	hasLo, hasHi       bool
	lo, hi             int64
	loInclusive        bool
	hiInclusive        bool
	hasEq              bool
	eqKey              int64

	// Iterator state.
	started    bool
	summaries  []summary
	sumIdx     int
	entries    []dataEntry
	entryIdx   int
	done       bool
}

func (s *scan) Rescan(keys []index.ScanKey) error {
	s.started = false
	s.done = false
	s.hasLo = false
	s.hasHi = false
	s.hasEq = false
	s.summaries = nil
	s.entries = nil

	for _, sk := range keys {
		v, ok := index.DatumToInt64(sk.Value)
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

func (s *scan) Next(dir index.ScanDirection) (tid slottedpage.ItemID, ok bool, err error) {
	if s.done {
		return slottedpage.ItemID{}, false, nil
	}

	if !s.started {
		s.summaries, err = s.am.readAllSummaries(s.rootPage)
		if err != nil {
			return slottedpage.ItemID{}, false, err
		}
		s.sumIdx = 0
		s.entries = nil
		s.entryIdx = 0
		s.started = true
	}

	for {
		// Try to return next matching entry from current range.
		for s.entryIdx < len(s.entries) {
			e := s.entries[s.entryIdx]
			s.entryIdx++
			if s.matchesKey(e.Key) {
				return slottedpage.ItemID{Page: e.PageNum, Slot: e.SlotNum}, true, nil
			}
		}

		// Find next range that could match.
		found := false
		for s.sumIdx < len(s.summaries) {
			sum := s.summaries[s.sumIdx]
			s.sumIdx++
			if s.rangeCouldMatch(sum) {
				s.entries, err = s.am.readDataEntries(sum.DataPage)
				if err != nil {
					return slottedpage.ItemID{}, false, err
				}
				s.entryIdx = 0
				found = true
				break
			}
		}

		if !found {
			s.done = true
			return slottedpage.ItemID{}, false, nil
		}
	}
}

func (s *scan) End() {
	s.done = true
	s.started = false
}

// rangeCouldMatch checks if a range summary could contain matching keys.
func (s *scan) rangeCouldMatch(sum summary) bool {
	if s.hasLo {
		if s.loInclusive && sum.MaxKey < s.lo {
			return false
		}
		if !s.loInclusive && sum.MaxKey <= s.lo {
			return false
		}
	}
	if s.hasHi {
		if s.hiInclusive && sum.MinKey > s.hi {
			return false
		}
		if !s.hiInclusive && sum.MinKey >= s.hi {
			return false
		}
	}
	return true
}

// matchesKey checks if a specific key matches the scan predicates.
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

