// Package gin implements a Generalized Inverted Index access method for
// LolaDB, mirroring PostgreSQL's GIN (src/backend/access/gin).
//
// GIN is an inverted index: each distinct key maps to a posting list of
// heap TIDs that contain that key. In PostgreSQL, GIN is used for
// full-text search, array containment (@>), and JSONB operators.
//
// On-disk layout (simplified):
//   - Root page: a sorted entry tree stored as a slotted page.
//     Each entry: key(8B) + postingPage(4B) = 12B.
//   - Posting pages: each holds a list of TIDs for one key.
//     Each TID: page(4B) + slot(2B) = 6B.
//   - Overflow via rightPtr in special area.
//
// The AM supports StrategyEqual scans (exact key match) and also
// exposes a ContainsKey helper for @> style queries.
package gin

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/gololadb/loladb/pkg/storage/index"
	"github.com/gololadb/loladb/pkg/storage/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
)

// -----------------------------------------------------------------------
// On-disk constants
// -----------------------------------------------------------------------

const specialSize = 8

const (
	spOffRightPtr = 0
	spOffFlags    = 4
)

// Entry tree entry: key(8B) + postingPage(4B) = 12B
const entrySize = 12

// Posting list TID: page(4B) + slot(2B) = 6B
const tidSize = 6

// -----------------------------------------------------------------------
// Entry encoding
// -----------------------------------------------------------------------

type entryItem struct {
	Key         int64
	PostingPage uint32
}

func encodeEntry(e *entryItem) []byte {
	buf := make([]byte, entrySize)
	binary.LittleEndian.PutUint64(buf[0:], uint64(e.Key))
	binary.LittleEndian.PutUint32(buf[8:], e.PostingPage)
	return buf
}

func decodeEntry(buf []byte) entryItem {
	return entryItem{
		Key:         int64(binary.LittleEndian.Uint64(buf[0:])),
		PostingPage: binary.LittleEndian.Uint32(buf[8:]),
	}
}

func encodeTID(tid slottedpage.ItemID) []byte {
	buf := make([]byte, tidSize)
	binary.LittleEndian.PutUint32(buf[0:], tid.Page)
	binary.LittleEndian.PutUint16(buf[4:], tid.Slot)
	return buf
}

func decodeTID(buf []byte) slottedpage.ItemID {
	return slottedpage.ItemID{
		Page: binary.LittleEndian.Uint32(buf[0:]),
		Slot: binary.LittleEndian.Uint16(buf[4:]),
	}
}

// -----------------------------------------------------------------------
// AM — implements index.IndexAM
// -----------------------------------------------------------------------

// AM is the GIN index access method.
type AM struct {
	alloc index.PageAllocator
}

// NewAM creates a GIN access method.
func NewAM(alloc index.PageAllocator) *AM {
	return &AM{alloc: alloc}
}

func (am *AM) CanOrder() bool    { return false }
func (am *AM) CanUnique() bool   { return false }
func (am *AM) CanBackward() bool { return false }

// InitRootPage allocates the entry tree root page.
func (am *AM) InitRootPage() (uint32, error) {
	pageNum, err := am.alloc.AllocPage()
	if err != nil {
		return 0, err
	}
	page := slottedpage.Init(slottedpage.PageTypeGIN, pageNum, specialSize)
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

// Insert adds a (key, TID) pair. If the key already exists in the entry
// tree, the TID is appended to its posting list. Otherwise a new entry
// and posting page are created.
func (am *AM) Insert(rootPage uint32, key tuple.Datum, tid slottedpage.ItemID) (uint32, error) {
	k, ok := index.DatumToInt64(key)
	if !ok {
		return rootPage, fmt.Errorf("gin: non-indexable datum type %d", key.Type)
	}

	// Read all entries from the root page.
	entries, err := am.readEntries(rootPage)
	if err != nil {
		return rootPage, err
	}

	// Binary search for the key.
	idx := sort.Search(len(entries), func(i int) bool { return entries[i].Key >= k })

	if idx < len(entries) && entries[idx].Key == k {
		// Key exists — append TID to its posting list.
		return rootPage, am.appendToPosting(entries[idx].PostingPage, tid)
	}

	// New key — allocate a posting page and insert the entry.
	postingPage, err := am.newPostingPage(tid)
	if err != nil {
		return rootPage, err
	}

	newEntry := entryItem{Key: k, PostingPage: postingPage}
	// Insert in sorted order.
	entries = append(entries, entryItem{})
	copy(entries[idx+1:], entries[idx:])
	entries[idx] = newEntry

	// Rewrite the entry tree page.
	return rootPage, am.rewriteEntries(rootPage, entries)
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

func (am *AM) readEntries(rootPage uint32) ([]entryItem, error) {
	buf, err := am.alloc.FetchPage(rootPage)
	if err != nil {
		return nil, err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		am.alloc.ReleasePage(rootPage)
		return nil, err
	}
	var entries []entryItem
	for i := uint16(0); ; i++ {
		raw, err := page.GetTuple(i)
		if err != nil {
			break
		}
		if len(raw) == entrySize {
			entries = append(entries, decodeEntry(raw))
		}
	}
	am.alloc.ReleasePage(rootPage)
	return entries, nil
}

func (am *AM) rewriteEntries(rootPage uint32, entries []entryItem) error {
	buf, err := am.alloc.FetchPage(rootPage)
	if err != nil {
		return err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		am.alloc.ReleasePage(rootPage)
		return err
	}
	pageNum := page.PageNum()
	newPage := slottedpage.Init(slottedpage.PageTypeGIN, pageNum, specialSize)
	sp := newPage.Special()
	binary.LittleEndian.PutUint32(sp[spOffRightPtr:], 0)
	for _, e := range entries {
		newPage.InsertTuple(encodeEntry(&e))
	}
	copy(buf, newPage.Bytes())
	am.alloc.MarkDirty(rootPage)
	am.alloc.ReleasePage(rootPage)
	return nil
}

func (am *AM) newPostingPage(tid slottedpage.ItemID) (uint32, error) {
	pageNum, err := am.alloc.AllocPage()
	if err != nil {
		return 0, err
	}
	page := slottedpage.Init(slottedpage.PageTypeGIN, pageNum, specialSize)
	sp := page.Special()
	binary.LittleEndian.PutUint32(sp[spOffRightPtr:], 0)
	page.InsertTuple(encodeTID(tid))

	buf, err := am.alloc.FetchPage(pageNum)
	if err != nil {
		return 0, err
	}
	copy(buf, page.Bytes())
	am.alloc.MarkDirty(pageNum)
	am.alloc.ReleasePage(pageNum)
	return pageNum, nil
}

func (am *AM) appendToPosting(postingPage uint32, tid slottedpage.ItemID) error {
	// Walk the chain to find a page with space.
	cur := postingPage
	for {
		buf, err := am.alloc.FetchPage(cur)
		if err != nil {
			return err
		}
		page, err := slottedpage.FromBytes(buf)
		if err != nil {
			am.alloc.ReleasePage(cur)
			return err
		}

		encoded := encodeTID(tid)
		if page.FreeSpace() >= uint16(len(encoded))+4 {
			page.InsertTuple(encoded)
			copy(buf, page.Bytes())
			am.alloc.MarkDirty(cur)
			am.alloc.ReleasePage(cur)
			return nil
		}

		sp := page.Special()
		next := binary.LittleEndian.Uint32(sp[spOffRightPtr:])
		am.alloc.ReleasePage(cur)

		if next != 0 {
			cur = next
			continue
		}

		// Allocate overflow page.
		newPageNum, err := am.alloc.AllocPage()
		if err != nil {
			return err
		}
		newPage := slottedpage.Init(slottedpage.PageTypeGIN, newPageNum, specialSize)
		newSp := newPage.Special()
		binary.LittleEndian.PutUint32(newSp[spOffRightPtr:], 0)
		newPage.InsertTuple(encoded)

		nbuf, err := am.alloc.FetchPage(newPageNum)
		if err != nil {
			return err
		}
		copy(nbuf, newPage.Bytes())
		am.alloc.MarkDirty(newPageNum)
		am.alloc.ReleasePage(newPageNum)

		// Link from current page.
		buf2, err := am.alloc.FetchPage(cur)
		if err != nil {
			return err
		}
		page2, err := slottedpage.FromBytes(buf2)
		if err != nil {
			am.alloc.ReleasePage(cur)
			return err
		}
		sp2 := page2.Special()
		binary.LittleEndian.PutUint32(sp2[spOffRightPtr:], newPageNum)
		copy(buf2, page2.Bytes())
		am.alloc.MarkDirty(cur)
		am.alloc.ReleasePage(cur)
		return nil
	}
}

func (am *AM) readPostingList(postingPage uint32) ([]slottedpage.ItemID, error) {
	var tids []slottedpage.ItemID
	cur := postingPage
	for cur != 0 {
		buf, err := am.alloc.FetchPage(cur)
		if err != nil {
			return tids, err
		}
		page, err := slottedpage.FromBytes(buf)
		if err != nil {
			am.alloc.ReleasePage(cur)
			return tids, err
		}
		for i := uint16(0); ; i++ {
			raw, err := page.GetTuple(i)
			if err != nil {
				break
			}
			if len(raw) == tidSize {
				tids = append(tids, decodeTID(raw))
			}
		}
		sp := page.Special()
		next := binary.LittleEndian.Uint32(sp[spOffRightPtr:])
		am.alloc.ReleasePage(cur)
		cur = next
	}
	return tids, nil
}

// -----------------------------------------------------------------------
// scan — implements index.IndexScan
// -----------------------------------------------------------------------

type scan struct {
	am       *AM
	rootPage uint32

	// Set by Rescan.
	hasKey bool
	key    int64

	// Iterator state.
	started bool
	tids    []slottedpage.ItemID
	tidIdx  int
	done    bool
}

func (s *scan) Rescan(keys []index.ScanKey) error {
	s.hasKey = false
	s.started = false
	s.done = false
	s.tids = nil
	s.tidIdx = 0

	for _, sk := range keys {
		if sk.Strategy == index.StrategyEqual {
			v, ok := index.DatumToInt64(sk.Value)
			if ok {
				s.hasKey = true
				s.key = v
			}
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
		if !s.hasKey {
			// GIN only supports equality scans.
			s.done = true
			return slottedpage.ItemID{}, false, nil
		}

		// Find the key in the entry tree.
		entries, err := s.am.readEntries(s.rootPage)
		if err != nil {
			return slottedpage.ItemID{}, false, err
		}
		idx := sort.Search(len(entries), func(i int) bool { return entries[i].Key >= s.key })
		if idx >= len(entries) || entries[idx].Key != s.key {
			s.done = true
			return slottedpage.ItemID{}, false, nil
		}

		// Load the posting list.
		s.tids, err = s.am.readPostingList(entries[idx].PostingPage)
		if err != nil {
			return slottedpage.ItemID{}, false, err
		}
		s.tidIdx = 0
	}

	if s.tidIdx >= len(s.tids) {
		s.done = true
		return slottedpage.ItemID{}, false, nil
	}

	result := s.tids[s.tidIdx]
	s.tidIdx++
	return result, true, nil
}

func (s *scan) End() {
	s.done = true
	s.started = false
}

var _ index.IndexScan = (*scan)(nil)

// -----------------------------------------------------------------------
// Datum conversion
// -----------------------------------------------------------------------

