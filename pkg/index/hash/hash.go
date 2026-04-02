// Package hash implements a linear-hashing index access method for LolaDB,
// mirroring PostgreSQL's hash AM (src/backend/access/hash).
//
// On-disk layout:
//   - Page 0 (root/meta): meta page with bucket count, masks, fill factor.
//   - Pages 1..N: bucket pages, each a slotted page holding hash entries.
//   - Overflow pages linked via rightPtr in the special area.
//
// Entries are 14 bytes: hash(uint32) + pageNum(uint32) + slotNum(uint16) +
// originalKey(int64 truncated to uint32 for collision detection... actually
// we store the full key for simplicity).
//
// Entry format: key(int64, 8B) + page(uint32, 4B) + slot(uint16, 2B) = 14B
// (same as btree for simplicity, but the key is the original value, not sorted).
//
// The AM only supports StrategyEqual scans.
package hash

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"

	"github.com/gololadb/loladb/pkg/index"
	"github.com/gololadb/loladb/pkg/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
)

// -----------------------------------------------------------------------
// On-disk constants
// -----------------------------------------------------------------------

// Meta page special area (20 bytes):
//
//	magic      (4B)
//	numBuckets (4B): current number of buckets
//	highMask   (4B): mask for full table
//	lowMask    (4B): mask for lower half
//	ntuples    (4B): total number of tuples
const metaSpecialSize = 20

const (
	metaOffMagic      = 0
	metaOffNumBuckets = 4
	metaOffHighMask   = 8
	metaOffLowMask    = 12
	metaOffNTuples    = 16
)

const hashMagic = 0x48415348 // "HASH"

// Bucket page special area (8 bytes):
//
//	overflowPage (4B): next overflow page (0 = none)
//	flags        (4B): reserved
const bucketSpecialSize = 8

const (
	bucketOffOverflow = 0
	bucketOffFlags    = 4
)

// Entry is a key-value pair stored in a hash bucket.
type entry struct {
	Key     int64
	PageNum uint32
	SlotNum uint16
}

const entrySize = 14

func encodeEntry(e *entry) []byte {
	buf := make([]byte, entrySize)
	binary.LittleEndian.PutUint64(buf[0:], uint64(e.Key))
	binary.LittleEndian.PutUint32(buf[8:], e.PageNum)
	binary.LittleEndian.PutUint16(buf[12:], e.SlotNum)
	return buf
}

func decodeEntry(buf []byte) entry {
	return entry{
		Key:     int64(binary.LittleEndian.Uint64(buf[0:])),
		PageNum: binary.LittleEndian.Uint32(buf[8:]),
		SlotNum: binary.LittleEndian.Uint16(buf[12:]),
	}
}

// hashKey computes a uint32 hash of an int64 key using FNV-1a.
func hashKey(key int64) uint32 {
	h := fnv.New32a()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(key))
	h.Write(buf[:])
	return h.Sum32()
}

// -----------------------------------------------------------------------
// Meta page helpers
// -----------------------------------------------------------------------

type meta struct {
	numBuckets uint32
	highMask   uint32
	lowMask    uint32
	ntuples    uint32
}

func readMeta(sp []byte) meta {
	return meta{
		numBuckets: binary.LittleEndian.Uint32(sp[metaOffNumBuckets:]),
		highMask:   binary.LittleEndian.Uint32(sp[metaOffHighMask:]),
		lowMask:    binary.LittleEndian.Uint32(sp[metaOffLowMask:]),
		ntuples:    binary.LittleEndian.Uint32(sp[metaOffNTuples:]),
	}
}

func writeMeta(sp []byte, m meta) {
	binary.LittleEndian.PutUint32(sp[metaOffMagic:], hashMagic)
	binary.LittleEndian.PutUint32(sp[metaOffNumBuckets:], m.numBuckets)
	binary.LittleEndian.PutUint32(sp[metaOffHighMask:], m.highMask)
	binary.LittleEndian.PutUint32(sp[metaOffLowMask:], m.lowMask)
	binary.LittleEndian.PutUint32(sp[metaOffNTuples:], m.ntuples)
}

// bucketFor computes the bucket number for a hash code using linear hashing masks.
func bucketFor(hashCode uint32, m meta) uint32 {
	bucket := hashCode & m.highMask
	if bucket >= m.numBuckets {
		bucket = hashCode & m.lowMask
	}
	return bucket
}

// computeMasks computes highMask and lowMask for a given bucket count.
func computeMasks(numBuckets uint32) (highMask, lowMask uint32) {
	// Find the highest power of 2 <= numBuckets.
	p := uint32(1)
	for p*2 <= numBuckets {
		p *= 2
	}
	highMask = p*2 - 1
	lowMask = p - 1
	return
}

// Initial number of buckets.
const initialBuckets = 4

// fillFactor: split when average entries per bucket exceeds this.
const fillFactor = 10

// -----------------------------------------------------------------------
// AM — implements index.IndexAM
// -----------------------------------------------------------------------

// AM is the hash index access method.
type AM struct {
	alloc index.PageAllocator
}

// NewAM creates a hash access method backed by the given allocator.
func NewAM(alloc index.PageAllocator) *AM {
	return &AM{alloc: alloc}
}

func (am *AM) CanOrder() bool    { return false }
func (am *AM) CanUnique() bool   { return false }
func (am *AM) CanBackward() bool { return true }

// InitRootPage allocates a meta page and initial bucket pages.
func (am *AM) InitRootPage() (uint32, error) {
	// Allocate meta page.
	metaPageNum, err := am.alloc.AllocPage()
	if err != nil {
		return 0, err
	}

	// Allocate initial bucket pages.
	bucketPages := make([]uint32, initialBuckets)
	for i := range bucketPages {
		pn, err := am.alloc.AllocPage()
		if err != nil {
			return 0, err
		}
		bucketPages[i] = pn
		// Initialize as empty bucket page.
		if err := am.initBucketPage(pn); err != nil {
			return 0, err
		}
	}

	// Write meta page.
	hm, lm := computeMasks(initialBuckets)
	m := meta{
		numBuckets: initialBuckets,
		highMask:   hm,
		lowMask:    lm,
		ntuples:    0,
	}

	metaPage := slottedpage.Init(slottedpage.PageTypeHash, metaPageNum, metaSpecialSize)
	sp := metaPage.Special()
	writeMeta(sp, m)

	// Store bucket page numbers as tuples in the meta page.
	for _, bp := range bucketPages {
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], bp)
		metaPage.InsertTuple(buf[:])
	}

	buf, err := am.alloc.FetchPage(metaPageNum)
	if err != nil {
		return 0, err
	}
	copy(buf, metaPage.Bytes())
	am.alloc.MarkDirty(metaPageNum)
	am.alloc.ReleasePage(metaPageNum)

	return metaPageNum, nil
}

func (am *AM) initBucketPage(pageNum uint32) error {
	page := slottedpage.Init(slottedpage.PageTypeHash, pageNum, bucketSpecialSize)
	sp := page.Special()
	binary.LittleEndian.PutUint32(sp[bucketOffOverflow:], 0)
	binary.LittleEndian.PutUint32(sp[bucketOffFlags:], 0)

	buf, err := am.alloc.FetchPage(pageNum)
	if err != nil {
		return err
	}
	copy(buf, page.Bytes())
	am.alloc.MarkDirty(pageNum)
	am.alloc.ReleasePage(pageNum)
	return nil
}

// Insert adds a single (key, TID) entry.
func (am *AM) Insert(rootPage uint32, key tuple.Datum, tid slottedpage.ItemID) (uint32, error) {
	k, ok := index.DatumToInt64(key)
	if !ok {
		return rootPage, fmt.Errorf("hash: non-indexable datum type %d", key.Type)
	}

	// Read meta.
	m, err := am.loadMeta(rootPage)
	if err != nil {
		return rootPage, err
	}

	h := hashKey(k)
	bucket := bucketFor(h, m)

	// Get bucket page number.
	bucketPageNum, err := am.getBucketPage(rootPage, bucket)
	if err != nil {
		return rootPage, err
	}

	// Insert into bucket chain.
	e := entry{Key: k, PageNum: tid.Page, SlotNum: tid.Slot}
	if err := am.insertIntoBucket(bucketPageNum, e); err != nil {
		return rootPage, err
	}

	// Update tuple count.
	m.ntuples++
	if err := am.saveMeta(rootPage, m); err != nil {
		return rootPage, err
	}

	// Check if we need to split.
	if m.ntuples > m.numBuckets*fillFactor {
		if err := am.split(rootPage); err != nil {
			return rootPage, err
		}
	}

	return rootPage, nil
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

func (am *AM) loadMeta(metaPage uint32) (meta, error) {
	buf, err := am.alloc.FetchPage(metaPage)
	if err != nil {
		return meta{}, err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		am.alloc.ReleasePage(metaPage)
		return meta{}, err
	}
	sp := page.Special()
	m := readMeta(sp)
	am.alloc.ReleasePage(metaPage)
	return m, nil
}

func (am *AM) saveMeta(metaPage uint32, m meta) error {
	buf, err := am.alloc.FetchPage(metaPage)
	if err != nil {
		return err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		am.alloc.ReleasePage(metaPage)
		return err
	}
	sp := page.Special()
	writeMeta(sp, m)
	am.alloc.MarkDirty(metaPage)
	am.alloc.ReleasePage(metaPage)
	return nil
}

func (am *AM) getBucketPage(metaPage uint32, bucket uint32) (uint32, error) {
	buf, err := am.alloc.FetchPage(metaPage)
	if err != nil {
		return 0, err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		am.alloc.ReleasePage(metaPage)
		return 0, err
	}
	raw, err := page.GetTuple(uint16(bucket))
	am.alloc.ReleasePage(metaPage)
	if err != nil {
		return 0, fmt.Errorf("hash: bucket %d not found in meta page", bucket)
	}
	return binary.LittleEndian.Uint32(raw), nil
}

func (am *AM) insertIntoBucket(pageNum uint32, e entry) error {
	encoded := encodeEntry(&e)

	// Walk the chain to find a page with space.
	cur := pageNum
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

		if page.FreeSpace() >= uint16(len(encoded))+4 {
			page.InsertTuple(encoded)
			copy(buf, page.Bytes())
			am.alloc.MarkDirty(cur)
			am.alloc.ReleasePage(cur)
			return nil
		}

		// Check overflow.
		sp := page.Special()
		overflow := binary.LittleEndian.Uint32(sp[bucketOffOverflow:])
		am.alloc.ReleasePage(cur)

		if overflow != 0 {
			cur = overflow
			continue
		}

		// Allocate overflow page.
		newPage, err := am.alloc.AllocPage()
		if err != nil {
			return err
		}
		if err := am.initBucketPage(newPage); err != nil {
			return err
		}

		// Link overflow.
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
		binary.LittleEndian.PutUint32(sp2[bucketOffOverflow:], newPage)
		copy(buf2, page2.Bytes())
		am.alloc.MarkDirty(cur)
		am.alloc.ReleasePage(cur)

		cur = newPage
	}
}

// split performs a linear hash split: adds one new bucket and redistributes
// entries from the split bucket.
func (am *AM) split(metaPage uint32) error {
	m, err := am.loadMeta(metaPage)
	if err != nil {
		return err
	}

	oldBucket := m.numBuckets & m.lowMask // bucket to split
	newBucketNum := m.numBuckets

	// Allocate new bucket page.
	newPageNum, err := am.alloc.AllocPage()
	if err != nil {
		return err
	}
	if err := am.initBucketPage(newPageNum); err != nil {
		return err
	}

	// Register new bucket in meta page.
	buf, err := am.alloc.FetchPage(metaPage)
	if err != nil {
		return err
	}
	page, err := slottedpage.FromBytes(buf)
	if err != nil {
		am.alloc.ReleasePage(metaPage)
		return err
	}
	var bpBuf [4]byte
	binary.LittleEndian.PutUint32(bpBuf[:], newPageNum)
	page.InsertTuple(bpBuf[:])
	copy(buf, page.Bytes())
	am.alloc.MarkDirty(metaPage)
	am.alloc.ReleasePage(metaPage)

	// Update meta.
	m.numBuckets++
	m.highMask, m.lowMask = computeMasks(m.numBuckets)
	if err := am.saveMeta(metaPage, m); err != nil {
		return err
	}

	// Collect all entries from the old bucket.
	oldPageNum, err := am.getBucketPage(metaPage, oldBucket)
	if err != nil {
		return err
	}
	entries, err := am.collectBucketEntries(oldPageNum)
	if err != nil {
		return err
	}

	// Clear old bucket pages.
	if err := am.clearBucketChain(oldPageNum); err != nil {
		return err
	}

	// Redistribute entries.
	for _, e := range entries {
		h := hashKey(e.Key)
		b := bucketFor(h, m)
		var targetPage uint32
		if b == newBucketNum {
			targetPage = newPageNum
		} else {
			targetPage = oldPageNum
		}
		if err := am.insertIntoBucket(targetPage, e); err != nil {
			return err
		}
	}

	return nil
}

func (am *AM) collectBucketEntries(pageNum uint32) ([]entry, error) {
	var entries []entry
	cur := pageNum
	for cur != 0 {
		buf, err := am.alloc.FetchPage(cur)
		if err != nil {
			return entries, err
		}
		page, err := slottedpage.FromBytes(buf)
		if err != nil {
			am.alloc.ReleasePage(cur)
			return entries, err
		}
		for i := uint16(0); ; i++ {
			raw, err := page.GetTuple(i)
			if err != nil {
				break
			}
			if len(raw) == entrySize {
				entries = append(entries, decodeEntry(raw))
			}
		}
		sp := page.Special()
		next := binary.LittleEndian.Uint32(sp[bucketOffOverflow:])
		am.alloc.ReleasePage(cur)
		cur = next
	}
	return entries, nil
}

func (am *AM) clearBucketChain(pageNum uint32) error {
	// Re-initialize the first page; we don't reclaim overflow pages
	// (simplification — PostgreSQL has a freelist for overflow pages).
	return am.initBucketPage(pageNum)
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
	started  bool
	curPage  uint32
	curSlot  uint16
	done     bool
}

func (s *scan) Rescan(keys []index.ScanKey) error {
	s.hasKey = false
	s.started = false
	s.done = false
	s.curPage = 0
	s.curSlot = 0

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

	if !s.hasKey {
		// Hash only supports equality scans. No key = no results.
		s.done = true
		return slottedpage.ItemID{}, false, nil
	}

	if !s.started {
		// Position to the correct bucket.
		m, err := s.am.loadMeta(s.rootPage)
		if err != nil {
			return slottedpage.ItemID{}, false, err
		}
		h := hashKey(s.key)
		bucket := bucketFor(h, m)
		bucketPage, err := s.am.getBucketPage(s.rootPage, bucket)
		if err != nil {
			return slottedpage.ItemID{}, false, err
		}
		s.curPage = bucketPage
		s.curSlot = 0
		s.started = true
	}

	// Walk the bucket chain looking for matching entries.
	for s.curPage != 0 {
		buf, err := s.am.alloc.FetchPage(s.curPage)
		if err != nil {
			return slottedpage.ItemID{}, false, err
		}
		page, err := slottedpage.FromBytes(buf)
		if err != nil {
			s.am.alloc.ReleasePage(s.curPage)
			return slottedpage.ItemID{}, false, err
		}

		for {
			raw, err := page.GetTuple(s.curSlot)
			if err != nil {
				break // no more tuples on this page
			}
			s.curSlot++
			if len(raw) != entrySize {
				continue
			}
			e := decodeEntry(raw)
			if e.Key == s.key {
				s.am.alloc.ReleasePage(s.curPage)
				return slottedpage.ItemID{Page: e.PageNum, Slot: e.SlotNum}, true, nil
			}
		}

		// Move to overflow page.
		sp := page.Special()
		nextPage := binary.LittleEndian.Uint32(sp[bucketOffOverflow:])
		s.am.alloc.ReleasePage(s.curPage)
		s.curPage = nextPage
		s.curSlot = 0
	}

	s.done = true
	return slottedpage.ItemID{}, false, nil
}

func (s *scan) End() {
	s.done = true
	s.started = false
}

var _ index.IndexScan = (*scan)(nil)

// -----------------------------------------------------------------------
// Datum conversion
// -----------------------------------------------------------------------

