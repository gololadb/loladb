package engine

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/jespino/loladb/pkg/bufferpool"
	"github.com/jespino/loladb/pkg/freelist"
	"github.com/jespino/loladb/pkg/mvcc"
	"github.com/jespino/loladb/pkg/pageio"
	"github.com/jespino/loladb/pkg/slottedpage"
	"github.com/jespino/loladb/pkg/superblock"
	"github.com/jespino/loladb/pkg/tuple"
	"github.com/jespino/loladb/pkg/wal"
)

// Engine ties together the buffer pool, WAL, superblock, and freelist
// into a single storage layer. All page modifications go through the
// engine so that the WAL-before-data protocol is enforced.
// CheckpointInterval is the number of WAL records between automatic
// checkpoints. This prevents the WAL from growing unboundedly.
const CheckpointInterval = 1000

type Engine struct {
	dataPath      string
	walPath       string
	IO            *pageio.PageIO
	Pool          *bufferpool.BufferPool
	WAL           *wal.WAL
	Super         *superblock.Superblock
	FreeList      *freelist.FreeList
	TxMgr         *mvcc.TxManager
	pageLocks     pageLockMap
	allocMu       sync.Mutex
	walSinceChkpt uint32 // WAL records since last checkpoint
}

// pageLockMap provides per-page mutexes so concurrent modifications
// to the same page are serialized.
type pageLockMap struct {
	mu    sync.Mutex
	locks map[uint32]*sync.Mutex
}

func (m *pageLockMap) lock(pageNum uint32) {
	m.mu.Lock()
	if m.locks == nil {
		m.locks = make(map[uint32]*sync.Mutex)
	}
	l, ok := m.locks[pageNum]
	if !ok {
		l = &sync.Mutex{}
		m.locks[pageNum] = l
	}
	m.mu.Unlock()
	l.Lock()
}

func (m *pageLockMap) unlock(pageNum uint32) {
	m.mu.Lock()
	l := m.locks[pageNum]
	m.mu.Unlock()
	l.Unlock()
}

// Open opens an existing database or creates a new one. It performs
// crash recovery (WAL replay) if needed.
func Open(dataPath string, poolSize int) (*Engine, error) {
	pio, err := pageio.Open(dataPath)
	if err != nil {
		return nil, fmt.Errorf("engine: open data file: %w", err)
	}

	walPath := dataPath + ".wal"
	w, err := wal.Open(walPath)
	if err != nil {
		pio.Close()
		return nil, fmt.Errorf("engine: open WAL: %w", err)
	}

	pool := bufferpool.New(pio, poolSize)

	e := &Engine{
		dataPath: dataPath,
		walPath:  walPath,
		IO:       pio,
		Pool:     pool,
		WAL:      w,
	}

	// Determine whether this is a new or existing database.
	numPages, err := pio.FilePages()
	if err != nil {
		e.closeAll()
		return nil, err
	}

	if numPages == 0 {
		if err := e.initNew(); err != nil {
			e.closeAll()
			return nil, fmt.Errorf("engine: init new db: %w", err)
		}
	} else {
		if err := e.loadExisting(); err != nil {
			e.closeAll()
			return nil, fmt.Errorf("engine: load existing db: %w", err)
		}
		if err := e.recover(); err != nil {
			e.closeAll()
			return nil, fmt.Errorf("engine: recovery: %w", err)
		}
	}

	e.TxMgr = mvcc.NewTxManager(e.Super.NextXID)

	return e, nil
}

// initNew creates the reserved pages for a fresh database.
func (e *Engine) initNew() error {
	e.Super = superblock.New()

	e.FreeList = freelist.New(e.Super.FreeListPage)
	e.FreeList.MarkUsed(0) // superblock
	e.FreeList.MarkUsed(1) // WAL control (reserved)
	e.FreeList.MarkUsed(2) // freelist

	if err := e.Super.Save(e.IO); err != nil {
		return err
	}
	if err := e.FreeList.Save(e.IO); err != nil {
		return err
	}
	return e.IO.Sync()
}

// loadExisting reads the superblock and freelist from an existing file.
func (e *Engine) loadExisting() error {
	sb, err := superblock.Load(e.IO)
	if err != nil {
		return err
	}
	e.Super = sb

	fl, err := freelist.Load(sb.FreeListPage, e.IO)
	if err != nil {
		return err
	}
	e.FreeList = fl
	return nil
}

// recover replays any WAL records whose LSN exceeds the superblock's
// checkpoint LSN, applying them directly to the data file pages.
func (e *Engine) recover() error {
	recs, err := e.WAL.ReadAll()
	if err != nil {
		return fmt.Errorf("read WAL: %w", err)
	}

	replayed := 0
	for _, rec := range recs {
		if rec.LSN <= e.Super.CheckpointLSN {
			continue
		}
		// Apply the physical change to the page.
		buf := make([]byte, pageio.PageSize)
		if err := e.IO.ReadPage(rec.PageNum, buf); err != nil {
			return fmt.Errorf("replay LSN %d: read page %d: %w", rec.LSN, rec.PageNum, err)
		}
		copy(buf[rec.Offset:rec.Offset+rec.Len], rec.Data)
		if err := e.IO.WritePage(rec.PageNum, buf); err != nil {
			return fmt.Errorf("replay LSN %d: write page %d: %w", rec.LSN, rec.PageNum, err)
		}
		replayed++
	}

	if replayed > 0 {
		if err := e.IO.Sync(); err != nil {
			return fmt.Errorf("sync after replay: %w", err)
		}
		// Update checkpoint to the last replayed LSN.
		last := recs[len(recs)-1].LSN
		e.Super.CheckpointLSN = last
		if err := e.Super.Save(e.IO); err != nil {
			return err
		}
		if err := e.IO.Sync(); err != nil {
			return err
		}
		if err := e.WAL.Truncate(); err != nil {
			return err
		}
	}

	return nil
}

// AllocPage allocates a new page from the freelist, marks it in the
// freelist, and returns its page number. The freelist is automatically
// grown if it runs out of capacity.
func (e *Engine) AllocPage() (uint32, error) {
	e.allocMu.Lock()
	defer e.allocMu.Unlock()

	pageNum, err := e.FreeList.Alloc()
	if err != nil {
		// Try growing the freelist.
		growPage, err2 := e.FreeList.Alloc()
		if err2 != nil {
			return 0, fmt.Errorf("engine: freelist exhausted: %w", err)
		}
		e.FreeList.Grow(growPage)
		pageNum, err = e.FreeList.Alloc()
		if err != nil {
			return 0, err
		}
	}
	if pageNum >= e.Super.TotalPages {
		e.Super.TotalPages = pageNum + 1
	}
	return pageNum, nil
}

// FreePage returns a page to the freelist.
func (e *Engine) FreePage(pageNum uint32) error {
	return e.FreeList.Free(pageNum)
}

// WriteTupleToPage implements the WAL-before-data protocol for writing
// a tuple into a slotted page that is already fetched (pinned) in the
// buffer pool. It:
//  1. Inserts the tuple into the in-memory slotted page.
//  2. Computes the region of the page that changed.
//  3. Writes a WAL record for that region.
//  4. Marks the page dirty in the buffer pool.
//
// The caller must have fetched the page via e.Pool.FetchPage and must
// call e.Pool.ReleasePage afterward.
func (e *Engine) WriteTupleToPage(xid, pageNum uint32, pageBuf []byte, tupleData []byte) (uint16, error) {
	page, err := slottedpage.FromBytes(pageBuf)
	if err != nil {
		return 0, err
	}

	slot, err := page.InsertTuple(tupleData)
	if err != nil {
		return 0, err
	}

	// Copy the modified page back into the buffer pool's buffer.
	newBytes := page.Bytes()
	copy(pageBuf, newBytes)

	// WAL: log the two dirty regions of the page.
	// Region 1: header + line pointers [0..lower)
	// Region 2: tuple data [upper..special or pageEnd)
	lower := binary.LittleEndian.Uint16(newBytes[2:4])
	upper := binary.LittleEndian.Uint16(newBytes[4:6])

	// Log header + line pointers.
	if _, err := e.WAL.Append(xid, pageNum, 0, lower, newBytes[:lower]); err != nil {
		return 0, fmt.Errorf("engine: WAL append header: %w", err)
	}

	// Log tuple data region.
	tupleLen := uint16(pageio.PageSize) - upper
	lsn, err := e.WAL.Append(xid, pageNum, upper, tupleLen, newBytes[upper:])
	if err != nil {
		return 0, fmt.Errorf("engine: WAL append: %w", err)
	}

	// Update the page LSN.
	page, _ = slottedpage.FromBytes(pageBuf)
	page.SetLSN(lsn)
	copy(pageBuf, page.Bytes())

	e.Pool.MarkDirty(pageNum)
	e.maybeCheckpoint(2) // two WAL records written
	return slot, nil
}

// WriteRawToPage implements the WAL-before-data protocol for an
// arbitrary byte-level write into a page. It:
//  1. Writes a WAL record for the region.
//  2. Applies the change to the in-memory page buffer.
//  3. Marks the page dirty.
func (e *Engine) WriteRawToPage(xid, pageNum uint32, pageBuf []byte, offset uint16, data []byte) error {
	_, err := e.WAL.Append(xid, pageNum, offset, uint16(len(data)), data)
	if err != nil {
		return fmt.Errorf("engine: WAL append: %w", err)
	}
	copy(pageBuf[offset:], data)
	e.Pool.MarkDirty(pageNum)
	e.maybeCheckpoint(1)
	return nil
}

// maybeCheckpoint triggers an automatic checkpoint if enough WAL
// records have accumulated since the last one.
func (e *Engine) maybeCheckpoint(records uint32) {
	e.walSinceChkpt += records
	if e.walSinceChkpt >= CheckpointInterval {
		e.Checkpoint()
		e.walSinceChkpt = 0
	}
}

// Checkpoint flushes all dirty pages, fsyncs the data file, updates
// the superblock's checkpoint LSN, and truncates the WAL.
func (e *Engine) Checkpoint() error {
	// 1. Sync WAL to disk first (WAL-before-data protocol: WAL must
	//    be durable before we flush data pages).
	if err := e.WAL.Sync(); err != nil {
		return fmt.Errorf("engine: sync WAL: %w", err)
	}

	// 2. Flush all dirty pages to disk.
	if err := e.Pool.FlushAll(); err != nil {
		return fmt.Errorf("engine: flush pool: %w", err)
	}

	// 2. Save the freelist.
	if err := e.FreeList.Save(e.IO); err != nil {
		return fmt.Errorf("engine: save freelist: %w", err)
	}

	// 3. Fsync data file.
	if err := e.IO.Sync(); err != nil {
		return fmt.Errorf("engine: sync data: %w", err)
	}

	// 4. Update checkpoint LSN and NextXID in superblock.
	e.Super.CheckpointLSN = e.WAL.NextLSN() - 1
	if e.TxMgr != nil {
		e.Super.NextXID = e.TxMgr.NextXID()
	}
	if err := e.Super.Save(e.IO); err != nil {
		return fmt.Errorf("engine: save superblock: %w", err)
	}
	if err := e.IO.Sync(); err != nil {
		return fmt.Errorf("engine: sync superblock: %w", err)
	}

	// 5. Truncate the WAL.
	if err := e.WAL.Truncate(); err != nil {
		return fmt.Errorf("engine: truncate WAL: %w", err)
	}

	e.walSinceChkpt = 0
	return nil
}

// Close performs a checkpoint, closes all files, and removes the WAL
// file (since all data has been flushed to the data file).
func (e *Engine) Close() error {
	if err := e.Checkpoint(); err != nil {
		e.closeAll()
		return err
	}
	walPath := e.walPath
	if err := e.closeAll(); err != nil {
		return err
	}
	// Remove the WAL file after clean shutdown — all data is in the
	// data file and the WAL is empty.
	os.Remove(walPath)
	return nil
}

// SeqScan traverses all tuples in a heap page chain starting at
// headPage, applying MVCC visibility using the given snapshot. For
// each visible tuple it calls fn with the ItemID and decoded tuple.
// If fn returns false the scan stops early.
func (e *Engine) SeqScan(headPage uint32, snap *mvcc.Snapshot, fn func(id slottedpage.ItemID, tup *tuple.Tuple) bool) error {
	curPage := headPage
	for curPage != 0 {
		pageBuf, err := e.Pool.FetchPage(curPage)
		if err != nil {
			return fmt.Errorf("engine: seqscan fetch page %d: %w", curPage, err)
		}

		sp, err := slottedpage.FromBytes(pageBuf)
		if err != nil {
			e.Pool.ReleasePage(curPage)
			return fmt.Errorf("engine: seqscan parse page %d: %w", curPage, err)
		}

		numSlots := sp.NumSlots()
		keepGoing := true
		for slot := uint16(0); slot < numSlots && keepGoing; slot++ {
			if !sp.SlotIsAlive(slot) {
				continue
			}
			raw, err := sp.GetTuple(slot)
			if err != nil {
				continue
			}
			tup, err := tuple.Decode(raw)
			if err != nil {
				continue
			}
			if !snap.IsVisible(tup.Xmin, tup.Xmax) {
				continue
			}
			keepGoing = fn(slottedpage.ItemID{Page: curPage, Slot: slot}, tup)
		}

		nextPage := sp.NextPage()
		e.Pool.ReleasePage(curPage)

		if !keepGoing {
			break
		}
		curPage = nextPage
	}
	return nil
}

// Delete performs a soft-delete on the tuple at the given ItemID by
// setting its xmax to the given transaction ID. The change goes
// through the WAL-before-data protocol.
func (e *Engine) Delete(xid uint32, id slottedpage.ItemID) error {
	e.pageLocks.lock(id.Page)
	defer e.pageLocks.unlock(id.Page)

	pageBuf, err := e.Pool.FetchPage(id.Page)
	if err != nil {
		return fmt.Errorf("engine: delete fetch page %d: %w", id.Page, err)
	}
	defer e.Pool.ReleasePage(id.Page)

	sp, err := slottedpage.FromBytes(pageBuf)
	if err != nil {
		return err
	}

	raw, err := sp.GetTuple(id.Slot)
	if err != nil {
		return fmt.Errorf("engine: delete get tuple slot %d: %w", id.Slot, err)
	}

	tup, err := tuple.Decode(raw)
	if err != nil {
		return err
	}

	if tup.Xmax != 0 {
		return fmt.Errorf("engine: tuple at page %d slot %d already deleted (xmax=%d)", id.Page, id.Slot, tup.Xmax)
	}

	// Patch xmax in the raw tuple bytes (offset 4-8 within the tuple).
	xmaxBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(xmaxBytes, xid)

	// We need the tuple's byte offset within the page. Since we can't
	// get that from the slotted page API directly, we re-encode with
	// the new xmax and overwrite the entire tuple.
	tup.Xmax = xid
	newRaw := tuple.Encode(tup)

	// Delete the old slot and insert the new tuple data.
	// Since tuples are fixed in-place by slot, we do a raw overwrite
	// of just the xmax field. The tuple header's xmax is at byte 4
	// relative to the tuple start. We need to find the tuple start
	// in the page.
	//
	// Actually, the simplest correct approach: re-read the line pointer
	// to find the tuple offset, then do a raw write of the xmax field.
	// We'll write the whole updated page through the WAL.

	// Re-parse to get a mutable page, update in place, WAL, mark dirty.
	// The raw tuple is at a specific offset in the page. Let's get it
	// by finding where the slotted page stores it.
	_ = newRaw // We'll use the raw-overwrite approach instead.

	// Find the tuple's location in the page via the line pointer.
	// Line pointer for slot N is at: headerSize + slot * 4
	lpOff := 24 + uint16(id.Slot)*4
	tupleOff := binary.LittleEndian.Uint16(pageBuf[lpOff:])

	// xmax is at byte 4 within the tuple
	pageOffset := tupleOff + 4

	return e.WriteRawToPage(xid, id.Page, pageBuf, pageOffset, xmaxBytes)
}

// Insert inserts a tuple into the given heap page (or its chain),
// allocating a new page if needed. Returns the ItemID of the inserted
// tuple.
func (e *Engine) Insert(xid, headPage uint32, columns []tuple.Datum) (slottedpage.ItemID, error) {
	tup := &tuple.Tuple{
		Xmin:    xid,
		Xmax:    0,
		Columns: columns,
	}
	encoded := tuple.Encode(tup)

	// Walk the page chain to find a page with enough space.
	curPage := headPage
	var prevPage uint32
	for curPage != 0 {
		e.pageLocks.lock(curPage)

		pageBuf, err := e.Pool.FetchPage(curPage)
		if err != nil {
			e.pageLocks.unlock(curPage)
			return slottedpage.ItemID{}, err
		}

		sp, err := slottedpage.FromBytes(pageBuf)
		if err != nil {
			e.Pool.ReleasePage(curPage)
			e.pageLocks.unlock(curPage)
			return slottedpage.ItemID{}, err
		}

		if sp.FreeSpace() >= uint16(len(encoded))+4 { // +4 for line pointer
			slot, err := e.WriteTupleToPage(xid, curPage, pageBuf, encoded)
			e.Pool.ReleasePage(curPage)
			e.pageLocks.unlock(curPage)
			if err != nil {
				return slottedpage.ItemID{}, err
			}
			return slottedpage.ItemID{Page: curPage, Slot: slot}, nil
		}

		nextPage := sp.NextPage()
		e.Pool.ReleasePage(curPage)
		e.pageLocks.unlock(curPage)
		prevPage = curPage
		curPage = nextPage
	}

	// No page with enough space — allocate a new one.
	newPageNum, err := e.AllocPage()
	if err != nil {
		return slottedpage.ItemID{}, fmt.Errorf("engine: alloc page for insert: %w", err)
	}

	// Link from the previous page.
	if prevPage != 0 {
		e.pageLocks.lock(prevPage)
		prevBuf, err := e.Pool.FetchPage(prevPage)
		if err != nil {
			e.pageLocks.unlock(prevPage)
			return slottedpage.ItemID{}, err
		}
		nextBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(nextBytes, newPageNum)
		// nextPage field is at offset 20 in the page header.
		if err := e.WriteRawToPage(xid, prevPage, prevBuf, 20, nextBytes); err != nil {
			e.Pool.ReleasePage(prevPage)
			e.pageLocks.unlock(prevPage)
			return slottedpage.ItemID{}, err
		}
		e.Pool.ReleasePage(prevPage)
		e.pageLocks.unlock(prevPage)
	}

	// Init the new page.
	e.pageLocks.lock(newPageNum)
	newBuf, err := e.Pool.FetchPage(newPageNum)
	if err != nil {
		e.pageLocks.unlock(newPageNum)
		return slottedpage.ItemID{}, err
	}
	sp := slottedpage.Init(slottedpage.PageTypeHeap, newPageNum, 0)
	copy(newBuf, sp.Bytes())

	slot, err := e.WriteTupleToPage(xid, newPageNum, newBuf, encoded)
	e.Pool.ReleasePage(newPageNum)
	e.pageLocks.unlock(newPageNum)
	if err != nil {
		return slottedpage.ItemID{}, err
	}

	return slottedpage.ItemID{Page: newPageNum, Slot: slot}, nil
}

// VacuumResult holds statistics about a vacuum operation.
type VacuumResult struct {
	PagesScanned  int
	TuplesScanned int
	TuplesRemoved int
	PagesCompacted int
	PagesFreed    int
}

// Vacuum reclaims space from dead tuples in the heap chain starting
// at headPage. A tuple is dead if:
//   - xmax != 0 (has been deleted)
//   - xmax is committed
//   - xmax < horizon (no active transaction can see it)
//
// Dead tuples have their line pointers zeroed. Pages are then
// compacted. Fully empty pages are freed to the freelist.
func (e *Engine) Vacuum(headPage uint32) (*VacuumResult, error) {
	horizon := e.TxMgr.OldestActiveXID()
	result := &VacuumResult{}

	curPage := headPage
	var prevPage uint32

	for curPage != 0 {
		e.pageLocks.lock(curPage)

		pageBuf, err := e.Pool.FetchPage(curPage)
		if err != nil {
			e.pageLocks.unlock(curPage)
			return result, err
		}

		sp, err := slottedpage.FromBytes(pageBuf)
		if err != nil {
			e.Pool.ReleasePage(curPage)
			e.pageLocks.unlock(curPage)
			return result, err
		}

		result.PagesScanned++
		numSlots := sp.NumSlots()
		deadCount := 0
		aliveCount := 0

		for slot := uint16(0); slot < numSlots; slot++ {
			if !sp.SlotIsAlive(slot) {
				continue
			}
			result.TuplesScanned++

			raw, err := sp.GetTuple(slot)
			if err != nil {
				continue
			}
			tup, err := tuple.Decode(raw)
			if err != nil {
				continue
			}

			if tup.Xmax == 0 {
				aliveCount++
				continue
			}

			// Check if the deleting transaction committed and is
			// below the horizon.
			state := e.TxMgr.State(tup.Xmax)
			if state == mvcc.TxCommitted && tup.Xmax < horizon {
				sp.DeleteTuple(slot)
				deadCount++
				result.TuplesRemoved++
			} else if state == mvcc.TxAborted {
				// Aborted delete — clear xmax to revive the tuple.
				// (The tuple is still alive, the delete never happened.)
				aliveCount++
			} else {
				aliveCount++
			}
		}

		if deadCount > 0 {
			sp.Compact()
			result.PagesCompacted++

			// Write the compacted page through WAL.
			newBytes := sp.Bytes()
			copy(pageBuf, newBytes)
			e.WAL.Append(0, curPage, 0, uint16(pageio.PageSize), newBytes)
			e.Pool.MarkDirty(curPage)
		}

		nextPage := sp.NextPage()

		// If the page is completely empty and it's not the head page,
		// unlink it from the chain and free it.
		if aliveCount == 0 && deadCount > 0 && curPage != headPage && prevPage != 0 {
			// Update previous page's nextPage to skip this one.
			e.Pool.ReleasePage(curPage)
			e.pageLocks.unlock(curPage)

			e.pageLocks.lock(prevPage)
			prevBuf, err := e.Pool.FetchPage(prevPage)
			if err == nil {
				nextBytes := make([]byte, 4)
				binary.LittleEndian.PutUint32(nextBytes, nextPage)
				e.WriteRawToPage(0, prevPage, prevBuf, 20, nextBytes)
				e.Pool.ReleasePage(prevPage)
			}
			e.pageLocks.unlock(prevPage)

			e.FreeList.Free(curPage)
			result.PagesFreed++

			// Don't update prevPage — it now points to nextPage.
			curPage = nextPage
			continue
		}

		e.Pool.ReleasePage(curPage)
		e.pageLocks.unlock(curPage)

		prevPage = curPage
		curPage = nextPage
	}

	return result, nil
}

// CountHeapPages counts the number of pages in a heap chain starting
// at headPage.
func (e *Engine) CountHeapPages(headPage uint32) (int32, error) {
	count := int32(0)
	cur := headPage
	for cur != 0 {
		buf, err := e.Pool.FetchPage(cur)
		if err != nil {
			return 0, err
		}
		sp, err := slottedpage.FromBytes(buf)
		if err != nil {
			e.Pool.ReleasePage(cur)
			return 0, err
		}
		count++
		next := sp.NextPage()
		e.Pool.ReleasePage(cur)
		cur = next
	}
	return count, nil
}

func (e *Engine) closeAll() error {
	var firstErr error
	if e.WAL != nil {
		if err := e.WAL.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if e.IO != nil {
		if err := e.IO.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
