package bufferpool

import (
	"fmt"
	"sync"

	"github.com/gololadb/loladb/pkg/storage/pageio"
)

// DefaultPoolSize is the default number of frames in the buffer pool.
// PostgreSQL uses shared_buffers = 128MB (32768 pages of 8KB).
// We use 4096 frames (16MB with 4KB pages) as a reasonable default.
const DefaultPoolSize = 4096

// frame holds one cached page in the buffer pool.
type frame struct {
	pageNum    uint32
	data       [pageio.PageSize]byte
	valid      bool // frame contains a loaded page
	dirty      bool
	pinCount   int
	usageCount int
}

// BufferPool is an in-memory page cache with clock-sweep eviction,
// modeled after PostgreSQL's shared_buffers.
type BufferPool struct {
	mu        sync.Mutex
	io        *pageio.PageIO
	frames    []frame
	pageMap   map[uint32]int // pageNum → frame index
	clockHand int
}

// New creates a buffer pool with the given number of frames
// backed by the provided PageIO.
func New(io *pageio.PageIO, size int) *BufferPool {
	if size <= 0 {
		size = DefaultPoolSize
	}
	return &BufferPool{
		io:      io,
		frames:  make([]frame, size),
		pageMap: make(map[uint32]int, size),
	}
}

// FetchPage loads a page into the pool (if not already cached), pins it,
// and returns a pointer to the in-memory page data. The caller must call
// ReleasePage when done.
func (bp *BufferPool) FetchPage(pageNum uint32) ([]byte, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Already in pool?
	if idx, ok := bp.pageMap[pageNum]; ok {
		f := &bp.frames[idx]
		f.pinCount++
		f.usageCount++
		return f.data[:], nil
	}

	// Need a frame — find one via clock-sweep
	idx, err := bp.evict()
	if err != nil {
		return nil, err
	}

	f := &bp.frames[idx]

	// Read the page from disk
	if err := bp.io.ReadPage(pageNum, f.data[:]); err != nil {
		return nil, fmt.Errorf("bufferpool: fetch page %d: %w", pageNum, err)
	}

	f.pageNum = pageNum
	f.valid = true
	f.dirty = false
	f.pinCount = 1
	f.usageCount = 1
	bp.pageMap[pageNum] = idx

	return f.data[:], nil
}

// ReleasePage decrements the pin count for a page, allowing it to be evicted.
func (bp *BufferPool) ReleasePage(pageNum uint32) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	idx, ok := bp.pageMap[pageNum]
	if !ok {
		return
	}
	f := &bp.frames[idx]
	if f.pinCount > 0 {
		f.pinCount--
	}
}

// MarkDirty marks a cached page as dirty so it will be flushed before eviction.
func (bp *BufferPool) MarkDirty(pageNum uint32) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if idx, ok := bp.pageMap[pageNum]; ok {
		bp.frames[idx].dirty = true
	}
}

// FlushAll writes all dirty pages to disk and fsyncs the file.
func (bp *BufferPool) FlushAll() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	return bp.flushAll()
}

func (bp *BufferPool) flushAll() error {
	for i := range bp.frames {
		if err := bp.flushFrame(i); err != nil {
			return err
		}
	}
	return bp.io.Sync()
}

// flushFrame writes a single dirty frame to disk and clears its dirty flag.
// Caller must hold bp.mu.
func (bp *BufferPool) flushFrame(idx int) error {
	f := &bp.frames[idx]
	if !f.valid || !f.dirty {
		return nil
	}
	if err := bp.io.WritePage(f.pageNum, f.data[:]); err != nil {
		return fmt.Errorf("bufferpool: flush page %d: %w", f.pageNum, err)
	}
	f.dirty = false
	return nil
}

// evict finds a victim frame using clock-sweep. If the victim is dirty it is
// flushed first. Returns the frame index. Caller must hold bp.mu.
func (bp *BufferPool) evict() (int, error) {
	n := len(bp.frames)

	// First pass: look for an unused (invalid) frame.
	for i := range bp.frames {
		if !bp.frames[i].valid {
			return i, nil
		}
	}

	// Clock-sweep: scan up to 2*n times to find a victim.
	for range 2 * n {
		f := &bp.frames[bp.clockHand]
		idx := bp.clockHand
		bp.clockHand = (bp.clockHand + 1) % n

		if f.pinCount > 0 {
			continue
		}
		if f.usageCount > 0 {
			f.usageCount--
			continue
		}

		// Victim found — flush if dirty, then reclaim.
		if f.dirty {
			if err := bp.flushFrame(idx); err != nil {
				return 0, err
			}
		}
		delete(bp.pageMap, f.pageNum)
		*f = frame{} // reset
		return idx, nil
	}

	return 0, fmt.Errorf("bufferpool: all frames are pinned, cannot evict")
}
