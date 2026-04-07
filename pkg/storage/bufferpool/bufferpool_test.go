package bufferpool

import (
	"path/filepath"
	"testing"

	"github.com/gololadb/loladb/pkg/storage/pageio"
)

func newTestPool(t *testing.T, size int) (*BufferPool, *pageio.PageIO) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.lodb")
	pio, err := pageio.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pio.Close() })
	return New(pio, size), pio
}

func TestFetchAndRelease(t *testing.T) {
	bp, pio := newTestPool(t, 8)

	buf := make([]byte, pageio.PageSize)
	buf[0] = 0xAA
	if err := pio.WritePage(0, buf); err != nil {
		t.Fatal(err)
	}

	page, err := bp.FetchPage(0)
	if err != nil {
		t.Fatal(err)
	}
	if page[0] != 0xAA {
		t.Fatalf("expected 0xAA, got %02X", page[0])
	}
	bp.ReleasePage(0)
}

func TestCacheHit(t *testing.T) {
	bp, _ := newTestPool(t, 8)

	page1, err := bp.FetchPage(5)
	if err != nil {
		t.Fatal(err)
	}
	page1[0] = 0x42
	bp.ReleasePage(5)

	page2, err := bp.FetchPage(5)
	if err != nil {
		t.Fatal(err)
	}
	if page2[0] != 0x42 {
		t.Fatalf("cache miss: expected 0x42, got %02X", page2[0])
	}
	bp.ReleasePage(5)
}

func TestMarkDirtyAndFlush(t *testing.T) {
	bp, pio := newTestPool(t, 8)

	page, err := bp.FetchPage(0)
	if err != nil {
		t.Fatal(err)
	}
	page[0] = 0xBB
	bp.MarkDirty(0)
	bp.ReleasePage(0)

	if err := bp.FlushAll(); err != nil {
		t.Fatal(err)
	}

	diskBuf := make([]byte, pageio.PageSize)
	if err := pio.ReadPage(0, diskBuf); err != nil {
		t.Fatal(err)
	}
	if diskBuf[0] != 0xBB {
		t.Fatalf("flush failed: expected 0xBB on disk, got %02X", diskBuf[0])
	}
}

func TestCleanPageNotFlushed(t *testing.T) {
	bp, pio := newTestPool(t, 8)

	buf := make([]byte, pageio.PageSize)
	buf[0] = 0x11
	if err := pio.WritePage(0, buf); err != nil {
		t.Fatal(err)
	}

	page, err := bp.FetchPage(0)
	if err != nil {
		t.Fatal(err)
	}
	page[0] = 0x99
	bp.ReleasePage(0)

	if err := bp.FlushAll(); err != nil {
		t.Fatal(err)
	}

	diskBuf := make([]byte, pageio.PageSize)
	if err := pio.ReadPage(0, diskBuf); err != nil {
		t.Fatal(err)
	}
	if diskBuf[0] != 0x11 {
		t.Fatalf("clean page was flushed: expected 0x11, got %02X", diskBuf[0])
	}
}

func TestEviction(t *testing.T) {
	bp, _ := newTestPool(t, 4)

	for pg := uint32(0); pg < 4; pg++ {
		page, err := bp.FetchPage(pg)
		if err != nil {
			t.Fatal(err)
		}
		page[0] = byte(pg + 1)
		bp.MarkDirty(pg)
		bp.ReleasePage(pg)
	}

	page, err := bp.FetchPage(10)
	if err != nil {
		t.Fatal(err)
	}
	bp.ReleasePage(10)
	_ = page

	evicted := 0
	bp.mu.Lock()
	for pg := uint32(0); pg < 4; pg++ {
		if _, ok := bp.pageMap[pg]; !ok {
			evicted++
		}
	}
	bp.mu.Unlock()
	if evicted == 0 {
		t.Fatal("expected at least one eviction")
	}
}

func TestEvictionFlushesDirty(t *testing.T) {
	bp, pio := newTestPool(t, 2)

	page0, err := bp.FetchPage(0)
	if err != nil {
		t.Fatal(err)
	}
	page0[0] = 0xDD
	bp.MarkDirty(0)
	bp.ReleasePage(0)

	page1, err := bp.FetchPage(1)
	if err != nil {
		t.Fatal(err)
	}
	page1[0] = 0xEE
	bp.MarkDirty(1)
	bp.ReleasePage(1)

	_, err = bp.FetchPage(2)
	if err != nil {
		t.Fatal(err)
	}
	bp.ReleasePage(2)

	diskBuf := make([]byte, pageio.PageSize)
	if err := pio.ReadPage(0, diskBuf); err != nil {
		t.Fatal(err)
	}
	disk0 := diskBuf[0]

	if err := pio.ReadPage(1, diskBuf); err != nil {
		t.Fatal(err)
	}
	disk1 := diskBuf[0]

	if disk0 != 0xDD && disk1 != 0xEE {
		t.Fatal("evicted dirty page was not flushed to disk")
	}
}

func TestAllPinnedError(t *testing.T) {
	bp, _ := newTestPool(t, 2)

	_, err := bp.FetchPage(0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = bp.FetchPage(1)
	if err != nil {
		t.Fatal(err)
	}

	_, err = bp.FetchPage(2)
	if err == nil {
		t.Fatal("expected error when all frames are pinned")
	}

	bp.ReleasePage(0)
	bp.ReleasePage(1)
}

func TestPinnedPageNotEvicted(t *testing.T) {
	bp, _ := newTestPool(t, 2)

	page0, err := bp.FetchPage(0)
	if err != nil {
		t.Fatal(err)
	}
	page0[0] = 0xAA

	_, err = bp.FetchPage(1)
	if err != nil {
		t.Fatal(err)
	}
	bp.ReleasePage(1)

	_, err = bp.FetchPage(2)
	if err != nil {
		t.Fatal(err)
	}
	bp.ReleasePage(2)

	bp.mu.Lock()
	_, ok := bp.pageMap[0]
	bp.mu.Unlock()
	if !ok {
		t.Fatal("pinned page 0 was evicted")
	}

	bp.ReleasePage(0)
}

func TestMultiplePinUnpin(t *testing.T) {
	bp, _ := newTestPool(t, 4)

	for i := 0; i < 5; i++ {
		_, err := bp.FetchPage(0)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 4; i++ {
		bp.ReleasePage(0)
	}

	bp.mu.Lock()
	idx := bp.pageMap[0]
	pin := bp.frames[idx].pinCount
	bp.mu.Unlock()
	if pin != 1 {
		t.Fatalf("expected pinCount=1, got %d", pin)
	}

	bp.ReleasePage(0)
}

func TestUsageCountDecay(t *testing.T) {
	bp, _ := newTestPool(t, 3)

	for i := 0; i < 5; i++ {
		_, err := bp.FetchPage(0)
		if err != nil {
			t.Fatal(err)
		}
		bp.ReleasePage(0)
	}

	_, err := bp.FetchPage(1)
	if err != nil {
		t.Fatal(err)
	}
	bp.ReleasePage(1)

	_, err = bp.FetchPage(2)
	if err != nil {
		t.Fatal(err)
	}
	bp.ReleasePage(2)

	_, err = bp.FetchPage(3)
	if err != nil {
		t.Fatal(err)
	}
	bp.ReleasePage(3)

	bp.mu.Lock()
	_, page0Cached := bp.pageMap[0]
	bp.mu.Unlock()
	if !page0Cached {
		t.Fatal("frequently used page 0 was evicted before less-used pages")
	}
}

func TestFetchBeyondEOF(t *testing.T) {
	bp, _ := newTestPool(t, 8)

	page, err := bp.FetchPage(999)
	if err != nil {
		t.Fatal(err)
	}
	for i, b := range page {
		if b != 0 {
			t.Fatalf("expected zero at byte %d, got %d", i, b)
		}
	}
	bp.ReleasePage(999)
}

func TestDefaultPoolSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.lodb")
	pio, err := pageio.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer pio.Close()

	bp := New(pio, 0)
	if len(bp.frames) != DefaultPoolSize {
		t.Fatalf("expected %d frames, got %d", DefaultPoolSize, len(bp.frames))
	}
}

func TestConcurrentFetchRelease(t *testing.T) {
	bp, _ := newTestPool(t, 32)

	errc := make(chan error, 20)
	for g := 0; g < 20; g++ {
		pg := uint32(g % 10)
		go func() {
			page, err := bp.FetchPage(pg)
			if err != nil {
				errc <- err
				return
			}
			_ = page[0]
			bp.ReleasePage(pg)
			errc <- nil
		}()
	}

	for i := 0; i < 20; i++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
}

func TestFlushAllEmpty(t *testing.T) {
	bp, _ := newTestPool(t, 4)
	if err := bp.FlushAll(); err != nil {
		t.Fatal(err)
	}
}

func TestWriteThrough(t *testing.T) {
	bp, pio := newTestPool(t, 4)

	page, err := bp.FetchPage(3)
	if err != nil {
		t.Fatal(err)
	}
	for i := range page {
		page[i] = byte(i % 199)
	}
	bp.MarkDirty(3)
	bp.ReleasePage(3)

	if err := bp.FlushAll(); err != nil {
		t.Fatal(err)
	}

	diskBuf := make([]byte, pageio.PageSize)
	if err := pio.ReadPage(3, diskBuf); err != nil {
		t.Fatal(err)
	}
	for i := range diskBuf {
		if diskBuf[i] != byte(i%199) {
			t.Fatalf("mismatch at byte %d: got %d, want %d", i, diskBuf[i], byte(i%199))
		}
	}
}

func TestReleaseUnknownPage(t *testing.T) {
	bp, _ := newTestPool(t, 4)
	bp.ReleasePage(999)
}

func TestMarkDirtyUnknownPage(t *testing.T) {
	bp, _ := newTestPool(t, 4)
	bp.MarkDirty(999)
}

func TestFullCycleEviction(t *testing.T) {
	bp, pio := newTestPool(t, 4)

	for pg := uint32(0); pg < 20; pg++ {
		page, err := bp.FetchPage(pg)
		if err != nil {
			t.Fatal(err)
		}
		page[0] = byte(pg)
		bp.MarkDirty(pg)
		bp.ReleasePage(pg)
	}

	if err := bp.FlushAll(); err != nil {
		t.Fatal(err)
	}

	diskBuf := make([]byte, pageio.PageSize)
	for pg := uint32(0); pg < 20; pg++ {
		if err := pio.ReadPage(pg, diskBuf); err != nil {
			t.Fatal(err)
		}
		if diskBuf[0] != byte(pg) {
			t.Fatalf("page %d: expected %d, got %d", pg, byte(pg), diskBuf[0])
		}
	}
}

func TestReleaseDoesNotGoNegative(t *testing.T) {
	bp, _ := newTestPool(t, 4)

	_, err := bp.FetchPage(0)
	if err != nil {
		t.Fatal(err)
	}
	bp.ReleasePage(0)
	bp.ReleasePage(0)

	bp.mu.Lock()
	pin := bp.frames[bp.pageMap[0]].pinCount
	bp.mu.Unlock()
	if pin != 0 {
		t.Fatalf("pinCount went negative or wrong: %d", pin)
	}
}

func TestStressSequential(t *testing.T) {
	bp, pio := newTestPool(t, 8)

	for pg := uint32(0); pg < 100; pg++ {
		page, err := bp.FetchPage(pg)
		if err != nil {
			t.Fatalf("fetch page %d: %v", pg, err)
		}
		page[0] = byte(pg)
		page[pageio.PageSize-1] = byte(pg)
		bp.MarkDirty(pg)
		bp.ReleasePage(pg)
	}

	if err := bp.FlushAll(); err != nil {
		t.Fatal(err)
	}

	diskBuf := make([]byte, pageio.PageSize)
	for pg := uint32(0); pg < 100; pg++ {
		if err := pio.ReadPage(pg, diskBuf); err != nil {
			t.Fatal(err)
		}
		if diskBuf[0] != byte(pg) || diskBuf[pageio.PageSize-1] != byte(pg) {
			t.Fatalf("page %d corrupted", pg)
		}
	}
}

func TestOverwriteViaPool(t *testing.T) {
	bp, _ := newTestPool(t, 4)

	page, err := bp.FetchPage(0)
	if err != nil {
		t.Fatal(err)
	}
	page[0] = 1
	bp.MarkDirty(0)
	bp.ReleasePage(0)

	page, err = bp.FetchPage(0)
	if err != nil {
		t.Fatal(err)
	}
	if page[0] != 1 {
		t.Fatalf("expected 1, got %d", page[0])
	}
	page[0] = 2
	bp.MarkDirty(0)
	bp.ReleasePage(0)

	page, err = bp.FetchPage(0)
	if err != nil {
		t.Fatal(err)
	}
	if page[0] != 2 {
		t.Fatalf("expected 2 after overwrite, got %d", page[0])
	}
	bp.ReleasePage(0)
}

func TestConcurrentDirtyFlush(t *testing.T) {
	bp, pio := newTestPool(t, 16)

	errc := make(chan error, 50)
	for g := 0; g < 50; g++ {
		pg := uint32(g % 16)
		go func(id int) {
			page, err := bp.FetchPage(pg)
			if err != nil {
				errc <- err
				return
			}
			page[0] = byte(pg)
			bp.MarkDirty(pg)
			bp.ReleasePage(pg)
			errc <- nil
		}(g)
	}

	for i := 0; i < 50; i++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}

	if err := bp.FlushAll(); err != nil {
		t.Fatal(err)
	}

	diskBuf := make([]byte, pageio.PageSize)
	for pg := uint32(0); pg < 16; pg++ {
		if err := pio.ReadPage(pg, diskBuf); err != nil {
			t.Fatal(err)
		}
		if diskBuf[0] != byte(pg) {
			t.Fatalf("page %d: expected %d, got %d", pg, byte(pg), diskBuf[0])
		}
	}
}
