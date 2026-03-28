package pageio

import (
	"fmt"
	"path/filepath"
	"testing"
)

func tempDBPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.lodb")
}

func TestReadWriteRoundtrip(t *testing.T) {
	pio, err := Open(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pio.Close()

	writeBuf := make([]byte, PageSize)
	for i := range writeBuf {
		writeBuf[i] = byte(i % 251)
	}

	if err := pio.WritePage(0, writeBuf); err != nil {
		t.Fatal(err)
	}

	readBuf := make([]byte, PageSize)
	if err := pio.ReadPage(0, readBuf); err != nil {
		t.Fatal(err)
	}

	for i := range writeBuf {
		if readBuf[i] != writeBuf[i] {
			t.Fatalf("mismatch at byte %d: got %d, want %d", i, readBuf[i], writeBuf[i])
		}
	}
}

func TestReadBeyondEOF(t *testing.T) {
	pio, err := Open(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pio.Close()

	buf := make([]byte, PageSize)
	for i := range buf {
		buf[i] = 0xFF
	}

	if err := pio.ReadPage(999, buf); err != nil {
		t.Fatal(err)
	}

	for i, b := range buf {
		if b != 0 {
			t.Fatalf("expected zero at byte %d, got %d", i, b)
		}
	}
}

func TestWriteExtendsFile(t *testing.T) {
	pio, err := Open(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pio.Close()

	buf := make([]byte, PageSize)
	buf[0] = 0xAB

	if err := pio.WritePage(5, buf); err != nil {
		t.Fatal(err)
	}

	pages, err := pio.FilePages()
	if err != nil {
		t.Fatal(err)
	}
	if pages != 6 {
		t.Fatalf("expected 6 pages, got %d", pages)
	}

	readBuf := make([]byte, PageSize)
	if err := pio.ReadPage(5, readBuf); err != nil {
		t.Fatal(err)
	}
	if readBuf[0] != 0xAB {
		t.Fatalf("expected 0xAB at byte 0, got %02X", readBuf[0])
	}
}

func TestMultiplePages(t *testing.T) {
	pio, err := Open(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pio.Close()

	for pg := uint32(0); pg < 10; pg++ {
		buf := make([]byte, PageSize)
		buf[0] = byte(pg)
		buf[PageSize-1] = byte(pg)
		if err := pio.WritePage(pg, buf); err != nil {
			t.Fatal(err)
		}
	}

	for pg := uint32(0); pg < 10; pg++ {
		buf := make([]byte, PageSize)
		if err := pio.ReadPage(pg, buf); err != nil {
			t.Fatal(err)
		}
		if buf[0] != byte(pg) || buf[PageSize-1] != byte(pg) {
			t.Fatalf("page %d: marker mismatch", pg)
		}
	}
}

func TestSync(t *testing.T) {
	pio, err := Open(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pio.Close()

	buf := make([]byte, PageSize)
	buf[0] = 42
	if err := pio.WritePage(0, buf); err != nil {
		t.Fatal(err)
	}

	if err := pio.Sync(); err != nil {
		t.Fatal("sync failed:", err)
	}
}

func TestFilePages_Empty(t *testing.T) {
	pio, err := Open(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pio.Close()

	pages, err := pio.FilePages()
	if err != nil {
		t.Fatal(err)
	}
	if pages != 0 {
		t.Fatalf("expected 0 pages for empty file, got %d", pages)
	}
}

func TestPersistAcrossReopen(t *testing.T) {
	path := tempDBPath(t)

	pio, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, PageSize)
	buf[0] = 0xDE
	buf[1] = 0xAD
	if err := pio.WritePage(0, buf); err != nil {
		t.Fatal(err)
	}
	if err := pio.Sync(); err != nil {
		t.Fatal(err)
	}
	pio.Close()

	pio2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer pio2.Close()

	readBuf := make([]byte, PageSize)
	if err := pio2.ReadPage(0, readBuf); err != nil {
		t.Fatal(err)
	}
	if readBuf[0] != 0xDE || readBuf[1] != 0xAD {
		t.Fatalf("data not persisted: got %02X %02X", readBuf[0], readBuf[1])
	}
}

func TestInvalidBufferSize(t *testing.T) {
	pio, err := Open(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pio.Close()

	small := make([]byte, 100)
	if err := pio.ReadPage(0, small); err == nil {
		t.Fatal("expected error for undersized buffer on read")
	}
	if err := pio.WritePage(0, small); err == nil {
		t.Fatal("expected error for undersized buffer on write")
	}
}

func TestOpenNonexistentDir(t *testing.T) {
	_, err := Open(filepath.Join(t.TempDir(), "nodir", "sub", "test.lodb"))
	if err == nil {
		t.Fatal("expected error opening file in nonexistent directory")
	}
}

func TestOverwritePage(t *testing.T) {
	pio, err := Open(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pio.Close()

	buf := make([]byte, PageSize)
	buf[0] = 1
	if err := pio.WritePage(0, buf); err != nil {
		t.Fatal(err)
	}

	buf[0] = 2
	if err := pio.WritePage(0, buf); err != nil {
		t.Fatal(err)
	}

	readBuf := make([]byte, PageSize)
	if err := pio.ReadPage(0, readBuf); err != nil {
		t.Fatal(err)
	}
	if readBuf[0] != 2 {
		t.Fatalf("overwrite failed: got %d, want 2", readBuf[0])
	}
}

func TestGapPagesAreZeroed(t *testing.T) {
	pio, err := Open(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pio.Close()

	buf := make([]byte, PageSize)
	buf[0] = 0xFF
	if err := pio.WritePage(3, buf); err != nil {
		t.Fatal(err)
	}

	readBuf := make([]byte, PageSize)
	for pg := uint32(0); pg < 3; pg++ {
		if err := pio.ReadPage(pg, readBuf); err != nil {
			t.Fatal(err)
		}
		for i, b := range readBuf {
			if b != 0 {
				t.Fatalf("gap page %d byte %d: expected 0, got %d", pg, i, b)
			}
		}
	}
}

func TestConcurrentAccess(t *testing.T) {
	pio, err := Open(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pio.Close()

	for pg := uint32(0); pg < 10; pg++ {
		buf := make([]byte, PageSize)
		buf[0] = byte(pg)
		if err := pio.WritePage(pg, buf); err != nil {
			t.Fatal(err)
		}
	}

	errc := make(chan error, 100)
	for g := 0; g < 10; g++ {
		pg := uint32(g)
		go func() {
			buf := make([]byte, PageSize)
			if err := pio.ReadPage(pg, buf); err != nil {
				errc <- err
				return
			}
			if buf[0] != byte(pg) {
				errc <- fmt.Errorf("page %d: got %d", pg, buf[0])
				return
			}
			errc <- nil
		}()
	}

	for i := 0; i < 10; i++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	pio, err := Open(tempDBPath(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := pio.Close(); err != nil {
		t.Fatal(err)
	}
	_ = pio.Close()
}
