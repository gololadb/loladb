package wal

import (
	"path/filepath"
	"testing"
)

func tempWALPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.lodb.wal")
}

func openTestWAL(t *testing.T) *WAL {
	t.Helper()
	w, err := Open(tempWALPath(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func TestAppendAndReadAll(t *testing.T) {
	w := openTestWAL(t)

	data := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	lsn, err := w.Append(10, 5, 100, uint16(len(data)), data)
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 1 {
		t.Fatalf("expected LSN 1, got %d", lsn)
	}

	recs, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}

	r := recs[0]
	if r.LSN != 1 || r.XID != 10 || r.PageNum != 5 || r.Offset != 100 || r.Len != 4 {
		t.Fatalf("record header mismatch: %+v", r)
	}
	for i, b := range data {
		if r.Data[i] != b {
			t.Fatalf("data mismatch at %d: got %02X, want %02X", i, r.Data[i], b)
		}
	}
}

func TestMultipleRecords(t *testing.T) {
	w := openTestWAL(t)

	for i := 0; i < 10; i++ {
		data := []byte{byte(i)}
		lsn, err := w.Append(uint32(i), uint32(i*2), uint16(i*10), 1, data)
		if err != nil {
			t.Fatal(err)
		}
		if lsn != uint32(i+1) {
			t.Fatalf("expected LSN %d, got %d", i+1, lsn)
		}
	}

	recs, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 10 {
		t.Fatalf("expected 10 records, got %d", len(recs))
	}

	for i, r := range recs {
		if r.LSN != uint32(i+1) {
			t.Fatalf("record %d: expected LSN %d, got %d", i, i+1, r.LSN)
		}
		if r.XID != uint32(i) {
			t.Fatalf("record %d: expected XID %d, got %d", i, i, r.XID)
		}
		if r.Data[0] != byte(i) {
			t.Fatalf("record %d: expected data %d, got %d", i, i, r.Data[0])
		}
	}
}

func TestTruncate(t *testing.T) {
	w := openTestWAL(t)

	for i := 0; i < 5; i++ {
		if _, err := w.Append(1, 0, 0, 1, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}

	if err := w.Truncate(); err != nil {
		t.Fatal(err)
	}

	recs, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected 0 records after truncate, got %d", len(recs))
	}
}

func TestAppendAfterTruncate(t *testing.T) {
	w := openTestWAL(t)

	lsn1, err := w.Append(1, 0, 0, 1, []byte{0xAA})
	if err != nil {
		t.Fatal(err)
	}
	if lsn1 != 1 {
		t.Fatalf("expected LSN 1, got %d", lsn1)
	}

	if err := w.Truncate(); err != nil {
		t.Fatal(err)
	}

	// LSN counter continues (does not reset)
	lsn2, err := w.Append(2, 0, 0, 1, []byte{0xBB})
	if err != nil {
		t.Fatal(err)
	}
	if lsn2 != 2 {
		t.Fatalf("expected LSN 2 after truncate, got %d", lsn2)
	}

	recs, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	if recs[0].LSN != 2 {
		t.Fatalf("expected record LSN 2, got %d", recs[0].LSN)
	}
}

func TestPersistAcrossReopen(t *testing.T) {
	path := tempWALPath(t)

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("hello")
	_, err = w.Append(42, 7, 0, uint16(len(data)), data)
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.Append(43, 8, 10, 3, []byte{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	// Reopen
	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	recs, err := w2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("expected 2 records after reopen, got %d", len(recs))
	}
	if string(recs[0].Data) != "hello" {
		t.Fatalf("record 0 data: got %q, want %q", recs[0].Data, "hello")
	}
	if recs[1].XID != 43 || recs[1].PageNum != 8 {
		t.Fatalf("record 1 mismatch: %+v", recs[1])
	}

	// Next LSN should continue from where we left off
	if w2.NextLSN() != 3 {
		t.Fatalf("expected nextLSN=3 after reopen, got %d", w2.NextLSN())
	}
}

func TestNextLSN(t *testing.T) {
	w := openTestWAL(t)

	if w.NextLSN() != 1 {
		t.Fatalf("expected initial nextLSN=1, got %d", w.NextLSN())
	}

	w.Append(0, 0, 0, 1, []byte{0})
	if w.NextLSN() != 2 {
		t.Fatalf("expected nextLSN=2, got %d", w.NextLSN())
	}

	w.Append(0, 0, 0, 1, []byte{0})
	w.Append(0, 0, 0, 1, []byte{0})
	if w.NextLSN() != 4 {
		t.Fatalf("expected nextLSN=4, got %d", w.NextLSN())
	}
}

func TestEmptyWAL(t *testing.T) {
	w := openTestWAL(t)

	recs, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected 0 records in empty WAL, got %d", len(recs))
	}
}

func TestLengthMismatchError(t *testing.T) {
	w := openTestWAL(t)

	_, err := w.Append(0, 0, 0, 10, []byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for length/data mismatch")
	}
}

func TestZeroLengthData(t *testing.T) {
	w := openTestWAL(t)

	lsn, err := w.Append(1, 2, 50, 0, []byte{})
	if err != nil {
		t.Fatal(err)
	}

	recs, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	if recs[0].LSN != lsn || recs[0].Len != 0 {
		t.Fatalf("unexpected record: %+v", recs[0])
	}
}

func TestLargeRecord(t *testing.T) {
	w := openTestWAL(t)

	// Maximum Len is uint16 max = 65535
	data := make([]byte, 4000)
	for i := range data {
		data[i] = byte(i % 251)
	}

	lsn, err := w.Append(99, 42, 0, uint16(len(data)), data)
	if err != nil {
		t.Fatal(err)
	}

	recs, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	if recs[0].LSN != lsn || recs[0].Len != uint16(len(data)) {
		t.Fatalf("header mismatch: %+v", recs[0])
	}
	for i, b := range recs[0].Data {
		if b != data[i] {
			t.Fatalf("data mismatch at byte %d", i)
		}
	}
}

func TestConcurrentAppend(t *testing.T) {
	w := openTestWAL(t)

	errc := make(chan error, 20)
	for g := 0; g < 20; g++ {
		go func(id int) {
			_, err := w.Append(uint32(id), 0, 0, 1, []byte{byte(id)})
			errc <- err
		}(g)
	}

	for i := 0; i < 20; i++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}

	recs, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 20 {
		t.Fatalf("expected 20 records, got %d", len(recs))
	}

	// All LSNs should be unique and in range [1, 20]
	seen := make(map[uint32]bool)
	for _, r := range recs {
		if r.LSN < 1 || r.LSN > 20 {
			t.Fatalf("LSN out of range: %d", r.LSN)
		}
		if seen[r.LSN] {
			t.Fatalf("duplicate LSN: %d", r.LSN)
		}
		seen[r.LSN] = true
	}
}

func TestRecordsOrderedByLSN(t *testing.T) {
	w := openTestWAL(t)

	for i := 0; i < 50; i++ {
		if _, err := w.Append(uint32(i), 0, 0, 1, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}

	recs, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(recs); i++ {
		if recs[i].LSN <= recs[i-1].LSN {
			t.Fatalf("records not ordered: LSN %d after %d", recs[i].LSN, recs[i-1].LSN)
		}
	}
}

func TestOpenNonexistentDir(t *testing.T) {
	_, err := Open(filepath.Join(t.TempDir(), "no", "dir", "test.wal"))
	if err == nil {
		t.Fatal("expected error opening WAL in nonexistent directory")
	}
}

func TestTruncateThenReopen(t *testing.T) {
	path := tempWALPath(t)

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(1, 0, 0, 1, []byte{0xAA})
	w.Append(2, 0, 0, 1, []byte{0xBB})
	w.Truncate()
	w.Close()

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	recs, err := w2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected 0 records after truncate+reopen, got %d", len(recs))
	}
}

func TestStressManyRecords(t *testing.T) {
	w := openTestWAL(t)

	n := 1000
	for i := 0; i < n; i++ {
		data := []byte{byte(i), byte(i >> 8)}
		if _, err := w.Append(uint32(i), uint32(i%100), uint16(i%4096), 2, data); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	recs, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != n {
		t.Fatalf("expected %d records, got %d", n, len(recs))
	}

	for i, r := range recs {
		if r.LSN != uint32(i+1) {
			t.Fatalf("record %d: expected LSN %d, got %d", i, i+1, r.LSN)
		}
		if r.Data[0] != byte(i) || r.Data[1] != byte(i>>8) {
			t.Fatalf("record %d: data mismatch", i)
		}
	}
}
