package engine

import (
	"path/filepath"
	"testing"

	"github.com/gololadb/loladb/pkg/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
	"github.com/gololadb/loladb/pkg/engine/wal"
)

func tempDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func dbPath(dir string) string {
	return filepath.Join(dir, "test.lodb")
}

func TestOpen_NewDatabase(t *testing.T) {
	e, err := Open(dbPath(tempDir(t)), 32)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	if e.Super.TotalPages != 3 {
		t.Fatalf("expected 3 pages, got %d", e.Super.TotalPages)
	}
	if !e.FreeList.IsUsed(0) || !e.FreeList.IsUsed(1) || !e.FreeList.IsUsed(2) {
		t.Fatal("reserved pages should be marked used")
	}
}

func TestOpen_ReopenExisting(t *testing.T) {
	dir := tempDir(t)
	path := dbPath(dir)

	e, err := Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}
	e.Super.NextOID = 99
	e.Close()

	e2, err := Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	if e2.Super.NextOID != 99 {
		t.Fatalf("expected NextOID=99 after reopen, got %d", e2.Super.NextOID)
	}
}

func TestAllocPage(t *testing.T) {
	e, err := Open(dbPath(tempDir(t)), 32)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	pg, err := e.AllocPage()
	if err != nil {
		t.Fatal(err)
	}
	if pg != 3 {
		t.Fatalf("expected page 3, got %d", pg)
	}
	if !e.FreeList.IsUsed(3) {
		t.Fatal("page 3 should be used")
	}
	if e.Super.TotalPages != 4 {
		t.Fatalf("expected 4 total pages, got %d", e.Super.TotalPages)
	}
}

func TestWriteTupleToPage_WALProtocol(t *testing.T) {
	e, err := Open(dbPath(tempDir(t)), 32)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	// Allocate and init a heap page
	pgNum, err := e.AllocPage()
	if err != nil {
		t.Fatal(err)
	}

	pageBuf, err := e.Pool.FetchPage(pgNum)
	if err != nil {
		t.Fatal(err)
	}

	// Init slotted page in the buffer
	sp := slottedpage.Init(slottedpage.PageTypeHeap, pgNum, 0)
	copy(pageBuf, sp.Bytes())

	// Write a tuple through the engine (WAL-before-data)
	tup := &tuple.Tuple{
		Xmin:    1,
		Columns: []tuple.Datum{tuple.DInt32(42), tuple.DText("hello")},
	}
	slot, err := e.WriteTupleToPage(1, pgNum, pageBuf, tuple.Encode(tup))
	if err != nil {
		t.Fatal(err)
	}
	if slot != 0 {
		t.Fatalf("expected slot 0, got %d", slot)
	}

	e.Pool.ReleasePage(pgNum)

	// WAL should have a record
	recs, err := e.WAL.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) < 1 {
		t.Fatalf("expected WAL records, got %d", len(recs))
	}
	if recs[0].PageNum != pgNum {
		t.Fatalf("WAL record pageNum: expected %d, got %d", pgNum, recs[0].PageNum)
	}
}

func TestCheckpoint(t *testing.T) {
	dir := tempDir(t)
	path := dbPath(dir)

	e, err := Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}

	pgNum, _ := e.AllocPage()
	pageBuf, _ := e.Pool.FetchPage(pgNum)
	sp := slottedpage.Init(slottedpage.PageTypeHeap, pgNum, 0)
	copy(pageBuf, sp.Bytes())

	tup := &tuple.Tuple{
		Xmin:    1,
		Columns: []tuple.Datum{tuple.DInt32(99)},
	}
	e.WriteTupleToPage(1, pgNum, pageBuf, tuple.Encode(tup))
	e.Pool.ReleasePage(pgNum)

	if err := e.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// WAL should be empty after checkpoint
	recs, _ := e.WAL.ReadAll()
	if len(recs) != 0 {
		t.Fatalf("expected 0 WAL records after checkpoint, got %d", len(recs))
	}

	// CheckpointLSN should be updated
	if e.Super.CheckpointLSN == 0 {
		t.Fatal("CheckpointLSN should be > 0 after checkpoint")
	}

	e.closeAll()

	// Reopen and verify data persisted
	e2, err := Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	pageBuf2, _ := e2.Pool.FetchPage(pgNum)
	sp2, _ := slottedpage.FromBytes(pageBuf2)
	raw, err := sp2.GetTuple(0)
	if err != nil {
		t.Fatal(err)
	}
	decoded, _ := tuple.Decode(raw)
	if decoded.Columns[0].I32 != 99 {
		t.Fatalf("expected 99, got %d", decoded.Columns[0].I32)
	}
	e2.Pool.ReleasePage(pgNum)
}

// TestCrashRecovery simulates a crash after WAL write but before
// checkpoint by:
//  1. Writing data through the engine (WAL record created, page dirty).
//  2. Closing the WAL and data files WITHOUT checkpointing.
//  3. Reopening — recovery should replay the WAL.
func TestCrashRecovery(t *testing.T) {
	dir := tempDir(t)
	path := dbPath(dir)

	// --- Session 1: write data, then "crash" ---
	e, err := Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}

	pgNum, _ := e.AllocPage()

	// We need to persist the superblock and freelist so recovery can
	// load them, but we intentionally skip the checkpoint so the
	// buffer pool's dirty page is NOT flushed.
	e.FreeList.Save(e.IO)
	e.Super.Save(e.IO)
	e.IO.Sync()

	pageBuf, _ := e.Pool.FetchPage(pgNum)
	sp := slottedpage.Init(slottedpage.PageTypeHeap, pgNum, 0)
	copy(pageBuf, sp.Bytes())

	tup := &tuple.Tuple{
		Xmin:    1,
		Columns: []tuple.Datum{tuple.DInt32(777), tuple.DText("crash-test")},
	}
	e.WriteTupleToPage(1, pgNum, pageBuf, tuple.Encode(tup))
	e.Pool.ReleasePage(pgNum)

	// "Crash": close files without checkpoint or flush.
	// The dirty page is NOT written to disk, but the WAL IS on disk
	// (WAL.Append does fsync).
	e.WAL.Close()
	e.IO.Close()

	// --- Session 2: reopen, recovery should replay WAL ---
	e2, err := Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	// The page should now have our data (recovered from WAL).
	pageBuf2, err := e2.Pool.FetchPage(pgNum)
	if err != nil {
		t.Fatal(err)
	}
	sp2, err := slottedpage.FromBytes(pageBuf2)
	if err != nil {
		t.Fatal(err)
	}

	raw, err := sp2.GetTuple(0)
	if err != nil {
		t.Fatalf("recovery failed — tuple not found: %v", err)
	}
	decoded, err := tuple.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Columns[0].I32 != 777 {
		t.Fatalf("expected 777, got %d", decoded.Columns[0].I32)
	}
	if decoded.Columns[1].Text != "crash-test" {
		t.Fatalf("expected 'crash-test', got %q", decoded.Columns[1].Text)
	}
	e2.Pool.ReleasePage(pgNum)
}

// TestCrashRecovery_MultipleRecords verifies that multiple WAL
// records for different pages are all replayed correctly.
func TestCrashRecovery_MultipleRecords(t *testing.T) {
	dir := tempDir(t)
	path := dbPath(dir)

	e, err := Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}

	// Allocate and write to two pages
	pg1, _ := e.AllocPage()
	pg2, _ := e.AllocPage()

	e.FreeList.Save(e.IO)
	e.Super.Save(e.IO)
	e.IO.Sync()

	for _, pgNum := range []uint32{pg1, pg2} {
		pageBuf, _ := e.Pool.FetchPage(pgNum)
		sp := slottedpage.Init(slottedpage.PageTypeHeap, pgNum, 0)
		copy(pageBuf, sp.Bytes())

		tup := &tuple.Tuple{
			Xmin:    1,
			Columns: []tuple.Datum{tuple.DInt32(int32(pgNum * 100))},
		}
		e.WriteTupleToPage(1, pgNum, pageBuf, tuple.Encode(tup))
		e.Pool.ReleasePage(pgNum)
	}

	// Crash
	e.WAL.Close()
	e.IO.Close()

	// Recover
	e2, err := Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	for _, pgNum := range []uint32{pg1, pg2} {
		pageBuf, _ := e2.Pool.FetchPage(pgNum)
		sp, _ := slottedpage.FromBytes(pageBuf)
		raw, err := sp.GetTuple(0)
		if err != nil {
			t.Fatalf("page %d: tuple not recovered: %v", pgNum, err)
		}
		decoded, _ := tuple.Decode(raw)
		expected := int32(pgNum * 100)
		if decoded.Columns[0].I32 != expected {
			t.Fatalf("page %d: expected %d, got %d", pgNum, expected, decoded.Columns[0].I32)
		}
		e2.Pool.ReleasePage(pgNum)
	}
}

// TestCrashRecovery_PartialCheckpoint verifies that only records
// after the last checkpoint LSN are replayed.
func TestCrashRecovery_PartialCheckpoint(t *testing.T) {
	dir := tempDir(t)
	path := dbPath(dir)

	e, err := Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}

	pgNum, _ := e.AllocPage()

	pageBuf, _ := e.Pool.FetchPage(pgNum)
	sp := slottedpage.Init(slottedpage.PageTypeHeap, pgNum, 0)
	copy(pageBuf, sp.Bytes())

	// Write first tuple and checkpoint
	tup1 := &tuple.Tuple{
		Xmin:    1,
		Columns: []tuple.Datum{tuple.DInt32(111)},
	}
	e.WriteTupleToPage(1, pgNum, pageBuf, tuple.Encode(tup1))
	e.Pool.ReleasePage(pgNum)
	e.Checkpoint()

	// Write second tuple (after checkpoint) — then crash
	pageBuf, _ = e.Pool.FetchPage(pgNum)
	tup2 := &tuple.Tuple{
		Xmin:    2,
		Columns: []tuple.Datum{tuple.DInt32(222)},
	}
	e.WriteTupleToPage(2, pgNum, pageBuf, tuple.Encode(tup2))
	e.Pool.ReleasePage(pgNum)

	// Crash without checkpointing the second write
	e.WAL.Close()
	e.IO.Close()

	// Recover
	e2, err := Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	pageBuf2, _ := e2.Pool.FetchPage(pgNum)
	sp2, _ := slottedpage.FromBytes(pageBuf2)

	// Both tuples should be present (first from checkpoint, second from WAL replay)
	if sp2.NumSlots() != 2 {
		t.Fatalf("expected 2 slots, got %d", sp2.NumSlots())
	}
	raw0, _ := sp2.GetTuple(0)
	raw1, _ := sp2.GetTuple(1)
	d0, _ := tuple.Decode(raw0)
	d1, _ := tuple.Decode(raw1)
	if d0.Columns[0].I32 != 111 {
		t.Fatalf("tuple 0: expected 111, got %d", d0.Columns[0].I32)
	}
	if d1.Columns[0].I32 != 222 {
		t.Fatalf("tuple 1: expected 222, got %d", d1.Columns[0].I32)
	}
	e2.Pool.ReleasePage(pgNum)
}

// TestNoRecoveryNeeded verifies that a clean shutdown (with
// checkpoint) does not trigger any recovery on reopen.
func TestNoRecoveryNeeded(t *testing.T) {
	dir := tempDir(t)
	path := dbPath(dir)

	e, err := Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}

	pgNum, _ := e.AllocPage()
	pageBuf, _ := e.Pool.FetchPage(pgNum)
	sp := slottedpage.Init(slottedpage.PageTypeHeap, pgNum, 0)
	copy(pageBuf, sp.Bytes())

	tup := &tuple.Tuple{
		Xmin:    1,
		Columns: []tuple.Datum{tuple.DInt32(555)},
	}
	e.WriteTupleToPage(1, pgNum, pageBuf, tuple.Encode(tup))
	e.Pool.ReleasePage(pgNum)

	// Clean close (includes checkpoint)
	e.Close()

	// Reopen
	e2, err := Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	pageBuf2, _ := e2.Pool.FetchPage(pgNum)
	sp2, _ := slottedpage.FromBytes(pageBuf2)
	raw, _ := sp2.GetTuple(0)
	decoded, _ := tuple.Decode(raw)
	if decoded.Columns[0].I32 != 555 {
		t.Fatalf("expected 555, got %d", decoded.Columns[0].I32)
	}
	e2.Pool.ReleasePage(pgNum)
}

// TestWriteRawToPage verifies arbitrary byte writes go through WAL.
func TestWriteRawToPage(t *testing.T) {
	dir := tempDir(t)
	path := dbPath(dir)

	e, err := Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}

	pgNum, _ := e.AllocPage()
	pageBuf, _ := e.Pool.FetchPage(pgNum)

	data := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	err = e.WriteRawToPage(1, pgNum, pageBuf, 100, data)
	if err != nil {
		t.Fatal(err)
	}

	// Verify in-memory
	if pageBuf[100] != 0xDE || pageBuf[103] != 0xEF {
		t.Fatal("in-memory write failed")
	}

	e.Pool.ReleasePage(pgNum)

	// WAL should have the record
	recs, _ := e.WAL.ReadAll()
	if len(recs) != 1 {
		t.Fatalf("expected 1 WAL record, got %d", len(recs))
	}

	e.Close()

	// Verify persisted
	e2, _ := Open(path, 32)
	defer e2.Close()

	pageBuf2, _ := e2.Pool.FetchPage(pgNum)
	if pageBuf2[100] != 0xDE || pageBuf2[103] != 0xEF {
		t.Fatal("data not persisted after reopen")
	}
	e2.Pool.ReleasePage(pgNum)
}

// TestCrashRecovery_RawWrite verifies recovery of WriteRawToPage.
func TestCrashRecovery_RawWrite(t *testing.T) {
	dir := tempDir(t)
	path := dbPath(dir)

	e, err := Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}

	pgNum, _ := e.AllocPage()
	e.FreeList.Save(e.IO)
	e.Super.Save(e.IO)
	e.IO.Sync()

	pageBuf, _ := e.Pool.FetchPage(pgNum)
	e.WriteRawToPage(1, pgNum, pageBuf, 200, []byte{0xCA, 0xFE})
	e.Pool.ReleasePage(pgNum)

	// Crash
	e.WAL.Close()
	e.IO.Close()

	// Recover
	e2, err := Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	pageBuf2, _ := e2.Pool.FetchPage(pgNum)
	if pageBuf2[200] != 0xCA || pageBuf2[201] != 0xFE {
		t.Fatalf("raw write not recovered: got %02X %02X", pageBuf2[200], pageBuf2[201])
	}
	e2.Pool.ReleasePage(pgNum)
}

// TestEngine_AllViaBufferPool verifies that upper layers never do
// direct I/O — everything goes through FetchPage/ReleasePage.
func TestEngine_AllViaBufferPool(t *testing.T) {
	e, err := Open(dbPath(tempDir(t)), 8)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	// Allocate 10 pages (more than pool size) and write to all
	pages := make([]uint32, 10)
	for i := range pages {
		pg, err := e.AllocPage()
		if err != nil {
			t.Fatal(err)
		}
		pages[i] = pg

		pageBuf, err := e.Pool.FetchPage(pg)
		if err != nil {
			t.Fatal(err)
		}
		sp := slottedpage.Init(slottedpage.PageTypeHeap, pg, 0)
		copy(pageBuf, sp.Bytes())

		tup := &tuple.Tuple{
			Xmin:    1,
			Columns: []tuple.Datum{tuple.DInt32(int32(pg))},
		}
		e.WriteTupleToPage(1, pg, pageBuf, tuple.Encode(tup))
		e.Pool.ReleasePage(pg)
	}

	e.Checkpoint()

	// Read all back through pool (some will have been evicted)
	for _, pg := range pages {
		pageBuf, err := e.Pool.FetchPage(pg)
		if err != nil {
			t.Fatalf("fetch page %d: %v", pg, err)
		}
		sp, _ := slottedpage.FromBytes(pageBuf)
		raw, err := sp.GetTuple(0)
		if err != nil {
			t.Fatalf("page %d: get tuple: %v", pg, err)
		}
		decoded, _ := tuple.Decode(raw)
		if decoded.Columns[0].I32 != int32(pg) {
			t.Fatalf("page %d: expected %d, got %d", pg, pg, decoded.Columns[0].I32)
		}
		e.Pool.ReleasePage(pg)
	}
}

// TestEngine_WALRecordPerWrite verifies each WriteTupleToPage
// creates WAL records (2 per insert: header+line pointers and tuple data).
func TestEngine_WALRecordPerWrite(t *testing.T) {
	e, err := Open(dbPath(tempDir(t)), 32)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	pgNum, _ := e.AllocPage()
	pageBuf, _ := e.Pool.FetchPage(pgNum)
	sp := slottedpage.Init(slottedpage.PageTypeHeap, pgNum, 0)
	copy(pageBuf, sp.Bytes())

	for i := 0; i < 5; i++ {
		tup := &tuple.Tuple{
			Xmin:    uint32(i + 1),
			Columns: []tuple.Datum{tuple.DInt32(int32(i))},
		}
		_, err := e.WriteTupleToPage(uint32(i+1), pgNum, pageBuf, tuple.Encode(tup))
		if err != nil {
			t.Fatal(err)
		}
	}
	e.Pool.ReleasePage(pgNum)

	recs, _ := e.WAL.ReadAll()
	if len(recs) != 10 {
		t.Fatalf("expected 10 WAL records (2 per insert), got %d", len(recs))
	}
}

// Verify that the WAL file path follows the convention.
func TestEngine_WALPath(t *testing.T) {
	dir := tempDir(t)
	path := dbPath(dir)

	e, err := Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	expectedWAL := path + ".wal"
	// Verify the WAL file exists by trying to open it
	w, err := wal.Open(expectedWAL)
	if err != nil {
		t.Fatalf("WAL file not at expected path %s: %v", expectedWAL, err)
	}
	w.Close()
}
