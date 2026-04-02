package engine

import (
	"fmt"
	"sync"
	"testing"

	"github.com/gololadb/loladb/pkg/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
)

// helper: create an engine and a heap page, return engine + head page num.
func setupHeap(t *testing.T) (*Engine, uint32) {
	t.Helper()
	e, err := Open(dbPath(tempDir(t)), 32)
	if err != nil {
		t.Fatal(err)
	}

	pgNum, err := e.AllocPage()
	if err != nil {
		t.Fatal(err)
	}

	// Init the heap page through the buffer pool.
	pageBuf, err := e.Pool.FetchPage(pgNum)
	if err != nil {
		t.Fatal(err)
	}
	sp := slottedpage.Init(slottedpage.PageTypeHeap, pgNum, 0)
	copy(pageBuf, sp.Bytes())
	e.Pool.MarkDirty(pgNum)
	e.Pool.ReleasePage(pgNum)

	return e, pgNum
}

func TestMVCC_InsertAndScan(t *testing.T) {
	e, headPage := setupHeap(t)
	defer e.Close()

	tx1 := e.TxMgr.Begin()
	e.Insert(tx1, headPage, []tuple.Datum{tuple.DInt32(1), tuple.DText("Alice")})
	e.Insert(tx1, headPage, []tuple.Datum{tuple.DInt32(2), tuple.DText("Bob")})
	e.TxMgr.Commit(tx1)

	// Read with a new transaction
	tx2 := e.TxMgr.Begin()
	snap := e.TxMgr.Snapshot(tx2)

	var results []string
	e.SeqScan(headPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		results = append(results, tup.Columns[1].Text)
		return true
	})

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0] != "Alice" || results[1] != "Bob" {
		t.Fatalf("unexpected results: %v", results)
	}
	e.TxMgr.Commit(tx2)
}

func TestMVCC_UncommittedNotVisible(t *testing.T) {
	e, headPage := setupHeap(t)
	defer e.Close()

	tx1 := e.TxMgr.Begin()
	e.Insert(tx1, headPage, []tuple.Datum{tuple.DInt32(1)})
	// tx1 does NOT commit

	// tx2 should not see tx1's insert
	tx2 := e.TxMgr.Begin()
	snap := e.TxMgr.Snapshot(tx2)

	count := 0
	e.SeqScan(headPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		count++
		return true
	})

	if count != 0 {
		t.Fatalf("expected 0 visible tuples, got %d", count)
	}
	e.TxMgr.Commit(tx2)
	e.TxMgr.Abort(tx1)
}

func TestMVCC_OwnInsertVisible(t *testing.T) {
	e, headPage := setupHeap(t)
	defer e.Close()

	tx1 := e.TxMgr.Begin()
	e.Insert(tx1, headPage, []tuple.Datum{tuple.DInt32(42)})

	// tx1's own snapshot should see its insert
	snap := e.TxMgr.Snapshot(tx1)
	count := 0
	e.SeqScan(headPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if tup.Columns[0].I32 != 42 {
			t.Fatalf("expected 42, got %d", tup.Columns[0].I32)
		}
		count++
		return true
	})

	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}
	e.TxMgr.Commit(tx1)
}

func TestMVCC_DeleteSoftDelete(t *testing.T) {
	e, headPage := setupHeap(t)
	defer e.Close()

	// Insert and commit
	tx1 := e.TxMgr.Begin()
	id, err := e.Insert(tx1, headPage, []tuple.Datum{tuple.DInt32(1), tuple.DText("delete-me")})
	if err != nil {
		t.Fatal(err)
	}
	e.TxMgr.Commit(tx1)

	// Delete
	tx2 := e.TxMgr.Begin()
	if err := e.Delete(tx2, id); err != nil {
		t.Fatal(err)
	}
	e.TxMgr.Commit(tx2)

	// New reader should not see the deleted tuple
	tx3 := e.TxMgr.Begin()
	snap := e.TxMgr.Snapshot(tx3)
	count := 0
	e.SeqScan(headPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		count++
		return true
	})
	if count != 0 {
		t.Fatalf("expected 0 after delete, got %d", count)
	}
	e.TxMgr.Commit(tx3)
}

func TestMVCC_DeleteVisibleToOlderSnapshot(t *testing.T) {
	e, headPage := setupHeap(t)
	defer e.Close()

	// tx1: insert and commit
	tx1 := e.TxMgr.Begin()
	e.Insert(tx1, headPage, []tuple.Datum{tuple.DInt32(1)})
	e.TxMgr.Commit(tx1)

	// tx2: take snapshot (long-running reader)
	tx2 := e.TxMgr.Begin()
	snap2 := e.TxMgr.Snapshot(tx2)

	// tx3: delete and commit
	tx3 := e.TxMgr.Begin()
	// Find the tuple via scan
	var targetID slottedpage.ItemID
	snapFind := e.TxMgr.Snapshot(tx3)
	e.SeqScan(headPage, snapFind, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		targetID = id
		return false
	})
	e.Delete(tx3, targetID)
	e.TxMgr.Commit(tx3)

	// tx2's old snapshot should still see the tuple (snapshot isolation)
	count := 0
	e.SeqScan(headPage, snap2, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		count++
		return true
	})
	if count != 1 {
		t.Fatalf("old snapshot should still see deleted tuple, got %d", count)
	}
	e.TxMgr.Commit(tx2)
}

func TestMVCC_AbortedDeleteStillVisible(t *testing.T) {
	e, headPage := setupHeap(t)
	defer e.Close()

	tx1 := e.TxMgr.Begin()
	id, _ := e.Insert(tx1, headPage, []tuple.Datum{tuple.DInt32(1)})
	e.TxMgr.Commit(tx1)

	// tx2: delete, then abort
	tx2 := e.TxMgr.Begin()
	e.Delete(tx2, id)
	e.TxMgr.Abort(tx2)

	// Tuple should still be visible
	tx3 := e.TxMgr.Begin()
	snap := e.TxMgr.Snapshot(tx3)
	count := 0
	e.SeqScan(headPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		count++
		return true
	})
	if count != 1 {
		t.Fatalf("aborted delete should leave tuple visible, got %d", count)
	}
	e.TxMgr.Commit(tx3)
}

func TestMVCC_AbortedInsertInvisible(t *testing.T) {
	e, headPage := setupHeap(t)
	defer e.Close()

	tx1 := e.TxMgr.Begin()
	e.Insert(tx1, headPage, []tuple.Datum{tuple.DInt32(1)})
	e.TxMgr.Abort(tx1)

	tx2 := e.TxMgr.Begin()
	snap := e.TxMgr.Snapshot(tx2)
	count := 0
	e.SeqScan(headPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		count++
		return true
	})
	if count != 0 {
		t.Fatalf("aborted insert should be invisible, got %d", count)
	}
	e.TxMgr.Commit(tx2)
}

func TestMVCC_MultipleInsertsAndDeletes(t *testing.T) {
	e, headPage := setupHeap(t)
	defer e.Close()

	// Insert 10 rows
	tx1 := e.TxMgr.Begin()
	ids := make([]slottedpage.ItemID, 10)
	for i := 0; i < 10; i++ {
		id, err := e.Insert(tx1, headPage, []tuple.Datum{tuple.DInt32(int32(i))})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}
	e.TxMgr.Commit(tx1)

	// Delete even-numbered rows
	tx2 := e.TxMgr.Begin()
	for i := 0; i < 10; i += 2 {
		if err := e.Delete(tx2, ids[i]); err != nil {
			t.Fatal(err)
		}
	}
	e.TxMgr.Commit(tx2)

	// Scan: should see only odd rows
	tx3 := e.TxMgr.Begin()
	snap := e.TxMgr.Snapshot(tx3)
	var visible []int32
	e.SeqScan(headPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		visible = append(visible, tup.Columns[0].I32)
		return true
	})

	if len(visible) != 5 {
		t.Fatalf("expected 5 visible, got %d: %v", len(visible), visible)
	}
	for i, v := range visible {
		expected := int32(i*2 + 1)
		if v != expected {
			t.Fatalf("index %d: expected %d, got %d", i, expected, v)
		}
	}
	e.TxMgr.Commit(tx3)
}

func TestMVCC_ScanStopsEarly(t *testing.T) {
	e, headPage := setupHeap(t)
	defer e.Close()

	tx := e.TxMgr.Begin()
	for i := 0; i < 10; i++ {
		e.Insert(tx, headPage, []tuple.Datum{tuple.DInt32(int32(i))})
	}
	e.TxMgr.Commit(tx)

	tx2 := e.TxMgr.Begin()
	snap := e.TxMgr.Snapshot(tx2)
	count := 0
	e.SeqScan(headPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		count++
		return count < 3 // stop after 3
	})
	if count != 3 {
		t.Fatalf("expected scan to stop at 3, got %d", count)
	}
	e.TxMgr.Commit(tx2)
}

func TestMVCC_InsertAutoAllocPage(t *testing.T) {
	e, headPage := setupHeap(t)
	defer e.Close()

	tx := e.TxMgr.Begin()

	// Insert enough tuples to overflow one page
	count := 0
	for i := 0; i < 200; i++ {
		_, err := e.Insert(tx, headPage, []tuple.Datum{
			tuple.DInt32(int32(i)),
			tuple.DText("padding to fill pages faster and trigger overflow"),
		})
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
		count++
	}
	e.TxMgr.Commit(tx)

	// Scan all — should find all inserted tuples across pages
	tx2 := e.TxMgr.Begin()
	snap := e.TxMgr.Snapshot(tx2)
	scanCount := 0
	e.SeqScan(headPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		scanCount++
		return true
	})

	if scanCount != count {
		t.Fatalf("expected %d, got %d", count, scanCount)
	}
	e.TxMgr.Commit(tx2)
}

func TestMVCC_DeleteAlreadyDeleted(t *testing.T) {
	e, headPage := setupHeap(t)
	defer e.Close()

	tx1 := e.TxMgr.Begin()
	id, _ := e.Insert(tx1, headPage, []tuple.Datum{tuple.DInt32(1)})
	e.TxMgr.Commit(tx1)

	tx2 := e.TxMgr.Begin()
	e.Delete(tx2, id)
	e.TxMgr.Commit(tx2)

	// Second delete should fail
	tx3 := e.TxMgr.Begin()
	err := e.Delete(tx3, id)
	if err == nil {
		t.Fatal("expected error deleting already-deleted tuple")
	}
	e.TxMgr.Abort(tx3)
}

// --- Concurrency tests ---

func TestMVCC_ConcurrentInserts(t *testing.T) {
	e, headPage := setupHeap(t)
	defer e.Close()

	var wg sync.WaitGroup
	n := 20
	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			tx := e.TxMgr.Begin()
			_, err := e.Insert(tx, headPage, []tuple.Datum{tuple.DInt32(int32(val))})
			if err != nil {
				errs <- fmt.Errorf("insert %d: %w", val, err)
				e.TxMgr.Abort(tx)
				return
			}
			e.TxMgr.Commit(tx)
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}

	// Read all
	tx := e.TxMgr.Begin()
	snap := e.TxMgr.Snapshot(tx)
	count := 0
	e.SeqScan(headPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		count++
		return true
	})
	if count != n {
		t.Fatalf("expected %d tuples, got %d", n, count)
	}
	e.TxMgr.Commit(tx)
}

func TestMVCC_ConcurrentReadersAndWriter(t *testing.T) {
	e, headPage := setupHeap(t)
	defer e.Close()

	// Pre-insert 10 committed rows
	tx := e.TxMgr.Begin()
	for i := 0; i < 10; i++ {
		e.Insert(tx, headPage, []tuple.Datum{tuple.DInt32(int32(i))})
	}
	e.TxMgr.Commit(tx)

	var wg sync.WaitGroup
	errs := make(chan string, 100)

	// 10 concurrent readers — each should see exactly 10 rows
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rx := e.TxMgr.Begin()
			snap := e.TxMgr.Snapshot(rx)

			count := 0
			e.SeqScan(headPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
				count++
				return true
			})
			if count != 10 && count != 15 {
				errs <- fmt.Sprintf("reader expected 10 or 15, got %d", count)
			}
			e.TxMgr.Commit(rx)
		}()
	}

	// 1 concurrent writer inserting 5 more rows
	wg.Add(1)
	go func() {
		defer wg.Done()
		wx := e.TxMgr.Begin()
		for i := 10; i < 15; i++ {
			e.Insert(wx, headPage, []tuple.Datum{tuple.DInt32(int32(i))})
		}
		e.TxMgr.Commit(wx)
	}()

	wg.Wait()
	close(errs)

	for msg := range errs {
		t.Fatal(msg)
	}

	// After all concurrent activity, a new reader should see at least 10
	// (and up to 15 depending on timing).
	rx := e.TxMgr.Begin()
	snap := e.TxMgr.Snapshot(rx)
	count := 0
	e.SeqScan(headPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		count++
		return true
	})
	if count < 10 || count > 15 {
		t.Fatalf("final count should be 10-15, got %d", count)
	}
	e.TxMgr.Commit(rx)
}

func TestMVCC_SnapshotIsolation_ConcurrentDelete(t *testing.T) {
	e, headPage := setupHeap(t)
	defer e.Close()

	// Insert a row and commit
	tx1 := e.TxMgr.Begin()
	id, _ := e.Insert(tx1, headPage, []tuple.Datum{tuple.DInt32(42)})
	e.TxMgr.Commit(tx1)

	// tx2: long reader takes snapshot BEFORE the delete
	tx2 := e.TxMgr.Begin()
	snap2 := e.TxMgr.Snapshot(tx2)

	// tx3: deletes and commits
	tx3 := e.TxMgr.Begin()
	e.Delete(tx3, id)
	e.TxMgr.Commit(tx3)

	// tx2 should still see the row
	count := 0
	e.SeqScan(headPage, snap2, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		count++
		return true
	})
	if count != 1 {
		t.Fatalf("snapshot isolation broken: expected 1, got %d", count)
	}

	// New reader should NOT see it
	tx4 := e.TxMgr.Begin()
	snap4 := e.TxMgr.Snapshot(tx4)
	count = 0
	e.SeqScan(headPage, snap4, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		count++
		return true
	})
	if count != 0 {
		t.Fatalf("new reader should see 0 after delete, got %d", count)
	}

	e.TxMgr.Commit(tx2)
	e.TxMgr.Commit(tx4)
}

func TestMVCC_PersistAcrossRestart(t *testing.T) {
	dir := tempDir(t)
	path := dbPath(dir)

	e, err := Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}

	headPage, _ := e.AllocPage()
	pageBuf, _ := e.Pool.FetchPage(headPage)
	sp := slottedpage.Init(slottedpage.PageTypeHeap, headPage, 0)
	copy(pageBuf, sp.Bytes())
	e.Pool.MarkDirty(headPage)
	e.Pool.ReleasePage(headPage)

	tx := e.TxMgr.Begin()
	e.Insert(tx, headPage, []tuple.Datum{tuple.DInt32(999)})
	e.TxMgr.Commit(tx)
	e.Close()

	// Reopen
	e2, err := Open(path, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	// The TxManager should resume from where it left off
	tx2 := e2.TxMgr.Begin()
	snap := e2.TxMgr.Snapshot(tx2)

	count := 0
	e2.SeqScan(headPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if tup.Columns[0].I32 != 999 {
			t.Fatalf("expected 999, got %d", tup.Columns[0].I32)
		}
		count++
		return true
	})
	if count != 1 {
		t.Fatalf("expected 1 tuple after restart, got %d", count)
	}
	e2.TxMgr.Commit(tx2)
}
