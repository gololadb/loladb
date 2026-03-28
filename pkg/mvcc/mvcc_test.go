package mvcc

import (
	"sync"
	"testing"
)

// --- TxManager tests ---

func TestBeginAndCommit(t *testing.T) {
	tm := NewTxManager(1)

	xid := tm.Begin()
	if xid != 1 {
		t.Fatalf("expected xid 1, got %d", xid)
	}
	if tm.ActiveCount() != 1 {
		t.Fatal("expected 1 active")
	}
	if tm.State(xid) != TxActive {
		t.Fatal("expected active state")
	}

	tm.Commit(xid)
	if tm.ActiveCount() != 0 {
		t.Fatal("expected 0 active")
	}
	if tm.State(xid) != TxCommitted {
		t.Fatal("expected committed state")
	}
}

func TestBeginAndAbort(t *testing.T) {
	tm := NewTxManager(1)

	xid := tm.Begin()
	tm.Abort(xid)
	if tm.State(xid) != TxAborted {
		t.Fatal("expected aborted state")
	}
	if tm.IsActive(xid) {
		t.Fatal("should not be active")
	}
}

func TestMultipleTransactions(t *testing.T) {
	tm := NewTxManager(1)

	x1 := tm.Begin()
	x2 := tm.Begin()
	x3 := tm.Begin()

	if tm.ActiveCount() != 3 {
		t.Fatalf("expected 3 active, got %d", tm.ActiveCount())
	}

	tm.Commit(x2)
	if tm.ActiveCount() != 2 {
		t.Fatal("expected 2 active")
	}
	if tm.IsActive(x2) {
		t.Fatal("x2 should not be active")
	}
	if !tm.IsActive(x1) || !tm.IsActive(x3) {
		t.Fatal("x1 and x3 should be active")
	}
}

func TestUnknownXIDAssumedCommitted(t *testing.T) {
	tm := NewTxManager(100)
	// XID 50 was never tracked — assumed committed
	if tm.State(50) != TxCommitted {
		t.Fatal("unknown XID should be assumed committed")
	}
}

func TestNextXID(t *testing.T) {
	tm := NewTxManager(10)
	if tm.NextXID() != 10 {
		t.Fatalf("expected 10, got %d", tm.NextXID())
	}
	tm.Begin()
	if tm.NextXID() != 11 {
		t.Fatalf("expected 11, got %d", tm.NextXID())
	}
}

// --- Snapshot visibility tests ---

func TestVisibility_BasicCommitted(t *testing.T) {
	tm := NewTxManager(1)

	// tx1 inserts a tuple and commits
	tx1 := tm.Begin()
	tm.Commit(tx1)

	// tx2 takes a snapshot — should see tx1's tuple
	tx2 := tm.Begin()
	snap := tm.Snapshot(tx2)

	if !snap.IsVisible(tx1, 0) {
		t.Fatal("committed tuple should be visible")
	}
}

func TestVisibility_UncommittedInserter(t *testing.T) {
	tm := NewTxManager(1)

	tx1 := tm.Begin()
	tx2 := tm.Begin()
	snap := tm.Snapshot(tx2)

	// tx1 hasn't committed — its tuple should not be visible to tx2
	if snap.IsVisible(tx1, 0) {
		t.Fatal("uncommitted tuple should not be visible")
	}
}

func TestVisibility_AbortedInserter(t *testing.T) {
	tm := NewTxManager(1)

	tx1 := tm.Begin()
	tm.Abort(tx1)

	tx2 := tm.Begin()
	snap := tm.Snapshot(tx2)

	if snap.IsVisible(tx1, 0) {
		t.Fatal("aborted tuple should not be visible")
	}
}

func TestVisibility_OwnInsert(t *testing.T) {
	tm := NewTxManager(1)

	tx1 := tm.Begin()
	snap := tm.Snapshot(tx1)

	// Own uncommitted insert should be visible
	if !snap.IsVisible(tx1, 0) {
		t.Fatal("own insert should be visible")
	}
}

func TestVisibility_OwnDelete(t *testing.T) {
	tm := NewTxManager(1)

	tx1 := tm.Begin()
	snap := tm.Snapshot(tx1)

	// Own insert + own delete → invisible
	if snap.IsVisible(tx1, tx1) {
		t.Fatal("own insert + own delete should be invisible")
	}
}

func TestVisibility_DeletedByCommitted(t *testing.T) {
	tm := NewTxManager(1)

	// tx1 inserts, commits
	tx1 := tm.Begin()
	tm.Commit(tx1)

	// tx2 deletes, commits
	tx2 := tm.Begin()
	tm.Commit(tx2)

	// tx3 takes snapshot — should NOT see the tuple (deleted by committed tx2)
	tx3 := tm.Begin()
	snap := tm.Snapshot(tx3)

	if snap.IsVisible(tx1, tx2) {
		t.Fatal("tuple deleted by committed tx should be invisible")
	}
}

func TestVisibility_DeletedByActive(t *testing.T) {
	tm := NewTxManager(1)

	// tx1 inserts, commits
	tx1 := tm.Begin()
	tm.Commit(tx1)

	// tx2 starts a delete (still active)
	tx2 := tm.Begin()

	// tx3 takes snapshot while tx2 is active — should still see the tuple
	tx3 := tm.Begin()
	snap := tm.Snapshot(tx3)

	if !snap.IsVisible(tx1, tx2) {
		t.Fatal("tuple deleted by active tx should still be visible")
	}
}

func TestVisibility_DeletedByAborted(t *testing.T) {
	tm := NewTxManager(1)

	tx1 := tm.Begin()
	tm.Commit(tx1)

	tx2 := tm.Begin()
	tm.Abort(tx2) // delete was aborted

	tx3 := tm.Begin()
	snap := tm.Snapshot(tx3)

	// Aborted delete — tuple should be visible
	if !snap.IsVisible(tx1, tx2) {
		t.Fatal("tuple with aborted delete should be visible")
	}
}

func TestVisibility_FutureInserter(t *testing.T) {
	tm := NewTxManager(1)

	tx1 := tm.Begin()
	snap := tm.Snapshot(tx1)

	// tx2 starts after the snapshot
	tx2 := tm.Begin()
	tm.Commit(tx2)

	// tx2's insert should not be visible to tx1's snapshot
	if snap.IsVisible(tx2, 0) {
		t.Fatal("future inserter should not be visible")
	}
}

func TestVisibility_FutureDeleter(t *testing.T) {
	tm := NewTxManager(1)

	// tx1 inserts, commits
	tx1 := tm.Begin()
	tm.Commit(tx1)

	// tx2 takes snapshot
	tx2 := tm.Begin()
	snap := tm.Snapshot(tx2)

	// tx3 deletes after the snapshot
	tx3 := tm.Begin()
	tm.Commit(tx3)

	// The tuple should still be visible (deleter is future to the snapshot)
	if !snap.IsVisible(tx1, tx3) {
		t.Fatal("tuple deleted by future tx should be visible")
	}
}

func TestVisibility_ActiveInserterAtSnapshotTime(t *testing.T) {
	tm := NewTxManager(1)

	tx1 := tm.Begin() // still active
	tx2 := tm.Begin()
	snap := tm.Snapshot(tx2) // tx1 is in the active list

	// Even if tx1 commits after snapshot creation, the snapshot
	// captured tx1 as active, so its tuple should not be visible.
	tm.Commit(tx1)

	if snap.IsVisible(tx1, 0) {
		t.Fatal("inserter active at snapshot time should not be visible, even if committed later")
	}
}

func TestVisibility_SnapshotIsolation(t *testing.T) {
	tm := NewTxManager(1)

	// tx1: insert row A, commit
	tx1 := tm.Begin()
	tm.Commit(tx1)

	// tx2: long-running reader
	tx2 := tm.Begin()
	snap2 := tm.Snapshot(tx2)

	// tx3: delete row A, commit
	tx3 := tm.Begin()
	tm.Commit(tx3)

	// tx2's snapshot should still see row A (snapshot isolation)
	if !snap2.IsVisible(tx1, tx3) {
		t.Fatal("snapshot isolation violated: reader should still see deleted row")
	}

	// New tx4 should NOT see row A (tx3 committed)
	tx4 := tm.Begin()
	snap4 := tm.Snapshot(tx4)
	if snap4.IsVisible(tx1, tx3) {
		t.Fatal("new snapshot should not see row deleted by committed tx")
	}

	tm.Commit(tx2)
	tm.Commit(tx4)
}

// --- Concurrency tests ---

func TestConcurrent_BeginCommit(t *testing.T) {
	tm := NewTxManager(1)
	var wg sync.WaitGroup
	n := 100

	xids := make([]uint32, n)
	var mu sync.Mutex

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			xid := tm.Begin()
			mu.Lock()
			xids[idx] = xid
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	if tm.ActiveCount() != n {
		t.Fatalf("expected %d active, got %d", n, tm.ActiveCount())
	}

	// All XIDs should be unique
	seen := make(map[uint32]bool)
	for _, xid := range xids {
		if seen[xid] {
			t.Fatalf("duplicate XID: %d", xid)
		}
		seen[xid] = true
	}

	// Commit all concurrently
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tm.Commit(xids[idx])
		}(i)
	}
	wg.Wait()

	if tm.ActiveCount() != 0 {
		t.Fatalf("expected 0 active, got %d", tm.ActiveCount())
	}
}

func TestConcurrent_SnapshotsDontInterfere(t *testing.T) {
	tm := NewTxManager(1)

	// Start several writer transactions
	writers := make([]uint32, 10)
	for i := range writers {
		writers[i] = tm.Begin()
	}

	// Take snapshots from multiple reader goroutines concurrently
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rx := tm.Begin()
			snap := tm.Snapshot(rx)
			// All writers should be in-progress and thus invisible
			for _, wx := range writers {
				if snap.IsVisible(wx, 0) {
					t.Errorf("active writer %d should not be visible", wx)
				}
			}
			tm.Commit(rx)
		}()
	}
	wg.Wait()

	for _, wx := range writers {
		tm.Commit(wx)
	}
}

func TestConcurrent_WriteReadIsolation(t *testing.T) {
	tm := NewTxManager(1)

	// tx1 inserts and commits
	tx1 := tm.Begin()
	tm.Commit(tx1)

	var wg sync.WaitGroup
	errs := make(chan string, 100)

	// Spawn readers that should all see tx1's tuple
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rx := tm.Begin()
			snap := tm.Snapshot(rx)
			if !snap.IsVisible(tx1, 0) {
				errs <- "committed tuple not visible"
			}
			tm.Commit(rx)
		}()
	}

	// Spawn a writer that deletes and commits
	wg.Add(1)
	delTx := tm.Begin()
	go func() {
		defer wg.Done()
		tm.Commit(delTx)
	}()

	wg.Wait()
	close(errs)

	for e := range errs {
		t.Fatal(e)
	}
}
