package mvcc

import "sync"

// TxState represents the state of a transaction in the commit log.
type TxState uint8

const (
	TxActive    TxState = 0
	TxCommitted TxState = 1
	TxAborted   TxState = 2
)

// TxManager assigns transaction IDs, tracks active transactions,
// generates snapshots, and maintains an in-memory commit log.
type TxManager struct {
	mu      sync.Mutex
	nextXID uint32
	active  map[uint32]struct{}  // set of currently active XIDs
	clog    map[uint32]TxState   // in-memory commit log
}

// NewTxManager creates a TxManager starting from the given next XID
// (typically loaded from the superblock).
func NewTxManager(nextXID uint32) *TxManager {
	if nextXID == 0 {
		nextXID = 1
	}
	return &TxManager{
		nextXID: nextXID,
		active:  make(map[uint32]struct{}),
		clog:    make(map[uint32]TxState),
	}
}

// Begin starts a new transaction and returns its XID.
func (tm *TxManager) Begin() uint32 {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	xid := tm.nextXID
	tm.nextXID++
	tm.active[xid] = struct{}{}
	tm.clog[xid] = TxActive
	return xid
}

// Commit marks a transaction as committed.
func (tm *TxManager) Commit(xid uint32) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	delete(tm.active, xid)
	tm.clog[xid] = TxCommitted
}

// Abort marks a transaction as aborted.
func (tm *TxManager) Abort(xid uint32) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	delete(tm.active, xid)
	tm.clog[xid] = TxAborted
}

// State returns the state of a transaction. Transactions not in the
// clog are assumed committed (for bootstrap / pre-existing tuples).
func (tm *TxManager) State(xid uint32) TxState {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if s, ok := tm.clog[xid]; ok {
		return s
	}
	// XIDs not tracked (e.g. from before the TxManager was created)
	// are assumed committed.
	return TxCommitted
}

// Snapshot creates an immutable snapshot of the current transactional
// state for use in visibility checks.
func (tm *TxManager) Snapshot(xid uint32) *Snapshot {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// xmin = lowest active XID (all XIDs below are committed or aborted)
	// xmax = nextXID (all XIDs >= xmax haven't started yet)
	xmin := tm.nextXID
	activeList := make([]uint32, 0, len(tm.active))
	for aid := range tm.active {
		activeList = append(activeList, aid)
		if aid < xmin {
			xmin = aid
		}
	}

	return &Snapshot{
		xid:    xid,
		xmin:   xmin,
		xmax:   tm.nextXID,
		active: activeList,
		clog:   tm,
	}
}

// NextXID returns the next XID that will be assigned.
func (tm *TxManager) NextXID() uint32 {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.nextXID
}

// IsActive returns true if the given XID is currently active.
func (tm *TxManager) IsActive(xid uint32) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	_, ok := tm.active[xid]
	return ok
}

// ActiveCount returns the number of currently active transactions.
func (tm *TxManager) ActiveCount() int {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return len(tm.active)
}

// OldestActiveXID returns the lowest XID among active transactions,
// or nextXID if there are no active transactions. Any committed
// transaction with XID < this value is guaranteed to not be visible
// to any current or future snapshot, making its effects safe to
// vacuum.
func (tm *TxManager) OldestActiveXID() uint32 {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	oldest := tm.nextXID
	for xid := range tm.active {
		if xid < oldest {
			oldest = xid
		}
	}
	return oldest
}
