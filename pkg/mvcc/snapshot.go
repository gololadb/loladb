package mvcc

// Snapshot is an immutable capture of transactional state at the time
// it was created. It is used to determine which tuples are visible to
// a particular transaction.
//
// Visibility rules (PostgreSQL-style):
//
//	A tuple (xmin, xmax) is visible to snapshot S if:
//	  1. xmin is committed AND xmin < S.xmax AND xmin is not in S.active
//	     (the creating transaction committed before this snapshot)
//	  2. AND one of:
//	     a. xmax == 0  (tuple has not been deleted)
//	     b. xmax is aborted
//	     c. xmax >= S.xmax  (deleting tx started after this snapshot)
//	     d. xmax is in S.active  (deleting tx was still running)
type Snapshot struct {
	xid    uint32   // the transaction that owns this snapshot
	xmin   uint32   // lowest active XID at snapshot time
	xmax   uint32   // next XID at snapshot time (upper bound)
	active []uint32 // XIDs that were active at snapshot time
	clog   *TxManager
}

// XID returns the transaction ID that owns this snapshot.
func (s *Snapshot) XID() uint32 { return s.xid }

// IsVisible determines whether a tuple with the given xmin and xmax
// is visible to this snapshot.
func (s *Snapshot) IsVisible(tupleXmin, tupleXmax uint32) bool {
	// --- Check xmin (inserting transaction) ---

	if tupleXmin == s.xid {
		// Our own transaction inserted this tuple — visible unless
		// we also deleted it.
		if tupleXmax == s.xid {
			return false // we deleted it ourselves
		}
		// We inserted it and haven't deleted it (or someone else
		// is trying to delete it, but from our perspective it's
		// still alive).
		if tupleXmax != 0 {
			// Someone else set xmax; only invisible if that tx committed
			// and is visible to us.
			return !s.xmaxCommittedAndVisible(tupleXmax)
		}
		return true
	}

	if !s.isCommitted(tupleXmin) {
		return false // inserting tx not committed → invisible
	}
	if tupleXmin >= s.xmax {
		return false // inserting tx started after our snapshot
	}
	if s.isInActive(tupleXmin) {
		return false // inserting tx was still running at snapshot time
	}

	// --- Check xmax (deleting transaction) ---

	if tupleXmax == 0 {
		return true // not deleted
	}

	if tupleXmax == s.xid {
		return false // we deleted it
	}

	if !s.isCommitted(tupleXmax) {
		return true // deleting tx not committed → still visible
	}
	if tupleXmax >= s.xmax {
		return true // deleting tx started after our snapshot
	}
	if s.isInActive(tupleXmax) {
		return true // deleting tx was still running at snapshot time
	}

	// Deleting tx committed before our snapshot → invisible
	return false
}

func (s *Snapshot) isCommitted(xid uint32) bool {
	return s.clog.State(xid) == TxCommitted
}

func (s *Snapshot) isInActive(xid uint32) bool {
	for _, a := range s.active {
		if a == xid {
			return true
		}
	}
	return false
}

func (s *Snapshot) xmaxCommittedAndVisible(xmax uint32) bool {
	if !s.isCommitted(xmax) {
		return false
	}
	if xmax >= s.xmax {
		return false
	}
	if s.isInActive(xmax) {
		return false
	}
	return true
}
