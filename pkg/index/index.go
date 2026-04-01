// Package index defines the generalized index access method interface,
// mirroring PostgreSQL's IndexAmRoutine (amapi.h) and index scan
// descriptors (genam.h / relscan.h).
//
// Each concrete index type (btree, hash, …) lives in its own subpackage
// and implements the IndexAM interface. The executor and catalog interact
// with indexes exclusively through this interface, enabling volcano-style
// iteration that is uniform across all access methods.
package index

import (
	"github.com/jespino/loladb/pkg/slottedpage"
	"github.com/jespino/loladb/pkg/tuple"
)

// -----------------------------------------------------------------------
// Strategy numbers — mirrors PostgreSQL's access/stratnum.h
// -----------------------------------------------------------------------

// Strategy identifies the comparison semantics of a scan key.
type Strategy uint16

const (
	// B-tree strategies (BTLessStrategyNumber … BTGreaterStrategyNumber).
	StrategyLess         Strategy = 1
	StrategyLessEqual    Strategy = 2
	StrategyEqual        Strategy = 3
	StrategyGreaterEqual Strategy = 4
	StrategyGreater      Strategy = 5
)

// -----------------------------------------------------------------------
// ScanKey — mirrors PostgreSQL's ScanKeyData (access/skey.h)
// -----------------------------------------------------------------------

// ScanKey describes a single search condition for an index scan.
// It identifies which indexed column to compare, the comparison
// strategy, and the constant value to compare against.
type ScanKey struct {
	// AttrNum is the 1-based index column number.
	AttrNum int32
	// Strategy is the comparison operator (=, <, >, ≤, ≥).
	Strategy Strategy
	// Value is the datum to compare against.
	Value tuple.Datum
}

// -----------------------------------------------------------------------
// ScanDirection — mirrors PostgreSQL's ScanDirection (access/sdir.h)
// -----------------------------------------------------------------------

// ScanDirection controls the traversal order of an index scan.
type ScanDirection int8

const (
	BackwardScan ScanDirection = -1
	NoMovement   ScanDirection = 0
	ForwardScan  ScanDirection = 1
)

// -----------------------------------------------------------------------
// IndexScan — volcano-style iterator (relscan.h IndexScanDescData)
// -----------------------------------------------------------------------

// IndexScan is a stateful, one-tuple-at-a-time iterator over index
// entries. It mirrors PostgreSQL's IndexScanDesc and the
// amgettuple / amrescan / amendscan function triple.
//
// Usage:
//
//	scan := am.BeginScan(rootPage, nkeys)
//	scan.Rescan(keys)
//	for {
//	    tid, ok, err := scan.Next(index.ForwardScan)
//	    if err != nil { … }
//	    if !ok { break }
//	    // use tid
//	}
//	scan.End()
type IndexScan interface {
	// Rescan (re)starts the scan with the given keys. Mirrors amrescan.
	// Calling Rescan resets any internal position so the scan starts
	// from the beginning with the new keys.
	Rescan(keys []ScanKey) error

	// Next returns the next matching heap TID. Returns ok=false when
	// the scan is exhausted. Mirrors amgettuple.
	Next(dir ScanDirection) (tid slottedpage.ItemID, ok bool, err error)

	// End releases all resources held by the scan. Mirrors amendscan.
	End()
}

// -----------------------------------------------------------------------
// PageAllocator — shared page I/O abstraction for all AMs
// -----------------------------------------------------------------------

// PageAllocator is the interface every index AM uses to allocate,
// read, and write pages through the buffer pool. It was previously
// defined in the btree package; it lives here so all AMs share it.
type PageAllocator interface {
	AllocPage() (uint32, error)
	FetchPage(pageNum uint32) ([]byte, error)
	ReleasePage(pageNum uint32)
	MarkDirty(pageNum uint32)
}

// -----------------------------------------------------------------------
// IndexAM — the access method interface (amapi.h IndexAmRoutine)
// -----------------------------------------------------------------------

// IndexAM is the Go equivalent of PostgreSQL's IndexAmRoutine.
// Each index type (btree, hash, …) provides an implementation.
type IndexAM interface {
	// -- Capability flags (mirrors amcanorder, amcanunique, …) ----------

	// CanOrder reports whether the AM supports ordered scans.
	CanOrder() bool
	// CanUnique reports whether the AM supports unique indexes.
	CanUnique() bool
	// CanBackward reports whether the AM supports backward scans.
	CanBackward() bool

	// -- Data manipulation (mirrors ambuild, aminsert) ------------------

	// Build bulk-loads the index from an iterator over (key, TID) pairs.
	// rootPage is the already-allocated and initialized root page.
	// Mirrors ambuild.
	Build(rootPage uint32, iter func(yield func(key tuple.Datum, tid slottedpage.ItemID) bool)) (newRoot uint32, err error)

	// Insert adds a single (key, TID) entry. Returns the (possibly
	// changed) root page number. Mirrors aminsert.
	Insert(rootPage uint32, key tuple.Datum, tid slottedpage.ItemID) (newRoot uint32, err error)

	// -- Scanning (mirrors ambeginscan) ---------------------------------

	// BeginScan creates a new scan on the index rooted at rootPage.
	// The returned IndexScan must have Rescan called before the first
	// Next. Mirrors ambeginscan.
	BeginScan(rootPage uint32) IndexScan

	// -- Page lifecycle -------------------------------------------------

	// InitRootPage allocates and initializes a fresh root page suitable
	// for this AM. Returns the page number.
	InitRootPage() (uint32, error)
}
