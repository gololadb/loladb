package catalog

import "sync"

// syscache is an in-memory cache for catalog lookups, mirroring
// PostgreSQL's catcache / syscache layer. It avoids repeated heap
// scans of pg_class and pg_attribute for the same relation.
//
// The cache is invalidated on any DDL operation (CreateTable,
// CreateIndex, CreateView, DropTable, etc.).
type syscache struct {
	mu       sync.RWMutex
	byName   map[string]*Relation // relname → Relation
	byOID    map[int32]*Relation  // OID → Relation
	columns  map[int32][]Column   // relOID → columns (sorted by attnum)
	valid    bool                 // false after invalidation
}

func newSyscache() *syscache {
	return &syscache{
		byName:  make(map[string]*Relation),
		byOID:   make(map[int32]*Relation),
		columns: make(map[int32][]Column),
	}
}

// invalidate clears the entire cache. Called after any DDL.
func (sc *syscache) invalidate() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.byName = make(map[string]*Relation)
	sc.byOID = make(map[int32]*Relation)
	sc.columns = make(map[int32][]Column)
	sc.valid = false
}

// lookupRelByName returns a cached Relation or nil.
func (sc *syscache) lookupRelByName(name string) (*Relation, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	r, ok := sc.byName[name]
	return r, ok
}

// lookupColumns returns cached columns or nil.
func (sc *syscache) lookupColumns(oid int32) ([]Column, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	cols, ok := sc.columns[oid]
	return cols, ok
}

// storeRel caches a relation by both name and OID.
func (sc *syscache) storeRel(r *Relation) {
	if r == nil {
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.byName[r.Name] = r
	sc.byOID[r.OID] = r
}

// storeColumns caches columns for a relation OID.
func (sc *syscache) storeColumns(oid int32, cols []Column) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.columns[oid] = cols
}
