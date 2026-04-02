package catalog

import (
	"fmt"
	"strings"
	"sync"

	"github.com/gololadb/loladb/pkg/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
)

// FuncDef represents a stored function or procedure.
type FuncDef struct {
	OID        int32
	Name       string
	Language   string   // "plpgsql", "sql"
	Body       string   // function body text
	ReturnType string   // return type name (e.g., "trigger", "integer", "void")
	ParamNames []string // parameter names
	ParamTypes []string // parameter type names
	Replace    bool
}

// TriggerDef represents a trigger on a table.
type TriggerDef struct {
	OID      int32
	Name     string
	TableOID int32
	FuncOID  int32
	Timing   int    // BEFORE=2, AFTER=4, INSTEAD=8
	Events   int    // INSERT=16, DELETE=32, UPDATE=64, TRUNCATE=128
	ForEach  string // "ROW" or "STATEMENT"
}

// Trigger timing/event constants matching gopgsql parser.
const (
	TrigBefore   = 1 << 1
	TrigAfter    = 1 << 2
	TrigInstead  = 1 << 3
	TrigInsert   = 1 << 4
	TrigDelete   = 1 << 5
	TrigUpdate   = 1 << 6
	TrigTruncate = 1 << 7
)

// funcStore holds in-memory function definitions.
type funcStore struct {
	mu    sync.RWMutex
	byOID map[int32]*FuncDef
	byName map[string]*FuncDef // lowercase name → def
}

func newFuncStore() *funcStore {
	return &funcStore{
		byOID:  make(map[int32]*FuncDef),
		byName: make(map[string]*FuncDef),
	}
}

func (fs *funcStore) add(f *FuncDef) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.byOID[f.OID] = f
	fs.byName[strings.ToLower(f.Name)] = f
}

func (fs *funcStore) findByName(name string) *FuncDef {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.byName[strings.ToLower(name)]
}

func (fs *funcStore) findByOID(oid int32) *FuncDef {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.byOID[oid]
}

func (fs *funcStore) remove(name string) *FuncDef {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	key := strings.ToLower(name)
	f := fs.byName[key]
	if f != nil {
		delete(fs.byName, key)
		delete(fs.byOID, f.OID)
	}
	return f
}

// triggerStore holds in-memory trigger definitions.
type triggerStore struct {
	mu       sync.RWMutex
	byOID    map[int32]*TriggerDef
	byTable  map[int32][]*TriggerDef // tableOID → triggers
}

func newTriggerStore() *triggerStore {
	return &triggerStore{
		byOID:   make(map[int32]*TriggerDef),
		byTable: make(map[int32][]*TriggerDef),
	}
}

func (ts *triggerStore) add(t *TriggerDef) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.byOID[t.OID] = t
	ts.byTable[t.TableOID] = append(ts.byTable[t.TableOID], t)
}

func (ts *triggerStore) removeByName(name string, tableOID int32) *TriggerDef {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	nameLower := strings.ToLower(name)
	triggers := ts.byTable[tableOID]
	for i, t := range triggers {
		if strings.ToLower(t.Name) == nameLower {
			delete(ts.byOID, t.OID)
			ts.byTable[tableOID] = append(triggers[:i], triggers[i+1:]...)
			return t
		}
	}
	return nil
}

func (ts *triggerStore) forTable(tableOID int32) []*TriggerDef {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.byTable[tableOID]
}

// CreateFunction stores a new function definition.
func (c *Catalog) CreateFunction(f *FuncDef) error {
	// Check for existing function with same name.
	existing := c.Funcs.findByName(f.Name)
	if existing != nil {
		if !f.Replace {
			return fmt.Errorf("function %q already exists", f.Name)
		}
		// Replace: reuse OID, update definition.
		f.OID = existing.OID
		c.Funcs.add(f)
		return c.persistFunction(f)
	}

	oid := c.Eng.Super.NextOID
	c.Eng.Super.NextOID++
	f.OID = int32(oid)
	c.Funcs.add(f)
	return c.persistFunction(f)
}

// FindFunction looks up a function by name.
func (c *Catalog) FindFunction(name string) *FuncDef {
	return c.Funcs.findByName(name)
}

// FindFunctionByOID looks up a function by OID.
func (c *Catalog) FindFunctionByOID(oid int32) *FuncDef {
	return c.Funcs.findByOID(oid)
}

// CreateTrigger stores a new trigger definition.
func (c *Catalog) CreateTrigger(t *TriggerDef) error {
	oid := c.Eng.Super.NextOID
	c.Eng.Super.NextOID++
	t.OID = int32(oid)
	c.Triggers.add(t)
	return c.persistTrigger(t)
}

// DropFunction removes a function by name.
func (c *Catalog) DropFunction(name string, missingOk bool) error {
	f := c.Funcs.remove(name)
	if f == nil {
		if missingOk {
			return nil
		}
		return fmt.Errorf("function %q does not exist", name)
	}
	// Remove from pg_proc heap page by rewriting without the dropped function.
	return c.rewritePgProc()
}

// DropTrigger removes a trigger by name from a table.
func (c *Catalog) DropTrigger(trigName, tableName string, missingOk bool) error {
	rel, err := c.FindRelation(tableName)
	if err != nil || rel == nil {
		if missingOk {
			return nil
		}
		return fmt.Errorf("table %q does not exist", tableName)
	}
	t := c.Triggers.removeByName(trigName, rel.OID)
	if t == nil {
		if missingOk {
			return nil
		}
		return fmt.Errorf("trigger %q on table %q does not exist", trigName, tableName)
	}
	return c.rewritePgTrigger()
}

// rewritePgProc rewrites the pg_proc heap page from the in-memory store.
func (c *Catalog) rewritePgProc() error {
	pgProcPage := c.Eng.Super.PgProcPage
	if pgProcPage == 0 {
		return nil
	}
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	var ids []slottedpage.ItemID
	c.Eng.SeqScan(pgProcPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		ids = append(ids, id)
		return true
	})
	for _, id := range ids {
		c.Eng.Delete(xid, id)
	}
	c.Eng.TxMgr.Commit(xid)

	c.Funcs.mu.RLock()
	defer c.Funcs.mu.RUnlock()
	for _, f := range c.Funcs.byOID {
		if err := c.persistFunction(f); err != nil {
			return err
		}
	}
	return nil
}

// rewritePgTrigger rewrites the pg_trigger heap page from the in-memory store.
func (c *Catalog) rewritePgTrigger() error {
	pgTriggerPage := c.Eng.Super.PgTriggerPage
	if pgTriggerPage == 0 {
		return nil
	}
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	var ids []slottedpage.ItemID
	c.Eng.SeqScan(pgTriggerPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		ids = append(ids, id)
		return true
	})
	for _, id := range ids {
		c.Eng.Delete(xid, id)
	}
	c.Eng.TxMgr.Commit(xid)

	c.Triggers.mu.RLock()
	defer c.Triggers.mu.RUnlock()
	for _, t := range c.Triggers.byOID {
		if err := c.persistTrigger(t); err != nil {
			return err
		}
	}
	return nil
}

// GetTableTriggers returns all triggers for a table.
func (c *Catalog) GetTableTriggers(tableName string) []*TriggerDef {
	rel, err := c.FindRelation(tableName)
	if err != nil || rel == nil {
		return nil
	}
	return c.Triggers.forTable(rel.OID)
}

// persistFunction writes a function definition to the pg_proc catalog table.
func (c *Catalog) persistFunction(f *FuncDef) error {
	row := []tuple.Datum{
		tuple.DInt32(f.OID),
		tuple.DText(f.Name),
		tuple.DText(f.Language),
		tuple.DText(f.Body),
		tuple.DText(f.ReturnType),
		tuple.DText(strings.Join(f.ParamNames, ",")),
		tuple.DText(strings.Join(f.ParamTypes, ",")),
	}

	pgProcPage := c.Eng.Super.PgProcPage
	if pgProcPage == 0 {
		return fmt.Errorf("pg_proc table not initialized")
	}

	xid := c.Eng.TxMgr.Begin()
	_, err := c.Eng.Insert(xid, pgProcPage, row)
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return err
	}
	c.Eng.TxMgr.Commit(xid)
	return nil
}

// persistTrigger writes a trigger definition to the pg_trigger catalog table.
func (c *Catalog) persistTrigger(t *TriggerDef) error {
	row := []tuple.Datum{
		tuple.DInt32(t.OID),
		tuple.DText(t.Name),
		tuple.DInt32(t.TableOID),
		tuple.DInt32(t.FuncOID),
		tuple.DInt32(int32(t.Timing)),
		tuple.DInt32(int32(t.Events)),
		tuple.DText(t.ForEach),
	}

	pgTriggerPage := c.Eng.Super.PgTriggerPage
	if pgTriggerPage == 0 {
		return fmt.Errorf("pg_trigger table not initialized")
	}

	xid := c.Eng.TxMgr.Begin()
	_, err := c.Eng.Insert(xid, pgTriggerPage, row)
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return err
	}
	c.Eng.TxMgr.Commit(xid)
	return nil
}

// loadFunctions loads function definitions from pg_proc into memory.
func (c *Catalog) loadFunctions() error {
	pgProcPage := c.Eng.Super.PgProcPage
	if pgProcPage == 0 {
		return nil
	}

	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	return c.Eng.SeqScan(pgProcPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) < 7 {
			return true
		}
		f := &FuncDef{
			OID:        tup.Columns[0].I32,
			Name:       tup.Columns[1].Text,
			Language:   tup.Columns[2].Text,
			Body:       tup.Columns[3].Text,
			ReturnType: tup.Columns[4].Text,
		}
		if names := tup.Columns[5].Text; names != "" {
			f.ParamNames = strings.Split(names, ",")
		}
		if types := tup.Columns[6].Text; types != "" {
			f.ParamTypes = strings.Split(types, ",")
		}
		c.Funcs.add(f)
		return true
	})
}

// loadTriggers loads trigger definitions from pg_trigger into memory.
func (c *Catalog) loadTriggers() error {
	pgTriggerPage := c.Eng.Super.PgTriggerPage
	if pgTriggerPage == 0 {
		return nil
	}

	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	return c.Eng.SeqScan(pgTriggerPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) < 7 {
			return true
		}
		t := &TriggerDef{
			OID:      tup.Columns[0].I32,
			Name:     tup.Columns[1].Text,
			TableOID: tup.Columns[2].I32,
			FuncOID:  tup.Columns[3].I32,
			Timing:   int(tup.Columns[4].I32),
			Events:   int(tup.Columns[5].I32),
			ForEach:  tup.Columns[6].Text,
		}
		c.Triggers.add(t)
		return true
	})
}
