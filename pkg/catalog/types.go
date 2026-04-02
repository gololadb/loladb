package catalog

import (
	"fmt"
	"strings"
	"sync"

	"github.com/gololadb/loladb/pkg/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
)

// CustomType represents a user-defined type (domain or enum).
type CustomType struct {
	OID      int32
	Name     string
	TypType  string // "d" = domain, "e" = enum
	BaseType tuple.DatumType // for domains: the underlying type
	EnumVals []string        // for enums: allowed values
}

// typeStore holds in-memory custom type definitions.
type typeStore struct {
	mu     sync.RWMutex
	byOID  map[int32]*CustomType
	byName map[string]*CustomType // lowercase name → type
}

func newTypeStore() *typeStore {
	return &typeStore{
		byOID:  make(map[int32]*CustomType),
		byName: make(map[string]*CustomType),
	}
}

func (ts *typeStore) add(t *CustomType) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.byOID[t.OID] = t
	ts.byName[strings.ToLower(t.Name)] = t
}

func (ts *typeStore) findByName(name string) *CustomType {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.byName[strings.ToLower(name)]
}

// CreateDomain registers a new domain type backed by a base type.
func (c *Catalog) CreateDomain(name string, baseType tuple.DatumType) error {
	if c.Types.findByName(name) != nil {
		return fmt.Errorf("type %q already exists", name)
	}
	oid := int32(c.Eng.Super.NextOID)
	c.Eng.Super.NextOID++

	ct := &CustomType{
		OID:      oid,
		Name:     name,
		TypType:  "d",
		BaseType: baseType,
	}
	c.Types.add(ct)

	// Persist to pg_type.
	return c.persistCustomType(ct)
}

// CreateEnum registers a new enum type with the given values.
func (c *Catalog) CreateEnum(name string, vals []string) error {
	if c.Types.findByName(name) != nil {
		return fmt.Errorf("type %q already exists", name)
	}
	oid := int32(c.Eng.Super.NextOID)
	c.Eng.Super.NextOID++

	ct := &CustomType{
		OID:      oid,
		Name:     name,
		TypType:  "e",
		BaseType: tuple.TypeText, // enums stored as text
		EnumVals: vals,
	}
	c.Types.add(ct)

	return c.persistCustomType(ct)
}

// ResolveType looks up a custom type by name and returns its storage type.
// Returns (type, true) if found, or (0, false) if not a custom type.
func (c *Catalog) ResolveType(name string) (tuple.DatumType, bool) {
	ct := c.Types.findByName(name)
	if ct == nil {
		return 0, false
	}
	return ct.BaseType, true
}

// persistCustomType writes a custom type to the pg_type heap page.
func (c *Catalog) persistCustomType(ct *CustomType) error {
	typPage := c.pgTypePage()
	if typPage == 0 {
		return nil
	}
	row := []tuple.Datum{
		tuple.DInt32(ct.OID),
		tuple.DText(ct.Name),
		tuple.DInt32(OIDPgCatalog),
		tuple.DInt32(-1), // typlen: variable
		tuple.DText(ct.TypType),
		tuple.DInt32(datumTypeToPgTypeOID(ct.BaseType)),
	}
	xid := c.Eng.TxMgr.Begin()
	_, err := c.Eng.Insert(xid, typPage, row)
	c.Eng.TxMgr.Commit(xid)
	return err
}

// pgTypePage returns the heap page for pg_type by looking it up in pg_class.
func (c *Catalog) pgTypePage() uint32 {
	rel, err := c.FindRelation("pg_type")
	if err != nil || rel == nil {
		return 0
	}
	return uint32(rel.HeadPage)
}

// loadCustomTypes loads user-defined types from pg_type on startup.
func (c *Catalog) loadCustomTypes() {
	typPage := c.pgTypePage()
	if typPage == 0 {
		return
	}
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	c.Eng.SeqScan(typPage, snap, func(_ slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) < 5 {
			return true
		}
		typtype := tup.Columns[4].Text
		// Only load custom types (domains and enums), skip base types.
		if typtype != "d" && typtype != "e" {
			return true
		}
		ct := &CustomType{
			OID:     tup.Columns[0].I32,
			Name:    tup.Columns[1].Text,
			TypType: typtype,
		}
		if typtype == "d" && len(tup.Columns) > 5 {
			ct.BaseType = tuple.DatumType(pgTypeOIDToDatumType(tup.Columns[5].I32))
		} else {
			ct.BaseType = tuple.TypeText // enums stored as text
		}
		c.Types.add(ct)
		return true
	})
	c.Eng.TxMgr.Commit(xid)
}
