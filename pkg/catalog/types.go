package catalog

import (
	"fmt"
	"strings"
	"sync"

	"github.com/gololadb/loladb/pkg/engine/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
)

// CustomType represents a user-defined type (domain or enum).
type CustomType struct {
	OID      int32
	Name     string
	TypType  string // "d" = domain, "e" = enum
	BaseType tuple.DatumType // for domains: the underlying type
	EnumVals []string        // for enums: allowed values
	NotNull  bool            // for domains: NOT NULL constraint
	CheckExpr string         // for domains: CHECK expression (raw SQL, uses VALUE keyword)
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

func (ts *typeStore) remove(name string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	key := strings.ToLower(name)
	ct := ts.byName[key]
	if ct != nil {
		delete(ts.byName, key)
		delete(ts.byOID, ct.OID)
	}
}

// RLock acquires a read lock on the type store.
func (ts *typeStore) RLock() { ts.mu.RLock() }

// RUnlock releases the read lock.
func (ts *typeStore) RUnlock() { ts.mu.RUnlock() }

// All returns all custom types. Caller must hold RLock.
func (ts *typeStore) All() []*CustomType {
	result := make([]*CustomType, 0, len(ts.byOID))
	for _, ct := range ts.byOID {
		result = append(result, ct)
	}
	return result
}

func (ts *typeStore) findByName(name string) *CustomType {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.byName[strings.ToLower(name)]
}

func (ts *typeStore) findByOID(oid int32) *CustomType {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.byOID[oid]
}

// CreateDomain registers a new domain type backed by a base type.
func (c *Catalog) CreateDomain(name string, baseType tuple.DatumType, notNull bool, checkExpr string) error {
	if c.Types.findByName(name) != nil {
		return fmt.Errorf("type %q already exists", name)
	}
	oid := int32(c.Eng.Super.NextOID)
	c.Eng.Super.NextOID++

	ct := &CustomType{
		OID:       oid,
		Name:      name,
		TypType:   "d",
		BaseType:  baseType,
		NotNull:   notNull,
		CheckExpr: checkExpr,
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

// DropType removes a custom type (domain or enum) by name.
func (c *Catalog) DropType(name string, missingOk bool) error {
	ct := c.Types.findByName(name)
	if ct == nil {
		if missingOk {
			return nil
		}
		return fmt.Errorf("type %q does not exist", name)
	}
	c.Types.remove(name)
	return c.deleteFromPgType(ct.OID)
}

// AlterEnumAddValue adds a new value to an existing enum type.
func (c *Catalog) AlterEnumAddValue(name, newVal string) error {
	ct := c.Types.findByName(name)
	if ct == nil {
		return fmt.Errorf("type %q does not exist", name)
	}
	if ct.TypType != "e" {
		return fmt.Errorf("%q is not an enum type", name)
	}
	for _, v := range ct.EnumVals {
		if v == newVal {
			return fmt.Errorf("enum label %q already exists", newVal)
		}
	}
	ct.EnumVals = append(ct.EnumVals, newVal)
	// Persist: delete old row and insert updated one.
	c.deleteFromPgType(ct.OID)
	return c.persistCustomType(ct)
}

// FindTypeByOID returns the custom type with the given OID, or nil.
func (c *Catalog) FindTypeByOID(oid int32) *CustomType {
	return c.Types.findByOID(oid)
}

// EnumValues returns the allowed values for an enum type, or nil if not an enum.
func (c *Catalog) EnumValues(name string) []string {
	ct := c.Types.findByName(name)
	if ct == nil || ct.TypType != "e" {
		return nil
	}
	return ct.EnumVals
}

// EnumOrdinal returns the 1-based ordinal position of a value in an enum.
// Returns 0 if the value is not found.
func (c *Catalog) EnumOrdinal(typeName, val string) int {
	ct := c.Types.findByName(typeName)
	if ct == nil || ct.TypType != "e" {
		return 0
	}
	for i, v := range ct.EnumVals {
		if v == val {
			return i + 1
		}
	}
	return 0
}

// DomainConstraints returns the NOT NULL and CHECK constraints for a domain.
// Returns (notNull, checkExpr, isDomain).
func (c *Catalog) DomainConstraints(name string) (bool, string, bool) {
	ct := c.Types.findByName(name)
	if ct == nil || ct.TypType != "d" {
		return false, "", false
	}
	return ct.NotNull, ct.CheckExpr, true
}

// ValidateDomainValue checks a value against a domain's constraints.
// sqlExec evaluates a SQL expression (e.g. "SELECT ...") and returns an error
// if the result is not true.
func (c *Catalog) ValidateDomainValue(domainName string, val tuple.Datum, sqlExec func(sql string) error) error {
	ct := c.Types.findByName(domainName)
	if ct == nil || ct.TypType != "d" {
		return nil // not a domain, no validation
	}
	if ct.NotNull && val.Type == tuple.TypeNull {
		return fmt.Errorf("domain %q does not allow null values", domainName)
	}
	if ct.CheckExpr != "" && val.Type != tuple.TypeNull {
		// Replace VALUE with the actual literal.
		literal := datumToSQL(val)
		expr := strings.ReplaceAll(ct.CheckExpr, "VALUE", literal)
		expr = strings.ReplaceAll(expr, "value", literal)
		sql := "SELECT " + expr
		if err := sqlExec(sql); err != nil {
			return fmt.Errorf("value for domain %q violates check constraint: %w", domainName, err)
		}
	}
	return nil
}

// datumToSQL converts a datum to a SQL literal string.
func datumToSQL(d tuple.Datum) string {
	switch d.Type {
	case tuple.TypeNull:
		return "NULL"
	case tuple.TypeBool:
		if d.I32 != 0 {
			return "true"
		}
		return "false"
	case tuple.TypeInt32:
		return fmt.Sprintf("%d", d.I32)
	case tuple.TypeInt64:
		return fmt.Sprintf("%d", d.I64)
	case tuple.TypeFloat64:
		return fmt.Sprintf("%g", d.F64)
	case tuple.TypeText:
		return "'" + strings.ReplaceAll(d.Text, "'", "''") + "'"
	default:
		return "'" + strings.ReplaceAll(d.Text, "'", "''") + "'"
	}
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

// deleteFromPgType removes a row from pg_type by OID.
func (c *Catalog) deleteFromPgType(oid int32) error {
	typPage := c.pgTypePage()
	if typPage == 0 {
		return nil
	}
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	var target *slottedpage.ItemID
	c.Eng.SeqScan(typPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) > 0 && tup.Columns[0].I32 == oid {
			target = &id
			return false
		}
		return true
	})
	if target != nil {
		c.Eng.Delete(xid, *target)
	}
	c.Eng.TxMgr.Commit(xid)
	return nil
}

// persistCustomType writes a custom type to the pg_type heap page.
func (c *Catalog) persistCustomType(ct *CustomType) error {
	typPage := c.pgTypePage()
	if typPage == 0 {
		return nil
	}
	var notNullInt int32
	if ct.NotNull {
		notNullInt = 1
	}
	row := []tuple.Datum{
		tuple.DInt32(ct.OID),
		tuple.DText(ct.Name),
		tuple.DInt32(OIDPgCatalog),
		tuple.DInt32(-1), // typlen: variable
		tuple.DText(ct.TypType),
		tuple.DInt32(datumTypeToPgTypeOID(ct.BaseType)),
		tuple.DInt32(notNullInt),
		tuple.DText(ct.CheckExpr),
		tuple.DText(strings.Join(ct.EnumVals, ",")),
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
		// Load domain constraints.
		if len(tup.Columns) > PgTypeTypnotnull {
			ct.NotNull = tup.Columns[PgTypeTypnotnull].I32 != 0
		}
		if len(tup.Columns) > PgTypeTypcheck {
			ct.CheckExpr = tup.Columns[PgTypeTypcheck].Text
		}
		// Load enum values.
		if len(tup.Columns) > PgTypeTypenumvals && tup.Columns[PgTypeTypenumvals].Text != "" {
			ct.EnumVals = strings.Split(tup.Columns[PgTypeTypenumvals].Text, ",")
		}
		c.Types.add(ct)
		return true
	})
	c.Eng.TxMgr.Commit(xid)
}
