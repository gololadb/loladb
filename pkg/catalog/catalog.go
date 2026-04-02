package catalog

import (
	"fmt"
	"sort"
	"strings"

	"github.com/gololadb/loladb/pkg/engine"
	"github.com/gololadb/loladb/pkg/index"
	"github.com/gololadb/loladb/pkg/index/brin"
	"github.com/gololadb/loladb/pkg/index/btree"
	"github.com/gololadb/loladb/pkg/index/gin"
	"github.com/gololadb/loladb/pkg/index/gist"
	"github.com/gololadb/loladb/pkg/index/hash"
	"github.com/gololadb/loladb/pkg/index/spgist"
	"github.com/gololadb/loladb/pkg/mvcc"
	"github.com/gololadb/loladb/pkg/slottedpage"
	"github.com/gololadb/loladb/pkg/toast"
	"github.com/gololadb/loladb/pkg/tuple"
)

// Legacy relation kind constants (integer). New code should use the
// string constants from oids.go (RelKindOrdinaryTable, etc.), but
// these are kept for backward compatibility during the transition.
const (
	RelKindTable = 0
	RelKindIndex = 1
	RelKindView  = 2
)

// ColumnDef describes a column in a table.
type ColumnDef struct {
	Name       string
	Type       tuple.DatumType
	TypeName   string // original SQL type name (for domain/enum validation)
	NotNull    bool   // column-level NOT NULL constraint
	DefaultExpr string // SQL text of DEFAULT expression (empty = no default)
}

// Relation holds metadata about a table or index (a row from pg_class).
type Relation struct {
	OID          int32
	Name         string
	Kind         int32
	Pages        int32
	HeadPage     int32
	OwnerOID     int32 // OID of the owning role (0 = no owner)
	NamespaceOID int32 // OID of the containing schema (pg_namespace)
}

// Column holds metadata about a column (a row from pg_attribute).
type Column struct {
	RelID       int32
	Name        string
	Num         int32  // 1-based
	Type        int32  // maps to tuple.DatumType
	TypeOID     int32  // raw pg_type OID (for custom type lookup)
	NotNull     bool   // attnotnull
	DefaultExpr string // SQL text of DEFAULT expression (empty = no default)
}

// IndexInfo holds metadata about an index (extra fields in the pg_class
// row for relkind=index).
type IndexInfo struct {
	Relation           // embedded: OID, Name, Kind, Pages, HeadPage (=root page)
	TableOID int32     // OID of the indexed table
	ColNum   int32     // 1-based column number being indexed
	Method   string    // access method: btree, hash, gin, gist, spgist, brin
}

// engineAllocator adapts the engine to the index.PageAllocator interface.
type engineAllocator struct {
	eng *engine.Engine
}

func (a *engineAllocator) AllocPage() (uint32, error)              { return a.eng.AllocPage() }
func (a *engineAllocator) FetchPage(pn uint32) ([]byte, error)     { return a.eng.Pool.FetchPage(pn) }
func (a *engineAllocator) ReleasePage(pn uint32)                   { a.eng.Pool.ReleasePage(pn) }
func (a *engineAllocator) MarkDirty(pn uint32)                     { a.eng.Pool.MarkDirty(pn) }

// Catalog provides DDL operations backed by pg_class and pg_attribute
// system tables that live in normal heap pages.
type Catalog struct {
	Eng        *engine.Engine
	alloc      *engineAllocator // shared page allocator
	IdxAMs     map[string]index.IndexAM // AM registry: method name → IndexAM
	Rules      *ruleStore       // in-memory rewrite rule storage (pg_rewrite)
	Policies   *policyStore     // in-memory RLS policy storage (pg_policy)
	ACLs       *aclStore        // in-memory object ACL cache
	Funcs      *funcStore       // in-memory function definitions (pg_proc)
	Triggers   *triggerStore    // in-memory trigger definitions (pg_trigger)
	Types      *typeStore       // in-memory custom type definitions (domains, enums)
	cache      *syscache        // catalog lookup cache
	SearchPath []string         // schema search path (default: ["public"])
}

// New wraps an engine with catalog operations. If the database is
// freshly created (PgClassPage == 0), it bootstraps the system tables.
func New(eng *engine.Engine) (*Catalog, error) {
	alloc := &engineAllocator{eng: eng}
	ams := map[string]index.IndexAM{
		"btree":  btree.NewAM(alloc),
		"hash":   hash.NewAM(alloc),
		"brin":   brin.NewAM(alloc),
		"gin":    gin.NewAM(alloc),
		"gist":   gist.NewAM(alloc),
		"spgist": spgist.NewAM(alloc),
	}
	// Load search path from superblock, or default to ["public"].
	searchPath := []string{"public"}
	if eng.Super.SearchPath != "" {
		searchPath = strings.Split(eng.Super.SearchPath, ",")
	}
	c := &Catalog{
		Eng: eng, alloc: alloc, IdxAMs: ams,
		Rules: newRuleStore(), Policies: newPolicyStore(), ACLs: newACLStore(),
		Funcs: newFuncStore(), Triggers: newTriggerStore(), Types: newTypeStore(),
		cache:      newSyscache(),
		SearchPath: searchPath,
	}

	if eng.Super.PgClassPage == 0 {
		// Fresh database — bootstrap all catalog tables.
		if err := c.bootstrap(); err != nil {
			return nil, fmt.Errorf("catalog: bootstrap: %w", err)
		}
	}

	// Load persisted data into in-memory stores.
	if err := c.loadACLs(); err != nil {
		return nil, fmt.Errorf("catalog: load acls: %w", err)
	}
	if err := c.loadRules(); err != nil {
		return nil, fmt.Errorf("catalog: load rules: %w", err)
	}
	if err := c.loadPolicies(); err != nil {
		return nil, fmt.Errorf("catalog: load policies: %w", err)
	}
	if err := c.loadFunctions(); err != nil {
		return nil, fmt.Errorf("catalog: load functions: %w", err)
	}
	if err := c.loadTriggers(); err != nil {
		return nil, fmt.Errorf("catalog: load triggers: %w", err)
	}
	c.loadCustomTypes()

	return c, nil
}

// getAM returns the IndexAM for the given method name. Falls back to btree.
func (c *Catalog) getAM(method string) index.IndexAM {
	if method == "" {
		method = "btree"
	}
	if am, ok := c.IdxAMs[method]; ok {
		return am
	}
	return c.IdxAMs["btree"]
}

// bootstrap is defined in bootstrap.go — it creates all catalog tables
// with self-describing rows.

// CreateTable creates a new table with the given name and columns.
// It allocates a heap page for the table, inserts metadata into
// pg_class and pg_attribute, and returns the OID.
// ownerOID is the OID of the role that owns this table (0 = no owner).
func (c *Catalog) CreateTable(name string, cols []ColumnDef) (int32, error) {
	return c.CreateTableOwned(name, cols, 0)
}

// CreateTableOwned creates a new table with an explicit owner.
func (c *Catalog) CreateTableOwned(name string, cols []ColumnDef, ownerOID int32) (int32, error) {
	return c.CreateTableInSchema(name, cols, ownerOID, "")
}

// CreateTableInSchema creates a new table in the specified schema (or current schema if empty).
func (c *Catalog) CreateTableInSchema(name string, cols []ColumnDef, ownerOID int32, schemaName string) (int32, error) {
	nsOID := c.CurrentSchemaOID()
	if schemaName != "" {
		nsOID = c.SchemaOID(schemaName)
		if nsOID == 0 {
			return 0, fmt.Errorf("schema %q does not exist", schemaName)
		}
	}

	// Check for duplicate name in the target schema.
	existing, _ := c.findRelationInNamespace(name, nsOID)
	if existing != nil {
		return 0, fmt.Errorf("catalog: table %q already exists", name)
	}

	// Allocate a heap page for the new table.
	headPage, err := c.Eng.AllocPage()
	if err != nil {
		return 0, fmt.Errorf("catalog: alloc table page: %w", err)
	}

	// Init the heap page.
	buf, err := c.Eng.Pool.FetchPage(headPage)
	if err != nil {
		return 0, err
	}
	sp := slottedpage.Init(slottedpage.PageTypeHeap, headPage, 0)
	copy(buf, sp.Bytes())
	c.Eng.Pool.MarkDirty(headPage)
	c.Eng.Pool.ReleasePage(headPage)

	oid := int32(c.Eng.Super.AllocOID())

	// Begin a transaction for the catalog writes.
	xid := c.Eng.TxMgr.Begin()

	// Insert into pg_class (new 12-column format).
	_, err = c.Eng.Insert(xid, c.Eng.Super.PgClassPage, pgClassRow(
		oid, name, nsOID, RelKindOrdinaryTable_S,
		1, 0, 0, ownerOID, "heap", int32(headPage), 0, 0,
	))
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return 0, fmt.Errorf("catalog: insert pg_class: %w", err)
	}

	// Insert into pg_attribute (new 8-column format).
	for i, col := range cols {
		typeOID := datumTypeToPgTypeOID(col.Type)
		// If the column uses a custom type (domain/enum), store its OID instead.
		if col.TypeName != "" {
			if ct := c.Types.findByName(col.TypeName); ct != nil {
				typeOID = ct.OID
			}
		}
		_, err = c.Eng.Insert(xid, c.Eng.Super.PgAttrPage, pgAttributeRow(
			oid, col.Name, typeOID, -1, int16(i+1), col.NotNull, col.DefaultExpr,
		))
		if err != nil {
			c.Eng.TxMgr.Abort(xid)
			return 0, fmt.Errorf("catalog: insert pg_attribute col %q: %w", col.Name, err)
		}
	}

	c.Eng.TxMgr.Commit(xid)
	c.cache.invalidate()
	return oid, nil
}

// CreateView registers a view in the catalog. A view is a relation with
// RelKindView and an associated _RETURN rewrite rule that holds the
// defining SELECT query. This mirrors PostgreSQL's DefineView() which
// creates a pg_class entry + a pg_rewrite _RETURN rule.
func (c *Catalog) CreateView(name string, cols []ColumnDef, definition string) (int32, error) {
	nsOID := c.CurrentSchemaOID()
	existing, _ := c.findRelationInNamespace(name, nsOID)
	if existing != nil {
		return 0, fmt.Errorf("catalog: relation %q already exists", name)
	}

	oid := int32(c.Eng.Super.AllocOID())

	xid := c.Eng.TxMgr.Begin()

	// Insert into pg_class with view relkind and HeadPage=0 (no storage).
	_, err := c.Eng.Insert(xid, c.Eng.Super.PgClassPage, pgClassRow(
		oid, name, nsOID, RelKindView_S,
		0, 0, 0, 0, "", 0, 0, 0,
	))
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return 0, fmt.Errorf("catalog: insert pg_class for view: %w", err)
	}

	// Insert columns into pg_attribute (new 8-column format).
	for i, col := range cols {
		_, err = c.Eng.Insert(xid, c.Eng.Super.PgAttrPage, pgAttributeRow(
			oid, col.Name, datumTypeToPgTypeOID(col.Type), -1, int16(i+1), false, "",
		))
		if err != nil {
			c.Eng.TxMgr.Abort(xid)
			return 0, fmt.Errorf("catalog: insert pg_attribute for view col %q: %w", col.Name, err)
		}
	}

	c.Eng.TxMgr.Commit(xid)

	// Persist the _RETURN rule to pg_rewrite.
	rule := &RewriteRule{
		Name:       "_RETURN",
		RelOID:     oid,
		Event:      RuleEventSelect,
		Action:     RuleActionInstead,
		Definition: definition,
		Enabled:    true,
	}
	if err := c.persistRule(rule); err != nil {
		return 0, fmt.Errorf("catalog: persist rule for view: %w", err)
	}

	// Also register in the in-memory store.
	c.Rules.AddRule(rule)

	c.cache.invalidate()
	return oid, nil
}

// persistRule writes a rewrite rule to the pg_rewrite heap page.
// Tuple format: (relOID int32, name text, event int32, action int32, definition text)
func (c *Catalog) persistRule(rule *RewriteRule) error {
	xid := c.Eng.TxMgr.Begin()
	_, err := c.Eng.Insert(xid, c.Eng.Super.PgRewritePage, []tuple.Datum{
		tuple.DInt32(rule.RelOID),
		tuple.DText(rule.Name),
		tuple.DInt32(int32(rule.Event)),
		tuple.DInt32(int32(rule.Action)),
		tuple.DText(rule.Definition),
	})
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return err
	}
	c.Eng.TxMgr.Commit(xid)
	return nil
}

// loadRules reads all rules from pg_rewrite into the in-memory store.
func (c *Catalog) loadRules() error {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	return c.Eng.SeqScan(c.Eng.Super.PgRewritePage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) < 5 {
			return true
		}
		rule := &RewriteRule{
			RelOID:     tup.Columns[0].I32,
			Name:       tup.Columns[1].Text,
			Event:      RuleEvent(tup.Columns[2].I32),
			Action:     RuleAction(tup.Columns[3].I32),
			Definition: tup.Columns[4].Text,
			Enabled:    true,
		}
		c.Rules.AddRule(rule)
		return true
	})
}

// CreatePolicy persists an RLS policy to pg_policy and registers it
// in the in-memory store.
// Tuple format: (relOID int32, name text, cmd int32, permissive int32,
//
//	roles text, usingExpr text, checkExpr text)
func (c *Catalog) CreatePolicy(policy *RLSPolicy) error {
	if err := c.persistPolicy(policy); err != nil {
		return err
	}
	c.Policies.AddPolicy(policy)
	return nil
}

func (c *Catalog) persistPolicy(policy *RLSPolicy) error {
	xid := c.Eng.TxMgr.Begin()

	permissive := int32(0)
	if policy.Permissive {
		permissive = 1
	}

	rolesStr := ""
	if len(policy.Roles) > 0 {
		for i, r := range policy.Roles {
			if i > 0 {
				rolesStr += ","
			}
			rolesStr += r
		}
	}

	_, err := c.Eng.Insert(xid, c.Eng.Super.PgPolicyPage, []tuple.Datum{
		tuple.DInt32(policy.RelOID),
		tuple.DText(policy.Name),
		tuple.DInt32(int32(policy.Cmd)),
		tuple.DInt32(permissive),
		tuple.DText(rolesStr),
		tuple.DText(policy.UsingExpr),
		tuple.DText(policy.CheckExpr),
	})
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return fmt.Errorf("catalog: persist policy: %w", err)
	}
	c.Eng.TxMgr.Commit(xid)
	return nil
}

func (c *Catalog) loadPolicies() error {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	return c.Eng.SeqScan(c.Eng.Super.PgPolicyPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) < 7 {
			return true
		}

		name := tup.Columns[1].Text

		// Handle RLS enabled flag sentinel rows.
		if name == "_RLS_ENABLED" {
			relOID := tup.Columns[0].I32
			if tup.Columns[2].I32 != 0 {
				c.Policies.EnableRLS(relOID)
			}
			return true
		}

		var roles []string
		rolesStr := tup.Columns[4].Text
		if rolesStr != "" {
			for _, r := range splitComma(rolesStr) {
				if r != "" {
					roles = append(roles, r)
				}
			}
		}

		policy := &RLSPolicy{
			RelOID:     tup.Columns[0].I32,
			Name:       name,
			Cmd:        PolicyCmd(tup.Columns[2].I32),
			Permissive: tup.Columns[3].I32 != 0,
			Roles:      roles,
			UsingExpr:  tup.Columns[5].Text,
			CheckExpr:  tup.Columns[6].Text,
		}
		c.Policies.AddPolicy(policy)
		return true
	})
}

func splitComma(s string) []string {
	var result []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			result = append(result, s[start:i])
			start = i + 1
		}
	}
	return result
}

// EnableRLS enables row-level security on a relation and persists the flag.
func (c *Catalog) EnableRLS(relOID int32) error {
	c.Policies.EnableRLS(relOID)
	return c.persistRLSFlag(relOID, true)
}

// DisableRLS disables row-level security on a relation and persists the flag.
func (c *Catalog) DisableRLS(relOID int32) error {
	c.Policies.DisableRLS(relOID)
	return c.persistRLSFlag(relOID, false)
}

func (c *Catalog) persistRLSFlag(relOID int32, enabled bool) error {
	xid := c.Eng.TxMgr.Begin()
	val := int32(0)
	if enabled {
		val = 1
	}
	_, err := c.Eng.Insert(xid, c.Eng.Super.PgPolicyPage, []tuple.Datum{
		tuple.DInt32(relOID),
		tuple.DText("_RLS_ENABLED"),
		tuple.DInt32(val), // cmd field reused as enabled flag
		tuple.DInt32(0),
		tuple.DText(""),
		tuple.DText(""),
		tuple.DText(""),
	})
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return err
	}
	c.Eng.TxMgr.Commit(xid)
	return nil
}

// IsRLSEnabled returns true if RLS is enabled for the relation.
func (c *Catalog) IsRLSEnabled(relOID int32) bool {
	return c.Policies.IsRLSEnabled(relOID)
}

// GetPoliciesForCmd returns applicable policies for a relation, command, and role.
func (c *Catalog) GetPoliciesForCmd(relOID int32, cmd PolicyCmd, role string) (permissive, restrictive []*RLSPolicy) {
	return c.Policies.GetPoliciesForCmd(relOID, cmd, role)
}

// tupleToRelation extracts a Relation from a pg_class tuple.
//
// New 12-column format (from bootstrap.go):
//
//	0:oid, 1:relname, 2:relnamespace, 3:relkind, 4:relpages,
//	5:reltuples, 6:relhasindex, 7:relowner, 8:relam, 9:relheadpage,
//	10:relindexoid, 11:relindexcol
//
// Legacy 6-column format (pre-refactor):
//
//	0:oid, 1:relname, 2:relkind, 3:relpages, 4:relheadpage, 5:relowner
// tupleToRelation extracts a Relation from a pg_class tuple.
//
// 12-column format:
//
//	0:oid, 1:relname, 2:relnamespace, 3:relkind(text), 4:relpages,
//	5:reltuples, 6:relhasindex, 7:relowner, 8:relam, 9:relheadpage,
//	10:relindexoid, 11:relindexcol
func tupleToRelation(tup *tuple.Tuple) *Relation {
	if len(tup.Columns) < 12 {
		return nil
	}
	return &Relation{
		OID:          tup.Columns[0].I32,
		Name:         tup.Columns[1].Text,
		Kind:         relKindStringToInt(tup.Columns[3].Text),
		Pages:        tup.Columns[4].I32,
		HeadPage:     tup.Columns[9].I32,
		OwnerOID:     tup.Columns[7].I32,
		NamespaceOID: tup.Columns[2].I32,
	}
}

// pgTypeOIDToDatumType maps a PostgreSQL type OID (from pg_type) to
// our internal tuple.DatumType.
func pgTypeOIDToDatumType(oid int32) int32 {
	switch oid {
	case OIDBool:
		return int32(tuple.TypeBool)
	case OIDInt2, OIDInt4, OIDOid:
		return int32(tuple.TypeInt32)
	case OIDInt8:
		return int32(tuple.TypeInt64)
	case OIDFloat8:
		return int32(tuple.TypeFloat64)
	case OIDText, OIDName, OIDChar:
		return int32(tuple.TypeText)
	default:
		return int32(tuple.TypeText) // fallback
	}
}

// datumTypeToPgTypeOID maps our internal DatumType to a PostgreSQL type OID.
func datumTypeToPgTypeOID(dt tuple.DatumType) int32 {
	switch dt {
	case tuple.TypeBool:
		return OIDBool
	case tuple.TypeInt32:
		return OIDInt4
	case tuple.TypeInt64:
		return OIDInt8
	case tuple.TypeFloat64:
		return OIDFloat8
	case tuple.TypeText:
		return OIDText
	default:
		return OIDText
	}
}

// relKindStringToInt converts the new string relkind to the legacy int.
func relKindStringToInt(s string) int32 {
	switch s {
	case RelKindOrdinaryTable_S:
		return RelKindTable
	case RelKindIndex_S:
		return 1 // RelKindIndex (int)
	case RelKindView_S:
		return 2 // RelKindView (int)
	default:
		return RelKindTable
	}
}

// relKindIntToString converts the legacy int relkind to the new string.
func relKindIntToString(k int32) string {
	switch k {
	case RelKindTable:
		return RelKindOrdinaryTable_S
	case 1: // RelKindIndex (int)
		return RelKindIndex_S
	case 2: // RelKindView (int)
		return RelKindView_S
	default:
		return RelKindOrdinaryTable_S
	}
}

func (c *Catalog) FindRelation(name string) (*Relation, error) {
	// Handle schema-qualified names (schema.table).
	if parts := strings.SplitN(name, ".", 2); len(parts) == 2 {
		return c.FindRelationQualified(parts[0], parts[1])
	}

	// Check cache first.
	if r, ok := c.cache.lookupRelByName(name); ok {
		return r, nil
	}

	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	r, err := c.findRelationWithSnapshot(name, snap)
	if err == nil && r != nil {
		c.cache.storeRel(r)
	}
	return r, err
}

func (c *Catalog) findRelationWithSnapshot(name string, snap *mvcc.Snapshot) (*Relation, error) {
	// Collect all relations with this name, then pick the best match
	// using the search path (pg_catalog first, then SearchPath).
	var candidates []*Relation
	err := c.Eng.SeqScan(c.Eng.Super.PgClassPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		r := tupleToRelation(tup)
		if r != nil && r.Name == name {
			candidates = append(candidates, r)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	// Multiple matches: resolve using search path.
	searchOrder := append([]string{"pg_catalog"}, c.SearchPath...)
	for _, ns := range searchOrder {
		nsOID := c.SchemaOID(ns)
		if nsOID == 0 {
			continue
		}
		for _, r := range candidates {
			if r.NamespaceOID == nsOID {
				return r, nil
			}
		}
	}
	// Fallback: return the first candidate.
	return candidates[0], nil
}

// GetColumns returns the columns for a relation OID, ordered by attnum.
func (c *Catalog) GetColumns(oid int32) ([]Column, error) {
	// Check cache first.
	if cols, ok := c.cache.lookupColumns(oid); ok {
		return cols, nil
	}

	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	cols, err := c.getColumnsWithSnapshot(oid, snap)
	if err == nil {
		c.cache.storeColumns(oid, cols)
	}
	return cols, err
}

// getColumnsWithSnapshot reads pg_attribute for the given relation OID.
// 8-column format: attrelid, attname, atttypid, attlen, attnum,
// atttypmod, attnotnull, attisdropped
func (c *Catalog) getColumnsWithSnapshot(oid int32, snap *mvcc.Snapshot) ([]Column, error) {
	var cols []Column
	err := c.Eng.SeqScan(c.Eng.Super.PgAttrPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) < 8 || tup.Columns[0].I32 != oid {
			return true
		}
		rawOID := tup.Columns[2].I32
		dt := pgTypeOIDToDatumType(rawOID)
		// If the OID is a custom type, resolve to its base storage type.
		if ct := c.Types.findByOID(rawOID); ct != nil {
			dt = int32(ct.BaseType)
		}
		col := Column{
			RelID:   tup.Columns[0].I32,
			Name:    tup.Columns[1].Text,
			Num:     tup.Columns[4].I32,
			Type:    dt,
			TypeOID: rawOID,
			NotNull: tup.Columns[PgAttrAttnotnull].I32 != 0,
		}
		// Read default expression if present (10-column format).
		if len(tup.Columns) >= PgAttrNumCols && tup.Columns[PgAttrAtthasdef].I32 != 0 {
			col.DefaultExpr = tup.Columns[PgAttrAttdefault].Text
		}
		cols = append(cols, col)
		return true
	})
	if err != nil {
		return nil, err
	}

	// Sort by attnum (insertion order is generally correct, but be safe).
	for i := 0; i < len(cols); i++ {
		for j := i + 1; j < len(cols); j++ {
			if cols[j].Num < cols[i].Num {
				cols[i], cols[j] = cols[j], cols[i]
			}
		}
	}
	return cols, nil
}

// ListTables returns all user relations of kind table (excludes catalog tables).
func (c *Catalog) ListTables() ([]Relation, error) {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	var tables []Relation
	err := c.Eng.SeqScan(c.Eng.Super.PgClassPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		r := tupleToRelation(tup)
		if r != nil && r.Kind == RelKindTable && r.OID >= FirstNormalOID {
			tables = append(tables, *r)
		}
		return true
	})
	return tables, err
}

// ListAllRelations returns all relations including catalog tables.
func (c *Catalog) ListAllRelations() ([]Relation, error) {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	var rels []Relation
	err := c.Eng.SeqScan(c.Eng.Super.PgClassPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		r := tupleToRelation(tup)
		if r != nil {
			rels = append(rels, *r)
		}
		return true
	})
	return rels, err
}

// InsertInto inserts a row into the named table, validating that the
// number and types of values match the table's schema.
func (c *Catalog) InsertInto(tableName string, values []tuple.Datum) (slottedpage.ItemID, error) {
	rel, err := c.FindRelation(tableName)
	if err != nil {
		return slottedpage.ItemID{}, err
	}
	if rel == nil {
		return slottedpage.ItemID{}, fmt.Errorf("catalog: table %q not found", tableName)
	}

	cols, err := c.GetColumns(rel.OID)
	if err != nil {
		return slottedpage.ItemID{}, err
	}

	if len(values) != len(cols) {
		return slottedpage.ItemID{}, fmt.Errorf("catalog: table %q expects %d columns, got %d", tableName, len(cols), len(values))
	}

	for i, col := range cols {
		if values[i].Type == tuple.TypeNull {
			continue // nulls allowed for any type
		}
		if !typeCompatible(values[i].Type, tuple.DatumType(col.Type)) {
			return slottedpage.ItemID{}, fmt.Errorf("catalog: column %q expects type %d, got %d", col.Name, col.Type, values[i].Type)
		}
		// Coerce Int32 → Int64 if needed.
		if tuple.DatumType(col.Type) == tuple.TypeInt64 && values[i].Type == tuple.TypeInt32 {
			values[i] = tuple.DInt64(int64(values[i].I32))
		}
	}

	// TOAST: store oversized text values out-of-line.
	values, err = toast.ToastValues(c.alloc, values)
	if err != nil {
		return slottedpage.ItemID{}, fmt.Errorf("catalog: toast %q: %w", tableName, err)
	}

	xid := c.Eng.TxMgr.Begin()
	id, err := c.Eng.Insert(xid, uint32(rel.HeadPage), values)
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return slottedpage.ItemID{}, fmt.Errorf("catalog: insert into %q: %w", tableName, err)
	}
	c.Eng.TxMgr.Commit(xid)

	// Update relpages if a new page was allocated.
	c.updateRelPages(rel)

	// Update all indexes on this table.
	indexes, err := c.getIndexesForTable(rel.OID)
	if err != nil {
		return id, nil // non-fatal: data was inserted
	}
	for _, idx := range indexes {
		colIdx := idx.ColNum - 1 // 0-based
		if int(colIdx) >= len(values) {
			continue
		}
		am := c.getAM(idx.Method)
		newRoot, err := am.Insert(uint32(idx.HeadPage), values[colIdx], id)
		if err != nil {
			return id, nil // non-fatal
		}
		if newRoot != uint32(idx.HeadPage) {
			c.updateIndexRootPage(idx.OID, newRoot)
		}
	}

	return id, nil
}

// SeqScan performs a sequential scan of the named table, applying
// MVCC visibility. fn is called for each visible row.
func (c *Catalog) SeqScan(tableName string, fn func(id slottedpage.ItemID, tup *tuple.Tuple) bool) error {
	rel, err := c.FindRelation(tableName)
	if err != nil {
		return err
	}
	if rel == nil {
		return fmt.Errorf("catalog: table %q not found", tableName)
	}

	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	return c.Eng.SeqScan(uint32(rel.HeadPage), snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		// Detoast any toast pointers.
		detoasted, err := toast.DetoastValues(c.alloc, tup.Columns)
		if err == nil {
			tup.Columns = detoasted
		}
		return fn(id, tup)
	})
}

// Delete performs a soft-delete on a tuple in the named table.
func (c *Catalog) Delete(tableName string, id slottedpage.ItemID) error {
	rel, err := c.FindRelation(tableName)
	if err != nil {
		return err
	}
	if rel == nil {
		return fmt.Errorf("catalog: table %q not found", tableName)
	}

	xid := c.Eng.TxMgr.Begin()
	if err := c.Eng.Delete(xid, id); err != nil {
		c.Eng.TxMgr.Abort(xid)
		return err
	}
	c.Eng.TxMgr.Commit(xid)
	return nil
}

// CreateIndex creates a B+Tree index on the given column of the named
// table. It registers the index in pg_class and populates it with
// existing data.
func (c *Catalog) CreateIndex(indexName, tableName, colName, method string) (int32, error) {
	if method == "" {
		method = "btree"
	}
	am := c.getAM(method)

	// Check for duplicate index name.
	existing, err := c.FindRelation(indexName)
	if err != nil {
		return 0, err
	}
	if existing != nil {
		return 0, fmt.Errorf("catalog: relation %q already exists", indexName)
	}

	// Find the table.
	table, err := c.FindRelation(tableName)
	if err != nil {
		return 0, err
	}
	if table == nil {
		return 0, fmt.Errorf("catalog: table %q not found", tableName)
	}

	// Find the column.
	cols, err := c.GetColumns(table.OID)
	if err != nil {
		return 0, err
	}
	var colNum int32
	for _, col := range cols {
		if col.Name == colName {
			colNum = col.Num
			break
		}
	}
	if colNum == 0 {
		return 0, fmt.Errorf("catalog: column %q not found in table %q", colName, tableName)
	}

	// Allocate and initialize root page via the index AM.
	rootPage, err := am.InitRootPage()
	if err != nil {
		return 0, err
	}

	oid := int32(c.Eng.Super.AllocOID())

	// Insert into pg_class (new 12-column format for index).
	// Place the index in the same schema as the table.
	xid := c.Eng.TxMgr.Begin()
	_, err = c.Eng.Insert(xid, c.Eng.Super.PgClassPage, pgClassRow(
		oid, indexName, table.NamespaceOID, RelKindIndex_S,
		1, 0, 0, 0, method, int32(rootPage), table.OID, colNum,
	))
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return 0, fmt.Errorf("catalog: insert pg_class for index: %w", err)
	}
	c.Eng.TxMgr.Commit(xid)

	// Populate the index with existing rows via Build.
	colIdx := colNum - 1
	scanXid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(scanXid)

	newRoot, err := am.Build(rootPage, func(yield func(key tuple.Datum, tid slottedpage.ItemID) bool) {
		c.Eng.SeqScan(uint32(table.HeadPage), snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
			if int(colIdx) >= len(tup.Columns) {
				return true
			}
			return yield(tup.Columns[colIdx], id)
		})
	})
	c.Eng.TxMgr.Commit(scanXid)
	if err != nil {
		return 0, err
	}

	if newRoot != rootPage {
		c.updateIndexRootPage(oid, newRoot)
	}

	c.cache.invalidate()
	return oid, nil
}

// IndexScan searches the named index for the given key and returns
// the matching heap tuples.
func (c *Catalog) IndexScan(indexName string, key int64) ([]*tuple.Tuple, []slottedpage.ItemID, error) {
	idx, err := c.findIndex(indexName)
	if err != nil {
		return nil, nil, err
	}
	if idx == nil {
		return nil, nil, fmt.Errorf("catalog: index %q not found", indexName)
	}

	am := c.getAM(idx.Method)
	scan := am.BeginScan(uint32(idx.HeadPage))
	defer scan.End()
	scan.Rescan([]index.ScanKey{
		{AttrNum: 1, Strategy: index.StrategyEqual, Value: tuple.DInt64(key)},
	})

	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	return c.fetchHeapTuples(scan, snap)
}

// BitmapIndexScan scans the named index for the given key and returns
// matching TIDs sorted by (page, slot). The caller is responsible for
// fetching heap tuples in page order (bitmap heap scan).
func (c *Catalog) BitmapIndexScan(indexName string, key int64) ([]slottedpage.ItemID, error) {
	idx, err := c.findIndex(indexName)
	if err != nil {
		return nil, err
	}
	if idx == nil {
		return nil, fmt.Errorf("catalog: index %q not found", indexName)
	}

	am := c.getAM(idx.Method)
	scan := am.BeginScan(uint32(idx.HeadPage))
	defer scan.End()
	scan.Rescan([]index.ScanKey{
		{AttrNum: 1, Strategy: index.StrategyEqual, Value: tuple.DInt64(key)},
	})

	var tids []slottedpage.ItemID
	for {
		tid, ok, err := scan.Next(index.ForwardScan)
		if err != nil {
			return tids, err
		}
		if !ok {
			break
		}
		tids = append(tids, tid)
	}

	// Sort by page then slot for sequential I/O.
	sort.Slice(tids, func(i, j int) bool {
		if tids[i].Page != tids[j].Page {
			return tids[i].Page < tids[j].Page
		}
		return tids[i].Slot < tids[j].Slot
	})
	return tids, nil
}

// FetchHeapTuple fetches a single tuple by TID with MVCC visibility check.
func (c *Catalog) FetchHeapTuple(tid slottedpage.ItemID) (*tuple.Tuple, error) {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	pageBuf, err := c.Eng.Pool.FetchPage(tid.Page)
	if err != nil {
		return nil, err
	}
	sp, err := slottedpage.FromBytes(pageBuf)
	if err != nil {
		c.Eng.Pool.ReleasePage(tid.Page)
		return nil, err
	}
	raw, err := sp.GetTuple(tid.Slot)
	c.Eng.Pool.ReleasePage(tid.Page)
	if err != nil {
		return nil, err
	}
	tup, err := tuple.Decode(raw)
	if err != nil {
		return nil, err
	}
	if !snap.IsVisible(tup.Xmin, tup.Xmax) {
		return nil, nil // not visible
	}
	detoasted, derr := toast.DetoastValues(c.alloc, tup.Columns)
	if derr == nil {
		tup.Columns = detoasted
	}
	return tup, nil
}

// ListIndexesForTable returns all indexes for the given table OID.
func (c *Catalog) ListIndexesForTable(tableOID int32) ([]IndexInfo, error) {
	return c.getIndexesForTable(tableOID)
}

// ListAllIndexes returns all indexes across all tables.
func (c *Catalog) ListAllIndexes() ([]IndexInfo, error) {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	var indexes []IndexInfo
	c.Eng.SeqScan(c.Eng.Super.PgClassPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		idx := tupleToIndexInfo(tup)
		if idx != nil {
			indexes = append(indexes, *idx)
		}
		return true
	})
	return indexes, nil
}

// tupleToIndexInfo extracts an IndexInfo from a pg_class tuple for an index.
// 12-column format: relam at col 8, relindexoid (table OID) at col 10,
// relindexcol at col 11.
func tupleToIndexInfo(tup *tuple.Tuple) *IndexInfo {
	r := tupleToRelation(tup)
	if r == nil || r.Kind != RelKindIndex || len(tup.Columns) < 12 {
		return nil
	}
	method := tup.Columns[8].Text
	if method == "" {
		method = "btree"
	}
	return &IndexInfo{
		Relation: *r,
		TableOID: tup.Columns[10].I32,
		ColNum:   tup.Columns[11].I32,
		Method:   method,
	}
}

// getIndexesForTable returns all indexes for the given table OID.
func (c *Catalog) getIndexesForTable(tableOID int32) ([]IndexInfo, error) {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	var indexes []IndexInfo
	c.Eng.SeqScan(c.Eng.Super.PgClassPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		idx := tupleToIndexInfo(tup)
		if idx != nil && idx.TableOID == tableOID {
			indexes = append(indexes, *idx)
		}
		return true
	})
	return indexes, nil
}

// findIndex finds an index relation by name.
func (c *Catalog) findIndex(name string) (*IndexInfo, error) {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	var found *IndexInfo
	c.Eng.SeqScan(c.Eng.Super.PgClassPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		idx := tupleToIndexInfo(tup)
		if idx != nil && idx.Name == name {
			found = idx
			return false
		}
		return true
	})
	return found, nil
}

// updateIndexRootPage updates the HeadPage (root page) for an index
// in pg_class after a B+Tree root split.
func (c *Catalog) updateIndexRootPage(indexOID int32, newRoot uint32) {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)

	var targetID slottedpage.ItemID
	var targetTup *tuple.Tuple
	c.Eng.SeqScan(c.Eng.Super.PgClassPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		r := tupleToRelation(tup)
		if r != nil && r.OID == indexOID && r.Kind == RelKindIndex {
			targetID = id
			targetTup = tup
			return false
		}
		return true
	})

	if targetTup == nil {
		c.Eng.TxMgr.Abort(xid)
		return
	}

	// Delete old entry and reinsert with updated root page.
	c.Eng.Delete(xid, targetID)

	// Clone all columns, patch relheadpage (col 9).
	newCols := make([]tuple.Datum, len(targetTup.Columns))
	copy(newCols, targetTup.Columns)
	newCols[9] = tuple.DInt32(int32(newRoot))

	c.Eng.Insert(xid, c.Eng.Super.PgClassPage, newCols)
	c.Eng.TxMgr.Commit(xid)
}

// Update performs an UPDATE as delete + insert (new tuple version).
// It soft-deletes the tuple at the given ItemID and inserts a new
// tuple with the updated values. Returns the new ItemID.
func (c *Catalog) Update(tableName string, id slottedpage.ItemID, newValues []tuple.Datum) (slottedpage.ItemID, error) {
	rel, err := c.FindRelation(tableName)
	if err != nil {
		return slottedpage.ItemID{}, err
	}
	if rel == nil {
		return slottedpage.ItemID{}, fmt.Errorf("catalog: table %q not found", tableName)
	}

	cols, err := c.GetColumns(rel.OID)
	if err != nil {
		return slottedpage.ItemID{}, err
	}
	if len(newValues) != len(cols) {
		return slottedpage.ItemID{}, fmt.Errorf("catalog: table %q expects %d columns, got %d", tableName, len(cols), len(newValues))
	}
	for i, col := range cols {
		if newValues[i].Type == tuple.TypeNull {
			continue
		}
		if !typeCompatible(newValues[i].Type, tuple.DatumType(col.Type)) {
			return slottedpage.ItemID{}, fmt.Errorf("catalog: column %q expects type %d, got %d", col.Name, col.Type, newValues[i].Type)
		}
		if tuple.DatumType(col.Type) == tuple.TypeInt64 && newValues[i].Type == tuple.TypeInt32 {
			newValues[i] = tuple.DInt64(int64(newValues[i].I32))
		}
	}

	// Delete old version.
	xid := c.Eng.TxMgr.Begin()
	if err := c.Eng.Delete(xid, id); err != nil {
		c.Eng.TxMgr.Abort(xid)
		return slottedpage.ItemID{}, fmt.Errorf("catalog: update delete: %w", err)
	}

	// TOAST new values.
	newValues, err = toast.ToastValues(c.alloc, newValues)
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return slottedpage.ItemID{}, err
	}

	// Insert new version.
	newID, err := c.Eng.Insert(xid, uint32(rel.HeadPage), newValues)
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return slottedpage.ItemID{}, fmt.Errorf("catalog: update insert: %w", err)
	}
	c.Eng.TxMgr.Commit(xid)

	// Update indexes.
	indexes, _ := c.getIndexesForTable(rel.OID)
	for _, idx := range indexes {
		colIdx := idx.ColNum - 1
		if int(colIdx) >= len(newValues) {
			continue
		}
		am := c.getAM(idx.Method)
		newRoot, err := am.Insert(uint32(idx.HeadPage), newValues[colIdx], newID)
		if err != nil {
			continue
		}
		if newRoot != uint32(idx.HeadPage) {
			c.updateIndexRootPage(idx.OID, newRoot)
		}
	}

	return newID, nil
}

// RangeScan performs an index range scan for keys in [lo, hi] and
// returns the matching heap tuples with MVCC visibility applied.
// fetchHeapTuples drains an index scan, fetching and filtering heap
// tuples through MVCC visibility. Shared by IndexScan and RangeScan.
func (c *Catalog) fetchHeapTuples(scan index.IndexScan, snap *mvcc.Snapshot) ([]*tuple.Tuple, []slottedpage.ItemID, error) {
	var tuples []*tuple.Tuple
	var ids []slottedpage.ItemID
	for {
		tid, ok, err := scan.Next(index.ForwardScan)
		if err != nil {
			return tuples, ids, err
		}
		if !ok {
			break
		}
		pageBuf, err := c.Eng.Pool.FetchPage(tid.Page)
		if err != nil {
			continue
		}
		sp, err := slottedpage.FromBytes(pageBuf)
		if err != nil {
			c.Eng.Pool.ReleasePage(tid.Page)
			continue
		}
		raw, err := sp.GetTuple(tid.Slot)
		c.Eng.Pool.ReleasePage(tid.Page)
		if err != nil {
			continue
		}
		tup, err := tuple.Decode(raw)
		if err != nil {
			continue
		}
		if !snap.IsVisible(tup.Xmin, tup.Xmax) {
			continue
		}
		detoasted, derr := toast.DetoastValues(c.alloc, tup.Columns)
		if derr == nil {
			tup.Columns = detoasted
		}
		tuples = append(tuples, tup)
		ids = append(ids, tid)
	}
	return tuples, ids, nil
}

func (c *Catalog) RangeScan(indexName string, lo, hi int64) ([]*tuple.Tuple, []slottedpage.ItemID, error) {
	idx, err := c.findIndex(indexName)
	if err != nil {
		return nil, nil, err
	}
	if idx == nil {
		return nil, nil, fmt.Errorf("catalog: index %q not found", indexName)
	}

	am := c.getAM(idx.Method)
	scan := am.BeginScan(uint32(idx.HeadPage))
	defer scan.End()
	scan.Rescan([]index.ScanKey{
		{AttrNum: 1, Strategy: index.StrategyGreaterEqual, Value: tuple.DInt64(lo)},
		{AttrNum: 1, Strategy: index.StrategyLessEqual, Value: tuple.DInt64(hi)},
	})

	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	return c.fetchHeapTuples(scan, snap)
}

// TableStats holds basic statistics about a table.
type TableStats struct {
	RelPages   int32
	TupleCount int64
	DeadCount  int64
	// ColumnStats maps column name → per-column statistics.
	ColumnStats map[string]*ColumnStats
}

// ColumnStats holds per-column statistics, mirroring pg_statistic.
type ColumnStats struct {
	NDistinct float64 // number of distinct values (or -frac if negative)
	NullFrac  float64 // fraction of null values
	// MCV (Most Common Values) — the top-N most frequent values and
	// their frequencies, mirroring pg_statistic's stavalues/stanumbers.
	MCVals  []string  // string representation of each MCV
	MCFreqs []float64 // frequency (fraction) of each MCV
}

// Stats gathers basic statistics for the named table.
// IsView returns true if the relation is a view.
func (c *Catalog) IsView(relOID int32) bool {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	var isView bool
	_ = c.Eng.SeqScan(c.Eng.Super.PgClassPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		r := tupleToRelation(tup)
		if r != nil && r.OID == relOID {
			isView = r.Kind == RelKindView
			return false
		}
		return true
	})
	return isView
}

// AddRule registers a rewrite rule for a relation. This is the
// equivalent of INSERT INTO pg_rewrite.
func (c *Catalog) AddRule(rule *RewriteRule) {
	c.Rules.AddRule(rule)
}

// GetRulesForEvent returns all enabled rules for a relation and event.
func (c *Catalog) GetRulesForEvent(relOID int32, event RuleEvent) []*RewriteRule {
	return c.Rules.GetRulesForEvent(relOID, event)
}

func (c *Catalog) Stats(tableName string) (*TableStats, error) {
	rel, err := c.FindRelation(tableName)
	if err != nil {
		return nil, err
	}
	if rel == nil {
		return nil, fmt.Errorf("catalog: table %q not found", tableName)
	}

	// Get column names for per-column stats.
	cols, _ := c.GetColumns(rel.OID)
	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = col.Name
	}

	stats := &TableStats{
		RelPages:    rel.Pages,
		ColumnStats: make(map[string]*ColumnStats, len(colNames)),
	}

	// Track distinct values and frequencies per column.
	// For large tables this is approximate (capped sample), matching
	// PostgreSQL's ANALYZE which samples rather than scanning everything.
	const maxDistinctTrack = 10000
	distinctSets := make([]map[string]struct{}, len(colNames))
	freqMaps := make([]map[string]int64, len(colNames))
	nullCounts := make([]int64, len(colNames))
	for i := range distinctSets {
		distinctSets[i] = make(map[string]struct{})
		freqMaps[i] = make(map[string]int64)
	}

	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	cur := uint32(rel.HeadPage)
	for cur != 0 {
		pageBuf, err := c.Eng.Pool.FetchPage(cur)
		if err != nil {
			break
		}
		sp, err := slottedpage.FromBytes(pageBuf)
		if err != nil {
			c.Eng.Pool.ReleasePage(cur)
			break
		}
		numSlots := sp.NumSlots()
		for slot := uint16(0); slot < numSlots; slot++ {
			if !sp.SlotIsAlive(slot) {
				continue
			}
			raw, err := sp.GetTuple(slot)
			if err != nil {
				continue
			}
			tup, err := tuple.Decode(raw)
			if err != nil {
				continue
			}
			if snap.IsVisible(tup.Xmin, tup.Xmax) {
				stats.TupleCount++
				// Track per-column distinct values and frequencies.
				for i := 0; i < len(colNames) && i < len(tup.Columns); i++ {
					d := tup.Columns[i]
					if d.Type == tuple.TypeNull {
						nullCounts[i]++
						continue
					}
					key := datumKey(d)
					if len(distinctSets[i]) < maxDistinctTrack {
						distinctSets[i][key] = struct{}{}
					}
					freqMaps[i][key]++
				}
			} else if tup.Xmax != 0 {
				stats.DeadCount++
			}
		}
		next := sp.NextPage()
		c.Eng.Pool.ReleasePage(cur)
		cur = next
	}

	// Compute per-column stats including MCVs.
	const maxMCV = 100 // PostgreSQL default: 100
	for i, name := range colNames {
		cs := &ColumnStats{
			NDistinct: float64(len(distinctSets[i])),
		}
		if stats.TupleCount > 0 {
			cs.NullFrac = float64(nullCounts[i]) / float64(stats.TupleCount)
		}

		// Extract top-N most common values.
		type valFreq struct {
			val  string
			freq int64
		}
		freqs := make([]valFreq, 0, len(freqMaps[i]))
		for v, f := range freqMaps[i] {
			freqs = append(freqs, valFreq{v, f})
		}
		sort.Slice(freqs, func(a, b int) bool {
			return freqs[a].freq > freqs[b].freq
		})
		n := maxMCV
		if n > len(freqs) {
			n = len(freqs)
		}
		// Only include values that appear more than average frequency.
		// This avoids storing MCVs for uniform distributions.
		avgFreq := float64(stats.TupleCount-nullCounts[i]) / cs.NDistinct
		if cs.NDistinct == 0 {
			avgFreq = 0
		}
		for j := 0; j < n; j++ {
			if float64(freqs[j].freq) <= avgFreq {
				break
			}
			cs.MCVals = append(cs.MCVals, freqs[j].val)
			cs.MCFreqs = append(cs.MCFreqs, float64(freqs[j].freq)/float64(stats.TupleCount))
		}

		stats.ColumnStats[name] = cs
	}

	return stats, nil
}

// datumKey returns a string key for a datum value, used for distinct tracking.
func datumKey(d tuple.Datum) string {
	switch d.Type {
	case tuple.TypeInt32:
		return fmt.Sprintf("i32:%d", d.I32)
	case tuple.TypeInt64:
		return fmt.Sprintf("i64:%d", d.I64)
	case tuple.TypeText:
		return "t:" + d.Text
	case tuple.TypeBool:
		if d.Bool {
			return "b:t"
		}
		return "b:f"
	case tuple.TypeFloat64:
		return fmt.Sprintf("f:%g", d.F64)
	default:
		return "null"
	}
}

// Vacuum reclaims space from dead tuples in the named table.
func (c *Catalog) Vacuum(tableName string) (*engine.VacuumResult, error) {
	rel, err := c.FindRelation(tableName)
	if err != nil {
		return nil, err
	}
	if rel == nil {
		return nil, fmt.Errorf("catalog: table %q not found", tableName)
	}

	result, err := c.Eng.Vacuum(uint32(rel.HeadPage))
	if err != nil {
		return nil, err
	}

	// Update relpages after vacuum.
	c.updateRelPages(rel)

	return result, nil
}

// updateRelPages counts the actual heap pages for a relation and
// updates relpages in pg_class if it changed.
func (c *Catalog) updateRelPages(rel *Relation) {
	count, err := c.Eng.CountHeapPages(uint32(rel.HeadPage))
	if err != nil || count == rel.Pages {
		return
	}

	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)

	var targetID slottedpage.ItemID
	var targetTup *tuple.Tuple
	c.Eng.SeqScan(c.Eng.Super.PgClassPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) >= 5 && tup.Columns[0].I32 == rel.OID {
			targetID = id
			targetTup = tup
			return false
		}
		return true
	})

	if targetTup == nil {
		c.Eng.TxMgr.Abort(xid)
		return
	}

	// Delete old entry and reinsert with updated page count.
	c.Eng.Delete(xid, targetID)

	newCols := make([]tuple.Datum, len(targetTup.Columns))
	copy(newCols, targetTup.Columns)

	// Patch relpages (col 4).
	newCols[4] = tuple.DInt32(count)

	c.Eng.Insert(xid, c.Eng.Super.PgClassPage, newCols)
	c.Eng.TxMgr.Commit(xid)

	rel.Pages = count
}

// typeCompatible checks if a datum type is compatible with a column type.
func typeCompatible(got, want tuple.DatumType) bool {
	if got == want {
		return true
	}
	// Int32 and Int64 are interchangeable.
	if (got == tuple.TypeInt32 || got == tuple.TypeInt64) &&
		(want == tuple.TypeInt32 || want == tuple.TypeInt64) {
		return true
	}
	return false
}


