package catalog

import (
	"fmt"

	"github.com/jespino/loladb/pkg/engine"
	"github.com/jespino/loladb/pkg/index"
	"github.com/jespino/loladb/pkg/index/brin"
	"github.com/jespino/loladb/pkg/index/btree"
	"github.com/jespino/loladb/pkg/index/gin"
	"github.com/jespino/loladb/pkg/index/gist"
	"github.com/jespino/loladb/pkg/index/hash"
	"github.com/jespino/loladb/pkg/index/spgist"
	"github.com/jespino/loladb/pkg/mvcc"
	"github.com/jespino/loladb/pkg/slottedpage"
	"github.com/jespino/loladb/pkg/toast"
	"github.com/jespino/loladb/pkg/tuple"
)

// Relation kinds.
const (
	RelKindTable = 0
	RelKindIndex = 1
	RelKindView  = 2
)

// ColumnDef describes a column in a table.
type ColumnDef struct {
	Name string
	Type tuple.DatumType
}

// Relation holds metadata about a table or index (a row from pg_class).
type Relation struct {
	OID      int32
	Name     string
	Kind     int32
	Pages    int32
	HeadPage int32
}

// Column holds metadata about a column (a row from pg_attribute).
type Column struct {
	RelID  int32
	Name   string
	Num    int32 // 1-based
	Type   int32 // maps to tuple.DatumType
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
	Eng      *engine.Engine
	alloc    *engineAllocator // shared page allocator
	IdxAMs   map[string]index.IndexAM // AM registry: method name → IndexAM
	Rules    *ruleStore       // in-memory rewrite rule storage (pg_rewrite)
	Policies *policyStore     // in-memory RLS policy storage (pg_policy)
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
	c := &Catalog{Eng: eng, alloc: alloc, IdxAMs: ams, Rules: newRuleStore(), Policies: newPolicyStore()}

	if eng.Super.PgClassPage == 0 {
		if err := c.bootstrap(); err != nil {
			return nil, fmt.Errorf("catalog: bootstrap: %w", err)
		}
	}

	// Load persisted rewrite rules into the in-memory store.
	if eng.Super.PgRewritePage != 0 {
		if err := c.loadRules(); err != nil {
			return nil, fmt.Errorf("catalog: load rules: %w", err)
		}
	}

	// Load persisted RLS policies into the in-memory store.
	if eng.Super.PgPolicyPage != 0 {
		if err := c.loadPolicies(); err != nil {
			return nil, fmt.Errorf("catalog: load policies: %w", err)
		}
	}

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

// bootstrap allocates heap pages for pg_class, pg_attribute, and
// pg_rewrite, storing their page numbers in the superblock.
func (c *Catalog) bootstrap() error {
	pgClassPage, err := c.Eng.AllocPage()
	if err != nil {
		return err
	}
	pgAttrPage, err := c.Eng.AllocPage()
	if err != nil {
		return err
	}
	pgRewritePage, err := c.Eng.AllocPage()
	if err != nil {
		return err
	}
	pgPolicyPage, err := c.Eng.AllocPage()
	if err != nil {
		return err
	}

	// Init the heap pages.
	for _, pg := range []uint32{pgClassPage, pgAttrPage, pgRewritePage, pgPolicyPage} {
		buf, err := c.Eng.Pool.FetchPage(pg)
		if err != nil {
			return err
		}
		sp := slottedpage.Init(slottedpage.PageTypeHeap, pg, 0)
		copy(buf, sp.Bytes())
		c.Eng.Pool.MarkDirty(pg)
		c.Eng.Pool.ReleasePage(pg)
	}

	c.Eng.Super.PgClassPage = pgClassPage
	c.Eng.Super.PgAttrPage = pgAttrPage
	c.Eng.Super.PgRewritePage = pgRewritePage
	c.Eng.Super.PgPolicyPage = pgPolicyPage

	return nil
}

// CreateTable creates a new table with the given name and columns.
// It allocates a heap page for the table, inserts metadata into
// pg_class and pg_attribute, and returns the OID.
func (c *Catalog) CreateTable(name string, cols []ColumnDef) (int32, error) {
	// Check for duplicate name.
	existing, err := c.FindRelation(name)
	if err != nil {
		return 0, err
	}
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

	// Insert into pg_class.
	_, err = c.Eng.Insert(xid, c.Eng.Super.PgClassPage, []tuple.Datum{
		tuple.DInt32(oid),
		tuple.DText(name),
		tuple.DInt32(RelKindTable),
		tuple.DInt32(1), // relpages
		tuple.DInt32(int32(headPage)),
	})
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return 0, fmt.Errorf("catalog: insert pg_class: %w", err)
	}

	// Insert into pg_attribute.
	for i, col := range cols {
		_, err = c.Eng.Insert(xid, c.Eng.Super.PgAttrPage, []tuple.Datum{
			tuple.DInt32(oid),
			tuple.DText(col.Name),
			tuple.DInt32(int32(i + 1)), // 1-based
			tuple.DInt32(int32(col.Type)),
		})
		if err != nil {
			c.Eng.TxMgr.Abort(xid)
			return 0, fmt.Errorf("catalog: insert pg_attribute col %q: %w", col.Name, err)
		}
	}

	c.Eng.TxMgr.Commit(xid)
	return oid, nil
}

// CreateView registers a view in the catalog. A view is a relation with
// RelKindView and an associated _RETURN rewrite rule that holds the
// defining SELECT query. This mirrors PostgreSQL's DefineView() which
// creates a pg_class entry + a pg_rewrite _RETURN rule.
func (c *Catalog) CreateView(name string, cols []ColumnDef, definition string) (int32, error) {
	existing, err := c.FindRelation(name)
	if err != nil {
		return 0, err
	}
	if existing != nil {
		return 0, fmt.Errorf("catalog: relation %q already exists", name)
	}

	oid := int32(c.Eng.Super.AllocOID())

	xid := c.Eng.TxMgr.Begin()

	// Insert into pg_class with RelKindView and HeadPage=0 (no storage).
	_, err = c.Eng.Insert(xid, c.Eng.Super.PgClassPage, []tuple.Datum{
		tuple.DInt32(oid),
		tuple.DText(name),
		tuple.DInt32(RelKindView),
		tuple.DInt32(0), // relpages (views have no storage)
		tuple.DInt32(0), // headpage (views have no storage)
	})
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return 0, fmt.Errorf("catalog: insert pg_class for view: %w", err)
	}

	// Insert columns into pg_attribute.
	for i, col := range cols {
		_, err = c.Eng.Insert(xid, c.Eng.Super.PgAttrPage, []tuple.Datum{
			tuple.DInt32(oid),
			tuple.DText(col.Name),
			tuple.DInt32(int32(i + 1)),
			tuple.DInt32(int32(col.Type)),
		})
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

// FindRelation searches pg_class for a relation by name. Returns nil
// if not found.
func (c *Catalog) FindRelation(name string) (*Relation, error) {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	return c.findRelationWithSnapshot(name, snap)
}

func (c *Catalog) findRelationWithSnapshot(name string, snap *mvcc.Snapshot) (*Relation, error) {
	var found *Relation
	err := c.Eng.SeqScan(c.Eng.Super.PgClassPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) >= 5 && tup.Columns[1].Text == name {
			found = &Relation{
				OID:      tup.Columns[0].I32,
				Name:     tup.Columns[1].Text,
				Kind:     tup.Columns[2].I32,
				Pages:    tup.Columns[3].I32,
				HeadPage: tup.Columns[4].I32,
			}
			return false
		}
		return true
	})
	return found, err
}

// GetColumns returns the columns for a relation OID, ordered by attnum.
func (c *Catalog) GetColumns(oid int32) ([]Column, error) {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	return c.getColumnsWithSnapshot(oid, snap)
}

func (c *Catalog) getColumnsWithSnapshot(oid int32, snap *mvcc.Snapshot) ([]Column, error) {
	var cols []Column
	err := c.Eng.SeqScan(c.Eng.Super.PgAttrPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) >= 4 && tup.Columns[0].I32 == oid {
			cols = append(cols, Column{
				RelID: tup.Columns[0].I32,
				Name:  tup.Columns[1].Text,
				Num:   tup.Columns[2].I32,
				Type:  tup.Columns[3].I32,
			})
		}
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

// ListTables returns all relations of kind table.
func (c *Catalog) ListTables() ([]Relation, error) {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	var tables []Relation
	err := c.Eng.SeqScan(c.Eng.Super.PgClassPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) >= 5 && tup.Columns[2].I32 == RelKindTable {
			tables = append(tables, Relation{
				OID:      tup.Columns[0].I32,
				Name:     tup.Columns[1].Text,
				Kind:     tup.Columns[2].I32,
				Pages:    tup.Columns[3].I32,
				HeadPage: tup.Columns[4].I32,
			})
		}
		return true
	})
	return tables, err
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

	// Insert into pg_class with extra columns for index metadata.
	// Columns: oid, relname, relkind, relpages, relheadpage, indrelid, indkey, indmethod
	xid := c.Eng.TxMgr.Begin()
	_, err = c.Eng.Insert(xid, c.Eng.Super.PgClassPage, []tuple.Datum{
		tuple.DInt32(oid),
		tuple.DText(indexName),
		tuple.DInt32(RelKindIndex),
		tuple.DInt32(1),
		tuple.DInt32(int32(rootPage)),
		tuple.DInt32(table.OID), // indrelid
		tuple.DInt32(colNum),    // indkey
		tuple.DText(method),     // indmethod
	})
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

// ListIndexesForTable returns all indexes for the given table OID.
func (c *Catalog) ListIndexesForTable(tableOID int32) ([]IndexInfo, error) {
	return c.getIndexesForTable(tableOID)
}

// getIndexesForTable returns all indexes for the given table OID.
func (c *Catalog) getIndexesForTable(tableOID int32) ([]IndexInfo, error) {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	var indexes []IndexInfo
	c.Eng.SeqScan(c.Eng.Super.PgClassPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) >= 7 && tup.Columns[2].I32 == RelKindIndex && tup.Columns[5].I32 == tableOID {
			method := "btree"
			if len(tup.Columns) >= 8 && tup.Columns[7].Type == tuple.TypeText {
				method = tup.Columns[7].Text
			}
			indexes = append(indexes, IndexInfo{
				Relation: Relation{
					OID:      tup.Columns[0].I32,
					Name:     tup.Columns[1].Text,
					Kind:     tup.Columns[2].I32,
					Pages:    tup.Columns[3].I32,
					HeadPage: tup.Columns[4].I32,
				},
				TableOID: tup.Columns[5].I32,
				ColNum:   tup.Columns[6].I32,
				Method:   method,
			})
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
		if len(tup.Columns) >= 7 && tup.Columns[1].Text == name && tup.Columns[2].I32 == RelKindIndex {
			method := "btree"
			if len(tup.Columns) >= 8 && tup.Columns[7].Type == tuple.TypeText {
				method = tup.Columns[7].Text
			}
			found = &IndexInfo{
				Relation: Relation{
					OID:      tup.Columns[0].I32,
					Name:     tup.Columns[1].Text,
					Kind:     tup.Columns[2].I32,
					Pages:    tup.Columns[3].I32,
					HeadPage: tup.Columns[4].I32,
				},
				TableOID: tup.Columns[5].I32,
				ColNum:   tup.Columns[6].I32,
				Method:   method,
			}
			return false
		}
		return true
	})
	return found, nil
}

// updateIndexRootPage updates the HeadPage (root page) for an index
// in pg_class after a B+Tree root split.
func (c *Catalog) updateIndexRootPage(indexOID int32, newRoot uint32) {
	// Find the pg_class tuple for this index and delete + reinsert it
	// with the updated root page. This is a simple approach.
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)

	var targetID slottedpage.ItemID
	var targetTup *tuple.Tuple
	c.Eng.SeqScan(c.Eng.Super.PgClassPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) >= 7 && tup.Columns[0].I32 == indexOID && tup.Columns[2].I32 == RelKindIndex {
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

	// Delete old entry.
	c.Eng.Delete(xid, targetID)

	// Insert updated entry, preserving all columns including method.
	newCols := []tuple.Datum{
		tuple.DInt32(targetTup.Columns[0].I32),
		tuple.DText(targetTup.Columns[1].Text),
		tuple.DInt32(targetTup.Columns[2].I32),
		tuple.DInt32(targetTup.Columns[3].I32),
		tuple.DInt32(int32(newRoot)),
		tuple.DInt32(targetTup.Columns[5].I32),
		tuple.DInt32(targetTup.Columns[6].I32),
	}
	if len(targetTup.Columns) >= 8 {
		newCols = append(newCols, targetTup.Columns[7])
	}
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
}

// Stats gathers basic statistics for the named table.
// IsView returns true if the relation is a view.
func (c *Catalog) IsView(relOID int32) bool {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	var kind int32
	_ = c.Eng.SeqScan(c.Eng.Super.PgClassPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) >= 5 && tup.Columns[0].I32 == relOID {
			kind = tup.Columns[2].I32
			return false
		}
		return true
	})
	return kind == RelKindView
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

	stats := &TableStats{RelPages: rel.Pages}

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
			} else if tup.Xmax != 0 {
				stats.DeadCount++
			}
		}
		next := sp.NextPage()
		c.Eng.Pool.ReleasePage(cur)
		cur = next
	}
	return stats, nil
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
	var found bool
	c.Eng.SeqScan(c.Eng.Super.PgClassPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) >= 5 && tup.Columns[0].I32 == rel.OID {
			targetID = id
			found = true
			return false
		}
		return true
	})

	if !found {
		c.Eng.TxMgr.Abort(xid)
		return
	}

	// Delete old entry and reinsert with updated page count.
	c.Eng.Delete(xid, targetID)

	cols := []tuple.Datum{
		tuple.DInt32(rel.OID),
		tuple.DText(rel.Name),
		tuple.DInt32(rel.Kind),
		tuple.DInt32(count),
		tuple.DInt32(rel.HeadPage),
	}
	c.Eng.Insert(xid, c.Eng.Super.PgClassPage, cols)
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


