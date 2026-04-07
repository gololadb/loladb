package catalog

import (
	"fmt"
	"strings"

	"github.com/gololadb/loladb/pkg/mvcc"
	"github.com/gololadb/loladb/pkg/engine/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
)

// Schema represents a row from pg_namespace.
type Schema struct {
	OID      int32
	Name     string
	OwnerOID int32
}

// pgNamespacePage returns the heap page for pg_namespace by looking it up in pg_class.
func (c *Catalog) pgNamespacePage() uint32 {
	rel, err := c.FindRelation("pg_namespace")
	if err != nil || rel == nil {
		return 0
	}
	return uint32(rel.HeadPage)
}

// FindSchema looks up a schema by name in pg_namespace.
func (c *Catalog) FindSchema(name string) (*Schema, error) {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	nsPage := c.pgNamespacePage()
	if nsPage == 0 {
		return nil, nil
	}
	var found *Schema
	c.Eng.SeqScan(nsPage, snap, func(_ slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) < 3 {
			return true
		}
		if tup.Columns[1].Text == name {
			found = &Schema{
				OID:      tup.Columns[0].I32,
				Name:     tup.Columns[1].Text,
				OwnerOID: tup.Columns[2].I32,
			}
			return false
		}
		return true
	})
	return found, nil
}

// ListSchemas returns all schemas from pg_namespace.
func (c *Catalog) ListSchemas() ([]Schema, error) {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	nsPage := c.pgNamespacePage()
	if nsPage == 0 {
		return nil, nil
	}
	var schemas []Schema
	c.Eng.SeqScan(nsPage, snap, func(_ slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) < 3 {
			return true
		}
		schemas = append(schemas, Schema{
			OID:      tup.Columns[0].I32,
			Name:     tup.Columns[1].Text,
			OwnerOID: tup.Columns[2].I32,
		})
		return true
	})
	return schemas, nil
}

// CreateSchema creates a new schema in pg_namespace.
func (c *Catalog) CreateSchema(name string, ifNotExists bool, ownerOID int32) error {
	if strings.HasPrefix(name, "pg_") {
		return fmt.Errorf("unacceptable schema name %q: schema names beginning with \"pg_\" are reserved", name)
	}
	existing, err := c.FindSchema(name)
	if err != nil {
		return err
	}
	if existing != nil {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("schema %q already exists", name)
	}

	oid := int32(c.Eng.Super.AllocOID())
	nsPage := c.pgNamespacePage()
	if nsPage == 0 {
		return fmt.Errorf("catalog: pg_namespace page not initialized")
	}

	xid := c.Eng.TxMgr.Begin()
	_, err = c.Eng.Insert(xid, nsPage, pgNamespaceRow(oid, name, ownerOID))
	if err != nil {
		c.Eng.TxMgr.Abort(xid)
		return fmt.Errorf("catalog: insert pg_namespace: %w", err)
	}
	c.Eng.TxMgr.Commit(xid)
	c.cache.invalidate()
	return nil
}

// DropSchema removes a schema from pg_namespace.
// If cascade is true, all objects in the schema are dropped first.
func (c *Catalog) DropSchema(name string, missingOk bool, cascade bool) error {
	if name == "pg_catalog" || name == "public" {
		return fmt.Errorf("cannot drop schema %q: it is required by the database system", name)
	}

	nsPage := c.pgNamespacePage()
	if nsPage == 0 {
		if missingOk {
			return nil
		}
		return fmt.Errorf("schema %q does not exist", name)
	}

	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)

	// Find the schema row.
	var target *slottedpage.ItemID
	var schemaOID int32
	c.Eng.SeqScan(nsPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) >= 2 && tup.Columns[1].Text == name {
			target = &id
			schemaOID = tup.Columns[0].I32
			return false
		}
		return true
	})

	if target == nil {
		c.Eng.TxMgr.Commit(xid)
		if missingOk {
			return nil
		}
		return fmt.Errorf("schema %q does not exist", name)
	}

	// Collect relations in this schema.
	type relItem struct {
		id  slottedpage.ItemID
		rel *Relation
	}
	var schemaRels []relItem
	c.Eng.SeqScan(c.Eng.Super.PgClassPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		r := tupleToRelation(tup)
		if r != nil && r.NamespaceOID == schemaOID {
			schemaRels = append(schemaRels, relItem{id: id, rel: r})
		}
		return true
	})

	if len(schemaRels) > 0 && !cascade {
		c.Eng.TxMgr.Commit(xid)
		return fmt.Errorf("cannot drop schema %q because other objects depend on it", name)
	}

	// CASCADE: drop all relations in the schema.
	// Drop indexes first, then tables/views.
	for _, ri := range schemaRels {
		if ri.rel.Kind == 1 { // RelKindIndex
			c.Eng.Delete(xid, ri.id)
		}
	}
	for _, ri := range schemaRels {
		if ri.rel.Kind != 1 { // not index
			// Delete pg_attribute rows for this relation.
			c.deleteAttributesForRel(xid, snap, ri.rel.OID)
			c.Eng.Delete(xid, ri.id)
		}
	}

	// Delete the schema row.
	c.Eng.Delete(xid, *target)
	c.Eng.TxMgr.Commit(xid)
	c.cache.invalidate()
	return nil
}

// SchemaOID returns the OID for a schema name, or 0 if not found.
func (c *Catalog) SchemaOID(name string) int32 {
	s, _ := c.FindSchema(name)
	if s != nil {
		return s.OID
	}
	return 0
}

// CurrentSchema returns the first schema in the search path that exists.
func (c *Catalog) CurrentSchema() string {
	for _, s := range c.SearchPath {
		if existing, _ := c.FindSchema(s); existing != nil {
			return s
		}
	}
	return "public"
}

// CurrentSchemaOID returns the OID of the current schema.
func (c *Catalog) CurrentSchemaOID() int32 {
	return c.SchemaOID(c.CurrentSchema())
}

// ResolveRelationName resolves an optionally schema-qualified name to a
// (schemaOID, unqualifiedName) pair. If schemaName is empty, the search
// path is used.
func (c *Catalog) ResolveRelationName(schemaName, relName string) (int32, string) {
	if schemaName != "" {
		return c.SchemaOID(schemaName), relName
	}
	// Search path: try each schema in order.
	// pg_catalog is always implicitly searched first.
	for _, ns := range append([]string{"pg_catalog"}, c.SearchPath...) {
		nsOID := c.SchemaOID(ns)
		if nsOID == 0 {
			continue
		}
		// Check if a relation with this name exists in this namespace.
		rel, _ := c.findRelationInNamespace(relName, nsOID)
		if rel != nil {
			return nsOID, relName
		}
	}
	// Default to current schema for creation.
	return c.CurrentSchemaOID(), relName
}

// findRelationInNamespace looks up a relation by name within a specific namespace.
func (c *Catalog) findRelationInNamespace(name string, nsOID int32) (*Relation, error) {
	xid := c.Eng.TxMgr.Begin()
	snap := c.Eng.TxMgr.Snapshot(xid)
	defer c.Eng.TxMgr.Commit(xid)

	var found *Relation
	c.Eng.SeqScan(c.Eng.Super.PgClassPage, snap, func(_ slottedpage.ItemID, tup *tuple.Tuple) bool {
		r := tupleToRelation(tup)
		if r != nil && r.Name == name && r.NamespaceOID == nsOID {
			found = r
			return false
		}
		return true
	})
	return found, nil
}

// FindRelationQualified looks up a relation by optional schema and name.
// If schema is empty, the search path is used (pg_catalog first, then SearchPath).
func (c *Catalog) FindRelationQualified(schema, name string) (*Relation, error) {
	if schema != "" {
		nsOID := c.SchemaOID(schema)
		if nsOID == 0 {
			return nil, fmt.Errorf("schema %q does not exist", schema)
		}
		return c.findRelationInNamespace(name, nsOID)
	}
	// Search path lookup.
	searchOrder := append([]string{"pg_catalog"}, c.SearchPath...)
	for _, ns := range searchOrder {
		nsOID := c.SchemaOID(ns)
		if nsOID == 0 {
			continue
		}
		rel, err := c.findRelationInNamespace(name, nsOID)
		if err != nil {
			return nil, err
		}
		if rel != nil {
			return rel, nil
		}
	}
	return nil, nil
}

// SetSearchPath updates the search path and persists it to the superblock.
func (c *Catalog) SetSearchPath(schemas []string) error {
	c.SearchPath = schemas
	c.Eng.Super.SearchPath = strings.Join(schemas, ",")
	return c.Eng.Super.Save(c.Eng.IO)
}

// deleteAttributesForRel deletes all pg_attribute rows for a given relation OID.
func (c *Catalog) deleteAttributesForRel(xid uint32, snap *mvcc.Snapshot, relOID int32) {
	var toDelete []slottedpage.ItemID
	c.Eng.SeqScan(c.Eng.Super.PgAttrPage, snap, func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if len(tup.Columns) > 0 && tup.Columns[0].I32 == relOID {
			toDelete = append(toDelete, id)
		}
		return true
	})
	for _, id := range toDelete {
		c.Eng.Delete(xid, id)
	}
}
