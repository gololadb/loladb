package catalog

import (
	"fmt"

	"github.com/gololadb/loladb/pkg/tuple"
)

// bootstrap creates the initial system catalog tables and populates
// them with self-describing rows. This mirrors PostgreSQL's bootstrap
// process (src/backend/catalog/heap.c) where the catalog tables are
// created by hand-inserting tuples before the normal DDL path is
// available.
//
// After bootstrap, the catalog is fully self-describing:
//   - pg_class has a row for every catalog table (including itself)
//   - pg_attribute has rows for every column of every catalog table
//   - pg_type has rows for the built-in types
//   - pg_namespace has rows for pg_catalog and public
func (c *Catalog) bootstrap() error {
	// Allocate heap pages for each catalog table.
	pages := make(map[int32]uint32) // OID → head page
	catalogOIDs := []int32{
		OIDPgNamespace, OIDPgType, OIDPgClass, OIDPgAttribute,
		OIDPgIndex, OIDPgAuthID, OIDPgAuthMembers, OIDPgACL,
		OIDPgRewrite, OIDPgPolicy, OIDPgProc, OIDPgTrigger,
		OIDPgPartition,
	}
	for _, oid := range catalogOIDs {
		page, err := c.Eng.AllocPage()
		if err != nil {
			return fmt.Errorf("bootstrap: alloc page for OID %d: %w", oid, err)
		}
		pages[oid] = page
	}

	// Store page pointers in superblock. Long-term only PgClassPage is
	// needed (everything else is discoverable from pg_class), but we
	// keep the others for backward compat during the transition.
	c.Eng.Super.PgClassPage = pages[OIDPgClass]
	c.Eng.Super.PgAttrPage = pages[OIDPgAttribute]
	c.Eng.Super.PgRewritePage = pages[OIDPgRewrite]
	c.Eng.Super.PgPolicyPage = pages[OIDPgPolicy]
	c.Eng.Super.PgAuthIDPage = pages[OIDPgAuthID]
	c.Eng.Super.PgAuthMembersPage = pages[OIDPgAuthMembers]
	c.Eng.Super.PgACLPage = pages[OIDPgACL]
	c.Eng.Super.PgProcPage = pages[OIDPgProc]
	c.Eng.Super.PgTriggerPage = pages[OIDPgTrigger]
	c.Eng.Super.PgPartitionPage = pages[OIDPgPartition]
	if err := c.Eng.Super.Save(c.Eng.IO); err != nil {
		return fmt.Errorf("bootstrap: write superblock: %w", err)
	}

	// Set NextOID past all catalog OIDs.
	c.Eng.Super.NextOID = FirstNormalOID

	// --- pg_namespace ---
	nsPage := pages[OIDPgNamespace]
	c.insertRaw(nsPage, pgNamespaceRow(OIDPgCatalog, "pg_catalog", 10))
	c.insertRaw(nsPage, pgNamespaceRow(OIDPublic, "public", 10))

	// --- pg_type ---
	typPage := pages[OIDPgType]
	for _, t := range builtinTypes() {
		c.insertRaw(typPage, t)
	}

	// --- pg_class (catalog tables themselves) ---
	classPage := pages[OIDPgClass]
	catTables := []struct {
		oid  int32
		name string
	}{
		{OIDPgNamespace, "pg_namespace"},
		{OIDPgType, "pg_type"},
		{OIDPgClass, "pg_class"},
		{OIDPgAttribute, "pg_attribute"},
		{OIDPgIndex, "pg_index"},
		{OIDPgAuthID, "pg_authid"},
		{OIDPgAuthMembers, "pg_auth_members"},
		{OIDPgACL, "pg_acl"},
		{OIDPgRewrite, "pg_rewrite"},
		{OIDPgPolicy, "pg_policy"},
		{OIDPgProc, "pg_proc"},
		{OIDPgTrigger, "pg_trigger"},
		{OIDPgPartition, "pg_partition"},
	}
	for _, t := range catTables {
		row := pgClassRow(t.oid, t.name, OIDPgCatalog, RelKindOrdinaryTable_S,
			0, 0, 0, 10, "heap", int32(pages[t.oid]), 0, 0)
		c.insertRaw(classPage, row)
	}

	// --- pg_attribute (columns of every catalog table) ---
	attrPage := pages[OIDPgAttribute]
	for _, def := range catalogColumnDefs() {
		c.insertRaw(attrPage, def)
	}

	// --- pg_authid (default superuser) ---
	authPage := pages[OIDPgAuthID]
	c.insertRaw(authPage, roleToTuple(&Role{
		OID:        10, // well-known OID for bootstrap superuser
		Name:       "loladb",
		SuperUser:  true,
		CreateDB:   true,
		CreateRole: true,
		Inherit:    true,
		Login:      true,
		BypassRLS:  true,
		ConnLimit:  -1,
	}))

	// Write superblock again with updated NextOID.
	if err := c.Eng.Super.Save(c.Eng.IO); err != nil {
		return fmt.Errorf("bootstrap: write superblock: %w", err)
	}

	return nil
}

// insertRaw inserts a tuple into the given heap page using the engine
// directly, bypassing the catalog API (needed during bootstrap).
func (c *Catalog) insertRaw(headPage uint32, cols []tuple.Datum) {
	xid := c.Eng.TxMgr.Begin()
	c.Eng.Insert(xid, headPage, cols)
	c.Eng.TxMgr.Commit(xid)
}

// pgNamespaceRow builds a pg_namespace tuple.
func pgNamespaceRow(oid int32, name string, owner int32) []tuple.Datum {
	return []tuple.Datum{
		tuple.DInt32(oid),
		tuple.DText(name),
		tuple.DInt32(owner),
	}
}

// pgClassRow builds a pg_class tuple.
func pgClassRow(oid int32, name string, namespace int32, kind string,
	pages, tuples, hasindex, owner int32, am string, headpage, indexoid, indexcol int32) []tuple.Datum {
	return []tuple.Datum{
		tuple.DInt32(oid),       // oid
		tuple.DText(name),       // relname
		tuple.DInt32(namespace), // relnamespace
		tuple.DText(kind),       // relkind
		tuple.DInt32(pages),     // relpages
		tuple.DInt32(tuples),    // reltuples
		tuple.DInt32(hasindex),  // relhasindex
		tuple.DInt32(owner),     // relowner
		tuple.DText(am),         // relam
		tuple.DInt32(headpage),  // relheadpage
		tuple.DInt32(indexoid),  // relindexoid (for indexes: table OID)
		tuple.DInt32(indexcol),  // relindexcol (for indexes: column num)
	}
}

// pgAttributeRow builds a pg_attribute tuple.
func pgAttributeRow(relid int32, name string, typid, typlen int32, num int16, notNull bool, defaultExpr string, typmods ...int32) []tuple.Datum {
	var nn int32
	if notNull {
		nn = 1
	}
	var hasDef int32
	if defaultExpr != "" {
		hasDef = 1
	}
	typmod := int32(-1)
	if len(typmods) > 0 {
		typmod = typmods[0]
	}
	return []tuple.Datum{
		tuple.DInt32(relid),        // attrelid
		tuple.DText(name),          // attname
		tuple.DInt32(typid),        // atttypid
		tuple.DInt32(typlen),       // attlen
		tuple.DInt32(int32(num)),   // attnum
		tuple.DInt32(typmod),       // atttypmod
		tuple.DInt32(nn),           // attnotnull
		tuple.DInt32(0),            // attisdropped
		tuple.DInt32(hasDef),       // atthasdef
		tuple.DText(defaultExpr),   // attdefault
		tuple.DText(""),            // attgenerated
	}
}

// pgAttributeRowGenerated builds a pg_attribute tuple for a generated column.
func pgAttributeRowGenerated(relid int32, name string, typid, typlen int32, num int16, notNull bool, generatedExpr string, typmod int32) []tuple.Datum {
	var nn int32
	if notNull {
		nn = 1
	}
	return []tuple.Datum{
		tuple.DInt32(relid),          // attrelid
		tuple.DText(name),            // attname
		tuple.DInt32(typid),          // atttypid
		tuple.DInt32(typlen),         // attlen
		tuple.DInt32(int32(num)),     // attnum
		tuple.DInt32(typmod),         // atttypmod
		tuple.DInt32(nn),             // attnotnull
		tuple.DInt32(0),              // attisdropped
		tuple.DInt32(0),              // atthasdef (generated cols don't use atthasdef)
		tuple.DText(""),              // attdefault
		tuple.DText(generatedExpr),   // attgenerated
	}
}

// builtinTypes returns pg_type rows for the built-in types.
func builtinTypes() [][]tuple.Datum {
	type typDef struct {
		oid  int32
		name string
		len  int32
	}
	types := []typDef{
		{OIDBool, "bool", 1},
		{OIDInt2, "int2", 2},
		{OIDInt4, "int4", 4},
		{OIDInt8, "int8", 8},
		{OIDFloat8, "float8", 8},
		{OIDText, "text", -1},
		{OIDName, "name", -1},
		{OIDOid, "oid", 4},
		{OIDChar, "char", 1},
		{OIDInt2Vec, "int2vector", -1},
		{OIDDate, "date", 4},
		{OIDTimestamp, "timestamp", 8},
		{OIDNumeric, "numeric", -1},
		{OIDJSON, "json", -1},
		{OIDJSONB, "jsonb", -1},
		{OIDUUID, "uuid", 16},
		{OIDInterval, "interval", 16},
		{OIDBytea, "bytea", -1},
		{OIDMoney, "money", 8},
		{OIDTextArray, "_text", -1},
	}
	var rows [][]tuple.Datum
	for _, t := range types {
		rows = append(rows, []tuple.Datum{
			tuple.DInt32(t.oid),
			tuple.DText(t.name),
			tuple.DInt32(OIDPgCatalog),
			tuple.DInt32(t.len),
			tuple.DText("b"), // base type
			tuple.DInt32(0),  // typbasetype (0 for base types)
			tuple.DInt32(0),  // typnotnull (0 for base types)
			tuple.DText(""),  // typcheck (empty for base types)
			tuple.DText(""),  // typenumvals (empty for base types)
		})
	}
	return rows
}

// catalogColumnDefs returns pg_attribute rows for all catalog tables.
func catalogColumnDefs() [][]tuple.Datum {
	type colDef struct {
		relOID int32
		name   string
		typOID int32
		typLen int32
		num    int16
	}

	defs := []colDef{
		// pg_namespace columns
		{OIDPgNamespace, "oid", OIDInt4, 4, 1},
		{OIDPgNamespace, "nspname", OIDText, -1, 2},
		{OIDPgNamespace, "nspowner", OIDInt4, 4, 3},

		// pg_type columns
		{OIDPgType, "oid", OIDInt4, 4, 1},
		{OIDPgType, "typname", OIDText, -1, 2},
		{OIDPgType, "typnamespace", OIDInt4, 4, 3},
		{OIDPgType, "typlen", OIDInt4, 4, 4},
		{OIDPgType, "typtype", OIDText, -1, 5},
		{OIDPgType, "typbasetype", OIDInt4, 4, 6},
		{OIDPgType, "typnotnull", OIDInt4, 4, 7},
		{OIDPgType, "typcheck", OIDText, -1, 8},
		{OIDPgType, "typenumvals", OIDText, -1, 9},

		// pg_class columns
		{OIDPgClass, "oid", OIDInt4, 4, 1},
		{OIDPgClass, "relname", OIDText, -1, 2},
		{OIDPgClass, "relnamespace", OIDInt4, 4, 3},
		{OIDPgClass, "relkind", OIDText, -1, 4},
		{OIDPgClass, "relpages", OIDInt4, 4, 5},
		{OIDPgClass, "reltuples", OIDInt4, 4, 6},
		{OIDPgClass, "relhasindex", OIDInt4, 4, 7},
		{OIDPgClass, "relowner", OIDInt4, 4, 8},
		{OIDPgClass, "relam", OIDText, -1, 9},
		{OIDPgClass, "relheadpage", OIDInt4, 4, 10},
		{OIDPgClass, "relindexoid", OIDInt4, 4, 11},
		{OIDPgClass, "relindexcol", OIDInt4, 4, 12},

		// pg_attribute columns
		{OIDPgAttribute, "attrelid", OIDInt4, 4, 1},
		{OIDPgAttribute, "attname", OIDText, -1, 2},
		{OIDPgAttribute, "atttypid", OIDInt4, 4, 3},
		{OIDPgAttribute, "attlen", OIDInt4, 4, 4},
		{OIDPgAttribute, "attnum", OIDInt4, 4, 5},
		{OIDPgAttribute, "atttypmod", OIDInt4, 4, 6},
		{OIDPgAttribute, "attnotnull", OIDInt4, 4, 7},
		{OIDPgAttribute, "attisdropped", OIDInt4, 4, 8},

		// pg_index columns
		{OIDPgIndex, "indexrelid", OIDInt4, 4, 1},
		{OIDPgIndex, "indrelid", OIDInt4, 4, 2},
		{OIDPgIndex, "indkey", OIDText, -1, 3},
		{OIDPgIndex, "indisunique", OIDInt4, 4, 4},

		// pg_authid columns (matches roleToTuple in auth.go)
		{OIDPgAuthID, "oid", OIDInt4, 4, 1},
		{OIDPgAuthID, "rolname", OIDText, -1, 2},
		{OIDPgAuthID, "rolsuper", OIDInt4, 4, 3},
		{OIDPgAuthID, "rolcreatedb", OIDInt4, 4, 4},
		{OIDPgAuthID, "rolcreaterole", OIDInt4, 4, 5},
		{OIDPgAuthID, "rolinherit", OIDInt4, 4, 6},
		{OIDPgAuthID, "rolcanlogin", OIDInt4, 4, 7},
		{OIDPgAuthID, "rolbypassrls", OIDInt4, 4, 8},
		{OIDPgAuthID, "rolconnlimit", OIDInt4, 4, 9},
		{OIDPgAuthID, "rolpassword", OIDText, -1, 10},

		// pg_auth_members columns
		{OIDPgAuthMembers, "roleid", OIDInt4, 4, 1},
		{OIDPgAuthMembers, "member", OIDInt4, 4, 2},
		{OIDPgAuthMembers, "admin_option", OIDInt4, 4, 3},

		// pg_acl columns (matches persistACLFull in auth.go)
		{OIDPgACL, "objoid", OIDInt4, 4, 1},
		{OIDPgACL, "grantee", OIDInt4, 4, 2},
		{OIDPgACL, "grantor", OIDInt4, 4, 3},
		{OIDPgACL, "privileges", OIDInt4, 4, 4},
		{OIDPgACL, "columns", OIDText, -1, 5},

		// pg_rewrite columns (matches persistRule in catalog.go)
		{OIDPgRewrite, "ev_class", OIDInt4, 4, 1},
		{OIDPgRewrite, "rulename", OIDText, -1, 2},
		{OIDPgRewrite, "ev_type", OIDInt4, 4, 3},
		{OIDPgRewrite, "ev_action", OIDInt4, 4, 4},
		{OIDPgRewrite, "definition", OIDText, -1, 5},

		// pg_policy columns (matches persistPolicy in catalog.go)
		{OIDPgPolicy, "polrelid", OIDInt4, 4, 1},
		{OIDPgPolicy, "polname", OIDText, -1, 2},
		{OIDPgPolicy, "polcmd", OIDInt4, 4, 3},
		{OIDPgPolicy, "polpermissive", OIDInt4, 4, 4},
		{OIDPgPolicy, "polroles", OIDText, -1, 5},
		{OIDPgPolicy, "polqual", OIDText, -1, 6},
		{OIDPgPolicy, "polwithcheck", OIDText, -1, 7},

		// pg_proc columns (matches persistFunction in functions.go)
		{OIDPgProc, "oid", OIDInt4, 4, 1},
		{OIDPgProc, "proname", OIDText, -1, 2},
		{OIDPgProc, "prolang", OIDText, -1, 3},
		{OIDPgProc, "prosrc", OIDText, -1, 4},
		{OIDPgProc, "prorettype", OIDText, -1, 5},
		{OIDPgProc, "proargnames", OIDText, -1, 6},
		{OIDPgProc, "proargtypes", OIDText, -1, 7},

		// pg_trigger columns (matches persistTrigger in functions.go)
		{OIDPgTrigger, "oid", OIDInt4, 4, 1},
		{OIDPgTrigger, "tgname", OIDText, -1, 2},
		{OIDPgTrigger, "tgrelid", OIDInt4, 4, 3},
		{OIDPgTrigger, "tgfoid", OIDInt4, 4, 4},
		{OIDPgTrigger, "tgtiming", OIDInt4, 4, 5},
		{OIDPgTrigger, "tgevents", OIDInt4, 4, 6},
		{OIDPgTrigger, "tgforeach", OIDText, -1, 7},

		// pg_partition columns (matches persistPartition* in catalog.go)
		{OIDPgPartition, "parent", OIDText, -1, 1},
		{OIDPgPartition, "child", OIDText, -1, 2},
		{OIDPgPartition, "strategy", OIDText, -1, 3},
		{OIDPgPartition, "keycols", OIDText, -1, 4},
		{OIDPgPartition, "boundtype", OIDText, -1, 5},
		{OIDPgPartition, "listvals", OIDText, -1, 6},
		{OIDPgPartition, "rangefrom", OIDText, -1, 7},
		{OIDPgPartition, "rangeto", OIDText, -1, 8},
	}

	var rows [][]tuple.Datum
	for _, d := range defs {
		rows = append(rows, pgAttributeRow(d.relOID, d.name, d.typOID, d.typLen, d.num, false, ""))
	}
	return rows
}
