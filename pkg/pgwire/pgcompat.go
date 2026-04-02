package pgwire

import (
	"fmt"
	"strings"

	"github.com/gololadb/loladb/pkg/tuple"
)

// CatalogProvider supplies schema metadata for pg_dump compatibility.
type CatalogProvider interface {
	ListTables() []TableInfo
	ListIndexes() []IndexMeta
}

type TableInfo struct {
	OID     int32
	Name    string
	Columns []ColumnInfo
}

type ColumnInfo struct {
	Name    string
	TypeOID int32
	Num     int16
}

type IndexMeta struct {
	OID       int32
	Name      string
	TableOID  int32
	TableName string
	ColName   string
	Method    string
}

// interceptQuery handles protocol commands, built-in functions, and
// pg_dump catalog queries that our SQL executor can't handle yet.
func interceptQuery(sql string, provider CatalogProvider) (*QueryResult, bool) {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)


	// Protocol commands.
	if strings.HasPrefix(upper, "SET ") {
		return &QueryResult{Message: "SET"}, true
	}
	if upper == "BEGIN" || strings.HasPrefix(upper, "BEGIN ") {
		return &QueryResult{Message: "BEGIN"}, true
	}
	if upper == "COMMIT" || upper == "END" {
		return &QueryResult{Message: "COMMIT"}, true
	}
	if upper == "ROLLBACK" {
		return &QueryResult{Message: "ROLLBACK"}, true
	}
	if strings.HasPrefix(upper, "LOCK ") {
		return &QueryResult{Message: "LOCK TABLE"}, true
	}
	if strings.HasPrefix(upper, "DEALLOCATE ") {
		return &QueryResult{Message: "DEALLOCATE"}, true
	}
	if upper == "DISCARD ALL" {
		return &QueryResult{Message: "DISCARD ALL"}, true
	}

	// Built-in functions.
	if strings.Contains(upper, "SET_CONFIG") {
		return &QueryResult{
			Columns: []string{"set_config"},
			Rows:    [][]tuple.Datum{{tuple.DText("")}},
			Message: "SELECT 1",
		}, true
	}
	if strings.Contains(upper, "PG_IS_IN_RECOVERY") {
		return &QueryResult{
			Columns: []string{"pg_is_in_recovery"},
			Rows:    [][]tuple.Datum{{tuple.DBool(false)}},
			Message: "SELECT 1",
		}, true
	}
	if strings.Contains(upper, "CURRENT_SETTING") {
		return &QueryResult{
			Columns: []string{"current_setting"},
			Rows:    [][]tuple.Datum{{tuple.DText("")}},
			Message: "SELECT 1",
		}, true
	}
	if strings.Contains(upper, "VERSION()") && !strings.Contains(upper, "SERVER_VERSION") {
		return &QueryResult{
			Columns: []string{"version"},
			Rows:    [][]tuple.Datum{{tuple.DText("PostgreSQL 16.0 (LolaDB)")}},
			Message: "SELECT 1",
		}, true
	}
	if strings.Contains(upper, "CURRENT_DATABASE") {
		return &QueryResult{
			Columns: []string{"current_database"},
			Rows:    [][]tuple.Datum{{tuple.DText("loladb")}},
			Message: "SELECT 1",
		}, true
	}
	if strings.Contains(upper, "CURRENT_SCHEMAS") {
		return &QueryResult{
			Columns: []string{"current_schemas"},
			Rows:    [][]tuple.Datum{{tuple.DText("{public}")}},
			Message: "SELECT 1",
		}, true
	}
	if strings.Contains(upper, "CURRENT_SCHEMA") {
		return &QueryResult{
			Columns: []string{"current_schema"},
			Rows:    [][]tuple.Datum{{tuple.DText("public")}},
			Message: "SELECT 1",
		}, true
	}

	// WITH RECURSIVE / WITH queries — pg_dump uses these for
	// materialized view dependencies. Return empty results.
	if strings.HasPrefix(upper, "WITH RECURSIVE") || strings.HasPrefix(upper, "WITH ") {
		return &QueryResult{
			Columns: []string{"classid", "objid", "refobjid"},
			Rows:    [][]tuple.Datum{},
			Message: "SELECT 0",
		}, true
	}

	// Extract the primary FROM table (first FROM not inside parens)
	// to route to the correct handler.
	primaryFrom := extractPrimaryFrom(upper)


	// pg_dump catalog queries backed by real data.
	if provider != nil {
		if r, ok := handleCatalogQuery(primaryFrom, upper, provider); ok {
			return r, true
		}
	}

	// Unimplemented catalog tables — empty result with correct columns.
	if r, ok := handleUnimplementedCatalog(primaryFrom); ok {
		return r, true
	}

	return nil, false
}

// extractPrimaryFrom finds the primary FROM table in a SQL query by
// scanning character-by-character to track parenthesis depth and find
// the first top-level FROM clause.
func extractPrimaryFrom(upper string) string {
	normalized := strings.ReplaceAll(upper, "PG_CATALOG.", "")

	depth := 0
	i := 0
	for i < len(normalized) {
		ch := normalized[i]
		if ch == '(' {
			depth++
			i++
			continue
		}
		if ch == ')' {
			depth--
			if depth < 0 {
				depth = 0
			}
			i++
			continue
		}
		// Look for "FROM " at depth 0.
		if depth == 0 && i+5 <= len(normalized) && normalized[i:i+5] == "FROM " {
			// Extract the table name after FROM.
			rest := normalized[i+5:]
			// Skip whitespace.
			rest = strings.TrimLeft(rest, " \t\n\r")
			// Extract the table name (up to space, comma, newline, paren).
			end := strings.IndexAny(rest, " \t\n\r,;()")
			if end == -1 {
				end = len(rest)
			}
			table := rest[:end]
			if table != "" {
				return "FROM " + table
			}
		}
		i++
	}
	return normalized
}

// handleCatalogQuery generates responses from real catalog data for
// pg_dump's primary catalog queries.
func handleCatalogQuery(primaryFrom, upper string, provider CatalogProvider) (*QueryResult, bool) {
	// pg_namespace
	if strings.Contains(primaryFrom, "FROM PG_NAMESPACE") {
		return &QueryResult{
			Columns: []string{"tableoid", "oid", "nspname", "nspowner", "nspacl", "acldefault"},
			Rows: [][]tuple.Datum{
				{tuple.DInt32(2615), tuple.DInt32(2200), tuple.DText("public"), tuple.DInt32(10), tuple.DNull(), tuple.DNull()},
			},
			Message: "SELECT 1",
		}, true
	}

	// pg_class — user tables
	// Column order matches pg_dump 16's SELECT exactly.
	if strings.Contains(primaryFrom, "FROM PG_CLASS") {
		tables := provider.ListTables()
		indexes := provider.ListIndexes()
		cols := []string{
			"tableoid", "oid", "relname", "relnamespace", "relkind",       // 0-4
			"reltype", "relowner", "relchecks", "relhasindex", "relhasrules", // 5-9
			"relpages", "relhastriggers", "relpersistence", "reloftype",   // 10-13
			"relacl", "acldefault",                                         // 14-15
			"foreignserver", "relfrozenxid", "tfrozenxid", "toid",         // 16-19
			"toastpages", "toast_reloptions", "owning_tab", "owning_col",  // 20-23
			"reltablespace", "relhasoids", "relispopulated", "relreplident", // 24-27
			"relrowsecurity", "relforcerowsecurity", "relminmxid", "tminmxid", // 28-31
			"reloptions", "checkoption", "amname", "is_identity_sequence", // 32-35
			"ispartition", // 36
		}
		var rows [][]tuple.Datum
		for _, t := range tables {
			hasIdx := "f"
			for _, idx := range indexes {
				if idx.TableOID == t.OID {
					hasIdx = "t"
					break
				}
			}
			rows = append(rows, []tuple.Datum{
				tuple.DInt32(1259), tuple.DInt32(t.OID),                    // tableoid, oid
				tuple.DText(t.Name), tuple.DInt32(2200),                    // relname, relnamespace
				tuple.DText("r"),                                           // relkind
				tuple.DInt32(0), tuple.DInt32(10),                          // reltype, relowner
				tuple.DInt32(0), tuple.DText(hasIdx), tuple.DText("f"),     // relchecks, relhasindex, relhasrules
				tuple.DInt32(0), tuple.DText("f"), tuple.DText("p"),        // relpages, relhastriggers, relpersistence
				tuple.DInt32(0),                                            // reloftype
				tuple.DNull(), tuple.DNull(),                               // relacl, acldefault
				tuple.DInt32(0), tuple.DInt32(0), tuple.DInt32(0), tuple.DInt32(0), // foreignserver, relfrozenxid, tfrozenxid, toid
				tuple.DInt32(0), tuple.DNull(), tuple.DNull(), tuple.DInt32(0), // toastpages, toast_reloptions, owning_tab, owning_col
				tuple.DNull(), tuple.DText("f"), tuple.DText("t"), tuple.DText("d"), // reltablespace, relhasoids, relispopulated, relreplident
				tuple.DText("f"), tuple.DText("f"), tuple.DInt32(1), tuple.DInt32(0), // relrowsecurity, relforcerowsecurity, relminmxid, tminmxid
				tuple.DNull(), tuple.DNull(), tuple.DText("heap"), tuple.DText("f"), // reloptions, checkoption, amname, is_identity_sequence
				tuple.DText("f"), // ispartition
			})
		}
		return &QueryResult{Columns: cols, Rows: rows, Message: fmt.Sprintf("SELECT %d", len(rows))}, true
	}

	// pg_attribute — columns for user tables
	if strings.Contains(primaryFrom, "FROM PG_ATTRIBUTE") {
		// Column-level ACL queries — we never have column ACLs, return empty.
		if strings.Contains(upper, "ATTACL IS NOT NULL") || strings.Contains(upper, "PG_INIT_PRIVS") {
			// Determine column list from query context.
			if strings.Contains(upper, "DISTINCT ATTRELID") {
				return &QueryResult{Columns: []string{"attrelid"}, Message: "SELECT 0"}, true
			}
			return &QueryResult{
				Columns: []string{"attname", "attacl", "acldefault", "privtype", "initprivs"},
				Message: "SELECT 0",
			}, true
		}
		tables := provider.ListTables()
		cols := []string{
			"attrelid", "attname", "atttypid", "attlen", "attnum",
			"atttypmod", "attnotnull", "atthasdef", "attidentity",
			"attgenerated", "attisdropped", "attcollation",
			"attacl", "attoptions", "attfdwoptions", "attmissingval",
			"atthasmissing",
		}
		var rows [][]tuple.Datum
		for _, t := range tables {
			for _, c := range t.Columns {
				typLen := int32(4)
				if c.TypeOID == 25 {
					typLen = -1
				}
				rows = append(rows, []tuple.Datum{
					tuple.DInt32(t.OID), tuple.DText(c.Name),
					tuple.DInt32(c.TypeOID), tuple.DInt32(typLen),
					tuple.DInt32(int32(c.Num)), tuple.DInt32(-1),
					tuple.DText("f"), tuple.DText("f"), tuple.DText(""),
					tuple.DText(""), tuple.DText("f"), tuple.DInt32(0),
					tuple.DNull(), tuple.DNull(), tuple.DNull(),
					tuple.DNull(), tuple.DText("f"),
				})
			}
		}
		return &QueryResult{Columns: cols, Rows: rows, Message: fmt.Sprintf("SELECT %d", len(rows))}, true
	}

	// pg_index
	if strings.Contains(primaryFrom, "FROM PG_INDEX") {
		indexes := provider.ListIndexes()
		cols := []string{
			"tableoid", "oid", "indrelid", "indexname", "indexdef",
			"indkey", "indisclustered", "contype", "conname",
			"condeferrable", "condeferred", "contableoid", "conoid",
			"condef", "tablespace", "indreloptions", "indisreplident",
			"parentidx", "indnkeyatts", "indnatts", "indstatcols",
			"indstatvals", "indnullsnotdistinct",
		}
		var rows [][]tuple.Datum
		for _, idx := range indexes {
			method := idx.Method
			if method == "" {
				method = "btree"
			}
			indexdef := fmt.Sprintf("CREATE INDEX %s ON public.%s USING %s (%s)",
				idx.Name, idx.TableName, method, idx.ColName)
			rows = append(rows, []tuple.Datum{
				tuple.DInt32(2610), tuple.DInt32(idx.OID),
				tuple.DInt32(idx.TableOID), tuple.DText(idx.Name),
				tuple.DText(indexdef), tuple.DText("1"),
				tuple.DText("f"), tuple.DText(""), tuple.DText(""),
				tuple.DText(""), tuple.DText(""), tuple.DText(""),
				tuple.DText(""), tuple.DText(""), tuple.DText(""),
				tuple.DText(""), tuple.DText("f"), tuple.DInt32(0),
				tuple.DInt32(1), tuple.DInt32(1), tuple.DText(""),
				tuple.DText(""), tuple.DText("f"),
			})
		}
		return &QueryResult{Columns: cols, Rows: rows, Message: fmt.Sprintf("SELECT %d", len(rows))}, true
	}

	// pg_attribute dump query — uses unnest() with complex expressions.
	// Match on the full upper string since primaryFrom won't catch UNNEST.
	if strings.Contains(upper, "A.ATTRELID") && (strings.Contains(upper, "PG_ATTRIBUTE") || strings.Contains(upper, "PG_CATALOG.PG_ATTRIBUTE")) && strings.Contains(upper, "UNNEST") {
		tables := provider.ListTables()
		cols := []string{
			"attrelid", "attnum", "attname", "attstattarget", "attstorage",
			"typstorage", "attnotnull", "atthasdef", "attisdropped", "attlen",
			"attalign", "attislocal", "atttypname", "attoptions", "attcollation",
			"attfdwoptions", "attcompression", "attidentity", "attmissingval",
			"attgenerated",
		}
		var rows [][]tuple.Datum
		for _, t := range tables {
			for _, c := range t.Columns {
				typName := "integer"
				storage := "p"
				attLen := int32(4)
				attAlign := "i"
				if c.TypeOID == 25 { // text
					typName = "text"
					storage = "x"
					attLen = -1
					attAlign = "i"
				} else if c.TypeOID == 16 { // bool
					typName = "boolean"
					attLen = 1
					attAlign = "c"
				} else if c.TypeOID == 701 { // float8
					typName = "double precision"
					attLen = 8
					attAlign = "d"
				} else if c.TypeOID == 20 { // int8
					typName = "bigint"
					attLen = 8
					attAlign = "d"
				}
				rows = append(rows, []tuple.Datum{
					tuple.DInt32(t.OID),          // attrelid
					tuple.DInt32(int32(c.Num)),   // attnum
					tuple.DText(c.Name),          // attname
					tuple.DInt32(-1),             // attstattarget
					tuple.DText(storage),         // attstorage
					tuple.DText(storage),         // typstorage
					tuple.DText("f"),             // attnotnull
					tuple.DText("f"),             // atthasdef
					tuple.DText("f"),             // attisdropped
					tuple.DInt32(attLen),         // attlen
					tuple.DText(attAlign),        // attalign
					tuple.DText("t"),             // attislocal
					tuple.DText(typName),         // atttypname
					tuple.DText(""),              // attoptions
					tuple.DInt32(0),              // attcollation
					tuple.DText(""),              // attfdwoptions
					tuple.DText(""),              // attcompression
					tuple.DText(""),              // attidentity
					tuple.DText(""),              // attmissingval
					tuple.DText(""),              // attgenerated
				})
			}
		}
		return &QueryResult{Columns: cols, Rows: rows, Message: fmt.Sprintf("SELECT %d", len(rows))}, true
	}

	// pg_index dump query — uses unnest() with complex expressions.
	if strings.Contains(upper, "UNNEST") && strings.Contains(upper, "PG_INDEX") && strings.Contains(upper, "INDEXRELID") {
		indexes := provider.ListIndexes()
		cols := []string{
			"tableoid", "oid", "indrelid", "indexname", "indexdef",
			"indkey", "indisclustered", "contype", "conname",
			"condeferrable", "condeferred", "contableoid", "conoid",
			"condef", "tablespace", "indreloptions", "indisreplident",
			"parentidx", "indnkeyatts", "indnatts", "indstatcols",
			"indstatvals", "indnullsnotdistinct",
		}
		var rows [][]tuple.Datum
		for _, idx := range indexes {
			method := idx.Method
			if method == "" {
				method = "btree"
			}
			indexdef := fmt.Sprintf("CREATE INDEX %s ON public.%s USING %s (%s)",
				idx.Name, idx.TableName, method, idx.ColName)
			rows = append(rows, []tuple.Datum{
				tuple.DInt32(2610), tuple.DInt32(idx.OID),
				tuple.DInt32(idx.TableOID), tuple.DText(idx.Name),
				tuple.DText(indexdef), tuple.DText("1"),
				tuple.DText("f"), tuple.DText(""), tuple.DText(""),
				tuple.DText(""), tuple.DText(""), tuple.DText(""),
				tuple.DText(""), tuple.DText(""), tuple.DText(""),
				tuple.DText(""), tuple.DText("f"), tuple.DInt32(0),
				tuple.DInt32(1), tuple.DInt32(1), tuple.DText(""),
				tuple.DText(""), tuple.DText("f"),
			})
		}
		return &QueryResult{Columns: cols, Rows: rows, Message: fmt.Sprintf("SELECT %d", len(rows))}, true
	}

	// pg_roles — return the bootstrap superuser
	if strings.Contains(primaryFrom, "FROM PG_ROLES") {
		return &QueryResult{
			Columns: []string{"oid", "rolname"},
			Rows:    [][]tuple.Datum{{tuple.DInt32(10), tuple.DText("loladb")}},
			Message: "SELECT 1",
		}, true
	}

	return nil, false
}

// handleUnimplementedCatalog returns empty results with correct column
// names for pg_dump queries against catalog tables we don't implement.
func handleUnimplementedCatalog(primaryFrom string) (*QueryResult, bool) {
	type stub struct {
		keyword string
		cols    []string
	}
	// Match on "FROM <table>" to identify the primary table.
	stubs := []stub{
		{"FROM PG_PROC", []string{"tableoid", "oid", "proname", "prolang", "pronargs", "proargtypes", "prorettype", "proacl", "acldefault", "pronamespace", "proowner"}},
		{"FROM PG_EXTENSION", []string{"tableoid", "oid", "extname", "nspname", "extrelocatable", "extversion", "extconfig", "extcondition"}},
		{"FROM PG_COLLATION", []string{"tableoid", "oid", "collname", "collnamespace", "collowner", "collprovider", "collisdeterministic", "collencoding", "collcollate", "collctype", "colliculocale"}},
		{"FROM PG_CONVERSION", []string{"tableoid", "oid", "conname", "connamespace", "conowner"}},
		{"FROM PG_LANGUAGE", []string{"tableoid", "oid", "lanname", "lanowner", "lanispl", "lanpltrusted", "lanplcallfoid", "laninline", "lanvalidator", "lanacl", "acldefault"}},
		{"FROM PG_CAST", []string{"tableoid", "oid", "castsource", "casttarget", "castfunc", "castcontext", "castmethod"}},
		{"FROM PG_OPERATOR", []string{"tableoid", "oid", "oprname", "oprnamespace", "oprowner", "oprkind", "oprcanmerge", "oprcanhash", "oprleft", "oprright", "oprresult", "oprcom", "oprnegate", "oprcode", "oprrest", "oprjoin"}},
		{"FROM PG_OPCLASS", []string{"tableoid", "oid", "opcname", "opcnamespace", "opcowner"}},
		{"FROM PG_OPFAMILY", []string{"tableoid", "oid", "opfname", "opfnamespace", "opfowner"}},
		{"FROM PG_CONSTRAINT", []string{"tableoid", "oid", "conname", "connamespace", "contype", "condeferrable", "condeferred", "convalidated", "conrelid", "contypid", "conindid", "confrelid", "confupdtype", "confdeltype", "confmatchtype", "conislocal", "coninhcount", "connoinherit", "conkey", "confkey", "conpfeqop", "conppeqop", "conffeqop", "conexclop", "conbin"}},
		{"FROM PG_TRIGGER", []string{"tableoid", "oid", "tgrelid", "tgname", "tgfoid", "tgtype", "tgenabled", "tgisinternal", "tgconstrrelid", "tgconstrindid", "tgconstraint", "tgdeferrable", "tginitdeferred", "tgnargs", "tgattr", "tgargs", "tgqual", "tgoldtable", "tgnewtable", "tgparentid"}},
		{"FROM PG_REWRITE", []string{"tableoid", "oid", "rulename", "ev_class", "ev_type", "ev_enabled", "is_instead"}},
		{"FROM PG_POLICY", []string{"tableoid", "oid", "polrelid", "polname", "polcmd", "polpermissive", "polroles", "polqual", "polwithcheck"}},
		{"FROM PG_DEPEND", []string{"classid", "objid", "objsubid", "refclassid", "refobjid", "refobjsubid", "deptype"}},
		{"FROM PG_INHERITS", []string{"inhrelid", "inhparent", "inhseqno"}},
		{"FROM PG_EVENT_TRIGGER", []string{"tableoid", "oid", "evtname", "evtevent", "evtowner", "evtfoid", "evtenabled", "evttags"}},
		{"FROM PG_PUBLICATION_REL", []string{"tableoid", "oid", "prpubid", "prrelid", "prqual", "prattrs"}},
		{"FROM PG_PUBLICATION_NAMESPACE", []string{"tableoid", "oid", "pnpubid", "pnnspid"}},
		{"FROM PG_PUBLICATION", []string{"tableoid", "oid", "pubname", "pubowner", "puballtables", "pubinsert", "pubupdate", "pubdelete", "pubtruncate", "pubviaroot"}},
		{"FROM PG_SUBSCRIPTION", []string{"tableoid", "oid", "subname", "subowner", "subconninfo", "subslotname", "subsynccommit", "subpublications"}},
		{"FROM PG_STATISTIC_EXT", []string{"tableoid", "oid", "stxname", "stxnamespace", "stxowner"}},
		{"FROM PG_STATISTIC", []string{"tableoid", "oid"}},
		{"FROM PG_INIT_PRIVS", []string{"objoid", "classoid", "objsubid", "privtype", "initprivs"}},
		{"FROM PG_SHDESCRIPTION", []string{"objoid", "classoid", "description"}},
		{"FROM PG_DESCRIPTION", []string{"objoid", "classoid", "objsubid", "description"}},
		{"FROM PG_ATTRDEF", []string{"tableoid", "oid", "adrelid", "adnum", "adbin"}},
		{"FROM PG_LARGEOBJECT_METADATA", []string{"tableoid", "oid", "lomowner", "lomacl", "acldefault"}},
		{"FROM PG_LARGEOBJECT", []string{"oid", "lomowner", "lomacl", "acldefault"}},
		{"FROM PG_SECLABEL", []string{"objoid", "classoid", "objsubid", "provider", "label"}},
		{"FROM PG_DEFAULT_ACL", []string{"tableoid", "oid", "defaclrole", "defaclnamespace", "defaclobjtype", "defaclacl", "acldefault"}},
		{"FROM PG_RANGE", []string{"rngtypid", "rngsubtype", "rngcollation", "rngsubopc", "rngcanonical", "rngsubdiff"}},
		{"FROM PG_ENUM", []string{"tableoid", "oid", "enumtypid", "enumsortorder", "enumlabel"}},
		{"FROM PG_SEQUENCE", []string{"tableoid", "oid", "seqrelid", "seqtypid", "seqstart", "seqincrement", "seqmax", "seqmin", "seqcache", "seqcycle"}},
		{"FROM PG_SETTINGS", []string{"name", "setting"}},
		{"FROM PG_ROLES", []string{"oid", "rolname"}},
		{"FROM PG_TRANSFORM", []string{"tableoid", "oid", "trftype", "trflang", "trffromsql", "trftosql"}},
		{"FROM PG_TS_", []string{"tableoid", "oid"}},
		{"FROM PG_FDW", []string{"tableoid", "oid"}},
		{"FROM PG_FOREIGN", []string{"tableoid", "oid"}},
		{"FROM PG_AM", []string{"tableoid", "oid", "amname", "amtype", "amhandler"}},
		{"FROM PG_PARTITIONED_TABLE", []string{"partrelid", "partstrat", "partnatts", "partdefid", "partattrs", "partclass", "partcollation", "partexprs"}},
		{"FROM PG_AGGREGATE", []string{"tableoid", "oid", "aggfnoid", "aggkind", "aggnumdirectargs", "aggtransfn", "aggfinalfn", "aggcombinefn", "aggserialfn", "aggdeserialfn", "aggmtransfn", "aggminvtransfn", "aggmfinalfn", "aggsortop", "aggtranstype", "aggtransspace", "aggmtranstype", "aggmtransspace", "agginitval", "aggminitval", "aggfinalextra", "aggmfinalextra", "aggfinalmodify", "aggmfinalmodify"}},
		{"FROM PG_ACCESS_METHOD", []string{"tableoid", "oid", "amname", "amtype"}},
		{"FROM PG_USER_MAPPING", []string{"tableoid", "oid", "umuser", "umserver", "umoptions"}},
		{"FROM PG_TYPE", []string{"tableoid", "oid", "typname", "typnamespace", "typacl", "acldefault", "typowner", "typelem", "typrelid", "typarray", "typinput", "typoutput", "typtype", "typcategory", "typispreferred", "typisdefined", "typdelim", "typbasetype", "typtypmod", "typlen", "typcollation", "typdefaultbin", "typdefault"}},
	}
	for _, s := range stubs {
		if strings.Contains(primaryFrom, s.keyword) {
			return &QueryResult{
				Columns: s.cols,
				Rows:    [][]tuple.Datum{},
				Message: "SELECT 0",
			}, true
		}
	}
	// Catch-all: any UNNEST-based query (pg_dump uses these for per-table metadata).
	if strings.Contains(primaryFrom, "FROM UNNEST") {
		return &QueryResult{
			Columns: []string{"_"},
			Rows:    [][]tuple.Datum{},
			Message: "SELECT 0",
		}, true
	}
	// Catch-all: any FROM PG_ query we don't have a specific stub for.
	if strings.Contains(primaryFrom, "FROM PG_") {
		return &QueryResult{
			Columns: []string{"_"},
			Rows:    [][]tuple.Datum{},
			Message: "SELECT 0",
		}, true
	}
	return nil, false
}
