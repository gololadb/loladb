package executor

import (
	"fmt"
	"strings"

	"github.com/gololadb/loladb/pkg/catalog"
	"github.com/gololadb/loladb/pkg/tuple"
)

// virtualCatalogTable checks if the table name refers to a virtual catalog
// table (information_schema.* or pg_catalog.*) and returns the result if so.
// Returns nil if the table is not a virtual catalog table.
func (ex *Executor) virtualCatalogTable(tableName string, alias string) *Result {
	lower := strings.ToLower(tableName)

	switch lower {
	case "information_schema.tables":
		return ex.infoSchemaTables(alias)
	case "information_schema.columns":
		return ex.infoSchemaColumns(alias)
	case "information_schema.schemata":
		return ex.infoSchemaSchemata(alias)
	case "pg_tables", "pg_catalog.pg_tables":
		return ex.pgTables(alias)
	case "pg_indexes", "pg_catalog.pg_indexes":
		return ex.pgIndexes(alias)
	case "pg_views", "pg_catalog.pg_views":
		return ex.pgViews(alias)
	case "pg_roles", "pg_catalog.pg_roles":
		return ex.pgRoles(alias)
	case "pg_stat_user_tables", "pg_catalog.pg_stat_user_tables":
		return ex.pgStatUserTables(alias)
	case "pg_namespace", "pg_catalog.pg_namespace":
		return ex.pgNamespace(alias)
	case "pg_stat_statements", "pg_catalog.pg_stat_statements":
		return ex.pgStatStatements(alias)
	default:
		return nil
	}
}

// schemaNameByOID returns the schema name for a given namespace OID.
func (ex *Executor) schemaNameByOID(nsOID int32) string {
	schemas, err := ex.Cat.ListSchemas()
	if err != nil {
		return "unknown"
	}
	for _, s := range schemas {
		if s.OID == nsOID {
			return s.Name
		}
	}
	return "unknown"
}

// datumTypeToSQL returns the SQL type name for a datum type.
func datumTypeToSQL(dt int32) string {
	switch tuple.DatumType(dt) {
	case tuple.TypeInt32:
		return "integer"
	case tuple.TypeInt64:
		return "bigint"
	case tuple.TypeFloat64:
		return "double precision"
	case tuple.TypeText:
		return "text"
	case tuple.TypeBool:
		return "boolean"
	case tuple.TypeDate:
		return "date"
	case tuple.TypeTimestamp:
		return "timestamp without time zone"
	case tuple.TypeNumeric:
		return "numeric"
	case tuple.TypeJSON:
		return "json"
	case tuple.TypeUUID:
		return "uuid"
	case tuple.TypeInterval:
		return "interval"
	case tuple.TypeBytea:
		return "bytea"
	default:
		return "USER-DEFINED"
	}
}

// qualifyColumns prefixes column names with the alias.
func qualifyColumns(cols []string, alias string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = alias + "." + c
	}
	return out
}

// ---------------------------------------------------------------------------
// information_schema.tables
// ---------------------------------------------------------------------------

func (ex *Executor) infoSchemaTables(alias string) *Result {
	if alias == "" {
		alias = "information_schema.tables"
	}
	cols := []string{
		"table_catalog", "table_schema", "table_name", "table_type",
	}

	rels, err := ex.Cat.ListAllRelations()
	if err != nil {
		return &Result{Columns: qualifyColumns(cols, alias), Message: "SELECT 0"}
	}

	var rows [][]tuple.Datum
	for _, rel := range rels {
		schema := ex.schemaNameByOID(rel.NamespaceOID)
		// Skip system catalog tables.
		if schema == "pg_catalog" {
			continue
		}
		tableType := "BASE TABLE"
		if rel.Kind == catalog.RelKindView {
			tableType = "VIEW"
		} else if rel.Kind == catalog.RelKindIndex {
			continue // indexes are not tables
		}
		rows = append(rows, []tuple.Datum{
			tuple.DText("loladb"),
			tuple.DText(schema),
			tuple.DText(rel.Name),
			tuple.DText(tableType),
		})
	}

	return &Result{
		Columns: qualifyColumns(cols, alias),
		Rows:    rows,
		Message: fmt.Sprintf("SELECT %d", len(rows)),
	}
}

// ---------------------------------------------------------------------------
// information_schema.columns
// ---------------------------------------------------------------------------

func (ex *Executor) infoSchemaColumns(alias string) *Result {
	if alias == "" {
		alias = "information_schema.columns"
	}
	cols := []string{
		"table_catalog", "table_schema", "table_name",
		"column_name", "ordinal_position", "column_default",
		"is_nullable", "data_type",
	}

	rels, err := ex.Cat.ListAllRelations()
	if err != nil {
		return &Result{Columns: qualifyColumns(cols, alias), Message: "SELECT 0"}
	}

	var rows [][]tuple.Datum
	for _, rel := range rels {
		schema := ex.schemaNameByOID(rel.NamespaceOID)
		if schema == "pg_catalog" {
			continue
		}
		if rel.Kind == catalog.RelKindIndex {
			continue
		}
		tableCols, err := ex.Cat.GetColumns(rel.OID)
		if err != nil {
			continue
		}
		for _, col := range tableCols {
			nullable := "YES"
			if col.NotNull {
				nullable = "NO"
			}
			defExpr := tuple.DNull()
			if col.DefaultExpr != "" {
				defExpr = tuple.DText(col.DefaultExpr)
			}
			rows = append(rows, []tuple.Datum{
				tuple.DText("loladb"),
				tuple.DText(schema),
				tuple.DText(rel.Name),
				tuple.DText(col.Name),
				tuple.DInt64(int64(col.Num)),
				defExpr,
				tuple.DText(nullable),
				tuple.DText(datumTypeToSQL(col.Type)),
			})
		}
	}

	return &Result{
		Columns: qualifyColumns(cols, alias),
		Rows:    rows,
		Message: fmt.Sprintf("SELECT %d", len(rows)),
	}
}

// ---------------------------------------------------------------------------
// information_schema.schemata
// ---------------------------------------------------------------------------

func (ex *Executor) infoSchemaSchemata(alias string) *Result {
	if alias == "" {
		alias = "information_schema.schemata"
	}
	cols := []string{"catalog_name", "schema_name", "schema_owner"}

	schemas, err := ex.Cat.ListSchemas()
	if err != nil {
		return &Result{Columns: qualifyColumns(cols, alias), Message: "SELECT 0"}
	}

	var rows [][]tuple.Datum
	for _, s := range schemas {
		rows = append(rows, []tuple.Datum{
			tuple.DText("loladb"),
			tuple.DText(s.Name),
			tuple.DText("loladb"), // owner placeholder
		})
	}

	return &Result{
		Columns: qualifyColumns(cols, alias),
		Rows:    rows,
		Message: fmt.Sprintf("SELECT %d", len(rows)),
	}
}

// ---------------------------------------------------------------------------
// pg_tables
// ---------------------------------------------------------------------------

func (ex *Executor) pgTables(alias string) *Result {
	if alias == "" {
		alias = "pg_tables"
	}
	cols := []string{"schemaname", "tablename", "tableowner", "hasindexes", "hasrules", "hastriggers"}

	rels, err := ex.Cat.ListAllRelations()
	if err != nil {
		return &Result{Columns: qualifyColumns(cols, alias), Message: "SELECT 0"}
	}

	var rows [][]tuple.Datum
	for _, rel := range rels {
		if rel.Kind != catalog.RelKindTable {
			continue
		}
		schema := ex.schemaNameByOID(rel.NamespaceOID)
		if schema == "pg_catalog" {
			continue
		}
		rows = append(rows, []tuple.Datum{
			tuple.DText(schema),
			tuple.DText(rel.Name),
			tuple.DText("loladb"),
			tuple.DBool(false),
			tuple.DBool(false),
			tuple.DBool(false),
		})
	}

	return &Result{
		Columns: qualifyColumns(cols, alias),
		Rows:    rows,
		Message: fmt.Sprintf("SELECT %d", len(rows)),
	}
}

// ---------------------------------------------------------------------------
// pg_indexes
// ---------------------------------------------------------------------------

func (ex *Executor) pgIndexes(alias string) *Result {
	if alias == "" {
		alias = "pg_indexes"
	}
	cols := []string{"schemaname", "tablename", "indexname", "indexdef"}

	rels, err := ex.Cat.ListAllRelations()
	if err != nil {
		return &Result{Columns: qualifyColumns(cols, alias), Message: "SELECT 0"}
	}

	// Build a map of OID → relation for table lookups.
	relByOID := make(map[int32]*catalog.Relation)
	for i := range rels {
		relByOID[rels[i].OID] = &rels[i]
	}

	var rows [][]tuple.Datum
	for _, rel := range rels {
		if rel.Kind != catalog.RelKindIndex {
			continue
		}
		schema := ex.schemaNameByOID(rel.NamespaceOID)
		if schema == "pg_catalog" {
			continue
		}
		// Find the table this index belongs to via getIndexesForTable.
		// For simplicity, use the index name to infer the table name.
		tableName := ""
		idxInfo := ex.findIndexInfo(rel.OID)
		if idxInfo != nil {
			if tbl, ok := relByOID[idxInfo.TableOID]; ok {
				tableName = tbl.Name
			}
		}
		rows = append(rows, []tuple.Datum{
			tuple.DText(schema),
			tuple.DText(tableName),
			tuple.DText(rel.Name),
			tuple.DText(""), // indexdef placeholder
		})
	}

	return &Result{
		Columns: qualifyColumns(cols, alias),
		Rows:    rows,
		Message: fmt.Sprintf("SELECT %d", len(rows)),
	}
}

// findIndexInfo looks up IndexInfo for an index by its OID.
func (ex *Executor) findIndexInfo(indexOID int32) *catalog.IndexInfo {
	// Scan all tables to find which one owns this index.
	rels, err := ex.Cat.ListAllRelations()
	if err != nil {
		return nil
	}
	for _, rel := range rels {
		if rel.Kind != catalog.RelKindTable {
			continue
		}
		indexes, err := ex.Cat.GetTableIndexes(rel.OID)
		if err != nil {
			continue
		}
		for _, idx := range indexes {
			if idx.OID == indexOID {
				return &idx
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// pg_views
// ---------------------------------------------------------------------------

func (ex *Executor) pgViews(alias string) *Result {
	if alias == "" {
		alias = "pg_views"
	}
	cols := []string{"schemaname", "viewname", "viewowner", "definition"}

	rels, err := ex.Cat.ListAllRelations()
	if err != nil {
		return &Result{Columns: qualifyColumns(cols, alias), Message: "SELECT 0"}
	}

	var rows [][]tuple.Datum
	for _, rel := range rels {
		if rel.Kind != catalog.RelKindView {
			continue
		}
		schema := ex.schemaNameByOID(rel.NamespaceOID)
		if schema == "pg_catalog" {
			continue
		}
		rows = append(rows, []tuple.Datum{
			tuple.DText(schema),
			tuple.DText(rel.Name),
			tuple.DText("loladb"),
			tuple.DText(""), // definition placeholder
		})
	}

	return &Result{
		Columns: qualifyColumns(cols, alias),
		Rows:    rows,
		Message: fmt.Sprintf("SELECT %d", len(rows)),
	}
}

// ---------------------------------------------------------------------------
// pg_roles
// ---------------------------------------------------------------------------

func (ex *Executor) pgRoles(alias string) *Result {
	if alias == "" {
		alias = "pg_roles"
	}
	cols := []string{
		"rolname", "rolsuper", "rolinherit", "rolcreaterole",
		"rolcreatedb", "rolcanlogin", "rolbypassrls", "rolconnlimit",
	}

	roles, err := ex.Cat.ListRoles()
	if err != nil {
		return &Result{Columns: qualifyColumns(cols, alias), Message: "SELECT 0"}
	}

	var rows [][]tuple.Datum
	for _, r := range roles {
		rows = append(rows, []tuple.Datum{
			tuple.DText(r.Name),
			tuple.DBool(r.SuperUser),
			tuple.DBool(r.Inherit),
			tuple.DBool(r.CreateRole),
			tuple.DBool(r.CreateDB),
			tuple.DBool(r.Login),
			tuple.DBool(r.BypassRLS),
			tuple.DInt64(int64(r.ConnLimit)),
		})
	}

	return &Result{
		Columns: qualifyColumns(cols, alias),
		Rows:    rows,
		Message: fmt.Sprintf("SELECT %d", len(rows)),
	}
}

// ---------------------------------------------------------------------------
// pg_stat_user_tables
// ---------------------------------------------------------------------------

func (ex *Executor) pgStatUserTables(alias string) *Result {
	if alias == "" {
		alias = "pg_stat_user_tables"
	}
	cols := []string{
		"relid", "schemaname", "relname",
		"seq_scan", "seq_tup_read", "n_live_tup",
	}

	rels, err := ex.Cat.ListAllRelations()
	if err != nil {
		return &Result{Columns: qualifyColumns(cols, alias), Message: "SELECT 0"}
	}

	var rows [][]tuple.Datum
	for _, rel := range rels {
		if rel.Kind != catalog.RelKindTable {
			continue
		}
		schema := ex.schemaNameByOID(rel.NamespaceOID)
		if schema == "pg_catalog" {
			continue
		}
		rows = append(rows, []tuple.Datum{
			tuple.DInt64(int64(rel.OID)),
			tuple.DText(schema),
			tuple.DText(rel.Name),
			tuple.DInt64(0), // seq_scan placeholder
			tuple.DInt64(0), // seq_tup_read placeholder
			tuple.DInt64(0), // n_live_tup placeholder
		})
	}

	return &Result{
		Columns: qualifyColumns(cols, alias),
		Rows:    rows,
		Message: fmt.Sprintf("SELECT %d", len(rows)),
	}
}

// ---------------------------------------------------------------------------
// pg_namespace
// ---------------------------------------------------------------------------

func (ex *Executor) pgNamespace(alias string) *Result {
	if alias == "" {
		alias = "pg_namespace"
	}
	cols := []string{"oid", "nspname", "nspowner"}

	schemas, err := ex.Cat.ListSchemas()
	if err != nil {
		return &Result{Columns: qualifyColumns(cols, alias), Message: "SELECT 0"}
	}

	var rows [][]tuple.Datum
	for _, s := range schemas {
		rows = append(rows, []tuple.Datum{
			tuple.DInt64(int64(s.OID)),
			tuple.DText(s.Name),
			tuple.DInt64(int64(s.OwnerOID)),
		})
	}

	return &Result{
		Columns: qualifyColumns(cols, alias),
		Rows:    rows,
		Message: fmt.Sprintf("SELECT %d", len(rows)),
	}
}



// ---------------------------------------------------------------------------
// pg_stat_statements
// ---------------------------------------------------------------------------

func (ex *Executor) pgStatStatements(alias string) *Result {
	if alias == "" {
		alias = "pg_stat_statements"
	}
	cols := []string{"userid", "dbid", "query", "calls", "total_time", "rows"}

	var rows [][]tuple.Datum
	for query, qs := range ex.Cat.QueryStats {
		rows = append(rows, []tuple.Datum{
			tuple.DInt64(0),
			tuple.DInt64(0),
			tuple.DText(query),
			tuple.DInt64(qs.Calls),
			tuple.DFloat64(0),
			tuple.DInt64(qs.Rows),
		})
	}

	return &Result{
		Columns: qualifyColumns(cols, alias),
		Rows:    rows,
		Message: fmt.Sprintf("SELECT %d", len(rows)),
	}
}
