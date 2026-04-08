package sql

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/gololadb/gopgsql/parser"
	"github.com/gololadb/loladb/pkg/catalog"
	"github.com/gololadb/loladb/pkg/tuple"
)

// tryExecAlterTable handles ALTER TABLE sub-commands that modify columns.
// Returns (result, true) if handled, or (nil, false) to fall through to
// the analyzer path for commands like ADD COLUMN, DROP COLUMN, ADD CONSTRAINT.
func (ex *Executor) tryExecAlterTable(n *parser.AlterTableStmt) (*Result, bool) {
	tableName := n.Relation.Relname

	// Only handle single-command ALTER TABLE statements with column-level ops.
	if len(n.Cmds) != 1 {
		return nil, false
	}
	cmd := n.Cmds[0]

	switch cmd.Subtype {
	case parser.AT_SetNotNull:
		if err := ex.Cat.AlterColumnSetNotNull(tableName, cmd.Name); err != nil {
			return &Result{Message: err.Error()}, true
		}
		return &Result{Message: "ALTER TABLE"}, true

	case parser.AT_DropNotNull:
		if err := ex.Cat.AlterColumnDropNotNull(tableName, cmd.Name); err != nil {
			return &Result{Message: err.Error()}, true
		}
		return &Result{Message: "ALTER TABLE"}, true

	case parser.AT_SetDefault:
		defExpr := ""
		if cmd.Def != nil {
			defExpr = parser.Deparse(cmd.Def)
		}
		if err := ex.Cat.AlterColumnSetDefault(tableName, cmd.Name, defExpr); err != nil {
			return &Result{Message: err.Error()}, true
		}
		return &Result{Message: "ALTER TABLE"}, true

	case parser.AT_DropDefault:
		if err := ex.Cat.AlterColumnDropDefault(tableName, cmd.Name); err != nil {
			return &Result{Message: err.Error()}, true
		}
		return &Result{Message: "ALTER TABLE"}, true

	case parser.AT_ChangeOwner:
		newOwner := cmd.Name
		role, _ := ex.Cat.FindRole(newOwner)
		var ownerOID int32
		if role != nil {
			ownerOID = role.OID
		}
		if err := ex.Cat.ChangeRelationOwner(tableName, ownerOID); err != nil {
			return &Result{Message: err.Error()}, true
		}
		return &Result{Message: "ALTER TABLE"}, true

	case parser.AT_AlterColumnType:
		if cmd.Def == nil {
			return nil, false
		}
		// cmd.Def is a *ColumnDef with the new TypeName.
		colDef, ok := cmd.Def.(*parser.ColumnDef)
		if !ok || colDef.TypeName == nil {
			return nil, false
		}
		typeName := ""
		if len(colDef.TypeName.Names) > 0 {
			typeName = colDef.TypeName.Names[len(colDef.TypeName.Names)-1]
		}
		typeOID := resolveTypeOID(typeName)
		if typeOID == 0 {
			return &Result{Message: fmt.Sprintf("type %q not recognized", typeName)}, true
		}
		if err := ex.Cat.AlterColumnType(tableName, cmd.Name, typeOID); err != nil {
			return &Result{Message: err.Error()}, true
		}
		return &Result{Message: "ALTER TABLE"}, true

	default:
		return nil, false
	}
}

// execRenameStmt handles ALTER TABLE ... RENAME COLUMN / RENAME TO.
func (ex *Executor) execRenameStmt(rs *parser.RenameStmt) (*Result, error) {
	if rs.Relation == nil {
		return nil, fmt.Errorf("RENAME: missing relation")
	}
	tableName := rs.Relation.Relname

	switch rs.RenameType {
	case parser.OBJECT_COLUMN:
		if err := ex.Cat.RenameColumn(tableName, rs.Subname, rs.Newname); err != nil {
			return nil, err
		}
		return &Result{Message: "ALTER TABLE"}, nil
	case parser.OBJECT_TABLE:
		if err := ex.Cat.RenameRelation(tableName, rs.Newname); err != nil {
			return nil, err
		}
		return &Result{Message: "ALTER TABLE"}, nil
	case parser.OBJECT_INDEX:
		if err := ex.Cat.RenameRelation(tableName, rs.Newname); err != nil {
			return nil, err
		}
		return &Result{Message: "ALTER INDEX"}, nil
	default:
		return nil, fmt.Errorf("RENAME: unsupported object type")
	}
}

// tryPreParse handles SQL statements the parser doesn't support natively.
// Returns (result, true) if handled, or (nil, false) to fall through.
func (ex *Executor) tryPreParse(sql string) (*Result, bool) {
	upper := strings.ToUpper(strings.TrimSpace(sql))

	// CREATE TABLE child PARTITION OF parent FOR VALUES ...
	if strings.HasPrefix(upper, "CREATE TABLE") && strings.Contains(upper, "PARTITION OF") {
		r, err := ex.execCreatePartitionOf(sql)
		if err != nil {
			return &Result{Message: err.Error()}, true
		}
		return r, true
	}

	// ALTER TABLE parent ATTACH PARTITION child FOR VALUES ...
	if strings.Contains(upper, "ATTACH PARTITION") {
		r, err := ex.execAttachPartition(sql)
		if err != nil {
			return &Result{Message: err.Error()}, true
		}
		return r, true
	}

	// ALTER TABLE parent DETACH PARTITION child
	if strings.Contains(upper, "DETACH PARTITION") {
		r, err := ex.execDetachPartition(sql)
		if err != nil {
			return &Result{Message: err.Error()}, true
		}
		return r, true
	}

	// CLUSTER [table [USING index]] — accept as no-op.
	if reCluster.MatchString(upper) {
		return &Result{Message: "CLUSTER"}, true
	}

	// ALTER TABLE ONLY ... — strip ONLY and re-execute.
	if reAlterTableOnly.MatchString(sql) {
		stripped := reAlterTableOnly.ReplaceAllString(sql, "ALTER TABLE $1")
		r, err := ex.Exec(stripped)
		if err != nil {
			return &Result{Message: err.Error()}, true
		}
		return r, true
	}

	return nil, false
}

var reAlterTableOnly = regexp.MustCompile(`(?i)^ALTER\s+TABLE\s+ONLY\s+(.+)$`)
var reCluster = regexp.MustCompile(`(?i)^CLUSTER\b`)

// Regex patterns for ATTACH PARTITION.
var (
	reAttachList    = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(\S+)\s+ATTACH\s+PARTITION\s+(\S+)\s+FOR\s+VALUES\s+IN\s*\(([^)]+)\)`)
	reAttachRange   = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(\S+)\s+ATTACH\s+PARTITION\s+(\S+)\s+FOR\s+VALUES\s+FROM\s*\(([^)]+)\)\s+TO\s*\(([^)]+)\)`)
	reAttachDefault = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(\S+)\s+ATTACH\s+PARTITION\s+(\S+)\s+DEFAULT`)
	reDetach        = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(\S+)\s+DETACH\s+PARTITION\s+(\S+)`)
)

// Regex patterns for CREATE TABLE ... PARTITION OF.
var (
	reCreatePartList    = regexp.MustCompile(`(?i)^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)\s+PARTITION\s+OF\s+(\S+)\s+FOR\s+VALUES\s+IN\s*\(([^)]+)\)\s*;?\s*$`)
	reCreatePartRange   = regexp.MustCompile(`(?i)^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)\s+PARTITION\s+OF\s+(\S+)\s+FOR\s+VALUES\s+FROM\s*\(([^)]+)\)\s+TO\s*\(([^)]+)\)\s*;?\s*$`)
	reCreatePartDefault = regexp.MustCompile(`(?i)^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)\s+PARTITION\s+OF\s+(\S+)\s+DEFAULT\s*;?\s*$`)
)

// execCreatePartitionOf handles CREATE TABLE child PARTITION OF parent FOR VALUES ...
// It creates the child table with the parent's column definitions and attaches it.
func (ex *Executor) execCreatePartitionOf(sql string) (*Result, error) {
	var child, parent string
	var pc catalog.PartitionChild

	if m := reCreatePartList.FindStringSubmatch(sql); m != nil {
		child, parent = m[1], m[2]
		pc = catalog.PartitionChild{
			TableName:  child,
			BoundType:  "list",
			ListValues: splitTrimValues(m[3]),
		}
	} else if m := reCreatePartRange.FindStringSubmatch(sql); m != nil {
		child, parent = m[1], m[2]
		pc = catalog.PartitionChild{
			TableName: child,
			BoundType: "range",
			RangeFrom: splitTrimValues(m[3]),
			RangeTo:   splitTrimValues(m[4]),
		}
	} else if m := reCreatePartDefault.FindStringSubmatch(sql); m != nil {
		child, parent = m[1], m[2]
		pc = catalog.PartitionChild{
			TableName: child,
			BoundType: "default",
		}
	} else {
		return nil, fmt.Errorf("unsupported CREATE TABLE ... PARTITION OF syntax")
	}

	// Strip schema prefix for lookup.
	parentBase := parent
	if idx := strings.LastIndex(parentBase, "."); idx >= 0 {
		parentBase = parentBase[idx+1:]
	}
	childBase := child
	if idx := strings.LastIndex(childBase, "."); idx >= 0 {
		childBase = childBase[idx+1:]
	}

	// Verify parent is partitioned.
	pinfo, ok := ex.Cat.Partitions[parentBase]
	if !ok {
		return nil, fmt.Errorf("table %q is not partitioned", parent)
	}

	// Get parent's columns to create the child with the same schema.
	parentRel, _ := ex.Cat.FindRelation(parentBase)
	if parentRel == nil {
		return nil, fmt.Errorf("relation %q does not exist", parent)
	}
	parentCols, _ := ex.Cat.GetColumns(parentRel.OID)
	if len(parentCols) == 0 {
		return nil, fmt.Errorf("parent table %q has no columns", parent)
	}

	// Build a CREATE TABLE statement for the child with the parent's columns.
	var colDefs []string
	for _, col := range parentCols {
		typeName := datumTypeToSQL(tuple.DatumType(col.Type))
		colDefs = append(colDefs, fmt.Sprintf("%s %s", col.Name, typeName))
	}
	createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", childBase, strings.Join(colDefs, ", "))

	// Create the child table.
	_, err := ex.Exec(createSQL)
	if err != nil {
		return nil, fmt.Errorf("creating partition %q: %w", child, err)
	}

	// Attach it to the parent.
	pc.TableName = childBase
	pinfo.Children = append(pinfo.Children, pc)

	return &Result{Message: fmt.Sprintf("CREATE TABLE %s", child)}, nil
}

func (ex *Executor) execAttachPartition(sql string) (*Result, error) {
	// Try LIST: FOR VALUES IN (...)
	if m := reAttachList.FindStringSubmatch(sql); m != nil {
		parent, child := m[1], m[2]
		vals := splitTrimValues(m[3])
		return ex.attachPartitionChild(parent, child, catalog.PartitionChild{
			TableName:  child,
			BoundType:  "list",
			ListValues: vals,
		})
	}

	// Try RANGE: FOR VALUES FROM (...) TO (...)
	if m := reAttachRange.FindStringSubmatch(sql); m != nil {
		parent, child := m[1], m[2]
		fromVals := splitTrimValues(m[3])
		toVals := splitTrimValues(m[4])
		return ex.attachPartitionChild(parent, child, catalog.PartitionChild{
			TableName: child,
			BoundType: "range",
			RangeFrom: fromVals,
			RangeTo:   toVals,
		})
	}

	// Try DEFAULT
	if m := reAttachDefault.FindStringSubmatch(sql); m != nil {
		parent, child := m[1], m[2]
		return ex.attachPartitionChild(parent, child, catalog.PartitionChild{
			TableName: child,
			BoundType: "default",
		})
	}

	return nil, fmt.Errorf("unsupported ATTACH PARTITION syntax")
}

func (ex *Executor) attachPartitionChild(parent, child string, pc catalog.PartitionChild) (*Result, error) {
	pinfo, ok := ex.Cat.Partitions[parent]
	if !ok {
		return nil, fmt.Errorf("table %q is not partitioned", parent)
	}

	// Verify child table exists.
	rel, _ := ex.Cat.FindRelation(child)
	if rel == nil {
		return nil, fmt.Errorf("relation %q does not exist", child)
	}

	pinfo.Children = append(pinfo.Children, pc)
	return &Result{Message: "ALTER TABLE"}, nil
}

func (ex *Executor) execDetachPartition(sql string) (*Result, error) {
	m := reDetach.FindStringSubmatch(sql)
	if m == nil {
		return nil, fmt.Errorf("unsupported DETACH PARTITION syntax")
	}
	parent, child := m[1], m[2]

	pinfo, ok := ex.Cat.Partitions[parent]
	if !ok {
		return nil, fmt.Errorf("table %q is not partitioned", parent)
	}

	found := false
	for i, c := range pinfo.Children {
		if strings.EqualFold(c.TableName, child) {
			pinfo.Children = append(pinfo.Children[:i], pinfo.Children[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("relation %q is not a partition of %q", child, parent)
	}

	return &Result{Message: "ALTER TABLE"}, nil
}

// splitTrimValues splits a comma-separated list and trims whitespace and quotes.
func splitTrimValues(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, len(parts))
	for i, p := range parts {
		p = strings.TrimSpace(p)
		// Strip surrounding single quotes.
		if len(p) >= 2 && p[0] == '\'' && p[len(p)-1] == '\'' {
			p = p[1 : len(p)-1]
		}
		result[i] = p
	}
	return result
}

// execAlterOwnerStmt handles ALTER object_type name OWNER TO newowner.
// This covers functions, statistics, and other non-table objects.
func (ex *Executor) execAlterOwnerStmt(ao *parser.AlterOwnerStmt) (*Result, error) {
	objName := ""
	if len(ao.Object) > 0 {
		objName = ao.Object[len(ao.Object)-1]
	}

	switch ao.ObjectType {
	case parser.OBJECT_TABLE, parser.OBJECT_INDEX, parser.OBJECT_SEQUENCE:
		role, _ := ex.Cat.FindRole(ao.NewOwner)
		var ownerOID int32
		if role != nil {
			ownerOID = role.OID
		}
		if err := ex.Cat.ChangeRelationOwner(objName, ownerOID); err != nil {
			return nil, err
		}
		return &Result{Message: "ALTER TABLE"}, nil
	case parser.OBJECT_FUNCTION:
		// Accept silently — function ownership is not enforced.
		return &Result{Message: "ALTER FUNCTION"}, nil
	default:
		// Accept other OWNER TO statements silently.
		return &Result{Message: "ALTER OWNER"}, nil
	}
}

// resolveTypeOID maps common SQL type names to internal type OIDs.
func resolveTypeOID(name string) int32 {
	switch strings.ToLower(name) {
	case "int", "int4", "integer":
		return 23 // INT4OID
	case "bigint", "int8":
		return 20 // INT8OID
	case "smallint", "int2":
		return 21 // INT2OID
	case "text", "varchar", "character varying":
		return 25 // TEXTOID
	case "bool", "boolean":
		return 16 // BOOLOID
	case "float4", "real":
		return 700
	case "float8", "double precision":
		return 701
	case "numeric", "decimal":
		return 1700
	case "json", "jsonb":
		return 114
	case "uuid":
		return 2950
	case "bytea":
		return 17
	case "date":
		return 1082
	case "timestamp", "timestamp without time zone":
		return 1114
	default:
		return 0
	}
}

// datumTypeToSQL maps a DatumType back to a SQL type name for CREATE TABLE.
func datumTypeToSQL(dt tuple.DatumType) string {
	switch dt {
	case tuple.TypeBool:
		return "BOOLEAN"
	case tuple.TypeInt32:
		return "INT4"
	case tuple.TypeInt64:
		return "INT8"
	case tuple.TypeFloat64:
		return "FLOAT8"
	case tuple.TypeText:
		return "TEXT"
	case tuple.TypeTimestamp:
		return "TIMESTAMP"
	case tuple.TypeNumeric:
		return "NUMERIC"
	case tuple.TypeJSON:
		return "JSONB"
	case tuple.TypeBytea:
		return "BYTEA"
	case tuple.TypeUUID:
		return "UUID"
	case tuple.TypeDate:
		return "DATE"
	case tuple.TypeInterval:
		return "INTERVAL"
	case tuple.TypeArray:
		return "TEXT[]"
	default:
		return "TEXT"
	}
}


