package sql

import (
	"fmt"
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

	case parser.AT_AttachPartition:
		pc, ok := cmd.Def.(*parser.PartitionCmd)
		if !ok || pc == nil {
			return &Result{Message: "ATTACH PARTITION: missing partition command"}, true
		}
		child := pc.Name.Relname
		pchild := boundSpecToPartitionChild(child, pc.Bound)
		if _, ok := ex.Cat.Partitions[tableName]; !ok {
			return &Result{Message: fmt.Sprintf("table %q is not partitioned", tableName)}, true
		}
		rel, _ := ex.Cat.FindRelation(child)
		if rel == nil {
			return &Result{Message: fmt.Sprintf("relation %q does not exist", child)}, true
		}
		if err := ex.Cat.AttachPartitionChild(tableName, pchild); err != nil {
			return &Result{Message: err.Error()}, true
		}
		return &Result{Message: "ALTER TABLE"}, true

	case parser.AT_DetachPartition:
		pc, ok := cmd.Def.(*parser.PartitionCmd)
		if !ok || pc == nil {
			return &Result{Message: "DETACH PARTITION: missing partition command"}, true
		}
		child := pc.Name.Relname
		if err := ex.Cat.DetachPartitionChild(tableName, child); err != nil {
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
	return nil, false
}

// execCreatePartitionOfParsed handles a parsed CREATE TABLE ... PARTITION OF statement.
func (ex *Executor) execCreatePartitionOfParsed(cs *parser.CreateStmt) (*Result, error) {
	childName := cs.Relation.Relname
	parentName := cs.PartitionOf.Relname

	// Verify parent is partitioned.
	if _, ok := ex.Cat.Partitions[parentName]; !ok {
		return nil, fmt.Errorf("table %q is not partitioned", parentName)
	}

	// Get parent's columns to create the child with the same schema.
	parentRel, _ := ex.Cat.FindRelation(parentName)
	if parentRel == nil {
		return nil, fmt.Errorf("relation %q does not exist", parentName)
	}
	parentCols, _ := ex.Cat.GetColumns(parentRel.OID)
	if len(parentCols) == 0 {
		return nil, fmt.Errorf("parent table %q has no columns", parentName)
	}

	// Build a CREATE TABLE statement for the child with the parent's columns.
	var colDefs []string
	for _, col := range parentCols {
		typeName := datumTypeToSQL(tuple.DatumType(col.Type))
		colDefs = append(colDefs, fmt.Sprintf("%s %s", col.Name, typeName))
	}
	createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", childName, strings.Join(colDefs, ", "))

	// Create the child table.
	_, err := ex.Exec(createSQL)
	if err != nil {
		return nil, fmt.Errorf("creating partition %q: %w", childName, err)
	}

	// Attach it to the parent.
	pc := boundSpecToPartitionChild(childName, cs.PartBound)
	if err := ex.Cat.AttachPartitionChild(parentName, pc); err != nil {
		return nil, err
	}

	return &Result{Message: fmt.Sprintf("CREATE TABLE %s", childName)}, nil
}

// boundSpecToPartitionChild converts a parser.PartitionBoundSpec to a catalog.PartitionChild.
func boundSpecToPartitionChild(childName string, bound *parser.PartitionBoundSpec) catalog.PartitionChild {
	pc := catalog.PartitionChild{TableName: childName}
	if bound == nil {
		return pc
	}
	if bound.IsDefault {
		pc.BoundType = "default"
		return pc
	}
	if bound.Strategy == "list" {
		pc.BoundType = "list"
		for _, v := range bound.ListValues {
			pc.ListValues = append(pc.ListValues, deparseAndTrimQuotes(v))
		}
	} else {
		pc.BoundType = "range"
		for _, v := range bound.LowerBound {
			pc.RangeFrom = append(pc.RangeFrom, deparseAndTrimQuotes(v))
		}
		for _, v := range bound.UpperBound {
			pc.RangeTo = append(pc.RangeTo, deparseAndTrimQuotes(v))
		}
	}
	return pc
}

// deparseAndTrimQuotes deparses an expression and strips surrounding single quotes.
func deparseAndTrimQuotes(e parser.Expr) string {
	s := parser.DeparseExpr(e)
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		s = s[1 : len(s)-1]
	}
	return s
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


