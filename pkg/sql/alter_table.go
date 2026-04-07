package sql

import (
	"fmt"
	"strings"

	"github.com/gololadb/gopgsql/parser"
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
