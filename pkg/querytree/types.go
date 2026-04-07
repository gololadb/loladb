package querytree

import "github.com/gololadb/loladb/pkg/tuple"

// JoinType identifies the kind of join.
type JoinType int

const (
	JoinInner JoinType = iota
	JoinLeft
	JoinRight
	JoinCross
	JoinFull
)

func (j JoinType) String() string {
	switch j {
	case JoinInner:
		return "INNER"
	case JoinLeft:
		return "LEFT"
	case JoinRight:
		return "RIGHT"
	case JoinCross:
		return "CROSS"
	case JoinFull:
		return "FULL"
	default:
		return "?"
	}
}

// ColDef describes a column in a CREATE TABLE statement.
type ColDef struct {
	Name          string
	Type          tuple.DatumType
	TypeName      string // original SQL type name (for domain/enum validation)
	Typmod        int32  // type modifier (-1 = unspecified; for NUMERIC: ((p<<16)|s)+4)
	NotNull       bool
	PrimaryKey    bool
	Unique        bool
	DefaultExpr   string // SQL text of DEFAULT expression
	CheckExpr     string // SQL text of CHECK expression
	CheckName     string // optional constraint name for CHECK
	GeneratedExpr string // SQL text of GENERATED ALWAYS AS (expr) STORED
}

// ForeignKeyDef holds a foreign key definition from CREATE TABLE.
type ForeignKeyDef struct {
	Name       string
	Columns    []string
	RefTable   string
	RefColumns []string
	OnDelete   string // "", "CASCADE", "SET NULL", "SET DEFAULT", "RESTRICT"
	OnUpdate   string
}
