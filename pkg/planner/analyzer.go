package planner

import (
	"fmt"
	"strings"

	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"

	"github.com/jespino/loladb/pkg/catalog"
	"github.com/jespino/loladb/pkg/tuple"
)

// Analyzer converts a parsed AST into a logical plan, resolving
// names against the catalog.
type Analyzer struct {
	Cat *catalog.Catalog
}

// Analyze converts a single AST statement into a logical plan.
func (a *Analyzer) Analyze(stmt tree.Statement) (LogicalNode, error) {
	switch n := stmt.(type) {
	case *tree.Select:
		return a.analyzeSelect(n)
	case *tree.Insert:
		return a.analyzeInsert(n)
	case *tree.Delete:
		return a.analyzeDelete(n)
	case *tree.Update:
		return a.analyzeUpdate(n)
	case *tree.CreateTable:
		return a.analyzeCreateTable(n)
	case *tree.CreateIndex:
		return a.analyzeCreateIndex(n)
	case *tree.Explain:
		child, err := a.Analyze(n.Statement)
		if err != nil {
			return nil, err
		}
		return &LogicalExplain{Child: child}, nil
	case *tree.SetVar:
		// SET statements configure client parameters; no-op for us.
		return &LogicalNoOp{Message: fmt.Sprintf("SET %s", n.Name)}, nil
	case *tree.AlterTable:
		return a.analyzeAlterTable(n)
	case *tree.CreateSequence:
		seqName := string(n.Name.TableName)
		return &LogicalCreateSequence{Name: seqName}, nil
	case *tree.CreateView:
		viewName := string(n.Name.TableName)
		definition := n.AsSource.String()
		return &LogicalCreateView{Name: viewName, Definition: definition}, nil
	case *tree.Execute:
		// PL/pgSQL EXECUTE; no-op.
		return &LogicalNoOp{Message: "EXECUTE"}, nil
	default:
		return nil, fmt.Errorf("analyzer: unsupported statement %T", stmt)
	}
}

func (a *Analyzer) analyzeSelect(n *tree.Select) (LogicalNode, error) {
	clause, ok := n.Select.(*tree.SelectClause)
	if !ok {
		return nil, fmt.Errorf("analyzer: unsupported SELECT form")
	}

	// FROM clause → base scans and joins
	var plan LogicalNode
	if len(clause.From.Tables) > 0 {
		var err error
		plan, err = a.analyzeFrom(clause.From.Tables)
		if err != nil {
			return nil, err
		}
	} else {
		// SELECT without FROM (e.g., SELECT pg_catalog.set_config(...))
		// Return a no-op that produces an empty result.
		return &LogicalNoOp{Message: "SELECT"}, nil
	}

	// WHERE → Filter
	if clause.Where != nil {
		pred, err := a.analyzeExpr(clause.Where.Expr, plan.OutputColumns())
		if err != nil {
			return nil, fmt.Errorf("analyzer: WHERE: %w", err)
		}
		plan = &LogicalFilter{Predicate: pred, Child: plan}
	}

	// SELECT list → Project
	outCols := plan.OutputColumns()
	selectAll := false
	for _, e := range clause.Exprs {
		if _, ok := e.Expr.(tree.UnqualifiedStar); ok {
			selectAll = true
			break
		}
	}

	if !selectAll {
		var exprs []Expr
		var names []string
		for _, se := range clause.Exprs {
			expr, err := a.analyzeExpr(se.Expr, outCols)
			if err != nil {
				return nil, fmt.Errorf("analyzer: SELECT expr: %w", err)
			}
			name := se.Expr.String()
			if se.As != "" {
				name = string(se.As)
			} else if col, ok := expr.(*ExprColumn); ok {
				name = col.Column
			}
			exprs = append(exprs, expr)
			names = append(names, name)
		}
		plan = &LogicalProject{Exprs: exprs, Names: names, Child: plan}
	}

	// ORDER BY → Sort
	if len(n.OrderBy) > 0 {
		var keys []SortKey
		for _, o := range n.OrderBy {
			expr, err := a.analyzeExpr(o.Expr, outCols)
			if err != nil {
				return nil, err
			}
			keys = append(keys, SortKey{Expr: expr, Desc: o.Direction == tree.Descending})
		}
		plan = &LogicalSort{Keys: keys, Child: plan}
	}

	// LIMIT
	if n.Limit != nil {
		limit := &LogicalLimit{Count: -1, Child: plan}
		if n.Limit.Count != nil {
			if num, ok := n.Limit.Count.(*tree.NumVal); ok {
				var v int64
				fmt.Sscanf(num.OrigString(), "%d", &v)
				limit.Count = v
			}
		}
		if n.Limit.Offset != nil {
			if num, ok := n.Limit.Offset.(*tree.NumVal); ok {
				var v int64
				fmt.Sscanf(num.OrigString(), "%d", &v)
				limit.Offset = v
			}
		}
		plan = limit
	}

	return plan, nil
}

func (a *Analyzer) analyzeFrom(tables tree.TableExprs) (LogicalNode, error) {
	if len(tables) == 0 {
		return nil, fmt.Errorf("analyzer: empty FROM")
	}

	plan, err := a.analyzeTableExpr(tables[0])
	if err != nil {
		return nil, err
	}

	// Multiple tables in FROM → implicit CROSS JOIN
	for i := 1; i < len(tables); i++ {
		right, err := a.analyzeTableExpr(tables[i])
		if err != nil {
			return nil, err
		}
		plan = &LogicalJoin{Type: JoinCross, Left: plan, Right: right}
	}

	return plan, nil
}

func (a *Analyzer) analyzeTableExpr(expr tree.TableExpr) (LogicalNode, error) {
	switch t := expr.(type) {
	case *tree.AliasedTableExpr:
		scan, err := a.analyzeTableExpr(t.Expr)
		if err != nil {
			return nil, err
		}
		if t.As.Alias != "" {
			if s, ok := scan.(*LogicalScan); ok {
				s.Alias = string(t.As.Alias)
			}
		}
		return scan, nil
	case *tree.TableName:
		tableName := string(t.TableName)
		rel, err := a.Cat.FindRelation(tableName)
		if err != nil || rel == nil {
			return nil, fmt.Errorf("analyzer: table %q not found", tableName)
		}
		cols, err := a.Cat.GetColumns(rel.OID)
		if err != nil {
			return nil, err
		}
		colNames := make([]string, len(cols))
		for i, c := range cols {
			colNames[i] = c.Name
		}
		return &LogicalScan{Table: tableName, Alias: tableName, Columns: colNames}, nil
	case *tree.JoinTableExpr:
		return a.analyzeJoin(t)
	default:
		return nil, fmt.Errorf("analyzer: unsupported table expr %T", expr)
	}
}

func (a *Analyzer) analyzeJoin(j *tree.JoinTableExpr) (LogicalNode, error) {
	left, err := a.analyzeTableExpr(j.Left)
	if err != nil {
		return nil, err
	}
	right, err := a.analyzeTableExpr(j.Right)
	if err != nil {
		return nil, err
	}

	jtype := JoinInner
	switch strings.ToUpper(j.JoinType) {
	case "JOIN", "INNER JOIN", "INNER", "":
		jtype = JoinInner
	case "LEFT", "LEFT JOIN", "LEFT OUTER JOIN":
		jtype = JoinLeft
	case "RIGHT", "RIGHT JOIN", "RIGHT OUTER JOIN":
		jtype = JoinRight
	case "CROSS", "CROSS JOIN":
		jtype = JoinCross
	default:
		jtype = JoinInner
	}

	node := &LogicalJoin{Type: jtype, Left: left, Right: right}

	if j.Cond != nil {
		if on, ok := j.Cond.(*tree.OnJoinCond); ok {
			outCols := node.OutputColumns()
			cond, err := a.analyzeExpr(on.Expr, outCols)
			if err != nil {
				return nil, fmt.Errorf("analyzer: JOIN ON: %w", err)
			}
			node.Condition = cond
		}
	}

	return node, nil
}

func (a *Analyzer) analyzeExpr(expr tree.Expr, availCols []string) (Expr, error) {
	switch e := expr.(type) {
	case *tree.NumVal:
		s := e.OrigString()
		if !strings.Contains(s, ".") {
			var v int64
			fmt.Sscanf(s, "%d", &v)
			return &ExprLiteral{Value: tuple.DInt64(v)}, nil
		}
		var f float64
		fmt.Sscanf(s, "%f", &f)
		return &ExprLiteral{Value: tuple.DFloat64(f)}, nil
	case *tree.StrVal:
		return &ExprLiteral{Value: tuple.DText(e.RawString())}, nil
	case *tree.DBool:
		return &ExprLiteral{Value: tuple.DBool(bool(*e))}, nil
	case *tree.ComparisonExpr:
		return a.analyzeComparison(e, availCols)
	case *tree.AndExpr:
		left, err := a.analyzeExpr(e.Left, availCols)
		if err != nil {
			return nil, err
		}
		right, err := a.analyzeExpr(e.Right, availCols)
		if err != nil {
			return nil, err
		}
		return &ExprBinOp{Op: OpAnd, Left: left, Right: right}, nil
	case *tree.OrExpr:
		left, err := a.analyzeExpr(e.Left, availCols)
		if err != nil {
			return nil, err
		}
		right, err := a.analyzeExpr(e.Right, availCols)
		if err != nil {
			return nil, err
		}
		return &ExprBinOp{Op: OpOr, Left: left, Right: right}, nil
	case *tree.NotExpr:
		child, err := a.analyzeExpr(e.Expr, availCols)
		if err != nil {
			return nil, err
		}
		return &ExprNot{Child: child}, nil
	case *tree.ParenExpr:
		return a.analyzeExpr(e.Expr, availCols)
	case *tree.CastExpr:
		return a.analyzeExpr(e.Expr, availCols)
	case *tree.UnresolvedName:
		return a.resolveColumnName(e, availCols)
	case tree.UnqualifiedStar:
		return &ExprStar{}, nil
	default:
		if expr == tree.DNull {
			return &ExprLiteral{Value: tuple.DNull()}, nil
		}
		// Try treating it as a column name
		name := expr.String()
		name = strings.Trim(name, "\"")
		return a.resolveColumnByName(name, "", availCols)
	}
}

func (a *Analyzer) analyzeComparison(e *tree.ComparisonExpr, cols []string) (Expr, error) {
	left, err := a.analyzeExpr(e.Left, cols)
	if err != nil {
		return nil, err
	}
	right, err := a.analyzeExpr(e.Right, cols)
	if err != nil {
		return nil, err
	}

	var op OpKind
	switch e.Operator {
	case tree.EQ:
		op = OpEq
	case tree.NE:
		op = OpNeq
	case tree.LT:
		op = OpLt
	case tree.LE:
		op = OpLte
	case tree.GT:
		op = OpGt
	case tree.GE:
		op = OpGte
	default:
		return nil, fmt.Errorf("analyzer: unsupported operator %s", e.Operator)
	}

	return &ExprBinOp{Op: op, Left: left, Right: right}, nil
}

func (a *Analyzer) resolveColumnName(name *tree.UnresolvedName, availCols []string) (Expr, error) {
	// name.NumParts: 1 = "col", 2 = "table.col"
	switch name.NumParts {
	case 1:
		colName := name.Parts[0]
		return a.resolveColumnByName(colName, "", availCols)
	case 2:
		colName := name.Parts[0]
		tableName := name.Parts[1]
		return a.resolveColumnByName(colName, tableName, availCols)
	default:
		return nil, fmt.Errorf("analyzer: unsupported name with %d parts", name.NumParts)
	}
}

func (a *Analyzer) resolveColumnByName(colName, tableName string, availCols []string) (Expr, error) {
	colName = strings.Trim(colName, "\"")
	tableName = strings.Trim(tableName, "\"")

	for i, qname := range availCols {
		parts := strings.SplitN(qname, ".", 2)
		var tbl, col string
		if len(parts) == 2 {
			tbl = parts[0]
			col = parts[1]
		} else {
			col = parts[0]
		}

		if tableName != "" {
			if strings.EqualFold(tbl, tableName) && strings.EqualFold(col, colName) {
				return &ExprColumn{Table: tbl, Column: col, Index: i}, nil
			}
		} else {
			if strings.EqualFold(col, colName) {
				return &ExprColumn{Table: tbl, Column: col, Index: i}, nil
			}
		}
	}

	return nil, fmt.Errorf("analyzer: column %q not found (available: %v)", colName, availCols)
}

// --- DML analysis ---

func (a *Analyzer) analyzeInsert(n *tree.Insert) (LogicalNode, error) {
	tn := n.Table.(*tree.TableName)
	tableName := string(tn.TableName)

	rows, ok := n.Rows.Select.(*tree.ValuesClause)
	if !ok {
		return nil, fmt.Errorf("analyzer: unsupported INSERT source")
	}

	var values [][]Expr
	for _, row := range rows.Rows {
		var rowExprs []Expr
		for _, e := range row {
			expr, err := a.analyzeExpr(e, nil)
			if err != nil {
				return nil, err
			}
			rowExprs = append(rowExprs, expr)
		}
		values = append(values, rowExprs)
	}

	return &LogicalInsert{Table: tableName, Values: values}, nil
}

func (a *Analyzer) analyzeDelete(n *tree.Delete) (LogicalNode, error) {
	tableName := extractTableName(n.Table)
	rel, err := a.Cat.FindRelation(tableName)
	if err != nil || rel == nil {
		return nil, fmt.Errorf("analyzer: table %q not found", tableName)
	}
	cols, err := a.Cat.GetColumns(rel.OID)
	if err != nil {
		return nil, err
	}
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = tableName + "." + c.Name
	}

	scan := &LogicalScan{Table: tableName, Alias: tableName, Columns: colNamesUnqualified(cols)}
	var child LogicalNode = scan

	if n.Where != nil {
		pred, err := a.analyzeExpr(n.Where.Expr, colNames)
		if err != nil {
			return nil, err
		}
		child = &LogicalFilter{Predicate: pred, Child: child}
	}

	return &LogicalDelete{Table: tableName, Child: child}, nil
}

func (a *Analyzer) analyzeUpdate(n *tree.Update) (LogicalNode, error) {
	tableName := extractTableName(n.Table)
	rel, err := a.Cat.FindRelation(tableName)
	if err != nil || rel == nil {
		return nil, fmt.Errorf("analyzer: table %q not found", tableName)
	}
	cols, err := a.Cat.GetColumns(rel.OID)
	if err != nil {
		return nil, err
	}
	colNames := make([]string, len(cols))
	colNamesPlain := make([]string, len(cols))
	colTypes := make([]tuple.DatumType, len(cols))
	for i, c := range cols {
		colNames[i] = tableName + "." + c.Name
		colNamesPlain[i] = c.Name
		colTypes[i] = tuple.DatumType(c.Type)
	}

	scan := &LogicalScan{Table: tableName, Alias: tableName, Columns: colNamesUnqualified(cols)}
	var child LogicalNode = scan

	if n.Where != nil {
		pred, err := a.analyzeExpr(n.Where.Expr, colNames)
		if err != nil {
			return nil, err
		}
		child = &LogicalFilter{Predicate: pred, Child: child}
	}

	var assignments []Assignment
	for _, e := range n.Exprs {
		colName := e.Names[0].String()
		val, err := a.analyzeExpr(e.Expr, colNames)
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, Assignment{Column: colName, Value: val})
	}

	return &LogicalUpdate{
		Table:       tableName,
		Assignments: assignments,
		Child:       child,
		Columns:     colNamesPlain,
		ColTypes:    colTypes,
	}, nil
}

func (a *Analyzer) analyzeCreateTable(n *tree.CreateTable) (LogicalNode, error) {
	tableName := string(n.Table.TableName)
	var cols []ColDef
	for _, def := range n.Defs {
		colDef, ok := def.(*tree.ColumnTableDef)
		if !ok {
			continue
		}
		dt := mapSQLType(colDef.Type.SQLString())
		cols = append(cols, ColDef{Name: string(colDef.Name), Type: dt})
	}
	return &LogicalCreateTable{Table: tableName, Columns: cols}, nil
}

func (a *Analyzer) analyzeCreateIndex(n *tree.CreateIndex) (LogicalNode, error) {
	return &LogicalCreateIndex{
		Index:  string(n.Name),
		Table:  string(n.Table.TableName),
		Column: string(n.Columns[0].Column),
	}, nil
}

func (a *Analyzer) analyzeAlterTable(n *tree.AlterTable) (LogicalNode, error) {
	tableName := n.Table.Parts[0] // Parts[0] is the object name
	var commands []string
	for _, cmd := range n.Cmds {
		switch c := cmd.(type) {
		case *tree.AlterTableAddConstraint:
			commands = append(commands, fmt.Sprintf("ADD CONSTRAINT %s", c.ConstraintDef))
		default:
			commands = append(commands, fmt.Sprintf("%T", c))
		}
	}
	return &LogicalAlterTable{Table: tableName, Commands: commands}, nil
}

// --- Helpers ---

func extractTableName(expr tree.TableExpr) string {
	switch t := expr.(type) {
	case *tree.TableName:
		return string(t.TableName)
	case *tree.AliasedTableExpr:
		return extractTableName(t.Expr)
	default:
		return fmt.Sprintf("%s", expr)
	}
}

func colNamesUnqualified(cols []catalog.Column) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}

func mapSQLType(sqlType string) tuple.DatumType {
	upper := strings.ToUpper(strings.TrimSpace(sqlType))

	// Strip any array suffix (e.g., "TEXT[]", "INT[]").
	if strings.HasSuffix(upper, "[]") {
		return tuple.TypeText
	}

	// Strip parenthesized parameters for matching (e.g., "NUMERIC(10,2)" → "NUMERIC").
	base := upper
	if idx := strings.IndexByte(base, '('); idx >= 0 {
		base = strings.TrimSpace(base[:idx])
	}

	switch base {
	// Integer types
	case "INT2", "SMALLINT":
		return tuple.TypeInt32
	case "INT4":
		return tuple.TypeInt32
	case "INT8", "INT", "BIGINT", "INTEGER":
		return tuple.TypeInt64
	case "SERIAL":
		return tuple.TypeInt64
	case "BIGSERIAL":
		return tuple.TypeInt64

	// Floating point / numeric types
	case "FLOAT4", "REAL":
		return tuple.TypeFloat64
	case "FLOAT8", "DOUBLE PRECISION", "DOUBLE":
		return tuple.TypeFloat64
	case "NUMERIC", "DECIMAL":
		return tuple.TypeFloat64

	// Boolean
	case "BOOL", "BOOLEAN":
		return tuple.TypeBool

	// Text types
	case "TEXT", "VARCHAR", "CHAR", "CHARACTER", "CHARACTER VARYING", "STRING", "BPCHAR", "NAME":
		return tuple.TypeText

	// Date/time types — store as text for now
	case "TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP WITH TIME ZONE":
		return tuple.TypeText
	case "DATE":
		return tuple.TypeText
	case "TIME", "TIMETZ", "TIME WITHOUT TIME ZONE", "TIME WITH TIME ZONE":
		return tuple.TypeText
	case "INTERVAL":
		return tuple.TypeText

	// Binary
	case "BYTEA":
		return tuple.TypeText

	// Full-text search
	case "TSVECTOR", "TSQUERY":
		return tuple.TypeText
	}

	// Substring-based fallbacks for compound types.
	switch {
	case strings.Contains(upper, "INT4"):
		return tuple.TypeInt32
	case strings.Contains(upper, "INT"):
		return tuple.TypeInt64
	case strings.Contains(upper, "SERIAL"):
		return tuple.TypeInt64
	case strings.Contains(upper, "TEXT"), strings.Contains(upper, "VARCHAR"),
		strings.Contains(upper, "CHAR"), strings.Contains(upper, "STRING"):
		return tuple.TypeText
	case strings.Contains(upper, "BOOL"):
		return tuple.TypeBool
	case strings.Contains(upper, "FLOAT"), strings.Contains(upper, "DOUBLE"),
		strings.Contains(upper, "REAL"), strings.Contains(upper, "NUMERIC"),
		strings.Contains(upper, "DECIMAL"):
		return tuple.TypeFloat64
	case strings.Contains(upper, "TIMESTAMP"), strings.Contains(upper, "DATE"),
		strings.Contains(upper, "TIME"):
		return tuple.TypeText
	case strings.Contains(upper, "BYTEA"):
		return tuple.TypeText
	}

	// Any unrecognized type (custom domains like public.year, public.mpaa_rating, etc.)
	// → default to TEXT.
	return tuple.TypeText
}
