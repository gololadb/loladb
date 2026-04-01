package planner

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jespino/gopgsql/parser"

	"github.com/jespino/loladb/pkg/catalog"
	"github.com/jespino/loladb/pkg/tuple"
)

// Analyzer performs semantic analysis on a raw parse tree, resolving
// names and types against the catalog to produce a Query tree.
//
// This mirrors PostgreSQL's parse_analyze() / transformStmt() pipeline
// (src/backend/parser/analyze.c). The key responsibilities are:
//
//  1. Build the range table (resolve table names → catalog OIDs)
//  2. Resolve column references to (RTIndex, ColNum) pairs
//  3. Type-check and coerce expressions
//  4. Build the join tree structure
//  5. Resolve the target list
type Analyzer struct {
	Cat *catalog.Catalog

	// Per-query state, reset for each Analyze call.
	rangeTable []*RangeTblEntry
}

// Analyze transforms a raw parse tree statement into a Query.
// This is the main entry point, equivalent to PostgreSQL's
// parse_analyze().
func (a *Analyzer) Analyze(stmt parser.Stmt) (*Query, error) {
	// Reset per-query state.
	a.rangeTable = nil

	switch n := stmt.(type) {
	case *parser.SelectStmt:
		return a.transformSelectStmt(n)
	case *parser.InsertStmt:
		return a.transformInsertStmt(n)
	case *parser.DeleteStmt:
		return a.transformDeleteStmt(n)
	case *parser.UpdateStmt:
		return a.transformUpdateStmt(n)
	case *parser.CreateStmt:
		return a.transformCreateStmt(n)
	case *parser.IndexStmt:
		return a.transformIndexStmt(n)
	case *parser.ExplainStmt:
		return a.transformExplainStmt(n)
	case *parser.VariableSetStmt:
		return a.makeUtilityQuery(UtilNoOp, &UtilityStmt{
			Type: UtilNoOp, Message: fmt.Sprintf("SET %s", n.Name),
		}), nil
	case *parser.AlterTableStmt:
		return a.transformAlterTableStmt(n)
	case *parser.CreateSeqStmt:
		seqName := lastNamePart(n.Name)
		return a.makeUtilityQuery(UtilCreateSequence, &UtilityStmt{
			Type: UtilCreateSequence, SeqName: seqName,
		}), nil
	case *parser.ViewStmt:
		return a.transformViewStmt(n)
	case *parser.CreatePolicyStmt:
		return a.transformCreatePolicyStmt(n)
	case *parser.ExecuteStmt:
		return a.makeUtilityQuery(UtilNoOp, &UtilityStmt{
			Type: UtilNoOp, Message: "EXECUTE",
		}), nil
	case *parser.CreateRoleStmt:
		return a.transformCreateRoleStmt(n)
	case *parser.AlterRoleStmt:
		return a.transformAlterRoleStmt(n)
	case *parser.DropRoleStmt:
		return a.transformDropRoleStmt(n)
	case *parser.GrantRoleStmt:
		return a.transformGrantRoleStmt(n)
	case *parser.GrantStmt:
		return a.transformGrantStmt(n)
	default:
		return nil, fmt.Errorf("analyzer: unsupported statement %T", stmt)
	}
}

// --- Range table management ---
// Mirrors PostgreSQL's addRangeTableEntry() in parse_relation.c.

// addRangeTableEntry resolves a table name against the catalog and
// adds it to the range table. Returns the 1-based index.
func (a *Analyzer) addRangeTableEntry(tableName, alias string) (*RangeTblEntry, error) {
	rel, err := a.Cat.FindRelation(tableName)
	if err != nil || rel == nil {
		return nil, fmt.Errorf("analyzer: relation %q does not exist", tableName)
	}
	cols, err := a.Cat.GetColumns(rel.OID)
	if err != nil {
		return nil, fmt.Errorf("analyzer: cannot get columns for %q: %w", tableName, err)
	}

	rteCols := make([]RTEColumn, len(cols))
	for i, c := range cols {
		rteCols[i] = RTEColumn{
			Name:   c.Name,
			Type:   tuple.DatumType(c.Type),
			ColNum: c.Num,
		}
	}

	if alias == "" {
		alias = tableName
	}

	rte := &RangeTblEntry{
		RTIndex:  len(a.rangeTable) + 1, // 1-based
		RelOID:   rel.OID,
		RelName:  tableName,
		Alias:    alias,
		Columns:  rteCols,
		HeadPage: rel.HeadPage,
	}
	a.rangeTable = append(a.rangeTable, rte)
	return rte, nil
}

// flattenedColumns returns all columns from the current range table
// as "alias.colname" strings, used for resolving unqualified names.
func (a *Analyzer) flattenedColumns() []string {
	var cols []string
	for _, rte := range a.rangeTable {
		for _, c := range rte.Columns {
			cols = append(cols, rte.Alias+"."+c.Name)
		}
	}
	return cols
}

// --- SELECT transformation ---
// Mirrors PostgreSQL's transformSelectStmt() in analyze.c.

func (a *Analyzer) transformSelectStmt(n *parser.SelectStmt) (*Query, error) {
	if len(n.ValuesLists) > 0 {
		return nil, fmt.Errorf("analyzer: bare VALUES clause not supported")
	}

	q := &Query{CommandType: CmdSelect}

	// Step 1: Process FROM clause → build range table and join tree.
	// Mirrors transformFromClause().
	fromList, err := a.transformFromClause(n.FromClause)
	if err != nil {
		return nil, err
	}
	if len(fromList) == 0 {
		// SELECT without FROM (e.g., SELECT 1).
		q.Utility = &UtilityStmt{Type: UtilNoOp, Message: "SELECT"}
		q.CommandType = CmdUtility
		q.RangeTable = a.rangeTable
		return q, nil
	}

	// Step 2: Transform WHERE clause.
	// Mirrors transformWhereClause().
	var qual AnalyzedExpr
	if n.WhereClause != nil {
		qual, err = a.transformExpr(n.WhereClause)
		if err != nil {
			return nil, fmt.Errorf("analyzer: WHERE: %w", err)
		}
	}

	q.JoinTree = &FromExpr{FromList: fromList, Quals: qual}

	// Step 3: Transform target list (SELECT expressions).
	// Mirrors transformTargetList().
	q.TargetList, err = a.transformTargetList(n.TargetList)
	if err != nil {
		return nil, err
	}

	// Step 4: Transform ORDER BY.
	// Mirrors transformSortClause().
	if len(n.SortClause) > 0 {
		for _, sb := range n.SortClause {
			expr, err := a.transformExpr(sb.Node)
			if err != nil {
				return nil, err
			}
			q.SortClause = append(q.SortClause, &SortClause{
				Expr: expr,
				Desc: sb.SortbyDir == parser.SORTBY_DESC,
			})
		}
	}

	// Step 5: Transform LIMIT/OFFSET.
	if n.LimitCount != nil {
		q.LimitCount, err = a.transformExpr(n.LimitCount)
		if err != nil {
			return nil, err
		}
	}
	if n.LimitOffset != nil {
		q.LimitOffset, err = a.transformExpr(n.LimitOffset)
		if err != nil {
			return nil, err
		}
	}

	q.RangeTable = a.rangeTable
	return q, nil
}

// transformFromClause processes the FROM clause items, adding range
// table entries and building join tree nodes.
// Mirrors PostgreSQL's transformFromClauseItem().
func (a *Analyzer) transformFromClause(items []parser.Node) ([]JoinTreeNode, error) {
	var result []JoinTreeNode
	for _, item := range items {
		node, err := a.transformFromItem(item)
		if err != nil {
			return nil, err
		}
		result = append(result, node)
	}
	return result, nil
}

func (a *Analyzer) transformFromItem(item parser.Node) (JoinTreeNode, error) {
	switch t := item.(type) {
	case *parser.RangeVar:
		alias := t.Relname
		if t.Alias != nil && t.Alias.Aliasname != "" {
			alias = t.Alias.Aliasname
		}
		rte, err := a.addRangeTableEntry(t.Relname, alias)
		if err != nil {
			return nil, err
		}
		return &RangeTblRef{RTIndex: rte.RTIndex}, nil

	case *parser.JoinExpr:
		return a.transformJoinExpr(t)

	default:
		return nil, fmt.Errorf("analyzer: unsupported FROM item %T", item)
	}
}

// transformJoinExpr processes an explicit JOIN, mirroring
// PostgreSQL's transformJoinOnClause().
func (a *Analyzer) transformJoinExpr(j *parser.JoinExpr) (JoinTreeNode, error) {
	left, err := a.transformFromItem(j.Larg)
	if err != nil {
		return nil, err
	}
	right, err := a.transformFromItem(j.Rarg)
	if err != nil {
		return nil, err
	}

	jtype := JoinInner
	switch j.Jointype {
	case parser.JOIN_INNER:
		jtype = JoinInner
	case parser.JOIN_LEFT:
		jtype = JoinLeft
	case parser.JOIN_RIGHT:
		jtype = JoinRight
	case parser.JOIN_CROSS:
		jtype = JoinCross
	}

	node := &JoinNode{
		JoinType: jtype,
		Left:     left,
		Right:    right,
		LeftRTI:  extractRTI(left),
		RightRTI: extractRTI(right),
	}

	if j.Quals != nil {
		node.Quals, err = a.transformExpr(j.Quals)
		if err != nil {
			return nil, fmt.Errorf("analyzer: JOIN ON: %w", err)
		}
	}

	return node, nil
}

func extractRTI(n JoinTreeNode) int {
	if ref, ok := n.(*RangeTblRef); ok {
		return ref.RTIndex
	}
	return 0
}

// transformTargetList resolves the SELECT target list.
// Mirrors PostgreSQL's transformTargetList().
func (a *Analyzer) transformTargetList(targets []*parser.ResTarget) ([]*TargetEntry, error) {
	var result []*TargetEntry
	resNo := 1

	for _, rt := range targets {
		// Check for SELECT *.
		if isStarTarget(rt) {
			// Expand * into individual columns from all RTEs.
			for _, rte := range a.rangeTable {
				for i, col := range rte.Columns {
					te := &TargetEntry{
						Expr: &ColumnVar{
							RTIndex:  rte.RTIndex,
							ColNum:   col.ColNum,
							ColName:  col.Name,
							Table:    rte.Alias,
							VarType:  col.Type,
							AttIndex: a.computeAttIndex(rte.RTIndex, int32(i+1)),
						},
						Name:  col.Name,
						ResNo: resNo,
					}
					result = append(result, te)
					resNo++
				}
			}
			continue
		}

		expr, err := a.transformExpr(rt.Val)
		if err != nil {
			return nil, fmt.Errorf("analyzer: SELECT expr: %w", err)
		}

		name := exprString(rt.Val)
		if rt.Name != "" {
			name = rt.Name
		} else if cv, ok := expr.(*ColumnVar); ok {
			name = cv.ColName
		}

		result = append(result, &TargetEntry{
			Expr:  expr,
			Name:  name,
			ResNo: resNo,
		})
		resNo++
	}

	return result, nil
}

// computeAttIndex computes the 0-based flattened column index for a
// given (RTIndex, colNum) pair across all range table entries.
func (a *Analyzer) computeAttIndex(rtIndex int, colNum int32) int {
	idx := 0
	for _, rte := range a.rangeTable {
		if rte.RTIndex == rtIndex {
			return idx + int(colNum) - 1
		}
		idx += len(rte.Columns)
	}
	return -1
}

// --- Expression transformation ---
// Mirrors PostgreSQL's transformExpr() in parse_expr.c.

func (a *Analyzer) transformExpr(expr parser.Expr) (AnalyzedExpr, error) {
	switch e := expr.(type) {
	case *parser.A_Const:
		return a.transformConst(e), nil
	case *parser.ColumnRef:
		return a.transformColumnRef(e)
	case *parser.A_Expr:
		return a.transformAExpr(e)
	case *parser.BoolExpr:
		return a.transformBoolExpr(e)
	case *parser.TypeCast:
		// Pass through to inner expression (type coercion not yet implemented).
		return a.transformExpr(e.Arg)
	case *parser.NullTest:
		return a.transformNullTest(e)
	case *parser.SQLValueFunction:
		// Represent current_user / session_user etc. as a ColumnVar with
		// a sentinel name so the rewriter can substitute the actual value.
		switch e.Op {
		case parser.SVFOP_CURRENT_USER, parser.SVFOP_CURRENT_ROLE, parser.SVFOP_USER, parser.SVFOP_SESSION_USER:
			return &ColumnVar{
				RTIndex: 0,
				ColNum:  0,
				ColName: "current_user",
				Table:   "",
				VarType: tuple.TypeText,
			}, nil
		default:
			return nil, fmt.Errorf("analyzer: unsupported SQL value function (op %d)", e.Op)
		}
	case *parser.ParamRef:
		return nil, fmt.Errorf("analyzer: parameter references ($%d) not supported", e.Number)
	default:
		if expr == nil {
			return &Const{Value: tuple.DNull(), ConstType: tuple.TypeNull}, nil
		}
		// Fallback: try to interpret as a column name.
		name := fmt.Sprintf("%v", expr)
		name = strings.Trim(name, "\"")
		return a.resolveColumnByName(name, "")
	}
}

// transformConst converts a parser constant to a typed Const node.
// Mirrors PostgreSQL's make_const() in parse_node.c.
func (a *Analyzer) transformConst(c *parser.A_Const) AnalyzedExpr {
	switch c.Val.Type {
	case parser.ValInt:
		return &Const{Value: tuple.DInt64(c.Val.Ival), ConstType: tuple.TypeInt64}
	case parser.ValFloat:
		f, err := strconv.ParseFloat(c.Val.Str, 64)
		if err != nil {
			return &Const{Value: tuple.DText(c.Val.Str), ConstType: tuple.TypeText}
		}
		return &Const{Value: tuple.DFloat64(f), ConstType: tuple.TypeFloat64}
	case parser.ValStr:
		// gopgsql represents boolean literals true/false as ValStr "t"/"f".
		if c.Val.Str == "t" {
			return &Const{Value: tuple.DBool(true), ConstType: tuple.TypeBool}
		}
		if c.Val.Str == "f" {
			return &Const{Value: tuple.DBool(false), ConstType: tuple.TypeBool}
		}
		return &Const{Value: tuple.DText(c.Val.Str), ConstType: tuple.TypeText}
	case parser.ValNull:
		return &Const{Value: tuple.DNull(), ConstType: tuple.TypeNull}
	default:
		return &Const{Value: tuple.DText(c.Val.Str), ConstType: tuple.TypeText}
	}
}

// transformColumnRef resolves a column reference against the range table.
// Mirrors PostgreSQL's transformColumnRef() in parse_expr.c, which
// calls colNameToVar() in parse_relation.c.
func (a *Analyzer) transformColumnRef(ref *parser.ColumnRef) (AnalyzedExpr, error) {
	if len(ref.Fields) == 1 {
		if _, ok := ref.Fields[0].(*parser.A_Star); ok {
			return &StarExpr{}, nil
		}
		if s, ok := ref.Fields[0].(*parser.String); ok {
			return a.resolveColumnByName(s.Str, "")
		}
	}
	if len(ref.Fields) == 2 {
		tableName := ""
		colName := ""
		if s, ok := ref.Fields[0].(*parser.String); ok {
			tableName = s.Str
		}
		if s, ok := ref.Fields[1].(*parser.String); ok {
			colName = s.Str
		} else if _, ok := ref.Fields[1].(*parser.A_Star); ok {
			return &StarExpr{}, nil
		}
		return a.resolveColumnByName(colName, tableName)
	}
	return nil, fmt.Errorf("analyzer: unsupported column ref with %d parts", len(ref.Fields))
}

// resolveColumnByName searches the range table for a matching column,
// mirroring PostgreSQL's colNameToVar() / scanRTEForColumn().
func (a *Analyzer) resolveColumnByName(colName, tableName string) (AnalyzedExpr, error) {
	colName = strings.Trim(colName, "\"")
	tableName = strings.Trim(tableName, "\"")

	for _, rte := range a.rangeTable {
		if tableName != "" && !strings.EqualFold(rte.Alias, tableName) {
			continue
		}
		for i, col := range rte.Columns {
			if strings.EqualFold(col.Name, colName) {
				return &ColumnVar{
					RTIndex:  rte.RTIndex,
					ColNum:   col.ColNum,
					ColName:  col.Name,
					Table:    rte.Alias,
					VarType:  col.Type,
					AttIndex: a.computeAttIndex(rte.RTIndex, int32(i+1)),
				}, nil
			}
		}
	}

	return nil, fmt.Errorf("analyzer: column %q not found", colName)
}

// transformAExpr resolves an operator expression.
// Mirrors PostgreSQL's transformAExprOp() in parse_expr.c.
func (a *Analyzer) transformAExpr(e *parser.A_Expr) (AnalyzedExpr, error) {
	// Unary prefix operator.
	if e.Lexpr == nil {
		right, err := a.transformExpr(e.Rexpr)
		if err != nil {
			return nil, err
		}
		if len(e.Name) > 0 && e.Name[len(e.Name)-1] == "-" {
			if c, ok := right.(*Const); ok {
				switch c.ConstType {
				case tuple.TypeInt64:
					return &Const{Value: tuple.DInt64(-c.Value.I64), ConstType: tuple.TypeInt64}, nil
				case tuple.TypeInt32:
					return &Const{Value: tuple.DInt32(-c.Value.I32), ConstType: tuple.TypeInt32}, nil
				case tuple.TypeFloat64:
					return &Const{Value: tuple.DFloat64(-c.Value.F64), ConstType: tuple.TypeFloat64}, nil
				}
			}
			return right, nil
		}
		return right, nil
	}

	left, err := a.transformExpr(e.Lexpr)
	if err != nil {
		return nil, err
	}
	right, err := a.transformExpr(e.Rexpr)
	if err != nil {
		return nil, err
	}

	opName := ""
	if len(e.Name) > 0 {
		opName = e.Name[len(e.Name)-1]
	}

	var op OpKind
	switch opName {
	case "=":
		op = OpEq
	case "<>", "!=":
		op = OpNeq
	case "<":
		op = OpLt
	case "<=":
		op = OpLte
	case ">":
		op = OpGt
	case ">=":
		op = OpGte
	default:
		return nil, fmt.Errorf("analyzer: unsupported operator %q", opName)
	}

	return &OpExpr{Op: op, Left: left, Right: right, ResultTyp: tuple.TypeBool}, nil
}

// transformBoolExpr resolves AND/OR/NOT expressions.
// Mirrors PostgreSQL's transformBoolExpr() in parse_expr.c.
func (a *Analyzer) transformBoolExpr(e *parser.BoolExpr) (AnalyzedExpr, error) {
	var args []AnalyzedExpr
	for _, arg := range e.Args {
		resolved, err := a.transformExpr(arg)
		if err != nil {
			return nil, err
		}
		args = append(args, resolved)
	}

	switch e.Op {
	case parser.AND_EXPR:
		return &BoolExprNode{Op: BoolAnd, Args: args}, nil
	case parser.OR_EXPR:
		return &BoolExprNode{Op: BoolOr, Args: args}, nil
	case parser.NOT_EXPR:
		return &BoolExprNode{Op: BoolNot, Args: args}, nil
	default:
		return nil, fmt.Errorf("analyzer: unsupported bool expr type %d", e.Op)
	}
}

// transformNullTest resolves IS [NOT] NULL.
// Mirrors PostgreSQL's transformNullTest() in parse_expr.c.
func (a *Analyzer) transformNullTest(e *parser.NullTest) (AnalyzedExpr, error) {
	arg, err := a.transformExpr(e.Arg)
	if err != nil {
		return nil, err
	}
	return &NullTestExpr{
		Arg:   arg,
		IsNot: e.NullTestType == parser.IS_NOT_NULL,
	}, nil
}

// --- DML transformations ---

// transformInsertStmt resolves an INSERT statement.
// Mirrors PostgreSQL's transformInsertStmt() in analyze.c.
func (a *Analyzer) transformInsertStmt(n *parser.InsertStmt) (*Query, error) {
	q := &Query{CommandType: CmdInsert}

	// Add the target relation to the range table.
	rte, err := a.addRangeTableEntry(n.Relation.Relname, "")
	if err != nil {
		return nil, err
	}
	q.ResultRelation = rte.RTIndex

	// Resolve VALUES.
	sel, ok := n.SelectStmt.(*parser.SelectStmt)
	if !ok || len(sel.ValuesLists) == 0 {
		return nil, fmt.Errorf("analyzer: unsupported INSERT source")
	}

	for _, row := range sel.ValuesLists {
		var resolvedRow []AnalyzedExpr
		for _, e := range row {
			expr, err := a.transformExpr(e)
			if err != nil {
				return nil, err
			}
			resolvedRow = append(resolvedRow, expr)
		}
		q.Values = append(q.Values, resolvedRow)
	}

	q.RangeTable = a.rangeTable
	return q, nil
}

// transformDeleteStmt resolves a DELETE statement.
// Mirrors PostgreSQL's transformDeleteStmt() in analyze.c.
func (a *Analyzer) transformDeleteStmt(n *parser.DeleteStmt) (*Query, error) {
	q := &Query{CommandType: CmdDelete}

	rte, err := a.addRangeTableEntry(n.Relation.Relname, "")
	if err != nil {
		return nil, err
	}
	q.ResultRelation = rte.RTIndex

	// Build join tree with the target table.
	fromList := []JoinTreeNode{&RangeTblRef{RTIndex: rte.RTIndex}}

	var qual AnalyzedExpr
	if n.WhereClause != nil {
		qual, err = a.transformExpr(n.WhereClause)
		if err != nil {
			return nil, err
		}
	}

	q.JoinTree = &FromExpr{FromList: fromList, Quals: qual}
	q.RangeTable = a.rangeTable
	return q, nil
}

// transformUpdateStmt resolves an UPDATE statement.
// Mirrors PostgreSQL's transformUpdateStmt() in analyze.c.
func (a *Analyzer) transformUpdateStmt(n *parser.UpdateStmt) (*Query, error) {
	q := &Query{CommandType: CmdUpdate}

	rte, err := a.addRangeTableEntry(n.Relation.Relname, "")
	if err != nil {
		return nil, err
	}
	q.ResultRelation = rte.RTIndex

	// Build join tree.
	fromList := []JoinTreeNode{&RangeTblRef{RTIndex: rte.RTIndex}}

	var qual AnalyzedExpr
	if n.WhereClause != nil {
		qual, err = a.transformExpr(n.WhereClause)
		if err != nil {
			return nil, err
		}
	}

	q.JoinTree = &FromExpr{FromList: fromList, Quals: qual}

	// Resolve SET assignments.
	// Mirrors transformUpdateTargetList() in analyze.c.
	for _, rt := range n.TargetList {
		colName := rt.Name
		// Find the column in the target relation.
		var colNum int32
		var colType tuple.DatumType
		found := false
		for _, col := range rte.Columns {
			if strings.EqualFold(col.Name, colName) {
				colNum = col.ColNum
				colType = col.Type
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("analyzer: column %q of relation %q does not exist", colName, rte.RelName)
		}

		val, err := a.transformExpr(rt.Val)
		if err != nil {
			return nil, err
		}

		q.Assignments = append(q.Assignments, &UpdateAssignment{
			ColName: colName,
			ColNum:  colNum,
			ColType: colType,
			Expr:    val,
		})
	}

	q.RangeTable = a.rangeTable
	return q, nil
}

// --- Utility statement transformations ---

func (a *Analyzer) transformCreateStmt(n *parser.CreateStmt) (*Query, error) {
	tableName := n.Relation.Relname
	var cols []ColDef
	for _, elt := range n.TableElts {
		colDef, ok := elt.(*parser.ColumnDef)
		if !ok {
			continue
		}
		dt := mapSQLType(typeNameToString(colDef.TypeName))
		cols = append(cols, ColDef{Name: colDef.Colname, Type: dt})
	}
	return a.makeUtilityQuery(UtilCreateTable, &UtilityStmt{
		Type: UtilCreateTable, TableName: tableName, Columns: cols,
	}), nil
}

func (a *Analyzer) transformIndexStmt(n *parser.IndexStmt) (*Query, error) {
	colName := ""
	if len(n.IndexParams) > 0 {
		colName = n.IndexParams[0].Name
	}
	method := n.AccessMethod
	if method == "" {
		method = "btree" // default, same as PostgreSQL
	}
	return a.makeUtilityQuery(UtilCreateIndex, &UtilityStmt{
		Type: UtilCreateIndex, IndexName: n.Idxname,
		IndexTable: n.Relation.Relname, IndexColumn: colName,
		IndexMethod: method,
	}), nil
}

func (a *Analyzer) transformExplainStmt(n *parser.ExplainStmt) (*Query, error) {
	// EXPLAIN wraps another query. Analyze the inner statement.
	inner, err := a.Analyze(n.Query)
	if err != nil {
		return nil, err
	}
	// Return the inner query; the caller (Exec) handles EXPLAIN.
	return inner, nil
}

func (a *Analyzer) transformAlterTableStmt(n *parser.AlterTableStmt) (*Query, error) {
	tableName := n.Relation.Relname

	// Check for RLS enable/disable commands.
	for _, cmd := range n.Cmds {
		switch cmd.Subtype {
		case parser.AT_EnableRowSecurity:
			return a.makeUtilityQuery(UtilEnableRLS, &UtilityStmt{
				Type: UtilEnableRLS, TableName: tableName,
			}), nil
		case parser.AT_DisableRowSecurity:
			return a.makeUtilityQuery(UtilDisableRLS, &UtilityStmt{
				Type: UtilDisableRLS, TableName: tableName,
			}), nil
		}
	}

	var commands []string
	for _, cmd := range n.Cmds {
		switch cmd.Subtype {
		case parser.AT_AddConstraint:
			commands = append(commands, fmt.Sprintf("ADD CONSTRAINT %v", cmd.Def))
		default:
			commands = append(commands, fmt.Sprintf("%v", cmd.Subtype))
		}
	}
	return a.makeUtilityQuery(UtilAlterTable, &UtilityStmt{
		Type: UtilAlterTable, TableName: tableName, AlterCmds: commands,
	}), nil
}

func (a *Analyzer) transformViewStmt(n *parser.ViewStmt) (*Query, error) {
	viewName := n.View.Relname

	// Analyze the view's defining query to resolve its output columns.
	// We create a temporary analyzer to avoid polluting our range table.
	viewAnalyzer := &Analyzer{Cat: a.Cat}
	viewQuery, err := viewAnalyzer.transformSelectStmt(n.Query.(*parser.SelectStmt))
	if err != nil {
		return nil, fmt.Errorf("analyzer: view %q definition: %w", viewName, err)
	}

	// Extract column definitions from the view query's target list.
	var viewCols []ColDef
	for _, te := range viewQuery.TargetList {
		viewCols = append(viewCols, ColDef{
			Name: te.Name,
			Type: te.Expr.ResultType(),
		})
	}

	return a.makeUtilityQuery(UtilCreateView, &UtilityStmt{
		Type: UtilCreateView, ViewName: viewName,
		ViewColumns: viewCols,
	}), nil
}

func (a *Analyzer) makeUtilityQuery(_ UtilityType, util *UtilityStmt) *Query {
	return &Query{
		CommandType: CmdUtility,
		Utility:     util,
		RangeTable:  a.rangeTable,
	}
}

// --- Helpers ---

func isStarTarget(rt *parser.ResTarget) bool {
	if ref, ok := rt.Val.(*parser.ColumnRef); ok {
		for _, f := range ref.Fields {
			if _, ok := f.(*parser.A_Star); ok {
				return true
			}
		}
	}
	return false
}

func exprString(expr parser.Expr) string {
	switch e := expr.(type) {
	case *parser.ColumnRef:
		var parts []string
		for _, f := range e.Fields {
			if s, ok := f.(*parser.String); ok {
				parts = append(parts, s.Str)
			} else if _, ok := f.(*parser.A_Star); ok {
				parts = append(parts, "*")
			}
		}
		return strings.Join(parts, ".")
	case *parser.A_Const:
		switch e.Val.Type {
		case parser.ValInt:
			return strconv.FormatInt(e.Val.Ival, 10)
		case parser.ValStr:
			return "'" + e.Val.Str + "'"
		default:
			return e.Val.Str
		}
	default:
		return fmt.Sprintf("%v", expr)
	}
}

func typeNameToString(tn *parser.TypeName) string {
	if tn == nil {
		return "TEXT"
	}
	if len(tn.Names) > 0 {
		return tn.Names[len(tn.Names)-1]
	}
	return "TEXT"
}

func lastNamePart(names []string) string {
	if len(names) == 0 {
		return ""
	}
	return names[len(names)-1]
}

func mapSQLType(sqlType string) tuple.DatumType {
	upper := strings.ToUpper(strings.TrimSpace(sqlType))

	if strings.HasSuffix(upper, "[]") {
		return tuple.TypeText
	}

	base := upper
	if idx := strings.IndexByte(base, '('); idx >= 0 {
		base = strings.TrimSpace(base[:idx])
	}

	switch base {
	case "INT2", "SMALLINT":
		return tuple.TypeInt32
	case "INT4":
		return tuple.TypeInt32
	case "INT8", "INT", "BIGINT", "INTEGER":
		return tuple.TypeInt64
	case "SERIAL", "BIGSERIAL":
		return tuple.TypeInt64
	case "FLOAT4", "REAL":
		return tuple.TypeFloat64
	case "FLOAT8", "DOUBLE PRECISION", "DOUBLE":
		return tuple.TypeFloat64
	case "NUMERIC", "DECIMAL":
		return tuple.TypeFloat64
	case "BOOL", "BOOLEAN":
		return tuple.TypeBool
	case "TEXT", "VARCHAR", "CHAR", "CHARACTER", "CHARACTER VARYING",
		"STRING", "BPCHAR", "NAME":
		return tuple.TypeText
	case "TIMESTAMP", "TIMESTAMPTZ",
		"TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP WITH TIME ZONE":
		return tuple.TypeText
	case "DATE":
		return tuple.TypeText
	case "TIME", "TIMETZ", "TIME WITHOUT TIME ZONE", "TIME WITH TIME ZONE":
		return tuple.TypeText
	case "INTERVAL":
		return tuple.TypeText
	case "BYTEA":
		return tuple.TypeText
	case "TSVECTOR", "TSQUERY":
		return tuple.TypeText
	}

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

	return tuple.TypeText
}

func (a *Analyzer) transformCreatePolicyStmt(n *parser.CreatePolicyStmt) (*Query, error) {
	tableName := ""
	if len(n.Table) > 0 {
		tableName = n.Table[len(n.Table)-1]
	}

	cmdName := "ALL"
	if n.CmdName != "" {
		cmdName = strings.ToUpper(n.CmdName)
	}

	var roles []string
	for _, r := range n.Roles {
		roles = append(roles, r)
	}

	usingExpr := ""
	if n.Qual != nil {
		usingExpr = exprToSQL(n.Qual)
	}

	checkExpr := ""
	if n.WithCheck != nil {
		checkExpr = exprToSQL(n.WithCheck)
	}

	return a.makeUtilityQuery(UtilCreatePolicy, &UtilityStmt{
		Type:             UtilCreatePolicy,
		PolicyName:       n.PolicyName,
		PolicyTable:      tableName,
		PolicyCmd:        cmdName,
		PolicyPermissive: n.Permissive,
		PolicyRoles:      roles,
		PolicyUsing:      usingExpr,
		PolicyCheck:      checkExpr,
	}), nil
}

// exprToSQL reconstructs a SQL expression string from a parse tree node.
func exprToSQL(expr parser.Expr) string {
	switch e := expr.(type) {
	case *parser.A_Const:
		switch e.Val.Type {
		case parser.ValInt:
			return strconv.FormatInt(e.Val.Ival, 10)
		case parser.ValStr:
			if e.Val.Str == "t" {
				return "true"
			}
			if e.Val.Str == "f" {
				return "false"
			}
			return "'" + e.Val.Str + "'"
		case parser.ValFloat:
			return e.Val.Str
		case parser.ValNull:
			return "NULL"
		default:
			return e.Val.Str
		}
	case *parser.ColumnRef:
		var parts []string
		for _, f := range e.Fields {
			if s, ok := f.(*parser.String); ok {
				parts = append(parts, s.Str)
			} else if _, ok := f.(*parser.A_Star); ok {
				parts = append(parts, "*")
			}
		}
		return strings.Join(parts, ".")
	case *parser.A_Expr:
		opName := ""
		if len(e.Name) > 0 {
			opName = e.Name[len(e.Name)-1]
		}
		if e.Lexpr == nil {
			return opName + exprToSQL(e.Rexpr)
		}
		return exprToSQL(e.Lexpr) + " " + opName + " " + exprToSQL(e.Rexpr)
	case *parser.BoolExpr:
		switch e.Op {
		case parser.AND_EXPR:
			parts := make([]string, len(e.Args))
			for i, arg := range e.Args {
				parts[i] = exprToSQL(arg)
			}
			return "(" + strings.Join(parts, " AND ") + ")"
		case parser.OR_EXPR:
			parts := make([]string, len(e.Args))
			for i, arg := range e.Args {
				parts[i] = exprToSQL(arg)
			}
			return "(" + strings.Join(parts, " OR ") + ")"
		case parser.NOT_EXPR:
			return "NOT " + exprToSQL(e.Args[0])
		}
	case *parser.TypeCast:
		return exprToSQL(e.Arg)
	case *parser.NullTest:
		if e.NullTestType == parser.IS_NULL {
			return exprToSQL(e.Arg) + " IS NULL"
		}
		return exprToSQL(e.Arg) + " IS NOT NULL"
	case *parser.SQLValueFunction:
		switch e.Op {
		case parser.SVFOP_CURRENT_USER:
			return "current_user"
		case parser.SVFOP_CURRENT_ROLE:
			return "current_role"
		case parser.SVFOP_SESSION_USER:
			return "session_user"
		case parser.SVFOP_CURRENT_CATALOG:
			return "current_catalog"
		case parser.SVFOP_CURRENT_SCHEMA:
			return "current_schema"
		case parser.SVFOP_CURRENT_DATE:
			return "current_date"
		case parser.SVFOP_CURRENT_TIMESTAMP:
			return "current_timestamp"
		default:
			return "current_user"
		}
	}
	return fmt.Sprintf("%v", expr)
}

// -----------------------------------------------------------------------
// Role / Grant statement transformers
// -----------------------------------------------------------------------

func defElemString(arg parser.Node) string {
	switch v := arg.(type) {
	case *parser.A_Const:
		return v.Val.Str
	case *parser.String:
		return v.Str
	}
	return ""
}

func (a *Analyzer) transformCreateRoleStmt(n *parser.CreateRoleStmt) (*Query, error) {
	opts := make(map[string]interface{})

	// CREATE USER implies LOGIN by default; CREATE ROLE does not.
	if n.StmtType == "USER" {
		opts["login"] = true
	}

	for _, opt := range n.Options {
		switch opt.Defname {
		case "superuser":
			opts["superuser"] = true
		case "nosuperuser":
			opts["superuser"] = false
		case "createdb":
			opts["createdb"] = true
		case "nocreatedb":
			opts["createdb"] = false
		case "createrole":
			opts["createrole"] = true
		case "nocreaterole":
			opts["createrole"] = false
		case "inherit":
			opts["inherit"] = true
		case "noinherit":
			opts["inherit"] = false
		case "login":
			opts["login"] = true
		case "nologin":
			opts["login"] = false
		case "bypassrls":
			opts["bypassrls"] = true
		case "nobypassrls":
			opts["bypassrls"] = false
		case "password":
			opts["password"] = defElemString(opt.Arg)
		case "connlimit":
			if c, ok := opt.Arg.(*parser.A_Const); ok {
				opts["connlimit"] = int32(c.Val.Ival)
			}
		}
	}

	return a.makeUtilityQuery(UtilCreateRole, &UtilityStmt{
		Type:         UtilCreateRole,
		RoleName:     n.RoleName,
		RoleOptions:  opts,
		RoleStmtType: n.StmtType,
	}), nil
}

func (a *Analyzer) transformAlterRoleStmt(n *parser.AlterRoleStmt) (*Query, error) {
	opts := make(map[string]interface{})

	for _, opt := range n.Options {
		switch opt.Defname {
		case "superuser":
			opts["superuser"] = true
		case "nosuperuser":
			opts["superuser"] = false
		case "createdb":
			opts["createdb"] = true
		case "nocreatedb":
			opts["createdb"] = false
		case "createrole":
			opts["createrole"] = true
		case "nocreaterole":
			opts["createrole"] = false
		case "inherit":
			opts["inherit"] = true
		case "noinherit":
			opts["inherit"] = false
		case "login":
			opts["login"] = true
		case "nologin":
			opts["login"] = false
		case "bypassrls":
			opts["bypassrls"] = true
		case "nobypassrls":
			opts["bypassrls"] = false
		case "password":
			opts["password"] = defElemString(opt.Arg)
		case "connlimit":
			if c, ok := opt.Arg.(*parser.A_Const); ok {
				opts["connlimit"] = int32(c.Val.Ival)
			}
		}
	}

	return a.makeUtilityQuery(UtilAlterRole, &UtilityStmt{
		Type:        UtilAlterRole,
		RoleName:    n.RoleName,
		RoleOptions: opts,
	}), nil
}

func (a *Analyzer) transformDropRoleStmt(n *parser.DropRoleStmt) (*Query, error) {
	return a.makeUtilityQuery(UtilDropRole, &UtilityStmt{
		Type:          UtilDropRole,
		DropRoles:     n.Roles,
		DropMissingOk: n.MissingOk,
	}), nil
}

func (a *Analyzer) transformGrantRoleStmt(n *parser.GrantRoleStmt) (*Query, error) {
	if n.IsGrant {
		return a.makeUtilityQuery(UtilGrantRole, &UtilityStmt{
			Type:         UtilGrantRole,
			GrantedRoles: n.GrantedRoles,
			Grantees:     n.Grantees,
			AdminOption:  n.AdminOption,
		}), nil
	}
	return a.makeUtilityQuery(UtilRevokeRole, &UtilityStmt{
		Type:         UtilRevokeRole,
		GrantedRoles: n.GrantedRoles,
		Grantees:     n.Grantees,
	}), nil
}

func (a *Analyzer) transformGrantStmt(n *parser.GrantStmt) (*Query, error) {
	var objects []string
	for _, obj := range n.Objects {
		if len(obj) > 0 {
			objects = append(objects, obj[len(obj)-1])
		}
	}

	targetType := "TABLE"
	switch n.TargetType {
	case parser.OBJECT_TABLE:
		targetType = "TABLE"
	case parser.OBJECT_SEQUENCE:
		targetType = "SEQUENCE"
	case parser.OBJECT_SCHEMA:
		targetType = "SCHEMA"
	case parser.OBJECT_FUNCTION:
		targetType = "FUNCTION"
	}

	if n.IsGrant {
		return a.makeUtilityQuery(UtilGrantPrivilege, &UtilityStmt{
			Type:        UtilGrantPrivilege,
			Privileges:  n.Privileges,
			PrivCols:    n.PrivCols,
			TargetType:  targetType,
			Objects:     objects,
			Grantees:    n.Grantees,
			GrantOption: n.GrantOption,
		}), nil
	}
	return a.makeUtilityQuery(UtilRevokePrivilege, &UtilityStmt{
		Type:       UtilRevokePrivilege,
		Privileges: n.Privileges,
		PrivCols:   n.PrivCols,
		TargetType: targetType,
		Objects:    objects,
		Grantees:   n.Grantees,
	}), nil
}

