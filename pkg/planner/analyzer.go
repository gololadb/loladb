package planner

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/gololadb/gopgsql/parser"

	"github.com/gololadb/loladb/pkg/catalog"
	"github.com/gololadb/loladb/pkg/tuple"
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
	case *parser.CreateFunctionStmt:
		return a.transformCreateFunctionStmt(n)
	case *parser.CreateTrigStmt:
		return a.transformCreateTrigStmt(n)
	case *parser.RemoveFuncStmt:
		return a.transformDropFunctionStmt(n)
	case *parser.TruncateStmt:
		return a.transformTruncateStmt(n)
	case *parser.DropStmt:
		return a.transformDropStmt(n)
	case *parser.AlterFunctionStmt:
		return a.transformAlterFunctionStmt(n)
	case *parser.CreateDomainStmt:
		return a.transformCreateDomainStmt(n)
	case *parser.CreateEnumStmt:
		return a.transformCreateEnumStmt(n)
	case *parser.AlterEnumStmt:
		return a.transformAlterEnumStmt(n)
	case *parser.CreateSchemaStmt:
		return a.transformCreateSchemaStmt(n)
	default:
		return nil, fmt.Errorf("analyzer: unsupported statement %T", stmt)
	}
}

// --- Range table management ---
// Mirrors PostgreSQL's addRangeTableEntry() in parse_relation.c.

// addRangeTableEntry resolves a table name against the catalog and
// adds it to the range table. Returns the 1-based index.
func (a *Analyzer) addRangeTableEntry(tableName, alias string) (*RangeTblEntry, error) {
	return a.addRangeTableEntryQualified("", tableName, alias)
}

// addRangeTableEntryQualified resolves a schema-qualified table name.
func (a *Analyzer) addRangeTableEntryQualified(schema, tableName, alias string) (*RangeTblEntry, error) {
	rel, err := a.Cat.FindRelationQualified(schema, tableName)
	if err != nil {
		return nil, err
	}
	if rel == nil {
		if schema != "" {
			return nil, fmt.Errorf("analyzer: relation %q.%q does not exist", schema, tableName)
		}
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

	// Use schema-qualified name for the RelName so the executor can
	// find the correct relation when multiple schemas have same-named tables.
	qualName := tableName
	if schema != "" {
		qualName = schema + "." + tableName
	}

	if alias == "" {
		alias = tableName
	}

	rte := &RangeTblEntry{
		RTIndex:  len(a.rangeTable) + 1, // 1-based
		RelOID:   rel.OID,
		RelName:  qualName,
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
	// Handle set operations (UNION / INTERSECT / EXCEPT).
	if n.Op != parser.SETOP_NONE && n.Larg != nil && n.Rarg != nil {
		return a.transformSetOp(n)
	}

	if len(n.ValuesLists) > 0 {
		return nil, fmt.Errorf("analyzer: bare VALUES clause not supported")
	}

	q := &Query{CommandType: CmdSelect}

	// DISTINCT
	if n.DistinctClause != nil {
		q.Distinct = true
	}

	// Step 1: Process FROM clause → build range table and join tree.
	// Mirrors transformFromClause().
	fromList, err := a.transformFromClause(n.FromClause)
	if err != nil {
		return nil, err
	}
	if len(fromList) == 0 {
		// SELECT without FROM (e.g., SELECT 1, SELECT 1+1).
		// Build target list and use an empty FromExpr — the planner
		// will produce a Result node that emits a single row.
		var targets []*TargetEntry
		for i, item := range n.TargetList {
			expr, err := a.transformExpr(item.Val)
			if err != nil {
				return nil, err
			}
			name := item.Name
			if name == "" {
				name = fmt.Sprintf("?column%d?", i)
			}
			targets = append(targets, &TargetEntry{Expr: expr, Name: name})
		}
		q.TargetList = targets
		q.JoinTree = &FromExpr{} // empty FROM → Result node
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

	// Step 4: Transform GROUP BY.
	if len(n.GroupClause) > 0 {
		for _, g := range n.GroupClause {
			expr, err := a.transformExpr(g)
			if err != nil {
				return nil, fmt.Errorf("analyzer: GROUP BY: %w", err)
			}
			q.GroupClause = append(q.GroupClause, expr)
		}
	}

	// Step 5: Transform HAVING clause.
	if n.HavingClause != nil {
		havingExpr, err := a.transformExpr(n.HavingClause)
		if err != nil {
			return nil, fmt.Errorf("analyzer: HAVING: %w", err)
		}
		q.HavingQual = havingExpr
	}

	// Step 6: Transform ORDER BY.
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

	// Step 7: Collect aggregate references from all expression trees
	// (target list, HAVING, ORDER BY) and assign sequential indices.
	for _, te := range q.TargetList {
		collectAggRefs(te.Expr, &q.AggRefs)
	}
	if q.HavingQual != nil {
		collectAggRefs(q.HavingQual, &q.AggRefs)
	}
	for _, sc := range q.SortClause {
		collectAggRefs(sc.Expr, &q.AggRefs)
	}
	if len(q.AggRefs) > 0 {
		q.HasAggs = true
		for i, ref := range q.AggRefs {
			ref.AggIndex = i
		}
	}

	// Step 8: Transform LIMIT/OFFSET.
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
		tableName := t.Relname
		alias := tableName
		if t.Alias != nil && t.Alias.Aliasname != "" {
			alias = t.Alias.Aliasname
		}
		rte, err := a.addRangeTableEntryQualified(t.Schemaname, tableName, alias)
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
		inner, err := a.transformExpr(e.Arg)
		if err != nil {
			return nil, err
		}
		typeName := typeNameToString(e.TypeName)
		castType := a.resolveColumnType(strings.ToLower(typeName))
		return &TypeCastExpr{Arg: inner, TargetType: strings.ToLower(typeName), CastType: castType}, nil
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
		case parser.SVFOP_CURRENT_SCHEMA:
			// Return the current schema as a constant.
			schema := a.Cat.CurrentSchema()
			return &Const{Value: tuple.DText(schema), ConstType: tuple.TypeText}, nil
		case parser.SVFOP_CURRENT_CATALOG:
			return &Const{Value: tuple.DText("loladb"), ConstType: tuple.TypeText}, nil
		default:
			return nil, fmt.Errorf("analyzer: unsupported SQL value function (op %d)", e.Op)
		}
	case *parser.FuncCall:
		return a.transformFuncCall(e)
	case *parser.CoalesceExpr:
		var args []AnalyzedExpr
		for _, arg := range e.Args {
			resolved, err := a.transformExpr(arg)
			if err != nil {
				return nil, err
			}
			args = append(args, resolved)
		}
		retType := tuple.TypeNull
		if len(args) > 0 {
			retType = args[0].ResultType()
		}
		return &FuncCallExpr{FuncName: "coalesce", Args: args, ReturnType: retType}, nil
	case *parser.CaseExpr:
		var arg AnalyzedExpr
		if e.Arg != nil {
			var err error
			arg, err = a.transformExpr(e.Arg)
			if err != nil {
				return nil, err
			}
		}
		whens := make([]CaseWhenClause, len(e.Args))
		retType := tuple.TypeNull
		for i, w := range e.Args {
			cond, err := a.transformExpr(w.Expr)
			if err != nil {
				return nil, err
			}
			result, err := a.transformExpr(w.Result)
			if err != nil {
				return nil, err
			}
			if i == 0 && result.ResultType() != tuple.TypeNull {
				retType = result.ResultType()
			}
			whens[i] = CaseWhenClause{Cond: cond, Result: result}
		}
		var elseExpr AnalyzedExpr
		if e.Defresult != nil {
			var err error
			elseExpr, err = a.transformExpr(e.Defresult)
			if err != nil {
				return nil, err
			}
			if retType == tuple.TypeNull && elseExpr.ResultType() != tuple.TypeNull {
				retType = elseExpr.ResultType()
			}
		}
		return &CaseExprNode{Arg: arg, Whens: whens, ElseExpr: elseExpr, ReturnTyp: retType}, nil

	case *parser.BooleanTest:
		arg, err := a.transformExpr(e.Arg)
		if err != nil {
			return nil, err
		}
		var kind BoolTestKind
		switch e.BooltestType {
		case parser.IS_TRUE:
			kind = BoolTestIsTrue
		case parser.IS_NOT_TRUE:
			kind = BoolTestIsNotTrue
		case parser.IS_FALSE:
			kind = BoolTestIsFalse
		case parser.IS_NOT_FALSE:
			kind = BoolTestIsNotFalse
		case parser.IS_UNKNOWN:
			kind = BoolTestIsUnknown
		case parser.IS_NOT_UNKNOWN:
			kind = BoolTestIsNotUnknown
		}
		return &BooleanTestExpr{Arg: arg, Test: kind}, nil

	case *parser.NullIfExpr:
		if len(e.Args) != 2 {
			return nil, fmt.Errorf("analyzer: NULLIF requires exactly 2 arguments")
		}
		var args []AnalyzedExpr
		for _, arg := range e.Args {
			resolved, err := a.transformExpr(arg)
			if err != nil {
				return nil, err
			}
			args = append(args, resolved)
		}
		retType := args[0].ResultType()
		return &FuncCallExpr{FuncName: "nullif", Args: args, ReturnType: retType}, nil

	case *parser.MinMaxExpr:
		var args []AnalyzedExpr
		for _, arg := range e.Args {
			resolved, err := a.transformExpr(arg)
			if err != nil {
				return nil, err
			}
			args = append(args, resolved)
		}
		retType := tuple.TypeNull
		if len(args) > 0 {
			retType = args[0].ResultType()
		}
		funcName := "greatest"
		if e.Op == parser.IS_LEAST {
			funcName = "least"
		}
		return &FuncCallExpr{FuncName: funcName, Args: args, ReturnType: retType}, nil

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
	case parser.ValBool:
		return &Const{Value: tuple.DBool(c.Val.Bool), ConstType: tuple.TypeBool}
	case parser.ValStr:
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
	if len(ref.Fields) == 3 {
		// schema.table.column — strip schema, resolve as table.column.
		tableName := ""
		colName := ""
		if s, ok := ref.Fields[1].(*parser.String); ok {
			tableName = s.Str
		}
		if s, ok := ref.Fields[2].(*parser.String); ok {
			colName = s.Str
		} else if _, ok := ref.Fields[2].(*parser.A_Star); ok {
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

	// Handle special expression kinds before resolving left/right.
	switch e.Kind {
	case parser.AEXPR_BETWEEN, parser.AEXPR_NOT_BETWEEN,
		parser.AEXPR_BETWEEN_SYM, parser.AEXPR_NOT_BETWEEN_SYM:
		return a.transformBetween(e)
	case parser.AEXPR_IN:
		return a.transformIn(e)
	case parser.AEXPR_LIKE, parser.AEXPR_ILIKE:
		return a.transformLike(e)
	case parser.AEXPR_DISTINCT:
		return a.transformDistinctFrom(e, false)
	case parser.AEXPR_NOT_DISTINCT:
		return a.transformDistinctFrom(e, true)
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
	var resultTyp tuple.DatumType
	switch opName {
	case "=":
		op = OpEq
		resultTyp = tuple.TypeBool
	case "<>", "!=":
		op = OpNeq
		resultTyp = tuple.TypeBool
	case "<":
		op = OpLt
		resultTyp = tuple.TypeBool
	case "<=":
		op = OpLte
		resultTyp = tuple.TypeBool
	case ">":
		op = OpGt
		resultTyp = tuple.TypeBool
	case ">=":
		op = OpGte
		resultTyp = tuple.TypeBool
	case "+":
		op = OpAdd
		resultTyp = inferArithType(left, right)
	case "-":
		op = OpSub
		resultTyp = inferArithType(left, right)
	case "*":
		op = OpMul
		resultTyp = inferArithType(left, right)
	case "/":
		op = OpDiv
		resultTyp = inferArithType(left, right)
	case "%":
		op = OpMod
		resultTyp = inferArithType(left, right)
	case "||":
		op = OpConcat
		resultTyp = tuple.TypeText
	default:
		return nil, fmt.Errorf("analyzer: unsupported operator %q", opName)
	}

	return &OpExpr{Op: op, Left: left, Right: right, ResultTyp: resultTyp}, nil
}

// transformLike handles LIKE / ILIKE / NOT LIKE / NOT ILIKE.
func (a *Analyzer) transformLike(e *parser.A_Expr) (AnalyzedExpr, error) {
	left, err := a.transformExpr(e.Lexpr)
	if err != nil {
		return nil, err
	}
	// Rexpr is either a single pattern or an ExprList (with ESCAPE).
	// We only support the simple case for now.
	var patternExpr parser.Expr
	if el, ok := e.Rexpr.(*parser.ExprList); ok && len(el.Items) > 0 {
		patternExpr = el.Items[0] // ignore ESCAPE for now
	} else {
		patternExpr = e.Rexpr
	}
	right, err := a.transformExpr(patternExpr)
	if err != nil {
		return nil, err
	}
	opName := ""
	if len(e.Name) > 0 {
		opName = e.Name[len(e.Name)-1]
	}
	var op OpKind
	switch opName {
	case "~~":
		op = OpLike
	case "~~*":
		op = OpILike
	case "!~~":
		op = OpNotLike
	case "!~~*":
		op = OpNotILike
	default:
		if e.Kind == parser.AEXPR_ILIKE {
			op = OpILike
		} else {
			op = OpLike
		}
	}
	return &OpExpr{Op: op, Left: left, Right: right, ResultTyp: tuple.TypeBool}, nil
}

// transformBetween desugars BETWEEN into AND/OR comparisons.
// x BETWEEN a AND b  →  x >= a AND x <= b
// x NOT BETWEEN a AND b  →  x < a OR x > b
func (a *Analyzer) transformBetween(e *parser.A_Expr) (AnalyzedExpr, error) {
	left, err := a.transformExpr(e.Lexpr)
	if err != nil {
		return nil, err
	}
	el, ok := e.Rexpr.(*parser.ExprList)
	if !ok || len(el.Items) != 2 {
		return nil, fmt.Errorf("analyzer: BETWEEN requires exactly 2 bounds")
	}
	low, err := a.transformExpr(el.Items[0])
	if err != nil {
		return nil, err
	}
	high, err := a.transformExpr(el.Items[1])
	if err != nil {
		return nil, err
	}
	// For SYMMETRIC variants, we'd need runtime min/max, but for now
	// treat them the same as regular BETWEEN.
	switch e.Kind {
	case parser.AEXPR_NOT_BETWEEN, parser.AEXPR_NOT_BETWEEN_SYM:
		// x < low OR x > high
		return &BoolExprNode{
			Op: BoolOr,
			Args: []AnalyzedExpr{
				&OpExpr{Op: OpLt, Left: left, Right: low, ResultTyp: tuple.TypeBool},
				&OpExpr{Op: OpGt, Left: left, Right: high, ResultTyp: tuple.TypeBool},
			},
			
		}, nil
	default:
		// x >= low AND x <= high
		return &BoolExprNode{
			Op: BoolAnd,
			Args: []AnalyzedExpr{
				&OpExpr{Op: OpGte, Left: left, Right: low, ResultTyp: tuple.TypeBool},
				&OpExpr{Op: OpLte, Left: left, Right: high, ResultTyp: tuple.TypeBool},
			},
			
		}, nil
	}
}

// transformIn desugars IN (val1, val2, ...) into x=a OR x=b OR ...
func (a *Analyzer) transformIn(e *parser.A_Expr) (AnalyzedExpr, error) {
	left, err := a.transformExpr(e.Lexpr)
	if err != nil {
		return nil, err
	}
	el, ok := e.Rexpr.(*parser.ExprList)
	if !ok {
		return nil, fmt.Errorf("analyzer: IN requires a value list")
	}
	var eqExprs []AnalyzedExpr
	for _, item := range el.Items {
		val, err := a.transformExpr(item)
		if err != nil {
			return nil, err
		}
		eqExprs = append(eqExprs, &OpExpr{Op: OpEq, Left: left, Right: val, ResultTyp: tuple.TypeBool})
	}
	if len(eqExprs) == 1 {
		return eqExprs[0], nil
	}
	return &BoolExprNode{Op: BoolOr, Args: eqExprs}, nil
}

// transformDistinctFrom handles IS [NOT] DISTINCT FROM.
func (a *Analyzer) transformDistinctFrom(e *parser.A_Expr, notDistinct bool) (AnalyzedExpr, error) {
	left, err := a.transformExpr(e.Lexpr)
	if err != nil {
		return nil, err
	}
	right, err := a.transformExpr(e.Rexpr)
	if err != nil {
		return nil, err
	}
	// IS DISTINCT FROM: like <> but treats NULL as a comparable value.
	// IS NOT DISTINCT FROM: like = but treats NULL as a comparable value.
	// We implement this as a special function call.
	funcName := "is_distinct_from"
	if notDistinct {
		funcName = "is_not_distinct_from"
	}
	return &FuncCallExpr{FuncName: funcName, Args: []AnalyzedExpr{left, right}, ReturnType: tuple.TypeBool}, nil
}

// inferArithType returns the result type for an arithmetic operation.
// If either operand is float64, the result is float64. Otherwise int64
// (promoting int32 to int64).
func inferArithType(left, right AnalyzedExpr) tuple.DatumType {
	lt := left.ResultType()
	rt := right.ResultType()
	if lt == tuple.TypeFloat64 || rt == tuple.TypeFloat64 {
		return tuple.TypeFloat64
	}
	return tuple.TypeInt64
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
func (a *Analyzer) transformSetOp(n *parser.SelectStmt) (*Query, error) {
	// Analyze left and right branches independently.
	leftAnalyzer := &Analyzer{Cat: a.Cat}
	leftQ, err := leftAnalyzer.transformSelectStmt(n.Larg)
	if err != nil {
		return nil, fmt.Errorf("analyzer: left side of set operation: %w", err)
	}
	rightAnalyzer := &Analyzer{Cat: a.Cat}
	rightQ, err := rightAnalyzer.transformSelectStmt(n.Rarg)
	if err != nil {
		return nil, fmt.Errorf("analyzer: right side of set operation: %w", err)
	}

	var opKind SetOpKind
	switch n.Op {
	case parser.SETOP_UNION:
		opKind = SetOpUnion
	case parser.SETOP_INTERSECT:
		opKind = SetOpIntersect
	case parser.SETOP_EXCEPT:
		opKind = SetOpExcept
	}

	q := &Query{
		CommandType: CmdSelect,
		SetOp:       opKind,
		SetAll:      n.All,
		SetLeft:     leftQ,
		SetRight:    rightQ,
		TargetList:  leftQ.TargetList, // column names from left side
	}

	// Handle ORDER BY / LIMIT on the combined result.
	if len(n.SortClause) > 0 {
		for _, sc := range n.SortClause {
			expr, err := a.transformExpr(sc.Node)
			if err != nil {
				return nil, err
			}
			q.SortClause = append(q.SortClause, &SortClause{
				Expr: expr,
				Desc: sc.SortbyDir == parser.SORTBY_DESC,
			})
		}
	}
	if n.LimitCount != nil {
		lc, err := a.transformExpr(n.LimitCount)
		if err != nil {
			return nil, err
		}
		q.LimitCount = lc
	}
	if n.LimitOffset != nil {
		lo, err := a.transformExpr(n.LimitOffset)
		if err != nil {
			return nil, err
		}
		q.LimitOffset = lo
	}

	return q, nil
}

func (a *Analyzer) transformInsertStmt(n *parser.InsertStmt) (*Query, error) {
	q := &Query{CommandType: CmdInsert}

	// Add the target relation to the range table.
	rte, err := a.addRangeTableEntryQualified(n.Relation.Schemaname, n.Relation.Relname, "")
	if err != nil {
		return nil, err
	}
	q.ResultRelation = rte.RTIndex

	// Extract explicit column list if present.
	if len(n.Cols) > 0 {
		for _, col := range n.Cols {
			q.InsertColumns = append(q.InsertColumns, col.Name)
		}
	}

	// Resolve source: VALUES or SELECT.
	sel, ok := n.SelectStmt.(*parser.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("analyzer: unsupported INSERT source")
	}

	if len(sel.ValuesLists) > 0 {
		// INSERT ... VALUES (...)
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
	} else {
		// INSERT ... SELECT ...
		subAnalyzer := &Analyzer{Cat: a.Cat}
		subQ, err := subAnalyzer.transformSelectStmt(sel)
		if err != nil {
			return nil, fmt.Errorf("analyzer: INSERT ... SELECT: %w", err)
		}
		q.SelectSource = subQ
	}

	// RETURNING clause.
	if len(n.ReturningList) > 0 {
		ret, err := a.transformReturningList(n.ReturningList)
		if err != nil {
			return nil, fmt.Errorf("analyzer: INSERT RETURNING: %w", err)
		}
		q.ReturningList = ret
	}

	q.RangeTable = a.rangeTable
	return q, nil
}

// transformDeleteStmt resolves a DELETE statement.
// Mirrors PostgreSQL's transformDeleteStmt() in analyze.c.
func (a *Analyzer) transformDeleteStmt(n *parser.DeleteStmt) (*Query, error) {
	q := &Query{CommandType: CmdDelete}

	rte, err := a.addRangeTableEntryQualified(n.Relation.Schemaname, n.Relation.Relname, "")
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

	// RETURNING clause.
	if len(n.ReturningList) > 0 {
		ret, err := a.transformReturningList(n.ReturningList)
		if err != nil {
			return nil, fmt.Errorf("analyzer: DELETE RETURNING: %w", err)
		}
		q.ReturningList = ret
	}

	q.RangeTable = a.rangeTable
	return q, nil
}

// transformUpdateStmt resolves an UPDATE statement.
// Mirrors PostgreSQL's transformUpdateStmt() in analyze.c.
func (a *Analyzer) transformUpdateStmt(n *parser.UpdateStmt) (*Query, error) {
	q := &Query{CommandType: CmdUpdate}

	rte, err := a.addRangeTableEntryQualified(n.Relation.Schemaname, n.Relation.Relname, "")
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

	// RETURNING clause.
	if len(n.ReturningList) > 0 {
		ret, err := a.transformReturningList(n.ReturningList)
		if err != nil {
			return nil, fmt.Errorf("analyzer: UPDATE RETURNING: %w", err)
		}
		q.ReturningList = ret
	}

	q.RangeTable = a.rangeTable
	return q, nil
}

// transformReturningList resolves a RETURNING clause into target entries.
func (a *Analyzer) transformReturningList(list []*parser.ResTarget) ([]*TargetEntry, error) {
	if len(list) == 0 {
		return nil, nil
	}
	var entries []*TargetEntry
	for _, rt := range list {
		if isStarTarget(rt) {
			// RETURNING * — expand all columns from the range table.
			for _, rte := range a.rangeTable {
				for _, col := range rte.Columns {
					entries = append(entries, &TargetEntry{
						Name: col.Name,
						Expr: &ColumnVar{
							RTIndex:  rte.RTIndex,
							ColNum:   col.ColNum,
							ColName:  col.Name,
							Table:    rte.RelName,
							VarType:  col.Type,
							AttIndex: int(col.ColNum - 1),
						},
					})
				}
			}
			continue
		}
		expr, err := a.transformExpr(rt.Val)
		if err != nil {
			return nil, err
		}
		name := rt.Name
		if name == "" {
			name = exprString(rt.Val)
		}
		entries = append(entries, &TargetEntry{Name: name, Expr: expr})
	}
	return entries, nil
}

// --- Utility statement transformations ---

func (a *Analyzer) transformCreateStmt(n *parser.CreateStmt) (*Query, error) {
	tableName := n.Relation.Relname
	schemaName := n.Relation.Schemaname
	var cols []ColDef
	for _, elt := range n.TableElts {
		colDef, ok := elt.(*parser.ColumnDef)
		if !ok {
			continue
		}
		sqlType := typeNameToString(colDef.TypeName)
		dt := a.resolveColumnType(sqlType)
		notNull := false
		primaryKey := false
		unique := false
		defaultExpr := ""
		for _, c := range colDef.Constraints {
			switch c.Contype {
			case parser.CONSTR_NOTNULL:
				notNull = true
			case parser.CONSTR_DEFAULT:
				if c.RawExpr != nil {
					defaultExpr = parser.DeparseExpr(c.RawExpr)
				}
			case parser.CONSTR_PRIMARY:
				primaryKey = true
				notNull = true // PRIMARY KEY implies NOT NULL
			case parser.CONSTR_UNIQUE:
				unique = true
			}
		}
		cols = append(cols, ColDef{Name: colDef.Colname, Type: dt, TypeName: sqlType, NotNull: notNull, PrimaryKey: primaryKey, Unique: unique, DefaultExpr: defaultExpr})
	}

	// Handle table-level constraints (e.g., PRIMARY KEY (col), UNIQUE (col)).
	for _, elt := range n.TableElts {
		con, ok := elt.(*parser.Constraint)
		if !ok {
			continue
		}
		switch con.Contype {
		case parser.CONSTR_PRIMARY:
			for _, key := range con.Keys {
				for i := range cols {
					if cols[i].Name == key {
						cols[i].PrimaryKey = true
						cols[i].NotNull = true
					}
				}
			}
		case parser.CONSTR_UNIQUE:
			for _, key := range con.Keys {
				for i := range cols {
					if cols[i].Name == key {
						cols[i].Unique = true
					}
				}
			}
		}
	}

	return a.makeUtilityQuery(UtilCreateTable, &UtilityStmt{
		Type: UtilCreateTable, TableName: tableName, TableSchema: schemaName, Columns: cols,
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

	// Handle single-command ALTER TABLE statements that map to dedicated utility types.
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
		case parser.AT_AddColumn:
			colDef, ok := cmd.Def.(*parser.ColumnDef)
			if !ok {
				return nil, fmt.Errorf("analyzer: ADD COLUMN missing column definition")
			}
			sqlType := typeNameToString(colDef.TypeName)
			dt := a.resolveColumnType(sqlType)
			notNull := colDef.IsNotNull
			defaultExpr := ""
			for _, c := range colDef.Constraints {
				switch c.Contype {
				case parser.CONSTR_NOTNULL:
					notNull = true
				case parser.CONSTR_DEFAULT:
					if c.RawExpr != nil {
						defaultExpr = parser.DeparseExpr(c.RawExpr)
					}
				}
			}
			return a.makeUtilityQuery(UtilAddColumn, &UtilityStmt{
				Type:      UtilAddColumn,
				TableName: tableName,
				AlterColDef: &ColDef{
					Name: colDef.Colname, Type: dt, TypeName: sqlType,
					NotNull: notNull, DefaultExpr: defaultExpr,
				},
				AlterIfNotExists: cmd.MissingOk,
			}), nil
		case parser.AT_DropColumn:
			return a.makeUtilityQuery(UtilDropColumn, &UtilityStmt{
				Type:         UtilDropColumn,
				TableName:    tableName,
				AlterColName: cmd.Name,
				AlterIfExists: cmd.MissingOk,
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
	case *parser.FuncCall:
		name := ""
		if len(e.Funcname) > 0 {
			name = e.Funcname[len(e.Funcname)-1]
		}
		if e.AggStar {
			return name + "(*)"
		}
		var args []string
		for _, arg := range e.Args {
			args = append(args, exprString(arg))
		}
		return name + "(" + strings.Join(args, ", ") + ")"
	case *parser.TypeCast:
		return exprString(e.Arg) + "::" + typeNameToString(e.TypeName)
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

// resolveColumnType resolves a SQL type name, checking custom types
// (domains, enums) in the catalog before falling back to built-in types.
func (a *Analyzer) resolveColumnType(sqlType string) tuple.DatumType {
	if dt, ok := a.Cat.ResolveType(sqlType); ok {
		return dt
	}
	return MapSQLType(sqlType)
}

// MapSQLType maps a SQL type name to a DatumType.
func MapSQLType(sqlType string) tuple.DatumType {
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


func (a *Analyzer) transformCreateFunctionStmt(n *parser.CreateFunctionStmt) (*Query, error) {
	name := ""
	if len(n.Funcname) > 0 {
		name = n.Funcname[len(n.Funcname)-1] // use last part (unqualified name)
	}

	language := "plpgsql"
	body := ""
	for _, opt := range n.Options {
		switch strings.ToLower(opt.Defname) {
		case "language":
			if s, ok := opt.Arg.(*parser.String); ok {
				language = strings.ToLower(s.Str)
			}
		case "as":
			if s, ok := opt.Arg.(*parser.String); ok {
				body = s.Str
			}
		}
	}

	retType := ""
	if n.ReturnType != nil && len(n.ReturnType.Names) > 0 {
		retType = n.ReturnType.Names[len(n.ReturnType.Names)-1]
	}

	var paramNames, paramTypes []string
	for _, p := range n.Parameters {
		paramNames = append(paramNames, p.Name)
		if p.ArgType != nil && len(p.ArgType.Names) > 0 {
			paramTypes = append(paramTypes, p.ArgType.Names[len(p.ArgType.Names)-1])
		} else {
			paramTypes = append(paramTypes, "unknown")
		}
	}

	return a.makeUtilityQuery(UtilCreateFunction, &UtilityStmt{
		Type:           UtilCreateFunction,
		FuncName:       name,
		FuncLanguage:   language,
		FuncBody:       body,
		FuncReturnType: retType,
		FuncParamNames: paramNames,
		FuncParamTypes: paramTypes,
		FuncReplace:    n.Replace,
	}), nil
}

func (a *Analyzer) transformCreateTrigStmt(n *parser.CreateTrigStmt) (*Query, error) {
	tableName := ""
	if n.Relation != nil {
		tableName = n.Relation.Relname
	}

	funcName := ""
	if len(n.Funcname) > 0 {
		funcName = n.Funcname[len(n.Funcname)-1]
	}

	forEach := "STATEMENT"
	if n.Row {
		forEach = "ROW"
	}

	return a.makeUtilityQuery(UtilCreateTrigger, &UtilityStmt{
		Type:        UtilCreateTrigger,
		TrigName:    n.Trigname,
		TrigTable:   tableName,
		TrigFuncName: funcName,
		TrigTiming:  n.Timing,
		TrigEvents:  n.Events,
		TrigForEach: forEach,
		TrigReplace: n.Replace,
	}), nil
}

func (a *Analyzer) transformDropFunctionStmt(n *parser.RemoveFuncStmt) (*Query, error) {
	name := ""
	if len(n.Funcname) > 0 {
		name = n.Funcname[len(n.Funcname)-1]
	}
	return a.makeUtilityQuery(UtilDropFunction, &UtilityStmt{
		Type:          UtilDropFunction,
		FuncName:      name,
		DropMissingOk: n.MissingOk,
	}), nil
}

func (a *Analyzer) transformTruncateStmt(n *parser.TruncateStmt) (*Query, error) {
	if len(n.Relations) == 0 {
		return nil, fmt.Errorf("analyzer: TRUNCATE requires at least one table")
	}
	tableName := n.Relations[0].Relname
	return a.makeUtilityQuery(UtilTruncate, &UtilityStmt{
		Type: UtilTruncate, TableName: tableName,
	}), nil
}

func (a *Analyzer) transformDropStmt(n *parser.DropStmt) (*Query, error) {
	switch n.RemoveType {
	case parser.OBJECT_TRIGGER:
		trigName := ""
		if len(n.Objects) > 0 && len(n.Objects[0]) > 0 {
			trigName = n.Objects[0][len(n.Objects[0])-1]
		}
		tableName := ""
		if len(n.Objects) > 1 && len(n.Objects[1]) > 0 {
			tableName = n.Objects[1][len(n.Objects[1])-1]
		}
		return a.makeUtilityQuery(UtilDropTrigger, &UtilityStmt{
			Type:          UtilDropTrigger,
			TrigName:      trigName,
			TrigTable:     tableName,
			DropMissingOk: n.MissingOk,
		}), nil
	case parser.OBJECT_TYPE, parser.OBJECT_DOMAIN:
		typeName := ""
		if len(n.Objects) > 0 && len(n.Objects[0]) > 0 {
			typeName = n.Objects[0][len(n.Objects[0])-1]
		}
		return a.makeUtilityQuery(UtilDropType, &UtilityStmt{
			Type:          UtilDropType,
			DropTypeName:  typeName,
			DropMissingOk: n.MissingOk,
		}), nil
	case parser.OBJECT_INDEX:
		indexName := ""
		if len(n.Objects) > 0 && len(n.Objects[0]) > 0 {
			indexName = n.Objects[0][len(n.Objects[0])-1]
		}
		return a.makeUtilityQuery(UtilDropIndex, &UtilityStmt{
			Type:          UtilDropIndex,
			IndexName:     indexName,
			DropMissingOk: n.MissingOk,
			DropCascade:   n.Behavior == parser.DROP_CASCADE,
		}), nil
	case parser.OBJECT_VIEW:
		viewName := ""
		if len(n.Objects) > 0 && len(n.Objects[0]) > 0 {
			viewName = n.Objects[0][len(n.Objects[0])-1]
		}
		return a.makeUtilityQuery(UtilDropView, &UtilityStmt{
			Type:          UtilDropView,
			ViewName:      viewName,
			DropMissingOk: n.MissingOk,
			DropCascade:   n.Behavior == parser.DROP_CASCADE,
		}), nil
	case parser.OBJECT_SCHEMA:
		schemaName := ""
		if len(n.Objects) > 0 && len(n.Objects[0]) > 0 {
			schemaName = n.Objects[0][len(n.Objects[0])-1]
		}
		return a.makeUtilityQuery(UtilDropSchema, &UtilityStmt{
			Type:          UtilDropSchema,
			SchemaName:    schemaName,
			DropMissingOk: n.MissingOk,
			DropCascade:   n.Behavior == parser.DROP_CASCADE,
		}), nil
	default:
		return nil, fmt.Errorf("analyzer: unsupported DROP object type %d", n.RemoveType)
	}
}

func (a *Analyzer) transformAlterFunctionStmt(n *parser.AlterFunctionStmt) (*Query, error) {
	name := ""
	if n.Func != nil && len(n.Func.Funcname) > 0 {
		name = n.Func.Funcname[len(n.Func.Funcname)-1]
	}
	u := &UtilityStmt{
		Type:     UtilAlterFunction,
		FuncName: name,
	}
	for _, act := range n.Actions {
		switch strings.ToLower(act.Defname) {
		case "rename":
			if s, ok := act.Arg.(*parser.String); ok {
				u.FuncNewName = s.Str
			}
		case "owner":
			if s, ok := act.Arg.(*parser.String); ok {
				u.FuncNewOwner = s.Str
			}
		}
	}
	return a.makeUtilityQuery(UtilAlterFunction, u), nil
}

func (a *Analyzer) transformCreateDomainStmt(n *parser.CreateDomainStmt) (*Query, error) {
	name := lastNamePart(n.Domainname)
	baseType := typeNameToString(n.TypeName)
	u := &UtilityStmt{
		Type:           UtilCreateDomain,
		DomainName:     name,
		DomainBaseType: baseType,
	}
	for _, c := range n.Constraints {
		switch c.Contype {
		case parser.CONSTR_NOTNULL:
			u.DomainNotNull = true
		case parser.CONSTR_CHECK:
			if c.RawExpr != nil {
				u.DomainCheck = parser.DeparseExpr(c.RawExpr)
			}
		}
	}
	return a.makeUtilityQuery(UtilCreateDomain, u), nil
}

func (a *Analyzer) transformCreateEnumStmt(n *parser.CreateEnumStmt) (*Query, error) {
	name := lastNamePart(n.TypeName)
	return a.makeUtilityQuery(UtilCreateEnum, &UtilityStmt{
		Type:     UtilCreateEnum,
		EnumName: name,
		EnumVals: n.Vals,
	}), nil
}

func (a *Analyzer) transformAlterEnumStmt(n *parser.AlterEnumStmt) (*Query, error) {
	name := lastNamePart(n.TypeName)
	return a.makeUtilityQuery(UtilAlterEnum, &UtilityStmt{
		Type:          UtilAlterEnum,
		AlterEnumName: name,
		AlterEnumVal:  n.NewVal,
	}), nil
}

func (a *Analyzer) transformCreateSchemaStmt(n *parser.CreateSchemaStmt) (*Query, error) {
	name := n.Schemaname
	if name == "" && n.AuthRole != "" {
		name = n.AuthRole
	}
	if name == "" {
		return nil, fmt.Errorf("analyzer: CREATE SCHEMA requires a name")
	}
	return a.makeUtilityQuery(UtilCreateSchema, &UtilityStmt{
		Type:              UtilCreateSchema,
		SchemaName:        name,
		SchemaIfNotExists: n.IfNotExists,
		SchemaAuthRole:    n.AuthRole,
	}), nil
}

// collectAggRefs walks an analyzed expression tree and appends any
// AggRef nodes found to the provided slice.
func collectAggRefs(expr AnalyzedExpr, refs *[]*AggRef) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *AggRef:
		*refs = append(*refs, e)
	case *OpExpr:
		collectAggRefs(e.Left, refs)
		collectAggRefs(e.Right, refs)
	case *BoolExprNode:
		for _, a := range e.Args {
			collectAggRefs(a, refs)
		}
	case *NullTestExpr:
		collectAggRefs(e.Arg, refs)
	case *FuncCallExpr:
		for _, a := range e.Args {
			collectAggRefs(a, refs)
		}
	case *TypeCastExpr:
		collectAggRefs(e.Arg, refs)
	}
}

// isAggregateFunc returns true if the function name is a known aggregate.
func isAggregateFunc(name string) bool {
	switch strings.ToLower(name) {
	case "count", "sum", "avg", "min", "max",
		"bool_and", "bool_or", "every",
		"string_agg", "array_agg":
		return true
	}
	return false
}

// transformFuncCall resolves a function call expression.
func (a *Analyzer) transformFuncCall(f *parser.FuncCall) (AnalyzedExpr, error) {
	// Get the unqualified function name.
	name := ""
	if len(f.Funcname) > 0 {
		name = strings.ToLower(f.Funcname[len(f.Funcname)-1])
	}

	// Resolve arguments.
	var args []AnalyzedExpr
	for _, arg := range f.Args {
		resolved, err := a.transformExpr(arg)
		if err != nil {
			return nil, err
		}
		args = append(args, resolved)
	}

	// Aggregate functions produce AggRef nodes.
	if isAggregateFunc(name) {
		var retType tuple.DatumType
		switch name {
		case "count":
			retType = tuple.TypeInt64
		case "sum":
			if len(args) > 0 {
				retType = args[0].ResultType()
			} else {
				retType = tuple.TypeInt64
			}
		case "avg":
			retType = tuple.TypeFloat64
		case "min", "max":
			if len(args) > 0 {
				retType = args[0].ResultType()
			} else {
				retType = tuple.TypeText
			}
		case "bool_and", "bool_or", "every":
			retType = tuple.TypeBool
		case "string_agg":
			retType = tuple.TypeText
		default:
			retType = tuple.TypeText
		}
		return &AggRef{
			AggFunc:   name,
			Args:      args,
			Star:      f.AggStar,
			Distinct:  f.AggDistinct,
			AggIndex:  -1, // set later by the planner
			ReturnTyp: retType,
		}, nil
	}

	// Determine return type based on function name.
	var retType tuple.DatumType
	switch name {
	// Date/time → text
	case "now", "current_timestamp", "current_date",
		"to_char", "to_date", "to_timestamp", "date_trunc", "age":
		retType = tuple.TypeText
	// Sequence / integer-returning
	case "nextval", "currval", "setval":
		retType = tuple.TypeInt64
	case "length", "char_length", "character_length",
		"octet_length", "bit_length", "ascii", "position", "array_length":
		retType = tuple.TypeInt64
	// String-returning
	case "upper", "lower", "concat", "concat_ws",
		"substring", "substr", "trim", "btrim", "ltrim", "rtrim",
		"replace", "overlay", "left", "right",
		"lpad", "rpad", "repeat", "reverse", "split_part",
		"initcap", "translate", "chr",
		"md5", "gen_random_uuid", "encode", "decode", "format",
		"regexp_replace", "string_to_array":
		retType = tuple.TypeText
	// Float-returning
	case "abs", "ceil", "ceiling", "floor", "round", "trunc", "truncate",
		"mod", "power", "pow", "sqrt", "cbrt", "sign",
		"random", "pi", "log", "ln", "log10", "exp",
		"extract", "date_part", "to_number":
		retType = tuple.TypeFloat64
	// Bool-returning
	case "regexp_match":
		retType = tuple.TypeText // returns matched text or NULL
	// Preserve input type
	case "coalesce", "nullif", "greatest", "least":
		if len(args) > 0 {
			retType = args[0].ResultType()
		} else {
			retType = tuple.TypeNull
		}
	default:
		retType = tuple.TypeText
	}

	return &FuncCallExpr{FuncName: name, Args: args, ReturnType: retType}, nil
}

// TransformExpr is an exported wrapper around transformExpr for use by
// the executor when evaluating DEFAULT expressions.
func (a *Analyzer) TransformExpr(expr parser.Expr) (AnalyzedExpr, error) {
	return a.transformExpr(expr)
}




