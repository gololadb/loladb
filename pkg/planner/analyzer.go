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

	// cteMap holds CTE definitions visible in the current query scope.
	cteMap map[string]*CTEDef
}

// NewAnalyzerWithRTE creates an Analyzer pre-loaded with a single range
// table entry. Used by the executor to evaluate generated column expressions
// where column references must resolve against the table's columns.
func NewAnalyzerWithRTE(cat *catalog.Catalog, cols []RTEColumn) *Analyzer {
	a := &Analyzer{Cat: cat}
	rte := &RangeTblEntry{
		RTIndex: 1,
		Alias:   "",
		Columns: cols,
	}
	a.rangeTable = []*RangeTblEntry{rte}
	return a
}

// Analyze transforms a raw parse tree statement into a Query.
// This is the main entry point, equivalent to PostgreSQL's
// parse_analyze().
func (a *Analyzer) Analyze(stmt parser.Stmt) (*Query, error) {
	// Reset per-query state.
	a.rangeTable = nil
	a.cteMap = nil

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
	// Check for virtual catalog tables first.
	qualName := tableName
	if schema != "" {
		qualName = schema + "." + tableName
	}
	if vcols := virtualCatalogColumns(qualName); vcols != nil {
		if alias == "" {
			alias = tableName
		}
		rte := &RangeTblEntry{
			RTIndex: len(a.rangeTable) + 1,
			RelName: qualName,
			Alias:   alias,
			Columns: vcols,
		}
		a.rangeTable = append(a.rangeTable, rte)
		return rte, nil
	}

	rel, err := a.Cat.FindRelationQualified(schema, tableName)
	if err != nil {
		return nil, err
	}
	if rel == nil {
		// Also check unqualified names for pg_catalog virtual tables.
		if schema == "" {
			if vcols := virtualCatalogColumns("pg_catalog." + tableName); vcols != nil {
				if alias == "" {
					alias = tableName
				}
				rte := &RangeTblEntry{
					RTIndex: len(a.rangeTable) + 1,
					RelName: "pg_catalog." + tableName,
					Alias:   alias,
					Columns: vcols,
				}
				a.rangeTable = append(a.rangeTable, rte)
				return rte, nil
			}
		}
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
	qualName = tableName
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
		return a.transformValuesClause(n)
	}

	q := &Query{CommandType: CmdSelect}

	// Process WITH clause (CTEs) before anything else so CTE names
	// are available when resolving FROM references.
	if n.WithClause != nil {
		if err := a.transformWithClause(n.WithClause, q); err != nil {
			return nil, err
		}
	}

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

	// Collect window function references from the target list.
	for _, te := range q.TargetList {
		collectWindowFuncs(te.Expr, &q.WindowFuncs)
	}

	// Step 4: Transform GROUP BY (including GROUPING SETS / CUBE / ROLLUP).
	if len(n.GroupClause) > 0 {
		hasGroupingSets := false
		for _, g := range n.GroupClause {
			if _, ok := g.(*parser.GroupingSet); ok {
				hasGroupingSets = true
				break
			}
		}

		if hasGroupingSets {
			// Collect all unique group expressions and build grouping sets.
			exprIndex := map[string]int{} // expr string → index
			for _, g := range n.GroupClause {
				gs, ok := g.(*parser.GroupingSet)
				if !ok {
					// Plain expression alongside grouping sets — treat as single-element set.
					expr, err := a.transformExpr(g)
					if err != nil {
						return nil, fmt.Errorf("analyzer: GROUP BY: %w", err)
					}
					key := expr.String()
					if _, exists := exprIndex[key]; !exists {
						exprIndex[key] = len(q.GroupClause)
						q.GroupClause = append(q.GroupClause, expr)
					}
					continue
				}
				sets := expandGroupingSet(gs)
				for _, set := range sets {
					var idxSet []int
					for _, node := range set {
						expr, err := a.transformExpr(node)
						if err != nil {
							return nil, fmt.Errorf("analyzer: GROUP BY: %w", err)
						}
						key := expr.String()
						idx, exists := exprIndex[key]
						if !exists {
							idx = len(q.GroupClause)
							exprIndex[key] = idx
							q.GroupClause = append(q.GroupClause, expr)
						}
						idxSet = append(idxSet, idx)
					}
					q.GroupingSets = append(q.GroupingSets, idxSet)
				}
			}
		} else {
			for _, g := range n.GroupClause {
				expr, err := a.transformExpr(g)
				if err != nil {
					return nil, fmt.Errorf("analyzer: GROUP BY: %w", err)
				}
				q.GroupClause = append(q.GroupClause, expr)
			}
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

// transformWithClause processes a WITH clause, analyzing each CTE and
// registering it in the analyzer's CTE map so FROM references can find it.
func (a *Analyzer) transformWithClause(wc *parser.WithClause, q *Query) error {
	if a.cteMap == nil {
		a.cteMap = make(map[string]*CTEDef)
	}

	for _, cte := range wc.CTEs {
		cteName := strings.ToLower(cte.Ctename)
		isRecursive := wc.Recursive

		selectStmt, ok := cte.Ctequery.(*parser.SelectStmt)
		if !ok {
			return fmt.Errorf("analyzer: CTE %q: only SELECT queries are supported", cteName)
		}

		if isRecursive {
			// For recursive CTEs, the query must be a UNION ALL with
			// the non-recursive term on the left and recursive term on the right.
			// Register a placeholder CTE first so the recursive term can
			// reference it.
			if selectStmt.Op != parser.SETOP_UNION {
				return fmt.Errorf("analyzer: recursive CTE %q must use UNION [ALL]", cteName)
			}

			// Analyze the non-recursive (initial) term first.
			savedRT := a.rangeTable
			savedCTEs := a.cteMap
			a.rangeTable = nil
			initQuery, err := a.transformSelectStmt(selectStmt.Larg)
			if err != nil {
				a.rangeTable = savedRT
				a.cteMap = savedCTEs
				return fmt.Errorf("analyzer: CTE %q initial term: %w", cteName, err)
			}
			a.rangeTable = savedRT
			a.cteMap = savedCTEs

			// Derive column info from the initial term.
			cols := cteColumnsFromQuery(cteName, cte.Aliascolnames, initQuery)

			// Register the CTE so the recursive term can self-reference.
			def := &CTEDef{
				Name:      cteName,
				Columns:   cols,
				Recursive: true,
			}
			a.cteMap[cteName] = def

			// Analyze the recursive term (which may reference the CTE itself).
			savedRT = a.rangeTable
			a.rangeTable = nil
			recQuery, err := a.transformSelectStmt(selectStmt.Rarg)
			if err != nil {
				a.rangeTable = savedRT
				return fmt.Errorf("analyzer: CTE %q recursive term: %w", cteName, err)
			}
			a.rangeTable = savedRT

			// Build a combined query that represents the full recursive CTE.
			// We store both parts in a SetOp query.
			combined := &Query{
				CommandType: CmdSelect,
				SetOp:       SetOpUnion,
				SetAll:       selectStmt.All,
				SetLeft:     initQuery,
				SetRight:    recQuery,
				TargetList:  initQuery.TargetList,
				RangeTable:  initQuery.RangeTable,
			}

			def.Query = combined
			q.CTEs = append(q.CTEs, def)
		} else {
			// Non-recursive CTE: analyze the subquery in isolation.
			savedRT := a.rangeTable
			savedCTEs := a.cteMap
			a.rangeTable = nil
			subQuery, err := a.transformSelectStmt(selectStmt)
			if err != nil {
				a.rangeTable = savedRT
				a.cteMap = savedCTEs
				return fmt.Errorf("analyzer: CTE %q: %w", cteName, err)
			}
			a.rangeTable = savedRT
			a.cteMap = savedCTEs

			cols := cteColumnsFromQuery(cteName, cte.Aliascolnames, subQuery)

			def := &CTEDef{
				Name:    cteName,
				Query:   subQuery,
				Columns: cols,
			}
			a.cteMap[cteName] = def
			q.CTEs = append(q.CTEs, def)
		}
	}
	return nil
}

// cteColumnsFromQuery derives RTEColumn metadata from a CTE's analyzed query.
func cteColumnsFromQuery(cteName string, aliasColNames []string, q *Query) []RTEColumn {
	cols := make([]RTEColumn, len(q.TargetList))
	for i, te := range q.TargetList {
		name := te.Name
		if i < len(aliasColNames) {
			name = aliasColNames[i]
		}
		cols[i] = RTEColumn{
			Name:   name,
			Type:   te.Expr.ResultType(),
			ColNum: int32(i + 1),
		}
	}
	return cols
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

		// Check if this references a CTE.
		if a.cteMap != nil {
			if cteDef, ok := a.cteMap[strings.ToLower(tableName)]; ok {
				return a.addCTERangeTableEntry(cteDef, alias)
			}
		}

		rte, err := a.addRangeTableEntryQualified(t.Schemaname, tableName, alias)
		if err != nil {
			return nil, err
		}
		// Apply column alias list: SELECT * FROM t AS a(x, y)
		if t.Alias != nil && len(t.Alias.Colnames) > 0 {
			for i, cn := range t.Alias.Colnames {
				if i < len(rte.Columns) {
					rte.Columns[i].Name = cn
				}
			}
		}
		return &RangeTblRef{RTIndex: rte.RTIndex}, nil

	case *parser.JoinExpr:
		return a.transformJoinExpr(t)

	case *parser.RangeSubselect:
		return a.transformRangeSubselect(t)

	case *parser.RangeTableSample:
		// TABLESAMPLE: process the inner relation, then attach sampling info.
		node, err := a.transformFromItem(t.Relation)
		if err != nil {
			return nil, err
		}
		// Attach sampling metadata to the RTE.
		if ref, ok := node.(*RangeTblRef); ok && ref.RTIndex > 0 && ref.RTIndex <= len(a.rangeTable) {
			rte := a.rangeTable[ref.RTIndex-1]
			rte.SampleMethod = strings.ToLower(t.Method)
			if len(t.Args) > 0 {
				rte.SamplePercent = parser.DeparseExpr(t.Args[0])
			}
		}
		return node, nil

	default:
		return nil, fmt.Errorf("analyzer: unsupported FROM item %T", item)
	}
}

// addCTERangeTableEntry creates a range table entry for a CTE reference.
func (a *Analyzer) addCTERangeTableEntry(cteDef *CTEDef, alias string) (JoinTreeNode, error) {
	rte := &RangeTblEntry{
		RTIndex:     len(a.rangeTable) + 1,
		RelName:     cteDef.Name,
		Alias:       alias,
		Columns:     cteDef.Columns,
		Subquery:    cteDef.Query,
		IsRecursive: cteDef.Recursive,
	}
	a.rangeTable = append(a.rangeTable, rte)
	return &RangeTblRef{RTIndex: rte.RTIndex}, nil
}

// transformRangeSubselect handles subqueries in FROM: (SELECT ...) AS alias.
func (a *Analyzer) transformRangeSubselect(rs *parser.RangeSubselect) (JoinTreeNode, error) {
	selectStmt, ok := rs.Subquery.(*parser.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("analyzer: subquery in FROM must be a SELECT")
	}

	alias := "subquery"
	if rs.Alias != nil && rs.Alias.Aliasname != "" {
		alias = rs.Alias.Aliasname
	}

	// Analyze the subquery. For LATERAL subqueries, keep the outer range
	// table visible so column references to preceding FROM items resolve.
	savedRT := a.rangeTable
	savedCTEs := a.cteMap
	outerRTCount := len(a.rangeTable)
	if !rs.Lateral {
		a.rangeTable = nil
	}
	subQuery, err := a.transformSelectStmt(selectStmt)
	if err != nil {
		a.rangeTable = savedRT
		a.cteMap = savedCTEs
		return nil, fmt.Errorf("analyzer: subquery %q: %w", alias, err)
	}
	a.rangeTable = savedRT
	a.cteMap = savedCTEs

	// For LATERAL subqueries, mark outer column references with AttIndex=-1
	// so they resolve via OuterRowContext at execution time rather than by
	// positional index (which would be wrong since the inner plan's rows
	// don't include outer columns).
	if rs.Lateral && outerRTCount > 0 {
		outerRTIs := make(map[int]bool, outerRTCount)
		for _, rte := range savedRT {
			outerRTIs[rte.RTIndex] = true
		}
		markOuterRefsInQuery(subQuery, outerRTIs)
	}

	// Build columns from the subquery's target list.
	cols := make([]RTEColumn, len(subQuery.TargetList))
	for i, te := range subQuery.TargetList {
		name := te.Name
		if rs.Alias != nil && i < len(rs.Alias.Colnames) {
			name = rs.Alias.Colnames[i]
		}
		cols[i] = RTEColumn{
			Name:   name,
			Type:   te.Expr.ResultType(),
			ColNum: int32(i + 1),
		}
	}

	rte := &RangeTblEntry{
		RTIndex:  len(a.rangeTable) + 1,
		RelName:  alias,
		Alias:    alias,
		Columns:  cols,
		Subquery: subQuery,
		Lateral:  rs.Lateral,
	}
	a.rangeTable = append(a.rangeTable, rte)
	return &RangeTblRef{RTIndex: rte.RTIndex}, nil
}

// markOuterRefsInQuery walks a Query's expressions and sets AttIndex=-1 on
// any ColumnVar that references an outer range table entry (identified by
// outerRTIs). This forces name-based resolution via OuterRowContext at
// execution time.
func markOuterRefsInQuery(q *Query, outerRTIs map[int]bool) {
	for _, te := range q.TargetList {
		markOuterRefsInExpr(te.Expr, outerRTIs)
	}
	if q.Qual != nil {
		markOuterRefsInExpr(q.Qual, outerRTIs)
	}
	if q.JoinTree != nil && q.JoinTree.Quals != nil {
		markOuterRefsInExpr(q.JoinTree.Quals, outerRTIs)
	}
	for _, sc := range q.SortClause {
		markOuterRefsInExpr(sc.Expr, outerRTIs)
	}
	for _, gb := range q.GroupClause {
		markOuterRefsInExpr(gb, outerRTIs)
	}
	if q.HavingQual != nil {
		markOuterRefsInExpr(q.HavingQual, outerRTIs)
	}
}

func markOuterRefsInExpr(ae AnalyzedExpr, outerRTIs map[int]bool) {
	if ae == nil {
		return
	}
	switch e := ae.(type) {
	case *ColumnVar:
		if outerRTIs[e.RTIndex] {
			e.AttIndex = -1
		}
	case *OpExpr:
		markOuterRefsInExpr(e.Left, outerRTIs)
		markOuterRefsInExpr(e.Right, outerRTIs)
	case *BoolExprNode:
		for _, arg := range e.Args {
			markOuterRefsInExpr(arg, outerRTIs)
		}
	case *FuncCallExpr:
		for _, arg := range e.Args {
			markOuterRefsInExpr(arg, outerRTIs)
		}
	case *AggRef:
		for _, arg := range e.Args {
			markOuterRefsInExpr(arg, outerRTIs)
		}
	case *TypeCastExpr:
		markOuterRefsInExpr(e.Arg, outerRTIs)
	case *NullTestExpr:
		markOuterRefsInExpr(e.Arg, outerRTIs)
	case *CaseExprNode:
		if e.Arg != nil {
			markOuterRefsInExpr(e.Arg, outerRTIs)
		}
		for _, w := range e.Whens {
			markOuterRefsInExpr(w.Cond, outerRTIs)
			markOuterRefsInExpr(w.Result, outerRTIs)
		}
		if e.ElseExpr != nil {
			markOuterRefsInExpr(e.ElseExpr, outerRTIs)
		}
	case *SubLinkExpr:
		// Don't descend into sublinks — they have their own scope.
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
	case parser.JOIN_FULL:
		jtype = JoinFull
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
		case parser.SVFOP_CURRENT_DATE:
			return &FuncCallExpr{FuncName: "current_date", Args: nil, ReturnType: tuple.TypeDate}, nil
		case parser.SVFOP_CURRENT_TIMESTAMP:
			return &FuncCallExpr{FuncName: "current_timestamp", Args: nil, ReturnType: tuple.TypeTimestamp}, nil
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

	case *parser.XmlExpr:
		return a.transformXmlExpr(e)

	case *parser.SubLink:
		return a.transformSubLink(e)

	case *parser.ParamRef:
		return nil, fmt.Errorf("analyzer: parameter references ($%d) not supported", e.Number)

	case *parser.A_ArrayExpr:
		// ARRAY[1, 2, 3] → evaluate elements and format as PG array literal.
		var elems []Expr
		for _, el := range e.Elements {
			ae, err := a.transformExpr(el)
			if err != nil {
				return nil, err
			}
			elems = append(elems, ae)
		}
		return &ArrayConstructExpr{Elements: elems}, nil

	case *parser.A_Indirection:
		// expr[idx] — array subscript or field access.
		arg, err := a.transformExpr(e.Arg)
		if err != nil {
			return nil, err
		}
		for _, ind := range e.Indirection {
			if idx, ok := ind.(*parser.A_Indices); ok {
				if idx.IsSlice {
					// Array slice: arr[lo:hi]
					var lower, upper AnalyzedExpr
					if idx.Lidx != nil {
						lower, err = a.transformExpr(idx.Lidx)
						if err != nil {
							return nil, err
						}
					}
					if idx.Uidx != nil {
						upper, err = a.transformExpr(idx.Uidx)
						if err != nil {
							return nil, err
						}
					}
					arg = &ArraySliceExpr{Array: arg, Lower: lower, Upper: upper}
				} else if idx.Uidx != nil {
					// Array subscript: arr[idx]
					idxExpr, err := a.transformExpr(idx.Uidx)
					if err != nil {
						return nil, err
					}
					arg = &ArraySubscriptExpr{Array: arg, Index: idxExpr}
				}
			}
		}
		return arg, nil

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
	case parser.AEXPR_SIMILAR:
		return a.transformSimilarTo(e)
	case parser.AEXPR_DISTINCT:
		return a.transformDistinctFrom(e, false)
	case parser.AEXPR_NOT_DISTINCT:
		return a.transformDistinctFrom(e, true)
	}

	// Row value comparisons: (a, b) op (c, d).
	if leftRow, ok := e.Lexpr.(*parser.RowExpr); ok {
		if rightRow, ok := e.Rexpr.(*parser.RowExpr); ok {
			return a.transformRowCompare(leftRow, rightRow, e.Name)
		}
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
		if left.ResultType() == tuple.TypeJSON {
			op = OpJSONDelete
			resultTyp = tuple.TypeJSON
		} else {
			op = OpSub
			resultTyp = inferArithType(left, right)
		}
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
	case "->":
		op = OpJSONArrow
		resultTyp = tuple.TypeJSON
	case "->>":
		op = OpJSONArrowText
		resultTyp = tuple.TypeText
	case "#>":
		op = OpJSONHashArrow
		resultTyp = tuple.TypeJSON
	case "#>>":
		op = OpJSONHashArrowText
		resultTyp = tuple.TypeText
	case "@>":
		if left != nil && left.ResultType() == tuple.TypeJSON {
			op = OpJSONContains
		} else {
			op = OpArrayContains
		}
		resultTyp = tuple.TypeBool
	case "<@":
		if left != nil && left.ResultType() == tuple.TypeJSON {
			op = OpJSONContainedBy
		} else {
			op = OpArrayContainedBy
		}
		resultTyp = tuple.TypeBool
	case "&&":
		op = OpArrayOverlap
		resultTyp = tuple.TypeBool
	case "?":
		op = OpJSONExists
		resultTyp = tuple.TypeBool
	case "?|":
		op = OpJSONExistsAny
		resultTyp = tuple.TypeBool
	case "?&":
		op = OpJSONExistsAll
		resultTyp = tuple.TypeBool
	case "#-":
		op = OpJSONDeletePath
		resultTyp = tuple.TypeJSON
	case "~":
		op = OpRegexMatch
		resultTyp = tuple.TypeBool
	case "~*":
		op = OpRegexIMatch
		resultTyp = tuple.TypeBool
	case "!~":
		op = OpRegexNotMatch
		resultTyp = tuple.TypeBool
	case "!~*":
		op = OpRegexNotIMatch
		resultTyp = tuple.TypeBool
	case "^@":
		op = OpStartsWith
		resultTyp = tuple.TypeBool
	case "@@":
		op = OpTSMatch
		resultTyp = tuple.TypeBool
	case "<->":
		op = OpGeomDistance
		resultTyp = tuple.TypeFloat64
	case "~=":
		op = OpGeomSame
		resultTyp = tuple.TypeBool
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

// transformSimilarTo handles [NOT] SIMILAR TO expressions.
func (a *Analyzer) transformSimilarTo(e *parser.A_Expr) (AnalyzedExpr, error) {
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
	op := OpSimilarTo
	if opName == "!~" || opName == "!~*" {
		op = OpNotSimilarTo
	}
	// Check for NOT SIMILAR TO via the Name list.
	for _, n := range e.Name {
		if strings.EqualFold(n, "!") || strings.EqualFold(n, "not") {
			op = OpNotSimilarTo
			break
		}
	}
	return &OpExpr{Op: op, Left: left, Right: right, ResultTyp: tuple.TypeBool}, nil
}

// transformBetween desugars BETWEEN into AND/OR comparisons.
// x BETWEEN a AND b  →  x >= a AND x <= b
// x NOT BETWEEN a AND b  →  x < a OR x > b
// transformRowCompare expands (a, b, ...) op (x, y, ...) into scalar comparisons.
// For = and !=: element-wise AND/OR.
// For <, >, <=, >=: lexicographic comparison.
func (a *Analyzer) transformRowCompare(left, right *parser.RowExpr, opNames []string) (AnalyzedExpr, error) {
	if len(left.Args) != len(right.Args) {
		return nil, fmt.Errorf("unequal number of entries in row expressions")
	}
	if len(left.Args) == 0 {
		return &Const{Value: tuple.DBool(true), ConstType: tuple.TypeBool}, nil
	}

	op := ""
	if len(opNames) > 0 {
		op = opNames[len(opNames)-1]
	}

	// Transform all element pairs.
	n := len(left.Args)
	leftExprs := make([]AnalyzedExpr, n)
	rightExprs := make([]AnalyzedExpr, n)
	for i := 0; i < n; i++ {
		var err error
		leftExprs[i], err = a.transformExpr(left.Args[i])
		if err != nil {
			return nil, err
		}
		rightExprs[i], err = a.transformExpr(right.Args[i])
		if err != nil {
			return nil, err
		}
	}

	switch op {
	case "=":
		// (a,b) = (x,y) → a=x AND b=y
		var conds []AnalyzedExpr
		for i := 0; i < n; i++ {
			conds = append(conds, &OpExpr{Op: OpEq, Left: leftExprs[i], Right: rightExprs[i]})
		}
		return andAll(conds), nil

	case "<>", "!=":
		// (a,b) <> (x,y) → a<>x OR b<>y
		var conds []AnalyzedExpr
		for i := 0; i < n; i++ {
			conds = append(conds, &OpExpr{Op: OpNeq, Left: leftExprs[i], Right: rightExprs[i]})
		}
		return orAll(conds), nil

	case "<", ">", "<=", ">=":
		// Lexicographic: (a,b) < (x,y) → a<x OR (a=x AND b<y)
		var ltOp, eqLeOp OpKind
		switch op {
		case "<":
			ltOp, eqLeOp = OpLt, OpLt
		case ">":
			ltOp, eqLeOp = OpGt, OpGt
		case "<=":
			ltOp, eqLeOp = OpLt, OpLte
		case ">=":
			ltOp, eqLeOp = OpGt, OpGte
		}
		// Build from right to left.
		// Last element: a[n-1] op b[n-1] (using eqLeOp for <= / >=)
		result := AnalyzedExpr(&OpExpr{Op: eqLeOp, Left: leftExprs[n-1], Right: rightExprs[n-1]})
		for i := n - 2; i >= 0; i-- {
			// a[i] < b[i] OR (a[i] = b[i] AND <rest>)
			strict := &OpExpr{Op: ltOp, Left: leftExprs[i], Right: rightExprs[i]}
			eq := &OpExpr{Op: OpEq, Left: leftExprs[i], Right: rightExprs[i]}
			eqAndRest := &BoolExprNode{Op: BoolAnd, Args: []AnalyzedExpr{eq, result}}
			result = &BoolExprNode{Op: BoolOr, Args: []AnalyzedExpr{strict, eqAndRest}}
		}
		return result, nil

	default:
		return nil, fmt.Errorf("unsupported row comparison operator: %s", op)
	}
}

// andAll combines multiple conditions with AND.
func andAll(conds []AnalyzedExpr) AnalyzedExpr {
	if len(conds) == 1 {
		return conds[0]
	}
	return &BoolExprNode{Op: BoolAnd, Args: conds}
}

// orAll combines multiple conditions with OR.
func orAll(conds []AnalyzedExpr) AnalyzedExpr {
	if len(conds) == 1 {
		return conds[0]
	}
	return &BoolExprNode{Op: BoolOr, Args: conds}
}

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
	switch e.Kind {
	case parser.AEXPR_NOT_BETWEEN:
		// x NOT BETWEEN a AND b → x < a OR x > b
		return &BoolExprNode{
			Op: BoolOr,
			Args: []AnalyzedExpr{
				&OpExpr{Op: OpLt, Left: left, Right: low, ResultTyp: tuple.TypeBool},
				&OpExpr{Op: OpGt, Left: left, Right: high, ResultTyp: tuple.TypeBool},
			},
		}, nil
	case parser.AEXPR_NOT_BETWEEN_SYM:
		// x NOT BETWEEN SYMMETRIC a AND b → NOT ((x >= a AND x <= b) OR (x >= b AND x <= a))
		return &BoolExprNode{
			Op: BoolNot,
			Args: []AnalyzedExpr{
				&BoolExprNode{
					Op: BoolOr,
					Args: []AnalyzedExpr{
						&BoolExprNode{Op: BoolAnd, Args: []AnalyzedExpr{
							&OpExpr{Op: OpGte, Left: left, Right: low, ResultTyp: tuple.TypeBool},
							&OpExpr{Op: OpLte, Left: left, Right: high, ResultTyp: tuple.TypeBool},
						}},
						&BoolExprNode{Op: BoolAnd, Args: []AnalyzedExpr{
							&OpExpr{Op: OpGte, Left: left, Right: high, ResultTyp: tuple.TypeBool},
							&OpExpr{Op: OpLte, Left: left, Right: low, ResultTyp: tuple.TypeBool},
						}},
					},
				},
			},
		}, nil
	case parser.AEXPR_BETWEEN_SYM:
		// x BETWEEN SYMMETRIC a AND b → (x >= a AND x <= b) OR (x >= b AND x <= a)
		return &BoolExprNode{
			Op: BoolOr,
			Args: []AnalyzedExpr{
				&BoolExprNode{Op: BoolAnd, Args: []AnalyzedExpr{
					&OpExpr{Op: OpGte, Left: left, Right: low, ResultTyp: tuple.TypeBool},
					&OpExpr{Op: OpLte, Left: left, Right: high, ResultTyp: tuple.TypeBool},
				}},
				&BoolExprNode{Op: BoolAnd, Args: []AnalyzedExpr{
					&OpExpr{Op: OpGte, Left: left, Right: high, ResultTyp: tuple.TypeBool},
					&OpExpr{Op: OpLte, Left: left, Right: low, ResultTyp: tuple.TypeBool},
				}},
			},
		}, nil
	default:
		// x BETWEEN a AND b → x >= a AND x <= b
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
// transformSubLink handles subquery expressions: EXISTS, IN, NOT IN, ANY, ALL, scalar.
func (a *Analyzer) transformXmlExpr(xe *parser.XmlExpr) (AnalyzedExpr, error) {
	// Map XmlExpr AST nodes to our built-in function calls.
	var funcName string
	switch xe.Op {
	case parser.IS_XMLCONCAT:
		funcName = "xmlconcat"
	case parser.IS_XMLELEMENT:
		funcName = "xmlelement"
	case parser.IS_XMLFOREST:
		funcName = "xmlforest"
	case parser.IS_XMLPARSE:
		funcName = "xmlparse"
	case parser.IS_XMLSERIALIZE:
		funcName = "xmlserialize"
	case parser.IS_XMLPI:
		funcName = "xmlelement"
	case parser.IS_XMLROOT:
		funcName = "xmlparse"
	case parser.IS_XMLEXISTS:
		funcName = "xmlparse"
	default:
		funcName = "xmlparse"
	}

	var args []AnalyzedExpr
	// For XMLELEMENT, prepend the element name as a string literal.
	if xe.Op == parser.IS_XMLELEMENT && xe.Name != "" {
		args = append(args, &Const{Value: tuple.DText(xe.Name), ConstType: tuple.TypeText})
	}
	for _, arg := range xe.Args {
		ae, err := a.transformExpr(arg)
		if err != nil {
			return nil, err
		}
		args = append(args, ae)
	}

	return &FuncCallExpr{
		FuncName:   funcName,
		Args:       args,
		ReturnType: tuple.TypeText,
	}, nil
}

func (a *Analyzer) transformSubLink(sl *parser.SubLink) (AnalyzedExpr, error) {
	selectStmt, ok := sl.Subselect.(*parser.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("analyzer: sublink subquery must be a SELECT")
	}

	// Analyze the subquery with the outer range table visible so
	// correlated references (e.g., WHERE t2.fk = t.id) can resolve.
	// The subquery adds its own entries; we restore the outer RT after.
	savedRT := a.rangeTable
	outerRTLen := len(savedRT) // RTEs with index <= this are outer refs
	subQuery, err := a.transformSelectStmt(selectStmt)
	if err != nil {
		a.rangeTable = savedRT
		return nil, fmt.Errorf("analyzer: subquery: %w", err)
	}
	a.rangeTable = savedRT

	// Strip outer RTEs from the subquery's range table so the planner
	// only sees the subquery's own tables. Mark outer column references
	// with AttIndex = -1 so they resolve via name-based lookup against
	// OuterRowContext at execution time. Adjust inner AttIndex values
	// to account for the removed outer columns.
	if outerRTLen > 0 && subQuery.RangeTable != nil {
		// Count total columns in outer RTEs to adjust inner AttIndex.
		outerColCount := 0
		for i := 0; i < outerRTLen && i < len(subQuery.RangeTable); i++ {
			outerColCount += len(subQuery.RangeTable[i].Columns)
		}
		markOuterColumnVars(subQuery, outerRTLen, outerColCount)
		subQuery.RangeTable = subQuery.RangeTable[outerRTLen:]
		// Re-number RTIndex in the remaining RTEs.
		for i, rte := range subQuery.RangeTable {
			rte.RTIndex = i + 1
		}
	}

	// Determine the return type from the subquery's first target column.
	subRetType := tuple.TypeNull
	if len(subQuery.TargetList) > 0 {
		subRetType = subQuery.TargetList[0].Expr.ResultType()
	}

	switch sl.SubLinkType {
	case parser.EXISTS_SUBLINK:
		return &SubLinkExpr{
			LinkType:      SubLinkExists,
			Subquery:      subQuery,
			SubReturnType: subRetType,
		}, nil

	case parser.ANY_SUBLINK:
		// expr IN (SELECT ...) or expr = ANY (SELECT ...)
		var testExpr AnalyzedExpr
		if sl.Testexpr != nil {
			testExpr, err = a.transformExpr(sl.Testexpr)
			if err != nil {
				return nil, fmt.Errorf("analyzer: IN subquery test expr: %w", err)
			}
		}
		opName := "="
		if len(sl.OperName) > 0 {
			opName = sl.OperName[0]
		}
		return &SubLinkExpr{
			LinkType:      SubLinkAny,
			TestExpr:      testExpr,
			OpName:        opName,
			Subquery:      subQuery,
			SubReturnType: subRetType,
		}, nil

	case parser.ALL_SUBLINK:
		// expr NOT IN (SELECT ...) or expr <> ALL (SELECT ...)
		var testExpr AnalyzedExpr
		if sl.Testexpr != nil {
			testExpr, err = a.transformExpr(sl.Testexpr)
			if err != nil {
				return nil, fmt.Errorf("analyzer: ALL subquery test expr: %w", err)
			}
		}
		opName := "="
		if len(sl.OperName) > 0 {
			opName = sl.OperName[0]
		}
		return &SubLinkExpr{
			LinkType:      SubLinkAll,
			TestExpr:      testExpr,
			OpName:        opName,
			Subquery:      subQuery,
			SubReturnType: subRetType,
		}, nil

	case parser.EXPR_SUBLINK:
		// Scalar subquery: (SELECT count(*) FROM ...)
		return &SubLinkExpr{
			LinkType:      SubLinkExprSubquery,
			Subquery:      subQuery,
			SubReturnType: subRetType,
		}, nil

	default:
		return nil, fmt.Errorf("analyzer: unsupported sublink type %d", sl.SubLinkType)
	}
}

// markOuterColumnVars walks the subquery's expression trees and marks
// ColumnVar nodes that reference outer RTEs (RTIndex <= outerRTLen)
// with AttIndex = -1 for name-based resolution. It also adjusts
// RTIndex and AttIndex for inner references to account for the
// stripped outer RTEs and their columns.
func markOuterColumnVars(q *Query, outerRTLen int, outerColCount int) {
	if q == nil {
		return
	}
	for _, te := range q.TargetList {
		markOuterCV(te.Expr, outerRTLen, outerColCount)
	}
	if q.JoinTree != nil {
		if q.JoinTree.Quals != nil {
			markOuterCV(q.JoinTree.Quals, outerRTLen, outerColCount)
		}
		for _, item := range q.JoinTree.FromList {
			markOuterCVJoinTree(item, outerRTLen, outerColCount)
		}
	}
	if q.HavingQual != nil {
		markOuterCV(q.HavingQual, outerRTLen, outerColCount)
	}
	for _, sc := range q.SortClause {
		markOuterCV(sc.Expr, outerRTLen, outerColCount)
	}
}

func markOuterCVJoinTree(node JoinTreeNode, outerRTLen int, outerColCount int) {
	switch n := node.(type) {
	case *RangeTblRef:
		if n.RTIndex > outerRTLen {
			n.RTIndex -= outerRTLen
		}
	case *JoinNode:
		markOuterCVJoinTree(n.Left, outerRTLen, outerColCount)
		markOuterCVJoinTree(n.Right, outerRTLen, outerColCount)
		if n.Quals != nil {
			markOuterCV(n.Quals, outerRTLen, outerColCount)
		}
		if n.LeftRTI > outerRTLen {
			n.LeftRTI -= outerRTLen
		}
		if n.RightRTI > outerRTLen {
			n.RightRTI -= outerRTLen
		}
	}
}

func markOuterCV(ae AnalyzedExpr, outerRTLen int, outerColCount int) {
	if ae == nil {
		return
	}
	switch e := ae.(type) {
	case *ColumnVar:
		if e.RTIndex > 0 && e.RTIndex <= outerRTLen {
			// Outer reference: force name-based resolution.
			e.AttIndex = -1
		} else if e.RTIndex > outerRTLen {
			// Inner reference: adjust RTIndex and AttIndex.
			e.RTIndex -= outerRTLen
			if e.AttIndex >= outerColCount {
				e.AttIndex -= outerColCount
			}
		}
	case *OpExpr:
		markOuterCV(e.Left, outerRTLen, outerColCount)
		markOuterCV(e.Right, outerRTLen, outerColCount)
	case *BoolExprNode:
		for _, arg := range e.Args {
			markOuterCV(arg, outerRTLen, outerColCount)
		}
	case *NullTestExpr:
		markOuterCV(e.Arg, outerRTLen, outerColCount)
	case *FuncCallExpr:
		for _, arg := range e.Args {
			markOuterCV(arg, outerRTLen, outerColCount)
		}
	case *TypeCastExpr:
		markOuterCV(e.Arg, outerRTLen, outerColCount)
	case *AggRef:
		for _, arg := range e.Args {
			markOuterCV(arg, outerRTLen, outerColCount)
		}
	case *CaseExprNode:
		if e.Arg != nil {
			markOuterCV(e.Arg, outerRTLen, outerColCount)
		}
		for _, w := range e.Whens {
			markOuterCV(w.Cond, outerRTLen, outerColCount)
			markOuterCV(w.Result, outerRTLen, outerColCount)
		}
		if e.ElseExpr != nil {
			markOuterCV(e.ElseExpr, outerRTLen, outerColCount)
		}
	case *SubLinkExpr:
		// Do NOT process TestExpr here — it belongs to the parent query,
		// not the subquery. The subquery's own outer refs are handled
		// when transformSubLink processes the inner subquery.
	}
}

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
// transformValuesClause handles bare VALUES (a, b), (c, d) as a standalone query.
// Produces a CmdSelect query with synthetic column names (column1, column2, ...).
// Multiple rows are handled by evaluating all value expressions and storing
// them in the Values field, which the planner converts to a LogicalValues node.
func (a *Analyzer) transformValuesClause(n *parser.SelectStmt) (*Query, error) {
	q := &Query{CommandType: CmdSelect, IsValues: true}

	// Determine column count from the first row.
	if len(n.ValuesLists) == 0 {
		return nil, fmt.Errorf("analyzer: empty VALUES clause")
	}
	numCols := len(n.ValuesLists[0])

	// Build synthetic target list: column1, column2, ...
	for i := 0; i < numCols; i++ {
		colName := fmt.Sprintf("column%d", i+1)
		q.TargetList = append(q.TargetList, &TargetEntry{
			Name: colName,
			Expr: &Const{Value: tuple.DNull(), ConstType: tuple.TypeNull}, // placeholder
		})
	}

	// Transform all value rows.
	for rowIdx, row := range n.ValuesLists {
		if len(row) != numCols {
			return nil, fmt.Errorf("analyzer: VALUES row %d has %d columns, expected %d", rowIdx+1, len(row), numCols)
		}
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

	return q, nil
}

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

	// ON CONFLICT clause.
	if n.OnConflict != nil {
		oc, err := a.transformOnConflict(n.OnConflict, rte)
		if err != nil {
			return nil, fmt.Errorf("analyzer: INSERT ON CONFLICT: %w", err)
		}
		q.OnConflict = oc
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

// transformOnConflict resolves an ON CONFLICT clause.
func (a *Analyzer) transformOnConflict(oc *parser.OnConflictClause, targetRTE *RangeTblEntry) (*OnConflictClause, error) {
	result := &OnConflictClause{}

	switch oc.Action {
	case parser.ONCONFLICT_NOTHING:
		result.Action = OnConflictNothing
	case parser.ONCONFLICT_UPDATE:
		result.Action = OnConflictUpdate
	default:
		return nil, fmt.Errorf("unsupported ON CONFLICT action")
	}

	// Resolve conflict target columns.
	if oc.Infer != nil {
		for _, elem := range oc.Infer.IndexElems {
			s, ok := elem.(*parser.String)
			if !ok {
				return nil, fmt.Errorf("unsupported conflict target element %T", elem)
			}
			// Verify column exists in target table.
			found := false
			for _, col := range targetRTE.Columns {
				if strings.EqualFold(col.Name, s.Str) {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("column %q does not exist in table %q", s.Str, targetRTE.RelName)
			}
			result.ConflictCols = append(result.ConflictCols, s.Str)
		}
	}

	// For DO UPDATE, resolve SET assignments.
	// Add an "excluded" pseudo-RTE so EXCLUDED.col references resolve.
	if result.Action == OnConflictUpdate {
		excludedRTE := &RangeTblEntry{
			RTIndex: len(a.rangeTable) + 1,
			RelName: "excluded",
			Alias:   "excluded",
			Columns: make([]RTEColumn, len(targetRTE.Columns)),
		}
		copy(excludedRTE.Columns, targetRTE.Columns)
		a.rangeTable = append(a.rangeTable, excludedRTE)

		for _, rt := range oc.TargetList {
			colName := rt.Name
			var colNum int32
			var colType tuple.DatumType
			found := false
			for _, col := range targetRTE.Columns {
				if strings.EqualFold(col.Name, colName) {
					colNum = col.ColNum
					colType = col.Type
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("column %q of relation %q does not exist", colName, targetRTE.RelName)
			}

			val, err := a.transformExpr(rt.Val)
			if err != nil {
				return nil, err
			}

			result.Assignments = append(result.Assignments, &UpdateAssignment{
				ColName: colName,
				ColNum:  colNum,
				ColType: colType,
				Expr:    val,
			})
		}

		// Optional WHERE clause on DO UPDATE.
		if oc.WhereClause != nil {
			w, err := a.transformExpr(oc.WhereClause)
			if err != nil {
				return nil, err
			}
			result.WhereClause = w
		}
	}

	return result, nil
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

	alias := ""
	if n.Relation.Alias != nil && n.Relation.Alias.Aliasname != "" {
		alias = n.Relation.Alias.Aliasname
	}
	rte, err := a.addRangeTableEntryQualified(n.Relation.Schemaname, n.Relation.Relname, alias)
	if err != nil {
		return nil, err
	}
	q.ResultRelation = rte.RTIndex

	// Build join tree: target table + optional FROM tables.
	fromList := []JoinTreeNode{&RangeTblRef{RTIndex: rte.RTIndex}}

	// Handle UPDATE ... FROM clause — add extra tables to the range table.
	if len(n.FromClause) > 0 {
		extraItems, err := a.transformFromClause(n.FromClause)
		if err != nil {
			return nil, fmt.Errorf("analyzer: UPDATE FROM: %w", err)
		}
		fromList = append(fromList, extraItems...)
	}

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
	var foreignKeys []ForeignKeyDef
	var inheritParents []string

	// INHERITS: copy columns from parent table(s).
	for _, inhNode := range n.InhRelations {
		if rv, ok := inhNode.(*parser.RangeVar); ok && a.Cat != nil {
			parentName := rv.Relname
			inheritParents = append(inheritParents, parentName)
			parentRel, _ := a.Cat.FindRelation(parentName)
			if parentRel != nil {
				parentCols, _ := a.Cat.GetColumns(parentRel.OID)
				for _, pc := range parentCols {
					cols = append(cols, ColDef{
						Name:          pc.Name,
						Type:          tuple.DatumType(pc.Type),
						Typmod:        pc.Typmod,
						NotNull:       pc.NotNull,
						DefaultExpr:   pc.DefaultExpr,
						GeneratedExpr: pc.GeneratedExpr,
					})
				}
			}
		}
	}

	for _, elt := range n.TableElts {
		// Handle LIKE source_table: copy columns from the source table.
		if like, ok := elt.(*parser.TableLikeClause); ok {
			if like.Relation != nil && a.Cat != nil {
				srcName := like.Relation.Relname
				srcRel, _ := a.Cat.FindRelation(srcName)
				if srcRel != nil {
					srcCols, _ := a.Cat.GetColumns(srcRel.OID)
					for _, sc := range srcCols {
						cols = append(cols, ColDef{
							Name:          sc.Name,
							Type:          tuple.DatumType(sc.Type),
							Typmod:        sc.Typmod,
							NotNull:       sc.NotNull,
							DefaultExpr:   sc.DefaultExpr,
							GeneratedExpr: sc.GeneratedExpr,
						})
					}
				}
			}
			continue
		}
		colDef, ok := elt.(*parser.ColumnDef)
		if !ok {
			continue
		}
		sqlType := typeNameToString(colDef.TypeName)
		// Check for array type (TypeName has ArrayBounds set).
		if colDef.TypeName != nil && len(colDef.TypeName.ArrayBounds) > 0 {
			sqlType = sqlType + "[]"
		}
		dt := a.resolveColumnType(sqlType)
		notNull := false
		primaryKey := false
		unique := false
		defaultExpr := ""
		checkExpr := ""
		checkName := ""
		generatedExpr := ""
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
			case parser.CONSTR_CHECK:
				if c.RawExpr != nil {
					checkExpr = parser.DeparseExpr(c.RawExpr)
					checkName = c.Conname
				}
			case parser.CONSTR_GENERATED:
				if c.RawExpr != nil {
					generatedExpr = parser.DeparseExpr(c.RawExpr)
				}
			case parser.CONSTR_IDENTITY:
				// GENERATED {ALWAYS | BY DEFAULT} AS IDENTITY
				// Auto-create a sequence and set the default to nextval().
				seqName := tableName + "_" + colDef.Colname + "_seq"
				defaultExpr = "nextval('" + seqName + "')"
				notNull = true
			case parser.CONSTR_FOREIGN:
				// Column-level REFERENCES: single column FK.
				refTable := ""
				if c.PkTable != nil {
					refTable = c.PkTable.Relname
				}
				fk := ForeignKeyDef{
					Name:       c.Conname,
					Columns:    []string{colDef.Colname},
					RefTable:   refTable,
					RefColumns: c.PkAttrs,
					OnDelete:   c.FkDelAction,
					OnUpdate:   c.FkUpdAction,
				}
				foreignKeys = append(foreignKeys, fk)
			}
		}
		typmod := computeTypmodFromParser(colDef.TypeName, dt)
		// SERIAL/BIGSERIAL: auto-create sequence default if not already set.
		upperType := strings.ToUpper(sqlType)
		if (upperType == "SERIAL" || upperType == "BIGSERIAL" || upperType == "SMALLSERIAL") && defaultExpr == "" {
			seqName := tableName + "_" + colDef.Colname + "_seq"
			defaultExpr = "nextval('" + seqName + "')"
			notNull = true
		}
		cols = append(cols, ColDef{Name: colDef.Colname, Type: dt, TypeName: sqlType, Typmod: typmod, NotNull: notNull, PrimaryKey: primaryKey, Unique: unique, DefaultExpr: defaultExpr, CheckExpr: checkExpr, CheckName: checkName, GeneratedExpr: generatedExpr})
	}

	// Handle table-level constraints (e.g., PRIMARY KEY (col), UNIQUE (col), CHECK, FOREIGN KEY).
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
		case parser.CONSTR_CHECK:
			// Table-level CHECK: attach to the first column referenced,
			// or store as a table-level check on the first column.
			if con.RawExpr != nil {
				expr := parser.DeparseExpr(con.RawExpr)
				if len(cols) > 0 {
					// Store on first column; the executor validates at table level.
					cols[0].CheckExpr = expr
					cols[0].CheckName = con.Conname
				}
			}
		case parser.CONSTR_FOREIGN:
			refTable := ""
			if con.PkTable != nil {
				refTable = con.PkTable.Relname
			}
			fk := ForeignKeyDef{
				Name:       con.Conname,
				Columns:    con.FkAttrs,
				RefTable:   refTable,
				RefColumns: con.PkAttrs,
				OnDelete:   con.FkDelAction,
				OnUpdate:   con.FkUpdAction,
			}
			foreignKeys = append(foreignKeys, fk)
		case parser.CONSTR_EXCLUSION:
			// EXCLUDE constraints: accepted but not enforced.
		}
	}

	// Extract partition spec if present.
	var partStrategy string
	var partKeyCols []string
	if n.PartitionSpec != nil {
		partStrategy = strings.ToLower(n.PartitionSpec.Strategy)
		for _, pe := range n.PartitionSpec.PartParams {
			if pe.Name != "" {
				partKeyCols = append(partKeyCols, pe.Name)
			}
		}
	}

	return a.makeUtilityQuery(UtilCreateTable, &UtilityStmt{
		Type: UtilCreateTable, TableName: tableName, TableSchema: schemaName,
		Columns: cols, ForeignKeys: foreignKeys,
		IsTemp:            n.Persistence == parser.RELPERSISTENCE_TEMP,
		PartitionStrategy: partStrategy,
		PartitionKeyCols:  partKeyCols,
		InheritParents:    inheritParents,
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
		return tuple.TypeArray
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
		return tuple.TypeNumeric
	case "BOOL", "BOOLEAN":
		return tuple.TypeBool
	case "TEXT", "VARCHAR", "CHAR", "CHARACTER", "CHARACTER VARYING",
		"STRING", "BPCHAR", "NAME":
		return tuple.TypeText
	case "TIMESTAMP", "TIMESTAMPTZ",
		"TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP WITH TIME ZONE":
		return tuple.TypeTimestamp
	case "DATE":
		return tuple.TypeDate
	case "TIME", "TIMETZ", "TIME WITHOUT TIME ZONE", "TIME WITH TIME ZONE":
		return tuple.TypeText
	case "INTERVAL":
		return tuple.TypeInterval
	case "JSON", "JSONB":
		return tuple.TypeJSON
	case "UUID":
		return tuple.TypeUUID
	case "BYTEA":
		return tuple.TypeBytea
	case "MONEY":
		return tuple.TypeMoney
	case "TSVECTOR", "TSQUERY":
		return tuple.TypeText
	case "INET", "CIDR", "MACADDR", "MACADDR8":
		return tuple.TypeText
	case "POINT", "LINE", "LSEG", "BOX", "PATH", "POLYGON", "CIRCLE":
		return tuple.TypeText
	case "XML":
		return tuple.TypeText
	case "INT4RANGE", "INT8RANGE", "NUMRANGE", "TSRANGE", "TSTZRANGE", "DATERANGE":
		return tuple.TypeText
	case "RECORD", "VOID":
		return tuple.TypeText
	case "OID", "REGCLASS", "REGTYPE", "REGPROC":
		return tuple.TypeInt64
	case "EVENT_TRIGGER", "TRIGGER":
		return tuple.TypeText
	case "TEXT[]", "INT[]", "INTEGER[]", "INT4[]", "INT8[]", "FLOAT8[]", "BOOLEAN[]":
		return tuple.TypeArray
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
		return tuple.TypeBytea
	}

	return tuple.TypeText
}

// computeTypmodFromParser extracts precision/scale from a parser TypeName
// and encodes it as a PostgreSQL-compatible typmod. Returns -1 if not applicable.
func computeTypmodFromParser(tn *parser.TypeName, dt tuple.DatumType) int32 {
	if dt != tuple.TypeNumeric || tn == nil || len(tn.Typmods) == 0 {
		return -1
	}
	// Extract integer constants from Typmods.
	vals := make([]int, 0, len(tn.Typmods))
	for _, tm := range tn.Typmods {
		if c, ok := tm.(*parser.A_Const); ok && c.Val.Type == parser.ValInt {
			vals = append(vals, int(c.Val.Ival))
		}
	}
	if len(vals) == 0 {
		return -1
	}
	p := vals[0]
	s := 0
	if len(vals) >= 2 {
		s = vals[1]
	}
	if p <= 0 {
		return -1
	}
	// PostgreSQL encoding: ((precision << 16) | scale) + VARHDRSZ(4)
	return int32((p << 16) | s) + 4
}

// NumericTypmodPrecisionScale extracts precision and scale from a typmod.
// Returns (0, 0, false) if the typmod is unspecified.
func NumericTypmodPrecisionScale(typmod int32) (precision, scale int, ok bool) {
	if typmod < 4 {
		return 0, 0, false
	}
	tm := typmod - 4
	return int(tm >> 16), int(tm & 0xFFFF), true
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

	// Extract trigger function arguments as string values.
	var trigArgs []string
	for _, arg := range n.Args {
		switch a := arg.(type) {
		case *parser.A_Const:
			trigArgs = append(trigArgs, a.Val.Str)
		case *parser.ColumnRef:
			// Fields are []Node containing *parser.String nodes.
			if len(a.Fields) > 0 {
				if s, ok := a.Fields[len(a.Fields)-1].(*parser.String); ok {
					trigArgs = append(trigArgs, s.Str)
				}
			}
		default:
			trigArgs = append(trigArgs, fmt.Sprintf("%v", a))
		}
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
		TrigArgs:    trigArgs,
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
	case parser.OBJECT_TABLE, parser.OBJECT_MATVIEW:
		tableName := ""
		if len(n.Objects) > 0 && len(n.Objects[0]) > 0 {
			tableName = n.Objects[0][len(n.Objects[0])-1]
		}
		return a.makeUtilityQuery(UtilDropTable, &UtilityStmt{
			Type:          UtilDropTable,
			TableName:     tableName,
			DropMissingOk: n.MissingOk,
			DropCascade:   n.Behavior == parser.DROP_CASCADE,
		}), nil
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
// collectWindowFuncs walks an expression tree and collects WindowFuncRef nodes.
func collectWindowFuncs(ae AnalyzedExpr, out *[]*WindowFuncRef) {
	if ae == nil {
		return
	}
	switch e := ae.(type) {
	case *WindowFuncRef:
		*out = append(*out, e)
	case *OpExpr:
		collectWindowFuncs(e.Left, out)
		collectWindowFuncs(e.Right, out)
	case *FuncCallExpr:
		for _, arg := range e.Args {
			collectWindowFuncs(arg, out)
		}
	case *TypeCastExpr:
		collectWindowFuncs(e.Arg, out)
	}
}

// isWindowOnlyFunc returns true for functions that are exclusively window
// functions (not regular aggregates).
func isWindowOnlyFunc(name string) bool {
	switch name {
	case "row_number", "rank", "dense_rank", "percent_rank", "cume_dist",
		"ntile", "lag", "lead", "first_value", "last_value", "nth_value":
		return true
	}
	return false
}

// windowFuncReturnType determines the return type for a window function.
func windowFuncReturnType(name string, args []AnalyzedExpr) tuple.DatumType {
	switch name {
	case "row_number", "rank", "dense_rank", "ntile":
		return tuple.TypeInt64
	case "percent_rank", "cume_dist":
		return tuple.TypeFloat64
	case "lag", "lead", "first_value", "last_value", "nth_value":
		if len(args) > 0 {
			return args[0].ResultType()
		}
		return tuple.TypeNull
	// Aggregate-as-window: inherit from the aggregate.
	case "count":
		return tuple.TypeInt64
	case "sum":
		if len(args) > 0 {
			return args[0].ResultType()
		}
		return tuple.TypeInt64
	case "avg":
		return tuple.TypeFloat64
	case "min", "max":
		if len(args) > 0 {
			return args[0].ResultType()
		}
		return tuple.TypeNull
	default:
		return tuple.TypeNull
	}
}

// transformWindowFunc creates a WindowFuncRef from a FuncCall with OVER.
func (a *Analyzer) transformWindowFunc(name string, args []AnalyzedExpr, f *parser.FuncCall) (AnalyzedExpr, error) {
	wd, err := a.analyzeWindowDef(f.Over)
	if err != nil {
		return nil, fmt.Errorf("analyzer: window function %s: %w", name, err)
	}

	retType := windowFuncReturnType(name, args)

	return &WindowFuncRef{
		FuncName:  name,
		Args:      args,
		Star:      f.AggStar,
		Distinct:  f.AggDistinct,
		WinDef:    wd,
		ReturnTyp: retType,
		WinIndex:  -1, // set later by the planner
	}, nil
}

// analyzeWindowDef converts a parser.WindowDef into an AnalyzedWindowDef.
func (a *Analyzer) analyzeWindowDef(wd *parser.WindowDef) (*AnalyzedWindowDef, error) {
	result := &AnalyzedWindowDef{}

	// PARTITION BY
	for _, expr := range wd.PartitionClause {
		resolved, err := a.transformExpr(expr)
		if err != nil {
			return nil, fmt.Errorf("PARTITION BY: %w", err)
		}
		result.PartitionBy = append(result.PartitionBy, resolved)
	}

	// ORDER BY
	for _, sb := range wd.OrderClause {
		expr, err := a.transformExpr(sb.Node)
		if err != nil {
			return nil, fmt.Errorf("ORDER BY: %w", err)
		}
		desc := sb.SortbyDir == parser.SORTBY_DESC
		_ = sb.SortbyNulls // nulls ordering not yet tracked in SortClause
		result.OrderBy = append(result.OrderBy, &SortClause{
			Expr: expr,
			Desc: desc,
		})
	}

	// Frame mode and bounds.
	opts := wd.FrameOptions
	if opts&parser.FRAMEOPTION_ROWS != 0 {
		result.FrameMode = FrameModeRows
	} else if opts&parser.FRAMEOPTION_GROUPS != 0 {
		result.FrameMode = FrameModeGroups
	} else {
		result.FrameMode = FrameModeRange
	}

	// Start bound.
	switch {
	case opts&parser.FRAMEOPTION_START_UNBOUNDED_PRECEDING != 0:
		result.FrameStart = WindowFrameBound{Type: BoundUnboundedPreceding}
	case opts&parser.FRAMEOPTION_START_CURRENT_ROW != 0:
		result.FrameStart = WindowFrameBound{Type: BoundCurrentRow}
	case opts&parser.FRAMEOPTION_START_OFFSET_PRECEDING != 0:
		if wd.StartOffset != nil {
			off, err := a.transformExpr(wd.StartOffset)
			if err != nil {
				return nil, err
			}
			result.FrameStart = WindowFrameBound{Type: BoundOffsetPreceding, Offset: off}
		}
	case opts&parser.FRAMEOPTION_START_OFFSET_FOLLOWING != 0:
		if wd.StartOffset != nil {
			off, err := a.transformExpr(wd.StartOffset)
			if err != nil {
				return nil, err
			}
			result.FrameStart = WindowFrameBound{Type: BoundOffsetFollowing, Offset: off}
		}
	default:
		result.FrameStart = WindowFrameBound{Type: BoundUnboundedPreceding}
	}

	// End bound.
	switch {
	case opts&parser.FRAMEOPTION_END_UNBOUNDED_FOLLOWING != 0:
		result.FrameEnd = WindowFrameBound{Type: BoundUnboundedFollowing}
	case opts&parser.FRAMEOPTION_END_CURRENT_ROW != 0:
		result.FrameEnd = WindowFrameBound{Type: BoundCurrentRow}
	case opts&parser.FRAMEOPTION_END_OFFSET_PRECEDING != 0:
		if wd.EndOffset != nil {
			off, err := a.transformExpr(wd.EndOffset)
			if err != nil {
				return nil, err
			}
			result.FrameEnd = WindowFrameBound{Type: BoundOffsetPreceding, Offset: off}
		}
	case opts&parser.FRAMEOPTION_END_OFFSET_FOLLOWING != 0:
		if wd.EndOffset != nil {
			off, err := a.transformExpr(wd.EndOffset)
			if err != nil {
				return nil, err
			}
			result.FrameEnd = WindowFrameBound{Type: BoundOffsetFollowing, Offset: off}
		}
	default:
		result.FrameEnd = WindowFrameBound{Type: BoundCurrentRow}
	}

	return result, nil
}

// expandGroupingSet expands a GroupingSet node into a list of expression lists.
// GROUPING SETS ((a), (b), ()) → [[a], [b], []]
// ROLLUP (a, b) → [[a, b], [a], []]
// CUBE (a, b) → [[a, b], [a], [b], []]
func expandGroupingSet(gs *parser.GroupingSet) [][]parser.Expr {
	switch gs.Kind {
	case parser.GROUPING_SET_SETS:
		var result [][]parser.Expr
		for _, item := range gs.Content {
			if nested, ok := item.(*parser.GroupingSet); ok {
				if nested.Kind == parser.GROUPING_SET_EMPTY {
					result = append(result, nil) // empty grouping set ()
				} else {
					// Nested ROLLUP/CUBE inside GROUPING SETS
					result = append(result, expandGroupingSet(nested)...)
				}
			} else if expr, ok := item.(parser.Expr); ok {
				result = append(result, []parser.Expr{expr})
			}
		}
		return result
	case parser.GROUPING_SET_ROLLUP:
		// ROLLUP(a, b, c) → (a,b,c), (a,b), (a), ()
		exprs := make([]parser.Expr, 0, len(gs.Content))
		for _, item := range gs.Content {
			if e, ok := item.(parser.Expr); ok {
				exprs = append(exprs, e)
			}
		}
		var result [][]parser.Expr
		for i := len(exprs); i >= 0; i-- {
			set := make([]parser.Expr, i)
			copy(set, exprs[:i])
			result = append(result, set)
		}
		return result
	case parser.GROUPING_SET_CUBE:
		// CUBE(a, b) → all 2^n subsets: (a,b), (a), (b), ()
		exprs := make([]parser.Expr, 0, len(gs.Content))
		for _, item := range gs.Content {
			if e, ok := item.(parser.Expr); ok {
				exprs = append(exprs, e)
			}
		}
		n := len(exprs)
		total := 1 << n
		var result [][]parser.Expr
		// Iterate from all-bits-set down to 0 for natural ordering.
		for mask := total - 1; mask >= 0; mask-- {
			var set []parser.Expr
			for bit := 0; bit < n; bit++ {
				if mask&(1<<bit) != 0 {
					set = append(set, exprs[bit])
				}
			}
			result = append(result, set)
		}
		return result
	case parser.GROUPING_SET_EMPTY:
		return [][]parser.Expr{nil}
	}
	return nil
}

func (a *Analyzer) isAggregateFunc(name string) bool {
	switch strings.ToLower(name) {
	case "count", "sum", "avg", "min", "max",
		"bool_and", "bool_or", "every",
		"string_agg", "array_agg",
		"stddev", "stddev_pop", "stddev_samp",
		"variance", "var_pop", "var_samp",
		"corr", "covar_pop", "covar_samp",
		"regr_slope", "regr_intercept", "regr_count", "regr_r2",
		"regr_avgx", "regr_avgy", "regr_sxx", "regr_syy", "regr_sxy",
		"percentile_cont", "percentile_disc", "mode",
		"rank", "dense_rank", "percent_rank", "cume_dist",
		"json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg",
		"xmlagg":
		return true
	}
	// Check custom aggregates in the catalog.
	if a.Cat != nil {
		if _, ok := a.Cat.CustomAggregates[strings.ToLower(name)]; ok {
			return true
		}
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

	// Window functions: FuncCall with an OVER clause.
	if f.Over != nil {
		return a.transformWindowFunc(name, args, f)
	}

	// Aggregate functions produce AggRef nodes.
	if a.isAggregateFunc(name) {
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
		case "stddev", "stddev_pop", "stddev_samp",
			"variance", "var_pop", "var_samp",
			"corr", "covar_pop", "covar_samp",
			"regr_slope", "regr_intercept", "regr_r2",
			"regr_avgx", "regr_avgy", "regr_sxx", "regr_syy", "regr_sxy":
			retType = tuple.TypeFloat64
		case "regr_count":
			retType = tuple.TypeInt64
		case "percentile_cont":
			retType = tuple.TypeFloat64
		case "percentile_disc":
			// Returns the type of the ORDER BY expression.
			retType = tuple.TypeFloat64
		case "mode":
			retType = tuple.TypeText
		case "rank", "dense_rank":
			retType = tuple.TypeInt64
		case "percent_rank", "cume_dist":
			retType = tuple.TypeFloat64
		case "json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg":
			retType = tuple.TypeJSON
		case "xmlagg":
			retType = tuple.TypeText
		default:
			retType = tuple.TypeText
		}

		// Handle WITHIN GROUP (ORDER BY ...) for ordered-set aggregates.
		var withinGroupExpr AnalyzedExpr
		if len(f.AggWithinGroup) > 0 {
			resolved, err := a.transformExpr(f.AggWithinGroup[0].Node)
			if err != nil {
				return nil, err
			}
			withinGroupExpr = resolved
			// For mode and percentile_disc, return type matches the sort expression.
			if name == "mode" || name == "percentile_disc" {
				retType = resolved.ResultType()
			}
		}

		return &AggRef{
			AggFunc:         name,
			Args:            args,
			Star:            f.AggStar,
			Distinct:        f.AggDistinct,
			AggIndex:        -1, // set later by the planner
			ReturnTyp:       retType,
			WithinGroupExpr: withinGroupExpr,
		}, nil
	}

	// Determine return type based on function name.
	var retType tuple.DatumType
	switch name {
	case "now", "current_timestamp":
		retType = tuple.TypeTimestamp
	case "current_date":
		retType = tuple.TypeDate
	case "age":
		retType = tuple.TypeInterval
	// Date/time → text
	case "to_char", "to_date", "to_timestamp", "date_trunc":
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
		"md5", "encode", "decode", "format",
		"regexp_replace", "string_to_array":
		retType = tuple.TypeText
	// Float-returning
	case "abs", "ceil", "ceiling", "floor", "round", "trunc", "truncate",
		"mod", "power", "pow", "sqrt", "cbrt", "sign",
		"random", "pi", "log", "ln", "log10", "exp",
		"extract", "date_part", "to_number":
		retType = tuple.TypeFloat64
	case "gen_random_uuid":
		retType = tuple.TypeUUID
	// Bool-returning
	case "regexp_match":
		retType = tuple.TypeText // returns matched text or NULL
	case "starts_with":
		retType = tuple.TypeBool
	case "pg_table_is_visible",
		"has_table_privilege", "has_schema_privilege", "has_database_privilege",
		"has_column_privilege", "has_function_privilege", "has_sequence_privilege":
		retType = tuple.TypeBool
	case "num_nonnulls", "num_nulls":
		retType = tuple.TypeInt64
	case "pg_table_size", "pg_total_relation_size", "pg_relation_size":
		retType = tuple.TypeInt64
	case "pg_backend_pid", "inet_server_port":
		retType = tuple.TypeInt32
	case "pg_postmaster_start_time":
		retType = tuple.TypeTimestamp
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

// AddRangeTableEntry is an exported wrapper for constraint evaluation.
func (a *Analyzer) AddRangeTableEntry(tableName, alias string) {
	a.addRangeTableEntry(tableName, alias)
}

// GetRangeTable returns the current range table.
func (a *Analyzer) GetRangeTable() []*RangeTblEntry {
	return a.rangeTable
}

// virtualCatalogColumns returns synthetic column definitions for virtual
// catalog tables (information_schema.*, pg_catalog.*). Returns nil if the
// table name is not a recognized virtual table.
func virtualCatalogColumns(qualName string) []RTEColumn {
	type colDef struct {
		name string
		typ  tuple.DatumType
	}
	var defs []colDef

	switch strings.ToLower(qualName) {
	case "information_schema.tables":
		defs = []colDef{
			{"table_catalog", tuple.TypeText},
			{"table_schema", tuple.TypeText},
			{"table_name", tuple.TypeText},
			{"table_type", tuple.TypeText},
		}
	case "information_schema.columns":
		defs = []colDef{
			{"table_catalog", tuple.TypeText},
			{"table_schema", tuple.TypeText},
			{"table_name", tuple.TypeText},
			{"column_name", tuple.TypeText},
			{"ordinal_position", tuple.TypeInt64},
			{"column_default", tuple.TypeText},
			{"is_nullable", tuple.TypeText},
			{"data_type", tuple.TypeText},
		}
	case "information_schema.schemata":
		defs = []colDef{
			{"catalog_name", tuple.TypeText},
			{"schema_name", tuple.TypeText},
			{"schema_owner", tuple.TypeText},
		}
	case "pg_catalog.pg_tables":
		defs = []colDef{
			{"schemaname", tuple.TypeText},
			{"tablename", tuple.TypeText},
			{"tableowner", tuple.TypeText},
			{"hasindexes", tuple.TypeBool},
			{"hasrules", tuple.TypeBool},
			{"hastriggers", tuple.TypeBool},
		}
	case "pg_catalog.pg_indexes":
		defs = []colDef{
			{"schemaname", tuple.TypeText},
			{"tablename", tuple.TypeText},
			{"indexname", tuple.TypeText},
			{"indexdef", tuple.TypeText},
		}
	case "pg_catalog.pg_views":
		defs = []colDef{
			{"schemaname", tuple.TypeText},
			{"viewname", tuple.TypeText},
			{"viewowner", tuple.TypeText},
			{"definition", tuple.TypeText},
		}
	case "pg_catalog.pg_roles":
		defs = []colDef{
			{"rolname", tuple.TypeText},
			{"rolsuper", tuple.TypeBool},
			{"rolinherit", tuple.TypeBool},
			{"rolcreaterole", tuple.TypeBool},
			{"rolcreatedb", tuple.TypeBool},
			{"rolcanlogin", tuple.TypeBool},
			{"rolbypassrls", tuple.TypeBool},
			{"rolconnlimit", tuple.TypeInt64},
		}
	case "pg_catalog.pg_stat_user_tables":
		defs = []colDef{
			{"relid", tuple.TypeInt64},
			{"schemaname", tuple.TypeText},
			{"relname", tuple.TypeText},
			{"seq_scan", tuple.TypeInt64},
			{"seq_tup_read", tuple.TypeInt64},
			{"n_live_tup", tuple.TypeInt64},
		}
	case "pg_catalog.pg_namespace":
		defs = []colDef{
			{"oid", tuple.TypeInt64},
			{"nspname", tuple.TypeText},
			{"nspowner", tuple.TypeInt64},
		}
	case "pg_catalog.pg_stat_statements", "pg_stat_statements":
		defs = []colDef{
			{"userid", tuple.TypeInt64},
			{"dbid", tuple.TypeInt64},
			{"query", tuple.TypeText},
			{"calls", tuple.TypeInt64},
			{"total_time", tuple.TypeFloat64},
			{"rows", tuple.TypeInt64},
		}
	default:
		return nil
	}

	cols := make([]RTEColumn, len(defs))
	for i, d := range defs {
		cols[i] = RTEColumn{Name: d.name, Type: d.typ, ColNum: int32(i + 1)}
	}
	return cols
}


