// Package rewriter implements the query rewrite rule system, sitting
// between the analyzer and the planner in the query processing pipeline.
//
// This mirrors PostgreSQL's rewriter (src/backend/rewrite/rewriteHandler.c).
// The rewriter's primary job is to expand views: when a query references
// a view, the rewriter replaces that reference with the view's defining
// query (the _RETURN rule). It also supports DML rules (ON INSERT/UPDATE/
// DELETE DO INSTEAD/ALSO).
//
// Pipeline position:
//
//	Parser → Analyzer → **Rewriter** → Planner → Optimizer → Executor
package rewriter

import (
	"fmt"
	"strings"

	"github.com/jespino/gopgsql/parser"

	"github.com/jespino/loladb/pkg/catalog"
	"github.com/jespino/loladb/pkg/planner"
)

// colMapping maps a view output column to the underlying table column.
type colMapping struct {
	newRTIndex int
	newColNum  int32
	newColName string
	newTable   string
}

// Rewriter applies rewrite rules to Query trees produced by the analyzer.
// It mirrors PostgreSQL's QueryRewrite() function.
type Rewriter struct {
	Cat      *catalog.Catalog
	Analyzer *planner.Analyzer

	// maxDepth prevents infinite recursion from circular view definitions.
	maxDepth int
}

// New creates a Rewriter.
func New(cat *catalog.Catalog, analyzer *planner.Analyzer) *Rewriter {
	return &Rewriter{
		Cat:      cat,
		Analyzer: analyzer,
		maxDepth: 16,
	}
}

// Rewrite applies all applicable rewrite rules to a Query tree,
// returning the rewritten query (or queries, for ALSO rules).
//
// This is the main entry point, equivalent to PostgreSQL's
// QueryRewrite() in rewriteHandler.c.
func (rw *Rewriter) Rewrite(query *planner.Query) ([]*planner.Query, error) {
	return rw.rewriteQuery(query, 0)
}

func (rw *Rewriter) rewriteQuery(query *planner.Query, depth int) ([]*planner.Query, error) {
	if depth > rw.maxDepth {
		return nil, fmt.Errorf("rewriter: maximum rule recursion depth exceeded")
	}

	switch query.CommandType {
	case planner.CmdSelect:
		return rw.rewriteSelect(query, depth)
	case planner.CmdInsert:
		return rw.rewriteDML(query, catalog.RuleEventInsert, depth)
	case planner.CmdUpdate:
		return rw.rewriteDML(query, catalog.RuleEventUpdate, depth)
	case planner.CmdDelete:
		return rw.rewriteDML(query, catalog.RuleEventDelete, depth)
	case planner.CmdUtility:
		// Utility statements are not rewritten.
		return []*planner.Query{query}, nil
	default:
		return []*planner.Query{query}, nil
	}
}

// rewriteSelect handles SELECT queries. The key operation is view
// expansion: for each range table entry that is a view, replace it
// with the view's defining subquery.
//
// This mirrors PostgreSQL's fireRIRrules() (RIR = Retrieve-Instead-Retrieve)
// in rewriteHandler.c.
func (rw *Rewriter) rewriteSelect(query *planner.Query, depth int) ([]*planner.Query, error) {
	// Walk the range table looking for views to expand.
	for i, rte := range query.RangeTable {
		if !rw.Cat.IsView(rte.RelOID) {
			continue
		}

		// Found a view — get its _RETURN rule.
		rules := rw.Cat.GetRulesForEvent(rte.RelOID, catalog.RuleEventSelect)
		if len(rules) == 0 {
			continue
		}

		rule := rules[0] // Views have exactly one _RETURN rule.
		if rule.Definition == "" {
			continue
		}

		// Parse and analyze the view definition to get a Query tree.
		viewQuery, err := rw.parseAndAnalyze(rule.Definition)
		if err != nil {
			return nil, fmt.Errorf("rewriter: expanding view %q: %w", rte.RelName, err)
		}

		// Recursively rewrite the view's query (handles nested views).
		rewritten, err := rw.rewriteQuery(viewQuery, depth+1)
		if err != nil {
			return nil, err
		}
		if len(rewritten) == 0 {
			return nil, fmt.Errorf("rewriter: view %q produced no queries", rte.RelName)
		}
		viewQuery = rewritten[0]

		// Merge the view's range table into the outer query and
		// adjust references. This is the core of view expansion.
		rw.expandViewInQuery(query, i, rte, viewQuery)
	}

	return []*planner.Query{query}, nil
}

// rewriteDML handles INSERT/UPDATE/DELETE queries by checking for
// applicable rules on the result relation.
//
// This mirrors PostgreSQL's RewriteQuery() for non-SELECT commands
// in rewriteHandler.c.
func (rw *Rewriter) rewriteDML(query *planner.Query, event catalog.RuleEvent, depth int) ([]*planner.Query, error) {
	if query.ResultRelation == 0 || query.ResultRelation > len(query.RangeTable) {
		return []*planner.Query{query}, nil
	}

	rte := query.RangeTable[query.ResultRelation-1]
	rules := rw.Cat.GetRulesForEvent(rte.RelOID, event)

	if len(rules) == 0 {
		// No rules — also expand any views in the FROM clause for
		// DML queries that have subselects.
		return rw.rewriteSelectInDML(query, depth)
	}

	var result []*planner.Query
	hasInstead := false

	for _, rule := range rules {
		switch rule.Action {
		case catalog.RuleActionNothing:
			// DO NOTHING: suppress the original query.
			hasInstead = true

		case catalog.RuleActionInstead:
			hasInstead = true
			if rule.Definition == "" {
				continue
			}
			ruleQuery, err := rw.parseAndAnalyze(rule.Definition)
			if err != nil {
				return nil, fmt.Errorf("rewriter: applying rule %q: %w", rule.Name, err)
			}
			rewritten, err := rw.rewriteQuery(ruleQuery, depth+1)
			if err != nil {
				return nil, err
			}
			result = append(result, rewritten...)

		case catalog.RuleActionAlso:
			if rule.Definition == "" {
				continue
			}
			ruleQuery, err := rw.parseAndAnalyze(rule.Definition)
			if err != nil {
				return nil, fmt.Errorf("rewriter: applying rule %q: %w", rule.Name, err)
			}
			rewritten, err := rw.rewriteQuery(ruleQuery, depth+1)
			if err != nil {
				return nil, err
			}
			result = append(result, rewritten...)
		}
	}

	// If no INSTEAD rule fired, keep the original query.
	if !hasInstead {
		result = append([]*planner.Query{query}, result...)
	}

	if len(result) == 0 {
		// All rules were DO NOTHING — return empty.
		return nil, nil
	}

	return result, nil
}

// rewriteSelectInDML expands views referenced in the FROM clause of
// DML statements (e.g., DELETE FROM view WHERE ...).
func (rw *Rewriter) rewriteSelectInDML(query *planner.Query, depth int) ([]*planner.Query, error) {
	// Check if the result relation itself is a view.
	if query.ResultRelation > 0 && query.ResultRelation <= len(query.RangeTable) {
		rte := query.RangeTable[query.ResultRelation-1]
		if rw.Cat.IsView(rte.RelOID) {
			// DML on a view without rules — error.
			return nil, fmt.Errorf("rewriter: cannot %s on view %q without appropriate rules",
				query.CommandType, rte.RelName)
		}
	}
	return []*planner.Query{query}, nil
}

// expandViewInQuery replaces a view's range table entry with the
// view's underlying query, merging range tables and adjusting
// column references.
//
// This mirrors PostgreSQL's ApplyRetrieveRule() in rewriteHandler.c.
func (rw *Rewriter) expandViewInQuery(
	query *planner.Query,
	rteIdx int,
	viewRTE *planner.RangeTblEntry,
	viewQuery *planner.Query,
) {
	// The view's range table entries get appended to the outer query's
	// range table. We need to offset all RTIndex references in the
	// view's query by the current range table size.
	baseOffset := len(query.RangeTable)

	// Append the view's range table entries with adjusted indices.
	for _, vrte := range viewQuery.RangeTable {
		newRTE := *vrte
		newRTE.RTIndex = baseOffset + vrte.RTIndex
		query.RangeTable = append(query.RangeTable, &newRTE)
	}

	// Replace the view RTE's columns with the view query's output
	// columns so that references to the view resolve correctly.
	// The view RTE stays in the range table but becomes a placeholder.

	// Build a mapping from the view's output columns to the underlying
	// expressions. For each target entry in the view query, we know
	// which underlying RTE column it references.
	var mappings []colMapping

	for _, te := range viewQuery.TargetList {
		if cv, ok := te.Expr.(*planner.ColumnVar); ok {
			mappings = append(mappings, colMapping{
				newRTIndex: baseOffset + cv.RTIndex,
				newColNum:  cv.ColNum,
				newColName: cv.ColName,
				newTable:   cv.Table,
			})
		} else {
			// Non-column expression in view target list — keep the
			// view column as-is (expression views not fully supported).
			mappings = append(mappings, colMapping{
				newRTIndex: viewRTE.RTIndex,
				newColNum:  int32(len(mappings) + 1),
				newColName: te.Name,
				newTable:   viewRTE.Alias,
			})
		}
	}

	// Rewrite column references in the outer query that point to the
	// view RTE to instead point to the underlying table columns.
	viewRTIndex := viewRTE.RTIndex
	rw.rewriteVarsInQuery(query, viewRTIndex, mappings, baseOffset)

	// Replace the view's join tree entry with the view query's join tree.
	if query.JoinTree != nil && viewQuery.JoinTree != nil {
		rw.replaceJoinTreeRef(query, viewRTIndex, viewQuery.JoinTree, baseOffset)
	}
}

// rewriteVarsInQuery walks all expressions in the query and replaces
// ColumnVar references to the view RTE with references to the
// underlying table columns.
func (rw *Rewriter) rewriteVarsInQuery(
	query *planner.Query,
	viewRTIndex int,
	mappings []colMapping,
	baseOffset int,
) {
	// Rewrite target list.
	for _, te := range query.TargetList {
		te.Expr = rw.rewriteVarsInExpr(te.Expr, viewRTIndex, mappings)
	}

	// Rewrite join tree quals.
	if query.JoinTree != nil && query.JoinTree.Quals != nil {
		query.JoinTree.Quals = rw.rewriteVarsInExpr(query.JoinTree.Quals, viewRTIndex, mappings)
	}

	// Rewrite sort clause.
	for _, sc := range query.SortClause {
		sc.Expr = rw.rewriteVarsInExpr(sc.Expr, viewRTIndex, mappings)
	}

	// Rewrite LIMIT/OFFSET (unlikely to reference view columns, but be thorough).
	if query.LimitCount != nil {
		query.LimitCount = rw.rewriteVarsInExpr(query.LimitCount, viewRTIndex, mappings)
	}
	if query.LimitOffset != nil {
		query.LimitOffset = rw.rewriteVarsInExpr(query.LimitOffset, viewRTIndex, mappings)
	}
}

// rewriteVarsInExpr replaces ColumnVar nodes that reference the view
// RTE with the mapped underlying column references.
func (rw *Rewriter) rewriteVarsInExpr(
	expr planner.AnalyzedExpr,
	viewRTIndex int,
	mappings []colMapping,
) planner.AnalyzedExpr {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *planner.ColumnVar:
		if e.RTIndex != viewRTIndex {
			return e
		}
		// Find the mapping for this column by matching column name.
		for _, m := range mappings {
			if strings.EqualFold(m.newColName, e.ColName) {
				return &planner.ColumnVar{
					RTIndex:  m.newRTIndex,
					ColNum:   m.newColNum,
					ColName:  m.newColName,
					Table:    m.newTable,
					VarType:  e.VarType,
					AttIndex: e.AttIndex, // will be recomputed by the planner
				}
			}
		}
		return e

	case *planner.OpExpr:
		return &planner.OpExpr{
			Op:        e.Op,
			Left:      rw.rewriteVarsInExpr(e.Left, viewRTIndex, mappings),
			Right:     rw.rewriteVarsInExpr(e.Right, viewRTIndex, mappings),
			ResultTyp: e.ResultTyp,
		}

	case *planner.BoolExprNode:
		newArgs := make([]planner.AnalyzedExpr, len(e.Args))
		for i, arg := range e.Args {
			newArgs[i] = rw.rewriteVarsInExpr(arg, viewRTIndex, mappings)
		}
		return &planner.BoolExprNode{Op: e.Op, Args: newArgs}

	case *planner.NullTestExpr:
		return &planner.NullTestExpr{
			Arg:   rw.rewriteVarsInExpr(e.Arg, viewRTIndex, mappings),
			IsNot: e.IsNot,
		}

	default:
		return expr
	}
}

// replaceJoinTreeRef replaces a RangeTblRef in the outer query's join
// tree with the view query's join tree (possibly including its own
// joins and quals).
func (rw *Rewriter) replaceJoinTreeRef(
	query *planner.Query,
	viewRTIndex int,
	viewJoinTree *planner.FromExpr,
	baseOffset int,
) {
	// Offset the view's join tree references.
	offsetItems := rw.offsetJoinTreeNodes(viewJoinTree.FromList, baseOffset)

	// Build the replacement: the view's FROM items plus its WHERE qual.
	// If the view has a WHERE clause, we need to AND it with the outer
	// query's existing quals.
	if viewJoinTree.Quals != nil {
		viewQual := rw.offsetExprRTIndexes(viewJoinTree.Quals, baseOffset)
		if query.JoinTree.Quals != nil {
			query.JoinTree.Quals = &planner.BoolExprNode{
				Op:   planner.BoolAnd,
				Args: []planner.AnalyzedExpr{query.JoinTree.Quals, viewQual},
			}
		} else {
			query.JoinTree.Quals = viewQual
		}
	}

	// Replace the view's RangeTblRef in the FROM list.
	newFromList := make([]planner.JoinTreeNode, 0, len(query.JoinTree.FromList)+len(offsetItems)-1)
	for _, item := range query.JoinTree.FromList {
		if ref, ok := item.(*planner.RangeTblRef); ok && ref.RTIndex == viewRTIndex {
			newFromList = append(newFromList, offsetItems...)
		} else {
			newFromList = append(newFromList, item)
		}
	}
	query.JoinTree.FromList = newFromList
}

// offsetJoinTreeNodes adjusts RTIndex values in join tree nodes.
func (rw *Rewriter) offsetJoinTreeNodes(items []planner.JoinTreeNode, offset int) []planner.JoinTreeNode {
	result := make([]planner.JoinTreeNode, len(items))
	for i, item := range items {
		switch n := item.(type) {
		case *planner.RangeTblRef:
			result[i] = &planner.RangeTblRef{RTIndex: n.RTIndex + offset}
		case *planner.JoinNode:
			result[i] = &planner.JoinNode{
				JoinType: n.JoinType,
				Left:     rw.offsetJoinTreeNodes([]planner.JoinTreeNode{n.Left}, offset)[0],
				Right:    rw.offsetJoinTreeNodes([]planner.JoinTreeNode{n.Right}, offset)[0],
				Quals:    rw.offsetExprRTIndexes(n.Quals, offset),
				LeftRTI:  n.LeftRTI + offset,
				RightRTI: n.RightRTI + offset,
			}
		default:
			result[i] = item
		}
	}
	return result
}

// offsetExprRTIndexes adjusts RTIndex values in ColumnVar expressions.
func (rw *Rewriter) offsetExprRTIndexes(expr planner.AnalyzedExpr, offset int) planner.AnalyzedExpr {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *planner.ColumnVar:
		return &planner.ColumnVar{
			RTIndex:  e.RTIndex + offset,
			ColNum:   e.ColNum,
			ColName:  e.ColName,
			Table:    e.Table,
			VarType:  e.VarType,
			AttIndex: e.AttIndex,
		}
	case *planner.OpExpr:
		return &planner.OpExpr{
			Op:        e.Op,
			Left:      rw.offsetExprRTIndexes(e.Left, offset),
			Right:     rw.offsetExprRTIndexes(e.Right, offset),
			ResultTyp: e.ResultTyp,
		}
	case *planner.BoolExprNode:
		newArgs := make([]planner.AnalyzedExpr, len(e.Args))
		for i, arg := range e.Args {
			newArgs[i] = rw.offsetExprRTIndexes(arg, offset)
		}
		return &planner.BoolExprNode{Op: e.Op, Args: newArgs}
	case *planner.NullTestExpr:
		return &planner.NullTestExpr{
			Arg:   rw.offsetExprRTIndexes(e.Arg, offset),
			IsNot: e.IsNot,
		}
	default:
		return expr
	}
}

// parseAndAnalyze parses a SQL string and runs it through the analyzer.
func (rw *Rewriter) parseAndAnalyze(sql string) (*planner.Query, error) {
	stmts, err := parser.Parse(strings.NewReader(sql), nil)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	if len(stmts) == 0 {
		return nil, fmt.Errorf("empty statement")
	}
	return rw.Analyzer.Analyze(stmts[0].Stmt)
}
