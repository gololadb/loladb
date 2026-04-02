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

	"github.com/gololadb/loladb/pkg/catalog"
	"github.com/gololadb/loladb/pkg/planner"
	"github.com/gololadb/loladb/pkg/tuple"
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

	// CurrentUser is the session-level role used for RLS policy evaluation.
	CurrentUser string

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
	// Step 1: Walk the range table looking for views to expand.
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

	// Step 2: Apply RLS policies to all range table entries.
	if err := rw.applyRLSPolicies(query); err != nil {
		return nil, err
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
// DML statements (e.g., DELETE FROM view WHERE ...) and applies RLS.
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

	// Apply RLS policies to the DML query.
	if err := rw.applyRLSPolicies(query); err != nil {
		return nil, err
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

// --- Row-Level Security ---
// Mirrors PostgreSQL's get_row_security_policies() in
// src/backend/rewrite/rowsecurity.c.

// applyRLSPolicies checks if any range table entries have RLS enabled
// and injects policy quals into the query. This is called after view
// expansion so that policies on the underlying tables are applied.
func (rw *Rewriter) applyRLSPolicies(query *planner.Query) error {
	if query.JoinTree == nil {
		return nil
	}

	cmd := cmdToPolicy(query.CommandType)

	for _, rte := range query.RangeTable {
		if !rw.Cat.IsRLSEnabled(rte.RelOID) {
			continue
		}

		permissive, restrictive := rw.Cat.GetPoliciesForCmd(rte.RelOID, cmd, rw.CurrentUser)

		// If RLS is enabled but no policies exist, default-deny:
		// inject a FALSE qual so no rows are returned.
		if len(permissive) == 0 && len(restrictive) == 0 {
			query.JoinTree.Quals = rw.injectDefaultDeny(query.JoinTree.Quals)
			continue
		}

		// Build the combined USING qual.
		// Permissive policies are OR'd together.
		var permQual planner.AnalyzedExpr
		for _, p := range permissive {
			if p.UsingExpr == "" {
				continue
			}
			policyQual, err := rw.parsePolicyExpr(p.UsingExpr, rte)
			if err != nil {
				return fmt.Errorf("rewriter: policy %q USING: %w", p.Name, err)
			}
			if permQual == nil {
				permQual = policyQual
			} else {
				permQual = &planner.BoolExprNode{
					Op:   planner.BoolOr,
					Args: []planner.AnalyzedExpr{permQual, policyQual},
				}
			}
		}

		// Restrictive policies are AND'd together.
		var restQual planner.AnalyzedExpr
		for _, p := range restrictive {
			if p.UsingExpr == "" {
				continue
			}
			policyQual, err := rw.parsePolicyExpr(p.UsingExpr, rte)
			if err != nil {
				return fmt.Errorf("rewriter: policy %q USING: %w", p.Name, err)
			}
			if restQual == nil {
				restQual = policyQual
			} else {
				restQual = &planner.BoolExprNode{
					Op:   planner.BoolAnd,
					Args: []planner.AnalyzedExpr{restQual, policyQual},
				}
			}
		}

		// Combine: (permissive_combined) AND (restrictive_combined)
		var combined planner.AnalyzedExpr
		if permQual != nil && restQual != nil {
			combined = &planner.BoolExprNode{
				Op:   planner.BoolAnd,
				Args: []planner.AnalyzedExpr{permQual, restQual},
			}
		} else if permQual != nil {
			combined = permQual
		} else if restQual != nil {
			combined = restQual
		}

		if combined == nil {
			continue
		}

		// Inject the combined qual into the query's join tree.
		if query.JoinTree.Quals != nil {
			query.JoinTree.Quals = &planner.BoolExprNode{
				Op:   planner.BoolAnd,
				Args: []planner.AnalyzedExpr{query.JoinTree.Quals, combined},
			}
		} else {
			query.JoinTree.Quals = combined
		}
	}

	return nil
}

// injectDefaultDeny adds a FALSE constant to deny all rows when RLS
// is enabled but no policies match.
func (rw *Rewriter) injectDefaultDeny(existing planner.AnalyzedExpr) planner.AnalyzedExpr {
	deny := &planner.Const{
		Value:     tuple.DBool(false),
		ConstType: tuple.TypeBool,
	}
	if existing != nil {
		return &planner.BoolExprNode{
			Op:   planner.BoolAnd,
			Args: []planner.AnalyzedExpr{existing, deny},
		}
	}
	return deny
}

// parsePolicyExpr parses a policy expression SQL string and resolves
// it against the given range table entry. The expression can reference
// columns of the table and the special value current_user.
func (rw *Rewriter) parsePolicyExpr(exprSQL string, rte *planner.RangeTblEntry) (planner.AnalyzedExpr, error) {
	// Wrap the expression in a SELECT WHERE to make it parseable.
	wrappedSQL := fmt.Sprintf("SELECT 1 FROM %s WHERE %s", rte.RelName, exprSQL)

	stmts, err := parser.Parse(strings.NewReader(wrappedSQL), nil)
	if err != nil {
		return nil, fmt.Errorf("parse policy expr: %w", err)
	}
	if len(stmts) == 0 {
		return nil, fmt.Errorf("empty policy expression")
	}

	// Analyze the wrapped query to resolve column references.
	policyQuery, err := rw.Analyzer.Analyze(stmts[0].Stmt)
	if err != nil {
		return nil, fmt.Errorf("analyze policy expr: %w", err)
	}

	if policyQuery.JoinTree == nil || policyQuery.JoinTree.Quals == nil {
		return nil, fmt.Errorf("policy expression produced no qualification")
	}

	qual := policyQuery.JoinTree.Quals

	// Rewrite column references in the policy qual to point to the
	// outer query's RTE instead of the policy's internal RTE.
	qual = rw.remapPolicyVars(qual, rte)

	// Replace current_user references with the actual current user value.
	qual = rw.resolveCurrentUser(qual)

	return qual, nil
}

// remapPolicyVars rewrites ColumnVar nodes in a policy expression to
// reference the outer query's range table entry.
func (rw *Rewriter) remapPolicyVars(expr planner.AnalyzedExpr, rte *planner.RangeTblEntry) planner.AnalyzedExpr {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *planner.ColumnVar:
		// Find the matching column in the outer RTE.
		for i, col := range rte.Columns {
			if strings.EqualFold(col.Name, e.ColName) {
				return &planner.ColumnVar{
					RTIndex:  rte.RTIndex,
					ColNum:   col.ColNum,
					ColName:  col.Name,
					Table:    rte.Alias,
					VarType:  col.Type,
					AttIndex: i,
				}
			}
		}
		return e
	case *planner.OpExpr:
		return &planner.OpExpr{
			Op:        e.Op,
			Left:      rw.remapPolicyVars(e.Left, rte),
			Right:     rw.remapPolicyVars(e.Right, rte),
			ResultTyp: e.ResultTyp,
		}
	case *planner.BoolExprNode:
		newArgs := make([]planner.AnalyzedExpr, len(e.Args))
		for i, arg := range e.Args {
			newArgs[i] = rw.remapPolicyVars(arg, rte)
		}
		return &planner.BoolExprNode{Op: e.Op, Args: newArgs}
	case *planner.NullTestExpr:
		return &planner.NullTestExpr{
			Arg:   rw.remapPolicyVars(e.Arg, rte),
			IsNot: e.IsNot,
		}
	default:
		return expr
	}
}

// resolveCurrentUser replaces ColumnVar nodes named "current_user"
// with a string constant of the actual current user.
func (rw *Rewriter) resolveCurrentUser(expr planner.AnalyzedExpr) planner.AnalyzedExpr {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *planner.ColumnVar:
		if strings.EqualFold(e.ColName, "current_user") {
			return &planner.Const{
				Value:     tuple.DText(rw.CurrentUser),
				ConstType: tuple.TypeText,
			}
		}
		return e
	case *planner.OpExpr:
		return &planner.OpExpr{
			Op:        e.Op,
			Left:      rw.resolveCurrentUser(e.Left),
			Right:     rw.resolveCurrentUser(e.Right),
			ResultTyp: e.ResultTyp,
		}
	case *planner.BoolExprNode:
		newArgs := make([]planner.AnalyzedExpr, len(e.Args))
		for i, arg := range e.Args {
			newArgs[i] = rw.resolveCurrentUser(arg)
		}
		return &planner.BoolExprNode{Op: e.Op, Args: newArgs}
	case *planner.NullTestExpr:
		return &planner.NullTestExpr{
			Arg:   rw.resolveCurrentUser(e.Arg),
			IsNot: e.IsNot,
		}
	default:
		return expr
	}
}

func cmdToPolicy(cmd planner.CmdType) catalog.PolicyCmd {
	switch cmd {
	case planner.CmdSelect:
		return catalog.PolicyCmdSelect
	case planner.CmdInsert:
		return catalog.PolicyCmdInsert
	case planner.CmdUpdate:
		return catalog.PolicyCmdUpdate
	case planner.CmdDelete:
		return catalog.PolicyCmdDelete
	default:
		return catalog.PolicyCmdAll
	}
}
