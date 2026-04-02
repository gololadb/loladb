package planner

import (
	"fmt"

	"github.com/gololadb/loladb/pkg/tuple"
)

// QueryToLogicalPlan converts a Query tree (output of the analyzer)
// into a LogicalNode tree (input to the optimizer). This bridges the
// PostgreSQL-style semantic analysis output to the existing planner.
//
// In PostgreSQL this role is played by the planner's
// query_planner() / grouping_planner() / subquery_planner() functions
// in src/backend/optimizer/plan/planner.c.
func QueryToLogicalPlan(q *Query) (LogicalNode, error) {
	switch q.CommandType {
	case CmdSelect:
		return queryToSelectPlan(q)
	case CmdInsert:
		return queryToInsertPlan(q)
	case CmdDelete:
		return queryToDeletePlan(q)
	case CmdUpdate:
		return queryToUpdatePlan(q)
	case CmdUtility:
		return queryToUtilityPlan(q)
	default:
		return nil, fmt.Errorf("planner: unsupported command type %s", q.CommandType)
	}
}

func queryToSelectPlan(q *Query) (LogicalNode, error) {
	if q.JoinTree == nil || len(q.JoinTree.FromList) == 0 {
		// SELECT without FROM → Result node (single virtual row).
		if len(q.TargetList) > 0 {
			// Evaluate expressions and project.
			var exprs []Expr
			var names []string
			for _, te := range q.TargetList {
				exprs = append(exprs, analyzedToExpr(te.Expr, q.RangeTable))
				names = append(names, te.Name)
			}
			return &LogicalResult{Exprs: exprs, Names: names}, nil
		}
		if q.Utility != nil {
			return &LogicalNoOp{Message: q.Utility.Message}, nil
		}
		return &LogicalNoOp{Message: "SELECT"}, nil
	}

	// Build the scan/join tree from the join tree nodes.
	plan, err := joinTreeToPlan(q.JoinTree.FromList, q.RangeTable)
	if err != nil {
		return nil, err
	}

	// WHERE clause → Filter.
	if q.JoinTree.Quals != nil {
		plan = &LogicalFilter{
			Predicate: analyzedToExpr(q.JoinTree.Quals, q.RangeTable),
			Child:     plan,
		}
	}

	// Target list → Project (unless it's SELECT *).
	if !isSelectStar(q.TargetList, q.RangeTable) {
		var exprs []Expr
		var names []string
		for _, te := range q.TargetList {
			exprs = append(exprs, analyzedToExpr(te.Expr, q.RangeTable))
			names = append(names, te.Name)
		}
		plan = &LogicalProject{Exprs: exprs, Names: names, Child: plan}
	}

	// ORDER BY → Sort.
	if len(q.SortClause) > 0 {
		outCols := plan.OutputColumns()
		var keys []SortKey
		for _, sc := range q.SortClause {
			keys = append(keys, SortKey{
				Expr: analyzedToExprWithCols(sc.Expr, q.RangeTable, outCols),
				Desc: sc.Desc,
			})
		}
		plan = &LogicalSort{Keys: keys, Child: plan}
	}

	// LIMIT / OFFSET.
	if q.LimitCount != nil || q.LimitOffset != nil {
		limit := &LogicalLimit{Count: -1, Child: plan}
		if q.LimitCount != nil {
			if c, ok := q.LimitCount.(*Const); ok {
				limit.Count = constToInt64(c)
			}
		}
		if q.LimitOffset != nil {
			if c, ok := q.LimitOffset.(*Const); ok {
				limit.Offset = constToInt64(c)
			}
		}
		plan = limit
	}

	return plan, nil
}

func queryToInsertPlan(q *Query) (LogicalNode, error) {
	rte := q.RangeTable[q.ResultRelation-1]
	var values [][]Expr
	for _, row := range q.Values {
		var rowExprs []Expr
		for _, e := range row {
			rowExprs = append(rowExprs, analyzedToExpr(e, q.RangeTable))
		}
		values = append(values, rowExprs)
	}
	return &LogicalInsert{Table: rte.RelName, Values: values}, nil
}

func queryToDeletePlan(q *Query) (LogicalNode, error) {
	rte := q.RangeTable[q.ResultRelation-1]

	colNames := make([]string, len(rte.Columns))
	for i, c := range rte.Columns {
		colNames[i] = c.Name
	}
	scan := &LogicalScan{Table: rte.RelName, Alias: rte.Alias, Columns: colNames}
	var child LogicalNode = scan

	if q.JoinTree != nil && q.JoinTree.Quals != nil {
		child = &LogicalFilter{
			Predicate: analyzedToExpr(q.JoinTree.Quals, q.RangeTable),
			Child:     child,
		}
	}

	return &LogicalDelete{Table: rte.RelName, Child: child}, nil
}

func queryToUpdatePlan(q *Query) (LogicalNode, error) {
	rte := q.RangeTable[q.ResultRelation-1]

	colNames := make([]string, len(rte.Columns))
	colTypes := make([]tuple.DatumType, len(rte.Columns))
	for i, c := range rte.Columns {
		colNames[i] = c.Name
		colTypes[i] = c.Type
	}
	scan := &LogicalScan{Table: rte.RelName, Alias: rte.Alias, Columns: colNames}
	var child LogicalNode = scan

	if q.JoinTree != nil && q.JoinTree.Quals != nil {
		child = &LogicalFilter{
			Predicate: analyzedToExpr(q.JoinTree.Quals, q.RangeTable),
			Child:     child,
		}
	}

	var assignments []Assignment
	for _, ua := range q.Assignments {
		assignments = append(assignments, Assignment{
			Column: ua.ColName,
			Value:  analyzedToExpr(ua.Expr, q.RangeTable),
		})
	}

	return &LogicalUpdate{
		Table:       rte.RelName,
		Assignments: assignments,
		Child:       child,
		Columns:     colNames,
		ColTypes:    colTypes,
	}, nil
}

func queryToUtilityPlan(q *Query) (LogicalNode, error) {
	u := q.Utility
	if u == nil {
		return &LogicalNoOp{Message: "UTILITY"}, nil
	}
	switch u.Type {
	case UtilCreateTable:
		return &LogicalCreateTable{Table: u.TableName, Columns: u.Columns}, nil
	case UtilCreateIndex:
		return &LogicalCreateIndex{Index: u.IndexName, Table: u.IndexTable, Column: u.IndexColumn, Method: u.IndexMethod}, nil
	case UtilCreateSequence:
		return &LogicalCreateSequence{Name: u.SeqName}, nil
	case UtilCreateView:
		return &LogicalCreateView{Name: u.ViewName, Definition: u.ViewDef, Columns: u.ViewColumns}, nil
	case UtilAlterTable:
		return &LogicalAlterTable{Table: u.TableName, Commands: u.AlterCmds}, nil
	case UtilCreatePolicy:
		return &LogicalCreatePolicy{
			Name: u.PolicyName, Table: u.PolicyTable, Cmd: u.PolicyCmd,
			Permissive: u.PolicyPermissive, Roles: u.PolicyRoles,
			Using: u.PolicyUsing, Check: u.PolicyCheck,
		}, nil
	case UtilEnableRLS:
		return &LogicalEnableRLS{Table: u.TableName}, nil
	case UtilDisableRLS:
		return &LogicalDisableRLS{Table: u.TableName}, nil
	case UtilCreateRole:
		return &LogicalCreateRole{RoleName: u.RoleName, Options: u.RoleOptions, StmtType: u.RoleStmtType}, nil
	case UtilAlterRole:
		return &LogicalAlterRole{RoleName: u.RoleName, Options: u.RoleOptions}, nil
	case UtilDropRole:
		return &LogicalDropRole{Roles: u.DropRoles, MissingOk: u.DropMissingOk}, nil
	case UtilGrantRole:
		return &LogicalGrantRole{GrantedRoles: u.GrantedRoles, Grantees: u.Grantees, AdminOption: u.AdminOption}, nil
	case UtilRevokeRole:
		return &LogicalRevokeRole{RevokedRoles: u.GrantedRoles, Grantees: u.Grantees}, nil
	case UtilGrantPrivilege:
		return &LogicalGrantPrivilege{Privileges: u.Privileges, PrivCols: u.PrivCols, TargetType: u.TargetType, Objects: u.Objects, Grantees: u.Grantees, GrantOption: u.GrantOption}, nil
	case UtilRevokePrivilege:
		return &LogicalRevokePrivilege{Privileges: u.Privileges, PrivCols: u.PrivCols, TargetType: u.TargetType, Objects: u.Objects, Grantees: u.Grantees}, nil
	case UtilCreateFunction:
		return &LogicalCreateFunction{
			Name: u.FuncName, Language: u.FuncLanguage, Body: u.FuncBody,
			ReturnType: u.FuncReturnType, ParamNames: u.FuncParamNames,
			ParamTypes: u.FuncParamTypes, Replace: u.FuncReplace,
		}, nil
	case UtilCreateTrigger:
		return &LogicalCreateTrigger{
			TrigName: u.TrigName, Table: u.TrigTable, FuncName: u.TrigFuncName,
			Timing: u.TrigTiming, Events: u.TrigEvents, ForEach: u.TrigForEach,
			Replace: u.TrigReplace,
		}, nil
	case UtilDropFunction:
		return &LogicalDropFunction{Name: u.FuncName, MissingOk: u.DropMissingOk}, nil
	case UtilDropTrigger:
		return &LogicalDropTrigger{TrigName: u.TrigName, Table: u.TrigTable, MissingOk: u.DropMissingOk}, nil
	case UtilAlterFunction:
		return &LogicalAlterFunction{Name: u.FuncName, NewName: u.FuncNewName, NewOwner: u.FuncNewOwner}, nil
	case UtilCreateDomain:
		return &LogicalCreateDomain{Name: u.DomainName, BaseType: u.DomainBaseType}, nil
	case UtilCreateEnum:
		return &LogicalCreateEnum{Name: u.EnumName, Vals: u.EnumVals}, nil
	case UtilNoOp:
		return &LogicalNoOp{Message: u.Message}, nil
	default:
		return &LogicalNoOp{Message: "UTILITY"}, nil
	}
}

// --- Join tree to plan conversion ---

func joinTreeToPlan(items []JoinTreeNode, rtes []*RangeTblEntry) (LogicalNode, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("planner: empty FROM clause")
	}

	plan, err := joinTreeNodeToPlan(items[0], rtes)
	if err != nil {
		return nil, err
	}

	// Multiple FROM items → implicit CROSS JOIN.
	for i := 1; i < len(items); i++ {
		right, err := joinTreeNodeToPlan(items[i], rtes)
		if err != nil {
			return nil, err
		}
		plan = &LogicalJoin{Type: JoinCross, Left: plan, Right: right}
	}

	return plan, nil
}

func joinTreeNodeToPlan(node JoinTreeNode, rtes []*RangeTblEntry) (LogicalNode, error) {
	switch n := node.(type) {
	case *RangeTblRef:
		rte := rtes[n.RTIndex-1]
		colNames := make([]string, len(rte.Columns))
		for i, c := range rte.Columns {
			colNames[i] = c.Name
		}
		return &LogicalScan{Table: rte.RelName, Alias: rte.Alias, Columns: colNames}, nil

	case *JoinNode:
		left, err := joinTreeNodeToPlan(n.Left, rtes)
		if err != nil {
			return nil, err
		}
		right, err := joinTreeNodeToPlan(n.Right, rtes)
		if err != nil {
			return nil, err
		}
		join := &LogicalJoin{Type: n.JoinType, Left: left, Right: right}
		if n.Quals != nil {
			join.Condition = analyzedToExpr(n.Quals, rtes)
		}
		return join, nil

	default:
		return nil, fmt.Errorf("planner: unsupported join tree node %T", node)
	}
}

// --- AnalyzedExpr to Expr conversion ---
// Converts the typed analyzed expressions back to the Expr interface
// used by the optimizer and executor.

func analyzedToExpr(ae AnalyzedExpr, rtes []*RangeTblEntry) Expr {
	switch e := ae.(type) {
	case *ColumnVar:
		return &ExprColumn{
			Table:  e.Table,
			Column: e.ColName,
			Index:  e.AttIndex,
		}
	case *Const:
		return &ExprLiteral{Value: e.Value}
	case *OpExpr:
		return &ExprBinOp{
			Op:    e.Op,
			Left:  analyzedToExpr(e.Left, rtes),
			Right: analyzedToExpr(e.Right, rtes),
		}
	case *BoolExprNode:
		return boolExprToExpr(e, rtes)
	case *NullTestExpr:
		return &ExprIsNull{
			Child: analyzedToExpr(e.Arg, rtes),
			Not:   e.IsNot,
		}
	case *StarExpr:
		return &ExprStar{}
	default:
		// Fallback: AnalyzedExpr already implements Expr.
		return ae
	}
}

func analyzedToExprWithCols(ae AnalyzedExpr, rtes []*RangeTblEntry, outCols []string) Expr {
	// For ORDER BY, we need to resolve against the output columns.
	if cv, ok := ae.(*ColumnVar); ok {
		// Try to find the column in the output columns list.
		target := cv.ColName
		if cv.Table != "" {
			target = cv.Table + "." + cv.ColName
		}
		for i, name := range outCols {
			parts := splitQualified(name)
			if len(parts) == 2 {
				if (cv.Table == "" || equalFold(parts[0], cv.Table)) && equalFold(parts[1], cv.ColName) {
					return &ExprColumn{Table: parts[0], Column: parts[1], Index: i}
				}
			} else if equalFold(name, target) || equalFold(name, cv.ColName) {
				return &ExprColumn{Table: cv.Table, Column: cv.ColName, Index: i}
			}
		}
	}
	return analyzedToExpr(ae, rtes)
}

func boolExprToExpr(b *BoolExprNode, rtes []*RangeTblEntry) Expr {
	switch b.Op {
	case BoolNot:
		return &ExprNot{Child: analyzedToExpr(b.Args[0], rtes)}
	case BoolAnd:
		left := analyzedToExpr(b.Args[0], rtes)
		for i := 1; i < len(b.Args); i++ {
			right := analyzedToExpr(b.Args[i], rtes)
			left = &ExprBinOp{Op: OpAnd, Left: left, Right: right}
		}
		return left
	case BoolOr:
		left := analyzedToExpr(b.Args[0], rtes)
		for i := 1; i < len(b.Args); i++ {
			right := analyzedToExpr(b.Args[i], rtes)
			left = &ExprBinOp{Op: OpOr, Left: left, Right: right}
		}
		return left
	}
	return analyzedToExpr(b.Args[0], rtes)
}

// --- Helpers ---

func isSelectStar(targets []*TargetEntry, rtes []*RangeTblEntry) bool {
	// Check if the target list is exactly all columns from all RTEs
	// (i.e., the result of expanding SELECT *).
	totalCols := 0
	for _, rte := range rtes {
		totalCols += len(rte.Columns)
	}
	if len(targets) != totalCols {
		return false
	}
	idx := 0
	for _, rte := range rtes {
		for _, col := range rte.Columns {
			if idx >= len(targets) {
				return false
			}
			cv, ok := targets[idx].Expr.(*ColumnVar)
			if !ok || cv.RTIndex != rte.RTIndex || cv.ColName != col.Name {
				return false
			}
			idx++
		}
	}
	return true
}

func constToInt64(c *Const) int64 {
	switch c.ConstType {
	case tuple.TypeInt64:
		return c.Value.I64
	case tuple.TypeInt32:
		return int64(c.Value.I32)
	default:
		return -1
	}
}

func splitQualified(name string) []string {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '.' {
			return []string{name[:i], name[i+1:]}
		}
	}
	return []string{name}
}

func equalFold(a, b string) bool {
	return len(a) == len(b) && (a == b || foldEqual(a, b))
}

func foldEqual(a, b string) bool {
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca != cb {
			if ca >= 'A' && ca <= 'Z' {
				ca += 'a' - 'A'
			}
			if cb >= 'A' && cb <= 'Z' {
				cb += 'a' - 'A'
			}
			if ca != cb {
				return false
			}
		}
	}
	return true
}
