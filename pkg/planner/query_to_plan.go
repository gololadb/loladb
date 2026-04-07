package planner

import (
	"fmt"
	"strings"

	"github.com/gololadb/loladb/pkg/tuple"
	qt "github.com/gololadb/loladb/pkg/querytree"
)

// QueryToLogicalPlan converts a qt.Query tree (output of the analyzer)
// into a LogicalNode tree (input to the optimizer). This bridges the
// PostgreSQL-style semantic analysis output to the existing planner.
//
// In PostgreSQL this role is played by the planner's
// query_planner() / grouping_planner() / subquery_planner() functions
// in src/backend/optimizer/plan/planner.c.
func QueryToLogicalPlan(q *qt.Query) (LogicalNode, error) {
	switch q.CommandType {
	case qt.CmdSelect:
		return queryToSelectPlan(q)
	case qt.CmdInsert:
		return queryToInsertPlan(q)
	case qt.CmdDelete:
		return queryToDeletePlan(q)
	case qt.CmdUpdate:
		return queryToUpdatePlan(q)
	case qt.CmdUtility:
		return queryToUtilityPlan(q)
	default:
		return nil, fmt.Errorf("planner: unsupported command type %s", q.CommandType)
	}
}

func queryToSelectPlan(q *qt.Query) (LogicalNode, error) {
	// Handle bare VALUES clause.
	if q.IsValues && len(q.Values) > 0 {
		var names []string
		for _, te := range q.TargetList {
			names = append(names, te.Name)
		}
		var rows [][]qt.Expr
		for _, row := range q.Values {
			var exprs []qt.Expr
			for _, e := range row {
				exprs = append(exprs, analyzedToExpr(e, q.RangeTable))
			}
			rows = append(rows, exprs)
		}
		return &LogicalValues{Names: names, Values: rows}, nil
	}

	// Handle set operations.
	if q.SetOp != qt.SetOpNone {
		return queryToSetOpPlan(q)
	}
	if q.JoinTree == nil || len(q.JoinTree.FromList) == 0 {
		// SELECT without FROM → Result node (single virtual row).
		if len(q.TargetList) > 0 {
			// Evaluate expressions and project.
			var exprs []qt.Expr
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

	// Aggregate node (GROUP BY / aggregate functions).
	if q.HasAggs || len(q.GroupClause) > 0 {
		var groupExprs []qt.Expr
		for _, g := range q.GroupClause {
			groupExprs = append(groupExprs, analyzedToExpr(g, q.RangeTable))
		}
		var aggDescs []AggDesc
		for _, ref := range q.AggRefs {
			var argExprs []qt.Expr
			for _, a := range ref.Args {
				argExprs = append(argExprs, analyzedToExpr(a, q.RangeTable))
			}
			var withinExpr qt.Expr
			if ref.WithinGroupExpr != nil {
				withinExpr = analyzedToExpr(ref.WithinGroupExpr, q.RangeTable)
			}
			aggDescs = append(aggDescs, AggDesc{
				Func:            ref.AggFunc,
				ArgExprs:        argExprs,
				Star:            ref.Star,
				Distinct:        ref.Distinct,
				WithinGroupExpr: withinExpr,
			})
		}
		var havingExpr qt.Expr
		if q.HavingQual != nil {
			havingExpr = analyzedToExpr(q.HavingQual, q.RangeTable)
			// Patch agg refs in HAVING too.
			patchAggExprs(havingExpr, len(groupExprs))
		}
		plan = &LogicalAggregate{
			GroupExprs:   groupExprs,
			AggDescs:     aggDescs,
			HavingQual:   havingExpr,
			GroupingSets: q.GroupingSets,
			Child:        plan,
		}
	}

	// Window functions → WindowAgg node.
	if len(q.WindowFuncs) > 0 {
		plan = buildWindowAggPlan(q, plan)
	}

	// Target list → Project (unless it's SELECT *).
	if !isSelectStar(q.TargetList, q.RangeTable) {
		var exprs []qt.Expr
		var names []string
		for _, te := range q.TargetList {
			exprs = append(exprs, analyzedToExpr(te.Expr, q.RangeTable))
			names = append(names, te.Name)
		}
		// When there's an aggregate below, patch expressions:
		// - qt.ExprAggRef: set NumGroupExprs so they read from the right offset
		// - qt.ExprColumn: reset Index to -1 to force name-based lookup
		//   (the aggregate output has a different column layout)
		if q.HasAggs || len(q.GroupClause) > 0 {
			numGroups := len(q.GroupClause)
			for _, expr := range exprs {
				patchAggExprs(expr, numGroups)
			}
		}
		plan = &LogicalProject{Exprs: exprs, Names: names, Child: plan}
	}

	// DISTINCT → Distinct node.
	if q.Distinct {
		plan = &LogicalDistinct{Child: plan}
	}

	// ORDER BY → Sort.
	if len(q.SortClause) > 0 {
		outCols := plan.OutputColumns()
		var keys []SortKey
		for _, sc := range q.SortClause {
			expr := analyzedToExprWithCols(sc.Expr, q.RangeTable, outCols)
			// Patch aggregate references in sort keys.
			if q.HasAggs || len(q.GroupClause) > 0 {
				patchAggExprs(expr, len(q.GroupClause))
			}
			keys = append(keys, SortKey{
				Expr: expr,
				Desc: sc.Desc,
			})
		}
		plan = &LogicalSort{Keys: keys, Child: plan}
	}

	// LIMIT / OFFSET.
	if q.LimitCount != nil || q.LimitOffset != nil {
		limit := &LogicalLimit{Count: -1, Child: plan}
		if q.LimitCount != nil {
			if c, ok := q.LimitCount.(*qt.Const); ok {
				limit.Count = constToInt64(c)
			}
		}
		if q.LimitOffset != nil {
			if c, ok := q.LimitOffset.(*qt.Const); ok {
				limit.Offset = constToInt64(c)
			}
		}
		plan = limit
	}

	return plan, nil
}

func queryToSetOpPlan(q *qt.Query) (LogicalNode, error) {
	left, err := queryToSelectPlan(q.SetLeft)
	if err != nil {
		return nil, err
	}
	right, err := queryToSelectPlan(q.SetRight)
	if err != nil {
		return nil, err
	}
	var plan LogicalNode = &LogicalSetOp{
		Op:    q.SetOp,
		All:   q.SetAll,
		Left:  left,
		Right: right,
	}

	// ORDER BY on the combined result.
	if len(q.SortClause) > 0 {
		var keys []SortKey
		for _, sc := range q.SortClause {
			expr := analyzedToExpr(sc.Expr, q.RangeTable)
			keys = append(keys, SortKey{Expr: expr, Desc: sc.Desc})
		}
		plan = &LogicalSort{Keys: keys, Child: plan}
	}

	// LIMIT / OFFSET.
	if q.LimitCount != nil || q.LimitOffset != nil {
		limit := &LogicalLimit{Count: -1, Child: plan}
		if q.LimitCount != nil {
			if c, ok := q.LimitCount.(*qt.Const); ok {
				limit.Count = constToInt64(c)
			}
		}
		if q.LimitOffset != nil {
			if c, ok := q.LimitOffset.(*qt.Const); ok {
				limit.Offset = constToInt64(c)
			}
		}
		plan = limit
	}

	return plan, nil
}

func queryToInsertPlan(q *qt.Query) (LogicalNode, error) {
	rte := q.RangeTable[q.ResultRelation-1]

	// INSERT ... SELECT
	if q.SelectSource != nil {
		selectPlan, err := queryToSelectPlan(q.SelectSource)
		if err != nil {
			return nil, err
		}
		return &LogicalInsertSelect{
			Table:      rte.RelName,
			Columns:    q.InsertColumns,
			SelectPlan: selectPlan,
		}, nil
	}

	// INSERT ... VALUES
	var values [][]qt.Expr
	for _, row := range q.Values {
		var rowExprs []qt.Expr
		for _, e := range row {
			rowExprs = append(rowExprs, analyzedToExpr(e, q.RangeTable))
		}
		values = append(values, rowExprs)
	}
	retExprs, retNames := convertReturning(q.ReturningList, q.RangeTable)
	node := &LogicalInsert{Table: rte.RelName, Columns: q.InsertColumns, Values: values, ReturningExprs: retExprs, ReturningNames: retNames}

	// ON CONFLICT
	if q.OnConflict != nil {
		ocp := &OnConflictPlan{
			Action:       q.OnConflict.Action,
			ConflictCols: q.OnConflict.ConflictCols,
		}
		for _, ua := range q.OnConflict.Assignments {
			ocp.Assignments = append(ocp.Assignments, Assignment{
				Column: ua.ColName,
				Value:  analyzedToExpr(ua.Expr, q.RangeTable),
			})
		}
		if q.OnConflict.WhereClause != nil {
			ocp.WhereExpr = analyzedToExpr(q.OnConflict.WhereClause, q.RangeTable)
		}
		node.OnConflict = ocp
	}

	return node, nil
}

func queryToDeletePlan(q *qt.Query) (LogicalNode, error) {
	rte := q.RangeTable[q.ResultRelation-1]

	colNames := make([]string, len(rte.Columns))
	for i, c := range rte.Columns {
		colNames[i] = c.Name
	}
	scan := &LogicalScan{Table: rte.RelName, Alias: rte.Alias, Columns: colNames,
		SampleMethod: rte.SampleMethod, SamplePercent: rte.SamplePercent}
	var child LogicalNode = scan

	if q.JoinTree != nil && q.JoinTree.Quals != nil {
		child = &LogicalFilter{
			Predicate: analyzedToExpr(q.JoinTree.Quals, q.RangeTable),
			Child:     child,
		}
	}

	retExprs, retNames := convertReturning(q.ReturningList, q.RangeTable)
	return &LogicalDelete{Table: rte.RelName, Child: child, ReturningExprs: retExprs, ReturningNames: retNames}, nil
}

func queryToUpdatePlan(q *qt.Query) (LogicalNode, error) {
	rte := q.RangeTable[q.ResultRelation-1]

	colNames := make([]string, len(rte.Columns))
	colTypes := make([]tuple.DatumType, len(rte.Columns))
	for i, c := range rte.Columns {
		colNames[i] = c.Name
		colTypes[i] = c.Type
	}

	// Build child plan from the join tree (handles UPDATE ... FROM).
	var child LogicalNode
	if q.JoinTree != nil && len(q.JoinTree.FromList) > 0 {
		var err error
		child, err = joinTreeToPlan(q.JoinTree.FromList, q.RangeTable)
		if err != nil {
			return nil, err
		}
	} else {
		child = &LogicalScan{Table: rte.RelName, Alias: rte.Alias, Columns: colNames,
			SampleMethod: rte.SampleMethod, SamplePercent: rte.SamplePercent}
	}

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

	retExprs, retNames := convertReturning(q.ReturningList, q.RangeTable)
	return &LogicalUpdate{
		Table:          rte.RelName,
		Assignments:    assignments,
		Child:          child,
		Columns:        colNames,
		ColTypes:       colTypes,
		ReturningExprs: retExprs,
		ReturningNames: retNames,
	}, nil
}

// convertReturning converts analyzed RETURNING target entries to qt.Expr slices.
func convertReturning(list []*qt.TargetEntry, rtes []*qt.RangeTblEntry) ([]qt.Expr, []string) {
	if len(list) == 0 {
		return nil, nil
	}
	exprs := make([]qt.Expr, len(list))
	names := make([]string, len(list))
	for i, te := range list {
		exprs[i] = analyzedToExpr(te.Expr, rtes)
		names[i] = te.Name
	}
	return exprs, names
}

func queryToUtilityPlan(q *qt.Query) (LogicalNode, error) {
	u := q.Utility
	if u == nil {
		return &LogicalNoOp{Message: "UTILITY"}, nil
	}
	switch u.Type {
	case qt.UtilCreateTable:
		return &LogicalCreateTable{
			Table: u.TableName, Schema: u.TableSchema, Columns: u.Columns,
			ForeignKeys: u.ForeignKeys, IsTemp: u.IsTemp,
			PartitionStrategy: u.PartitionStrategy, PartitionKeyCols: u.PartitionKeyCols,
			InheritParents: u.InheritParents,
		}, nil
	case qt.UtilCreateIndex:
		return &LogicalCreateIndex{Index: u.IndexName, Table: u.IndexTable, Column: u.IndexColumn, Method: u.IndexMethod}, nil
	case qt.UtilCreateSequence:
		return &LogicalCreateSequence{Name: u.SeqName}, nil
	case qt.UtilCreateView:
		return &LogicalCreateView{Name: u.ViewName, Definition: u.ViewDef, Columns: u.ViewColumns}, nil
	case qt.UtilAlterTable:
		return &LogicalAlterTable{Table: u.TableName, Commands: u.AlterCmds}, nil
	case qt.UtilCreatePolicy:
		return &LogicalCreatePolicy{
			Name: u.PolicyName, Table: u.PolicyTable, Cmd: u.PolicyCmd,
			Permissive: u.PolicyPermissive, Roles: u.PolicyRoles,
			Using: u.PolicyUsing, Check: u.PolicyCheck,
		}, nil
	case qt.UtilEnableRLS:
		return &LogicalEnableRLS{Table: u.TableName}, nil
	case qt.UtilDisableRLS:
		return &LogicalDisableRLS{Table: u.TableName}, nil
	case qt.UtilCreateRole:
		return &LogicalCreateRole{RoleName: u.RoleName, Options: u.RoleOptions, StmtType: u.RoleStmtType}, nil
	case qt.UtilAlterRole:
		return &LogicalAlterRole{RoleName: u.RoleName, Options: u.RoleOptions}, nil
	case qt.UtilDropRole:
		return &LogicalDropRole{Roles: u.DropRoles, MissingOk: u.DropMissingOk}, nil
	case qt.UtilGrantRole:
		return &LogicalGrantRole{GrantedRoles: u.GrantedRoles, Grantees: u.Grantees, AdminOption: u.AdminOption}, nil
	case qt.UtilRevokeRole:
		return &LogicalRevokeRole{RevokedRoles: u.GrantedRoles, Grantees: u.Grantees}, nil
	case qt.UtilGrantPrivilege:
		return &LogicalGrantPrivilege{Privileges: u.Privileges, PrivCols: u.PrivCols, TargetType: u.TargetType, Objects: u.Objects, Grantees: u.Grantees, GrantOption: u.GrantOption}, nil
	case qt.UtilRevokePrivilege:
		return &LogicalRevokePrivilege{Privileges: u.Privileges, PrivCols: u.PrivCols, TargetType: u.TargetType, Objects: u.Objects, Grantees: u.Grantees}, nil
	case qt.UtilCreateFunction:
		return &LogicalCreateFunction{
			Name: u.FuncName, Language: u.FuncLanguage, Body: u.FuncBody,
			ReturnType: u.FuncReturnType, ParamNames: u.FuncParamNames,
			ParamTypes: u.FuncParamTypes, Replace: u.FuncReplace,
		}, nil
	case qt.UtilCreateTrigger:
		return &LogicalCreateTrigger{
			TrigName: u.TrigName, Table: u.TrigTable, FuncName: u.TrigFuncName,
			Timing: u.TrigTiming, Events: u.TrigEvents, ForEach: u.TrigForEach,
			Replace: u.TrigReplace, Args: u.TrigArgs,
		}, nil
	case qt.UtilDropFunction:
		return &LogicalDropFunction{Name: u.FuncName, MissingOk: u.DropMissingOk}, nil
	case qt.UtilDropTrigger:
		return &LogicalDropTrigger{TrigName: u.TrigName, Table: u.TrigTable, MissingOk: u.DropMissingOk}, nil
	case qt.UtilAlterFunction:
		return &LogicalAlterFunction{Name: u.FuncName, NewName: u.FuncNewName, NewOwner: u.FuncNewOwner}, nil
	case qt.UtilCreateDomain:
		return &LogicalCreateDomain{Name: u.DomainName, BaseType: u.DomainBaseType, NotNull: u.DomainNotNull, CheckExpr: u.DomainCheck}, nil
	case qt.UtilCreateEnum:
		return &LogicalCreateEnum{Name: u.EnumName, Vals: u.EnumVals}, nil
	case qt.UtilDropType:
		return &LogicalDropType{Name: u.DropTypeName, MissingOk: u.DropMissingOk}, nil
	case qt.UtilAlterEnum:
		return &LogicalAlterEnum{Name: u.AlterEnumName, NewVal: u.AlterEnumVal}, nil
	case qt.UtilCreateSchema:
		return &LogicalCreateSchema{Name: u.SchemaName, IfNotExists: u.SchemaIfNotExists, AuthRole: u.SchemaAuthRole}, nil
	case qt.UtilDropTable:
		return &LogicalDropTable{Name: u.TableName, MissingOk: u.DropMissingOk, Cascade: u.DropCascade}, nil
	case qt.UtilDropSchema:
		return &LogicalDropSchema{Name: u.SchemaName, MissingOk: u.DropMissingOk, Cascade: u.DropCascade}, nil
	case qt.UtilTruncate:
		return &LogicalTruncate{Table: u.TableName}, nil
	case qt.UtilDropIndex:
		return &LogicalDropIndex{Name: u.IndexName, MissingOk: u.DropMissingOk, Cascade: u.DropCascade}, nil
	case qt.UtilDropView:
		return &LogicalDropView{Name: u.ViewName, MissingOk: u.DropMissingOk, Cascade: u.DropCascade}, nil
	case qt.UtilAddColumn:
		return &LogicalAddColumn{Table: u.TableName, Col: *u.AlterColDef, IfNotExists: u.AlterIfNotExists}, nil
	case qt.UtilDropColumn:
		return &LogicalDropColumn{Table: u.TableName, ColName: u.AlterColName, IfExists: u.AlterIfExists}, nil
	case qt.UtilNoOp:
		return &LogicalNoOp{Message: u.Message}, nil
	default:
		return &LogicalNoOp{Message: "UTILITY"}, nil
	}
}

// --- Join tree to plan conversion ---

func joinTreeToPlan(items []qt.JoinTreeNode, rtes []*qt.RangeTblEntry) (LogicalNode, error) {
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
		plan = &LogicalJoin{Type: qt.JoinCross, Left: plan, Right: right}
	}

	return plan, nil
}

func joinTreeNodeToPlan(node qt.JoinTreeNode, rtes []*qt.RangeTblEntry) (LogicalNode, error) {
	switch n := node.(type) {
	case *qt.RangeTblRef:
		rte := rtes[n.RTIndex-1]
		colNames := make([]string, len(rte.Columns))
		for i, c := range rte.Columns {
			colNames[i] = c.Name
		}

		// Subquery / CTE scan.
		if rte.Subquery != nil {
			if rte.IsRecursive {
				// Recursive CTE: build plans for both init and recursive terms.
				initPlan, err := queryToSelectPlan(rte.Subquery.SetLeft)
				if err != nil {
					return nil, fmt.Errorf("planner: recursive CTE %q init: %w", rte.RelName, err)
				}
				recPlan, err := queryToSelectPlan(rte.Subquery.SetRight)
				if err != nil {
					return nil, fmt.Errorf("planner: recursive CTE %q recursive: %w", rte.RelName, err)
				}
				return &LogicalSubqueryScan{
					Alias:         rte.Alias,
					Columns:       colNames,
					ChildPlan:     recPlan,
					IsRecursive:   true,
					RecursiveInit: initPlan,
					Lateral:       rte.Lateral,
				}, nil
			}
			childPlan, err := QueryToLogicalPlan(rte.Subquery)
			if err != nil {
				return nil, fmt.Errorf("planner: CTE %q: %w", rte.RelName, err)
			}
			return &LogicalSubqueryScan{
				Alias:     rte.Alias,
				Columns:   colNames,
				ChildPlan: childPlan,
				Lateral:   rte.Lateral,
			}, nil
		}

		return &LogicalScan{Table: rte.RelName, Alias: rte.Alias, Columns: colNames,
			SampleMethod: rte.SampleMethod, SamplePercent: rte.SamplePercent}, nil

	case *qt.JoinNode:
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

// --- qt.AnalyzedExpr to qt.Expr conversion ---
// Converts the typed analyzed expressions back to the qt.Expr interface
// used by the optimizer and executor.

// AnalyzedToExpr converts an qt.AnalyzedExpr to an executable qt.Expr.
// Exported for use by the executor (e.g., evaluating DEFAULT expressions).
func AnalyzedToExpr(ae qt.AnalyzedExpr, rtes []*qt.RangeTblEntry) qt.Expr {
	return analyzedToExpr(ae, rtes)
}

func analyzedToExpr(ae qt.AnalyzedExpr, rtes []*qt.RangeTblEntry) qt.Expr {
	switch e := ae.(type) {
	case *qt.ColumnVar:
		return &qt.ExprColumn{
			Table:  e.Table,
			Column: e.ColName,
			Index:  e.AttIndex,
		}
	case *qt.Const:
		return &qt.ExprLiteral{Value: e.Value}
	case *qt.OpExpr:
		return &qt.ExprBinOp{
			Op:    e.Op,
			Left:  analyzedToExpr(e.Left, rtes),
			Right: analyzedToExpr(e.Right, rtes),
		}
	case *qt.BoolExprNode:
		return boolExprToExpr(e, rtes)
	case *qt.NullTestExpr:
		return &qt.ExprIsNull{
			Child: analyzedToExpr(e.Arg, rtes),
			Not:   e.IsNot,
		}
	case *qt.AggRef:
		return &qt.ExprAggRef{AggIndex: e.AggIndex, NumGroupExprs: 0} // patched by queryToSelectPlan
	case *qt.TypeCastExpr:
		return &qt.ExprCast{
			Inner:      analyzedToExpr(e.Arg, rtes),
			TargetType: e.CastType,
			TypeName:   e.TargetType,
		}
	case *qt.FuncCallExpr:
		args := make([]qt.Expr, len(e.Args))
		for i, a := range e.Args {
			args[i] = analyzedToExpr(a, rtes)
		}
		return &qt.ExprFunc{Name: e.FuncName, Args: args}
	case *qt.ArrayConstructExpr:
		// Elements are already qt.Expr — just return as-is.
		return e
	case *qt.ArraySubscriptExpr:
		return e
	case *qt.ArraySliceExpr:
		return e
	case *qt.StarExpr:
		return &qt.ExprStar{}
	case *qt.CaseExprNode:
		node := &qt.ExprCase{}
		if e.Arg != nil {
			node.Arg = analyzedToExpr(e.Arg, rtes)
		}
		node.Whens = make([]qt.ExprCaseWhen, len(e.Whens))
		for i, w := range e.Whens {
			node.Whens[i] = qt.ExprCaseWhen{
				Cond:   analyzedToExpr(w.Cond, rtes),
				Result: analyzedToExpr(w.Result, rtes),
			}
		}
		if e.ElseExpr != nil {
			node.ElseExpr = analyzedToExpr(e.ElseExpr, rtes)
		}
		return node
	case *qt.BooleanTestExpr:
		return &qt.ExprBoolTest{
			Arg:  analyzedToExpr(e.Arg, rtes),
			Test: e.Test,
		}
	case *qt.WindowFuncRef:
		// The WindowAgg node appends computed values to each row.
		// WinIndex points to the column position in the extended row.
		return &qt.ExprColumn{
			Table:  "",
			Column: fmt.Sprintf("win_%d", e.WinIndex),
			Index:  e.WinIndex,
		}
	case *qt.SubLinkExpr:
		sl := &qt.ExprSubLink{
			LinkType: e.LinkType,
			OpName:   e.OpName,
			Subquery: e.Subquery,
		}
		if e.TestExpr != nil {
			sl.TestExpr = analyzedToExpr(e.TestExpr, rtes)
		}
		return sl
	default:
		// Fallback: qt.AnalyzedExpr already implements qt.Expr.
		return ae
	}
}

func analyzedToExprWithCols(ae qt.AnalyzedExpr, rtes []*qt.RangeTblEntry, outCols []string) qt.Expr {
	// For ORDER BY, we need to resolve against the output columns.
	switch e := ae.(type) {
	case *qt.ColumnVar:
		// Try to find the column in the output columns list.
		target := e.ColName
		if e.Table != "" {
			target = e.Table + "." + e.ColName
		}
		for i, name := range outCols {
			parts := splitQualified(name)
			if len(parts) == 2 {
				if (e.Table == "" || equalFold(parts[0], e.Table)) && equalFold(parts[1], e.ColName) {
					return &qt.ExprColumn{Table: parts[0], Column: parts[1], Index: i}
				}
			} else if equalFold(name, target) || equalFold(name, e.ColName) {
				return &qt.ExprColumn{Table: e.Table, Column: e.ColName, Index: i}
			}
		}
	case *qt.AggRef:
		// Match the aggregate expression against the output column names
		// so the sort key reads from the Project's output by index.
		// Try both qualified (table.col) and unqualified (col) forms
		// since exprString uses unqualified names.
		aggStr := e.String()
		for i, name := range outCols {
			if equalFold(name, aggStr) {
				return &qt.ExprColumn{Column: name, Index: i}
			}
		}
		// Try with unqualified argument names.
		unqualStr := aggUnqualifiedString(e)
		if unqualStr != aggStr {
			for i, name := range outCols {
				if equalFold(name, unqualStr) {
					return &qt.ExprColumn{Column: name, Index: i}
				}
			}
		}
	}
	return analyzedToExpr(ae, rtes)
}

func boolExprToExpr(b *qt.BoolExprNode, rtes []*qt.RangeTblEntry) qt.Expr {
	switch b.Op {
	case qt.BoolNot:
		return &qt.ExprNot{Child: analyzedToExpr(b.Args[0], rtes)}
	case qt.BoolAnd:
		left := analyzedToExpr(b.Args[0], rtes)
		for i := 1; i < len(b.Args); i++ {
			right := analyzedToExpr(b.Args[i], rtes)
			left = &qt.ExprBinOp{Op: qt.OpAnd, Left: left, Right: right}
		}
		return left
	case qt.BoolOr:
		left := analyzedToExpr(b.Args[0], rtes)
		for i := 1; i < len(b.Args); i++ {
			right := analyzedToExpr(b.Args[i], rtes)
			left = &qt.ExprBinOp{Op: qt.OpOr, Left: left, Right: right}
		}
		return left
	}
	return analyzedToExpr(b.Args[0], rtes)
}

// --- Helpers ---

func isSelectStar(targets []*qt.TargetEntry, rtes []*qt.RangeTblEntry) bool {
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
			cv, ok := targets[idx].Expr.(*qt.ColumnVar)
			if !ok || cv.RTIndex != rte.RTIndex || cv.ColName != col.Name {
				return false
			}
			idx++
		}
	}
	return true
}

func constToInt64(c *qt.Const) int64 {
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

// aggUnqualifiedString returns the qt.AggRef's string representation with
// unqualified column names (no table prefix), matching how exprString
// formats function calls in the target list.
func aggUnqualifiedString(a *qt.AggRef) string {
	if a.Star {
		return a.AggFunc + "(*)"
	}
	args := make([]string, len(a.Args))
	for i, arg := range a.Args {
		if cv, ok := arg.(*qt.ColumnVar); ok {
			args[i] = cv.ColName // unqualified
		} else {
			args[i] = arg.String()
		}
	}
	return a.AggFunc + "(" + strings.Join(args, ", ") + ")"
}

func equalFold(a, b string) bool {
	return len(a) == len(b) && (a == b || foldEqual(a, b))
}

// patchAggExprs patches expressions in a Project that sits above an
// Aggregate node:
// - qt.ExprAggRef: sets NumGroupExprs so they read from the right offset
// - qt.ExprColumn: resets Index to -1 to force name-based lookup (the
//   aggregate output has a different column layout than the scan)
func patchAggExprs(expr qt.Expr, numGroupExprs int) {
	switch e := expr.(type) {
	case *qt.ExprAggRef:
		e.NumGroupExprs = numGroupExprs
	case *qt.ExprColumn:
		e.Index = -1
	case *qt.ExprBinOp:
		patchAggExprs(e.Left, numGroupExprs)
		patchAggExprs(e.Right, numGroupExprs)
	case *qt.ExprNot:
		patchAggExprs(e.Child, numGroupExprs)
	case *qt.ExprIsNull:
		patchAggExprs(e.Child, numGroupExprs)
	case *qt.ExprFunc:
		for _, a := range e.Args {
			patchAggExprs(a, numGroupExprs)
		}
	case *qt.ExprCast:
		patchAggExprs(e.Inner, numGroupExprs)
	}
}

// buildWindowAggPlan creates a LogicalWindowAgg node from the query's
// window function references. It also assigns WinIndex values to each
// qt.WindowFuncRef so the project layer can reference the computed values.
func buildWindowAggPlan(q *qt.Query, child LogicalNode) LogicalNode {
	var descs []WindowFuncDesc
	childCols := child.OutputColumns()
	baseIndex := len(childCols)

	for i, wf := range q.WindowFuncs {
		var argExprs []qt.Expr
		for _, a := range wf.Args {
			argExprs = append(argExprs, analyzedToExpr(a, q.RangeTable))
		}

		var partExprs []qt.Expr
		var orderExprs []SortExpr
		if wf.WinDef != nil {
			for _, p := range wf.WinDef.PartitionBy {
				partExprs = append(partExprs, analyzedToExpr(p, q.RangeTable))
			}
			for _, o := range wf.WinDef.OrderBy {
				orderExprs = append(orderExprs, SortExpr{
					Expr: analyzedToExpr(o.Expr, q.RangeTable),
					Desc: o.Desc,
				})
			}
		}

		desc := WindowFuncDesc{
			FuncName:    wf.FuncName,
			ArgExprs:    argExprs,
			Star:        wf.Star,
			Distinct:    wf.Distinct,
			PartitionBy: partExprs,
			OrderBy:     orderExprs,
		}
		if wf.WinDef != nil {
			desc.FrameMode = wf.WinDef.FrameMode
			desc.FrameStart = wf.WinDef.FrameStart
			desc.FrameEnd = wf.WinDef.FrameEnd
		}
		descs = append(descs, desc)

		// Assign the output column index so qt.WindowFuncRef.Eval can find it.
		wf.WinIndex = baseIndex + i
	}

	return &LogicalWindowAgg{
		Child:    child,
		WinFuncs: descs,
	}
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

// AnalyzedToExprPublic is an exported wrapper around analyzedToExpr.
func AnalyzedToExprPublic(ae qt.AnalyzedExpr, rtes []*qt.RangeTblEntry) qt.Expr {
	return analyzedToExpr(ae, rtes)
}


