# querytree

Defines the query tree data structures that flow between the analyzer, rewriter, planner, and executor. This is the shared intermediate representation for all query processing stages.

## Key types

- **`Query`** — Top-level query node. Carries the command type (SELECT, INSERT, UPDATE, DELETE, utility), range table, target list, join tree, sort/group/limit clauses, CTEs, and set operations.
- **`Expr` / `AnalyzedExpr`** — Expression tree interface and its analyzed (type-resolved) form. Concrete nodes include `OpExpr`, `ColumnVar`, `Const`, `FuncCallExpr`, `AggRef`, `SubLinkExpr`, `BoolExprNode`, `CaseExprNode`, `WindowFuncRef`, etc.
- **`RangeTblEntry`** — A range table entry describing a table, subquery, CTE, or values list referenced by the query.
- **`TargetEntry`** — A single output column in the query's target list.
- **`JoinTreeNode`** — Nodes in the FROM/JOIN tree (`JoinNode`, `RangeTblRef`, `FromExpr`).
- **`Row`** — Runtime row representation used during expression evaluation.

## Files

- `query.go` — `Query`, `RangeTblEntry`, `TargetEntry`, join tree types, sort/set-op/CTE definitions, command type and utility type constants.
- `expr.go` — Expression node types, operator kinds, boolean/null test kinds, and `Eval()` implementations.
- `funcs.go` — Built-in function evaluation, type coercion, datum comparison, and runtime helpers (`EvalAnalyzedExpr`, `EvalBool`, `EvalBuiltinFunc`, `FormatInterval`).
- `types.go` — `JoinType`, `ColDef`, and `ForeignKeyDef` shared by the planner and analyzer.
