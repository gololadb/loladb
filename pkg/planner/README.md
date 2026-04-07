# planner

Converts analyzed query trees into executable plans through two stages: logical planning and physical optimization.

## Logical planning

`QueryToLogicalPlan` transforms a `querytree.Query` into a tree of `LogicalNode` values — scans, joins, filters, projections, aggregations, sorts, limits, set operations, window aggregates, and DDL/DML nodes. This is a direct structural translation with no cost reasoning.

## Physical optimization

The `Optimizer` converts the logical plan into a `PhysicalNode` tree using cost-based decisions. It chooses between sequential scans vs. index scans, nested-loop joins vs. hash joins, and applies predicate pushdown. Cost estimation uses configurable constants modeled after PostgreSQL's defaults.

## Pipeline position

```
Parser → Analyzer → Rewriter → **Planner → Optimizer** → Executor
```

## Files

- `logical.go` — Logical plan node types (`LogicalScan`, `LogicalJoin`, `LogicalFilter`, `LogicalProject`, etc.) and DDL/DML nodes.
- `physical.go` — Physical plan node types (`PhysSeqScan`, `PhysIndexScan`, `PhysHashJoin`, `PhysNestedLoopJoin`, etc.).
- `query_to_plan.go` — `QueryToLogicalPlan` and expression conversion from analyzed form to executable form.
- `optimizer.go` — Cost-based optimizer that produces physical plans from logical plans.
- `cost.go` — Cost model constants and `PlanCost` type.
- `explain.go` — `EXPLAIN` output formatting for physical plan trees.
