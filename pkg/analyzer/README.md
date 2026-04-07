# analyzer

Performs semantic analysis on raw parse trees, producing resolved query trees (`querytree.Query`). This mirrors PostgreSQL's `parse_analyze()` / `transformStmt()` pipeline.

The analyzer:

- Resolves table and column names against the catalog.
- Resolves function calls, aggregates, and window functions.
- Infers and checks expression types.
- Transforms subqueries, CTEs, and set operations.
- Handles INSERT/UPDATE/DELETE target resolution and RETURNING clauses.
- Transforms utility statements (DDL) into `UtilityStmt` nodes.

## Pipeline position

```
Parser → **Analyzer** → Rewriter → Planner → Optimizer → Executor
```

The analyzer receives a `parser.Stmt` (from the external `gopgsql/parser`) and returns a `*querytree.Query` ready for the rewriter.
