# executor

Volcano-style plan executor. Walks a `PhysicalNode` tree produced by the planner/optimizer and executes it against the storage engine.

Handles all physical node types: sequential scans, index scans (B-tree, hash, GIN, GiST, SP-GiST, BRIN), bitmap scans, nested-loop and hash joins, filters, projections, aggregations, sorts, limits, distinct, set operations, window aggregates, subquery scans, INSERT/UPDATE/DELETE, and all DDL operations (CREATE TABLE, CREATE INDEX, etc.).

Also serves virtual catalog tables (`pg_class`, `pg_attribute`, `pg_type`, `pg_index`, etc.) for `information_schema` and `pg_catalog` queries.

## Pipeline position

```
Parser → Analyzer → Rewriter → Planner → Optimizer → **Executor**
```
