# pkg

Internal packages for LolaDB. The query processing pipeline flows through these packages in order:

```
Parser → Analyzer → Rewriter → Planner → Optimizer → Executor
```

| Package | Role |
|---------|------|
| `querytree` | Query tree data structures shared across the pipeline |
| `analyzer` | Semantic analysis — resolves names, types, and expressions |
| `rewriter` | View expansion and DML rule rewriting |
| `planner` | Logical/physical plan generation and cost-based optimization |
| `executor` | Volcano-style plan execution |
| `sql` | Top-level SQL session coordinator — ties the pipeline together |
| `catalog` | System catalog (tables, columns, indexes, roles, policies) |
| `storage` | Page-oriented storage engine with WAL and buffer pool |
| `mvcc` | Multi-version concurrency control and snapshot isolation |
| `pgwire` | PostgreSQL v3 wire protocol (frontend/backend messaging) |
| `tuple` | Row serialization, datum types, and type system |
| `pl` | Procedural language runtimes (PL/pgSQL, PL/JS, PL/Starlark) |
