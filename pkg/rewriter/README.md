# rewriter

Implements the query rewrite rule system, mirroring PostgreSQL's rewriter (`src/backend/rewrite/rewriteHandler.c`).

The rewriter sits between the analyzer and the planner. Its primary job is to expand views: when a query references a view, the rewriter replaces that reference with the view's defining subquery (the `_RETURN` rule). It also supports DML rules (`ON INSERT/UPDATE/DELETE DO INSTEAD/ALSO`).

## Pipeline position

```
Parser → Analyzer → **Rewriter** → Planner → Optimizer → Executor
```

The rewriter receives a `*querytree.Query` and returns one or more rewritten queries. A single input query may produce multiple output queries when `ALSO` rules fire.
