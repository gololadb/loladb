# sql

Top-level SQL session coordinator. Ties together the full query processing pipeline: parsing, analysis, rewriting, planning, optimization, and execution.

`Executor` is the main entry point. It:

- Manages transaction state, savepoints, and cursors.
- Wires up runtime callbacks for subquery execution, user-defined functions, enum ordinals, and session settings.
- Dispatches procedural language calls to PL/pgSQL, PL/JS, and PL/Starlark interpreters.
- Handles `EXPLAIN` / `EXPLAIN ANALYZE`.
- Coordinates multi-statement execution and implicit transactions.
