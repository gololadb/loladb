# plstarlark

Starlark procedural language for LolaDB. Starlark is a Python-like language designed for safe embedding with deterministic, hermetic execution (no filesystem access, no network, no threads).

Functions receive arguments as Starlark values and can execute SQL via a built-in `sql_exec()` callback. Return values are converted back to PostgreSQL datum types.
