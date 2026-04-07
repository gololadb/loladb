# pl

Procedural language runtimes for user-defined functions and triggers.

| Package | Language | Description |
|---------|----------|-------------|
| `plpgsql` | PL/pgSQL | PostgreSQL's standard procedural language. Interprets PL/pgSQL ASTs with support for variables, control flow, loops, cursors, exception handling, and trigger functions. |
| `pljs` | PL/JS | JavaScript procedural language. Executes functions in an embedded JS runtime with SQL callback support. |
| `plstarlark` | PL/Starlark | Starlark (Python-like) procedural language. Designed for safe embedding with deterministic execution. |

All three runtimes share a common callback interface (`SQLExecFunc`) for executing SQL from within procedural code.
