# plpgsql

PL/pgSQL interpreter that executes ASTs produced by the `gopgsql/parser`. Mirrors PostgreSQL's PL/pgSQL runtime (`src/pl/plpgsql`).

Supports variable declarations, assignments, IF/ELSIF/ELSE, CASE, LOOP/WHILE/FOR, FOREACH, RETURN, RAISE, EXECUTE (dynamic SQL), cursors, exception handling (BEGIN/EXCEPTION), and trigger functions with access to `NEW`/`OLD` row variables and `TG_*` trigger context.
