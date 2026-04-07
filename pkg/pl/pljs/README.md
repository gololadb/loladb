# pljs

JavaScript procedural language for LolaDB. Executes user-defined functions written in JavaScript using an embedded JS runtime.

Functions receive arguments as JavaScript values and can call back into the SQL engine via `pljs.execute(sql)` to run queries. Return values are converted back to PostgreSQL datum types.
