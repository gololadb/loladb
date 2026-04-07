# Pending PostgreSQL Features

Gap analysis between LolaDB and PostgreSQL. Organized by category with priority
indicators based on how commonly the feature is used in real applications.

**Legend:** 🔴 High priority — 🟡 Medium priority — 🟢 Low priority / niche

---

## What LolaDB Already Supports

For context, here is what is currently implemented:

- **DML:** SELECT, INSERT (VALUES, SELECT), UPDATE, DELETE, TRUNCATE,
  INSERT/UPDATE/DELETE ... RETURNING
- **DDL:** CREATE/DROP TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW,
  CREATE/DROP SCHEMA, CREATE SEQUENCE, ALTER TABLE (ADD/DROP COLUMN,
  ADD CONSTRAINT, RLS enable/disable), CREATE FUNCTION, CREATE TRIGGER,
  CREATE DOMAIN, CREATE TYPE (enum), CREATE POLICY (RLS)
- **Clauses:** WHERE, ORDER BY, LIMIT, OFFSET, GROUP BY, HAVING,
  JOIN (INNER, LEFT, RIGHT, CROSS), DISTINCT, UNION/INTERSECT/EXCEPT,
  WITH / WITH RECURSIVE (CTEs), subqueries in FROM,
  subqueries in expressions (IN, EXISTS, NOT IN, ANY, ALL, scalar, correlated)
- **Expressions:** Arithmetic (+, -, *, /, %), comparison (=, <>, <, >, <=, >=),
  AND/OR/NOT, IS [NOT] NULL, IS TRUE/FALSE/UNKNOWN, CASE (simple + searched),
  CAST, COALESCE, NULLIF, GREATEST, LEAST, LIKE/ILIKE/NOT LIKE/NOT ILIKE,
  BETWEEN, IN (value list), IS [NOT] DISTINCT FROM, string concatenation (`||`),
  JSON operators (`->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`)
- **Constraints:** PRIMARY KEY, UNIQUE (with auto-index creation and enforcement)
- **Aggregates:** count, sum, avg, min, max, bool_and, bool_or, every, string_agg, array_agg
- **Functions:** ~65 scalar functions (math, string, date/time, regex, formatting, encoding)
- **Types:** int32, int64, float64, text, bool, date, timestamp, numeric (with precision/scale),
  json/jsonb, uuid, interval, bytea, money, arrays (+ domains, enums)
- **Indexes:** B+Tree, Hash, BRIN, GIN, GiST, SP-GiST
- **Storage:** Slotted pages, TOAST, WAL, buffer pool (clock-sweep), freelist
- **Concurrency:** MVCC with snapshot isolation, transaction manager
- **Optimizer:** Cost-based with DP join reordering, hash join, nested loop join,
  index scan, bitmap scan, selectivity estimation, column statistics
- **Other:** PL/pgSQL interpreter, pgwire protocol, EXPLAIN, rewrite rules, RLS

---

## 1. SQL Expressions and Operators

### ✅ SIMILAR TO

```sql
SELECT * FROM t WHERE name SIMILAR TO '%(foo|bar)%';
```

Implemented: SQL pattern converted to Go regexp (%, _, |, character classes, grouping).

### ✅ BETWEEN SYMMETRIC

```sql
SELECT * FROM t WHERE x BETWEEN SYMMETRIC 10 AND 5;
```

Implemented: desugars to `(x >= a AND x <= b) OR (x >= b AND x <= a)`.
NOT BETWEEN SYMMETRIC also supported.

### 🟡 Row value comparisons

```sql
SELECT * FROM t WHERE (a, b) > (1, 'x');
```

Comparing composite row values. Not implemented.

### 🟢 Array operators (`@>`, `<@`, `&&`, `||`)

Not applicable until native array types are added.

---

## 2. Data Types

### 🟡 JSON additional operators (`?|`, `?&`, `-`, `#-`)

JSON/JSONB types support `->`, `->>`, `#>`, `#>>`, `@>`, `<@`, and `?` operators.
Still missing: `?|` (any key exists), `?&` (all keys exist), `-` (delete key),
and `#-` (delete path).

### 🟡 Array operators and indexing

Arrays have a native datum type and `TEXT[]` column syntax works. Missing:
`ARRAY[...]` constructor, array indexing (`arr[1]`), slicing, containment
operators (`@>`, `<@`, `&&`), and `unnest()`.

### 🟢 Geometric types (point, line, box, circle, polygon, path)

Niche use case. PostgreSQL supports these with operators and GiST indexing.

### 🟢 Network types (inet, cidr, macaddr)

### 🟢 Range types (int4range, tsrange, etc.)

### 🟢 Composite types (row types)

### 🟢 XML

---

## 3. Query Features

### ✅ Window Functions

```sql
SELECT name, salary, rank() OVER (PARTITION BY dept ORDER BY salary DESC)
FROM employees;

SELECT date, amount, sum(amount) OVER (ORDER BY date) AS running_total
FROM transactions;
```

Supported window functions: `row_number()`, `rank()`, `dense_rank()`, `lag()`,
`lead()`, `first_value()`, `last_value()`, `ntile()`, `percent_rank()`,
`cume_dist()`, `nth_value()`, and aggregate-as-window (`sum`, `count`, `avg`,
`min`, `max`). Supports `PARTITION BY`, `ORDER BY` (ASC/DESC), and `OVER ()`.

### ✅ INSERT ... ON CONFLICT (UPSERT)

```sql
INSERT INTO kv (key, val) VALUES ('a', 1)
ON CONFLICT (key) DO UPDATE SET val = EXCLUDED.val;
```

Supports `DO NOTHING` and `DO UPDATE SET` with `EXCLUDED` pseudo-table.
Conflict target specified by column list. Multi-row VALUES supported.

### ✅ UPDATE ... FROM

```sql
UPDATE orders SET status = 'shipped'
FROM shipments WHERE shipments.order_id = orders.id;
```

Supports multi-table UPDATE with FROM clause. SET expressions can
reference columns from joined tables.

### ✅ FULL OUTER JOIN

```sql
SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id;
```

Implemented in nested loop and hash join executors with inner-row tracking.

### 🟡 LATERAL joins

```sql
SELECT * FROM users u, LATERAL (
  SELECT * FROM orders o WHERE o.user_id = u.id ORDER BY date DESC LIMIT 3
) recent;
```

### 🟡 GROUPING SETS / CUBE / ROLLUP

```sql
SELECT brand, size, sum(sales) FROM items
GROUP BY GROUPING SETS ((brand), (size), ());
```

Advanced grouping. Not implemented.

### ✅ VALUES as a standalone query

```sql
VALUES (1, 'a'), (2, 'b'), (3, 'c');
```

Implemented: bare VALUES produces rows with synthetic column names (column1, column2, ...).

### ✅ Table aliases with column lists

```sql
SELECT a, b FROM (SELECT 1, 2) AS t(a, b);
```

Implemented: column alias lists on both table and subquery aliases.

### 🟢 TABLESAMPLE

```sql
SELECT * FROM big_table TABLESAMPLE BERNOULLI(10);
```

### 🟢 FETCH FIRST / OFFSET ... ROWS (SQL standard syntax)

LIMIT/OFFSET work, but the SQL:2008 standard syntax is not supported.

---

## 4. Transactions

### ✅ Real transaction support (BEGIN / COMMIT / ROLLBACK)

Full transaction control: BEGIN starts a transaction block, COMMIT makes
changes permanent, ROLLBACK undoes all DML (INSERT/UPDATE/DELETE) since BEGIN.
Failed transactions reject commands until ROLLBACK. COMMIT of a failed
transaction performs ROLLBACK (PostgreSQL behavior).

### ✅ SAVEPOINT / ROLLBACK TO SAVEPOINT

```sql
BEGIN;
INSERT INTO t VALUES (1);
SAVEPOINT sp1;
INSERT INTO t VALUES (2);
ROLLBACK TO sp1;
COMMIT;  -- only row 1 is committed
```

Supports nested savepoints, ROLLBACK TO (including recovery from failed
transaction state), and RELEASE SAVEPOINT.

### ✅ Transaction isolation levels

```sql
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SHOW transaction_isolation;
```

SET TRANSACTION ISOLATION LEVEL is accepted (all levels map to snapshot
isolation). SHOW transaction_isolation / default_transaction_isolation /
server_version and other session variables are supported.

### 🟡 Row-level locking (SELECT ... FOR UPDATE / FOR SHARE)

```sql
SELECT * FROM accounts WHERE id = 1 FOR UPDATE;
```

No row-level locks. The design doc mentions this as a future item.

### 🟢 Two-phase commit (PREPARE TRANSACTION)

---

## 5. Constraints

### ✅ FOREIGN KEY / REFERENCES

```sql
CREATE TABLE orders (
  user_id INT REFERENCES users(id) ON DELETE CASCADE
);
```

Supports column-level `REFERENCES` and table-level `FOREIGN KEY` syntax.
Referential integrity enforced on INSERT/UPDATE of child table and
DELETE/UPDATE of parent table. Actions: NO ACTION (default), RESTRICT,
CASCADE, SET NULL, SET DEFAULT.

### ✅ CHECK constraints (column-level)

```sql
CREATE TABLE products (price NUMERIC CHECK (price > 0));
```

CHECK expressions evaluated on INSERT and UPDATE. NULL values pass
(SQL three-valued logic). Named constraints supported via
`CONSTRAINT name CHECK (expr)`.

### 🟡 EXCLUDE constraints

```sql
ALTER TABLE reservations ADD EXCLUDE USING gist (room WITH =, period WITH &&);
```

### 🟡 Deferrable constraints

```sql
CREATE TABLE t (id INT PRIMARY KEY DEFERRABLE INITIALLY DEFERRED);
```

---

## 6. DDL

### 🟡 ALTER TABLE ALTER COLUMN / RENAME COLUMN

```sql
ALTER TABLE users ALTER COLUMN name SET NOT NULL;
ALTER TABLE users RENAME COLUMN name TO full_name;
ALTER TABLE users ALTER COLUMN age TYPE BIGINT;
```

ALTER TABLE supports ADD/DROP COLUMN but not ALTER COLUMN type, SET/DROP NOT NULL,
SET/DROP DEFAULT, or RENAME COLUMN.

### ✅ CREATE TABLE ... AS / SELECT INTO

```sql
CREATE TABLE summary AS SELECT dept, count(*) FROM employees GROUP BY dept;
```

Implemented: creates table from query result columns, inserts data. Supports
IF NOT EXISTS and WITH NO DATA.

### 🟡 CREATE TABLE ... LIKE

```sql
CREATE TABLE new_users (LIKE users INCLUDING ALL);
```

### 🟡 ALTER INDEX / REINDEX

### 🟡 CREATE TEMPORARY TABLE

```sql
CREATE TEMP TABLE scratch (id INT, data TEXT);
```

No temporary table support.

### 🟡 CREATE TABLE with INHERITS (table inheritance)

```sql
CREATE TABLE cities (name TEXT, population INT);
CREATE TABLE capitals (state TEXT) INHERITS (cities);
```

### 🟡 Partitioned tables (PARTITION BY RANGE/LIST/HASH)

```sql
CREATE TABLE logs (ts TIMESTAMP, msg TEXT) PARTITION BY RANGE (ts);
CREATE TABLE logs_2024 PARTITION OF logs FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
```

### 🟢 CREATE TABLESPACE

### 🟢 CREATE EXTENSION

### 🟢 COMMENT ON

### 🟢 SECURITY LABEL

---

## 7. Functions and Operators

### ✅ Type casting with `::`

```sql
SELECT '42'::integer;
```

Both `CAST(x AS type)` and `::` shorthand syntax supported. Casts between
text, integer, bigint, float, boolean, date, timestamp, numeric, json,
uuid, interval, bytea, and arrays. Chained casts (`42::text::integer`)
and casts in expressions (`WHERE val::integer > 100`) work.

### 🟡 Array constructors and operators

```sql
SELECT ARRAY[1, 2, 3];
SELECT arr[1] FROM t;
```

Arrays have a native datum type but `ARRAY[...]` constructor syntax, indexing,
and array-specific operators are not yet supported.

### 🟡 JSON additional operators (`?|`, `?&`, `-`, `#-`)

Core JSON operators (`->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`) are implemented.
Still missing: `?|` (any key exists), `?&` (all keys exist), `-` (delete key/index),
and `#-` (delete path).

### ✅ Pattern matching operators (`~`, `~*`, `!~`, `!~*`)

Implemented: POSIX regex operators dispatched to Go's regexp package.

### ✅ `num_nonnulls()` / `num_nulls()`

Implemented: count non-null/null arguments.

### ✅ `starts_with()` / `^@` operator

Implemented: `starts_with(text, prefix)` function and `^@` operator.

### 🟢 Full-text search operators (`@@`, `to_tsvector`, `to_tsquery`)

---

## 8. Aggregate Functions

### 🟡 Ordered-set aggregates

```sql
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY salary) FROM employees;
SELECT mode() WITHIN GROUP (ORDER BY status) FROM orders;
```

`percentile_cont`, `percentile_disc`, `mode`.

### 🟡 Statistical aggregates

`stddev`, `stddev_pop`, `stddev_samp`, `variance`, `var_pop`, `var_samp`,
`corr`, `covar_pop`, `covar_samp`, `regr_*`.

### 🟡 Hypothetical-set aggregates

`rank(val) WITHIN GROUP (ORDER BY col)`, `dense_rank`, `percent_rank`, `cume_dist`.

### 🟢 `xmlagg`, `json_agg`, `jsonb_agg`, `json_object_agg`

---

## 9. System Catalog and Introspection

### ✅ information_schema

```sql
SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';
SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'users';
```

Virtual catalog tables: `information_schema.tables` (table_catalog,
table_schema, table_name, table_type), `information_schema.columns`
(column_name, ordinal_position, column_default, is_nullable, data_type),
`information_schema.schemata`. Queryable with WHERE, ORDER BY, joins.

### ✅ pg_catalog system views

`pg_tables`, `pg_indexes`, `pg_views`, `pg_roles`, `pg_stat_user_tables`,
`pg_namespace`. Accessible both qualified (`pg_catalog.pg_tables`) and
unqualified (`pg_tables`). Generated dynamically from catalog metadata.

### ✅ System information functions

`current_user`, `current_database()`, `current_schema()`, `version()`,
`pg_typeof()`, `pg_table_size()`, `pg_total_relation_size()`,
`pg_table_is_visible()`, `pg_backend_pid()`, `pg_postmaster_start_time()`,
`inet_server_addr()`, `inet_server_port()`, `has_table_privilege()`,
`has_schema_privilege()`, `has_database_privilege()`, and more.

### ✅ Object information functions

`obj_description()`, `col_description()`, `shobj_description()`,
`pg_get_viewdef()`, `pg_get_indexdef()`, `pg_get_constraintdef()`,
`pg_get_expr()`.

---

## 10. COPY and Bulk Operations

### ✅ COPY

```sql
COPY users FROM '/path/to/users.csv' WITH (FORMAT csv, HEADER);
COPY (SELECT * FROM users) TO STDOUT WITH (FORMAT csv);
```

Implemented: COPY TO STDOUT (text/csv), COPY FROM file (text/csv), COPY (query) TO STDOUT,
column lists, HEADER option, NULL handling, and text-format escaping.

### ✅ COPY ... FROM STDIN (pgwire protocol)

The pgwire COPY sub-protocol is implemented: CopyInResponse/CopyData/CopyDone/CopyFail
message handling for streaming bulk data from clients.

---

## 11. Maintenance and Administration

### ✅ VACUUM

```sql
VACUUM;
VACUUM FULL users;
VACUUM ANALYZE;
```

Implemented: VACUUM reclaims dead tuples from MVCC deletes/updates, compacts pages,
and frees empty pages. Supports VACUUM, VACUUM FULL, VACUUM FREEZE, VACUUM ANALYZE,
and bare VACUUM (all tables). FREEZE is accepted but is a no-op.

### ✅ ANALYZE (statistics collection)

```sql
ANALYZE users;
```

Implemented: ANALYZE refreshes column statistics (NDistinct, NullFrac, MCV) via
catalog.Stats(). Supports single-table and all-tables modes.

### 🟡 CLUSTER

```sql
CLUSTER users USING users_pkey;
```

Physically reorder table rows to match an index.

### 🟡 REINDEX

```sql
REINDEX TABLE users;
```

### 🟢 pg_stat_statements / query statistics

### 🟢 LISTEN / NOTIFY

```sql
LISTEN channel_name;
NOTIFY channel_name, 'payload';
```

Async notification system. Niche but useful for real-time applications.

---

## 12. Prepared Statements and Cursors

### ✅ PREPARE / EXECUTE (server-side)

```sql
PREPARE get_user(int) AS SELECT * FROM users WHERE id = $1;
EXECUTE get_user(42);
DEALLOCATE get_user;
```

Implemented: SQL-level PREPARE stores parameterized queries with `$N` parameter
references. EXECUTE substitutes parameters and runs through the full pipeline.
DEALLOCATE and DEALLOCATE ALL supported. Works with SELECT, INSERT, UPDATE, DELETE.

### 🟡 DECLARE / FETCH / CLOSE (cursors)

```sql
DECLARE cur CURSOR FOR SELECT * FROM large_table;
FETCH 100 FROM cur;
CLOSE cur;
```

No cursor support.

---

## 13. Security

### 🟡 GRANT / REVOKE (fine-grained)

```sql
GRANT SELECT ON users TO readonly_role;
REVOKE INSERT ON users FROM public;
```

ACL infrastructure exists in the catalog but SQL-level GRANT/REVOKE for
table-level and column-level privileges is not fully wired.

### 🟡 Column-level privileges

```sql
GRANT SELECT (name, email) ON users TO app_role;
```

### 🟢 Row-level security with USING and WITH CHECK

RLS policies exist but the `WITH CHECK` clause (for INSERT/UPDATE validation)
may not be fully enforced.

---

## 14. Procedural Languages

### 🟡 PL/pgSQL: EXCEPTION handling

```sql
BEGIN
  INSERT INTO t VALUES (1);
EXCEPTION WHEN unique_violation THEN
  UPDATE t SET val = val + 1 WHERE id = 1;
END;
```

The PL/pgSQL interpreter supports IF, CASE, LOOP, WHILE, FOR, RETURN, RAISE,
PERFORM, EXECUTE, but EXCEPTION blocks are not implemented.

### 🟡 PL/pgSQL: FOREACH ... IN ARRAY

### 🟡 PL/pgSQL: RETURN NEXT / RETURN QUERY (set-returning functions)

### 🟢 Other PL languages (PL/Python, PL/Perl, PL/v8)

---

## 15. Advanced Features

### ✅ Generated columns (STORED)

```sql
CREATE TABLE t (
  a INT,
  b INT,
  c INT GENERATED ALWAYS AS (a + b) STORED
);
```

Stored generated columns are computed on INSERT and UPDATE. Explicit writes
to generated columns are rejected.

### 🟡 Identity columns (GENERATED ALWAYS AS IDENTITY)

```sql
CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY);
```

SERIAL/BIGSERIAL work via sequences, but the SQL-standard identity column syntax
is not supported.

### 🟡 Materialized views

```sql
CREATE MATERIALIZED VIEW monthly_sales AS
SELECT month, sum(amount) FROM sales GROUP BY month;

REFRESH MATERIALIZED VIEW monthly_sales;
```

### 🟡 Event triggers

```sql
CREATE EVENT TRIGGER audit_ddl ON ddl_command_end
EXECUTE FUNCTION log_ddl();
```

### 🟢 Foreign data wrappers (FDW)

```sql
CREATE FOREIGN TABLE remote_users (...) SERVER remote_pg;
```

### 🟢 Logical replication / publications / subscriptions

### 🟢 Advisory locks

```sql
SELECT pg_advisory_lock(12345);
```

---

## Summary by Priority

### Must-have for basic SQL compatibility (🔴)

| Feature | Category |
|---|---|

### Important for real-world applications (🟡)

| Feature | Category |
|---|---|
| LATERAL joins | Queries |
| GROUPING SETS / CUBE / ROLLUP | Queries |
| FOR UPDATE / FOR SHARE | Transactions |

| TEMPORARY tables | DDL |
| Table partitioning | DDL |
| JSON additional operators (`?|`, `?&`, `-`, `#-`) | Operators |
| Array operators and indexing | Types |
| Statistical aggregates | Aggregates |
| Ordered-set aggregates | Aggregates |
| Cursors | Queries |
| GRANT / REVOKE | Security |
| PL/pgSQL EXCEPTION blocks | PL/pgSQL |
| Materialized views | DDL |
| ANALYZE command | Maintenance |
