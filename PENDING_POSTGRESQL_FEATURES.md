# Pending PostgreSQL Features

Gap analysis between LolaDB and PostgreSQL. Only features not yet implemented
are listed here. See git history for previously completed items.

---

## What LolaDB Already Supports

- **DML:** SELECT, INSERT (VALUES, SELECT), UPDATE, DELETE, TRUNCATE,
  INSERT/UPDATE/DELETE ... RETURNING
- **DDL:** CREATE/DROP TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW,
  CREATE/DROP SCHEMA, CREATE SEQUENCE, ALTER TABLE (ADD/DROP COLUMN,
  ADD CONSTRAINT, OWNER TO, RLS enable/disable), ALTER TABLE ONLY,
  ALTER TABLE ATTACH/DETACH PARTITION, CREATE TABLE ... PARTITION BY
  (RANGE/LIST/HASH), CREATE TABLE ... INHERITS (table inheritance),
  CREATE FUNCTION, CREATE TRIGGER, CREATE EVENT TRIGGER,
  CREATE DOMAIN, CREATE TYPE (enum), CREATE POLICY (RLS),
  CREATE AGGREGATE (user-defined aggregates with sfunc/stype/initcond/finalfunc),
  CREATE TABLE ... AS, CREATE TABLE ... LIKE, CREATE TEMPORARY TABLE,
  CREATE TABLESPACE (no-op), CREATE EXTENSION (no-op), COMMENT ON,
  SECURITY LABEL, REINDEX, ALTER INDEX RENAME
- **Clauses:** WHERE, ORDER BY, LIMIT, OFFSET, FETCH FIRST/OFFSET ROWS,
  GROUP BY, HAVING, GROUPING SETS / CUBE / ROLLUP,
  JOIN (INNER, LEFT, RIGHT, FULL OUTER, CROSS), LATERAL joins, DISTINCT,
  UNION/INTERSECT/EXCEPT, WITH / WITH RECURSIVE (CTEs), subqueries in FROM,
  subqueries in expressions (IN, EXISTS, NOT IN, ANY, ALL, scalar, correlated),
  VALUES as standalone query, table aliases with column lists, TABLESAMPLE
- **Expressions:** Arithmetic (+, -, *, /, %), comparison (=, <>, <, >, <=, >=),
  AND/OR/NOT, IS [NOT] NULL, IS TRUE/FALSE/UNKNOWN, CASE (simple + searched),
  CAST, ::, COALESCE, NULLIF, GREATEST, LEAST, LIKE/ILIKE/NOT LIKE/NOT ILIKE,
  SIMILAR TO, BETWEEN, BETWEEN SYMMETRIC, IN (value list),
  IS [NOT] DISTINCT FROM, string concatenation (`||`),
  array concatenation (`||`), row value comparisons,
  JSON operators (`->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?|`, `?&`, `-`, `#-`),
  array operators (`@>`, `<@`, `&&`),
  full-text search (`@@`, `to_tsvector`, `to_tsquery`),
  geometric distance (`<->`), geometric same (`~=`),
  pattern matching (`~`, `~*`, `!~`, `!~*`), starts_with / `^@`
- **Constraints:** PRIMARY KEY, UNIQUE, FOREIGN KEY (CASCADE, SET NULL, RESTRICT,
  SET DEFAULT, NO ACTION), CHECK, EXCLUDE (accepted), DEFERRABLE / INITIALLY DEFERRED
- **Window functions:** row_number, rank, dense_rank, lag, lead, first_value,
  last_value, ntile, percent_rank, cume_dist, nth_value, aggregate-as-window
- **Aggregates:** count, sum, avg, min, max, bool_and, bool_or, every,
  string_agg, array_agg, json_agg, jsonb_agg, json_object_agg, jsonb_object_agg,
  xmlagg, percentile_cont, percentile_disc, mode,
  stddev, stddev_pop, stddev_samp, variance, var_pop, var_samp,
  corr, covar_pop, covar_samp, regr_slope, regr_intercept, regr_count, regr_r2,
  regr_avgx, regr_avgy, regr_sxx, regr_syy, regr_sxy,
  hypothetical-set (rank, dense_rank, percent_rank, cume_dist WITHIN GROUP)
- **Functions:** ~65+ scalar functions (math, string, date/time, regex, formatting,
  encoding, network, range, geometric, XML, system info, object info),
  num_nonnulls, num_nulls
- **Types:** int32, int64, float64, text, bool, date, timestamp, numeric,
  json/jsonb, uuid, interval, bytea, money, arrays, domains, enums,
  inet/cidr/macaddr, range types (int4range, tsrange, etc.),
  XML, composite types (CREATE TYPE AS accepted),
  geometric types (point, line, lseg, box, path, polygon, circle)
- **Indexes:** B+Tree, Hash, BRIN, GIN, GiST, SP-GiST
- **Storage:** Slotted pages, TOAST, WAL, buffer pool (clock-sweep), freelist
- **Concurrency:** MVCC with snapshot isolation, transaction manager,
  BEGIN/COMMIT/ROLLBACK, SAVEPOINT/ROLLBACK TO, transaction isolation levels,
  SELECT FOR UPDATE/FOR SHARE (accepted), PREPARE TRANSACTION (accepted)
- **Optimizer:** Cost-based with DP join reordering, hash join, nested loop join,
  index scan, bitmap scan, selectivity estimation, column statistics
- **COPY:** COPY TO STDOUT, COPY FROM file, COPY FROM STDIN (pgwire),
  text/csv formats, HEADER, column lists
- **Maintenance:** VACUUM (FULL, FREEZE, ANALYZE), ANALYZE (column statistics),
  CLUSTER (accepted)
- **Other:** PL/pgSQL interpreter (EXCEPTION blocks, FOREACH ARRAY,
  RETURN NEXT/QUERY, user-defined function calls in SQL expressions),
  PL/JS (JavaScript via goja, plv8-compatible SPI bridge),
  PL/Starlark (Python-like via starlark-go, spi.execute() bridge),
  pgwire protocol, EXPLAIN, INSERT ON CONFLICT (UPSERT), UPDATE FROM,
  rewrite rules, RLS, GRANT/REVOKE ON SCHEMA,
  set_config/current_setting (session GUC), tsvector_update_trigger,
  LISTEN/NOTIFY/UNLISTEN (accepted), advisory locks (no-op),
  pg_stat_statements (query counting), PREPARE/EXECUTE/DEALLOCATE,
  cursors (DECLARE/FETCH/CLOSE), materialized views (CREATE/REFRESH),
  FDW (CREATE FOREIGN TABLE/SERVER/DATA WRAPPER, no-op),
  logical replication (CREATE PUBLICATION/SUBSCRIPTION, no-op)

---

## Remaining Pending Features

All tracked features have been implemented. The PENDING list is empty.
