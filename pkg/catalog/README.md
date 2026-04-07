# catalog

System catalog managing all database metadata. Mirrors PostgreSQL's `pg_catalog` system tables.

Tracks tables, columns, indexes, sequences, views, functions, triggers, domains, enums, roles, policies, schemas, and rewrite rules. The catalog is bootstrapped from scratch on first startup and is fully self-describing afterward.

## Key components

- **`Catalog`** — Central struct holding all metadata. Provides lookup, creation, alteration, and drop methods for every object type.
- **`syscache`** — In-memory cache for catalog lookups, mirroring PostgreSQL's catcache/syscache layer. Avoids repeated heap scans for hot metadata.
- **`auth`** — Role and membership management (`pg_authid`, `pg_auth_members`).
- **`bootstrap`** — Initial catalog population (system tables, built-in types, default roles).
- **`functions`** — Built-in function and aggregate registry with type resolution.
- **`oids`** — Well-known OID constants mirroring PostgreSQL's `pg_catalog`.
- **`policies`** — Row-level security policy storage and evaluation.
- **`rules`** — Rewrite rule storage for views and DML rules.
- **`schema`** — Schema (namespace) management.
- **`types`** — Type metadata and OID-to-type mappings.
