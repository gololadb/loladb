[![Built with Ona](https://ona.com/build-with-ona.svg)](https://app.ona.com/#https://github.com/gololadb/loladb)

# LolaDB

A PostgreSQL-inspired relational database written in pure Go. No C dependencies, no CGo, no embedded database libraries — every layer from page I/O to the SQL optimizer is implemented from scratch.

This is a **learning project**. It exists to explore how a real database engine works by building one, piece by piece, following PostgreSQL's internal architecture as a reference. It is not intended for production use. If you're looking for a production database, use PostgreSQL.

That said, if you want to read the source, poke at the internals, break things, or add features — you're very welcome. The codebase is ~31k lines of Go with no code generation, and every component is designed to be readable.

## What it can do

- Full SQL pipeline: parsing → analysis → rewriting → planning → optimization → execution
- PostgreSQL wire protocol (`psql`, `pg_dump`, and other PostgreSQL clients connect directly)
- MVCC with snapshot isolation
- Write-ahead logging (WAL) with crash recovery
- Cost-based query optimizer with join reordering (dynamic programming)
- Six index types: B-tree, Hash, GIN, GiST, SP-GiST, BRIN
- TOAST for large values
- Buffer pool with clock-sweep eviction
- Roles, privileges, row-level security, and password authentication
- TLS support (auto-generated self-signed certificates or custom)
- `pg_dump` compatibility — produces valid, restorable PostgreSQL dumps
- Interactive CLI, TUI, and single-command execution modes

## ⚠️ Not for production

LolaDB lacks many things a production database needs: crash-safe checkpointing under concurrent load, proper connection pooling, replication, VACUUM, most of the PostgreSQL type system, and thousands of edge cases that PostgreSQL handles correctly after decades of development. **Do not store data you care about in LolaDB.**

## Getting started

```
go build -o loladb ./cmd/loladb
```

### Create a database and run queries

```
./loladb create mydb.lodb
./loladb cli mydb.lodb
```

```sql
CREATE TABLE users (id INTEGER, name TEXT, email TEXT);
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
SELECT * FROM users;
```

### Start a PostgreSQL-compatible server

```
./loladb serve mydb.lodb :5432
```

Then connect with any PostgreSQL client:

```
psql -h 127.0.0.1 -U loladb -d loladb
```

### Other commands

```
loladb create <path>            Create a new database
loladb info <path>              Display database metadata
loladb cli <path>               Interactive SQL shell
loladb exec <path> "<sql>"      Execute a single statement
loladb serve <path> [addr]      Start wire protocol server (default :5432)
loladb tui <path>               Terminal UI shell
loladb <path> < file.sql        Import SQL from stdin
```

## Architecture

The engine follows a layered architecture inspired by PostgreSQL's internals. Each layer depends only on the layers below it.

```
┌──────────────────────────────────────────────┐
│  CLI / TUI / pgwire server                   │
├──────────────────────────────────────────────┤
│  SQL Parser (gopgsql — recursive descent)    │
├──────────────────────────────────────────────┤
│  Analyzer → Rewriter → Planner → Optimizer   │
├──────────────────────────────────────────────┤
│  Executor (Volcano-style iterators)          │
├──────────────────────────────────────────────┤
│  Catalog (pg_class, pg_attribute, pg_type…)  │
├──────────────────────────────────────────────┤
│  Index AM (B-tree, Hash, GIN, GiST, …)      │
├──────────────────────────────────────────────┤
│  MVCC (snapshot isolation)                   │
├──────────────────────────────────────────────┤
│  Tuple encoding / TOAST                      │
├──────────────────────────────────────────────┤
│  Slotted pages (heap storage)                │
├──────────────────────────────────────────────┤
│  WAL (write-ahead log)                       │
├──────────────────────────────────────────────┤
│  Buffer pool (clock-sweep)                   │
├──────────────────────────────────────────────┤
│  Page I/O                                    │
└──────────────────────────────────────────────┘
         │
         ▼
   database.lodb  +  database.lodb.wal
```

## Project structure

```
loladb/
├── cmd/loladb/           # CLI, TUI, server, import commands
├── pkg/
│   ├── pageio/           # Raw page read/write (4KB pages)
│   ├── bufferpool/       # Clock-sweep buffer pool
│   ├── wal/              # Write-ahead log
│   ├── slottedpage/      # Slotted page heap storage
│   ├── tuple/            # Tuple encoding/decoding
│   ├── toast/            # TOAST for oversized values
│   ├── freelist/         # Page allocation bitmap
│   ├── superblock/       # Database metadata page
│   ├── mvcc/             # Snapshot isolation
│   ├── index/            # Index access methods
│   │   ├── btree/        #   B-tree
│   │   ├── hash/         #   Hash
│   │   ├── gin/          #   GIN (inverted)
│   │   ├── gist/         #   GiST (generalized search tree)
│   │   ├── spgist/       #   SP-GiST (space-partitioned)
│   │   └── brin/         #   BRIN (block range)
│   ├── engine/           # Ties storage layers together
│   ├── catalog/          # System catalogs (pg_class, pg_attribute, …)
│   ├── sql/              # SQL pipeline: parse → analyze → plan → execute
│   ├── planner/          # Analyzer, logical/physical planning, optimizer
│   ├── rewriter/         # Query rewriter (view expansion, RLS)
│   ├── executor/         # Volcano-style plan execution
│   └── pgwire/           # PostgreSQL v3 wire protocol
├── test/                 # Integration tests
│   └── pagila/           # Pagila dataset tests (optimizer validation)
└── design.md             # Detailed architecture document
```

## Running tests

```
go test ./...
```

There are ~395 tests covering storage, catalog, SQL execution, the wire protocol, and query plan comparison against PostgreSQL 16 using the Pagila dataset.

## Exploring the code

Some good starting points:

- **How a query executes end-to-end**: start at `pkg/sql/executor.go` (`Exec` method), which calls the parser, analyzer, rewriter, planner, and executor in sequence.
- **How pages are stored**: `pkg/slottedpage/` implements PostgreSQL-style slotted pages. `pkg/bufferpool/` manages the in-memory cache.
- **How the optimizer works**: `pkg/planner/optimizer.go` implements cost-based optimization with dynamic programming for join ordering. `pkg/planner/cost.go` has the cost model.
- **How the wire protocol works**: `pkg/pgwire/pgwire.go` implements the PostgreSQL v3 frontend/backend protocol. `pkg/pgwire/pgcompat.go` handles pg_dump compatibility.
- **How indexes work**: each subdirectory under `pkg/index/` is a self-contained index access method.
- **The full architecture**: `design.md` has a detailed description of every layer.

## Contributing

This is a personal learning project, but contributions are welcome. If you want to add a feature, fix a bug, or just experiment — go for it. There's no formal process; open a PR and we'll figure it out.

Some areas that would be interesting to work on:

- More SQL support (aggregates, subqueries, CTEs, window functions)
- VACUUM and dead tuple cleanup
- Parallel query execution
- More complete type system
- COPY FROM (import) via the wire protocol
- Better error messages

## License

MIT
