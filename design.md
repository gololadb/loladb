# LolaDB — Architecture and Implementation Plan

## Project Vision

LolaDB is an embedded relational database engine in a single file, written in Go, strongly inspired by the internal fundamentals of PostgreSQL. The goal is to offer real guarantees of durability (WAL), concurrency (MVCC with snapshot isolation), and a storage model based on slotted pages and B+Tree indexes — all contained in a single `.lodb` file plus an auxiliary WAL file.

The intention is not to replicate PostgreSQL, but to build a didactic and functional engine that respects its fundamental architectural principles and can evolve incrementally.

---

## 1. File Model

```
database.lodb          ← data pages (fixed size: 4096 bytes)
database.lodb.wal      ← write-ahead log (append-only)
```

All persistent state lives in fixed-size pages within the `.lodb` file. The file is treated as an array of pages addressable by number (`pageNum * PAGE_SIZE`). The WAL is a separate sequential file that is truncated after each successful checkpoint.

### Reserved Page Layout

| Page | Content | Description |
|------|---------|-------------|
| 0 | Superblock | Global metadata, catalog pointers, OID/XID generators |
| 1 | WAL Control | LSN of last checkpoint, recovery state |
| 2 | Free-list Bitmap | Page allocation bitmap (1 bit = 1 page) |
| 3+ | Data | Heap pages, B+Tree pages, overflow pages |

---

## 2. Architecture Layers

The architecture follows a strictly layered model. Each layer only depends on the layers below it.

```
┌─────────────────────────────────────────────┐
│  CLI Tool (loladb cli/tui/exec/info/create) │
├─────────────────────────────────────────────┤
│  Layer 10: SQL Parser (postgresql-parser)    │
│  DDL · DML · SELECT · JOINs · EXPLAIN       │
├─────────────────────────────────────────────┤
│  Layer 9: Analyzer + Rewriter               │
│  Name resolution, type checking, pushdown   │
├─────────────────────────────────────────────┤
│  Layer 9: Planner / Optimizer               │
│  Path generation · Join ordering (DP)       │
│  Cost model · Selectivity estimation        │
│  Nested Loop / Hash Join / Merge Join       │
├─────────────────────────────────────────────┤
│  Layer 8: Executor (Volcano iterators)      │
│  Open / Next / Close per plan node          │
├─────────────────────────────────────────────┤
│  Layer 7: Catalog (pg_class, pg_attribute)  │
├─────────────────────────────────────────────┤
│  Layer 6: B+Tree Index                      │
├─────────────────────────────────────────────┤
│  Layer 5: MVCC — Snapshot Isolation         │
├─────────────────────────────────────────────┤
│  Layer 4: Tuple Encoding / Decoding + TOAST │
├─────────────────────────────────────────────┤
│  Layer 3: Slotted Pages (Heap)              │
├─────────────────────────────────────────────┤
│  Layer 2: WAL (Write-Ahead Log)             │
├─────────────────────────────────────────────┤
│  Layer 1: Buffer Pool (Clock-Sweep)          │
├─────────────────────────────────────────────┤
│  Layer 0: Page I/O (raw read/write)         │
└─────────────────────────────────────────────┘
         │
         ▼
   [ database.lodb ]   [ database.lodb.wal ]
```

---

## 3. Detailed Design by Layer

### 3.0 — Page I/O

Responsibility: reading and writing 4096-byte blocks to/from the file, with file-level mutual exclusion.

Main interface:
- `ReadPage(pageNum, buf)` — reads a page; returns zeros if beyond EOF.
- `WritePage(pageNum, buf)` — writes a page, extending the file if necessary.
- `Sync()` — `fsync` of the file.
- `FilePages()` — total number of pages in the file.

Design decisions: `ReadAt`/`WriteAt` are used with a global mutex. In future phases, this can be migrated to `pwrite`/`pread` for page-level concurrency, or even `mmap` for reads.

### 3.1 — Buffer Pool

Responsibility: in-memory page cache with a clock-sweep replacement policy (the same one PostgreSQL uses for `shared_buffers`).

Key concepts:
- Each frame has a `pinCount` (number of active users) and a `usageCount` (for clock-sweep).
- A pinned page cannot be evicted.
- Pages marked as dirty are flushed before eviction.
- The default size is 256 frames (1 MB), configurable.

Interface:
- `FetchPage(pageNum) → buf` — loads or retrieves from cache; increments pin.
- `ReleasePage(pageNum)` — decrements pin.
- `MarkDirty(pageNum)` — marks for deferred write.
- `FlushAll()` — writes all dirty pages to disk and performs `fsync`.

The clock-sweep traverses frames circularly: if a frame has `pinCount > 0`, it skips it; if `usageCount > 0`, it decrements it; if both are 0, it is the victim.

### 3.2 — WAL (Write-Ahead Log)

Responsibility: ensuring durability and enabling crash recovery. Every modification to a data page is first recorded in the WAL.

WAL record format:

```
┌──────┬──────┬─────────┬────────┬──────┬──────────┐
│ LSN  │ XID  │ PageNum │ Offset │ Len  │ Data     │
│ 4B   │ 4B   │ 4B      │ 2B     │ 2B   │ variable │
└──────┴──────┴─────────┴────────┴──────┴──────────┘
```

WAL-before-data protocol:
1. Serialize the WAL record.
2. Append to the `.wal` file.
3. Only then modify the page in the buffer pool.
4. On checkpoint: flush buffer pool → fsync data file → update LSN in superblock → truncate WAL.

Recovery: when opening the database, if the WAL LSN exceeds the superblock's LSN, the records are replayed by applying each change to the corresponding page.

### 3.3 — Slotted Pages

Responsibility: storing variable-length tuples within a fixed-size page, following PostgreSQL's design.

```
┌─────────────────────────────────────────────┐
│  Page Header (24 bytes)                     │
│  ┌─────┬─────┬────────┬────────┬──────────┐ │
│  │type │flags│lower   │upper   │special   │ │
│  │ 1B  │ 1B  │ 2B     │ 2B     │ 2B       │ │
│  ├─────┴─────┴────────┴────────┴──────────┤ │
│  │pageNum (4B) │ lsn (4B)                 │ │
│  │numSlots(2B) │ freeSpace(2B)            │ │
│  │nextPage(4B)                            │ │
│  └────────────────────────────────────────┘ │
├─────────────────────────────────────────────┤
│  Line Pointers (grow →)                     │
│  [offset:2B | length:2B] × N               │
├─────────────────────────────────────────────┤
│              free space                     │
├─────────────────────────────────────────────┤
│  Tuple Data (← grows from the end)         │
│  Tuples are stacked from the end of the    │
│  page toward the center.                    │
└─────────────────────────────────────────────┘
```

- `lower` points to the end of the line pointer array.
- `upper` points to the start of the first tuple (or to the end of the page if empty).
- Free space is `upper - lower - sizeof(LinePointer)`.
- `special` marks the start of a special area at the end of the page (used by B+Tree for node metadata).

Insertion: it is calculated whether the tuple + a new line pointer will fit. The tuple is written just below `upper`, and a line pointer is added at the end of the array.

### 3.4 — Tuple Encoding

Responsibility: serializing and deserializing tuples with MVCC headers and typed columns.

Tuple format:

```
┌─────────────────── Tuple Header (16B) ──────────────────┐
│ xmin (4B) │ xmax (4B) │ flags (2B) │ natts (2B) │ datalen (4B) │
├─────────────────── Payload ─────────────────────────────┤
│ [type:1B | data:variable] × natts                       │
└─────────────────────────────────────────────────────────┘
```

Supported types (initial phase):
- `Null` (0 bytes of data)
- `Int32` (4 bytes)
- `Int64` (8 bytes)
- `Text` (2 bytes length + N bytes UTF-8)
- `Bool` (1 byte)
- `Float64` (8 bytes)

Each value is prefixed with a 1-byte type tag, which allows flexible schemas and facilitates format evolution.

### 3.5 — MVCC (Multi-Version Concurrency Control)

Responsibility: enabling concurrent reads without locks through snapshot isolation, following PostgreSQL's model.

Each physical tuple carries `xmin` (transaction that created it) and `xmax` (transaction that deleted it, 0 if alive). Data is not immediately deleted — it is marked as dead so that VACUUM can clean it up later.

Snapshot `S` visibility rules:
- A tuple is visible if `xmin` belongs to a committed transaction that started before `S`, and `xmax` is 0 or belongs to a transaction that is still active or future with respect to `S`.

Components:
- `TxManager` — assigns XIDs, maintains the set of active transactions, generates snapshots.
- `Snapshot` — immutable capture of transactional state; contains `xid`, `xmin`, `xmax`, and the list of active XIDs.
- `IsVisible(xmin, xmax)` — evaluated for each tuple during a scan.

Initial limitations: there is no persistent commit log (clog). In the initial phase, transactions are assumed committed if they are not in the active list. A persistent commit log on disk will be added in later phases.

### 3.6 — B+Tree Index

Responsibility: persistent ordered indexes on heap table columns, stored in slotted pages with type `PageBTreeInt` / `PageBTreeLeaf`.

Structure:
- Leaf nodes contain `(key, ItemID)` pairs where `ItemID` is the ctid (page, slot) of the tuple in the heap.
- Internal nodes contain `(key, childPage)` pairs plus a `rightPtr` pointer to the rightmost child.
- Leaf nodes are linked with `rightPtr` for efficient range scans.
- Node metadata (`level`, `numKeys`, `rightPtr`) is stored in the special area of the slotted page.

Operations:
- `Search(key)` — descent from root to leaf; binary search at each node.
- `Insert(key, itemID)` — insertion with leaf splits. The split produces a new right node and propagates the median key to the parent. If the root splits, a new root is created.
- `RangeScan(lo, hi)` — (future phase) traverses linked leaves.

### 3.7 — Catalog

Responsibility: storing metadata for all relations (tables and indexes) and their columns as system tables — exactly like PostgreSQL.

System tables:

**pg_class** — one row per relation:
- `oid` (int32): unique identifier
- `relname` (text): table/index name
- `relkind` (int32): 0 = table, 1 = index
- `relpages` (int32): number of pages
- `relheadpage` (int32): first page of the heap

**pg_attribute** — one row per column:
- `attrelid` (int32): OID of the relation
- `attname` (text): column name
- `attnum` (int32): ordinal number (1-based)
- `atttype` (int32): data type (mapped to DatumType)

Both tables live in normal heap pages pointed to from the superblock. They are queried with the same SeqScan + MVCC mechanism as any user table. This has an elegant consequence: the catalog is transactional — a `CREATE TABLE` that fails leaves no trace.

### 3.8 — Executor / Public API

Responsibility: exposing high-level operations that orchestrate all layers.

```go
db, _ := loladb.Open("mydata.lodb")
defer db.Close()

// DDL
db.CreateTable("users", []ColumnDef{
    {Name: "id",    Type: DatumInt32},
    {Name: "name",  Type: DatumText},
    {Name: "email", Type: DatumText},
})

// DML
db.Insert("users", []Datum{DInt(1), DText("Alice"), DText("alice@example.com")})

// Query
db.SeqScan("users", func(ctid ItemID, cols []Datum) bool {
    fmt.Printf("id=%d name=%s\n", cols[0].I32, cols[1].Text)
    return true // continue
})

// Soft delete (MVCC)
db.Delete("users", ItemID{Page: 3, Slot: 0})

// Durability
db.Checkpoint()
```

---

## 4. Implementation Plan by Phases

### Phase 1 — Foundations (storage + basic read/write)

**Goal:** be able to create a file, write tuples, and read them back.

Tasks:
1. Implement `PageIO` with tests verifying read/write roundtrip.
2. Implement the `Superblock` — serialization, deserialization, new database initialization.
3. Implement the `FreeList` bitmap — allocate and free pages.
4. Implement `SlottedPage` — tuple insertion and reading with line pointers.
5. Implement `TupleEncoding` — serialization of typed columns.
6. Integration test: create file → allocate page → insert tuples → read back → verify.

Deliverable: a `.lodb` file that persists tuples and survives restarts.

### Phase 2 — Buffer Pool + WAL

**Goal:** page cache with intelligent eviction and crash durability.

Tasks:
1. Implement `BufferPool` with clock-sweep, pin/unpin, dirty tracking.
2. Migrate the entire storage layer to go through the buffer pool (never direct I/O from upper layers).
3. Implement `WAL` — record append, sequential read.
4. Implement WAL-before-data protocol in write operations.
5. Implement `Checkpoint` — flush → fsync → update LSN → truncate WAL.
6. Implement `Recovery` — on open, detect crash and replay WAL.
7. Tests: simulate crash (kill process after WAL write, before flush) and verify recovery.

Deliverable: the database recovers its consistent state after a crash.

### Phase 3 — MVCC + Transactions

**Goal:** multiple concurrent readers/writers with snapshot isolation.

Tasks:
1. Implement `TxManager` — XID assignment, active transaction tracking.
2. Implement `Snapshot` with visibility rules.
3. Add `xmin`/`xmax` to all tuple writes.
4. Implement `Delete` as soft-delete (set xmax).
5. Implement `SeqScan` with visibility filter.
6. Concurrency tests: concurrent goroutines inserting and reading; verify isolation.
7. Implement basic commit log (in-memory first, persistent later).

Deliverable: consistent reads under concurrent writes.

### Phase 4 — Catalog

**Goal:** tables are registered in the catalog and discovered dynamically.

Tasks:
1. Create `pg_class` and `pg_attribute` as heap tables during initialization.
2. Implement `CreateTable` — insert into catalog + allocate heap page.
3. Implement `findRelation` — search pg_class by name.
4. Implement `getColumns` — search pg_attribute by OID.
5. Validate types in `Insert` against the catalog schema.
6. Tests: create multiple tables, verify that the catalog lists them correctly.

Deliverable: functional DDL with persistent metadata.

### Phase 5 — B+Tree Indexes

**Goal:** indexes that speed up key lookups.

Tasks:
1. Implement B+Tree nodes on slotted pages with special area.
2. Implement `Search` — descent + binary search.
3. Implement `Insert` with leaf splits.
4. Implement internal node splits (full propagation).
5. Implement `CreateIndex` — register in pg_class as `relkind=index`.
6. Integrate: have `Insert` on a table automatically update associated indexes.
7. Implement `IndexScan` — search by index → get ctid → fetch from heap.
8. Tests: insert thousands of records, verify tree correctness and speed vs SeqScan.

Deliverable: key-based queries with O(log N) complexity.

### Phase 6 — Overflow Pages + Multi-page Tables

**Goal:** tables that grow beyond a single page.

Tasks:
1. Implement heap page chains (`nextPage` field in the header).
2. When a page is full, allocate a new one and link it.
3. Update `relpages` in pg_class.
4. Have `SeqScan` traverse the entire chain.
5. Implement TOAST-like support for tuples that exceed page size (splitting large values).
6. Tests: insert thousands of records, verify chain integrity.

Deliverable: tables with no practical size limit.

### Phase 7 — VACUUM

**Goal:** reclaim space from dead tuples.

Tasks:
1. Identify tuples where `xmax` belongs to a committed transaction and is not visible to any active snapshot.
2. Mark line pointers as dead (offset=0, length=0).
3. Compact page: move live tuples toward the end, adjust line pointers, update `upper`.
4. Free completely empty pages to the free-list.
5. Tests: insert → delete → vacuum → verify reclaimed space.

Deliverable: the database does not grow indefinitely with deletes.

### Phase 8 — Improvements and Polish

**Goal:** robustness, performance, and additional features.

Tasks:
- UPDATE as delete + insert (new tuple version).
- Persistent commit log (clog) in dedicated pages.
- Composite key support in B+Tree.
- Range scans in B+Tree (traverse linked leaves).
- Basic statistics for the planner (tuple count, pages, value distribution).
- Row-level locks for concurrent writes (currently only snapshot isolation for reads).
- Minimal SQL parser (optional — can be used as a Go library without SQL).
- Comparative benchmarks against SQLite.

### Phase 9 — Query Planner, Optimizer, and Joins

**Goal:** separate SQL processing into a proper pipeline (parsing → rewriting → planning → optimization → execution), add JOIN support, and implement a PostgreSQL-style cost-based optimizer. Add EXPLAIN support.

The current SQL executor conflates parsing, planning, and execution into a single pass with no join support and no cost-based decisions. Phase 9 restructures this into a proper pipeline modeled after PostgreSQL's query processing architecture:

```
SQL Text
   │
   ▼
┌──────────┐
│  Parser  │  (postgresql-parser library)
└────┬─────┘
     │  Parse Tree (AST)
     ▼
┌──────────────┐
│  Analyzer    │  Resolve names, check types, expand *
└────┬─────────┘
     │  Query Tree (validated AST)
     ▼
┌──────────────┐
│  Rewriter    │  View expansion, predicate pushdown, subquery flattening
└────┬─────────┘
     │  Rewritten Query Tree
     ▼
┌──────────────┐
│  Planner /   │  Generate access paths, enumerate join orders,
│  Optimizer   │  estimate costs, select cheapest plan
└────┬─────────┘
     │  Physical Plan Tree
     ▼
┌──────────────┐
│  Executor    │  Volcano-style iterators: Open / Next / Close
└────┬─────────┘
     │  Result Rows
     ▼
   Client
```

---

#### 9.1 — Query Representation

**Logical Plan** — a tree of relational algebra operators that describes *what* the query does, independent of how. This is the output of the analyzer/rewriter.

| Node | Description |
|------|-------------|
| `LogicalScan(table)` | Base relation access |
| `LogicalFilter(predicate, child)` | Row selection (WHERE) |
| `LogicalProject(exprs, child)` | Column selection / computation |
| `LogicalJoin(type, condition, left, right)` | Join two relations (INNER, LEFT, RIGHT, CROSS) |
| `LogicalSort(keys, child)` | ORDER BY |
| `LogicalLimit(count, offset, child)` | LIMIT / OFFSET |
| `LogicalAggregate(groupBy, aggs, child)` | GROUP BY + aggregate functions (future) |
| `LogicalInsert(table, values)` | Insert rows |
| `LogicalDelete(table, predicate)` | Delete matching rows |
| `LogicalUpdate(table, assignments, predicate)` | Update matching rows |

**Predicate / Expression** types:
- `ExprColumn(table, column)` — column reference
- `ExprLiteral(datum)` — constant value
- `ExprBinOp(op, left, right)` — comparison (`=`, `<`, `>`, `<=`, `>=`, `<>`, `AND`, `OR`)
- `ExprNot(child)` — boolean negation
- `ExprIsNull(child)` / `ExprIsNotNull(child)`
- `ExprFunc(name, args)` — function call (future: COUNT, SUM, etc.)

---

#### 9.2 — Optimizer Architecture (PostgreSQL-style)

The optimizer follows PostgreSQL's approach of generating and costing *access paths* for each base relation, then finding the cheapest way to join them together. The key stages are:

##### Stage 1 — Preprocessing

- **Predicate pushdown:** move WHERE conditions as close to the scan as possible. For joins, split the WHERE into join conditions and per-table filters.
- **Constant folding:** evaluate constant expressions at plan time (e.g., `WHERE id = 1 + 1` → `WHERE id = 2`).
- **Subquery flattening:** convert simple `IN (SELECT ...)` to joins (future).

##### Stage 2 — Base Relation Access Paths

For each table referenced in the query, generate candidate *paths* — alternative ways to read the table's rows. Each path has an estimated startup cost, total cost, and row count.

| Path | When Used | Cost Model |
|------|-----------|------------|
| **SeqScan** | Always available | `startup=0`, `total = relpages × seq_page_cost + tupleCount × cpu_tuple_cost` |
| **IndexScan** | When an index matches a WHERE predicate (equality or range) | `startup=0`, `total = tree_height × random_page_cost + selectivity × tupleCount × (cpu_tuple_cost + cpu_index_tuple_cost)` |
| **IndexOnlyScan** | When the index covers all required columns (future) | Similar to IndexScan but avoids heap fetch |

**Cost constants** (configurable, with PostgreSQL-like defaults):

| Constant | Default | Meaning |
|----------|---------|---------|
| `seq_page_cost` | 1.0 | Cost of reading one page sequentially |
| `random_page_cost` | 4.0 | Cost of reading one random page (seek) |
| `cpu_tuple_cost` | 0.01 | Cost of processing one tuple |
| `cpu_index_tuple_cost` | 0.005 | Cost of processing one index entry |
| `cpu_operator_cost` | 0.0025 | Cost of evaluating one operator/function |

##### Stage 3 — Selectivity Estimation

Selectivity is the fraction of rows that satisfy a predicate (0.0 to 1.0). It drives row count estimates through the plan.

| Predicate | Selectivity Estimate |
|-----------|---------------------|
| `col = constant` | `1 / n_distinct` (from statistics), or `0.01` as default |
| `col > constant` | `(max - constant) / (max - min)`, or `0.33` as default |
| `col < constant` | `(constant - min) / (max - min)`, or `0.33` as default |
| `col BETWEEN lo AND hi` | `(hi - lo) / (max - min)`, or `0.25` as default |
| `col IS NULL` | `null_fraction` from statistics, or `0.01` |
| `AND(a, b)` | `sel(a) × sel(b)` (independence assumption) |
| `OR(a, b)` | `sel(a) + sel(b) - sel(a) × sel(b)` |
| `NOT(a)` | `1 - sel(a)` |

**Table statistics** (stored in a `pg_statistic`-like structure, or computed on-demand via ANALYZE):
- `relpages` — number of heap pages (already in pg_class)
- `reltuples` — estimated number of live tuples
- `n_distinct` — number of distinct values per column
- `null_fraction` — fraction of NULLs per column
- `min_value` / `max_value` — range bounds per column (for range selectivity)

##### Stage 4 — Join Planning

When a query involves multiple tables, the optimizer must decide:
1. **Join order** — which tables to join first
2. **Join method** — how to physically execute each join

**Join methods:**

| Method | Algorithm | Best When |
|--------|-----------|-----------|
| **Nested Loop Join** | For each row in outer, scan inner | Inner is small or has index on join key |
| **Hash Join** | Build hash table on inner, probe with outer | Equi-joins, inner fits in memory |
| **Merge Join** | Sort both sides, merge | Both sides sorted on join key, or pre-sorted via index (future) |

**Join order enumeration (PostgreSQL approach):**

For small numbers of relations (≤ `geqo_threshold`, default 12), use **dynamic programming**:

1. Start with the base relations as level-1 sets: `{A}`, `{B}`, `{C}`
2. For level k (2 to N), consider all ways to join a level-(k-1) set with a level-1 set, or two smaller sets that partition the level-k set
3. For each candidate join pair, try all applicable join methods (nested loop, hash join, merge join) and keep only the cheapest plan for each relation set
4. The cheapest plan for the full set `{A, B, C, ...}` is the final join plan

For example, with tables A, B, C:
```
Level 1: {A}, {B}, {C}               — base scans
Level 2: {A,B}, {A,C}, {B,C}         — try A⋈B, A⋈C, B⋈C
Level 3: {A,B,C}                      — try {A,B}⋈C, {A,C}⋈B, {B,C}⋈A
         → pick cheapest overall
```

**Join cost formulas:**

| Method | Cost |
|--------|------|
| Nested Loop | `outer_rows × (inner_startup + inner_total_per_rescan)` |
| Nested Loop (indexed inner) | `outer_rows × (index_scan_cost_per_key)` |
| Hash Join | `build_cost + outer_rows × cpu_operator_cost + inner_rows × cpu_tuple_cost` |
| Merge Join | `sort_cost(outer) + sort_cost(inner) + (outer_rows + inner_rows) × cpu_operator_cost` |

**Join condition extraction:**

The WHERE clause is decomposed into:
- **Join predicates** — reference columns from two different tables (e.g., `users.id = orders.user_id`)
- **Base predicates** — reference only one table (pushed down to the scan)

Only join methods applicable to the predicate type are considered:
- Equi-joins (`=`) → all three methods
- Non-equi-joins (`>`, `<`, etc.) → nested loop only
- Cross joins (no predicate) → nested loop or hash join

##### Stage 5 — Plan Selection

The optimizer returns the cheapest physical plan for the full query. In case of ties, prefer the plan with lower startup cost (better for LIMIT queries).

---

#### 9.3 — Physical Plan Nodes

The executor uses the Volcano (iterator) model: each node implements `Open()`, `Next() → Row`, `Close()`. Rows flow upward one at a time.

| Node | Description |
|------|-------------|
| `SeqScanNode(table)` | Full table scan via heap page chain |
| `IndexScanNode(index, key/range)` | B+Tree lookup → heap fetch |
| `FilterNode(predicate, child)` | Evaluate predicate per row |
| `ProjectNode(exprs, child)` | Compute output columns |
| `NestedLoopJoinNode(type, condition, outer, inner)` | For each outer row, scan inner |
| `HashJoinNode(condition, outer, inner)` | Build hash table on inner, probe with outer |
| `SortNode(keys, child)` | In-memory sort (future: external sort) |
| `LimitNode(count, offset, child)` | Stop after N rows |
| `InsertNode(table, values)` | Insert rows via engine |
| `DeleteNode(table, child)` | Soft-delete matched rows |
| `UpdateNode(table, assignments, child)` | Delete + insert for matched rows |

---

#### 9.4 — EXPLAIN

`EXPLAIN` returns the physical plan as a human-readable tree without executing it. `EXPLAIN ANALYZE` executes the plan and annotates each node with actual row counts and timing.

```sql
EXPLAIN SELECT * FROM users WHERE id = 42;
```
```
IndexScan on users using idx_users_id (id = 42)
  Cost: 4.01..4.02  Rows: 1  Width: 36
```

```sql
EXPLAIN SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true;
```
```
Hash Join (u.id = o.user_id)
  Cost: 15.50..42.30  Rows: 50  Width: 72
  -> SeqScan on orders o
       Cost: 0.00..12.00  Rows: 200  Width: 36
  -> Hash
       -> Filter (active = true)
            Cost: 0.00..8.50  Rows: 25  Width: 36
            -> SeqScan on users u
                 Cost: 0.00..5.00  Rows: 100  Width: 36
```

```sql
EXPLAIN ANALYZE SELECT * FROM users WHERE name = 'Alice';
```
```
Filter (name = 'Alice')
  Cost: 0.00..5.10  Rows: 1  Width: 36
  Actual: rows=1 time=0.05ms
  -> SeqScan on users
       Cost: 0.00..5.00  Rows: 100  Width: 36
       Actual: rows=100 time=0.03ms
Planning time: 0.01ms
Execution time: 0.05ms
```

---

#### 9.5 — SQL Syntax Extensions for Joins

The parser (postgresql-parser) already supports full PostgreSQL join syntax. Phase 9 adds execution support for:

```sql
-- INNER JOIN (explicit)
SELECT u.name, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id;

-- INNER JOIN (implicit / comma join)
SELECT u.name, o.total FROM users u, orders o WHERE u.id = o.user_id;

-- LEFT OUTER JOIN
SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id;

-- CROSS JOIN
SELECT * FROM colors CROSS JOIN sizes;

-- Multi-way join
SELECT u.name, o.id, p.name
FROM users u
JOIN orders o ON u.id = o.user_id
JOIN products p ON o.product_id = p.id
WHERE u.active = true;
```

---

#### 9.6 — Tasks

1. **Expressions:** define `Expr` interface and concrete types (Column, Literal, BinOp, Not, IsNull, Func) with evaluation against a row.
2. **Logical Plan:** define `LogicalNode` interface and all concrete types (Scan, Filter, Project, Join, Sort, Limit, Insert, Delete, Update).
3. **Analyzer:** convert parsed AST into a logical plan tree — resolve table/column names against the catalog, expand `SELECT *`, validate types.
4. **Predicate pushdown:** implement rewrite rule that pushes Filter nodes down past Project and into Join children and Scan nodes.
5. **Base relation paths:** for each LogicalScan, generate candidate paths (SeqScan, IndexScan) with cost estimates based on catalog statistics.
6. **Selectivity estimation:** implement selectivity formulas for equality, range, NULL, AND/OR/NOT predicates using table statistics.
7. **Join planning (dynamic programming):** implement the PostgreSQL-style bottom-up join enumeration. For each pair of relation sets, try all applicable join methods and keep the cheapest.
8. **Nested Loop Join executor:** implement `NestedLoopJoinNode` with inner restart capability. Support indexed inner scan (parameterized path).
9. **Hash Join executor:** implement `HashJoinNode` — build phase constructs an in-memory hash table on the inner relation's join key, probe phase streams outer rows through it.
10. **Volcano executor:** implement `Open/Next/Close` protocol for all physical nodes. `Next()` returns one row at a time.
11. **EXPLAIN:** format the physical plan tree as indented text with cost, estimated rows, and width.
12. **EXPLAIN ANALYZE:** wrap each node with timing/counting instrumentation, execute, and annotate the plan.
13. **ANALYZE command:** implement `ANALYZE <table>` to compute and store `reltuples`, `n_distinct`, `null_fraction`, `min_value`, `max_value` per column in a `pg_statistic` catalog table.
14. **Refactor SQL executor:** replace the current single-pass executor with the new pipeline (parse → analyze → rewrite → plan → execute).
15. **Tests:** join correctness (inner, left, cross, multi-way), join order selection, index vs. seqscan selection, EXPLAIN output, EXPLAIN ANALYZE timing, predicate pushdown verification, cost estimation accuracy.

**Deliverable:** queries with joins are supported and automatically optimized. The optimizer chooses join methods and join order based on cost. Users can inspect plans via EXPLAIN.

### Phase 10 — CLI Tool

**Goal:** a user-friendly command-line tool for creating, managing, and querying LolaDB databases.

```
loladb create <path>           — create a new empty database
loladb info <path>             — display database metadata
loladb cli <path>              — open an interactive SQL shell (REPL)
loladb tui <path>              — open a terminal UI for browsing data
loladb exec <path> "<sql>"     — execute a SQL statement non-interactively
```

#### `loladb create <path>`

Creates a new `.lodb` file with initialized superblock, WAL control page, and freelist. Prints confirmation with the file path and size.

#### `loladb info <path>`

Opens the database read-only and displays:
- File path and size
- Superblock: magic, version, NextOID, NextXID, CheckpointLSN, TotalPages
- Freelist: used pages, free pages, capacity
- Catalog: list of tables with OID, name, column count, relpages, tuple count
- Indexes: list of indexes with name, table, column, root page

#### `loladb cli <path>`

An interactive SQL REPL with:
- Readline-style line editing (using `golang.org/x/term` or `github.com/chzyer/readline`)
- Multi-line statement support (accumulate until `;`)
- Tabular result formatting with column headers
- Special commands: `\dt` (list tables), `\di` (list indexes), `\d <table>` (describe table), `\q` (quit)
- EXPLAIN support
- Command history

#### `loladb tui <path>`

A terminal UI built with [Bubble Tea](https://github.com/charmbracelet/bubbletea) featuring:
- Table browser: list of tables in a sidebar, select to view data
- Data view: paginated table view with scrolling
- SQL input: a text input area to run queries
- Schema view: columns and types for the selected table
- Stats view: tuple count, page count, dead tuples
- Index view: list of indexes, their columns and root pages
- Keyboard navigation: Tab to switch panels, Enter to execute, arrows to scroll

#### `loladb exec <path> "<sql>"`

Non-interactive mode: opens the database, executes the given SQL statement, prints results in tabular or JSON format, and exits. Useful for scripting.

#### Tasks

1. Set up `cmd/loladb/main.go` with subcommand routing (using `os.Args` or a lightweight CLI library like `cobra` or manual dispatch).
2. Implement `create` subcommand — call `engine.Open` + `catalog.New` + close.
3. Implement `info` subcommand — open read-only, print superblock, freelist stats, catalog contents.
4. Implement `cli` subcommand — REPL with readline, multi-line accumulation, tabular output, `\` meta-commands.
5. Implement `exec` subcommand — single-shot SQL execution with tabular output.
6. Implement `tui` subcommand — Bubble Tea application with table browser, data view, SQL input, and schema panels.
7. Add `--format` flag to `exec` and `cli` for output format (`table`, `csv`, `json`).
8. Tests: test CLI commands via subprocess execution, verify create/info/exec roundtrip.

**Deliverable:** a complete command-line tool for managing LolaDB databases interactively and programmatically.

---

## 5. Go Project Structure

```
loladb/
├── cmd/
│   └── loladb/                 # CLI tool (Phase 10)
│       ├── main.go             # Subcommand routing
│       ├── create.go           # loladb create
│       ├── info.go             # loladb info
│       ├── cli.go              # loladb cli (SQL REPL)
│       ├── exec.go             # loladb exec
│       └── tui.go              # loladb tui (Bubble Tea)
├── pkg/
│   ├── pageio/                 # Layer 0: Page I/O
│   ├── bufferpool/             # Layer 1: Buffer Pool (clock-sweep)
│   ├── wal/                    # Layer 2: WAL (write-ahead log)
│   ├── superblock/             # Superblock (page 0 metadata)
│   ├── freelist/               # Free-list bitmap (chained pages)
│   ├── slottedpage/            # Layer 3: Slotted Pages (heap)
│   ├── tuple/                  # Layer 4: Tuple encoding + Datum types
│   ├── toast/                  # TOAST (oversized value storage)
│   ├── mvcc/                   # Layer 5: MVCC + TxManager + Snapshots
│   ├── btree/                  # Layer 6: B+Tree Index
│   ├── engine/                 # Storage engine (WAL protocol, checkpoint, recovery)
│   ├── catalog/                # Layer 7: System catalog (pg_class, pg_attribute)
│   ├── planner/                # Layer 9: Query planning + Optimization (Phase 9)
│   │   ├── expr.go             # Expression types (Column, Literal, BinOp, ...)
│   │   ├── logical.go          # Logical plan nodes (Scan, Filter, Join, ...)
│   │   ├── analyzer.go         # AST → Logical plan (name resolution, type check)
│   │   ├── rewriter.go         # Predicate pushdown, constant folding
│   │   ├── stats.go            # Table/column statistics, selectivity estimation
│   │   ├── paths.go            # Access path generation (SeqScan, IndexScan)
│   │   ├── joinorder.go        # Join order enumeration (dynamic programming)
│   │   ├── cost.go             # Cost model (cost constants, formulas)
│   │   ├── physical.go         # Physical plan nodes (SeqScan, HashJoin, ...)
│   │   ├── optimizer.go        # Top-level: logical → physical plan
│   │   └── explain.go          # EXPLAIN / EXPLAIN ANALYZE formatting
│   ├── executor/               # Layer 8: Volcano-style plan executor (Phase 9)
│   │   ├── executor.go         # Open/Next/Close iterator protocol
│   │   ├── scan.go             # SeqScanNode, IndexScanNode
│   │   ├── join.go             # NestedLoopJoinNode, HashJoinNode
│   │   ├── filter.go           # FilterNode
│   │   ├── project.go          # ProjectNode
│   │   └── dml.go              # InsertNode, DeleteNode, UpdateNode
│   └── sql/                    # Layer 8: SQL parser + executor
│       └── executor.go         # Parse → Plan → Optimize → Execute
└── test/
    └── integration_test.go     # End-to-end Phase 1 tests
```

---

## 6. Design Principles

**WAL-first.** No modification to a data page occurs without first recording the change in the WAL. This is the durability guarantee.

**The catalog is just another table.** Just like in PostgreSQL, pg_class and pg_attribute are normal heap tables, queried with the same engine. This simplifies the architecture and makes DDL transactional.

**MVCC without read locks.** Readers never block writers and vice versa. Each transaction sees a consistent snapshot from the moment it started.

**Strict layers.** Each layer only invokes lower layers. The executor does not touch Page I/O directly — it always goes through the buffer pool.

**Pages as the atomic unit.** Every data structure (heap tuples, B+Tree nodes, catalog metadata) is stored in 4096-byte pages. This simplifies the cache, the WAL, and recovery.

**OIDs as universal identifiers.** Every object (table, index, type) has a unique OID assigned monotonically, stored in the superblock.
