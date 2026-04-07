# mvcc

Multi-version concurrency control and snapshot isolation, mirroring PostgreSQL's MVCC model.

Each transaction operates on a consistent snapshot of the database. Tuple visibility is determined by comparing the tuple's `xmin` (creating transaction) and `xmax` (deleting transaction) against the snapshot's active transaction set.

## Key types

- **`Snapshot`** — Immutable capture of transactional state at a point in time. Provides `IsVisible(xmin, xmax)` to determine whether a tuple version is visible to this transaction.
- **`TxState`** — Transaction commit/abort status tracked in the commit log.
