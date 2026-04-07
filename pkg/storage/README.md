# storage

Page-oriented storage engine. The `Engine` struct ties together the buffer pool, WAL, superblock, and freelist into a single layer that enforces the WAL-before-data protocol for all page modifications.

Provides heap table operations (insert, scan, update, delete) on top of slotted pages, with MVCC tuple headers for snapshot isolation. Automatic checkpointing flushes dirty pages to disk periodically.

## Subpackages

| Package | Role |
|---------|------|
| `bufferpool` | In-memory page cache with clock-sweep eviction |
| `freelist` | Bitmap-based free page allocator |
| `index` | Generalized index access method interface |
| `index/btree` | B+Tree index |
| `index/hash` | Linear-hashing index |
| `index/gin` | Generalized Inverted Index |
| `index/gist` | Generalized Search Tree index |
| `index/spgist` | Space-Partitioned GiST index |
| `index/brin` | Block Range Index |
| `pageio` | Raw page-level file I/O |
| `slottedpage` | Slotted page format (header, slot directory, tuple data) |
| `superblock` | Global database metadata (page 0) |
| `toast` | Oversized datum storage (The Oversized-Attribute Storage Technique) |
| `wal` | Write-ahead log |
