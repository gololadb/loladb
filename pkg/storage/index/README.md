# index

Defines the generalized index access method interface (`IndexAM`). Each concrete index type lives in its own subpackage and implements this interface.

The interface provides:
- `Insert(key, pageNum, slotNum)` — add an entry.
- `Delete(key, pageNum, slotNum)` — remove an entry.
- `ScanStart / ScanNext / ScanStop` — volcano-style iteration over matching entries.

## Subpackages

| Package | Index type | Best for |
|---------|-----------|----------|
| `btree` | B+Tree | Range queries, ordered scans, equality |
| `hash` | Linear hashing | Equality-only lookups |
| `gin` | Generalized Inverted Index | Full-text search, array containment |
| `gist` | Generalized Search Tree | Range overlap, nearest-neighbor |
| `spgist` | Space-Partitioned GiST | Radix trees, partitioned search spaces |
| `brin` | Block Range Index | Large, naturally ordered tables |
