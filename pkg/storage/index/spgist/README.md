# spgist

Space-Partitioned GiST index access method, mirroring PostgreSQL's SP-GiST (`src/backend/access/spgist`).

Unlike GiST (which uses a balanced tree), SP-GiST is an unbalanced partitioned search tree. For LolaDB's int64 keys, it implements a radix tree that partitions the key space by bit prefixes. A single page can hold a mix of inner and leaf tuples.

Supports equality and range scans.
