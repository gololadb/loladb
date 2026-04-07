# btree

B+Tree index access method, mirroring PostgreSQL's nbtree (`src/backend/access/nbtree`).

Internal nodes store separator keys and child page pointers. Leaf nodes store keys with heap tuple locations (page number + slot number) and are linked for efficient range scans. Pages use the slotted page format with a 12-byte special area for tree navigation (left sibling, right sibling, level).

Supports equality and range scans (`=`, `<`, `<=`, `>`, `>=`).
