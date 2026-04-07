# gist

Generalized Search Tree index access method, mirroring PostgreSQL's GiST (`src/backend/access/gist`).

GiST is a balanced tree where each internal node stores a bounding predicate that covers all entries in its subtree. For LolaDB's int64 keys, the bounding predicate is a [min, max] range. Consistent checks range overlap, Union merges ranges, and Penalty measures range expansion cost for insertion.

Supports range overlap and containment queries.
