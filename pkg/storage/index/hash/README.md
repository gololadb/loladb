# hash

Linear-hashing index access method, mirroring PostgreSQL's hash index (`src/backend/access/hash`).

Entries are 14 bytes: hash (4 bytes) + page number (4 bytes) + slot number (2 bytes) + key (8 bytes, reserved). Buckets split incrementally as the load factor grows.

Supports equality scans only.
