# gin

Generalized Inverted Index access method, mirroring PostgreSQL's GIN (`src/backend/access/gin`).

GIN is an inverted index: each distinct key maps to a posting list of heap tuple locations. Designed for indexing composite values where each row contains multiple keys (arrays, full-text documents, JSONB).

Supports exact key match scans and containment queries.
