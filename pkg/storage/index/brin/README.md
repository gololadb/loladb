# brin

Block Range Index access method, mirroring PostgreSQL's BRIN (`src/backend/access/brin`).

BRIN stores min/max summaries for contiguous ranges of heap pages. Each range summary covers a configurable number of heap pages (`PagesPerRange`). The index is very compact — one summary entry per range rather than one entry per row.

During a scan, BRIN checks each range summary against the scan key and skips ranges that cannot contain matching rows. Most effective on large, naturally ordered tables where correlated values cluster on the same pages.
