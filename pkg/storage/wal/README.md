# wal

Append-only write-ahead log. Every page modification is recorded as a WAL entry before the data page is written, ensuring crash recovery can replay lost changes.

WAL records are buffered in memory and flushed to disk on commit or when the buffer fills. The storage engine triggers automatic checkpoints after a configurable number of WAL records to bound recovery time and WAL file growth.
