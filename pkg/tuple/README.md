# tuple

Row serialization format and datum type system.

## Key types

- **`Tuple`** — Serialized representation of a row with MVCC headers (`xmin`, `xmax`, flags). Tuples are stored on slotted pages and carry a fixed 16-byte header followed by column data.
- **`DatumType`** — Identifies the type of a column value (int64, float64, text, bool, bytea, null, timestamp, numeric, UUID, JSON/JSONB, array, interval, date, time, etc.).
- **`Datum`** — A single typed column value with serialization and deserialization methods.
- **`Header`** — The 16-byte MVCC header prepended to every tuple.

The package also provides datum encoding/decoding, type comparison, and null handling.
