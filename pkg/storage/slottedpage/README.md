# slottedpage

Slotted page format used by heap tables and indexes. Each page has a fixed-size header, a slot directory that grows forward, and tuple data that grows backward from the end of the page.

The header tracks page type, flags, slot count, and free space pointers. Supported page types include heap, B-tree internal/leaf, hash, GIN, GiST, SP-GiST, BRIN, and freelist pages.

Provides insert, read, update, delete, and compaction operations on individual slots.
