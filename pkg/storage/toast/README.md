# toast

The Oversized-Attribute Storage Technique. Handles datum values that exceed the inline size limit (~1 KB).

When a text or bytea value is too large to fit in a heap tuple, it is split into fixed-size chunks stored on dedicated toast pages. The original tuple stores a compact toast pointer (page number + total length) in place of the full value. On read, the chunks are reassembled transparently.
