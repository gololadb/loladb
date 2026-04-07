# bufferpool

In-memory page cache with clock-sweep eviction, mirroring PostgreSQL's shared buffer pool.

Pages are loaded from disk into fixed-size frames. The pool uses a clock-sweep algorithm to choose eviction victims when all frames are occupied. Dirty pages are written back to disk on eviction or during checkpoints.

Default pool size is 4096 frames (16 MB with 4 KB pages).
