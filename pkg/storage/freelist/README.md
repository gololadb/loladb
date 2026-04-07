# freelist

Bitmap-based free page allocator. Tracks which data pages are in use and which are available for allocation.

Each freelist page is a bitmap covering up to 32,736 data pages. Pages are chained together for databases that exceed a single bitmap page. Allocation scans for the first free bit; deallocation clears the bit.
