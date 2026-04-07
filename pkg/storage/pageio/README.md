# pageio

Raw page-level read/write access to database files. All access is serialized through a mutex.

Pages are fixed-size blocks (4 KB by default). `ReadPage` and `WritePage` operate on page numbers. Reads beyond the current end of file return zeroed buffers. `Sync` flushes buffered writes to stable storage.
