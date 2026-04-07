# superblock

Global database metadata stored on page 0. The superblock holds the database-wide state needed to bootstrap the storage engine on startup: magic number, version, page count, freelist head pointer, and WAL position.

All fields are little-endian. The superblock is updated during checkpoints.
