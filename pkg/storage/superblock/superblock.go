package superblock

import (
	"encoding/binary"
	"fmt"

	"github.com/gololadb/loladb/pkg/storage/pageio"
)

const (
	// SuperblockPage is the page number reserved for the superblock.
	SuperblockPage = 0

	// Magic identifies a valid LolaDB file.
	Magic = 0x4C4F4C41 // "LOLA" in little-endian

	// Version is the current superblock format version.
	Version = 1
)

// Superblock holds global metadata for the database. It is always
// stored in page 0 of the data file.
//
// Layout (all little-endian):
//
//	Offset  Size  Field
//	  0      4    Magic
//	  4      4    Version
//	  8      4    NextOID        — next object identifier to assign
//	 12      4    NextXID        — next transaction ID to assign
//	 16      4    CheckpointLSN  — LSN of the last successful checkpoint
//	 20      4    PgClassPage    — first heap page of pg_class
//	 24      4    PgAttrPage     — first heap page of pg_attribute
//	 28      4    FreeListPage   — page number of the free-list bitmap
//	 32      4    TotalPages     — total pages allocated in the data file
//	 36      4    PgRewritePage  — first heap page of pg_rewrite (rule storage)
//	 40      4    PgPolicyPage   — first heap page of pg_policy (RLS policies)
//	 44      4    PgAuthIDPage   — first heap page of pg_authid (roles)
//	 48      4    PgAuthMembersPage — first heap page of pg_auth_members (role membership)
//	 52      4    PgACLPage      — first heap page of pg_acl (object privileges)
//	 56      4    PgProcPage     — first heap page of pg_proc (functions)
//	 60      4    PgTriggerPage  — first heap page of pg_trigger (triggers)
//
// The rest of the page (bytes 64..4095) is reserved and zero-filled.
type Superblock struct {
	Magic             uint32
	Version           uint32
	NextOID           uint32
	NextXID           uint32
	CheckpointLSN     uint32
	PgClassPage       uint32
	PgAttrPage        uint32
	FreeListPage      uint32
	TotalPages        uint32
	PgRewritePage     uint32
	PgPolicyPage      uint32
	PgAuthIDPage      uint32
	PgAuthMembersPage uint32
	PgACLPage         uint32
	PgProcPage        uint32
	PgTriggerPage     uint32
	PgPartitionPage   uint32
	SearchPath        string // comma-separated schema names (persisted, max 254 bytes)
}

const serializedSize = 16 * 4 // 64 bytes

// New returns a Superblock initialised with default values for a
// fresh database.
func New() *Superblock {
	return &Superblock{
		Magic:         Magic,
		Version:       Version,
		NextOID:       1,
		NextXID:       1,
		CheckpointLSN: 0,
		PgClassPage:   0, // set during catalog bootstrap
		PgAttrPage:    0,
		FreeListPage:  2, // page 2 by convention
		TotalPages:    3, // pages 0 (superblock), 1 (WAL control), 2 (freelist)
	}
}

// Serialize writes the superblock into a full page-sized buffer.
func (sb *Superblock) Serialize() []byte {
	buf := make([]byte, pageio.PageSize)
	binary.LittleEndian.PutUint32(buf[0:4], sb.Magic)
	binary.LittleEndian.PutUint32(buf[4:8], sb.Version)
	binary.LittleEndian.PutUint32(buf[8:12], sb.NextOID)
	binary.LittleEndian.PutUint32(buf[12:16], sb.NextXID)
	binary.LittleEndian.PutUint32(buf[16:20], sb.CheckpointLSN)
	binary.LittleEndian.PutUint32(buf[20:24], sb.PgClassPage)
	binary.LittleEndian.PutUint32(buf[24:28], sb.PgAttrPage)
	binary.LittleEndian.PutUint32(buf[28:32], sb.FreeListPage)
	binary.LittleEndian.PutUint32(buf[32:36], sb.TotalPages)
	binary.LittleEndian.PutUint32(buf[36:40], sb.PgRewritePage)
	binary.LittleEndian.PutUint32(buf[40:44], sb.PgPolicyPage)
	binary.LittleEndian.PutUint32(buf[44:48], sb.PgAuthIDPage)
	binary.LittleEndian.PutUint32(buf[48:52], sb.PgAuthMembersPage)
	binary.LittleEndian.PutUint32(buf[52:56], sb.PgACLPage)
	binary.LittleEndian.PutUint32(buf[56:60], sb.PgProcPage)
	binary.LittleEndian.PutUint32(buf[60:64], sb.PgTriggerPage)
	// PgPartitionPage at offset 320 (beyond SearchPath area).
	binary.LittleEndian.PutUint32(buf[320:324], sb.PgPartitionPage)
	// SearchPath: length-prefixed string at offset 64 (max 254 bytes).
	if sb.SearchPath != "" {
		sp := sb.SearchPath
		if len(sp) > 254 {
			sp = sp[:254]
		}
		binary.LittleEndian.PutUint16(buf[64:66], uint16(len(sp)))
		copy(buf[66:66+len(sp)], sp)
	}
	return buf
}

// Deserialize reads a superblock from a page-sized buffer.
func Deserialize(buf []byte) (*Superblock, error) {
	if len(buf) < serializedSize {
		return nil, fmt.Errorf("superblock: buffer too small (%d bytes)", len(buf))
	}

	sb := &Superblock{
		Magic:         binary.LittleEndian.Uint32(buf[0:4]),
		Version:       binary.LittleEndian.Uint32(buf[4:8]),
		NextOID:       binary.LittleEndian.Uint32(buf[8:12]),
		NextXID:       binary.LittleEndian.Uint32(buf[12:16]),
		CheckpointLSN: binary.LittleEndian.Uint32(buf[16:20]),
		PgClassPage:   binary.LittleEndian.Uint32(buf[20:24]),
		PgAttrPage:    binary.LittleEndian.Uint32(buf[24:28]),
		FreeListPage:  binary.LittleEndian.Uint32(buf[28:32]),
		TotalPages:    binary.LittleEndian.Uint32(buf[32:36]),
		PgRewritePage:     binary.LittleEndian.Uint32(buf[36:40]),
		PgPolicyPage:      binary.LittleEndian.Uint32(buf[40:44]),
		PgAuthIDPage:      binary.LittleEndian.Uint32(buf[44:48]),
		PgAuthMembersPage: binary.LittleEndian.Uint32(buf[48:52]),
		PgACLPage:         binary.LittleEndian.Uint32(buf[52:56]),
		PgProcPage:        binary.LittleEndian.Uint32(buf[56:60]),
		PgTriggerPage:     binary.LittleEndian.Uint32(buf[60:64]),
	}

	// PgPartitionPage at offset 320.
	if len(buf) >= 324 {
		sb.PgPartitionPage = binary.LittleEndian.Uint32(buf[320:324])
	}
	// SearchPath: length-prefixed string at offset 64.
	if len(buf) >= 66 {
		spLen := binary.LittleEndian.Uint16(buf[64:66])
		if spLen > 0 && int(66+spLen) <= len(buf) {
			sb.SearchPath = string(buf[66 : 66+spLen])
		}
	}

	if sb.Magic != Magic {
		return nil, fmt.Errorf("superblock: bad magic %08X (expected %08X)", sb.Magic, Magic)
	}
	if sb.Version != Version {
		return nil, fmt.Errorf("superblock: unsupported version %d (expected %d)", sb.Version, Version)
	}
	return sb, nil
}

// Load reads the superblock from page 0 of the given PageIO.
func Load(io *pageio.PageIO) (*Superblock, error) {
	buf := make([]byte, pageio.PageSize)
	if err := io.ReadPage(SuperblockPage, buf); err != nil {
		return nil, fmt.Errorf("superblock: read page 0: %w", err)
	}
	return Deserialize(buf)
}

// Save writes the superblock to page 0 of the given PageIO.
func (sb *Superblock) Save(io *pageio.PageIO) error {
	return io.WritePage(SuperblockPage, sb.Serialize())
}

// AllocOID returns the next OID and increments the counter.
func (sb *Superblock) AllocOID() uint32 {
	oid := sb.NextOID
	sb.NextOID++
	return oid
}

// AllocXID returns the next transaction ID and increments the counter.
func (sb *Superblock) AllocXID() uint32 {
	xid := sb.NextXID
	sb.NextXID++
	return xid
}
