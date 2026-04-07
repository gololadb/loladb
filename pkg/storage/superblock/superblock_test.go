package superblock

import (
	"path/filepath"
	"testing"

	"github.com/gololadb/loladb/pkg/storage/pageio"
)

func TestNewDefaults(t *testing.T) {
	sb := New()
	if sb.Magic != Magic {
		t.Fatalf("bad magic: %08X", sb.Magic)
	}
	if sb.Version != Version {
		t.Fatalf("bad version: %d", sb.Version)
	}
	if sb.NextOID != 1 {
		t.Fatalf("expected NextOID=1, got %d", sb.NextOID)
	}
	if sb.NextXID != 1 {
		t.Fatalf("expected NextXID=1, got %d", sb.NextXID)
	}
	if sb.FreeListPage != 2 {
		t.Fatalf("expected FreeListPage=2, got %d", sb.FreeListPage)
	}
	if sb.TotalPages != 3 {
		t.Fatalf("expected TotalPages=3, got %d", sb.TotalPages)
	}
	if sb.CheckpointLSN != 0 {
		t.Fatalf("expected CheckpointLSN=0, got %d", sb.CheckpointLSN)
	}
}

func TestSerializeDeserializeRoundtrip(t *testing.T) {
	sb := New()
	sb.NextOID = 42
	sb.NextXID = 100
	sb.CheckpointLSN = 55
	sb.PgClassPage = 3
	sb.PgAttrPage = 4
	sb.TotalPages = 10

	buf := sb.Serialize()
	if len(buf) != pageio.PageSize {
		t.Fatalf("expected %d bytes, got %d", pageio.PageSize, len(buf))
	}

	sb2, err := Deserialize(buf)
	if err != nil {
		t.Fatal(err)
	}

	if *sb != *sb2 {
		t.Fatalf("roundtrip mismatch:\n  got  %+v\n  want %+v", sb2, sb)
	}
}

func TestDeserialize_BadMagic(t *testing.T) {
	buf := make([]byte, pageio.PageSize)
	// Zero magic
	_, err := Deserialize(buf)
	if err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestDeserialize_BadVersion(t *testing.T) {
	sb := New()
	sb.Version = 99
	buf := sb.Serialize()
	// Fix the magic back but keep bad version
	_, err := Deserialize(buf)
	if err == nil {
		t.Fatal("expected error for bad version")
	}
}

func TestDeserialize_BufferTooSmall(t *testing.T) {
	_, err := Deserialize(make([]byte, 10))
	if err == nil {
		t.Fatal("expected error for small buffer")
	}
}

func TestSerialize_ReservedBytesAreZero(t *testing.T) {
	sb := New()
	buf := sb.Serialize()
	for i := serializedSize; i < pageio.PageSize; i++ {
		if buf[i] != 0 {
			t.Fatalf("reserved byte %d is %d, expected 0", i, buf[i])
		}
	}
}

func TestSaveAndLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.lodb")
	pio, err := pageio.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer pio.Close()

	sb := New()
	sb.NextOID = 77
	sb.PgClassPage = 5
	sb.PgAttrPage = 6

	if err := sb.Save(pio); err != nil {
		t.Fatal(err)
	}
	if err := pio.Sync(); err != nil {
		t.Fatal(err)
	}

	sb2, err := Load(pio)
	if err != nil {
		t.Fatal(err)
	}
	if *sb != *sb2 {
		t.Fatalf("save/load mismatch:\n  got  %+v\n  want %+v", sb2, sb)
	}
}

func TestLoadPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.lodb")

	pio, err := pageio.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	sb := New()
	sb.NextOID = 200
	sb.NextXID = 300
	sb.CheckpointLSN = 150
	sb.TotalPages = 50
	if err := sb.Save(pio); err != nil {
		t.Fatal(err)
	}
	pio.Sync()
	pio.Close()

	pio2, err := pageio.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer pio2.Close()

	sb2, err := Load(pio2)
	if err != nil {
		t.Fatal(err)
	}
	if sb2.NextOID != 200 || sb2.NextXID != 300 || sb2.CheckpointLSN != 150 || sb2.TotalPages != 50 {
		t.Fatalf("unexpected values after reopen: %+v", sb2)
	}
}

func TestLoadFromEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.lodb")
	pio, err := pageio.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer pio.Close()

	_, err = Load(pio)
	if err == nil {
		t.Fatal("expected error loading superblock from empty file")
	}
}

func TestAllocOID(t *testing.T) {
	sb := New()
	if sb.NextOID != 1 {
		t.Fatal("unexpected initial NextOID")
	}

	oid1 := sb.AllocOID()
	oid2 := sb.AllocOID()
	oid3 := sb.AllocOID()

	if oid1 != 1 || oid2 != 2 || oid3 != 3 {
		t.Fatalf("AllocOID sequence: got %d, %d, %d", oid1, oid2, oid3)
	}
	if sb.NextOID != 4 {
		t.Fatalf("NextOID after 3 allocs: got %d, want 4", sb.NextOID)
	}
}

func TestAllocXID(t *testing.T) {
	sb := New()

	xid1 := sb.AllocXID()
	xid2 := sb.AllocXID()

	if xid1 != 1 || xid2 != 2 {
		t.Fatalf("AllocXID sequence: got %d, %d", xid1, xid2)
	}
	if sb.NextXID != 3 {
		t.Fatalf("NextXID after 2 allocs: got %d, want 3", sb.NextXID)
	}
}

func TestOverwriteSuperblock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.lodb")
	pio, err := pageio.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer pio.Close()

	sb := New()
	sb.NextOID = 10
	if err := sb.Save(pio); err != nil {
		t.Fatal(err)
	}

	// Overwrite with new values
	sb.NextOID = 20
	sb.CheckpointLSN = 99
	if err := sb.Save(pio); err != nil {
		t.Fatal(err)
	}

	sb2, err := Load(pio)
	if err != nil {
		t.Fatal(err)
	}
	if sb2.NextOID != 20 {
		t.Fatalf("expected NextOID=20, got %d", sb2.NextOID)
	}
	if sb2.CheckpointLSN != 99 {
		t.Fatalf("expected CheckpointLSN=99, got %d", sb2.CheckpointLSN)
	}
}
