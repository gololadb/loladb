package toast

import (
	"testing"

	"github.com/gololadb/loladb/pkg/storage/pageio"
	"github.com/gololadb/loladb/pkg/tuple"
)

type memAlloc struct {
	pages    map[uint32][]byte
	nextPage uint32
}

func newMemAlloc() *memAlloc {
	return &memAlloc{pages: make(map[uint32][]byte), nextPage: 10}
}

func (m *memAlloc) AllocPage() (uint32, error) {
	pn := m.nextPage
	m.nextPage++
	m.pages[pn] = make([]byte, pageio.PageSize)
	return pn, nil
}

func (m *memAlloc) FetchPage(pn uint32) ([]byte, error) {
	buf, ok := m.pages[pn]
	if !ok {
		m.pages[pn] = make([]byte, pageio.PageSize)
		buf = m.pages[pn]
	}
	return buf, nil
}

func (m *memAlloc) ReleasePage(pn uint32) {}
func (m *memAlloc) MarkDirty(pn uint32)   {}

func TestStoreAndLoad_Small(t *testing.T) {
	alloc := newMemAlloc()
	data := []byte("hello toast")

	pg, err := Store(alloc, data)
	if err != nil {
		t.Fatal(err)
	}

	got, err := Load(alloc, pg, len(data))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("expected %q, got %q", data, got)
	}
}

func TestStoreAndLoad_Large(t *testing.T) {
	alloc := newMemAlloc()
	// Create data larger than one chunk.
	data := make([]byte, ChunkSize*3+500)
	for i := range data {
		data[i] = byte(i % 251)
	}

	pg, err := Store(alloc, data)
	if err != nil {
		t.Fatal(err)
	}

	got, err := Load(alloc, pg, len(data))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(data) {
		t.Fatalf("length: expected %d, got %d", len(data), len(got))
	}
	for i := range data {
		if got[i] != data[i] {
			t.Fatalf("mismatch at byte %d", i)
		}
	}
}

func TestStoreAndLoad_ExactChunkSize(t *testing.T) {
	alloc := newMemAlloc()
	data := make([]byte, ChunkSize)
	for i := range data {
		data[i] = byte(i % 199)
	}

	pg, err := Store(alloc, data)
	if err != nil {
		t.Fatal(err)
	}

	got, err := Load(alloc, pg, len(data))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(data) {
		t.Fatalf("length: expected %d, got %d", len(data), len(got))
	}
}

func TestMakeAndDecodePointer(t *testing.T) {
	d := MakePointer(42, 5000)
	pg, totalLen, ok := DecodePointer(d)
	if !ok {
		t.Fatal("should be a toast pointer")
	}
	if pg != 42 {
		t.Fatalf("expected page 42, got %d", pg)
	}
	if totalLen != 5000 {
		t.Fatalf("expected len 5000, got %d", totalLen)
	}
}

func TestDecodePointer_NotToast(t *testing.T) {
	d := tuple.DInt32(42)
	_, _, ok := DecodePointer(d)
	if ok {
		t.Fatal("should not be a toast pointer")
	}
}

func TestNeedsToast(t *testing.T) {
	small := tuple.DText("hello")
	if NeedsToast(small) {
		t.Fatal("small text should not need toast")
	}

	big := tuple.DText(string(make([]byte, MaxInlineSize+1)))
	if !NeedsToast(big) {
		t.Fatal("big text should need toast")
	}

	num := tuple.DInt32(42)
	if NeedsToast(num) {
		t.Fatal("int should not need toast")
	}
}

func TestToastAndDetoastValues(t *testing.T) {
	alloc := newMemAlloc()

	bigBytes := make([]byte, 2000)
	for i := range bigBytes {
		bigBytes[i] = byte(i % 251)
	}
	bigText := string(bigBytes)

	values := []tuple.Datum{
		tuple.DInt32(1),
		tuple.DText(bigText),
		tuple.DText("small"),
	}

	toasted, err := ToastValues(alloc, values)
	if err != nil {
		t.Fatal(err)
	}

	// Column 0 (int) should be unchanged.
	if toasted[0].Type != tuple.TypeInt32 || toasted[0].I32 != 1 {
		t.Fatal("int should be unchanged")
	}
	// Column 1 (big text) should be a toast pointer.
	if toasted[1].Type != ToastPointerType {
		t.Fatal("big text should be toasted")
	}
	// Column 2 (small text) should be unchanged.
	if toasted[2].Type != tuple.TypeText || toasted[2].Text != "small" {
		t.Fatal("small text should be unchanged")
	}

	// Detoast.
	detoasted, err := DetoastValues(alloc, toasted)
	if err != nil {
		t.Fatal(err)
	}
	if detoasted[1].Type != tuple.TypeText {
		t.Fatal("should be detoasted back to text")
	}
	if len(detoasted[1].Text) != 2000 {
		t.Fatalf("expected 2000 chars, got %d", len(detoasted[1].Text))
	}
}

func TestStore_Empty(t *testing.T) {
	alloc := newMemAlloc()
	_, err := Store(alloc, []byte{})
	if err == nil {
		t.Fatal("expected error for empty data")
	}
}
