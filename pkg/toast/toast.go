package toast

import (
	"encoding/binary"
	"fmt"

	"github.com/gololadb/loladb/pkg/pageio"
	"github.com/gololadb/loladb/pkg/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
)

// MaxInlineSize is the maximum byte length of a Text datum before it
// is TOASTed. Values larger than this are stored out-of-line in toast
// pages and replaced with a toast pointer in the main tuple.
//
// We pick a conservative threshold: about 1/4 of page usable space,
// ensuring at least a few tuples fit per page even with large values.
const MaxInlineSize = 1000

// ToastPointerType is a special DatumType tag for a toast pointer.
// It is stored as: [type=6 (1B)] [toastPage(4B)] [totalLen(4B)]
const ToastPointerType tuple.DatumType = 6

// ChunkSize is the maximum data bytes per toast chunk page.
// We use the page minus the slotted page header (24B) and one line
// pointer (4B), with some margin.
const ChunkSize = pageio.PageSize - 100

// PageAllocator is the interface for allocating and accessing pages.
type PageAllocator interface {
	AllocPage() (uint32, error)
	FetchPage(pageNum uint32) ([]byte, error)
	ReleasePage(pageNum uint32)
	MarkDirty(pageNum uint32)
}

// Store stores a large byte slice across one or more toast pages,
// chaining them via nextPage. Returns the page number of the first
// toast page.
func Store(alloc PageAllocator, data []byte) (uint32, error) {
	if len(data) == 0 {
		return 0, fmt.Errorf("toast: cannot store empty data")
	}

	var firstPage, prevPage uint32
	offset := 0

	for offset < len(data) {
		end := offset + ChunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[offset:end]

		pageNum, err := alloc.AllocPage()
		if err != nil {
			return 0, fmt.Errorf("toast: alloc page: %w", err)
		}

		buf, err := alloc.FetchPage(pageNum)
		if err != nil {
			return 0, err
		}

		sp := slottedpage.Init(slottedpage.PageTypeHeap, pageNum, 0)
		sp.InsertTuple(chunk)
		copy(buf, sp.Bytes())
		alloc.MarkDirty(pageNum)
		alloc.ReleasePage(pageNum)

		if firstPage == 0 {
			firstPage = pageNum
		}

		// Link from previous page.
		if prevPage != 0 {
			prevBuf, err := alloc.FetchPage(prevPage)
			if err != nil {
				return 0, err
			}
			prevSp, _ := slottedpage.FromBytes(prevBuf)
			prevSp.SetNextPage(pageNum)
			copy(prevBuf, prevSp.Bytes())
			alloc.MarkDirty(prevPage)
			alloc.ReleasePage(prevPage)
		}

		prevPage = pageNum
		offset = end
	}

	return firstPage, nil
}

// Load reads a toasted value from the chain starting at toastPage.
func Load(alloc PageAllocator, toastPage uint32, totalLen int) ([]byte, error) {
	result := make([]byte, 0, totalLen)
	cur := toastPage

	for cur != 0 {
		buf, err := alloc.FetchPage(cur)
		if err != nil {
			return nil, fmt.Errorf("toast: fetch page %d: %w", cur, err)
		}
		sp, err := slottedpage.FromBytes(buf)
		if err != nil {
			alloc.ReleasePage(cur)
			return nil, err
		}
		chunk, err := sp.GetTuple(0)
		if err != nil {
			alloc.ReleasePage(cur)
			return nil, fmt.Errorf("toast: read chunk page %d: %w", cur, err)
		}
		result = append(result, chunk...)
		next := sp.NextPage()
		alloc.ReleasePage(cur)
		cur = next
	}

	return result, nil
}

// MakePointer creates a toast pointer datum from a toast page number
// and total length.
func MakePointer(toastPage uint32, totalLen int) tuple.Datum {
	// We store the pointer as an Int64 datum with a special encoding:
	// high 32 bits = toastPage, low 32 bits = totalLen
	val := int64(toastPage)<<32 | int64(uint32(totalLen))
	return tuple.Datum{Type: ToastPointerType, I64: val}
}

// DecodePointer extracts toastPage and totalLen from a toast pointer.
func DecodePointer(d tuple.Datum) (toastPage uint32, totalLen int, ok bool) {
	if d.Type != ToastPointerType {
		return 0, 0, false
	}
	toastPage = uint32(d.I64 >> 32)
	totalLen = int(uint32(d.I64))
	return toastPage, totalLen, true
}

// NeedsToast returns true if a text value exceeds MaxInlineSize.
func NeedsToast(d tuple.Datum) bool {
	return d.Type == tuple.TypeText && len(d.Text) > MaxInlineSize
}

// ToastValues processes a slice of datums, storing any oversized text
// values out-of-line and replacing them with toast pointers. Returns
// the modified datums.
func ToastValues(alloc PageAllocator, values []tuple.Datum) ([]tuple.Datum, error) {
	result := make([]tuple.Datum, len(values))
	copy(result, values)

	for i, d := range result {
		if !NeedsToast(d) {
			continue
		}
		data := []byte(d.Text)
		toastPage, err := Store(alloc, data)
		if err != nil {
			return nil, fmt.Errorf("toast column %d: %w", i, err)
		}
		result[i] = MakePointer(toastPage, len(data))
	}
	return result, nil
}

// DetoastValues resolves any toast pointers in a tuple's columns,
// replacing them with the full text values.
func DetoastValues(alloc PageAllocator, columns []tuple.Datum) ([]tuple.Datum, error) {
	result := make([]tuple.Datum, len(columns))
	copy(result, columns)

	for i, d := range result {
		toastPage, totalLen, ok := DecodePointer(d)
		if !ok {
			continue
		}
		data, err := Load(alloc, toastPage, totalLen)
		if err != nil {
			return nil, fmt.Errorf("detoast column %d: %w", i, err)
		}
		result[i] = tuple.DText(string(data))
	}
	return result, nil
}

func init() {
	// Register the toast pointer type with the tuple encoder/decoder.
	// Toast pointers are stored as Int64 values with type tag 6.
	// The tuple package handles unknown types by checking the tag,
	// but since we reuse I64 storage, encode/decode works via Int64 path.
	// We need to register custom encode/decode — but the tuple package
	// uses a fixed type switch, so we'll handle this at the catalog layer.
	_ = binary.LittleEndian // suppress unused import
}
