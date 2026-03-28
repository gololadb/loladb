package freelist

import (
	"encoding/binary"
	"fmt"

	"github.com/jespino/loladb/pkg/pageio"
)

const (
	// nextPtrSize is the space reserved at the end of each bitmap page
	// for the next-page pointer (0 = no next page).
	nextPtrSize = 4

	// bitmapBytes is the usable bitmap area per page.
	bitmapBytes = pageio.PageSize - nextPtrSize

	// BitsPerPage is the number of data-page bits tracked by one
	// freelist page.
	BitsPerPage = bitmapBytes * 8 // 32,736
)

// bitmapPage is one page in the freelist chain.
type bitmapPage struct {
	pageNum  uint32
	bits     [bitmapBytes]byte
	nextPage uint32 // 0 means end of chain
}

func (bp *bitmapPage) serialize() []byte {
	buf := make([]byte, pageio.PageSize)
	copy(buf[:bitmapBytes], bp.bits[:])
	binary.LittleEndian.PutUint32(buf[bitmapBytes:], bp.nextPage)
	return buf
}

func deserializeBitmapPage(pageNum uint32, buf []byte) *bitmapPage {
	bp := &bitmapPage{pageNum: pageNum}
	copy(bp.bits[:], buf[:bitmapBytes])
	bp.nextPage = binary.LittleEndian.Uint32(buf[bitmapBytes:])
	return bp
}

// FreeList is a chained bitmap page allocator. Each page in the chain
// tracks BitsPerPage data pages via a bitmap, with the last 4 bytes
// holding a pointer to the next bitmap page (0 = end of chain).
type FreeList struct {
	pages []*bitmapPage
}

// New creates a FreeList with a single empty bitmap page at pageNum.
func New(pageNum uint32) *FreeList {
	return &FreeList{
		pages: []*bitmapPage{{pageNum: pageNum}},
	}
}

// Load reads the entire freelist chain starting at pageNum from disk.
func Load(pageNum uint32, io *pageio.PageIO) (*FreeList, error) {
	fl := &FreeList{}
	cur := pageNum
	for cur != 0 {
		buf := make([]byte, pageio.PageSize)
		if err := io.ReadPage(cur, buf); err != nil {
			return nil, fmt.Errorf("freelist: load page %d: %w", cur, err)
		}
		bp := deserializeBitmapPage(cur, buf)
		fl.pages = append(fl.pages, bp)
		cur = bp.nextPage
	}
	if len(fl.pages) == 0 {
		return nil, fmt.Errorf("freelist: no pages loaded from page %d", pageNum)
	}
	return fl, nil
}

// Save writes all bitmap pages in the chain to disk.
func (fl *FreeList) Save(io *pageio.PageIO) error {
	for _, bp := range fl.pages {
		if err := io.WritePage(bp.pageNum, bp.serialize()); err != nil {
			return fmt.Errorf("freelist: save page %d: %w", bp.pageNum, err)
		}
	}
	return nil
}

// Alloc finds the first free bit across all bitmap pages, marks it as
// used, and returns the global page number.
// Returns an error if every bit in every bitmap page is set.
func (fl *FreeList) Alloc() (uint32, error) {
	for idx, bp := range fl.pages {
		for i := 0; i < bitmapBytes; i++ {
			if bp.bits[i] == 0xFF {
				continue
			}
			for bit := 0; bit < 8; bit++ {
				if bp.bits[i]&(1<<bit) == 0 {
					bp.bits[i] |= 1 << bit
					return uint32(idx)*BitsPerPage + uint32(i*8+bit), nil
				}
			}
		}
	}
	return 0, fmt.Errorf("freelist: no free pages available (use Grow to add capacity)")
}

// Free marks a global page number as free. Returns an error if the
// page is beyond the tracked range.
func (fl *FreeList) Free(page uint32) error {
	idx, local := fl.locate(page)
	if idx >= len(fl.pages) {
		return fmt.Errorf("freelist: page %d out of range", page)
	}
	byteIdx := local / 8
	bitIdx := local % 8
	fl.pages[idx].bits[byteIdx] &^= 1 << bitIdx
	return nil
}

// MarkUsed marks a global page number as in use.
func (fl *FreeList) MarkUsed(page uint32) error {
	idx, local := fl.locate(page)
	if idx >= len(fl.pages) {
		return fmt.Errorf("freelist: page %d out of range", page)
	}
	byteIdx := local / 8
	bitIdx := local % 8
	fl.pages[idx].bits[byteIdx] |= 1 << bitIdx
	return nil
}

// IsUsed returns true if the global page number is marked as in use.
func (fl *FreeList) IsUsed(page uint32) bool {
	idx, local := fl.locate(page)
	if idx >= len(fl.pages) {
		return false
	}
	byteIdx := local / 8
	bitIdx := local % 8
	return fl.pages[idx].bits[byteIdx]&(1<<bitIdx) != 0
}

// Grow adds a new bitmap page to the chain, expanding capacity by
// BitsPerPage. The caller provides the page number for the new
// bitmap page (which should itself be marked as used afterward).
func (fl *FreeList) Grow(newPageNum uint32) {
	last := fl.pages[len(fl.pages)-1]
	last.nextPage = newPageNum
	fl.pages = append(fl.pages, &bitmapPage{pageNum: newPageNum})
}

// Capacity returns the total number of pages trackable by the current
// chain length.
func (fl *FreeList) Capacity() uint32 {
	return uint32(len(fl.pages)) * BitsPerPage
}

// FreeCount returns the total number of free bits across all pages.
func (fl *FreeList) FreeCount() uint32 {
	var count uint32
	for _, bp := range fl.pages {
		for i := 0; i < bitmapBytes; i++ {
			b := bp.bits[i]
			for bit := 0; bit < 8; bit++ {
				if b&(1<<bit) == 0 {
					count++
				}
			}
		}
	}
	return count
}

// UsedCount returns the total number of used bits across all pages.
func (fl *FreeList) UsedCount() uint32 {
	return fl.Capacity() - fl.FreeCount()
}

// PageCount returns the number of bitmap pages in the chain.
func (fl *FreeList) PageCount() int {
	return len(fl.pages)
}

// locate converts a global page number to a (chain index, local bit)
// pair.
func (fl *FreeList) locate(page uint32) (int, uint32) {
	idx := int(page / BitsPerPage)
	local := page % BitsPerPage
	return idx, local
}
