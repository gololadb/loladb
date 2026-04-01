package slottedpage

import (
	"encoding/binary"
	"fmt"

	"github.com/jespino/loladb/pkg/pageio"
)

// Page type constants.
const (
	PageTypeHeap      = 1
	PageTypeBTreeInt  = 2
	PageTypeBTreeLeaf = 3
	PageTypeHash      = 4
	PageTypeBRIN      = 5
	PageTypeGIN       = 6
	PageTypeGiST      = 7
	PageTypeSPGiST    = 8
)

// Page flag constants.
const (
	FlagNone = 0
)

// Header layout offsets and sizes.
const (
	headerSize   = 24
	linePointerSize = 4 // offset (2B) + length (2B)

	offType      = 0
	offFlags     = 1
	offLower     = 2
	offUpper     = 4
	offSpecial   = 6
	offPageNum   = 8
	offLSN       = 12
	offNumSlots  = 16
	offFreeSpace = 18
	offNextPage  = 20
)

// ItemID identifies a tuple within a page by its slot index.
type ItemID struct {
	Page uint32
	Slot uint16
}

// linePointer is an in-memory representation of a line pointer entry.
type linePointer struct {
	offset uint16 // byte offset of the tuple data within the page
	length uint16 // length of the tuple data (0 means dead/empty slot)
}

// Page is an in-memory slotted page backed by a PageSize byte array.
// It provides methods to insert, read, and delete variable-length
// tuples following the PostgreSQL slotted-page layout.
type Page struct {
	data [pageio.PageSize]byte
}

// Init initialises a fresh slotted page with the given type, page
// number, and special area size. The special area is reserved at the
// end of the page (before tuple data grows toward it).
func Init(pageType uint8, pageNum uint32, specialSize uint16) *Page {
	p := &Page{}
	p.setType(pageType)
	p.setFlags(FlagNone)
	p.setLower(headerSize)
	special := uint16(pageio.PageSize) - specialSize
	p.setSpecial(special)
	p.setUpper(special)
	p.setPageNum(pageNum)
	p.setLSN(0)
	p.setNumSlots(0)
	p.updateFreeSpace()
	p.setNextPage(0)
	return p
}

// FromBytes wraps an existing page-sized buffer into a Page.
func FromBytes(buf []byte) (*Page, error) {
	if len(buf) != pageio.PageSize {
		return nil, fmt.Errorf("slottedpage: buffer must be %d bytes, got %d", pageio.PageSize, len(buf))
	}
	p := &Page{}
	copy(p.data[:], buf)
	return p, nil
}

// Bytes returns a copy of the raw page data.
func (p *Page) Bytes() []byte {
	out := make([]byte, pageio.PageSize)
	copy(out, p.data[:])
	return out
}

// DataRef returns a direct reference to the underlying page array.
// Callers may read/write it but must respect the slotted-page layout.
func (p *Page) DataRef() *[pageio.PageSize]byte {
	return &p.data
}

// --- Tuple operations ---

// InsertTuple adds a variable-length tuple to the page. Returns the
// slot index assigned to it. Returns an error if there is not enough
// free space for the tuple plus its line pointer.
func (p *Page) InsertTuple(tuple []byte) (uint16, error) {
	needed := uint16(len(tuple)) + linePointerSize
	if p.freeSpace() < needed {
		return 0, fmt.Errorf("slottedpage: not enough space (need %d, have %d)", needed, p.freeSpace())
	}

	// Write tuple data just below upper.
	upper := p.getUpper() - uint16(len(tuple))
	copy(p.data[upper:], tuple)
	p.setUpper(upper)

	// Append a line pointer at lower.
	slot := p.getNumSlots()
	lpOff := p.getLower()
	binary.LittleEndian.PutUint16(p.data[lpOff:], upper)
	binary.LittleEndian.PutUint16(p.data[lpOff+2:], uint16(len(tuple)))
	p.setLower(lpOff + linePointerSize)

	p.setNumSlots(slot + 1)
	p.updateFreeSpace()
	return slot, nil
}

// GetTuple returns a copy of the tuple data at the given slot index.
// Returns an error if the slot is out of range or dead.
func (p *Page) GetTuple(slot uint16) ([]byte, error) {
	lp, err := p.getLinePointer(slot)
	if err != nil {
		return nil, err
	}
	if lp.length == 0 {
		return nil, fmt.Errorf("slottedpage: slot %d is dead", slot)
	}
	out := make([]byte, lp.length)
	copy(out, p.data[lp.offset:lp.offset+lp.length])
	return out, nil
}

// DeleteTuple marks a slot as dead by zeroing its line pointer
// offset and length. The space is not immediately reclaimed; use
// Compact to defragment.
func (p *Page) DeleteTuple(slot uint16) error {
	if slot >= p.getNumSlots() {
		return fmt.Errorf("slottedpage: slot %d out of range (have %d)", slot, p.getNumSlots())
	}
	lpOff := headerSize + uint16(slot)*linePointerSize
	binary.LittleEndian.PutUint16(p.data[lpOff:], 0)
	binary.LittleEndian.PutUint16(p.data[lpOff+2:], 0)
	return nil
}

// Compact defragments the page by repacking all live tuples toward
// the end of the page (just before the special area), reclaiming
// space left by dead slots. Line pointers are updated in place.
// Dead slots remain as zero-length placeholders to preserve slot
// numbering.
func (p *Page) Compact() {
	special := p.getSpecial()
	writePos := special
	numSlots := p.getNumSlots()

	// Walk slots in reverse order so tuples are repacked from the
	// end of the page.
	type liveTuple struct {
		slot uint16
		data []byte
	}
	var live []liveTuple
	for i := uint16(0); i < numSlots; i++ {
		lp, _ := p.getLinePointer(i)
		if lp.length == 0 {
			continue
		}
		td := make([]byte, lp.length)
		copy(td, p.data[lp.offset:lp.offset+lp.length])
		live = append(live, liveTuple{slot: i, data: td})
	}

	// Clear the tuple area.
	lower := p.getLower()
	for i := lower; i < special; i++ {
		p.data[i] = 0
	}

	// Rewrite tuples.
	for _, lt := range live {
		writePos -= uint16(len(lt.data))
		copy(p.data[writePos:], lt.data)
		// Update line pointer.
		lpOff := headerSize + uint16(lt.slot)*linePointerSize
		binary.LittleEndian.PutUint16(p.data[lpOff:], writePos)
		binary.LittleEndian.PutUint16(p.data[lpOff+2:], uint16(len(lt.data)))
	}

	p.setUpper(writePos)
	p.updateFreeSpace()
}

// --- Slot inspection ---

// NumSlots returns the number of slots (including dead ones).
func (p *Page) NumSlots() uint16 {
	return p.getNumSlots()
}

// SlotIsAlive returns true if the slot has a non-zero length line
// pointer.
func (p *Page) SlotIsAlive(slot uint16) bool {
	lp, err := p.getLinePointer(slot)
	if err != nil {
		return false
	}
	return lp.length > 0
}

// FreeSpace returns the usable free space in the page.
func (p *Page) FreeSpace() uint16 {
	return p.freeSpace()
}

// --- Header accessors ---

func (p *Page) Type() uint8        { return p.data[offType] }
func (p *Page) Flags() uint8       { return p.data[offFlags] }
func (p *Page) PageNum() uint32    { return binary.LittleEndian.Uint32(p.data[offPageNum:]) }
func (p *Page) LSN() uint32        { return binary.LittleEndian.Uint32(p.data[offLSN:]) }
func (p *Page) NextPage() uint32   { return binary.LittleEndian.Uint32(p.data[offNextPage:]) }

func (p *Page) SetLSN(lsn uint32)        { p.setLSN(lsn) }
func (p *Page) SetNextPage(next uint32)   { p.setNextPage(next) }
func (p *Page) SetFlags(flags uint8)      { p.setFlags(flags) }

// Special returns a slice into the special area at the end of the
// page. The caller can read/write B+Tree node metadata here.
func (p *Page) Special() []byte {
	s := p.getSpecial()
	return p.data[s:]
}

// --- Internal header helpers ---

func (p *Page) setType(v uint8)     { p.data[offType] = v }
func (p *Page) setFlags(v uint8)    { p.data[offFlags] = v }
func (p *Page) getLower() uint16    { return binary.LittleEndian.Uint16(p.data[offLower:]) }
func (p *Page) setLower(v uint16)   { binary.LittleEndian.PutUint16(p.data[offLower:], v) }
func (p *Page) getUpper() uint16    { return binary.LittleEndian.Uint16(p.data[offUpper:]) }
func (p *Page) setUpper(v uint16)   { binary.LittleEndian.PutUint16(p.data[offUpper:], v) }
func (p *Page) getSpecial() uint16  { return binary.LittleEndian.Uint16(p.data[offSpecial:]) }
func (p *Page) setSpecial(v uint16) { binary.LittleEndian.PutUint16(p.data[offSpecial:], v) }
func (p *Page) setPageNum(v uint32) { binary.LittleEndian.PutUint32(p.data[offPageNum:], v) }
func (p *Page) setLSN(v uint32)     { binary.LittleEndian.PutUint32(p.data[offLSN:], v) }
func (p *Page) getNumSlots() uint16 { return binary.LittleEndian.Uint16(p.data[offNumSlots:]) }
func (p *Page) setNumSlots(v uint16) {
	binary.LittleEndian.PutUint16(p.data[offNumSlots:], v)
}
func (p *Page) setNextPage(v uint32)  { binary.LittleEndian.PutUint32(p.data[offNextPage:], v) }
func (p *Page) updateFreeSpace() {
	fs := p.freeSpace()
	binary.LittleEndian.PutUint16(p.data[offFreeSpace:], fs)
}

func (p *Page) freeSpace() uint16 {
	upper := p.getUpper()
	lower := p.getLower()
	if upper <= lower {
		return 0
	}
	return upper - lower
}

func (p *Page) getLinePointer(slot uint16) (linePointer, error) {
	if slot >= p.getNumSlots() {
		return linePointer{}, fmt.Errorf("slottedpage: slot %d out of range (have %d)", slot, p.getNumSlots())
	}
	off := headerSize + uint16(slot)*linePointerSize
	return linePointer{
		offset: binary.LittleEndian.Uint16(p.data[off:]),
		length: binary.LittleEndian.Uint16(p.data[off+2:]),
	}, nil
}
