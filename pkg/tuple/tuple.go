package tuple

import (
	"encoding/binary"
	"fmt"
)

// Header layout (16 bytes).
const HeaderSize = 16

const (
	offXmin    = 0
	offXmax    = 4
	offFlags   = 8
	offNatts   = 10
	offDatalen = 12
)

// Tuple is the serialized representation of a row with MVCC headers
// and typed column values.
//
//	Header (16B): xmin(4) | xmax(4) | flags(2) | natts(2) | datalen(4)
//	Payload:      [type:1B | data:variable] × natts
type Tuple struct {
	Xmin    uint32
	Xmax    uint32
	Flags   uint16
	Columns []Datum
}

// Encode serializes a Tuple into a byte slice suitable for storage
// in a slotted page.
func Encode(t *Tuple) []byte {
	// Encode payload first so we know its length.
	var payload []byte
	for _, d := range t.Columns {
		payload = EncodeDatum(payload, d)
	}

	buf := make([]byte, HeaderSize+len(payload))
	binary.LittleEndian.PutUint32(buf[offXmin:], t.Xmin)
	binary.LittleEndian.PutUint32(buf[offXmax:], t.Xmax)
	binary.LittleEndian.PutUint16(buf[offFlags:], t.Flags)
	binary.LittleEndian.PutUint16(buf[offNatts:], uint16(len(t.Columns)))
	binary.LittleEndian.PutUint32(buf[offDatalen:], uint32(len(payload)))
	copy(buf[HeaderSize:], payload)
	return buf
}

// Decode deserializes a byte slice back into a Tuple.
func Decode(buf []byte) (*Tuple, error) {
	if len(buf) < HeaderSize {
		return nil, fmt.Errorf("tuple: buffer too small (%d < %d)", len(buf), HeaderSize)
	}

	t := &Tuple{
		Xmin:  binary.LittleEndian.Uint32(buf[offXmin:]),
		Xmax:  binary.LittleEndian.Uint32(buf[offXmax:]),
		Flags: binary.LittleEndian.Uint16(buf[offFlags:]),
	}
	natts := int(binary.LittleEndian.Uint16(buf[offNatts:]))
	datalen := int(binary.LittleEndian.Uint32(buf[offDatalen:]))

	if len(buf) < HeaderSize+datalen {
		return nil, fmt.Errorf("tuple: buffer truncated (need %d, have %d)", HeaderSize+datalen, len(buf))
	}

	payload := buf[HeaderSize : HeaderSize+datalen]
	t.Columns = make([]Datum, 0, natts)
	offset := 0
	for i := 0; i < natts; i++ {
		d, n, err := DecodeDatum(payload, offset)
		if err != nil {
			return nil, fmt.Errorf("tuple: column %d: %w", i, err)
		}
		t.Columns = append(t.Columns, d)
		offset += n
	}

	if offset != datalen {
		return nil, fmt.Errorf("tuple: payload length mismatch (decoded %d, expected %d)", offset, datalen)
	}

	return t, nil
}
