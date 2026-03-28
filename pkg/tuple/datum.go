package tuple

import (
	"encoding/binary"
	"fmt"
	"math"
)

// DatumType identifies the type of a column value.
type DatumType uint8

const (
	TypeNull    DatumType = 0
	TypeInt32   DatumType = 1
	TypeInt64   DatumType = 2
	TypeText    DatumType = 3
	TypeBool    DatumType = 4
	TypeFloat64      DatumType = 5
	TypeToastPointer DatumType = 6
)

// Datum is a single typed column value.
type Datum struct {
	Type DatumType
	I32  int32
	I64  int64
	Text string
	Bool bool
	F64  float64
}

// Convenience constructors.

func DNull() Datum              { return Datum{Type: TypeNull} }
func DInt32(v int32) Datum      { return Datum{Type: TypeInt32, I32: v} }
func DInt64(v int64) Datum      { return Datum{Type: TypeInt64, I64: v} }
func DText(v string) Datum      { return Datum{Type: TypeText, Text: v} }
func DBool(v bool) Datum        { return Datum{Type: TypeBool, Bool: v} }
func DFloat64(v float64) Datum  { return Datum{Type: TypeFloat64, F64: v} }

// EncodeDatum appends the serialized form of a Datum to buf and
// returns the extended buffer. Format: [type:1B | data:variable].
func EncodeDatum(buf []byte, d Datum) []byte {
	buf = append(buf, byte(d.Type))
	switch d.Type {
	case TypeNull:
		// no data
	case TypeInt32:
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, uint32(d.I32))
		buf = append(buf, b...)
	case TypeInt64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(d.I64))
		buf = append(buf, b...)
	case TypeText:
		b := make([]byte, 2)
		binary.LittleEndian.PutUint16(b, uint16(len(d.Text)))
		buf = append(buf, b...)
		buf = append(buf, []byte(d.Text)...)
	case TypeBool:
		if d.Bool {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	case TypeFloat64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, math.Float64bits(d.F64))
		buf = append(buf, b...)
	case TypeToastPointer:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(d.I64))
		buf = append(buf, b...)
	}
	return buf
}

// DecodeDatum reads one Datum from buf starting at offset, returning
// the datum and the number of bytes consumed.
func DecodeDatum(buf []byte, offset int) (Datum, int, error) {
	if offset >= len(buf) {
		return Datum{}, 0, fmt.Errorf("tuple: datum offset %d beyond buffer length %d", offset, len(buf))
	}

	typ := DatumType(buf[offset])
	pos := offset + 1

	switch typ {
	case TypeNull:
		return DNull(), pos - offset, nil
	case TypeInt32:
		if pos+4 > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: int32 truncated at offset %d", offset)
		}
		v := int32(binary.LittleEndian.Uint32(buf[pos:]))
		return DInt32(v), pos + 4 - offset, nil
	case TypeInt64:
		if pos+8 > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: int64 truncated at offset %d", offset)
		}
		v := int64(binary.LittleEndian.Uint64(buf[pos:]))
		return DInt64(v), pos + 8 - offset, nil
	case TypeText:
		if pos+2 > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: text length truncated at offset %d", offset)
		}
		length := int(binary.LittleEndian.Uint16(buf[pos:]))
		pos += 2
		if pos+length > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: text data truncated at offset %d", offset)
		}
		s := string(buf[pos : pos+length])
		return DText(s), pos + length - offset, nil
	case TypeBool:
		if pos+1 > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: bool truncated at offset %d", offset)
		}
		return DBool(buf[pos] != 0), pos + 1 - offset, nil
	case TypeFloat64:
		if pos+8 > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: float64 truncated at offset %d", offset)
		}
		v := math.Float64frombits(binary.LittleEndian.Uint64(buf[pos:]))
		return DFloat64(v), pos + 8 - offset, nil
	case TypeToastPointer:
		if pos+8 > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: toast pointer truncated at offset %d", offset)
		}
		v := int64(binary.LittleEndian.Uint64(buf[pos:]))
		return Datum{Type: TypeToastPointer, I64: v}, pos + 8 - offset, nil
	default:
		return Datum{}, 0, fmt.Errorf("tuple: unknown datum type %d at offset %d", typ, offset)
	}
}
