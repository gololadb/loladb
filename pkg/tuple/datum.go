package tuple

import (
	"encoding/binary"
	"fmt"
	"math"
)

// DatumType identifies the type of a column value.
type DatumType uint8

const (
	TypeNull         DatumType = 0
	TypeInt32        DatumType = 1
	TypeInt64        DatumType = 2
	TypeText         DatumType = 3
	TypeBool         DatumType = 4
	TypeFloat64      DatumType = 5
	TypeToastPointer DatumType = 6
	TypeDate         DatumType = 7  // days since Unix epoch (stored as I64)
	TypeTimestamp    DatumType = 8  // microseconds since Unix epoch (stored as I64)
	TypeNumeric      DatumType = 9  // arbitrary-precision decimal (stored as Text)
	TypeJSON         DatumType = 10 // JSON/JSONB (stored as Text)
	TypeUUID         DatumType = 11 // UUID (stored as Text, canonical format)
	TypeInterval     DatumType = 12 // Interval (I64=microseconds, I32=months)
	TypeBytea        DatumType = 13 // Binary data (stored as Text, hex-encoded)
	TypeArray        DatumType = 14 // Array (stored as Text, PG array literal format)
	TypeMoney        DatumType = 15 // Money (stored as I64, value in cents)
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
func DDate(days int64) Datum    { return Datum{Type: TypeDate, I64: days} }
func DTimestamp(us int64) Datum { return Datum{Type: TypeTimestamp, I64: us} }
func DNumeric(v string) Datum   { return Datum{Type: TypeNumeric, Text: v} }
func DJSON(v string) Datum      { return Datum{Type: TypeJSON, Text: v} }
func DUUID(v string) Datum      { return Datum{Type: TypeUUID, Text: v} }
func DInterval(months int32, microseconds int64) Datum {
	return Datum{Type: TypeInterval, I32: months, I64: microseconds}
}
func DBytea(v string) Datum  { return Datum{Type: TypeBytea, Text: v} }
func DArray(v string) Datum  { return Datum{Type: TypeArray, Text: v} }
func DMoney(cents int64) Datum { return Datum{Type: TypeMoney, I64: cents} }

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
	case TypeDate, TypeTimestamp:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(d.I64))
		buf = append(buf, b...)
	case TypeNumeric, TypeJSON, TypeUUID:
		b := make([]byte, 2)
		binary.LittleEndian.PutUint16(b, uint16(len(d.Text)))
		buf = append(buf, b...)
		buf = append(buf, []byte(d.Text)...)
	case TypeInterval:
		b := make([]byte, 12)
		binary.LittleEndian.PutUint32(b[0:4], uint32(d.I32))   // months
		binary.LittleEndian.PutUint64(b[4:12], uint64(d.I64))  // microseconds
		buf = append(buf, b...)
	case TypeBytea, TypeArray:
		b := make([]byte, 2)
		binary.LittleEndian.PutUint16(b, uint16(len(d.Text)))
		buf = append(buf, b...)
		buf = append(buf, []byte(d.Text)...)
	case TypeMoney:
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
	case TypeDate:
		if pos+8 > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: date truncated at offset %d", offset)
		}
		v := int64(binary.LittleEndian.Uint64(buf[pos:]))
		return DDate(v), pos + 8 - offset, nil
	case TypeTimestamp:
		if pos+8 > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: timestamp truncated at offset %d", offset)
		}
		v := int64(binary.LittleEndian.Uint64(buf[pos:]))
		return DTimestamp(v), pos + 8 - offset, nil
	case TypeNumeric:
		if pos+2 > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: numeric length truncated at offset %d", offset)
		}
		length := int(binary.LittleEndian.Uint16(buf[pos:]))
		pos += 2
		if pos+length > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: numeric data truncated at offset %d", offset)
		}
		s := string(buf[pos : pos+length])
		return DNumeric(s), pos + length - offset, nil
	case TypeJSON:
		if pos+2 > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: json length truncated at offset %d", offset)
		}
		length := int(binary.LittleEndian.Uint16(buf[pos:]))
		pos += 2
		if pos+length > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: json data truncated at offset %d", offset)
		}
		s := string(buf[pos : pos+length])
		return DJSON(s), pos + length - offset, nil
	case TypeUUID:
		if pos+2 > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: uuid length truncated at offset %d", offset)
		}
		length := int(binary.LittleEndian.Uint16(buf[pos:]))
		pos += 2
		if pos+length > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: uuid data truncated at offset %d", offset)
		}
		s := string(buf[pos : pos+length])
		return DUUID(s), pos + length - offset, nil
	case TypeInterval:
		if pos+12 > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: interval truncated at offset %d", offset)
		}
		months := int32(binary.LittleEndian.Uint32(buf[pos : pos+4]))
		us := int64(binary.LittleEndian.Uint64(buf[pos+4 : pos+12]))
		return DInterval(months, us), pos + 12 - offset, nil
	case TypeBytea:
		if pos+2 > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: bytea length truncated at offset %d", offset)
		}
		length := int(binary.LittleEndian.Uint16(buf[pos:]))
		pos += 2
		if pos+length > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: bytea data truncated at offset %d", offset)
		}
		s := string(buf[pos : pos+length])
		return DBytea(s), pos + length - offset, nil
	case TypeArray:
		if pos+2 > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: array length truncated at offset %d", offset)
		}
		length := int(binary.LittleEndian.Uint16(buf[pos:]))
		pos += 2
		if pos+length > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: array data truncated at offset %d", offset)
		}
		s := string(buf[pos : pos+length])
		return DArray(s), pos + length - offset, nil
	case TypeMoney:
		if pos+8 > len(buf) {
			return Datum{}, 0, fmt.Errorf("tuple: money truncated at offset %d", offset)
		}
		v := int64(binary.LittleEndian.Uint64(buf[pos:]))
		return DMoney(v), pos + 8 - offset, nil
	default:
		return Datum{}, 0, fmt.Errorf("tuple: unknown datum type %d at offset %d", typ, offset)
	}
}
