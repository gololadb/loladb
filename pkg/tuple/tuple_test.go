package tuple

import (
	"math"
	"testing"
)

// --- Datum tests ---

func TestDatumNull(t *testing.T) {
	d := DNull()
	buf := EncodeDatum(nil, d)
	got, n, err := DecodeDatum(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 byte, consumed %d", n)
	}
	if got.Type != TypeNull {
		t.Fatalf("expected Null, got %d", got.Type)
	}
}

func TestDatumInt32(t *testing.T) {
	for _, v := range []int32{0, 1, -1, math.MaxInt32, math.MinInt32} {
		d := DInt32(v)
		buf := EncodeDatum(nil, d)
		got, n, err := DecodeDatum(buf, 0)
		if err != nil {
			t.Fatal(err)
		}
		if n != 5 {
			t.Fatalf("expected 5 bytes, consumed %d", n)
		}
		if got.I32 != v {
			t.Fatalf("expected %d, got %d", v, got.I32)
		}
	}
}

func TestDatumInt64(t *testing.T) {
	for _, v := range []int64{0, -42, math.MaxInt64, math.MinInt64} {
		d := DInt64(v)
		buf := EncodeDatum(nil, d)
		got, n, err := DecodeDatum(buf, 0)
		if err != nil {
			t.Fatal(err)
		}
		if n != 9 {
			t.Fatalf("expected 9 bytes, consumed %d", n)
		}
		if got.I64 != v {
			t.Fatalf("expected %d, got %d", v, got.I64)
		}
	}
}

func TestDatumText(t *testing.T) {
	for _, s := range []string{"", "hello", "café ☕", string(make([]byte, 1000))} {
		d := DText(s)
		buf := EncodeDatum(nil, d)
		got, n, err := DecodeDatum(buf, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got.Text != s {
			t.Fatalf("expected %q, got %q", s, got.Text)
		}
		// 1 (type) + 2 (len) + len(s)
		if n != 3+len(s) {
			t.Fatalf("expected %d bytes, consumed %d", 3+len(s), n)
		}
	}
}

func TestDatumBool(t *testing.T) {
	for _, v := range []bool{true, false} {
		d := DBool(v)
		buf := EncodeDatum(nil, d)
		got, _, err := DecodeDatum(buf, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got.Bool != v {
			t.Fatalf("expected %v, got %v", v, got.Bool)
		}
	}
}

func TestDatumFloat64(t *testing.T) {
	for _, v := range []float64{0, -1.5, math.Pi, math.Inf(1), math.NaN()} {
		d := DFloat64(v)
		buf := EncodeDatum(nil, d)
		got, n, err := DecodeDatum(buf, 0)
		if err != nil {
			t.Fatal(err)
		}
		if n != 9 {
			t.Fatalf("expected 9 bytes, consumed %d", n)
		}
		if math.IsNaN(v) {
			if !math.IsNaN(got.F64) {
				t.Fatal("expected NaN")
			}
		} else if got.F64 != v {
			t.Fatalf("expected %v, got %v", v, got.F64)
		}
	}
}

func TestMultipleDatums(t *testing.T) {
	var buf []byte
	buf = EncodeDatum(buf, DInt32(42))
	buf = EncodeDatum(buf, DText("hello"))
	buf = EncodeDatum(buf, DBool(true))
	buf = EncodeDatum(buf, DNull())

	offset := 0
	d, n, _ := DecodeDatum(buf, offset)
	if d.I32 != 42 {
		t.Fatal("datum 0")
	}
	offset += n

	d, n, _ = DecodeDatum(buf, offset)
	if d.Text != "hello" {
		t.Fatal("datum 1")
	}
	offset += n

	d, n, _ = DecodeDatum(buf, offset)
	if d.Bool != true {
		t.Fatal("datum 2")
	}
	offset += n

	d, _, _ = DecodeDatum(buf, offset)
	if d.Type != TypeNull {
		t.Fatal("datum 3")
	}
}

func TestDecodeDatum_Truncated(t *testing.T) {
	// Int32 needs 5 bytes; give only 3
	buf := []byte{byte(TypeInt32), 0, 0}
	_, _, err := DecodeDatum(buf, 0)
	if err == nil {
		t.Fatal("expected error for truncated int32")
	}
}

func TestDecodeDatum_UnknownType(t *testing.T) {
	buf := []byte{255}
	_, _, err := DecodeDatum(buf, 0)
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}

func TestDecodeDatum_EmptyBuffer(t *testing.T) {
	_, _, err := DecodeDatum([]byte{}, 0)
	if err == nil {
		t.Fatal("expected error for empty buffer")
	}
}

// --- Tuple tests ---

func TestTupleRoundtrip(t *testing.T) {
	tup := &Tuple{
		Xmin:  10,
		Xmax:  0,
		Flags: 0,
		Columns: []Datum{
			DInt32(1),
			DText("Alice"),
			DText("alice@example.com"),
			DBool(true),
			DFloat64(3.14),
			DNull(),
			DInt64(9999999999),
		},
	}

	buf := Encode(tup)
	got, err := Decode(buf)
	if err != nil {
		t.Fatal(err)
	}

	if got.Xmin != tup.Xmin || got.Xmax != tup.Xmax || got.Flags != tup.Flags {
		t.Fatalf("header mismatch: %+v vs %+v", got, tup)
	}
	if len(got.Columns) != len(tup.Columns) {
		t.Fatalf("column count: %d vs %d", len(got.Columns), len(tup.Columns))
	}

	if got.Columns[0].I32 != 1 {
		t.Fatal("col 0")
	}
	if got.Columns[1].Text != "Alice" {
		t.Fatal("col 1")
	}
	if got.Columns[2].Text != "alice@example.com" {
		t.Fatal("col 2")
	}
	if got.Columns[3].Bool != true {
		t.Fatal("col 3")
	}
	if got.Columns[4].F64 != 3.14 {
		t.Fatal("col 4")
	}
	if got.Columns[5].Type != TypeNull {
		t.Fatal("col 5")
	}
	if got.Columns[6].I64 != 9999999999 {
		t.Fatal("col 6")
	}
}

func TestTupleEmptyColumns(t *testing.T) {
	tup := &Tuple{Xmin: 1, Xmax: 2, Flags: 3, Columns: []Datum{}}
	buf := Encode(tup)
	got, err := Decode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Xmin != 1 || got.Xmax != 2 || got.Flags != 3 {
		t.Fatal("header mismatch")
	}
	if len(got.Columns) != 0 {
		t.Fatal("expected 0 columns")
	}
}

func TestTupleMVCCHeaders(t *testing.T) {
	tup := &Tuple{
		Xmin:    100,
		Xmax:    200,
		Flags:   0xABCD,
		Columns: []Datum{DInt32(42)},
	}
	buf := Encode(tup)
	got, err := Decode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Xmin != 100 || got.Xmax != 200 || got.Flags != 0xABCD {
		t.Fatalf("MVCC headers: xmin=%d xmax=%d flags=%04X", got.Xmin, got.Xmax, got.Flags)
	}
}

func TestDecodeTuple_TooSmall(t *testing.T) {
	_, err := Decode(make([]byte, 5))
	if err == nil {
		t.Fatal("expected error for small buffer")
	}
}

func TestDecodeTuple_Truncated(t *testing.T) {
	tup := &Tuple{Columns: []Datum{DText("hello")}}
	buf := Encode(tup)
	// Chop off the last few bytes
	_, err := Decode(buf[:len(buf)-3])
	if err == nil {
		t.Fatal("expected error for truncated tuple")
	}
}

func TestTupleAllNulls(t *testing.T) {
	tup := &Tuple{Columns: []Datum{DNull(), DNull(), DNull()}}
	buf := Encode(tup)
	got, err := Decode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(got.Columns))
	}
	for i, c := range got.Columns {
		if c.Type != TypeNull {
			t.Fatalf("column %d: expected Null, got %d", i, c.Type)
		}
	}
	// 3 nulls = 3 type bytes + 16 header = 19 bytes total
	if len(buf) != HeaderSize+3 {
		t.Fatalf("expected %d bytes, got %d", HeaderSize+3, len(buf))
	}
}

func TestTupleLargeText(t *testing.T) {
	big := string(make([]byte, 5000))
	tup := &Tuple{Columns: []Datum{DText(big)}}
	buf := Encode(tup)
	got, err := Decode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Columns[0].Text) != 5000 {
		t.Fatalf("expected 5000 chars, got %d", len(got.Columns[0].Text))
	}
}
