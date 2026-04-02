package catalog

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gololadb/loladb/pkg/slottedpage"
	"github.com/gololadb/loladb/pkg/tuple"
)

func TestOverflow_ThousandsOfRows(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("events", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "msg", Type: tuple.TypeText},
	})

	n := 2000
	for i := 0; i < n; i++ {
		_, err := cat.InsertInto("events", []tuple.Datum{
			tuple.DInt32(int32(i)),
			tuple.DText(fmt.Sprintf("event-%d", i)),
		})
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// Verify all rows via SeqScan.
	count := 0
	cat.SeqScan("events", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		count++
		return true
	})
	if count != n {
		t.Fatalf("expected %d rows, got %d", n, count)
	}
}

func TestOverflow_RelPagesUpdated(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("data", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "payload", Type: tuple.TypeText},
	})

	// Insert enough to fill multiple pages.
	for i := 0; i < 200; i++ {
		cat.InsertInto("data", []tuple.Datum{
			tuple.DInt32(int32(i)),
			tuple.DText("some payload to fill pages faster"),
		})
	}

	rel, err := cat.FindRelation("data")
	if err != nil {
		t.Fatal(err)
	}
	if rel.Pages <= 1 {
		t.Fatalf("expected multiple pages, got %d", rel.Pages)
	}
}

func TestOverflow_ChainIntegrity(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("big", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "data", Type: tuple.TypeText},
	})

	// Insert rows with ~100 bytes each to force many pages.
	n := 500
	for i := 0; i < n; i++ {
		cat.InsertInto("big", []tuple.Datum{
			tuple.DInt32(int32(i)),
			tuple.DText(strings.Repeat("x", 80)),
		})
	}

	// Verify all rows are accessible and in order.
	var ids []int32
	cat.SeqScan("big", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		ids = append(ids, tup.Columns[0].I32)
		return true
	})
	if len(ids) != n {
		t.Fatalf("expected %d rows, got %d", n, len(ids))
	}
	for i, v := range ids {
		if v != int32(i) {
			t.Fatalf("row %d: expected id %d, got %d", i, i, v)
		}
	}
}

func TestOverflow_WithIndex(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("indexed", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "val", Type: tuple.TypeText},
	})
	cat.CreateIndex("idx_indexed_id", "indexed", "id", "btree")

	n := 1000
	for i := 0; i < n; i++ {
		cat.InsertInto("indexed", []tuple.Datum{
			tuple.DInt32(int32(i)),
			tuple.DText(fmt.Sprintf("val-%d", i)),
		})
	}

	// Spot-check via index.
	for _, k := range []int32{0, 100, 500, 999} {
		tuples, _, err := cat.IndexScan("idx_indexed_id", int64(k))
		if err != nil {
			t.Fatal(err)
		}
		if len(tuples) != 1 {
			t.Fatalf("key %d: expected 1, got %d", k, len(tuples))
		}
		if tuples[0].Columns[0].I32 != k {
			t.Fatalf("key %d: got %d", k, tuples[0].Columns[0].I32)
		}
	}

	// SeqScan count.
	count := 0
	cat.SeqScan("indexed", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		count++
		return true
	})
	if count != n {
		t.Fatalf("seqscan: expected %d, got %d", n, count)
	}
}

func TestToast_LargeTextColumn(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("docs", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "body", Type: tuple.TypeText},
	})

	// Create a text value larger than MaxInlineSize (1000).
	bigBody := strings.Repeat("A", 3000)

	_, err := cat.InsertInto("docs", []tuple.Datum{
		tuple.DInt32(1),
		tuple.DText(bigBody),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Read it back — should be detoasted transparently.
	var gotBody string
	cat.SeqScan("docs", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		gotBody = tup.Columns[1].Text
		return false
	})

	if gotBody != bigBody {
		t.Fatalf("expected %d chars, got %d", len(bigBody), len(gotBody))
	}
}

func TestToast_MixedColumns(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("mixed", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "small", Type: tuple.TypeText},
		{Name: "big", Type: tuple.TypeText},
	})

	smallVal := "hello"
	bigVal := strings.Repeat("B", 5000)

	cat.InsertInto("mixed", []tuple.Datum{
		tuple.DInt32(42),
		tuple.DText(smallVal),
		tuple.DText(bigVal),
	})

	cat.SeqScan("mixed", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		if tup.Columns[0].I32 != 42 {
			t.Fatalf("id: expected 42, got %d", tup.Columns[0].I32)
		}
		if tup.Columns[1].Text != smallVal {
			t.Fatalf("small: expected %q, got %q", smallVal, tup.Columns[1].Text)
		}
		if tup.Columns[2].Text != bigVal {
			t.Fatalf("big: expected %d chars, got %d", len(bigVal), len(tup.Columns[2].Text))
		}
		return false
	})
}

func TestToast_MultipleLargeRows(t *testing.T) {
	cat := newTestCatalog(t)
	cat.CreateTable("articles", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "content", Type: tuple.TypeText},
	})

	n := 20
	for i := 0; i < n; i++ {
		body := strings.Repeat(string(rune('A'+i%26)), 2000+i*100)
		cat.InsertInto("articles", []tuple.Datum{
			tuple.DInt32(int32(i)),
			tuple.DText(body),
		})
	}

	count := 0
	cat.SeqScan("articles", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		expectedLen := 2000 + int(tup.Columns[0].I32)*100
		if len(tup.Columns[1].Text) != expectedLen {
			t.Errorf("row %d: expected %d chars, got %d", tup.Columns[0].I32, expectedLen, len(tup.Columns[1].Text))
		}
		count++
		return true
	})
	if count != n {
		t.Fatalf("expected %d, got %d", n, count)
	}
}

func TestToast_PersistAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/test.lodb"

	eng, _ := openEngine(t, path)
	cat, _ := New(eng)
	cat.CreateTable("docs", []ColumnDef{
		{Name: "id", Type: tuple.TypeInt32},
		{Name: "body", Type: tuple.TypeText},
	})

	bigBody := strings.Repeat("Z", 4000)
	cat.InsertInto("docs", []tuple.Datum{
		tuple.DInt32(1), tuple.DText(bigBody),
	})
	eng.Close()

	eng2, _ := openEngine(t, path)
	defer eng2.Close()
	cat2, _ := New(eng2)

	var got string
	cat2.SeqScan("docs", func(id slottedpage.ItemID, tup *tuple.Tuple) bool {
		got = tup.Columns[1].Text
		return false
	})
	if got != bigBody {
		t.Fatalf("expected %d chars after restart, got %d", len(bigBody), len(got))
	}
}
