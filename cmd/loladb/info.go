package main

import (
	"fmt"

	"github.com/jespino/loladb/pkg/catalog"
	"github.com/jespino/loladb/pkg/engine"
	"github.com/jespino/loladb/pkg/tuple"
)

func runInfo(path string) {
	eng, err := engine.Open(path, 0)
	if err != nil {
		fatal(fmt.Sprintf("Failed to open database: %v", err))
	}
	defer eng.Close()

	cat, err := catalog.New(eng)
	if err != nil {
		fatal(fmt.Sprintf("Failed to load catalog: %v", err))
	}

	sb := eng.Super

	fmt.Println("=== LolaDB Database Info ===")
	fmt.Printf("File:            %s\n", path)
	fmt.Printf("Magic:           0x%08X\n", sb.Magic)
	fmt.Printf("Version:         %d\n", sb.Version)
	fmt.Printf("NextOID:         %d\n", sb.NextOID)
	fmt.Printf("NextXID:         %d\n", sb.NextXID)
	fmt.Printf("CheckpointLSN:   %d\n", sb.CheckpointLSN)
	fmt.Printf("TotalPages:      %d\n", sb.TotalPages)
	fmt.Printf("FreeListPage:    %d\n", sb.FreeListPage)
	fmt.Printf("PgClassPage:     %d\n", sb.PgClassPage)
	fmt.Printf("PgAttrPage:      %d\n", sb.PgAttrPage)
	fmt.Println()

	// Freelist stats.
	fmt.Printf("Freelist:        capacity=%d used=%d free=%d\n",
		eng.FreeList.Capacity(), eng.FreeList.UsedCount(), eng.FreeList.FreeCount())
	fmt.Println()

	// Tables.
	tables, _ := cat.ListTables()
	fmt.Printf("Tables: %d\n", len(tables))
	for _, t := range tables {
		cols, _ := cat.GetColumns(t.OID)
		stats, _ := cat.Stats(t.Name)
		tupleCount := int64(0)
		if stats != nil {
			tupleCount = stats.TupleCount
		}
		fmt.Printf("  %-20s  OID=%-4d  pages=%-3d  tuples=%-6d  columns=%d\n",
			t.Name, t.OID, t.Pages, tupleCount, len(cols))
		for _, c := range cols {
			fmt.Printf("    %-18s  %s\n", c.Name, typeName(c.Type))
		}
	}
	fmt.Println()

	// Indexes.
	fmt.Println("Indexes:")
	indexCount := 0
	for _, t := range tables {
		indexes, _ := cat.ListIndexesForTable(t.OID)
		for _, idx := range indexes {
			cols, _ := cat.GetColumns(t.OID)
			colName := "?"
			if int(idx.ColNum-1) < len(cols) {
				colName = cols[idx.ColNum-1].Name
			}
			fmt.Printf("  %-20s  on %-15s (%s)  root_page=%d\n",
				idx.Name, t.Name, colName, idx.HeadPage)
			indexCount++
		}
	}
	if indexCount == 0 {
		fmt.Println("  (none)")
	}
}

func typeName(t int32) string {
	switch tuple.DatumType(t) {
	case tuple.TypeNull:
		return "NULL"
	case tuple.TypeInt32:
		return "INT4"
	case tuple.TypeInt64:
		return "INT8"
	case tuple.TypeText:
		return "TEXT"
	case tuple.TypeBool:
		return "BOOL"
	case tuple.TypeFloat64:
		return "FLOAT8"
	default:
		return fmt.Sprintf("TYPE(%d)", t)
	}
}

