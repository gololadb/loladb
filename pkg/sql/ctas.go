package sql

import (
	"fmt"

	"github.com/gololadb/gopgsql/parser"

	"github.com/gololadb/loladb/pkg/catalog"
	"github.com/gololadb/loladb/pkg/tuple"
)

// execCreateTableAs handles CREATE TABLE ... AS SELECT.
func (ex *Executor) execCreateTableAs(ctas *parser.CreateTableAsStmt) (*Result, error) {
	if ctas.Into == nil || ctas.Into.Rel == nil {
		return nil, fmt.Errorf("CREATE TABLE AS: missing target table name")
	}
	tableName := ctas.Into.Rel.Relname

	// Check if table already exists.
	if ctas.IfNotExists {
		if rel, _ := ex.Cat.FindRelation(tableName); rel != nil {
			return &Result{Message: "SELECT 0"}, nil
		}
	}

	// Execute the source query.
	querySQL := parser.Deparse(ctas.Query)
	r, err := ex.Exec(querySQL)
	if err != nil {
		return nil, fmt.Errorf("CREATE TABLE AS: %w", err)
	}

	// Build column definitions from the result set.
	cols := make([]catalog.ColumnDef, len(r.Columns))
	for i, name := range r.Columns {
		// Infer type from the first row if available, otherwise default to TEXT.
		dt := tuple.TypeText
		if len(r.Rows) > 0 && i < len(r.Rows[0]) {
			dt = r.Rows[0][i].Type
			if dt == tuple.TypeNull {
				dt = tuple.TypeText
			}
		}
		cols[i] = catalog.ColumnDef{
			Name:    name,
			Type:    dt,
			Typmod:  -1,
		}
	}

	// Create the table.
	if _, err := ex.Cat.CreateTable(tableName, cols); err != nil {
		return nil, fmt.Errorf("CREATE TABLE AS: %w", err)
	}

	// Insert data if WITH DATA (default).
	count := int64(0)
	if ctas.WithData {
		for _, row := range r.Rows {
			if _, err := ex.Cat.InsertInto(tableName, row); err != nil {
				return nil, fmt.Errorf("CREATE TABLE AS: insert: %w", err)
			}
			count++
		}
	}

	return &Result{
		Message: fmt.Sprintf("SELECT %d", count),
	}, nil
}
