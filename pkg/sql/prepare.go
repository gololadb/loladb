package sql

import (
	"fmt"
	"strings"

	"github.com/gololadb/gopgsql/parser"
)

// PreparedStmt holds a server-side prepared statement.
type PreparedStmt struct {
	Name     string
	Query    string   // original SQL text of the prepared query
	ParamTypes []string // declared parameter type names (may be empty)
}

// execPrepare handles PREPARE name [(types)] AS stmt.
func (ex *Executor) execPrepare(ps *parser.PrepareStmt) (*Result, error) {
	name := strings.ToLower(ps.Name)

	// Deparse the query back to SQL text for storage.
	querySQL := parser.Deparse(ps.Query)

	var paramTypes []string
	for _, tn := range ps.Argtypes {
		if len(tn.Names) > 0 {
			paramTypes = append(paramTypes, tn.Names[len(tn.Names)-1])
		}
	}

	if ex.preparedStmts == nil {
		ex.preparedStmts = make(map[string]*PreparedStmt)
	}

	ex.preparedStmts[name] = &PreparedStmt{
		Name:       name,
		Query:      querySQL,
		ParamTypes: paramTypes,
	}

	return &Result{Message: "PREPARE"}, nil
}

// execExecute handles EXECUTE name [(params)].
func (ex *Executor) execExecute(es *parser.ExecuteStmt) (*Result, error) {
	name := strings.ToLower(es.Name)

	if ex.preparedStmts == nil {
		return nil, fmt.Errorf("prepared statement %q does not exist", name)
	}
	ps, ok := ex.preparedStmts[name]
	if !ok {
		return nil, fmt.Errorf("prepared statement %q does not exist", name)
	}

	// Deparse each parameter expression to get its SQL literal value.
	paramValues := make([]string, len(es.Params))
	for i, p := range es.Params {
		paramValues[i] = parser.Deparse(p)
	}

	// Substitute $1, $2, ... in the stored query with actual values.
	resolved := substituteParams(ps.Query, paramValues)

	// Execute the resolved query through the normal pipeline.
	return ex.Exec(resolved)
}

// execDeallocate handles DEALLOCATE [PREPARE] name | ALL.
func (ex *Executor) execDeallocate(ds *parser.DeallocateStmt) (*Result, error) {
	if ds.IsAll {
		ex.preparedStmts = nil
		return &Result{Message: "DEALLOCATE ALL"}, nil
	}

	name := strings.ToLower(ds.Name)
	if ex.preparedStmts == nil || ex.preparedStmts[name] == nil {
		return nil, fmt.Errorf("prepared statement %q does not exist", name)
	}
	delete(ex.preparedStmts, name)
	return &Result{Message: "DEALLOCATE"}, nil
}

// substituteParams replaces $1, $2, ... in sql with the corresponding
// parameter values. Handles quoted strings to avoid replacing inside them.
func substituteParams(sql string, params []string) string {
	var buf strings.Builder
	i := 0
	n := len(sql)

	for i < n {
		ch := sql[i]

		// Skip single-quoted strings.
		if ch == '\'' {
			buf.WriteByte(ch)
			i++
			for i < n {
				if sql[i] == '\'' {
					buf.WriteByte(sql[i])
					i++
					// Escaped quote ''
					if i < n && sql[i] == '\'' {
						buf.WriteByte(sql[i])
						i++
						continue
					}
					break
				}
				buf.WriteByte(sql[i])
				i++
			}
			continue
		}

		// Check for $N parameter reference.
		if ch == '$' && i+1 < n && sql[i+1] >= '1' && sql[i+1] <= '9' {
			j := i + 1
			for j < n && sql[j] >= '0' && sql[j] <= '9' {
				j++
			}
			numStr := sql[i+1 : j]
			num := 0
			for _, d := range numStr {
				num = num*10 + int(d-'0')
			}
			if num >= 1 && num <= len(params) {
				buf.WriteString(params[num-1])
			} else {
				buf.WriteString(sql[i:j])
			}
			i = j
			continue
		}

		buf.WriteByte(ch)
		i++
	}

	return buf.String()
}
