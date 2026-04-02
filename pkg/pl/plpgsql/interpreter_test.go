package plpgsql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gololadb/loladb/pkg/tuple"
)

// mockExecSQL returns a simple SQL executor for testing.
func mockExecSQL() SQLExecFunc {
	return func(sql string) (*SQLResult, error) {
		upper := strings.ToUpper(strings.TrimSpace(sql))

		// Handle SELECT <expr> for expression evaluation.
		if strings.HasPrefix(upper, "SELECT ") {
			expr := strings.TrimSpace(sql[7:])

			// Simple integer literals.
			if n, ok := parseInt(expr); ok {
				return &SQLResult{
					Columns: []string{"?column?"},
					Rows:    [][]tuple.Datum{{tuple.DInt32(int32(n))}},
				}, nil
			}

			// Simple arithmetic: a + b, a * b, a - b.
			if parts := splitArith(expr); parts != nil {
				a, _ := parseInt(parts[0])
				b, _ := parseInt(parts[2])
				var result int
				switch parts[1] {
				case "+":
					result = a + b
				case "-":
					result = a - b
				case "*":
					result = a * b
				}
				return &SQLResult{
					Columns: []string{"?column?"},
					Rows:    [][]tuple.Datum{{tuple.DInt32(int32(result))}},
				}, nil
			}

			// Boolean expressions.
			if expr == "true" || expr == "TRUE" {
				return &SQLResult{
					Columns: []string{"?column?"},
					Rows:    [][]tuple.Datum{{tuple.DBool(true)}},
				}, nil
			}
			if expr == "false" || expr == "FALSE" {
				return &SQLResult{
					Columns: []string{"?column?"},
					Rows:    [][]tuple.Datum{{tuple.DBool(false)}},
				}, nil
			}

			// Comparison: a < b, a > b, a = b, a <= b, a >= b.
			if parts := splitComparison(expr); parts != nil {
				a, _ := parseInt(parts[0])
				b, _ := parseInt(parts[2])
				var result bool
				switch parts[1] {
				case "<":
					result = a < b
				case ">":
					result = a > b
				case "=":
					result = a == b
				case "<=":
					result = a <= b
				case ">=":
					result = a >= b
				}
				return &SQLResult{
					Columns: []string{"?column?"},
					Rows:    [][]tuple.Datum{{tuple.DBool(result)}},
				}, nil
			}

			// String literals.
			if strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'") {
				return &SQLResult{
					Columns: []string{"?column?"},
					Rows:    [][]tuple.Datum{{tuple.DText(expr[1 : len(expr)-1])}},
				}, nil
			}

			// NULL.
			if upper == "SELECT NULL" {
				return &SQLResult{
					Columns: []string{"?column?"},
					Rows:    [][]tuple.Datum{{tuple.DNull()}},
				}, nil
			}

			return nil, fmt.Errorf("mock: cannot evaluate %q", sql)
		}

		// DML statements return empty results.
		return &SQLResult{Message: "OK"}, nil
	}
}

func parseInt(s string) (int, bool) {
	s = strings.TrimSpace(s)
	n := 0
	neg := false
	i := 0
	if len(s) > 0 && s[0] == '-' {
		neg = true
		i = 1
	}
	if i >= len(s) {
		return 0, false
	}
	for ; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		n = n*10 + int(s[i]-'0')
	}
	if neg {
		n = -n
	}
	return n, true
}

func splitArith(expr string) []string {
	for _, op := range []string{" + ", " - ", " * "} {
		idx := strings.Index(expr, op)
		if idx >= 0 {
			return []string{
				strings.TrimSpace(expr[:idx]),
				strings.TrimSpace(op),
				strings.TrimSpace(expr[idx+len(op):]),
			}
		}
	}
	return nil
}

func splitComparison(expr string) []string {
	for _, op := range []string{" <= ", " >= ", " < ", " > ", " = "} {
		idx := strings.Index(expr, op)
		if idx >= 0 {
			return []string{
				strings.TrimSpace(expr[:idx]),
				strings.TrimSpace(op),
				strings.TrimSpace(expr[idx+len(op):]),
			}
		}
	}
	return nil
}

func TestExecFunction_SimpleReturn(t *testing.T) {
	interp := New(mockExecSQL())
	result, err := interp.ExecFunction(`
		DECLARE
			x integer;
		BEGIN
			x := 42;
			RETURN x;
		END
	`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Value.I32 != 42 {
		t.Fatalf("expected 42, got %v", result.Value)
	}
}

func TestExecFunction_IfElse(t *testing.T) {
	interp := New(mockExecSQL())
	result, err := interp.ExecFunction(`
		DECLARE
			x integer;
			y integer;
		BEGIN
			x := 10;
			IF x > 5 THEN
				y := 1;
			ELSE
				y := 0;
			END IF;
			RETURN y;
		END
	`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Value.I32 != 1 {
		t.Fatalf("expected 1, got %v", result.Value)
	}
}

func TestExecFunction_WhileLoop(t *testing.T) {
	interp := New(mockExecSQL())
	result, err := interp.ExecFunction(`
		DECLARE
			i integer;
			total integer;
		BEGIN
			i := 1;
			total := 0;
			WHILE i <= 5 LOOP
				total := total + i;
				i := i + 1;
			END LOOP;
			RETURN total;
		END
	`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Value.I32 != 15 {
		t.Fatalf("expected 15 (1+2+3+4+5), got %v", result.Value)
	}
}

func TestExecFunction_ForLoop(t *testing.T) {
	interp := New(mockExecSQL())
	result, err := interp.ExecFunction(`
		DECLARE
			total integer;
		BEGIN
			total := 0;
			FOR i IN 1..5 LOOP
				total := total + i;
			END LOOP;
			RETURN total;
		END
	`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Value.I32 != 15 {
		t.Fatalf("expected 15, got %v", result.Value)
	}
}

func TestExecFunction_RaiseException(t *testing.T) {
	interp := New(mockExecSQL())
	_, err := interp.ExecFunction(`
		BEGIN
			RAISE EXCEPTION 'something went wrong';
		END
	`, nil)
	if err == nil {
		t.Fatal("expected error from RAISE EXCEPTION")
	}
	if !strings.Contains(err.Error(), "something went wrong") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExecFunction_WithParams(t *testing.T) {
	interp := New(mockExecSQL())
	params := map[string]tuple.Datum{
		"a": tuple.DInt32(10),
		"b": tuple.DInt32(20),
	}
	result, err := interp.ExecFunction(`
		DECLARE
			total integer;
		BEGIN
			total := a + b;
			RETURN total;
		END
	`, params)
	if err != nil {
		t.Fatal(err)
	}
	if result.Value.I32 != 30 {
		t.Fatalf("expected 30, got %v", result.Value)
	}
}

func TestExecTrigger_ModifyNew(t *testing.T) {
	interp := New(mockExecSQL())
	td := &TriggerData{
		TgName:  "test_trigger",
		TgTable: "test_table",
		TgOp:    "INSERT",
		TgWhen:  "BEFORE",
		TgLevel: "ROW",
		NewRow: map[string]tuple.Datum{
			"id":    tuple.DInt32(1),
			"value": tuple.DInt32(10),
		},
		ColNames: []string{"id", "value"},
	}

	// Trigger doubles the value column.
	result, err := interp.ExecTrigger(`
		BEGIN
			NEW.value := NEW.value * 2;
			RETURN NEW;
		END
	`, td)
	if err != nil {
		t.Fatal(err)
	}
	if result.TriggerRow == nil {
		t.Fatal("expected non-nil TriggerRow")
	}
	if result.TriggerRow["value"].I32 != 20 {
		t.Fatalf("expected value=20, got %v", result.TriggerRow["value"])
	}
	// id should be preserved.
	if result.TriggerRow["id"].I32 != 1 {
		t.Fatalf("expected id=1, got %v", result.TriggerRow["id"])
	}
}
