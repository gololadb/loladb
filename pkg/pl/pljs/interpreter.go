// Package pljs implements a JavaScript procedural language for LolaDB
// using the goja ES5.1+ runtime. Functions declared with LANGUAGE pljs
// have their body executed as JavaScript with arguments available as
// globals and a plv8-compatible SPI bridge for database access.
package pljs

import (
	"fmt"
	"strings"

	"github.com/dop251/goja"

	"github.com/gololadb/loladb/pkg/tuple"
)

// SQLResult mirrors the executor result shape.
type SQLResult struct {
	Columns []string
	Rows    [][]tuple.Datum
	Message string
}

// SQLExecFunc executes a SQL string and returns the result.
type SQLExecFunc func(sql string) (*SQLResult, error)

// FuncResult holds the return value of a PL/JS function.
type FuncResult struct {
	Value  tuple.Datum
	IsNull bool
}

// Interpreter executes JavaScript function bodies.
type Interpreter struct {
	execSQL SQLExecFunc
}

// New creates an interpreter that delegates SQL to the given executor.
func New(execSQL SQLExecFunc) *Interpreter {
	return &Interpreter{execSQL: execSQL}
}

// ExecFunction runs a JS function body with the given parameters.
func (interp *Interpreter) ExecFunction(body string, params map[string]tuple.Datum) (*FuncResult, error) {
	vm := goja.New()

	// Inject parameters as globals.
	var paramNames []string
	var paramArgs []string
	for name, val := range params {
		vm.Set(name, datumToJS(val))
		// Only use named params (not $1, $2) for the wrapper function.
		if !strings.HasPrefix(name, "$") {
			paramNames = append(paramNames, name)
			paramArgs = append(paramArgs, name)
		}
	}

	// Inject SPI bridge as plv8 object.
	interp.injectSPI(vm)

	// Wrap body in a function call. If the body already contains "return",
	// wrap it as a function body. Otherwise treat it as an expression.
	var script string
	if strings.Contains(body, "return") {
		script = fmt.Sprintf("(function(%s) {\n%s\n})(%s)",
			strings.Join(paramNames, ", "),
			body,
			strings.Join(paramArgs, ", "))
	} else {
		// Try as expression first, fall back to function body.
		script = fmt.Sprintf("(function(%s) { return (%s); })(%s)",
			strings.Join(paramNames, ", "),
			body,
			strings.Join(paramArgs, ", "))
	}

	val, err := vm.RunString(script)
	if err != nil {
		// If expression mode failed, try as function body.
		if !strings.Contains(body, "return") {
			script = fmt.Sprintf("(function(%s) {\n%s\n})(%s)",
				strings.Join(paramNames, ", "),
				body,
				strings.Join(paramArgs, ", "))
			val, err = vm.RunString(script)
		}
		if err != nil {
			return nil, fmt.Errorf("pljs: %w", err)
		}
	}

	return &FuncResult{Value: jsToDatum(val), IsNull: goja.IsNull(val) || goja.IsUndefined(val)}, nil
}

// ExecBlock runs an anonymous JS block (DO $$ ... $$ LANGUAGE pljs).
func (interp *Interpreter) ExecBlock(body string) error {
	vm := goja.New()
	interp.injectSPI(vm)

	_, err := vm.RunString(body)
	if err != nil {
		return fmt.Errorf("pljs DO block: %w", err)
	}
	return nil
}

// injectSPI adds the plv8 object with execute() and elog() methods.
func (interp *Interpreter) injectSPI(vm *goja.Runtime) {
	plv8 := vm.NewObject()

	// plv8.execute(sql) → array of row objects.
	plv8.Set("execute", func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) < 1 {
			return goja.Null()
		}
		sql := call.Arguments[0].String()
		result, err := interp.execSQL(sql)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		// Convert rows to array of JS objects.
		rows := make([]interface{}, len(result.Rows))
		for i, row := range result.Rows {
			obj := make(map[string]interface{})
			for j, col := range result.Columns {
				if j < len(row) {
					obj[col] = datumToJS(row[j])
				}
			}
			rows[i] = obj
		}
		return vm.ToValue(rows)
	})

	// plv8.elog(level, msg) — log a message (simplified: just accept and ignore).
	plv8.Set("elog", func(call goja.FunctionCall) goja.Value {
		return goja.Undefined()
	})

	vm.Set("plv8", plv8)
}

// datumToJS converts a tuple.Datum to a Go value suitable for goja.
func datumToJS(d tuple.Datum) interface{} {
	switch d.Type {
	case tuple.TypeNull:
		return nil
	case tuple.TypeBool:
		return d.Bool
	case tuple.TypeInt32:
		return int64(d.I32)
	case tuple.TypeInt64:
		return d.I64
	case tuple.TypeFloat64:
		return d.F64
	case tuple.TypeText:
		return d.Text
	case tuple.TypeJSON:
		return d.Text // JSON as string
	default:
		return d.Text
	}
}

// jsToDatum converts a goja.Value back to a tuple.Datum.
func jsToDatum(v goja.Value) tuple.Datum {
	if v == nil || goja.IsNull(v) || goja.IsUndefined(v) {
		return tuple.DNull()
	}
	exported := v.Export()
	switch val := exported.(type) {
	case bool:
		return tuple.DBool(val)
	case int64:
		return tuple.DInt64(val)
	case float64:
		// If it's a whole number, return as int64.
		if val == float64(int64(val)) && val >= -1<<53 && val <= 1<<53 {
			return tuple.DInt64(int64(val))
		}
		return tuple.DFloat64(val)
	case string:
		return tuple.DText(val)
	default:
		return tuple.DText(fmt.Sprintf("%v", val))
	}
}
