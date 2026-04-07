// Package plstarlark implements a Starlark procedural language for LolaDB.
// Starlark is a Python-like language designed for embedding. Functions
// declared with LANGUAGE plstarlark have their body executed with
// arguments as globals and an spi.execute() bridge for database access.
package plstarlark

import (
	"fmt"
	"strings"

	"go.starlark.net/starlark"
	starlarkjson "go.starlark.net/lib/json"

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

// FuncResult holds the return value of a Starlark function.
type FuncResult struct {
	Value  tuple.Datum
	IsNull bool
}

// Interpreter executes Starlark function bodies.
type Interpreter struct {
	execSQL SQLExecFunc
}

// New creates an interpreter that delegates SQL to the given executor.
func New(execSQL SQLExecFunc) *Interpreter {
	return &Interpreter{execSQL: execSQL}
}

// ExecFunction runs a Starlark function body with the given parameters.
func (interp *Interpreter) ExecFunction(body string, params map[string]tuple.Datum) (*FuncResult, error) {
	// Build the predeclared globals: parameters + SPI bridge.
	predeclared := starlark.StringDict{}

	for name, val := range params {
		predeclared[name] = datumToStarlark(val)
	}

	// Add SPI module.
	predeclared["spi"] = interp.makeSPIModule()

	// Add json module for convenience.
	predeclared["json"] = starlarkjson.Module

	// Build parameter names for the wrapper function.
	var paramNames []string
	for name := range params {
		if !strings.HasPrefix(name, "$") {
			paramNames = append(paramNames, name)
		}
	}

	// Wrap the body in a function definition and call it.
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("def __func__(%s):\n", strings.Join(paramNames, ", ")))
	for _, line := range strings.Split(body, "\n") {
		sb.WriteString("  ")
		sb.WriteString(line)
		sb.WriteString("\n")
	}
	sb.WriteString("__result__ = __func__(")
	sb.WriteString(strings.Join(paramNames, ", "))
	sb.WriteString(")\n")

	thread := &starlark.Thread{Name: "plstarlark"}
	globals, err := starlark.ExecFile(thread, "<function>", sb.String(), predeclared)
	if err != nil {
		return nil, fmt.Errorf("plstarlark: %w", err)
	}

	result, ok := globals["__result__"]
	if !ok || result == starlark.None {
		return &FuncResult{IsNull: true, Value: tuple.DNull()}, nil
	}

	return &FuncResult{Value: starlarkToDatum(result)}, nil
}

// ExecBlock runs an anonymous Starlark block (DO $$ ... $$ LANGUAGE plstarlark).
func (interp *Interpreter) ExecBlock(body string) error {
	predeclared := starlark.StringDict{
		"spi":  interp.makeSPIModule(),
		"json": starlarkjson.Module,
	}

	thread := &starlark.Thread{Name: "plstarlark-do"}
	_, err := starlark.ExecFile(thread, "<do-block>", body, predeclared)
	if err != nil {
		return fmt.Errorf("plstarlark DO block: %w", err)
	}
	return nil
}

// makeSPIModule creates a Starlark struct with an execute() method.
func (interp *Interpreter) makeSPIModule() *starlarkModule {
	return &starlarkModule{execSQL: interp.execSQL}
}

// starlarkModule implements starlark.HasAttrs to provide spi.execute().
type starlarkModule struct {
	execSQL SQLExecFunc
}

func (m *starlarkModule) String() string        { return "<spi module>" }
func (m *starlarkModule) Type() string          { return "spi_module" }
func (m *starlarkModule) Freeze()               {}
func (m *starlarkModule) Truth() starlark.Bool  { return true }
func (m *starlarkModule) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }

func (m *starlarkModule) Attr(name string) (starlark.Value, error) {
	if name == "execute" {
		return starlark.NewBuiltin("spi.execute", m.execute), nil
	}
	return nil, nil
}

func (m *starlarkModule) AttrNames() []string {
	return []string{"execute"}
}

// execute implements spi.execute(sql) → list of dicts.
func (m *starlarkModule) execute(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if len(args) < 1 {
		return starlark.None, fmt.Errorf("spi.execute requires a SQL string argument")
	}
	sqlStr, ok := starlark.AsString(args[0])
	if !ok {
		return starlark.None, fmt.Errorf("spi.execute: argument must be a string")
	}

	result, err := m.execSQL(sqlStr)
	if err != nil {
		return starlark.None, err
	}

	// Convert rows to list of dicts.
	rows := starlark.NewList(nil)
	for _, row := range result.Rows {
		dict := starlark.NewDict(len(result.Columns))
		for j, col := range result.Columns {
			if j < len(row) {
				dict.SetKey(starlark.String(col), datumToStarlark(row[j]))
			}
		}
		rows.Append(dict)
	}
	return rows, nil
}

// datumToStarlark converts a tuple.Datum to a starlark.Value.
func datumToStarlark(d tuple.Datum) starlark.Value {
	switch d.Type {
	case tuple.TypeNull:
		return starlark.None
	case tuple.TypeBool:
		return starlark.Bool(d.Bool)
	case tuple.TypeInt32:
		return starlark.MakeInt(int(d.I32))
	case tuple.TypeInt64:
		return starlark.MakeInt64(d.I64)
	case tuple.TypeFloat64:
		return starlark.Float(d.F64)
	case tuple.TypeText:
		return starlark.String(d.Text)
	default:
		return starlark.String(d.Text)
	}
}

// starlarkToDatum converts a starlark.Value back to a tuple.Datum.
func starlarkToDatum(v starlark.Value) tuple.Datum {
	switch val := v.(type) {
	case starlark.NoneType:
		return tuple.DNull()
	case starlark.Bool:
		return tuple.DBool(bool(val))
	case starlark.Int:
		i, ok := val.Int64()
		if ok {
			return tuple.DInt64(i)
		}
		return tuple.DText(val.String())
	case starlark.Float:
		return tuple.DFloat64(float64(val))
	case starlark.String:
		return tuple.DText(string(val))
	default:
		return tuple.DText(v.String())
	}
}
