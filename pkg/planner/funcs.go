package planner

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gololadb/loladb/pkg/tuple"
)

// evalBuiltinFunc evaluates a built-in SQL function by name.
func evalBuiltinFunc(name string, args []AnalyzedExpr, row *Row) (tuple.Datum, error) {
	switch strings.ToLower(name) {
	case "now", "current_timestamp":
		return tuple.DText(time.Now().UTC().Format("2006-01-02 15:04:05")), nil
	case "current_date":
		return tuple.DText(time.Now().UTC().Format("2006-01-02")), nil
	case "nextval":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("nextval requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		seqName := val.Text
		return tuple.DInt64(nextvalFor(seqName)), nil
	case "currval":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("currval requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		return tuple.DInt64(currvalFor(val.Text)), nil
	case "setval":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("setval requires at least 2 arguments")
		}
		seqVal, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		numVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		var v int64
		switch numVal.Type {
		case tuple.TypeInt64:
			v = numVal.I64
		case tuple.TypeInt32:
			v = int64(numVal.I32)
		default:
			return tuple.DNull(), fmt.Errorf("setval: expected integer, got %v", numVal.Type)
		}
		setvalFor(seqVal.Text, v)
		return tuple.DInt64(v), nil
	case "coalesce":
		for _, a := range args {
			val, err := a.Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			if val.Type != tuple.TypeNull {
				return val, nil
			}
		}
		return tuple.DNull(), nil
	case "nullif":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("nullif requires 2 arguments")
		}
		v1, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		v2, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if CompareDatums(v1, v2) == 0 {
			return tuple.DNull(), nil
		}
		return v1, nil
	case "length", "char_length", "character_length":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("%s requires 1 argument", name)
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		return tuple.DInt64(int64(len(val.Text))), nil
	case "upper":
		if len(args) < 1 {
			return tuple.DNull(), nil
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		return tuple.DText(strings.ToUpper(val.Text)), nil
	case "lower":
		if len(args) < 1 {
			return tuple.DNull(), nil
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		return tuple.DText(strings.ToLower(val.Text)), nil
	case "concat":
		var sb strings.Builder
		for _, a := range args {
			val, err := a.Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			if val.Type != tuple.TypeNull {
				sb.WriteString(fmt.Sprintf("%v", datumToString(val)))
			}
		}
		return tuple.DText(sb.String()), nil
	default:
		return tuple.DNull(), fmt.Errorf("function %s is not supported", name)
	}
}

func datumToString(d tuple.Datum) string {
	switch d.Type {
	case tuple.TypeText:
		return d.Text
	case tuple.TypeInt32:
		return fmt.Sprintf("%d", d.I32)
	case tuple.TypeInt64:
		return fmt.Sprintf("%d", d.I64)
	case tuple.TypeFloat64:
		return fmt.Sprintf("%g", d.F64)
	case tuple.TypeBool:
		if d.Bool {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

// castDatum converts a datum to the target type.
func castDatum(val tuple.Datum, targetType tuple.DatumType, typeName string) (tuple.Datum, error) {
	if val.Type == tuple.TypeNull {
		return tuple.DNull(), nil
	}

	// regclass is a PostgreSQL pseudo-type that resolves a relation name
	// to its OID. We treat it as a pass-through returning the text value.
	if strings.Contains(typeName, "regclass") {
		return val, nil
	}

	switch targetType {
	case tuple.TypeInt32:
		switch val.Type {
		case tuple.TypeInt32:
			return val, nil
		case tuple.TypeInt64:
			return tuple.DInt32(int32(val.I64)), nil
		case tuple.TypeFloat64:
			return tuple.DInt32(int32(val.F64)), nil
		case tuple.TypeText:
			var i int64
			if _, err := fmt.Sscanf(val.Text, "%d", &i); err != nil {
				return tuple.DNull(), fmt.Errorf("invalid input syntax for integer: %q", val.Text)
			}
			return tuple.DInt32(int32(i)), nil
		case tuple.TypeBool:
			if val.Bool {
				return tuple.DInt32(1), nil
			}
			return tuple.DInt32(0), nil
		}
	case tuple.TypeInt64:
		switch val.Type {
		case tuple.TypeInt64:
			return val, nil
		case tuple.TypeInt32:
			return tuple.DInt64(int64(val.I32)), nil
		case tuple.TypeFloat64:
			return tuple.DInt64(int64(val.F64)), nil
		case tuple.TypeText:
			var i int64
			if _, err := fmt.Sscanf(val.Text, "%d", &i); err != nil {
				return tuple.DNull(), fmt.Errorf("invalid input syntax for integer: %q", val.Text)
			}
			return tuple.DInt64(i), nil
		case tuple.TypeBool:
			if val.Bool {
				return tuple.DInt64(1), nil
			}
			return tuple.DInt64(0), nil
		}
	case tuple.TypeFloat64:
		switch val.Type {
		case tuple.TypeFloat64:
			return val, nil
		case tuple.TypeInt32:
			return tuple.DFloat64(float64(val.I32)), nil
		case tuple.TypeInt64:
			return tuple.DFloat64(float64(val.I64)), nil
		case tuple.TypeText:
			var f float64
			if _, err := fmt.Sscanf(val.Text, "%g", &f); err != nil {
				return tuple.DNull(), fmt.Errorf("invalid input syntax for numeric: %q", val.Text)
			}
			return tuple.DFloat64(f), nil
		}
	case tuple.TypeBool:
		switch val.Type {
		case tuple.TypeBool:
			return val, nil
		case tuple.TypeInt32:
			return tuple.DBool(val.I32 != 0), nil
		case tuple.TypeInt64:
			return tuple.DBool(val.I64 != 0), nil
		case tuple.TypeText:
			lower := strings.ToLower(strings.TrimSpace(val.Text))
			switch lower {
			case "true", "t", "yes", "y", "on", "1":
				return tuple.DBool(true), nil
			case "false", "f", "no", "n", "off", "0":
				return tuple.DBool(false), nil
			default:
				return tuple.DNull(), fmt.Errorf("invalid input syntax for boolean: %q", val.Text)
			}
		}
	case tuple.TypeText:
		return tuple.DText(datumToString(val)), nil
	}

	// Fallback: return as-is.
	return val, nil
}

// In-memory sequence state. Sequences are identified by name and
// auto-increment on each nextval() call. This is sufficient for
// the Pagila dataset and learning purposes.
var (
	seqMu   sync.Mutex
	seqVals = map[string]int64{}
)

func nextvalFor(name string) int64 {
	seqMu.Lock()
	defer seqMu.Unlock()
	seqVals[name]++
	return seqVals[name]
}

func currvalFor(name string) int64 {
	seqMu.Lock()
	defer seqMu.Unlock()
	return seqVals[name]
}

func setvalFor(name string, val int64) {
	seqMu.Lock()
	defer seqMu.Unlock()
	seqVals[name] = val
}
