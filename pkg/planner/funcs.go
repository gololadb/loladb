package planner

import (
	"fmt"
	"math"
	"math/rand"
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

	// --- String functions ---
	case "substring", "substr":
		// substring(str, start) or substring(str, start, length)
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("substring requires 2 or 3 arguments")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		startVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if startVal.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		s := datumToString(val)
		runes := []rune(s)
		start := int(startVal.I64) - 1 // SQL is 1-based
		if start < 0 {
			start = 0
		}
		if start >= len(runes) {
			return tuple.DText(""), nil
		}
		if len(args) >= 3 {
			lenVal, err := args[2].Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			if lenVal.Type == tuple.TypeNull {
				return tuple.DNull(), nil
			}
			length := int(lenVal.I64)
			if length < 0 {
				length = 0
			}
			end := start + length
			if end > len(runes) {
				end = len(runes)
			}
			return tuple.DText(string(runes[start:end])), nil
		}
		return tuple.DText(string(runes[start:])), nil

	case "trim", "btrim":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("trim requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		return tuple.DText(strings.TrimSpace(datumToString(val))), nil

	case "ltrim":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("ltrim requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		s := datumToString(val)
		if len(args) >= 2 {
			charsVal, err := args[1].Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			return tuple.DText(strings.TrimLeft(s, datumToString(charsVal))), nil
		}
		return tuple.DText(strings.TrimLeft(s, " \t\n\r")), nil

	case "rtrim":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("rtrim requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		s := datumToString(val)
		if len(args) >= 2 {
			charsVal, err := args[1].Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			return tuple.DText(strings.TrimRight(s, datumToString(charsVal))), nil
		}
		return tuple.DText(strings.TrimRight(s, " \t\n\r")), nil

	case "replace":
		if len(args) < 3 {
			return tuple.DNull(), fmt.Errorf("replace requires 3 arguments")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		fromVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		toVal, err := args[2].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		return tuple.DText(strings.ReplaceAll(datumToString(val), datumToString(fromVal), datumToString(toVal))), nil

	case "position":
		// Parser sends position(haystack, needle) per PG convention.
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("position requires 2 arguments")
		}
		strVal, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		subVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if subVal.Type == tuple.TypeNull || strVal.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		idx := strings.Index(datumToString(strVal), datumToString(subVal))
		if idx < 0 {
			return tuple.DInt64(0), nil
		}
		// Convert byte index to rune index for 1-based result
		runeIdx := len([]rune(datumToString(strVal)[:idx]))
		return tuple.DInt64(int64(runeIdx + 1)), nil

	case "left":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("left requires 2 arguments")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		nVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		runes := []rune(datumToString(val))
		n := int(nVal.I64)
		if n < 0 {
			n = 0
		}
		if n > len(runes) {
			n = len(runes)
		}
		return tuple.DText(string(runes[:n])), nil

	case "right":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("right requires 2 arguments")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		nVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		runes := []rune(datumToString(val))
		n := int(nVal.I64)
		if n < 0 {
			n = 0
		}
		if n > len(runes) {
			n = len(runes)
		}
		return tuple.DText(string(runes[len(runes)-n:])), nil

	case "lpad":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("lpad requires 2 or 3 arguments")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		lenVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		s := datumToString(val)
		targetLen := int(lenVal.I64)
		pad := " "
		if len(args) >= 3 {
			padVal, err := args[2].Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			pad = datumToString(padVal)
		}
		runes := []rune(s)
		if len(runes) >= targetLen {
			return tuple.DText(string(runes[:targetLen])), nil
		}
		padRunes := []rune(pad)
		if len(padRunes) == 0 {
			return tuple.DText(s), nil
		}
		needed := targetLen - len(runes)
		var sb strings.Builder
		for i := 0; i < needed; i++ {
			sb.WriteRune(padRunes[i%len(padRunes)])
		}
		sb.WriteString(s)
		return tuple.DText(sb.String()), nil

	case "rpad":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("rpad requires 2 or 3 arguments")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		lenVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		s := datumToString(val)
		targetLen := int(lenVal.I64)
		pad := " "
		if len(args) >= 3 {
			padVal, err := args[2].Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			pad = datumToString(padVal)
		}
		runes := []rune(s)
		if len(runes) >= targetLen {
			return tuple.DText(string(runes[:targetLen])), nil
		}
		padRunes := []rune(pad)
		if len(padRunes) == 0 {
			return tuple.DText(s), nil
		}
		var sb strings.Builder
		sb.WriteString(s)
		needed := targetLen - len(runes)
		for i := 0; i < needed; i++ {
			sb.WriteRune(padRunes[i%len(padRunes)])
		}
		return tuple.DText(sb.String()), nil

	case "repeat":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("repeat requires 2 arguments")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		nVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		n := int(nVal.I64)
		if n < 0 {
			n = 0
		}
		return tuple.DText(strings.Repeat(datumToString(val), n)), nil

	case "reverse":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("reverse requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		runes := []rune(datumToString(val))
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return tuple.DText(string(runes)), nil

	case "split_part":
		if len(args) < 3 {
			return tuple.DNull(), fmt.Errorf("split_part requires 3 arguments")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		delimVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		partVal, err := args[2].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		parts := strings.Split(datumToString(val), datumToString(delimVal))
		idx := int(partVal.I64) - 1 // 1-based
		if idx < 0 || idx >= len(parts) {
			return tuple.DText(""), nil
		}
		return tuple.DText(parts[idx]), nil

	// --- Math functions ---
	case "abs":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("abs requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		f, ok := toFloat64Func(val)
		if !ok {
			return tuple.DNull(), fmt.Errorf("abs: non-numeric argument")
		}
		return numericResult(math.Abs(f), val.Type), nil
	case "ceil", "ceiling":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("ceil requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		f, ok := toFloat64Func(val)
		if !ok {
			return tuple.DNull(), fmt.Errorf("ceil: non-numeric argument")
		}
		return numericResult(math.Ceil(f), val.Type), nil
	case "floor":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("floor requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		f, ok := toFloat64Func(val)
		if !ok {
			return tuple.DNull(), fmt.Errorf("floor: non-numeric argument")
		}
		return numericResult(math.Floor(f), val.Type), nil
	case "round":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("round requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		f, ok := toFloat64Func(val)
		if !ok {
			return tuple.DNull(), fmt.Errorf("round: non-numeric argument")
		}
		// Optional second argument: number of decimal places.
		if len(args) >= 2 {
			pval, err := args[1].Eval(row)
			if err == nil && pval.Type != tuple.TypeNull {
				p, _ := toFloat64Func(pval)
				scale := math.Pow(10, p)
				return tuple.DFloat64(math.Round(f*scale) / scale), nil
			}
		}
		return numericResult(math.Round(f), val.Type), nil
	case "trunc", "truncate":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("trunc requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		f, ok := toFloat64Func(val)
		if !ok {
			return tuple.DNull(), fmt.Errorf("trunc: non-numeric argument")
		}
		return numericResult(math.Trunc(f), val.Type), nil
	case "mod":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("mod requires 2 arguments")
		}
		v1, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		v2, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if v1.Type == tuple.TypeNull || v2.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		f1, _ := toFloat64Func(v1)
		f2, _ := toFloat64Func(v2)
		if f2 == 0 {
			return tuple.DNull(), fmt.Errorf("division by zero")
		}
		return numericResult(math.Mod(f1, f2), v1.Type), nil
	case "power", "pow":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("power requires 2 arguments")
		}
		v1, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		v2, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if v1.Type == tuple.TypeNull || v2.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		f1, _ := toFloat64Func(v1)
		f2, _ := toFloat64Func(v2)
		return tuple.DFloat64(math.Pow(f1, f2)), nil
	case "sqrt":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("sqrt requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		f, _ := toFloat64Func(val)
		if f < 0 {
			return tuple.DNull(), fmt.Errorf("cannot take square root of a negative number")
		}
		return tuple.DFloat64(math.Sqrt(f)), nil
	case "cbrt":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("cbrt requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		f, _ := toFloat64Func(val)
		return tuple.DFloat64(math.Cbrt(f)), nil
	case "sign":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("sign requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		f, _ := toFloat64Func(val)
		switch {
		case f < 0:
			return numericResult(-1, val.Type), nil
		case f > 0:
			return numericResult(1, val.Type), nil
		default:
			return numericResult(0, val.Type), nil
		}
	case "random":
		return tuple.DFloat64(rand.Float64()), nil
	case "pi":
		return tuple.DFloat64(math.Pi), nil
	case "log", "ln":
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
		f, _ := toFloat64Func(val)
		if f <= 0 {
			return tuple.DNull(), fmt.Errorf("cannot take logarithm of zero or negative number")
		}
		if name == "log" && len(args) >= 2 {
			// log(base, x)
			v2, err := args[1].Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			f2, _ := toFloat64Func(v2)
			if f2 <= 0 {
				return tuple.DNull(), fmt.Errorf("cannot take logarithm of zero or negative number")
			}
			return tuple.DFloat64(math.Log(f2) / math.Log(f)), nil
		}
		return tuple.DFloat64(math.Log(f)), nil
	case "log10":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("log10 requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		f, _ := toFloat64Func(val)
		if f <= 0 {
			return tuple.DNull(), fmt.Errorf("cannot take logarithm of zero or negative number")
		}
		return tuple.DFloat64(math.Log10(f)), nil
	case "exp":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("exp requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		f, _ := toFloat64Func(val)
		return tuple.DFloat64(math.Exp(f)), nil
	case "greatest":
		return evalMinMax(args, row, false)
	case "least":
		return evalMinMax(args, row, true)

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

// toFloat64Func converts a numeric Datum to float64.
// Returns the value and true on success, or 0 and false for non-numeric types.
func toFloat64Func(d tuple.Datum) (float64, bool) {
	switch d.Type {
	case tuple.TypeFloat64:
		return d.F64, true
	case tuple.TypeInt64:
		return float64(d.I64), true
	case tuple.TypeInt32:
		return float64(d.I32), true
	default:
		return 0, false
	}
}

// numericResult wraps a float64 result back into the original numeric type
// to preserve type consistency (e.g. abs(int) returns int).
func numericResult(f float64, origType tuple.DatumType) tuple.Datum {
	switch origType {
	case tuple.TypeInt32:
		return tuple.DInt32(int32(f))
	case tuple.TypeInt64:
		return tuple.DInt64(int64(f))
	default:
		return tuple.DFloat64(f)
	}
}

// evalMinMax evaluates GREATEST (least=false) or LEAST (least=true) across args.
func evalMinMax(args []AnalyzedExpr, row *Row, least bool) (tuple.Datum, error) {
	if len(args) == 0 {
		return tuple.DNull(), nil
	}
	best, err := args[0].Eval(row)
	if err != nil {
		return tuple.DNull(), err
	}
	if best.Type == tuple.TypeNull {
		return tuple.DNull(), nil
	}
	bestF, ok := toFloat64Func(best)
	if !ok {
		return tuple.DNull(), fmt.Errorf("greatest/least: non-numeric argument")
	}
	for _, a := range args[1:] {
		val, err := a.Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		f, ok := toFloat64Func(val)
		if !ok {
			return tuple.DNull(), fmt.Errorf("greatest/least: non-numeric argument")
		}
		if (least && f < bestF) || (!least && f > bestF) {
			bestF = f
			best = val
		}
	}
	return best, nil
}
