package planner

import (
	cryptoRand "crypto/rand"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gololadb/loladb/pkg/tuple"
)

// evalBuiltinFunc evaluates a built-in SQL function by name.
func evalBuiltinFunc(name string, args []AnalyzedExpr, row *Row) (tuple.Datum, error) {
	switch strings.ToLower(name) {
	case "now", "current_timestamp":
		return tuple.DTimestamp(time.Now().UTC().UnixMicro()), nil
	case "current_date":
		now := time.Now().UTC()
		days := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).Unix() / 86400
		return tuple.DDate(days), nil
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

	case "concat_ws":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("concat_ws requires at least 1 argument")
		}
		sepVal, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if sepVal.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		sep := datumToString(sepVal)
		var parts []string
		for _, a := range args[1:] {
			val, err := a.Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			if val.Type != tuple.TypeNull {
				parts = append(parts, datumToString(val))
			}
		}
		return tuple.DText(strings.Join(parts, sep)), nil

	case "initcap":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("initcap requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		s := datumToString(val)
		runes := []rune(s)
		wordStart := true
		for i, r := range runes {
			if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
				wordStart = true
			} else if wordStart {
				runes[i] = []rune(strings.ToUpper(string(r)))[0]
				wordStart = false
			} else {
				runes[i] = []rune(strings.ToLower(string(r)))[0]
			}
		}
		return tuple.DText(string(runes)), nil

	case "translate":
		if len(args) < 3 {
			return tuple.DNull(), fmt.Errorf("translate requires 3 arguments")
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
		s := datumToString(val)
		fromChars := []rune(datumToString(fromVal))
		toChars := []rune(datumToString(toVal))
		// Build mapping
		mapping := make(map[rune]rune)
		deleteSet := make(map[rune]bool)
		for i, r := range fromChars {
			if _, exists := mapping[r]; exists {
				continue // first occurrence wins
			}
			if i < len(toChars) {
				mapping[r] = toChars[i]
			} else {
				deleteSet[r] = true
			}
		}
		var sb strings.Builder
		for _, r := range s {
			if deleteSet[r] {
				continue
			}
			if rep, ok := mapping[r]; ok {
				sb.WriteRune(rep)
			} else {
				sb.WriteRune(r)
			}
		}
		return tuple.DText(sb.String()), nil

	case "ascii":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("ascii requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		s := datumToString(val)
		if len(s) == 0 {
			return tuple.DInt64(0), nil
		}
		return tuple.DInt64(int64([]rune(s)[0])), nil

	case "chr":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("chr requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		code, _ := toFloat64Func(val)
		return tuple.DText(string(rune(int(code)))), nil

	case "octet_length":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("octet_length requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		return tuple.DInt64(int64(len(datumToString(val)))), nil

	case "bit_length":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("bit_length requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		return tuple.DInt64(int64(len(datumToString(val))) * 8), nil

	case "md5":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("md5 requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		hash := md5.Sum([]byte(datumToString(val)))
		return tuple.DText(hex.EncodeToString(hash[:])), nil

	case "gen_random_uuid":
		var uuid [16]byte
		cryptoRand.Read(uuid[:])
		uuid[6] = (uuid[6] & 0x0f) | 0x40 // version 4
		uuid[8] = (uuid[8] & 0x3f) | 0x80 // variant 10
		return tuple.DUUID(fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
			uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])), nil

	case "overlay":
		// overlay(str, replacement, start [, count])
		if len(args) < 3 {
			return tuple.DNull(), fmt.Errorf("overlay requires 3 or 4 arguments")
		}
		strVal, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if strVal.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		replVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		startVal, err := args[2].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		s := []rune(datumToString(strVal))
		repl := []rune(datumToString(replVal))
		start := int(startVal.I64) - 1 // 1-based
		if start < 0 {
			start = 0
		}
		count := len(repl) // default: replace length of replacement
		if len(args) >= 4 {
			countVal, err := args[3].Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			count = int(countVal.I64)
		}
		if start > len(s) {
			start = len(s)
		}
		end := start + count
		if end > len(s) {
			end = len(s)
		}
		var sb strings.Builder
		sb.WriteString(string(s[:start]))
		sb.WriteString(string(repl))
		sb.WriteString(string(s[end:]))
		return tuple.DText(sb.String()), nil

	case "extract", "date_part":
		// extract(field, source) — field is a text constant like 'year'
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("%s requires 2 arguments", name)
		}
		fieldVal, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		srcVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if srcVal.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		field := strings.ToLower(datumToString(fieldVal))
		src := datumToString(srcVal)
		t, err := parseTimestamp(src)
		if err != nil {
			return tuple.DNull(), fmt.Errorf("%s: cannot parse timestamp %q: %v", name, src, err)
		}
		return extractField(field, t)

	case "date_trunc":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("date_trunc requires 2 arguments")
		}
		fieldVal, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		srcVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if srcVal.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		field := strings.ToLower(datumToString(fieldVal))
		src := datumToString(srcVal)
		t, err := parseTimestamp(src)
		if err != nil {
			return tuple.DNull(), fmt.Errorf("date_trunc: cannot parse timestamp %q: %v", src, err)
		}
		return truncTimestamp(field, t)

	case "age":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("age requires 1 or 2 arguments")
		}
		var t1, t2 time.Time
		v1, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if v1.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		t1, err = parseTimestamp(datumToString(v1))
		if err != nil {
			return tuple.DNull(), fmt.Errorf("age: cannot parse timestamp: %v", err)
		}
		if len(args) >= 2 {
			v2, err := args[1].Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			if v2.Type == tuple.TypeNull {
				return tuple.DNull(), nil
			}
			t2, err = parseTimestamp(datumToString(v2))
			if err != nil {
				return tuple.DNull(), fmt.Errorf("age: cannot parse timestamp: %v", err)
			}
		} else {
			t2 = t1
			t1 = time.Now().UTC()
		}
		return ageToInterval(t1, t2), nil

	case "regexp_replace":
		// regexp_replace(source, pattern, replacement [, flags])
		if len(args) < 3 {
			return tuple.DNull(), fmt.Errorf("regexp_replace requires 3 or 4 arguments")
		}
		srcVal, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if srcVal.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		patVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		replVal, err := args[2].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		flags := ""
		if len(args) >= 4 {
			fVal, err := args[3].Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			flags = datumToString(fVal)
		}
		pattern := datumToString(patVal)
		if strings.Contains(flags, "i") {
			pattern = "(?i)" + pattern
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return tuple.DNull(), fmt.Errorf("regexp_replace: invalid pattern: %v", err)
		}
		src := datumToString(srcVal)
		repl := datumToString(replVal)
		if strings.Contains(flags, "g") {
			return tuple.DText(re.ReplaceAllString(src, repl)), nil
		}
		// Default: replace first occurrence only
		loc := re.FindStringIndex(src)
		if loc == nil {
			return tuple.DText(src), nil
		}
		matched := src[loc[0]:loc[1]]
		expanded := re.ReplaceAllString(matched, repl)
		return tuple.DText(src[:loc[0]] + expanded + src[loc[1]:]), nil

	case "regexp_match":
		// regexp_match(source, pattern [, flags]) — returns first match as text (simplified)
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("regexp_match requires 2 or 3 arguments")
		}
		srcVal, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if srcVal.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		patVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		flags := ""
		if len(args) >= 3 {
			fVal, err := args[2].Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			flags = datumToString(fVal)
		}
		pattern := datumToString(patVal)
		if strings.Contains(flags, "i") {
			pattern = "(?i)" + pattern
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return tuple.DNull(), fmt.Errorf("regexp_match: invalid pattern: %v", err)
		}
		matches := re.FindStringSubmatch(datumToString(srcVal))
		if matches == nil {
			return tuple.DNull(), nil
		}
		// If there are capture groups, return the first group; otherwise the whole match
		if len(matches) > 1 {
			return tuple.DText(matches[1]), nil
		}
		return tuple.DText(matches[0]), nil

	case "to_char":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("to_char requires 2 arguments")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		fmtVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		pgFmt := datumToString(fmtVal)
		// Numeric to_char
		if val.Type == tuple.TypeInt32 || val.Type == tuple.TypeInt64 || val.Type == tuple.TypeFloat64 {
			f, _ := toFloat64Func(val)
			return tuple.DText(pgNumericToChar(f, pgFmt)), nil
		}
		// Timestamp to_char
		t, err := parseTimestamp(datumToString(val))
		if err != nil {
			// Fallback: treat as string
			return tuple.DText(datumToString(val)), nil
		}
		return tuple.DText(pgTimestampToChar(t, pgFmt)), nil

	case "to_number":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("to_number requires 2 arguments")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		// Strip non-numeric characters based on format, then parse
		s := datumToString(val)
		s = strings.ReplaceAll(s, ",", "")
		s = strings.ReplaceAll(s, "$", "")
		s = strings.ReplaceAll(s, " ", "")
		var f float64
		if _, err := fmt.Sscanf(s, "%f", &f); err != nil {
			return tuple.DNull(), fmt.Errorf("to_number: invalid input %q", datumToString(val))
		}
		return tuple.DFloat64(f), nil

	case "to_date":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("to_date requires 2 arguments")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		fmtVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		goFmt := pgDateFormatToGo(datumToString(fmtVal))
		t, err := time.Parse(goFmt, datumToString(val))
		if err != nil {
			return tuple.DNull(), fmt.Errorf("to_date: cannot parse %q with format %q: %v", datumToString(val), datumToString(fmtVal), err)
		}
		return tuple.DDate(t.Unix() / 86400), nil

	case "to_timestamp":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("to_timestamp requires 1 or 2 arguments")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		// Single-arg: epoch seconds
		if len(args) == 1 {
			f, ok := toFloat64Func(val)
			if !ok {
				return tuple.DNull(), fmt.Errorf("to_timestamp: non-numeric argument")
			}
			sec := int64(f)
			nsec := int64((f - float64(sec)) * 1e9)
			t := time.Unix(sec, nsec).UTC()
			return tuple.DTimestamp(t.UnixMicro()), nil
		}
		// Two-arg: string + format
		fmtVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		goFmt := pgDateFormatToGo(datumToString(fmtVal))
		t, err := time.Parse(goFmt, datumToString(val))
		if err != nil {
			return tuple.DNull(), fmt.Errorf("to_timestamp: cannot parse %q with format %q: %v", datumToString(val), datumToString(fmtVal), err)
		}
		return tuple.DTimestamp(t.UnixMicro()), nil

	case "encode":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("encode requires 2 arguments")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		fmtVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		data := []byte(datumToString(val))
		switch strings.ToLower(datumToString(fmtVal)) {
		case "hex":
			return tuple.DText(hex.EncodeToString(data)), nil
		case "base64":
			return tuple.DText(base64.StdEncoding.EncodeToString(data)), nil
		case "escape":
			return tuple.DText(string(data)), nil
		default:
			return tuple.DNull(), fmt.Errorf("encode: unsupported format %q", datumToString(fmtVal))
		}

	case "decode":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("decode requires 2 arguments")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		fmtVal, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		s := datumToString(val)
		switch strings.ToLower(datumToString(fmtVal)) {
		case "hex":
			decoded, err := hex.DecodeString(s)
			if err != nil {
				return tuple.DNull(), fmt.Errorf("decode: invalid hex: %v", err)
			}
			return tuple.DText(string(decoded)), nil
		case "base64":
			decoded, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return tuple.DNull(), fmt.Errorf("decode: invalid base64: %v", err)
			}
			return tuple.DText(string(decoded)), nil
		case "escape":
			return tuple.DText(s), nil
		default:
			return tuple.DNull(), fmt.Errorf("decode: unsupported format %q", datumToString(fmtVal))
		}

	case "format":
		// format(formatstr, val1, val2, ...) — simplified PG format()
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("format requires at least 1 argument")
		}
		fmtVal, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if fmtVal.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		fmtStr := datumToString(fmtVal)
		// Evaluate remaining args
		vals := make([]string, len(args)-1)
		for i, a := range args[1:] {
			v, err := a.Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			vals[i] = datumToString(v)
		}
		// Replace %s, %I, %L with positional args (simplified)
		result := pgFormat(fmtStr, vals)
		return tuple.DText(result), nil

	case "string_to_array":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("string_to_array requires 2 or 3 arguments")
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
		s := datumToString(val)
		delim := datumToString(delimVal)
		parts := strings.Split(s, delim)
		// Optional null-string argument
		if len(args) >= 3 {
			nullVal, err := args[2].Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			nullStr := datumToString(nullVal)
			for i, p := range parts {
				if p == nullStr {
					parts[i] = "NULL"
				}
			}
		}
		return tuple.DText("{" + strings.Join(parts, ",") + "}"), nil

	case "array_length":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("array_length requires 2 arguments")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		s := datumToString(val)
		// Parse {a,b,c} format
		s = strings.TrimPrefix(s, "{")
		s = strings.TrimSuffix(s, "}")
		if s == "" {
			return tuple.DInt64(0), nil
		}
		return tuple.DInt64(int64(len(strings.Split(s, ",")))), nil

	// --- JSON functions ---
	case "json_extract_path_text", "jsonb_extract_path_text":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("%s requires at least 2 arguments", name)
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DNull(), nil
		}
		jsonStr := datumToString(val)
		var obj interface{}
		if err := json.Unmarshal([]byte(jsonStr), &obj); err != nil {
			return tuple.DNull(), fmt.Errorf("%s: invalid JSON: %v", name, err)
		}
		// Walk the path.
		current := obj
		for i := 1; i < len(args); i++ {
			keyVal, err := args[i].Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			key := datumToString(keyVal)
			m, ok := current.(map[string]interface{})
			if !ok {
				return tuple.DNull(), nil
			}
			current, ok = m[key]
			if !ok {
				return tuple.DNull(), nil
			}
		}
		if current == nil {
			return tuple.DNull(), nil
		}
		return tuple.DText(fmt.Sprintf("%v", current)), nil

	case "json_array_length", "jsonb_array_length":
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
		var arr []interface{}
		if err := json.Unmarshal([]byte(datumToString(val)), &arr); err != nil {
			return tuple.DNull(), fmt.Errorf("%s: not a JSON array", name)
		}
		return tuple.DInt64(int64(len(arr))), nil

	case "json_typeof", "jsonb_typeof":
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
		var obj interface{}
		if err := json.Unmarshal([]byte(datumToString(val)), &obj); err != nil {
			return tuple.DNull(), fmt.Errorf("%s: invalid JSON", name)
		}
		switch obj.(type) {
		case map[string]interface{}:
			return tuple.DText("object"), nil
		case []interface{}:
			return tuple.DText("array"), nil
		case string:
			return tuple.DText("string"), nil
		case float64:
			return tuple.DText("number"), nil
		case bool:
			return tuple.DText("boolean"), nil
		case nil:
			return tuple.DText("null"), nil
		default:
			return tuple.DText("unknown"), nil
		}

	case "json_build_object", "jsonb_build_object":
		if len(args)%2 != 0 {
			return tuple.DNull(), fmt.Errorf("%s requires an even number of arguments", name)
		}
		m := make(map[string]interface{})
		for i := 0; i < len(args); i += 2 {
			kv, err := args[i].Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			vv, err := args[i+1].Eval(row)
			if err != nil {
				return tuple.DNull(), err
			}
			key := datumToString(kv)
			m[key] = datumToJSONValue(vv)
		}
		b, _ := json.Marshal(m)
		return tuple.DJSON(string(b)), nil

	case "row_to_json":
		// Simplified: convert a text representation to JSON.
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("row_to_json requires 1 argument")
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		return tuple.DJSON(datumToString(val)), nil

	case "to_json", "to_jsonb":
		if len(args) < 1 {
			return tuple.DNull(), fmt.Errorf("%s requires 1 argument", name)
		}
		val, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if val.Type == tuple.TypeNull {
			return tuple.DJSON("null"), nil
		}
		v := datumToJSONValue(val)
		b, _ := json.Marshal(v)
		return tuple.DJSON(string(b)), nil

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

	case "is_distinct_from":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("is_distinct_from requires 2 arguments")
		}
		lv, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		rv, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if lv.Type == tuple.TypeNull && rv.Type == tuple.TypeNull {
			return tuple.DBool(false), nil
		}
		if lv.Type == tuple.TypeNull || rv.Type == tuple.TypeNull {
			return tuple.DBool(true), nil
		}
		return tuple.DBool(CompareDatums(lv, rv) != 0), nil

	case "is_not_distinct_from":
		if len(args) < 2 {
			return tuple.DNull(), fmt.Errorf("is_not_distinct_from requires 2 arguments")
		}
		lv, err := args[0].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		rv, err := args[1].Eval(row)
		if err != nil {
			return tuple.DNull(), err
		}
		if lv.Type == tuple.TypeNull && rv.Type == tuple.TypeNull {
			return tuple.DBool(true), nil
		}
		if lv.Type == tuple.TypeNull || rv.Type == tuple.TypeNull {
			return tuple.DBool(false), nil
		}
		return tuple.DBool(CompareDatums(lv, rv) == 0), nil

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
	case tuple.TypeDate:
		return time.Unix(d.I64*86400, 0).UTC().Format("2006-01-02")
	case tuple.TypeTimestamp:
		return time.Unix(0, d.I64*1000).UTC().Format("2006-01-02 15:04:05")
	case tuple.TypeNumeric, tuple.TypeJSON, tuple.TypeUUID, tuple.TypeBytea, tuple.TypeArray:
		return d.Text
	case tuple.TypeInterval:
		return FormatInterval(d.I32, d.I64)
	case tuple.TypeMoney:
		dollars := d.I64 / 100
		cents := d.I64 % 100
		if cents < 0 {
			cents = -cents
		}
		return fmt.Sprintf("$%d.%02d", dollars, cents)
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
	case tuple.TypeDate:
		s := datumToString(val)
		t, err := parseTimestamp(s)
		if err != nil {
			return tuple.DNull(), fmt.Errorf("invalid input syntax for date: %q", s)
		}
		days := t.Unix() / 86400
		return tuple.DDate(days), nil
	case tuple.TypeTimestamp:
		s := datumToString(val)
		t, err := parseTimestamp(s)
		if err != nil {
			return tuple.DNull(), fmt.Errorf("invalid input syntax for timestamp: %q", s)
		}
		us := t.UnixMicro()
		return tuple.DTimestamp(us), nil
	case tuple.TypeNumeric:
		s := datumToString(val)
		// Validate it's a valid number.
		s = strings.TrimSpace(s)
		if s == "" {
			return tuple.DNull(), fmt.Errorf("invalid input syntax for numeric: %q", s)
		}
		return tuple.DNumeric(s), nil
	case tuple.TypeJSON:
		s := datumToString(val)
		return tuple.DJSON(s), nil
	case tuple.TypeUUID:
		s := strings.TrimSpace(datumToString(val))
		return tuple.DUUID(s), nil
	case tuple.TypeInterval:
		if val.Type == tuple.TypeInterval {
			return val, nil
		}
		s := datumToString(val)
		months, us, err := parseInterval(s)
		if err != nil {
			return tuple.DNull(), fmt.Errorf("invalid input syntax for interval: %q", s)
		}
		return tuple.DInterval(months, us), nil
	case tuple.TypeBytea:
		if val.Type == tuple.TypeBytea {
			return val, nil
		}
		s := datumToString(val)
		// Accept \x hex format or raw text.
		if strings.HasPrefix(s, "\\x") {
			return tuple.DBytea(s), nil
		}
		// Encode raw text as hex.
		return tuple.DBytea("\\x" + hex.EncodeToString([]byte(s))), nil
	case tuple.TypeArray:
		if val.Type == tuple.TypeArray {
			return val, nil
		}
		return tuple.DArray(datumToString(val)), nil
	case tuple.TypeMoney:
		if val.Type == tuple.TypeMoney {
			return val, nil
		}
		s := strings.TrimSpace(datumToString(val))
		// Strip currency symbol and commas.
		s = strings.TrimPrefix(s, "$")
		s = strings.ReplaceAll(s, ",", "")
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return tuple.DNull(), fmt.Errorf("invalid input syntax for money: %q", s)
		}
		cents := int64(math.Round(f * 100))
		return tuple.DMoney(cents), nil
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

// datumToJSONValue converts a datum to a Go value suitable for json.Marshal.
func datumToJSONValue(d tuple.Datum) interface{} {
	switch d.Type {
	case tuple.TypeNull:
		return nil
	case tuple.TypeBool:
		return d.Bool
	case tuple.TypeInt32:
		return d.I32
	case tuple.TypeInt64:
		return d.I64
	case tuple.TypeFloat64:
		return d.F64
	case tuple.TypeJSON:
		var v interface{}
		if err := json.Unmarshal([]byte(d.Text), &v); err == nil {
			return v
		}
		return d.Text
	default:
		return datumToString(d)
	}
}

// parseTimestamp tries several common timestamp/date formats.
func parseTimestamp(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	formats := []string{
		"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05",
		"2006-01-02",
		"01/02/2006",
		"Jan 2, 2006",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp format: %q", s)
}

// extractField extracts a date/time field from a time.Time.
func extractField(field string, t time.Time) (tuple.Datum, error) {
	switch field {
	case "year":
		return tuple.DFloat64(float64(t.Year())), nil
	case "month":
		return tuple.DFloat64(float64(t.Month())), nil
	case "day":
		return tuple.DFloat64(float64(t.Day())), nil
	case "hour":
		return tuple.DFloat64(float64(t.Hour())), nil
	case "minute":
		return tuple.DFloat64(float64(t.Minute())), nil
	case "second":
		return tuple.DFloat64(float64(t.Second()) + float64(t.Nanosecond())/1e9), nil
	case "dow":
		return tuple.DFloat64(float64(t.Weekday())), nil
	case "doy":
		return tuple.DFloat64(float64(t.YearDay())), nil
	case "week":
		_, w := t.ISOWeek()
		return tuple.DFloat64(float64(w)), nil
	case "quarter":
		return tuple.DFloat64(float64((t.Month()-1)/3 + 1)), nil
	case "epoch":
		return tuple.DFloat64(float64(t.Unix()) + float64(t.Nanosecond())/1e9), nil
	case "milliseconds", "millisecond":
		return tuple.DFloat64(float64(t.Second())*1000 + float64(t.Nanosecond())/1e6), nil
	case "microseconds", "microsecond":
		return tuple.DFloat64(float64(t.Second())*1e6 + float64(t.Nanosecond())/1e3), nil
	default:
		return tuple.DNull(), fmt.Errorf("extract: unsupported field %q", field)
	}
}

// truncTimestamp truncates a timestamp to the given precision.
func truncTimestamp(field string, t time.Time) (tuple.Datum, error) {
	var result time.Time
	switch field {
	case "microseconds", "microsecond":
		result = t.Truncate(time.Microsecond)
	case "milliseconds", "millisecond":
		result = t.Truncate(time.Millisecond)
	case "second":
		result = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, t.Location())
	case "minute":
		result = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
	case "hour":
		result = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	case "day":
		result = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case "week":
		// Truncate to Monday of the ISO week
		weekday := int(t.Weekday())
		if weekday == 0 {
			weekday = 7
		}
		result = time.Date(t.Year(), t.Month(), t.Day()-(weekday-1), 0, 0, 0, 0, t.Location())
	case "month":
		result = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	case "quarter":
		q := (t.Month() - 1) / 3
		result = time.Date(t.Year(), q*3+1, 1, 0, 0, 0, 0, t.Location())
	case "year":
		result = time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
	default:
		return tuple.DNull(), fmt.Errorf("date_trunc: unsupported field %q", field)
	}
	return tuple.DTimestamp(result.UnixMicro()), nil
}

// formatAge formats the difference between two timestamps as a PG-style interval string.
func formatAge(t1, t2 time.Time) string {
	if t1.Before(t2) {
		return "-" + formatAge(t2, t1)
	}
	years := t1.Year() - t2.Year()
	months := int(t1.Month()) - int(t2.Month())
	days := t1.Day() - t2.Day()
	if days < 0 {
		months--
		// Days in previous month
		prev := time.Date(t1.Year(), t1.Month(), 0, 0, 0, 0, 0, t1.Location())
		days += prev.Day()
	}
	if months < 0 {
		years--
		months += 12
	}
	var parts []string
	if years != 0 {
		parts = append(parts, fmt.Sprintf("%d years", years))
	}
	if months != 0 {
		parts = append(parts, fmt.Sprintf("%d mons", months))
	}
	if days != 0 {
		parts = append(parts, fmt.Sprintf("%d days", days))
	}
	if len(parts) == 0 {
		return "0 days"
	}
	return strings.Join(parts, " ")
}

// ageToInterval computes the difference between two timestamps as a native interval.
func ageToInterval(t1, t2 time.Time) tuple.Datum {
	negative := t1.Before(t2)
	if negative {
		t1, t2 = t2, t1
	}
	years := t1.Year() - t2.Year()
	months := int(t1.Month()) - int(t2.Month())
	days := t1.Day() - t2.Day()
	if days < 0 {
		months--
		prev := time.Date(t1.Year(), t1.Month(), 0, 0, 0, 0, 0, t1.Location())
		days += prev.Day()
	}
	if months < 0 {
		years--
		months += 12
	}
	totalMonths := int32(years*12 + months)
	us := int64(days) * 24 * 3600 * 1e6
	if negative {
		totalMonths = -totalMonths
		us = -us
	}
	return tuple.DInterval(totalMonths, us)
}

// pgDateFormatToGo converts a PG date format string to a Go time layout.
func pgDateFormatToGo(pgFmt string) string {
	r := strings.NewReplacer(
		"YYYY", "2006", "YY", "06",
		"MM", "01", "DD", "02",
		"HH24", "15", "HH12", "03", "HH", "03",
		"MI", "04", "SS", "05",
		"MS", "000", "US", "000000",
		"AM", "PM", "am", "pm",
		"TZ", "MST",
		"Month", "January", "Mon", "Jan",
		"Day", "Monday", "Dy", "Mon",
		"D", "2", // day of week
	)
	return r.Replace(pgFmt)
}

// pgTimestampToChar formats a time.Time using a PG-style format string.
func pgTimestampToChar(t time.Time, pgFmt string) string {
	// Replace PG tokens with Go layout tokens, then format.
	goFmt := pgDateFormatToGo(pgFmt)
	result := t.Format(goFmt)
	// Handle tokens that Go doesn't support natively.
	result = strings.ReplaceAll(result, "Q", fmt.Sprintf("%d", (t.Month()-1)/3+1))
	return result
}

// pgNumericToChar formats a number using a PG-style numeric format string.
func pgNumericToChar(f float64, pgFmt string) string {
	// Count decimal places from format
	dotIdx := strings.Index(pgFmt, ".")
	if dotIdx < 0 {
		// No decimal: format as integer
		return fmt.Sprintf("%d", int64(f))
	}
	decimals := 0
	for _, c := range pgFmt[dotIdx+1:] {
		if c == '9' || c == '0' {
			decimals++
		}
	}
	result := fmt.Sprintf("%.*f", decimals, f)
	// Handle comma grouping if format contains commas
	if strings.Contains(pgFmt, ",") {
		parts := strings.SplitN(result, ".", 2)
		intPart := parts[0]
		negative := false
		if len(intPart) > 0 && intPart[0] == '-' {
			negative = true
			intPart = intPart[1:]
		}
		var grouped []byte
		for i, j := len(intPart)-1, 0; i >= 0; i, j = i-1, j+1 {
			if j > 0 && j%3 == 0 {
				grouped = append(grouped, ',')
			}
			grouped = append(grouped, intPart[i])
		}
		// Reverse
		for i, j := 0, len(grouped)-1; i < j; i, j = i+1, j-1 {
			grouped[i], grouped[j] = grouped[j], grouped[i]
		}
		if negative {
			result = "-" + string(grouped)
		} else {
			result = string(grouped)
		}
		if len(parts) > 1 {
			result += "." + parts[1]
		}
	}
	return result
}

// pgFormat implements a simplified PG format() function.
// Supports %s (string), %I (identifier), %L (literal), and %% (literal %).
func pgFormat(fmtStr string, vals []string) string {
	var sb strings.Builder
	argIdx := 0
	i := 0
	for i < len(fmtStr) {
		if fmtStr[i] == '%' && i+1 < len(fmtStr) {
			next := fmtStr[i+1]
			switch next {
			case 's':
				if argIdx < len(vals) {
					sb.WriteString(vals[argIdx])
					argIdx++
				}
				i += 2
			case 'I':
				if argIdx < len(vals) {
					sb.WriteByte('"')
					sb.WriteString(strings.ReplaceAll(vals[argIdx], "\"", "\"\""))
					sb.WriteByte('"')
					argIdx++
				}
				i += 2
			case 'L':
				if argIdx < len(vals) {
					sb.WriteByte('\'')
					sb.WriteString(strings.ReplaceAll(vals[argIdx], "'", "''"))
					sb.WriteByte('\'')
					argIdx++
				}
				i += 2
			case '%':
				sb.WriteByte('%')
				i += 2
			default:
				sb.WriteByte(fmtStr[i])
				i++
			}
		} else {
			sb.WriteByte(fmtStr[i])
			i++
		}
	}
	return sb.String()
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

// ---------------------------------------------------------------------------
// Interval parsing and formatting
// ---------------------------------------------------------------------------

// parseInterval parses a PostgreSQL interval string like '1 year 2 months 3 days 04:05:06'
// into months (int32) and microseconds (int64).
func parseInterval(s string) (months int32, microseconds int64, err error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, 0, fmt.Errorf("empty interval string")
	}

	// Try HH:MM:SS or HH:MM:SS.ffffff format first (standalone time).
	if us, ok := tryParseIntervalTime(s); ok {
		return 0, us, nil
	}

	// Parse "N unit" pairs, with optional trailing time component.
	parts := strings.Fields(s)
	i := 0
	for i < len(parts) {
		// Check if this part is a time component (contains ':').
		if strings.Contains(parts[i], ":") {
			us, ok := tryParseIntervalTime(parts[i])
			if !ok {
				return 0, 0, fmt.Errorf("invalid interval time component: %q", parts[i])
			}
			microseconds += us
			i++
			continue
		}

		// Expect a number followed by a unit.
		if i+1 >= len(parts) {
			return 0, 0, fmt.Errorf("interval: missing unit for value %q", parts[i])
		}
		val, ferr := strconv.ParseFloat(parts[i], 64)
		if ferr != nil {
			return 0, 0, fmt.Errorf("interval: invalid number %q", parts[i])
		}
		unit := strings.ToLower(strings.TrimSuffix(parts[i+1], ","))
		i += 2

		switch {
		case strings.HasPrefix(unit, "year"):
			months += int32(val) * 12
		case strings.HasPrefix(unit, "mon"):
			months += int32(val)
		case strings.HasPrefix(unit, "week"):
			microseconds += int64(val * 7 * 24 * 3600 * 1e6)
		case strings.HasPrefix(unit, "day"):
			microseconds += int64(val * 24 * 3600 * 1e6)
		case strings.HasPrefix(unit, "hour"):
			microseconds += int64(val * 3600 * 1e6)
		case strings.HasPrefix(unit, "min"):
			microseconds += int64(val * 60 * 1e6)
		case strings.HasPrefix(unit, "sec"):
			microseconds += int64(val * 1e6)
		case strings.HasPrefix(unit, "millisec"):
			microseconds += int64(val * 1e3)
		case strings.HasPrefix(unit, "microsec"):
			microseconds += int64(val)
		default:
			return 0, 0, fmt.Errorf("interval: unknown unit %q", unit)
		}
	}
	return months, microseconds, nil
}

// tryParseIntervalTime tries to parse a time component like "04:05:06" or "04:05:06.123456".
func tryParseIntervalTime(s string) (int64, bool) {
	if !strings.Contains(s, ":") {
		return 0, false
	}
	negative := false
	if strings.HasPrefix(s, "-") {
		negative = true
		s = s[1:]
	}
	timeParts := strings.Split(s, ":")
	if len(timeParts) < 2 || len(timeParts) > 3 {
		return 0, false
	}
	hours, err := strconv.Atoi(timeParts[0])
	if err != nil {
		return 0, false
	}
	mins, err := strconv.Atoi(timeParts[1])
	if err != nil {
		return 0, false
	}
	var secs float64
	if len(timeParts) == 3 {
		secs, err = strconv.ParseFloat(timeParts[2], 64)
		if err != nil {
			return 0, false
		}
	}
	us := int64(hours)*3600*1e6 + int64(mins)*60*1e6 + int64(secs*1e6)
	if negative {
		us = -us
	}
	return us, true
}

// FormatInterval formats an interval (months, microseconds) as a PostgreSQL-style string.
func FormatInterval(months int32, microseconds int64) string {
	var parts []string
	if months != 0 {
		years := months / 12
		mons := months % 12
		if years != 0 {
			parts = append(parts, fmt.Sprintf("%d years", years))
		}
		if mons != 0 {
			parts = append(parts, fmt.Sprintf("%d mons", mons))
		}
	}

	// Break microseconds into days + time.
	totalUS := microseconds
	days := totalUS / (24 * 3600 * 1e6)
	totalUS -= days * 24 * 3600 * 1e6
	if days != 0 {
		parts = append(parts, fmt.Sprintf("%d days", days))
	}

	if totalUS != 0 || len(parts) == 0 {
		negative := totalUS < 0
		if negative {
			totalUS = -totalUS
		}
		hours := totalUS / (3600 * 1e6)
		totalUS -= hours * 3600 * 1e6
		mins := totalUS / (60 * 1e6)
		totalUS -= mins * 60 * 1e6
		secs := totalUS / 1e6
		totalUS -= secs * 1e6

		timeStr := ""
		if totalUS > 0 {
			timeStr = fmt.Sprintf("%02d:%02d:%02d.%06d", hours, mins, secs, totalUS)
			// Trim trailing zeros from fractional seconds.
			timeStr = strings.TrimRight(timeStr, "0")
		} else {
			timeStr = fmt.Sprintf("%02d:%02d:%02d", hours, mins, secs)
		}
		if negative {
			timeStr = "-" + timeStr
		}
		parts = append(parts, timeStr)
	}

	return strings.Join(parts, " ")
}

