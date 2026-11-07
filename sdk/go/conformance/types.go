package conformance

import (
	"bytes"
	"database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// typeCase is a column of the types check: a logical type, and the edge
// values it writes, one a row; nil is null.
type typeCase struct {
	logical string
	field   arrow.Field
	values  []any
}

func (c typeCase) column() string { return "t_" + c.logical }

func extension(name string) arrow.Metadata {
	return arrow.NewMetadata([]string{"ARROW:extension:name", "ARROW:extension:metadata"}, []string{name, ""})
}

func at(layout, s string) time.Time {
	t, err := time.Parse(layout, s)
	if err != nil {
		panic(err)
	}
	return t
}

// typeCases are ADR-0014's logical types, with the values the engine
// hands a destination at their edges: infinities clamped to the types'
// limits, times in microseconds.
func typeCases(nanos bool) []typeCase {
	const ts = "2006-01-02 15:04:05.999999999"
	f32 := func(f float32) float32 { return f }
	cases := []typeCase{
		{"boolean", arrow.Field{Type: arrow.FixedWidthTypes.Boolean}, []any{true, false}},
		{"int16", arrow.Field{Type: arrow.PrimitiveTypes.Int16}, []any{int16(math.MinInt16), int16(math.MaxInt16), int16(0)}},
		{"int32", arrow.Field{Type: arrow.PrimitiveTypes.Int32}, []any{int32(math.MinInt32), int32(math.MaxInt32), int32(-1)}},
		{"int64", arrow.Field{Type: arrow.PrimitiveTypes.Int64}, []any{int64(math.MinInt64), int64(math.MaxInt64), int64(1<<53 + 1)}},
		{"float32", arrow.Field{Type: arrow.PrimitiveTypes.Float32}, []any{
			f32(float32(math.NaN())), f32(float32(math.Inf(1))), f32(float32(math.Inf(-1))), f32(math.MaxFloat32), f32(math.SmallestNonzeroFloat32), f32(0.1),
		}},
		{"float64", arrow.Field{Type: arrow.PrimitiveTypes.Float64}, []any{
			math.NaN(), math.Inf(1), math.Inf(-1), math.MaxFloat64, math.SmallestNonzeroFloat64, 0.1,
		}},
		{"string", arrow.Field{Type: arrow.BinaryTypes.String}, []any{
			"", "héllo ✓ 𝄞 日本", `a "quote", a \ and a '`, "line\nbreak\ttab\r", strings.Repeat("long ", 2000),
		}},
		{"binary", arrow.Field{Type: arrow.BinaryTypes.Binary}, []any{[]byte{}, allBytes(), []byte{0}}},
		{"decimal", arrow.Field{Type: &arrow.Decimal128Type{Precision: 38, Scale: 9}}, []any{
			"99999999999999999999999999999.999999999", "-99999999999999999999999999999.999999999", "0.000000001", "0", "-12.5",
		}},
		{"decimal_10_2", arrow.Field{Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}}, []any{"12345678.90", "-0.01", "99999999.99"}},
		{"timestamp_tz", arrow.Field{Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}}, []any{
			at(ts, "0001-01-01 00:00:00"), at(ts, "9999-12-31 23:59:59.999999"), at(ts, "1970-01-01 00:00:00"), at(ts, "2026-11-08 12:34:56.789012"), at(ts, "1969-12-31 23:59:59.999999"),
		}},
		{"timestamp", arrow.Field{Type: &arrow.TimestampType{Unit: arrow.Microsecond}}, []any{
			at(ts, "0001-01-01 00:00:00"), at(ts, "9999-12-31 23:59:59.999999"), at(ts, "1970-01-01 00:00:00"), at(ts, "2026-03-29 02:30:00.000001"),
		}},
		{"date", arrow.Field{Type: arrow.FixedWidthTypes.Date32}, []any{
			at(ts, "0001-01-01 00:00:00"), at(ts, "9999-12-31 00:00:00"), at(ts, "1970-01-01 00:00:00"), at(ts, "2024-02-29 00:00:00"),
		}},
		{"time", arrow.Field{Type: arrow.FixedWidthTypes.Time64us}, []any{
			time.Duration(0), 24*time.Hour - time.Microsecond, 12*time.Hour + 34*time.Minute + 56*time.Second + 789012*time.Microsecond,
		}},
		{"interval", arrow.Field{Type: arrow.FixedWidthTypes.MonthDayNanoInterval}, []any{
			arrow.MonthDayNanoInterval{Months: 1, Days: 2, Nanoseconds: int64(3*time.Hour + 4*time.Minute + 5*time.Second + 6*time.Microsecond)},
			arrow.MonthDayNanoInterval{Days: -1}, arrow.MonthDayNanoInterval{}, arrow.MonthDayNanoInterval{Months: 14, Nanoseconds: -int64(time.Microsecond)},
		}},
		{"list", arrow.Field{Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)}, []any{[]any{int64(1), nil, int64(3)}, []any{}, []any{int64(math.MaxInt64)}}},
		{"json", arrow.Field{Type: arrow.BinaryTypes.String, Metadata: extension("arrow.json")}, []any{
			`{"a": [1, 2, {"b": null}], "c": "✓", "d": true}`, `[]`, `"text"`, `null`, `123`,
		}},
		{"uuid", arrow.Field{Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: extension("arrow.uuid")}, []any{
			uuid.Nil, uuid.Max, uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
		}},
	}
	if nanos {
		cases = append(cases, typeCase{"timestamp_ns", arrow.Field{Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}}, []any{
			at(ts, "1677-09-21 00:12:43.145224192"), at(ts, "2262-04-11 23:47:16.854775807"), at(ts, "2026-11-08 12:34:56.123456789"),
		}})
	}
	for i := range cases {
		cases[i].field.Name, cases[i].field.Nullable = cases[i].column(), true
	}
	return cases
}

func allBytes() []byte {
	b := make([]byte, 256)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// typesRows is how many rows the types stream has: one per value of the
// longest case, and one of nulls.
func typesRows(cases []typeCase) int {
	n := 0
	for _, c := range cases {
		n = max(n, len(c.values))
	}
	return n + 1
}

// buildTypes is the types stream's batch: an id per row, and each case's
// values.
func buildTypes(stream string, cases []typeCase) (arrow.RecordBatch, error) {
	data := []arrow.Field{dataFields[0]}
	for _, c := range cases {
		data = append(data, c.field)
	}
	b := array.NewRecordBuilder(memory.DefaultAllocator, streamSchema(stream, data))
	defer b.Release()
	now := arrow.Timestamp(time.Now().UnixMicro())
	rows := typesRows(cases)
	m := len(data)
	for row := range rows {
		b.Field(0).(*array.Int64Builder).Append(int64(row))
		for i, c := range cases {
			var v any
			if row < len(c.values) {
				v = c.values[row]
			}
			if err := appendValue(b.Field(i+1), c.field.Type, v); err != nil {
				return nil, fmt.Errorf("%s: %w", c.column(), err)
			}
		}
		b.Field(m).(*array.FixedSizeBinaryBuilder).Append(rowID(stream, record{id: int64(row)}))
		b.Field(m + 1).(*array.StringBuilder).Append("insert")
		b.Field(m + 2).(*array.TimestampBuilder).Append(now)
		b.Field(m + 3).(*array.TimestampBuilder).Append(now)
	}
	return b.NewRecordBatch(), nil
}

func appendValue(b array.Builder, dt arrow.DataType, v any) error {
	if v == nil {
		b.AppendNull()
		return nil
	}
	switch b := b.(type) {
	case *array.BooleanBuilder:
		b.Append(v.(bool))
	case *array.Int16Builder:
		b.Append(v.(int16))
	case *array.Int32Builder:
		b.Append(v.(int32))
	case *array.Int64Builder:
		b.Append(v.(int64))
	case *array.Float32Builder:
		b.Append(v.(float32))
	case *array.Float64Builder:
		b.Append(v.(float64))
	case *array.StringBuilder:
		b.Append(v.(string))
	case *array.BinaryBuilder:
		b.Append(v.([]byte))
	case *array.FixedSizeBinaryBuilder:
		u := v.(uuid.UUID)
		b.Append(u[:])
	case *array.Decimal128Builder:
		t := dt.(*arrow.Decimal128Type)
		n, err := decimal128.FromString(v.(string), t.Precision, t.Scale)
		if err != nil {
			return err
		}
		b.Append(n)
	case *array.TimestampBuilder:
		t := v.(time.Time)
		if dt.(*arrow.TimestampType).Unit == arrow.Nanosecond {
			b.Append(arrow.Timestamp(t.UnixNano()))
		} else {
			b.Append(arrow.Timestamp(t.UnixMicro()))
		}
	case *array.Date32Builder:
		b.Append(arrow.Date32FromTime(v.(time.Time)))
	case *array.Time64Builder:
		b.Append(arrow.Time64(v.(time.Duration) / time.Microsecond))
	case *array.MonthDayNanoIntervalBuilder:
		b.Append(v.(arrow.MonthDayNanoInterval))
	case *array.ListBuilder:
		b.Append(true)
		vb := b.ValueBuilder().(*array.Int64Builder)
		for _, e := range v.([]any) {
			if e == nil {
				vb.AppendNull()
			} else {
				vb.Append(e.(int64))
			}
		}
	default:
		return fmt.Errorf("no value for a %T", b)
	}
	return nil
}

// want is a written value's canonical text, as norm makes what's read.
func (c typeCase) want(v any) string {
	if v == nil {
		return "null"
	}
	s, err := c.norm(v)
	if err != nil {
		panic(err)
	}
	return s
}

// norm is a value read back as the case's canonical text, from the forms
// DestinationSuite.Read documents; "null" for none.
func (c typeCase) norm(v any) (string, error) {
	v = unwrap(v)
	if v == nil {
		return "null", nil
	}
	bad := func() (string, error) { return "", fmt.Errorf("can't read a %T, %v, as %s", v, v, c.logical) }
	switch c.logical {
	case "boolean":
		switch x := v.(type) {
		case bool:
			return strconv.FormatBool(x), nil
		case string:
			switch strings.ToLower(x) {
			case "true", "t":
				return "true", nil
			case "false", "f":
				return "false", nil
			}
		}
	case "int16", "int32", "int64":
		if n, ok := integer(v); ok {
			return n.String(), nil
		}
	case "float32", "float64":
		bits := 64
		if c.logical == "float32" {
			bits = 32
		}
		if f, ok := float(v); ok {
			if bits == 32 {
				f = float64(float32(f))
			}
			return floatText(f, bits), nil
		}
	case "string":
		switch x := v.(type) {
		case string:
			return strconv.Quote(x), nil
		case []byte:
			return strconv.Quote(string(x)), nil
		}
	case "binary":
		switch x := v.(type) {
		case []byte:
			return hex.EncodeToString(x), nil
		case string:
			if h, ok := strings.CutPrefix(x, `\x`); ok {
				if _, err := hex.DecodeString(h); err == nil {
					return strings.ToLower(h), nil
				}
			}
			if b, err := base64.StdEncoding.Strict().DecodeString(x); err == nil {
				return hex.EncodeToString(b), nil
			}
			return hex.EncodeToString([]byte(x)), nil
		}
	case "decimal", "decimal_10_2":
		if r, ok := rational(v); ok {
			return r.FloatString(int(c.field.Type.(*arrow.Decimal128Type).Scale)), nil
		}
	case "timestamp_tz", "timestamp", "timestamp_ns":
		t, ok := timestamp(v)
		if !ok {
			return bad()
		}
		layout := "2006-01-02T15:04:05.000000"
		if c.logical == "timestamp_ns" {
			layout = "2006-01-02T15:04:05.000000000"
		}
		if c.logical != "timestamp" {
			return t.UTC().Format(layout + "Z"), nil
		}
		return t.Format(layout), nil
	case "date":
		if t, ok := timestamp(v); ok {
			return t.Format("2006-01-02"), nil
		}
	case "time":
		if d, ok := timeOfDay(v); ok {
			return (time.Time{}).Add(d).Format("15:04:05.000000"), nil
		}
	case "interval":
		if iv, ok := interval(v); ok {
			return fmt.Sprintf("%d months %d days %d ns", iv.Months, iv.Days, iv.Nanoseconds), nil
		}
	case "list":
		elems, ok := list(v)
		if !ok {
			return bad()
		}
		out := make([]string, len(elems))
		for i, e := range elems {
			if e = unwrap(e); e == nil {
				out[i] = "null"
			} else if n, ok := integer(e); ok {
				out[i] = n.String()
			} else {
				return bad()
			}
		}
		return "[" + strings.Join(out, ",") + "]", nil
	case "json":
		var b []byte
		switch x := v.(type) {
		case string:
			b = []byte(x)
		case []byte:
			b = x
		case json.RawMessage:
			b = x
		}
		// What isn't JSON text is a JSON value its reader decoded, such as
		// a string.
		if b == nil || !json.Valid(b) {
			var err error
			if b, err = json.Marshal(v); err != nil {
				return bad()
			}
		}
		return canonicalJSON(b), nil
	case "uuid":
		switch x := v.(type) {
		case string:
			if u, err := uuid.Parse(x); err == nil {
				return u.String(), nil
			}
		case []byte:
			if u, err := uuid.FromBytes(x); err == nil {
				return u.String(), nil
			}
		}
		if rv := reflect.ValueOf(v); rv.Kind() == reflect.Array && rv.Len() == 16 && rv.Type().Elem().Kind() == reflect.Uint8 {
			var u uuid.UUID
			reflect.Copy(reflect.ValueOf(u[:]), rv)
			return u.String(), nil
		}
	}
	return bad()
}

// unwrap turns pointers, a driver's Valid-flagged structs and
// driver.Valuers into what they hold; nil for null.
func unwrap(v any) any {
	for range 4 {
		if v == nil {
			return nil
		}
		rv := reflect.ValueOf(v)
		switch {
		case rv.Kind() == reflect.Pointer && !isBig(v):
			if rv.IsNil() {
				return nil
			}
			v = rv.Elem().Interface()
			continue
		case rv.Kind() == reflect.Struct:
			if f := rv.FieldByName("Valid"); f.IsValid() && f.Kind() == reflect.Bool && !f.Bool() {
				return nil
			}
		}
		switch x := v.(type) {
		case json.RawMessage:
			if len(x) == 0 {
				return nil
			}
			return v
		case time.Time, []byte, string, arrow.MonthDayNanoInterval:
			return v
		}
		if structFields(v, "Months", "Days") || structFields(v, "Microseconds") {
			return v
		}
		if val, ok := v.(driver.Valuer); ok {
			dv, err := val.Value()
			if err != nil {
				return v
			}
			v = dv
			continue
		}
		return v
	}
	return v
}

func isBig(v any) bool {
	switch v.(type) {
	case *big.Int, *big.Rat, *big.Float:
		return true
	}
	return false
}

func structFields(v any, names ...string) bool {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Struct {
		return false
	}
	for _, n := range names {
		if !rv.FieldByName(n).IsValid() {
			return false
		}
	}
	return true
}

func integer(v any) (*big.Int, bool) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return big.NewInt(rv.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return new(big.Int).SetUint64(rv.Uint()), true
	case reflect.Float32, reflect.Float64:
		f := rv.Float()
		if f == math.Trunc(f) && math.Abs(f) <= 1<<53 {
			return big.NewInt(int64(f)), true
		}
		return nil, false
	}
	switch x := v.(type) {
	case *big.Int:
		return x, true
	case json.Number:
		n, ok := new(big.Int).SetString(string(x), 10)
		return n, ok
	case string:
		n, ok := new(big.Int).SetString(strings.TrimSpace(x), 10)
		return n, ok
	}
	return nil, false
}

func float(v any) (float64, bool) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Float32, reflect.Float64:
		return rv.Float(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), true
	}
	var s string
	switch x := v.(type) {
	case json.Number:
		s = string(x)
	case string:
		s = x
	default:
		return 0, false
	}
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "nan":
		return math.NaN(), true
	case "inf", "+inf", "infinity", "+infinity":
		return math.Inf(1), true
	case "-inf", "-infinity":
		return math.Inf(-1), true
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return f, err == nil
}

func floatText(f float64, bits int) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "+Inf"
	case math.IsInf(f, -1):
		return "-Inf"
	}
	return strconv.FormatFloat(f, 'g', -1, bits)
}

func rational(v any) (*big.Rat, bool) {
	switch x := v.(type) {
	case *big.Rat:
		return x, true
	case *big.Int:
		return new(big.Rat).SetInt(x), true
	case *big.Float:
		r, _ := x.Rat(nil)
		return r, r != nil
	case json.Number:
		return new(big.Rat).SetString(string(x))
	case string:
		return new(big.Rat).SetString(strings.TrimSpace(x))
	case float32, float64:
		f, _ := float(x)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, false
		}
		return new(big.Rat).SetFloat64(f), true
	case fmt.Stringer:
		return new(big.Rat).SetString(x.String())
	}
	if n, ok := integer(v); ok {
		return new(big.Rat).SetInt(n), true
	}
	return nil, false
}

var timestampLayouts = []string{
	time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999Z07",
	"2006-01-02 15:04:05.999999999", "2006-01-02",
}

func timestamp(v any) (time.Time, bool) {
	switch x := v.(type) {
	case time.Time:
		return x, true
	case string:
		for _, l := range timestampLayouts {
			if t, err := time.Parse(l, strings.TrimSpace(x)); err == nil {
				return t, true
			}
		}
	}
	return time.Time{}, false
}

func timeOfDay(v any) (time.Duration, bool) {
	switch x := v.(type) {
	case time.Duration:
		return x, true
	case string:
		t, err := time.Parse("15:04:05.999999999", strings.TrimSpace(x))
		if err != nil {
			return 0, false
		}
		return t.Sub(t.Truncate(24 * time.Hour)), true
	case time.Time:
		return time.Duration(x.Hour())*time.Hour + time.Duration(x.Minute())*time.Minute + time.Duration(x.Second())*time.Second +
			time.Duration(x.Nanosecond()), true
	}
	if structFields(v, "Microseconds") && !structFields(v, "Days") {
		return time.Duration(reflect.ValueOf(v).FieldByName("Microseconds").Int()) * time.Microsecond, true
	}
	if n, ok := integer(v); ok && n.IsInt64() {
		return time.Duration(n.Int64()) * time.Microsecond, true
	}
	return 0, false
}

var (
	isoInterval = regexp.MustCompile(`^(-)?P(?:(-?\d+)Y)?(?:(-?\d+)M)?(?:(-?\d+)W)?(?:(-?\d+)D)?(?:T(?:(-?\d+)H)?(?:(-?\d+)M)?(?:(-?\d+(?:\.\d+)?)S)?)?$`)
	pgUnit      = regexp.MustCompile(`(-?\d+) (year|mon|day)s?`)
	pgClock     = regexp.MustCompile(`(-)?(\d+):(\d\d):(\d\d(?:\.\d+)?)`)
)

func interval(v any) (arrow.MonthDayNanoInterval, bool) {
	switch x := v.(type) {
	case arrow.MonthDayNanoInterval:
		return x, true
	case string:
		return parseInterval(strings.TrimSpace(x))
	}
	if m, ok := v.(map[string]any); ok {
		return intervalObject(m)
	}
	if !structFields(v, "Months", "Days") {
		return arrow.MonthDayNanoInterval{}, false
	}
	rv := reflect.ValueOf(v)
	var ns int64
	switch {
	case rv.FieldByName("Nanoseconds").IsValid():
		ns = rv.FieldByName("Nanoseconds").Int()
	case rv.FieldByName("Microseconds").IsValid():
		ns = rv.FieldByName("Microseconds").Int() * 1000
	default:
		return arrow.MonthDayNanoInterval{}, false
	}
	return monthDayNano(rv.FieldByName("Months").Int(), rv.FieldByName("Days").Int(), ns)
}

// intervalObject reads an interval as JSON spells Arrow's: months, days,
// and nanoseconds or microseconds.
func intervalObject(m map[string]any) (arrow.MonthDayNanoInterval, bool) {
	field := func(name string) (int64, bool) {
		for k, v := range m {
			if strings.EqualFold(k, name) {
				n, ok := integer(v)
				return n.Int64(), ok && n.IsInt64()
			}
		}
		return 0, false
	}
	months, ok1 := field("months")
	days, ok2 := field("days")
	ns, ok3 := field("nanoseconds")
	if !ok3 {
		var us int64
		us, ok3 = field("microseconds")
		ns = us * 1000
	}
	if !ok1 || !ok2 || !ok3 {
		return arrow.MonthDayNanoInterval{}, false
	}
	return monthDayNano(months, days, ns)
}

func monthDayNano(months, days, ns int64) (arrow.MonthDayNanoInterval, bool) {
	if months < math.MinInt32 || months > math.MaxInt32 || days < math.MinInt32 || days > math.MaxInt32 {
		return arrow.MonthDayNanoInterval{}, false
	}
	return arrow.MonthDayNanoInterval{Months: int32(months), Days: int32(days), Nanoseconds: ns}, true
}

// nanos is a count of seconds, with a fraction, in nanoseconds.
func nanos(seconds string) int64 {
	sec, ok := new(big.Rat).SetString(cmpOr(seconds, "0"))
	if !ok {
		return 0
	}
	ns := new(big.Rat).Mul(sec, big.NewRat(1e9, 1))
	return new(big.Int).Quo(ns.Num(), ns.Denom()).Int64()
}

// parseInterval reads ISO 8601's durations and Postgres's own text.
func parseInterval(s string) (arrow.MonthDayNanoInterval, bool) {
	if m := isoInterval.FindStringSubmatch(s); m != nil && s != "P" {
		n := func(i int) int64 { x, _ := strconv.ParseInt(m[i], 10, 64); return x }
		months, days, ns := n(2)*12+n(3), n(4)*7+n(5), (n(6)*3600+n(7)*60)*1e9+nanos(m[8])
		if m[1] == "-" {
			months, days, ns = -months, -days, -ns
		}
		return monthDayNano(months, days, ns)
	}
	var months, days, ns int64
	rest := s
	for _, m := range pgUnit.FindAllStringSubmatch(s, -1) {
		n, _ := strconv.ParseInt(m[1], 10, 64)
		switch m[2] {
		case "year":
			months += n * 12
		case "mon":
			months += n
		case "day":
			days += n
		}
		rest = strings.Replace(rest, m[0], "", 1)
	}
	if m := pgClock.FindStringSubmatch(rest); m != nil {
		h, _ := strconv.ParseInt(m[2], 10, 64)
		mi, _ := strconv.ParseInt(m[3], 10, 64)
		ns = (h*3600+mi*60)*1e9 + nanos(m[4])
		if m[1] == "-" {
			ns = -ns
		}
		rest = strings.Replace(rest, m[0], "", 1)
	}
	if strings.TrimSpace(rest) != "" || s == "" {
		return arrow.MonthDayNanoInterval{}, false
	}
	return monthDayNano(months, days, ns)
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func list(v any) ([]any, bool) {
	switch x := v.(type) {
	case []any:
		return x, true
	case string, []byte, json.RawMessage:
		var b []byte
		switch y := x.(type) {
		case string:
			b = []byte(y)
		case []byte:
			b = y
		case json.RawMessage:
			b = y
		}
		d := json.NewDecoder(bytes.NewReader(b))
		d.UseNumber()
		var out []any
		if err := d.Decode(&out); err != nil {
			return nil, false
		}
		return out, true
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false
	}
	out := make([]any, rv.Len())
	for i := range out {
		out[i] = rv.Index(i).Interface()
	}
	return out, true
}

// Types: every logical type's edge values read back as they were written,
// but for the types the suite says the destination's mapping table
// doesn't keep.
func (r *destinationRun) checkTypes(t T) {
	r.process(t)
	r.needRead(t)
	cases := typeCases(r.spec.GetDestination().GetNanosecondTimestamps())
	var skipped []string
	cases = slices.DeleteFunc(cases, func(c typeCase) bool {
		why, ok := r.s.LossyTypes[c.logical]
		if ok {
			skipped = append(skipped, c.logical+": "+why)
		}
		return ok
	})
	for _, s := range skipped {
		flagf(t, "not checked, as the destination's mapping table says: %s", s)
	}
	if r.staged() {
		// Parquet has no interval with nanoseconds, so staging can't carry
		// one; the engine's writer refuses it too.
		cases = slices.DeleteFunc(cases, func(c typeCase) bool { return c.logical == "interval" })
		flagf(t, "interval not checked: Parquet staging can't carry a month-day-nanosecond interval")
	}
	s := r.session(t, r.spec.GetDestination().GetScopes()[0], protocolv1.WriteMode_WRITE_MODE_APPEND)
	data := []arrow.Field{dataFields[0]}
	for _, c := range cases {
		data = append(data, c.field)
	}
	s.data, s.streams = data, []string{typesStream}
	w, _ := s.begin(1, nil, nil)
	b, err := buildTypes(typesStream, cases)
	if err != nil {
		t.Fatalf("%v", err)
	}
	err = s.write(w, epoch(1, 1), typesStream, b)
	b.Release()
	if err != nil {
		t.Fatalf("%s the types stream's batch: %s: %v", s.where(), codeOf(err), err)
	}
	staged, err := s.preCommit(w, epoch(1, 1))
	if err != nil {
		t.Fatalf("%s pre-commit of the types stream: %s: %v", s.where(), codeOf(err), err)
	}
	s.commit(w, epoch(1, 1), staged)
	rows := s.read(typesStream)
	byID := map[string]map[string]any{}
	for _, row := range rows {
		byID[fmt.Sprint(row["id"])] = row
	}
	if len(byID) != typesRows(cases) {
		t.Errorf("%s the types stream has %d rows, not %d", s.where(), len(byID), typesRows(cases))
	}
	for _, c := range cases {
		for i := range typesRows(cases) {
			row, ok := byID[strconv.Itoa(i)]
			if !ok {
				continue
			}
			var want any
			if i < len(c.values) {
				want = c.values[i]
			}
			got, err := c.norm(row[c.column()])
			switch {
			case err != nil:
				t.Errorf("%s: row %d: %v", c.logical, i, err)
			case got != c.want(want):
				t.Errorf("%s: row %d: wrote %s, read back %s", c.logical, i, c.want(want), got)
			}
		}
	}
	s.close(w)
}

const typesStream = "types"
