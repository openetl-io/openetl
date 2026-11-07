package conformance

import (
	"encoding/json"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/google/uuid"
)

// What readers of destinations return, read as each case's values.
func TestTypesNorm(t *testing.T) {
	cases := map[string]typeCase{}
	for _, c := range typeCases(true) {
		cases[c.logical] = c
	}
	type pgInterval struct {
		Microseconds int64
		Days, Months int32
		Valid        bool
	}
	type pgTime struct {
		Microseconds int64
		Valid        bool
	}
	ts := time.Date(2026, 11, 8, 12, 34, 56, 789012000, time.UTC)
	tests := []struct {
		logical string
		read    any
		want    any
	}{
		{"boolean", "t", true},
		{"int64", json.Number("9223372036854775807"), int64(math.MaxInt64)},
		{"int64", "-9223372036854775808", int64(math.MinInt64)},
		{"int16", float64(-32768), int16(math.MinInt16)},
		{"float64", "Infinity", math.Inf(1)},
		{"float64", "-Infinity", math.Inf(-1)},
		{"float64", "NaN", math.NaN()},
		{"float32", float64(float32(0.1)), float32(0.1)},
		{"float32", json.Number("3.4028235e+38"), float32(math.MaxFloat32)},
		{"string", []byte("héllo"), "héllo"},
		{"binary", `\x00ff`, []byte{0, 0xff}},
		{"decimal", "0.0000000010", "0.000000001"},
		{"decimal", big.NewRat(-25, 2), "-12.5"},
		{"decimal", json.Number("99999999999999999999999999999.999999999"), "99999999999999999999999999999.999999999"},
		{"timestamp_tz", "2026-11-08T13:34:56.789012+01:00", ts},
		{"timestamp_tz", "2026-11-08 12:34:56.789012Z", ts},
		{"timestamp_tz", ts.In(time.FixedZone("x", 3600)), ts},
		{"timestamp", "2026-11-08 12:34:56.789012", ts},
		{"timestamp_ns", "2262-04-11T23:47:16.854775807Z", time.Date(2262, 4, 11, 23, 47, 16, 854775807, time.UTC)},
		{"date", "9999-12-31", time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"date", time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"time", "23:59:59.999999", 24*time.Hour - time.Microsecond},
		{"time", pgTime{Microseconds: 1, Valid: true}, time.Microsecond},
		{"time", int64(45296789012), 12*time.Hour + 34*time.Minute + 56*time.Second + 789012*time.Microsecond},
		{"interval", "1 mon 2 days 03:04:05.000006", arrow.MonthDayNanoInterval{Months: 1, Days: 2, Nanoseconds: int64(3*time.Hour + 4*time.Minute + 5*time.Second + 6*time.Microsecond)}},
		{"interval", "1 year 2 mons -00:00:00.000001", arrow.MonthDayNanoInterval{Months: 14, Nanoseconds: -1000}},
		{"interval", "-1 days", arrow.MonthDayNanoInterval{Days: -1}},
		{"interval", "P1M2DT3H4M5.000006S", arrow.MonthDayNanoInterval{Months: 1, Days: 2, Nanoseconds: int64(3*time.Hour + 4*time.Minute + 5*time.Second + 6*time.Microsecond)}},
		{"interval", "PT0S", arrow.MonthDayNanoInterval{}},
		{"interval", pgInterval{Months: 14, Microseconds: -1, Valid: true}, arrow.MonthDayNanoInterval{Months: 14, Nanoseconds: -1000}},
		{"list", []int64{1, 3}, []any{int64(1), int64(3)}},
		{"list", `[1, null, 3]`, []any{int64(1), nil, int64(3)}},
		{"list", []*int64{nil}, []any{nil}},
		{"json", map[string]any{"b": 1, "a": []any{}}, `{"a": [], "b": 1}`},
		{"json", []byte(`{"a" : 1}`), `{"a":1}`},
		{"uuid", [16]byte(uuid.Max), uuid.Max},
		{"uuid", "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11", uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")},
		{"uuid", uuid.Nil[:], uuid.Nil},
		{"int32", pgTime{Valid: false}, nil},
		{"string", (*string)(nil), nil},
	}
	for _, tc := range tests {
		c := cases[tc.logical]
		got, err := c.norm(tc.read)
		if err != nil {
			t.Errorf("%s %#v: %v", tc.logical, tc.read, err)
			continue
		}
		if want := c.want(tc.want); got != want {
			t.Errorf("%s %#v: read as %s, want %s", tc.logical, tc.read, got, want)
		}
	}
	for _, bad := range []struct {
		logical string
		read    any
	}{
		{"int64", "1.5"}, {"interval", "soon"}, {"uuid", "nope"}, {"json", "{"}, {"date", 3.5},
	} {
		if got, err := cases[bad.logical].norm(bad.read); err == nil {
			t.Errorf("%s %#v: read as %s, want an error", bad.logical, bad.read, got)
		}
	}
}
