package postgres

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func encodeNumeric(t *testing.T, s string) []byte {
	t.Helper()
	var n pgtype.Numeric
	if err := n.Scan(s); err != nil {
		t.Fatal(err)
	}
	b, err := pgtype.NewMap().Encode(pgtype.NumericOID, pgtype.BinaryFormatCode, n, nil)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDecodeNumeric(t *testing.T) {
	cases := []struct {
		in    string
		scale int32
		want  string
	}{
		{"0", 2, "0.00"},
		{"1.5", 1, "1.5"},
		{"-1234.5678", 4, "-1234.5678"},
		{"12345678.9", 2, "12345678.90"},
		{"100000000", 0, "100000000"},
		{"0.0001", 4, "0.0001"},
		{"0.00012", 5, "0.00012"},
		{"1000000000000000000000000000000", 0, "1000000000000000000000000000000"},
		{"1.2345678901234567890123456789012345678", 37, "1.2345678901234567890123456789012345678"},
		{"99999999999999999999999999999999999999", 0, "99999999999999999999999999999999999999"},
		{"-0.5", 3, "-0.500"},
	}
	for _, c := range cases {
		n, err := decodeNumeric(encodeNumeric(t, c.in), c.scale)
		if err != nil {
			t.Errorf("%s: %v", c.in, err)
			continue
		}
		if got := n.ToString(c.scale); got != c.want {
			t.Errorf("%s at scale %d = %s, want %s", c.in, c.scale, got, c.want)
		}
	}
	for _, in := range []string{"1.25", "NaN"} {
		if _, err := decodeNumeric(encodeNumeric(t, in), 1); err == nil {
			t.Errorf("%s at scale 1: no error", in)
		}
	}
	// Postgres's own spelling of 1.5: digits 1 and 5000, weight 0, dscale 1.
	n, err := decodeNumeric([]byte{0, 2, 0, 0, 0, 0, 0, 1, 0, 1, 0x13, 0x88}, 1)
	if err != nil || n.ToString(1) != "1.5" {
		t.Errorf("1.5 = %v, %v", n.ToString(1), err)
	}
}

func TestNumericTypmod(t *testing.T) {
	enc := func(p, s int32) int32 { return (p<<16 | s&0x7ff) + 4 }
	for _, c := range []struct {
		typmod int32
		p, s   int32
		ok     bool
	}{
		{-1, 0, 0, false},
		{enc(10, 2), 10, 2, true},
		{enc(38, 38), 38, 38, true},
		{enc(39, 2), 39, 2, false},
		{enc(5, -2), 5, -2, false},
	} {
		p, s, ok := numericTypmod(c.typmod)
		if p != c.p || s != c.s || ok != c.ok {
			t.Errorf("typmod %d = (%d, %d, %v), want (%d, %d, %v)", c.typmod, p, s, ok, c.p, c.s, c.ok)
		}
	}
}
