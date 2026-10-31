package postgres

import (
	"testing"
)

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

func TestNumericType(t *testing.T) {
	enc := func(p, s int32) int32 { return (p<<16 | s&0x7ff) + 4 }
	for typmod, want := range map[int32]string{
		-1:          "decimal(38, 9)",
		enc(12, 2):  "decimal(12, 2)",
		enc(39, 2):  "decimal(38, 9)",
		enc(5, -2):  "decimal(38, 9)",
		enc(38, 38): "decimal(38, 38)",
	} {
		if got := numericType(typmod).String(); got != want {
			t.Errorf("typmod %d = %s, want %s", typmod, got, want)
		}
	}
}
