// Package ints converts the SDK's integers to the protocol's without
// overflowing: each function says what it gives for a value the target
// can't hold.
package ints

import "math"

// Enum is v as the protocol enum P, or P's unspecified value, 0, for a v
// outside int32.
func Enum[P ~int32, T ~int](v T) P {
	if v < math.MinInt32 || v > math.MaxInt32 {
		return 0
	}
	return P(v)
}

// Uint32 is n, clamped to 0 and math.MaxUint32.
func Uint32(n int) uint32 {
	switch {
	case n < 0:
		return 0
	case uint64(n) > math.MaxUint32:
		return math.MaxUint32
	}
	return uint32(n)
}

// Uint64 is n, or 0 for a negative n: for counts, which are never negative.
func Uint64[T ~int | ~int64](n T) uint64 {
	if n < 0 {
		return 0
	}
	return uint64(n)
}
