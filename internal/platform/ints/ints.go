// Package ints converts between integer types without overflowing: each
// function says what it gives for a value the target can't hold.
package ints

import "math"

// Uint64 is n, or 0 for a negative n: for counts, epochs and generations,
// which are never negative.
func Uint64(n int64) uint64 {
	if n < 0 {
		return 0
	}
	return uint64(n)
}

// Int64 is n, and false if n is larger than math.MaxInt64.
func Int64(n uint64) (int64, bool) {
	if n > math.MaxInt64 {
		return 0, false
	}
	return int64(n), true
}

// Int64Clamped is n, or math.MaxInt64 for a larger n.
func Int64Clamped(n uint64) int64 {
	if n > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(n)
}

// Uint32 is n, clamped to 0 and math.MaxUint32.
func Uint32[T ~int | ~int64](n T) uint32 {
	switch {
	case n < 0:
		return 0
	case int64(n) > math.MaxUint32:
		return math.MaxUint32
	}
	return uint32(n)
}

// Enum is v as the proto enum P, or P's unspecified value, 0, for a v
// outside int32.
func Enum[P ~int32, T ~int](v T) P {
	if int64(v) < math.MinInt32 || int64(v) > math.MaxInt32 {
		return 0
	}
	return P(v)
}

// Int32 is n, clamped to math.MinInt32 and math.MaxInt32.
func Int32[T ~int | ~int64](n T) int32 {
	switch {
	case n < math.MinInt32:
		return math.MinInt32
	case n > math.MaxInt32:
		return math.MaxInt32
	}
	return int32(n)
}
