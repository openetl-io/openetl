package ints

import (
	"math"
	"strconv"
	"testing"
)

func TestEdges(t *testing.T) {
	if Uint64(-1) != 0 || Uint64(math.MaxInt64) != math.MaxInt64 {
		t.Error("Uint64")
	}
	if n, ok := Int64(math.MaxInt64); !ok || n != math.MaxInt64 {
		t.Error("Int64 at its max")
	}
	if _, ok := Int64(math.MaxInt64 + 1); ok {
		t.Error("Int64 past its max")
	}
	if Int32(int64(math.MaxInt32)+1) != math.MaxInt32 || Int32(int64(math.MinInt32)-1) != math.MinInt32 || Int32(7) != 7 {
		t.Error("Int32")
	}
	if Int64Clamped(math.MaxInt64) != math.MaxInt64 || Int64Clamped(math.MaxUint64) != math.MaxInt64 || Int64Clamped(7) != 7 {
		t.Error("Int64Clamped")
	}
	if Uint32(-1) != 0 || Uint32(int64(math.MaxUint32)+1) != math.MaxUint32 || Uint32(int64(math.MaxUint32)) != math.MaxUint32 || Uint32(7) != 7 {
		t.Error("Uint32")
	}
	type mode int
	type protoMode int32
	if Enum[protoMode](mode(3)) != 3 || Enum[protoMode](mode(math.MinInt32)) != math.MinInt32 {
		t.Error("Enum")
	}
	if strconv.IntSize == 64 && (Enum[protoMode](mode(math.MaxInt)) != 0 || Enum[protoMode](mode(math.MinInt)) != 0) {
		t.Error("Enum past int32")
	}
}
