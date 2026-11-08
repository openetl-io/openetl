package ints

import (
	"math"
	"strconv"
	"testing"
)

type mode int

type protoMode int32

func TestEdges(t *testing.T) {
	if Enum[protoMode](mode(3)) != 3 || Enum[protoMode](mode(math.MinInt32)) != math.MinInt32 {
		t.Error("Enum")
	}
	if strconv.IntSize == 64 && (Enum[protoMode](mode(math.MaxInt)) != 0 || Enum[protoMode](mode(math.MinInt)) != 0) {
		t.Error("Enum past int32")
	}
	if Uint32(-1) != 0 || Uint32(7) != 7 || strconv.IntSize == 64 && Uint32(math.MaxInt) != math.MaxUint32 {
		t.Error("Uint32")
	}
	if Uint64(-1) != 0 || Uint64(int64(math.MaxInt64)) != math.MaxInt64 {
		t.Error("Uint64")
	}
}
