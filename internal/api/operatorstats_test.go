package api

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

// A watermark JSON can't hold is left out, and the rest of the operator's
// numbers are kept; one at either end of the range JSON holds reads back.
func TestOperatorStatsKeepAWatermarkJSONCantHoldOut(t *testing.T) {
	first := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	last := time.Date(9999, 12, 31, 23, 59, 59, 999_999_000, time.UTC)
	in := []*apiv1.OperatorStats{
		{Operator: "before", RowsIn: 1, Watermark: timestamppb.New(first.Add(-time.Microsecond))},
		{Operator: "after", RowsIn: 2, Watermark: timestamppb.New(last.Add(time.Microsecond))},
		{Operator: "first", Watermark: timestamppb.New(first)},
		{Operator: "last", Watermark: timestamppb.New(last)},
	}
	b, err := marshalOperatorStats(in)
	if err != nil {
		t.Fatal(err)
	}
	got, err := unmarshalOperatorStats(b)
	if err != nil {
		t.Fatal(err)
	}
	want := []*apiv1.OperatorStats{
		{Operator: "before", RowsIn: 1},
		{Operator: "after", RowsIn: 2},
		{Operator: "first", Watermark: timestamppb.New(first)},
		{Operator: "last", Watermark: timestamppb.New(last)},
	}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if !proto.Equal(got[i], want[i]) {
			t.Errorf("got %v, want %v", got[i], want[i])
		}
	}
	if in[0].GetWatermark() == nil {
		t.Error("the request's message changed")
	}
}
