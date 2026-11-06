//go:build integration

package api_test

import (
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
)

// A checkpoint whose stream got less than its planned level lists both.
func TestCheckpointBelowPlan(t *testing.T) {
	ctx := t.Context()
	e := newHoldsEnv(t)
	_, err := e.runner.ReportCheckpoint(ctx, connect.NewRequest(&dataplanev1.ReportCheckpointRequest{
		PipelineId: e.pipeline.String(), RunId: "run1", Generation: 2, Epoch: 5, CreateTime: timestamppb.Now(),
		Streams: []*dataplanev1.CheckpointStream{
			{Name: "orders", Rows: 10, Forced: true, Level: apiv1.DeliveryLevel_DELIVERY_LEVEL_AT_LEAST_ONCE, PlannedLevel: apiv1.DeliveryLevel_DELIVERY_LEVEL_EXACTLY_ONCE},
			{Name: "users", Rows: 1},
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	cps, err := e.viewer.ListCheckpoints(ctx, connect.NewRequest(&apiv1.ListCheckpointsRequest{Parent: e.parent}))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(cps.Msg.GetCheckpoints()); n != 1 {
		t.Fatalf("%d checkpoints", n)
	}
	for _, st := range cps.Msg.GetCheckpoints()[0].GetStreams() {
		got := st.GetLevel().String() + " " + st.GetPlannedLevel().String()
		want := map[string]string{
			"orders": "DELIVERY_LEVEL_AT_LEAST_ONCE DELIVERY_LEVEL_EXACTLY_ONCE",
			"users":  "DELIVERY_LEVEL_UNSPECIFIED DELIVERY_LEVEL_UNSPECIFIED",
		}[st.GetName()]
		if got != want {
			t.Errorf("%s: %s, want %s", st.GetName(), got, want)
		}
	}
}
