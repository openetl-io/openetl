//go:build integration

package api_test

import (
	"net/http"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
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

// A sync's rows come from the newest checkpoint's total, so a report that was
// dropped between two others leaves nothing missing.
func TestSyncRowsSurviveADroppedReport(t *testing.T) {
	ctx := t.Context()
	e := newHoldsEnv(t)
	report := func(epoch uint64, rows uint64, total *uint64) {
		t.Helper()
		_, err := e.runner.ReportCheckpoint(ctx, connect.NewRequest(&dataplanev1.ReportCheckpointRequest{
			PipelineId: e.pipeline.String(), RunId: "run1", Generation: 1, Epoch: epoch, CreateTime: timestamppb.Now(),
			Streams: []*dataplanev1.CheckpointStream{{Name: "orders", Rows: rows, SyncRows: total}},
		}))
		if err != nil {
			t.Fatal(err)
		}
	}
	rows := func() uint64 {
		t.Helper()
		sc := apiv1connect.NewSyncServiceClient(http.DefaultClient, e.url)
		res, err := sc.ListSyncs(ctx, connect.NewRequest(&apiv1.ListSyncsRequest{Parent: e.parent}))
		if err != nil {
			t.Fatal(err)
		}
		return res.Msg.GetSyncs()[0].GetStreams()[0].GetRows()
	}
	ten, twentyFive := uint64(10), uint64(25)
	report(1, 10, &ten)
	// Epoch 2's report, 5 rows, was dropped.
	report(3, 10, &twentyFive)
	if got := rows(); got != 25 {
		t.Errorf("rows = %d, want 25: the total of the newest report", got)
	}

	// A checkpoint from before totals is summed, as before.
	if _, err := e.conn.Exec(ctx, `UPDATE checkpoint_mirror SET streams = '[{"name": "orders", "rows": 7, "quarantined_rows": 0}]'`); err != nil {
		t.Fatal(err)
	}
	if got := rows(); got != 14 {
		t.Errorf("rows without totals = %d, want the epochs' sum, 14", got)
	}
}
