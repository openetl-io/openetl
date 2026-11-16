//go:build integration

package api_test

import (
	"net/http"
	"slices"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
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

// The mirror keeps the operators whose state a checkpoint names: none is
// an empty list, and the plan reads the newest checkpoint's.
func TestCheckpointKeepsItsOperators(t *testing.T) {
	ctx := t.Context()
	e := newHoldsEnv(t)
	for i, ops := range [][]string{{"window-a", "window-b"}, nil} {
		if _, err := e.runner.ReportCheckpoint(ctx, connect.NewRequest(&dataplanev1.ReportCheckpointRequest{
			PipelineId: e.pipeline.String(), RunId: "run1", Generation: 1, Epoch: uint64(i + 1), CreateTime: timestamppb.Now(),
			Streams: []*dataplanev1.CheckpointStream{{Name: "orders", Rows: 1}}, Operators: ops,
		})); err != nil {
			t.Fatal(err)
		}
		var got []string
		if err := e.conn.QueryRow(ctx, "SELECT operators FROM checkpoint_mirror WHERE pipeline_id = $1 AND epoch = $2", e.pipeline, i+1).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got == nil || !slices.Equal(got, ops) {
			t.Errorf("epoch %d keeps %#v, want %v", i+1, got, ops)
		}
	}
}

// What each operator did in an epoch reads back with its checkpoint, and a
// checkpoint without any has none.
func TestCheckpointKeepsItsOperatorStats(t *testing.T) {
	ctx := t.Context()
	e := newHoldsEnv(t)
	at := timestamppb.Now()
	stats := []*apiv1.OperatorStats{
		{Operator: "window-a", RowsIn: 12, Inserts: 3, Updates: 1, LateRows: 2, Windows: 7, Watermark: at, MemoryBytes: 64, StoredBytes: 1 << 40,
			BarrierRequests: &apiv1.StateRequests{Count: 2, Bytes: 900}, ReadRequests: &apiv1.StateRequests{Count: 5, Bytes: 4096}},
		{Operator: "window-b", EndOfInput: true},
	}
	for i, s := range [][]*apiv1.OperatorStats{stats, nil} {
		if _, err := e.runner.ReportCheckpoint(ctx, connect.NewRequest(&dataplanev1.ReportCheckpointRequest{
			PipelineId: e.pipeline.String(), RunId: "run1", Generation: 1, Epoch: uint64(i + 1), CreateTime: timestamppb.Now(),
			Streams: []*dataplanev1.CheckpointStream{{Name: "orders", Rows: 1}}, OperatorStats: s,
		})); err != nil {
			t.Fatal(err)
		}
	}
	cps, err := e.viewer.ListCheckpoints(ctx, connect.NewRequest(&apiv1.ListCheckpointsRequest{Parent: e.parent}))
	if err != nil {
		t.Fatal(err)
	}
	got := cps.Msg.GetCheckpoints()
	if len(got) != 2 || got[0].GetEpoch() != 2 || len(got[0].GetOperators()) != 0 {
		t.Fatalf("checkpoints %v", got)
	}
	ops := got[1].GetOperators()
	if len(ops) != 2 || !proto.Equal(ops[0], stats[0]) || !proto.Equal(ops[1], stats[1]) {
		t.Errorf("operators %v, want %v", ops, stats)
	}
}
