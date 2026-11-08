//go:build integration

package api_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/orchestration/activities"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

func (e *holdsEnv) spec(t *testing.T) *dataplanev1.GetSyncSpecResponse {
	t.Helper()
	spec, err := e.runner.GetSyncSpec(t.Context(), connect.NewRequest(&dataplanev1.GetSyncSpecRequest{PipelineId: e.pipeline.String(), Version: "v1"}))
	if err != nil {
		t.Fatal(err)
	}
	return spec.Msg
}

func TestResetsAndRewinds(t *testing.T) {
	e := newHoldsEnv(t)
	ctx := t.Context()
	if _, err := e.viewer.ResetPipeline(ctx, connect.NewRequest(&apiv1.ResetPipelineRequest{Name: e.parent})); code(err) != connect.CodePermissionDenied {
		t.Fatalf("a viewer's reset: %v, want PermissionDenied", err)
	}
	if _, err := e.owner.ResetPipeline(ctx, connect.NewRequest(&apiv1.ResetPipelineRequest{Name: e.parent, Streams: []string{"orders", "orders"}})); err != nil {
		t.Fatal(err)
	}
	if e.sched.n != 1 {
		t.Fatalf("the continuous pipeline was refreshed %d times, want once", e.sched.n)
	}
	resets := e.spec(t).GetResets()
	if len(resets) != 1 || strings.Join(resets[0].GetStreams(), ",") != "orders" {
		t.Fatalf("resets = %v, want one of orders", resets)
	}
	var n int
	if err := e.conn.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE type = 'pipeline.reset' AND data -> 'streams' ? 'orders'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("pipeline.reset events: %d (%v), want 1", n, err)
	}
	// A checkpoint that took it ends it.
	if _, err := e.runner.ReportCheckpoint(ctx, connect.NewRequest(&dataplanev1.ReportCheckpointRequest{
		PipelineId: e.pipeline.String(), RunId: "run1", Generation: 1, Epoch: 1, CreateTime: timestamppb.Now(), LastReset: resets[0].GetId(),
	})); err != nil {
		t.Fatal(err)
	}
	if got := e.spec(t).GetResets(); len(got) != 0 {
		t.Fatalf("resets after a checkpoint took it: %v", got)
	}

	if _, err := e.owner.RewindPipeline(ctx, connect.NewRequest(&apiv1.RewindPipelineRequest{Name: e.parent})); err != nil {
		t.Fatal(err)
	}
	approval := e.spec(t).GetRewind()
	if approval == "" {
		t.Fatal("GetSyncSpec has no rewind after RewindPipeline")
	}
	_, err := e.runner.ReportRewind(ctx, connect.NewRequest(&dataplanev1.ReportRewindRequest{
		PipelineId: e.pipeline.String(), RunId: "run1", Approval: approval, Reason: "checkpoint: record unreadable: record 2-g1 is missing",
		Unreadable: &dataplanev1.CheckpointRef{Generation: 1, Epoch: 2}, Restored: &dataplanev1.CheckpointRef{Generation: 1, Epoch: 1},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if got := e.spec(t).GetRewind(); got != "" {
		t.Fatalf("the rewind is still pending after it was taken: %s", got)
	}
	var severity, message string
	var actions []byte
	err = e.conn.QueryRow(ctx, `SELECT severity, message, actions FROM incidents WHERE class = 'checkpoint_corrupt' AND status = 'open'`).Scan(&severity, &message, &actions)
	if err != nil {
		t.Fatal(err)
	}
	var taken []struct{ Kind, State string }
	if err := json.Unmarshal(actions, &taken); err != nil {
		t.Fatal(err)
	}
	if severity != "warning" || !strings.Contains(message, "restarted from the one before it, 1-g1, by a person's approval") ||
		len(taken) != 1 || taken[0].Kind != "rewind" || taken[0].State != "applied" {
		t.Fatalf("incident: %s, %q, %s", severity, message, actions)
	}
	// The pipeline isn't paused: it carries on.
	p, err := e.owner.GetPipeline(ctx, connect.NewRequest(&apiv1.GetPipelineRequest{Name: e.parent}))
	if err != nil || p.Msg.GetPipeline().GetPaused() {
		t.Fatalf("after a rewind the pipeline is paused (%v)", err)
	}
}

func TestRetentionProbes(t *testing.T) {
	e := newHoldsEnv(t)
	ctx := t.Context()
	report := func(at time.Time) {
		t.Helper()
		_, err := e.runner.ReportCheckpoint(ctx, connect.NewRequest(&dataplanev1.ReportCheckpointRequest{
			PipelineId: e.pipeline.String(), RunId: "run1", Generation: 1, Epoch: uint64(at.Unix()), CreateTime: timestamppb.New(at),
			Progress: []*protocolv1.Progress{{LagBytes: new(uint64(10)), RetentionLeftBytes: new(uint64(90))}},
		}))
		if err != nil {
			t.Fatal(err)
		}
	}
	report(time.Now().Add(-time.Hour))
	a := &activities.Activities{Store: e.store}
	in := contract.ListRetentionProbesInput{Before: time.Now().Add(-contract.RetentionProbeEvery), Max: 10}
	probes, err := a.ListRetentionProbes(ctx, in)
	if err != nil || len(probes) != 0 {
		t.Fatalf("probes of a pipeline whose run is going: %v (%v)", probes, err)
	}
	if _, err := e.conn.Exec(ctx, `UPDATE syncs SET status = 'failed', end_time = now()`); err != nil {
		t.Fatal(err)
	}
	if probes, err = a.ListRetentionProbes(ctx, in); err != nil || len(probes) != 1 || probes[0].PipelineID != e.pipeline.String() || probes[0].Op != contract.OpRetention {
		t.Fatalf("probes = %+v (%v), want the pipeline's", probes, err)
	}
	if again, err := a.ListRetentionProbes(ctx, in); err != nil || len(again) != 0 {
		t.Fatalf("probed again at once: %+v (%v)", again, err)
	}
	probe := func(lag, left uint64) {
		t.Helper()
		_, err := e.runner.ReportRetention(ctx, connect.NewRequest(&dataplanev1.ReportRetentionRequest{
			PipelineId: e.pipeline.String(), OperationId: probes[0].OpID, Checkpoint: &dataplanev1.CheckpointRef{Generation: 1, Epoch: 1},
			Progress: []*protocolv1.Progress{{LagBytes: new(lag), RetentionLeftBytes: new(left)}},
		}))
		if err != nil {
			t.Fatal(err)
		}
	}
	state := func() (status, severity, message string) {
		t.Helper()
		err := e.conn.QueryRow(ctx, `SELECT status, severity, message FROM incidents WHERE class = 'retention_at_risk' ORDER BY first_seen_time DESC LIMIT 1`).
			Scan(&status, &severity, &message)
		if err != nil {
			t.Fatal(err)
		}
		return status, severity, message
	}
	probe(85, 15)
	if status, severity, message := state(); status != "open" || severity != "critical" || !strings.Contains(message, "while it isn't running") {
		t.Fatalf("after a probe at 85%%: %s, %s, %q", status, severity, message)
	}
	probe(10, 90)
	if status, _, _ := state(); status != "resolved" {
		t.Fatalf("after a probe at 10%%: %s", status)
	}
	// Another operation's ID isn't the probe's.
	_, err = e.runner.ReportRetention(ctx, connect.NewRequest(&dataplanev1.ReportRetentionRequest{
		PipelineId: e.pipeline.String(), OperationId: "0198c2e5-4b5f-7000-8000-000000000000",
	}))
	if code(err) != connect.CodePermissionDenied {
		t.Fatalf("a report with an unknown operation: %v, want PermissionDenied", err)
	}
}
