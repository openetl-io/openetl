//go:build integration

package api_test

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
)

func (e *holdsEnv) paused(t *testing.T) []string {
	t.Helper()
	res, err := e.owner.GetPipeline(t.Context(), connect.NewRequest(&apiv1.GetPipelineRequest{Name: e.parent}))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, s := range res.Msg.GetPipeline().GetPausedStreams() {
		out = append(out, s.GetStream())
	}
	return out
}

// A stream that quarantines too much is paused on its own, which restarts
// the continuous run and reaches the runner; a person resumes it, and may
// pause a stream that can stop on its own.
func TestStreamPauses(t *testing.T) {
	ctx := t.Context()
	e := newHoldsEnv(t)
	incremental, cdc := apiv1.SyncMode_SYNC_MODE_INCREMENTAL, apiv1.SyncMode_SYNC_MODE_CDC
	_, err := e.runner.ReportCheckpoint(ctx, connect.NewRequest(&dataplanev1.ReportCheckpointRequest{
		PipelineId: e.pipeline.String(), RunId: "run1", Generation: 1, Epoch: 1, CreateTime: timestamppb.Now(),
		Streams: []*dataplanev1.CheckpointStream{
			{Name: "orders", Rows: 1000, QuarantinedRows: 500, Read: incremental, QuarantineFile: "s3://lake/o/p/_quarantine/1/1/orders.jsonl"},
			{Name: "users", Rows: 10, Read: incremental},
			{Name: "changes", Rows: 10, Read: cdc},
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if got := e.paused(t); len(got) != 1 || got[0] != "orders" || e.sched.n != 1 {
		t.Fatalf("paused %v with %d refreshes; want orders, and the run restarted", got, e.sched.n)
	}
	spec, err := e.runner.GetSyncSpec(ctx, connect.NewRequest(&dataplanev1.GetSyncSpecRequest{PipelineId: e.pipeline.String(), Version: "v1"}))
	if err != nil {
		t.Fatal(err)
	}
	if got := spec.Msg.GetPausedStreams(); len(got) != 1 || got[0] != "orders" {
		t.Errorf("the runner gets paused streams %v", got)
	}

	if _, err := e.viewer.ResumePipeline(ctx, connect.NewRequest(&apiv1.ResumePipelineRequest{Name: e.parent, Stream: "orders"})); code(err) != connect.CodePermissionDenied {
		t.Errorf("a viewer resumed a stream: %v", err)
	}
	if _, err := e.owner.ResumePipeline(ctx, connect.NewRequest(&apiv1.ResumePipelineRequest{Name: e.parent, Stream: "orders"})); err != nil {
		t.Fatal(err)
	}
	if got := e.paused(t); len(got) != 0 || e.sched.n != 2 {
		t.Errorf("after resuming, paused %v with %d refreshes", got, e.sched.n)
	}

	for stream, want := range map[string]connect.Code{"changes": connect.CodeFailedPrecondition, "nope": connect.CodeNotFound, "users": 0} {
		_, err := e.owner.PausePipeline(ctx, connect.NewRequest(&apiv1.PausePipelineRequest{Name: e.parent, Stream: stream}))
		if want == 0 && err != nil || want != 0 && code(err) != want {
			t.Errorf("pausing %s: %v, want %v", stream, err, want)
		}
	}
	if _, err := e.owner.PausePipeline(ctx, connect.NewRequest(&apiv1.PausePipelineRequest{Name: e.parent, Stream: "orders"})); err != nil {
		t.Fatal(err)
	}
	// changes is still read, so orders isn't the last.
	if got := e.paused(t); len(got) != 2 {
		t.Errorf("paused %v, want orders and users", got)
	}
}

// Heal's overrides show on the pipeline, reach the runner, and go once a
// person removes them.
func TestRemovePipelineOverride(t *testing.T) {
	ctx := t.Context()
	e := newHoldsEnv(t)
	if _, err := e.conn.Exec(ctx, `INSERT INTO pipeline_overrides (org_id, pipeline_id, field, value, reason)
		SELECT org_id, id, 'source.rate_limit_scale', '0.5', '3 syncs in a row failed rate limited.' FROM pipelines WHERE id = $1`, e.pipeline); err != nil {
		t.Fatal(err)
	}
	spec, err := e.runner.GetSyncSpec(ctx, connect.NewRequest(&dataplanev1.GetSyncSpecRequest{PipelineId: e.pipeline.String(), Version: "v1"}))
	if err != nil {
		t.Fatal(err)
	}
	if got := spec.Msg.GetRateLimitScale(); got != 0.5 {
		t.Errorf("the runner gets rate limit scale %v", got)
	}
	p, err := e.viewer.GetPipeline(ctx, connect.NewRequest(&apiv1.GetPipelineRequest{Name: e.parent}))
	if err != nil {
		t.Fatal(err)
	}
	if o := p.Msg.GetPipeline().GetOverrides(); len(o) != 1 || o[0].GetField() != "source.rate_limit_scale" || o[0].GetValue() != "0.5" || o[0].GetSpecValue() != "1" {
		t.Errorf("overrides %v", o)
	}
	if _, err := e.owner.RemovePipelineOverride(ctx, connect.NewRequest(&apiv1.RemovePipelineOverrideRequest{Name: e.parent, Field: "schedule.cron"})); code(err) != connect.CodeInvalidArgument {
		t.Errorf("removing an override of a field heal doesn't override: %v", err)
	}
	res, err := e.owner.RemovePipelineOverride(ctx, connect.NewRequest(&apiv1.RemovePipelineOverrideRequest{Name: e.parent, Field: "source.rate_limit_scale"}))
	if err != nil {
		t.Fatal(err)
	}
	if o := res.Msg.GetPipeline().GetOverrides(); len(o) != 0 {
		t.Errorf("after removing it, overrides %v", o)
	}
}

// The schedules the holds environment's server set last, by pipeline ID.
var upserted = map[string]*apiv1.Schedule{}

func (r *refreshes) Upsert(_ context.Context, _, pipeline string, s *apiv1.Schedule, _ bool) error {
	upserted[pipeline] = s
	return nil
}

func (r *refreshes) StopStream(context.Context, string) error { return nil }

// An apply keeps heal's override of a field its version doesn't change,
// and the schedule it sets has the override; one that changes the field
// drops it, and its plan says so.
func TestApplyKeepsOverrides(t *testing.T) {
	ctx := t.Context()
	e := newHoldsEnv(t)
	applier := apiv1connect.NewApplyServiceClient(http.DefaultClient, e.url)
	docs := func(every, mode string) []*apiv1.Document {
		return []*apiv1.Document{{Content: []byte(`
apiVersion: openetl.io/v1alpha1
kind: Source
metadata: { name: gen }
spec: { connector: faker, config: { seed: 1, count: 10 } }
`)}, {Content: []byte(`
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata: { name: files }
spec: { connector: parquet, config: { url: "file:///tmp/openetl-overrides-test" } }
`)}, {Content: []byte(`
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata: { name: hourly }
spec:
  schedule: { every: ` + every + ` }
  heal: { mode: ` + mode + ` }
  source: { ref: gen }
  destination: { ref: files }
`)}}
	}
	apply := func(every, mode string, validateOnly bool) *apiv1.ApplyResponse {
		t.Helper()
		res, err := applier.Apply(ctx, connect.NewRequest(&apiv1.ApplyRequest{Documents: docs(every, mode), ValidateOnly: validateOnly}))
		if err != nil {
			t.Fatal(err)
		}
		return res.Msg
	}
	apply("1h", "auto_safe", false)
	var id string
	if err := e.conn.QueryRow(ctx, `SELECT id::text FROM pipelines WHERE name = 'hourly'`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := e.conn.Exec(ctx, `INSERT INTO pipeline_overrides (org_id, pipeline_id, field, value, reason)
		SELECT org_id, id, 'schedule.every', '"7200s"', '3 syncs in a row failed rate limited.' FROM pipelines WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}

	res := apply("1h", "suggest", false)
	if o := res.GetPlans()[0].GetOverrides(); len(o) != 1 || o[0].GetValue() != `"7200s"` || o[0].GetSpecValue() != `"3600s"` {
		t.Errorf("a version that doesn't change the schedule plans overrides %v", o)
	}
	if got := upserted[id].GetEvery().AsDuration(); got != 2*time.Hour {
		t.Errorf("the apply set syncs every %v, want heal's 2h", got)
	}

	plan := apply("30m", "suggest", true).GetPlans()[0]
	if len(plan.GetOverrides()) != 0 || !slices.ContainsFunc(plan.GetNotes(), func(n string) bool { return strings.Contains(n, "override of schedule.every") }) {
		t.Errorf("a version that changes the schedule plans overrides %v, notes %q", plan.GetOverrides(), plan.GetNotes())
	}
	apply("30m", "suggest", false)
	if got := upserted[id].GetEvery().AsDuration(); got != 30*time.Minute {
		t.Errorf("the apply set syncs every %v, want the spec's 30m", got)
	}
	var n int
	if err := e.conn.QueryRow(ctx, `SELECT count(*) FROM pipeline_overrides WHERE pipeline_id = $1`, id).Scan(&n); err != nil || n != 0 {
		t.Errorf("%d overrides left, %v", n, err)
	}
}
