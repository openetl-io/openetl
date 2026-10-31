//go:build e2e

package e2e

import (
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
)

func temporalClient(t *testing.T) client.Client {
	t.Helper()
	tc, err := client.DialContext(t.Context(), client.Options{HostPort: env.temporal, Namespace: env.ns})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tc.Close)
	return tc
}

// history returns the events of the pipeline's latest sync.
func history(t *testing.T, pipeline string) []*historypb.HistoryEvent {
	t.Helper()
	run, err := query[string](t, `SELECT s.workflow_id || ' ' || s.run_id FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id
		WHERE p.name = $1 ORDER BY s.start_time DESC LIMIT 1`, pipeline)
	if err != nil {
		t.Fatal(err)
	}
	wid, rid, _ := strings.Cut(run, " ")
	it := temporalClient(t).GetWorkflowHistory(t.Context(), wid, rid, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var events []*historypb.HistoryEvent
	for it.HasNext() {
		ev, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, ev)
	}
	return events
}

// A runner told to stop drains its attempt to a checkpoint and exits; another
// runner resumes from that checkpoint, and the drain doesn't count as a retry.
func TestRunnerDrains(t *testing.T) {
	first := startRunner(t)
	name := unique("drained")
	apply(t, pipelineDocs(name, slowFaker, `{"commit": {"interval": "1s"}}`)...)
	first.engine(t)
	eventually(t, 30*time.Second, "a committed epoch", func() bool { return committedEpochs(t, schema(name)) > 0 })
	if err := first.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-first.done:
	case <-time.After(30 * time.Second):
		t.Fatal("the runner didn't drain within 30 s")
	}
	if !strings.Contains(first.out.String(), "sync attempt drained") {
		t.Fatalf("the runner exited without draining:\n%s", first.out.String())
	}
	startRunner(t)
	status, stats := waitSync(t, name, 90*time.Second)
	if status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if !strings.Contains(stats, `"Generation": 2`) {
		t.Errorf("stats %s: want the second attempt to have finished", stats)
	}
	checkRows(t, schema(name), 2000, "users", "purchases")

	var scheduled int
	var drained bool
	for _, ev := range history(t, name) {
		if a := ev.GetActivityTaskScheduledEventAttributes(); a != nil && a.GetActivityType().GetName() == contract.RunSyncAttempt {
			scheduled++
			if p := a.GetRetryPolicy(); scheduled == 2 && p.GetMaximumAttempts() != 5 {
				t.Errorf("the restarted attempt may run %d times, want 5", p.GetMaximumAttempts())
			}
		}
		if f := ev.GetActivityTaskFailedEventAttributes(); f != nil && f.GetFailure().GetApplicationFailureInfo().GetType() == "DRAINING" {
			drained = true
		}
	}
	if scheduled != 2 || !drained {
		t.Errorf("RunSyncAttempt scheduled %d times, drained: %v; want 2 and a drain", scheduled, drained)
	}
}

func pipelines() apiv1connect.PipelineServiceClient {
	return apiv1connect.NewPipelineServiceClient(http.DefaultClient, env.apiURL)
}

func syncCount(t *testing.T, pipeline string) int {
	t.Helper()
	n, err := query[int](t, `SELECT count(*) FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id WHERE p.name = $1`, pipeline)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// syncNow starts a sync and waits for it to finish.
func syncNow(t *testing.T, pipeline string) (status, stats string) {
	t.Helper()
	before := syncCount(t, pipeline)
	if _, err := pipelines().SyncPipeline(t.Context(), connect.NewRequest(&apiv1.SyncPipelineRequest{Name: "workspaces/default/pipelines/" + pipeline})); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, "the sync to start", func() bool { return syncCount(t, pipeline) > before })
	return waitSync(t, pipeline, 60*time.Second)
}

func wantCode(t *testing.T, err error, code connect.Code, what string) {
	t.Helper()
	if connect.CodeOf(err) != code {
		t.Errorf("%s: got %v, want %s", what, err, code)
	}
}

func schedulePaused(t *testing.T, pipeline string) bool {
	t.Helper()
	id, err := query[string](t, `SELECT id::text FROM pipelines WHERE name = $1`, pipeline)
	if err != nil {
		t.Fatal(err)
	}
	desc, err := temporalClient(t).ScheduleClient().GetHandle(t.Context(), contract.ScheduleID(id)).Describe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return desc.Schedule.State.Paused
}

// Pausing pauses the schedule, applying again leaves it paused, and a paused
// pipeline or one that is syncing doesn't start another sync.
func TestPauseResumeAndSyncNow(t *testing.T) {
	startRunner(t)
	name := unique("paused")
	full := "workspaces/default/pipelines/" + name
	faker := `{"seed": 4, "count": 1000, "batchSize": 50, "delay": "200ms"}`
	apply(t, pipelineDocs(name, faker, `{}`)...)
	eventually(t, 30*time.Second, "the first sync to start", func() bool { return syncCount(t, name) == 1 })
	_, err := pipelines().SyncPipeline(t.Context(), connect.NewRequest(&apiv1.SyncPipelineRequest{Name: full}))
	wantCode(t, err, connect.CodeFailedPrecondition, "sync while syncing")
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}

	res, err := pipelines().PausePipeline(t.Context(), connect.NewRequest(&apiv1.PausePipelineRequest{Name: full}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Msg.GetPipeline().GetPaused() || !schedulePaused(t, name) {
		t.Fatalf("paused %v, schedule paused %v", res.Msg.GetPipeline().GetPaused(), schedulePaused(t, name))
	}
	_, err = pipelines().SyncPipeline(t.Context(), connect.NewRequest(&apiv1.SyncPipelineRequest{Name: full}))
	wantCode(t, err, connect.CodeFailedPrecondition, "sync while paused")

	docs := pipelineDocs(name, faker, `{}`)
	docs[2] = strings.Replace(docs[2], `"every": "1h"`, `"every": "2h"`, 1)
	if r := apply(t, docs...); r[2].GetAction() != apiv1.ApplyAction_APPLY_ACTION_UPDATED {
		t.Fatalf("reapplied the pipeline: %v", r[2].GetAction())
	}
	got, err := pipelines().GetPipeline(t.Context(), connect.NewRequest(&apiv1.GetPipelineRequest{Name: full}))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Msg.GetPipeline().GetPaused() || !schedulePaused(t, name) {
		t.Fatalf("after applying, paused %v, schedule paused %v", got.Msg.GetPipeline().GetPaused(), schedulePaused(t, name))
	}

	if _, err := pipelines().ResumePipeline(t.Context(), connect.NewRequest(&apiv1.ResumePipelineRequest{Name: full})); err != nil {
		t.Fatal(err)
	}
	if schedulePaused(t, name) {
		t.Fatal("the schedule is still paused")
	}
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if n := syncCount(t, name); n != 2 {
		t.Fatalf("%d syncs, want 2", n)
	}
}
