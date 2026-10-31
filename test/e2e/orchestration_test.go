//go:build e2e

package e2e

import (
	"strings"
	"syscall"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"

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
