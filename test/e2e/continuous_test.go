//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
)

// About 50 rows a second per stream, forever.
const endlessFaker = `{"seed": 3, "endless": true, "batchSize": 10, "delay": "200ms"}`

func continuousDocs(name, faker string) []string {
	return []string{
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q},
			"spec": {"connector": "faker", "config": %s}}`, name, faker),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, name),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"mode": "continuous", "source": {"ref": %q}, "destination": {"ref": %q, "namespace": %q},
			"delivery": {"commit": {"interval": "1s"}}}}`, name, name, name, schema(name)),
	}
}

// startContinuous applies a continuous pipeline, and deletes it when the
// test ends, so its job doesn't run on later tests' runners.
func startContinuous(t *testing.T, name string) {
	t.Helper()
	apply(t, continuousDocs(name, endlessFaker)...)
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
	})
}

// runs is each run of the pipeline, oldest first: its status and version.
func runs(t *testing.T, pipeline string) []string {
	t.Helper()
	v, err := query[string](t, `SELECT coalesce(string_agg(s.status || ' ' || s.version, ',' ORDER BY s.start_time), '')
		FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id WHERE p.name = $1 AND s.continuous`, pipeline)
	if err != nil {
		t.Fatal(err)
	}
	if v == "" {
		return nil
	}
	return strings.Split(v, ",")
}

func statuses(runs []string) string {
	var out []string
	for _, r := range runs {
		status, _, _ := strings.Cut(r, " ")
		out = append(out, status)
	}
	return strings.Join(out, " ")
}

// landed is how many users rows the pipeline has written, and fails unless
// each id is there once.
func landed(t *testing.T, name string) int {
	t.Helper()
	ident := pgx.Identifier{schema(name), "users"}.Sanitize()
	v, err := query[string](t, "SELECT count(*) || ' ' || count(DISTINCT id) FROM "+ident)
	if err != nil {
		return 0
	}
	var rows, ids int
	if _, err := fmt.Sscan(v, &rows, &ids); err != nil {
		t.Fatal(err)
	}
	if rows != ids {
		t.Fatalf("%s has %d rows but %d ids", ident, rows, ids)
	}
	return rows
}

func waitRows(t *testing.T, name string, more int) {
	t.Helper()
	from := landed(t, name)
	eventually(t, 60*time.Second, fmt.Sprintf("%d more rows in %s", more, name), func() bool { return landed(t, name) >= from+more })
}

func waitRuns(t *testing.T, name, want string) []string {
	t.Helper()
	var got []string
	eventually(t, 60*time.Second, "runs "+want, func() bool {
		got = runs(t, name)
		return statuses(got) == want
	})
	return got
}

// A continuous pipeline runs until it's stopped: a source change gives it a
// new version, which stops the run and starts one of the new version; a
// pause stops it, and a resume starts another; deleting it stops it.
func TestContinuous(t *testing.T) {
	startRunner(t)
	name := unique("continuous")
	startContinuous(t, name)
	waitRows(t, name, 100)
	first := waitRuns(t, name, "running")

	apply(t, continuousDocs(name, strings.Replace(endlessFaker, `"batchSize": 10`, `"batchSize": 20`, 1))...)
	upgraded := waitRuns(t, name, "stopped running")
	v1 := strings.TrimPrefix(first[0], "running ")
	if upgraded[0] != "stopped "+v1 || upgraded[1] == "running "+v1 {
		t.Errorf("runs %v after %v: want the new version running", upgraded, first)
	}
	waitRows(t, name, 100)

	if _, err := pipelines().SyncPipeline(t.Context(), connect.NewRequest(&apiv1.SyncPipelineRequest{Name: "workspaces/default/pipelines/" + name})); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("sync now: %v, want FAILED_PRECONDITION", err)
	}

	if _, err := pipelines().PausePipeline(t.Context(), connect.NewRequest(&apiv1.PausePipelineRequest{Name: "workspaces/default/pipelines/" + name})); err != nil {
		t.Fatal(err)
	}
	waitRuns(t, name, "stopped stopped")
	paused := landed(t, name)
	time.Sleep(3 * time.Second)
	if n := landed(t, name); n != paused {
		t.Errorf("%d rows landed while paused", n-paused)
	}

	if _, err := pipelines().ResumePipeline(t.Context(), connect.NewRequest(&apiv1.ResumePipelineRequest{Name: "workspaces/default/pipelines/" + name})); err != nil {
		t.Fatal(err)
	}
	waitRuns(t, name, "stopped stopped running")
	waitRows(t, name, 100)

	// Checkpoints reach the API after the rows land.
	var res *connect.Response[apiv1.ListSyncsResponse]
	eventually(t, 10*time.Second, "the run's first checkpoint report", func() bool {
		var err error
		res, err = apiv1connect.NewSyncServiceClient(env.http, env.apiURL).ListSyncs(t.Context(), connect.NewRequest(&apiv1.ListSyncsRequest{Parent: "workspaces/default/pipelines/" + name}))
		return err == nil && len(res.Msg.GetSyncs()) > 1 && res.Msg.GetSyncs()[0].GetLastCommitTime() != nil
	})
	latest := res.Msg.GetSyncs()[0]
	if !latest.GetContinuous() || latest.GetState() != apiv1.SyncState_SYNC_STATE_RUNNING || latest.GetLastCommitTime() == nil {
		t.Errorf("latest run %v", latest)
	}
	if s := res.Msg.GetSyncs()[1]; s.GetState() != apiv1.SyncState_SYNC_STATE_STOPPED || len(s.GetStreams()) != 2 || s.GetStreams()[0].GetRows() == 0 {
		t.Errorf("a stopped run %v", s)
	}

	// The job stops at a checkpoint once a heartbeat brings the
	// cancellation.
	if _, err := pipelines().DeletePipeline(t.Context(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name})); err != nil {
		t.Fatal(err)
	}
	last := -1
	eventually(t, 30*time.Second, "rows to stop landing after the pipeline was deleted", func() bool {
		n := landed(t, name)
		if n == last {
			return true
		}
		last = n
		time.Sleep(2 * time.Second)
		return false
	})
}

// hosts reports whether the runner is running a job's engine.
func (r *runner) hosts() bool {
	return exec.Command("pgrep", "-P", fmt.Sprint(r.cmd.Process.Pid), "^openetl-engine").Run() == nil
}

func host(t *testing.T, rs ...*runner) *runner {
	t.Helper()
	var h *runner
	eventually(t, 60*time.Second, "a runner to run the job", func() bool {
		for _, r := range rs {
			if r.hosts() {
				h = r
				return true
			}
		}
		return false
	})
	return h
}

// A stopping runner drains its job to a checkpoint, and another runner
// carries it on at once, in the same run. A runner that dies loses its job
// to another once its heartbeats time out. A runner that froze and comes
// back finds its job replaced, and stops it. No row lands twice.
func TestContinuousMovesBetweenRunners(t *testing.T) {
	a, b := startRunner(t), startRunner(t)
	name := unique("continuous-moves")
	startContinuous(t, name)
	waitRows(t, name, 100)

	first := host(t, a, b)
	other := a
	if first == a {
		other = b
	}
	if err := first.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	host(t, other)
	select {
	case <-first.done:
	case <-time.After(30 * time.Second):
		t.Fatal("the drained runner didn't exit")
	}
	if !strings.Contains(first.out.String(), "stream job drained") {
		t.Errorf("the first runner didn't drain its job:\n%s", first.out.String())
	}
	waitRows(t, name, 100)

	c := startRunner(t)
	other.kill(t)
	host(t, c)
	waitRows(t, name, 100)
	if got := statuses(runs(t, name)); got != "running" {
		t.Errorf("runs %q, want one running", got)
	}

	d := startRunner(t)
	if err := c.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	host(t, d)
	waitRows(t, name, 100)
	if err := c.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, "the thawed runner to stop its replaced job", func() bool { return !c.hosts() })
	waitRows(t, name, 100)
	if got := statuses(runs(t, name)); got != "running" {
		t.Errorf("runs %q, want one running", got)
	}
	gen, err := query[int64](t, `SELECT max(c.generation) FROM checkpoint_mirror c JOIN pipelines p ON p.id = c.pipeline_id WHERE p.name = $1`, name)
	if err != nil || gen < 4 {
		t.Errorf("the latest generation is %d (%v), want at least 4", gen, err)
	}
}
