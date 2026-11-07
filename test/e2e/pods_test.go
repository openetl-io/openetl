//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

// startPodLauncher starts a pod launcher of the data plane that runs each
// attempt as a process of its own: jobs of m and up go to pods.
func startPodLauncher(t *testing.T, plane, token string, extra ...string) *runner {
	t.Helper()
	return startRunnerWith(t, token, env.apiURL, append([]string{"--data-plane", plane, "--launcher", "process", "--pod-min-size", "m"}, extra...)...)
}

// sizedSyncOn applies a scheduled pipeline on the data plane with these
// spec.resources.
func sizedSyncOn(t *testing.T, name, plane, faker, res string) {
	t.Helper()
	ds := pipelineDocs(name, faker, `{"commit": {"interval": "1s"}}`)
	ds[2] = strings.Replace(ds[2], `"source":`, fmt.Sprintf(`"dataPlane": %q, "resources": %s, "source":`, plane, res), 1)
	apply(t, ds...)
}

// podOf is the PID of the pod running the pipeline's current attempt, and
// the attempt, once there is one.
func podOf(t *testing.T, name string) (pid int, attempt string) {
	t.Helper()
	eventually(t, 60*time.Second, "a pod running "+name, func() bool {
		v, err := query[string](t, `SELECT r.id || ' ' || d.id FROM dispatches d
			JOIN attempts a ON a.org_id = d.org_id AND a.id = d.id
			JOIN pipelines p ON p.org_id = a.org_id AND p.id = a.pipeline_id
			JOIN runners r ON r.org_id = d.org_id AND r.id = d.runner AND r.attempt = d.id
			WHERE p.name = $1 AND d.state = 'running' AND d.launcher IS NOT NULL
			ORDER BY a.create_time DESC LIMIT 1`, name)
		if err != nil {
			return false
		}
		runner, id, _ := strings.Cut(v, " ")
		_, p, _ := strings.Cut(runner, ":")
		pid, attempt = mustAtoi(t, p), id
		return true
	})
	return pid, attempt
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// pods is how many pods, the launcher's child openetld processes, run now.
func (r *runner) pods(t *testing.T) int {
	t.Helper()
	out, err := exec.Command("pgrep", "-P", fmt.Sprint(r.cmd.Process.Pid), "^openetld").Output()
	if err != nil {
		return 0
	}
	return len(strings.Fields(string(out)))
}

// Large jobs run in pods of their own, small ones on the shared runner. A
// pod killed mid-epoch is replaced, and one stopped as Kubernetes stops it
// drains to a checkpoint; either way every row lands once. Pods go once
// their attempts end.
func TestAttemptsInPods(t *testing.T) {
	plane, token := placementPlane(t)
	launcher := startPodLauncher(t, plane, token)
	startPlaneRunner(t, plane, token, "1,2Gi")

	// A sync of size m goes to a pod.
	batch := unique("pod-sync")
	sizedSyncOn(t, batch, plane, `{"seed": 4, "count": 2000, "batchSize": 100}`, `{"size": "m"}`)
	if status, stats := waitSync(t, batch, 90*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkRows(t, schema(batch), 2000, "users", "purchases")
	ran, err := query[string](t, `SELECT d.launcher || ' ' || d.runner || ' ' || (r.attempt = d.id) FROM dispatches d
		JOIN attempts a ON a.org_id = d.org_id AND a.id = d.id
		JOIN pipelines p ON p.org_id = a.org_id AND p.id = a.pipeline_id
		JOIN runners r ON r.org_id = d.org_id AND r.id = d.runner
		WHERE p.name = $1`, batch)
	if f := strings.Fields(ran); err != nil || len(f) != 3 || !strings.HasSuffix(f[0], ":"+strconv.Itoa(launcher.cmd.Process.Pid)) || f[1] == f[0] || f[2] != "true" {
		t.Fatalf("the sync ran as %q (%v), want in a pod of the launcher", ran, err)
	}

	// A small stream stays on the shared runner; a large one gets a pod.
	small := unique("pod-small")
	sizedStreamOn(t, small, plane, `{"size": "s"}`)
	large := unique("pod-large")
	sizedStreamOn(t, large, plane, `{"size": "l"}`)
	waitRows(t, small, 100)
	waitRows(t, large, 100)
	if n, err := query[int](t, `SELECT count(*) FROM dispatches d JOIN attempts a ON a.org_id = d.org_id AND a.id = d.id
		JOIN pipelines p ON p.org_id = a.org_id AND p.id = a.pipeline_id WHERE p.name = $1 AND d.launcher IS NOT NULL`, small); err != nil || n != 0 {
		t.Errorf("the small stream had %d attempts in pods (%v), want none", n, err)
	}

	// Killed mid-epoch: its launcher sees it die, and the attempt is placed
	// again in a new pod.
	pid, first := podOf(t, large)
	eventually(t, 30*time.Second, "a committed epoch", func() bool { return committedEpochs(t, schema(large)) > 0 })
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	eventually(t, 60*time.Second, "a new pod", func() bool {
		_, now := podOf(t, large)
		return now != first
	})
	waitRows(t, large, 100)

	// Stopped as Kubernetes evicts a pod: it drains to a checkpoint and ends
	// DRAINING, and the job starts again at once.
	pid, second := podOf(t, large)
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	eventually(t, 60*time.Second, "the drained attempt", func() bool {
		code, err := query[string](t, "SELECT code FROM attempts WHERE id = $1", second)
		return err == nil && code == "DRAINING"
	})
	eventually(t, 60*time.Second, "a third pod", func() bool {
		_, now := podOf(t, large)
		return now != second
	})
	waitRows(t, large, 100)

	for _, name := range []string{small, large} {
		if _, err := pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name})); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 60*time.Second, "the pods to go", func() bool { return launcher.pods(t) == 0 })
	landed(t, large)
}
