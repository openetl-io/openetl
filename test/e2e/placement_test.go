//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
)

// placementPlane makes a data plane for a test's own runners, which earlier
// tests' streams don't reach, and returns its name and token.
func placementPlane(t *testing.T) (name, token string) {
	t.Helper()
	name = unique("placement")
	pgExec(t, "INSERT INTO data_planes (id, org_id) SELECT '"+name+"', id FROM orgs")
	tok, err := apiv1connect.NewAccessServiceClient(env.http, env.apiURL).CreateRunnerToken(t.Context(), connect.NewRequest(&apiv1.CreateRunnerTokenRequest{Parent: "dataPlanes/" + name}))
	if err != nil {
		t.Fatal(err)
	}
	return name, tok.Msg.GetSecret()
}

func startPlaneRunner(t *testing.T, plane, token, capacity string) *runner {
	t.Helper()
	return startRunnerWith(t, token, env.apiURL, "--data-plane", plane, "--capacity", capacity)
}

// streamOn starts a continuous pipeline on the data plane.
func streamOn(t *testing.T, name, plane string) {
	t.Helper()
	ds := continuousDocs(name, endlessFaker)
	ds[2] = strings.Replace(ds[2], `"source":`, fmt.Sprintf(`"dataPlane": %q, "source":`, plane), 1)
	apply(t, ds...)
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
	})
}

// Attempts go where they fit: the runner they leave least room on first, and
// no more than each has room for. The rest wait, and start when room frees up.
func TestPlacementByCapacity(t *testing.T) {
	plane, token := placementPlane(t)
	small := startPlaneRunner(t, plane, token, "1,2Gi")
	large := startPlaneRunner(t, plane, token, "2,4Gi")
	eventually(t, 30*time.Second, "both runners to report", func() bool {
		n, err := query[int](t, "SELECT count(*) FROM runners WHERE data_plane_id = $1 AND capacity_cpu > 0", plane)
		return err == nil && n == 2
	})

	var names []string
	for i := range 4 {
		name := unique(fmt.Sprintf("placed-%d", i))
		names = append(names, name)
		streamOn(t, name, plane)
	}
	placed := func(state string) int {
		n, err := query[int](t, "SELECT count(*) FROM dispatches WHERE data_plane_id = $1 AND kind = 'RunStreamJob' AND state = $2", plane, state)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	eventually(t, 60*time.Second, "three jobs running and one waiting", func() bool { return placed("running") == 3 && placed("pending") == 1 })
	// Never more than fits, however long it runs.
	time.Sleep(3 * time.Second)
	if got := placed("running"); got != 3 {
		t.Errorf("%d jobs running, want 3", got)
	}
	used := func(r *runner) int {
		n, err := query[int](t, "SELECT count(*) FROM dispatches WHERE data_plane_id = $1 AND runner LIKE '%:' || $2::text AND state = 'running'", plane, strconv.Itoa(r.cmd.Process.Pid))
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if s, l := used(small), used(large); s != 1 || l != 2 {
		t.Errorf("small runs %d and large %d, want 1 and 2", s, l)
	}

	// Deleting a pipeline frees room for the one waiting.
	if _, err := pipelines().DeletePipeline(t.Context(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + names[0]})); err != nil {
		t.Fatal(err)
	}
	for _, name := range names[1:] {
		waitRows(t, name, 100)
	}
	eventually(t, 60*time.Second, "the last job to start", func() bool { return placed("pending") == 0 && placed("running") == 3 })
	reserved, err := query[int](t, "SELECT sum(reserved_cpu) FROM runners WHERE data_plane_id = $1", plane)
	if err != nil || reserved != 3000 {
		t.Errorf("runners report %d millicores reserved (%v), want 3000", reserved, err)
	}
}

// A size no runner of the data plane can take fails the run, naming it, once
// it has waited long enough for a larger runner to turn up.
func TestRunnerTooSmallSaysSo(t *testing.T) {
	plane, token := placementPlane(t)
	startPlaneRunner(t, plane, token, "0.5,1Gi")
	name := unique("toosmall")
	streamOn(t, name, plane)
	eventually(t, time.Minute, "the run to fail", func() bool {
		e, err := query[string](t, `SELECT coalesce(s.error, '') FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id WHERE p.name = $1 ORDER BY s.start_time DESC LIMIT 1`, name)
		if err != nil {
			return false
		}
		if strings.Contains(e, "doesn't fit any runner") {
			if !strings.Contains(e, "CONFIG_INVALID") || !strings.Contains(e, "1 vCPU, 2 GiB") || !strings.Contains(e, "the largest has 0.5 vCPU, 1 GiB") {
				t.Errorf("error %q", e)
			}
			return true
		}
		return false
	})
}
