//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
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
// it has waited long enough for a larger runner to turn up. With no runner yet,
// the plan can't refuse it. Auto takes what the runner has room for instead.
func TestRunnerTooSmallSaysSo(t *testing.T) {
	plane, token := placementPlane(t)
	name := unique("toosmall")
	sizedStreamOn(t, name, plane, `{"size": "s"}`)
	startPlaneRunner(t, plane, token, "0.5,1Gi")
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
	auto := unique("fits")
	streamOn(t, auto, plane)
	waitRows(t, auto, 100)
	if got := latestAttempts(t, auto); len(got) != 1 || got[0].cpu != 500 || got[0].why != "largest_runner" {
		t.Errorf("attempts %+v, want one at the most the runner can take", got)
	}
}

// sizedStreamOn starts a continuous pipeline with these spec.resources.
func sizedStreamOn(t *testing.T, name, plane, res string) {
	t.Helper()
	ds := continuousDocs(name, endlessFaker)
	ds[2] = strings.Replace(ds[2], `"source":`, fmt.Sprintf(`"dataPlane": %q, "resources": %s, "source":`, plane, res), 1)
	apply(t, ds...)
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
	})
}

// attemptOf is one attempt of the pipeline's latest sync.
type attemptOf struct {
	cpu  int
	why  string
	code string
}

func latestAttempts(t *testing.T, name string) []attemptOf {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	rows, err := conn.Query(t.Context(), `
		SELECT a.size_cpu, a.size_reason, a.code FROM attempts a
		JOIN pipelines p ON p.org_id = a.org_id AND p.id = a.pipeline_id
		WHERE p.name = $1 AND a.sync_id = (
			SELECT s.id FROM syncs s WHERE s.org_id = p.org_id AND s.pipeline_id = p.id ORDER BY s.start_time DESC LIMIT 1)
		ORDER BY a.attempt`, name)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (attemptOf, error) {
		var a attemptOf
		return a, r.Scan(&a.cpu, &a.why, &a.code)
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// planOf is what a dry run plans for the pipeline in docs.
func planOf(t *testing.T, docs ...string) *apiv1.PipelinePlan {
	t.Helper()
	req := &apiv1.ApplyRequest{ValidateOnly: true}
	for i, d := range docs {
		req.Documents = append(req.Documents, &apiv1.Document{Origin: fmt.Sprintf("doc %d", i+1), Content: []byte(d)})
	}
	res, err := apiv1connect.NewApplyServiceClient(env.http, env.apiURL).Apply(t.Context(), connect.NewRequest(req))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Msg.GetPlans()) != 1 {
		t.Fatalf("plans %v", res.Msg.GetPlans())
	}
	return res.Msg.GetPlans()[0]
}

// A size in the spec goes only where it fits, and only to the runners of the
// pipeline's pool, and the plan refuses what no runner there can take.
func TestSizesAndPools(t *testing.T) {
	plane, token := placementPlane(t)
	startPlaneRunner(t, plane, token, "1,2Gi")
	large := startPlaneRunner(t, plane, token, "2,4Gi")
	batch := startRunnerWith(t, token, env.apiURL, "--data-plane", plane, "--capacity", "1,2Gi", "--pool", "batch")
	eventually(t, 30*time.Second, "the runners to report", func() bool {
		n, err := query[int](t, "SELECT count(*) FROM runners WHERE data_plane_id = $1 AND capacity_cpu > 0", plane)
		return err == nil && n == 3
	})

	refused := continuousDocs(unique("too-large"), endlessFaker)
	refused[2] = strings.Replace(refused[2], `"source":`, fmt.Sprintf(`"dataPlane": %q, "resources": {"size": "l"}, "source":`, plane), 1)
	if err := applyErr(t, refused...); err == nil || !strings.Contains(err.Error(), "larger than any runner") || !strings.Contains(err.Error(), "the largest has 2 vCPU, 4 GiB") {
		t.Errorf("a size no runner can take: %v", err)
	}
	refused[2] = strings.Replace(refused[2], `"size": "l"`, `"size": "s", "pool": "nowhere"`, 1)
	if p := planOf(t, refused...); len(p.GetProblems()) != 0 || !strings.Contains(strings.Join(p.GetNotes(), " "), `pool "nowhere"`) {
		t.Errorf("a pool with no runners: %v, %v", p.GetProblems(), p.GetNotes())
	}

	runsOn := func(name string) string {
		r, err := query[string](t, `SELECT coalesce(d.runner, '') FROM dispatches d
			JOIN attempts a ON a.org_id = d.org_id AND a.id = d.id
			JOIN pipelines p ON p.org_id = a.org_id AND p.id = a.pipeline_id
			WHERE p.name = $1 AND d.state = 'running'`, name)
		if err != nil {
			return ""
		}
		return r
	}
	name := unique("sized")
	sizedStreamOn(t, name, plane, `{"size": "m"}`)
	eventually(t, 60*time.Second, "the job to run on the large runner", func() bool { return strings.HasSuffix(runsOn(name), fmt.Sprintf(":%d", large.cmd.Process.Pid)) })
	if got := latestAttempts(t, name); len(got) != 1 || got[0].cpu != 2000 || got[0].why != "spec" {
		t.Errorf("attempts %+v, want one sized m by the spec", got)
	}

	pooled := unique("pooled")
	sizedStreamOn(t, pooled, plane, `{"size": "s", "pool": "batch"}`)
	eventually(t, 60*time.Second, "the job to run in its pool", func() bool { return strings.HasSuffix(runsOn(pooled), fmt.Sprintf(":%d", batch.cmd.Process.Pid)) })
}

// An attempt killed for memory is retried one size up, and later syncs start
// from there. Each attempt records what it used, and the next sizes from that.
func TestSizesFollowAttempts(t *testing.T) {
	r := startRunner(t)
	name := unique("sizes")
	faker := `{"seed": 3, "count": 2000, "batchSize": 50, "delay": "50ms"}`
	docs := pipelineDocs(name, faker, `{"commit": {"interval": "1s"}}`)
	apply(t, docs...)
	pid := r.engine(t, name)
	eventually(t, 30*time.Second, "a committed epoch", func() bool { return committedEpochs(t, schema(name)) > 0 })
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if status, stats := waitSync(t, name, 90*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	got := latestAttempts(t, name)
	want := []attemptOf{{1000, "default", "RESOURCE_EXHAUSTED"}, {2000, "out_of_memory", ""}}
	if !slices.Equal(got, want) {
		t.Errorf("attempts %+v, want %+v", got, want)
	}

	sync := mustQuery[string](t, `SELECT s.id::text FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id WHERE p.name = $1 ORDER BY s.start_time DESC LIMIT 1`, name)
	res, err := syncs().GetSync(t.Context(), connect.NewRequest(&apiv1.GetSyncRequest{Name: "workspaces/default/pipelines/" + name + "/syncs/" + sync}))
	if err != nil {
		t.Fatal(err)
	}
	as := res.Msg.GetSync().GetAttempts()
	if len(as) != 2 || as[1].GetSize().GetPreset() != "m" || as[1].GetSizeReason() != apiv1.JobSizeReason_JOB_SIZE_REASON_OUT_OF_MEMORY {
		t.Fatalf("attempts %v", as)
	}
	u := as[1].GetUsage()
	if u.GetCpuSeconds() <= 0 || u.GetPeakMemoryBytes() <= 0 || u.GetSeconds() <= 0 || u.GetRunnerCpuSeconds() <= 0 || u.GetRunnerMemoryBytes() <= 0 {
		t.Errorf("usage %v", u)
	}
	if as[1].GetRows() == 0 || as[1].GetBytes() == 0 {
		t.Errorf("attempt %v moved nothing", as[1])
	}
	cps, err := pipelines().ListCheckpoints(t.Context(), connect.NewRequest(&apiv1.ListCheckpointsRequest{Parent: "workspaces/default/pipelines/" + name}))
	if err != nil {
		t.Fatal(err)
	}
	if len(cps.Msg.GetCheckpoints()) == 0 || cps.Msg.GetCheckpoints()[0].GetUsage().GetSeconds() <= 0 {
		t.Errorf("checkpoints %v have no usage", cps.Msg.GetCheckpoints())
	}

	if j := planOf(t, docs[2]).GetJob(); j.GetSize().GetPreset() != "m" || j.GetReason() != apiv1.JobSizeReason_JOB_SIZE_REASON_OUT_OF_MEMORY {
		t.Errorf("plan's job %v", j)
	}
	if _, err := pipelines().SyncPipeline(t.Context(), connect.NewRequest(&apiv1.SyncPipelineRequest{Name: "workspaces/default/pipelines/" + name})); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, "the next sync", func() bool {
		n, err := query[string](t, `SELECT s.id::text FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id WHERE p.name = $1 ORDER BY s.start_time DESC LIMIT 1`, name)
		return err == nil && n != sync
	})
	if status, stats := waitSync(t, name, 90*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if got := latestAttempts(t, name); len(got) != 1 || got[0].cpu != 2000 || got[0].why != "out_of_memory" {
		t.Errorf("next sync's attempts %+v, want it to start at m", got)
	}
}

// With no attempts yet, auto starts from the largest of the connectors' hints.
func TestConnectorHintSizesTheFirstAttempt(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"id": 1}]`))
	}))
	defer api.Close()
	startRunner(t)
	name := unique("hinted")
	apply(t, strings.Replace(manifest(name, "0.1.0", "/v1/contacts"), "spec:\n", "spec:\n  resources: {hint: m}\n", 1))
	docs := []string{
		sourceDoc(name, fmt.Sprintf(`{"connector": %q, "config": {"base_url": %q, "api_key": {"secret": %q}}}`, name, api.URL, setSecret(t, "k1"))),
		destinationDoc(name, `{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}`),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"schedule": {"every": "1h"}, "source": {"ref": %q}, "destination": {"ref": %q, "namespace": %q},
			"delivery": {"require": "at_least_once"}}}`, name, name, name, schema(name)),
	}
	if j := planOf(t, docs...).GetJob(); j.GetSize().GetPreset() != "m" || j.GetReason() != apiv1.JobSizeReason_JOB_SIZE_REASON_HINT || j.GetPool() != "default" {
		t.Errorf("plan's job %v", j)
	}
	apply(t, docs...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if got := latestAttempts(t, name); len(got) != 1 || got[0].cpu != 2000 || got[0].why != "hint" {
		t.Errorf("attempts %+v, want one sized m by the hint", got)
	}
}
