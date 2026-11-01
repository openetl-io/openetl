//go:build e2e

package e2e

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
)

// A pipeline version pins its source's config: changing the source gives the
// pipeline a new version, which the next sync runs, and the old version keeps
// the old config.
func TestVersionsPinConfigs(t *testing.T) {
	startRunner(t)
	name := unique("versions")
	res := applyAll(t, false, pipelineDocs(name, `{"seed": 4, "count": 50}`, `{}`)...)
	v1 := res.GetResources()[2].GetVersion()
	if len(res.GetDependents()) != 0 {
		t.Errorf("creating a pipeline with its source: dependents %v", res.GetDependents())
	}
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}

	changed := pipelineDocs(name, `{"seed": 4, "count": 80}`, `{}`)[0]
	dry := applyAll(t, true, changed)
	if d := dry.GetDependents(); len(d) != 1 || d[0].GetName() != "workspaces/default/pipelines/"+name || d[0].GetAction() != apiv1.ApplyAction_APPLY_ACTION_UPDATED {
		t.Fatalf("validating the change: dependents %v", d)
	}
	pipelineVersion := func() string {
		v, err := query[string](t, "SELECT version FROM pipelines WHERE name = $1", name)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if v := pipelineVersion(); v != v1 {
		t.Fatalf("validating changed the version to %s", v)
	}
	deps := applyAll(t, false, changed).GetDependents()
	if len(deps) != 1 || deps[0].GetVersion() != dry.GetDependents()[0].GetVersion() || deps[0].GetVersion() == v1 {
		t.Fatalf("changing the source: dependents %v; validating said %v", deps, dry.GetDependents())
	}
	v2 := deps[0].GetVersion()
	if v := pipelineVersion(); v != v2 {
		t.Fatalf("the pipeline's version is %s, want %s", v, v2)
	}
	if p, err := query[string](t, "SELECT provenance FROM pipeline_versions WHERE version = $1", v2); err != nil || p != "api:doc 1" {
		t.Errorf("v2's provenance is %q, %v", p, err)
	}

	id, err := query[string](t, "SELECT id::text FROM pipelines WHERE name = $1", name)
	if err != nil {
		t.Fatal(err)
	}
	api := dataplanev1connect.NewDataPlaneServiceClient(env.runner, env.apiURL)
	for v, want := range map[string]float64{v1: 50, v2: 80} {
		spec, err := api.GetSyncSpec(t.Context(), connect.NewRequest(&dataplanev1.GetSyncSpecRequest{PipelineId: id, Version: v}))
		if err != nil {
			t.Fatal(err)
		}
		if got := spec.Msg.GetSource().GetConfig().GetFields()["count"].GetNumberValue(); got != want {
			t.Errorf("version %.12s: count %v, want %v", v, got, want)
		}
	}

	users := func() int {
		n, err := query[int](t, "SELECT count(*) FROM "+pgx.Identifier{schema(name), "users"}.Sanitize())
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	before := users()
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if ran, err := query[string](t, `SELECT s.version FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id
		WHERE p.name = $1 ORDER BY s.start_time DESC LIMIT 1`, name); err != nil || ran != v2 {
		t.Errorf("the sync ran version %s, %v; want %s", ran, err, v2)
	}
	if n := users() - before; n != 80 {
		t.Errorf("the sync loaded %d users, want 80", n)
	}

	res = applyAll(t, false, pipelineDocs(name, `{"seed": 4, "count": 80}`, `{}`)...)
	for _, r := range res.GetResources() {
		if r.GetAction() != apiv1.ApplyAction_APPLY_ACTION_UNCHANGED {
			t.Errorf("reapplied %s: %v", r.GetName(), r.GetAction())
		}
	}
	if len(res.GetDependents()) != 0 {
		t.Errorf("reapplying: dependents %v", res.GetDependents())
	}
}
