//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
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

// The plan uses the source's latest catalog and the destination's declared
// capabilities, and Apply refuses a change whose plan fails.
func TestPlan(t *testing.T) {
	startRunner(t)
	name := unique("plan")
	faker := `{"seed": 5, "count": 20, "overlap": 5}`
	first := applyAll(t, false, pipelineDocs(name, faker, `{}`)...)
	if p := first.GetPlans(); len(p) != 1 || len(p[0].GetStreams()) != 0 || len(p[0].GetNotes()) != 1 || !strings.Contains(p[0].GetNotes()[0], "hasn't been discovered") {
		t.Fatalf("before discovery: %v", p)
	}
	sources := apiv1connect.NewSourceServiceClient(env.http, env.apiURL)
	if _, err := sources.DiscoverSource(t.Context(), connect.NewRequest(&apiv1.DiscoverSourceRequest{Name: "workspaces/default/sources/" + name})); err != nil {
		t.Fatal(err)
	}

	plan := applyAll(t, true, pipelineDocs(name, faker, `{}`)...).GetPlans()[0]
	var got []string
	for _, s := range plan.GetStreams() {
		got = append(got, fmt.Sprintf("%s %v %v", s.GetName(), s.GetLevel(), s.GetDedupe()))
	}
	if want := "users DELIVERY_LEVEL_EXACTLY_ONCE true, purchases DELIVERY_LEVEL_EXACTLY_ONCE true"; strings.Join(got, ", ") != want ||
		len(plan.GetProblems())+len(plan.GetNotes())+len(plan.GetChanges()) != 0 {
		t.Errorf("into Postgres: streams %v, want %s; plan %v", got, want, plan)
	}

	toParquet := docs(name, `{"connector": "faker", "config": `+faker+`}`, parquetDest(t.TempDir()), `{}`)
	res := applyAll(t, true, toParquet...)
	plan = res.GetPlans()[0]
	fields := map[string]bool{}
	for _, c := range plan.GetChanges() {
		fields[c.GetField()] = true
	}
	if !fields["destination.version"] || !fields["destination.connector"] || !fields["destination.namespace"] {
		t.Errorf("changes %v", plan.GetChanges())
	}
	if p := plan.GetProblems(); len(p) != 2 || !strings.Contains(p[1], `stream "purchases" gets at_least_once (superset replay, and the destination can't dedupe), below exactly_once`) {
		t.Errorf("into Parquet: problems %v", p)
	}
	before, err := query[string](t, "SELECT version FROM pipelines WHERE name = $1", name)
	if err != nil {
		t.Fatal(err)
	}
	err = applyErr(t, toParquet...)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "workspaces/default/pipelines/"+name+`: delivery.require: stream "users"`) {
		t.Fatalf("applying a failing plan: %v", err)
	}
	if after, err := query[string](t, "SELECT version FROM pipelines WHERE name = $1", name); err != nil || after != before {
		t.Errorf("the refused apply changed the version from %s to %s, %v", before, after, err)
	}

	toParquet[2] = strings.Replace(toParquet[2], `"delivery": {}`, `"delivery": {"require": "at_least_once"}`, 1)
	res = applyAll(t, false, toParquet...)
	if p := res.GetPlans()[0]; len(p.GetProblems()) != 0 || p.GetStreams()[0].GetLevel() != apiv1.DeliveryLevel_DELIVERY_LEVEL_AT_LEAST_ONCE {
		t.Errorf("at least once into Parquet: %v", p)
	}
}
