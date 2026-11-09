package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
)

type fakeApply struct {
	apiv1connect.UnimplementedApplyServiceHandler
}

func (fakeApply) Apply(_ context.Context, req *connect.Request[apiv1.ApplyRequest]) (*connect.Response[apiv1.ApplyResponse], error) {
	if !req.Msg.GetValidateOnly() {
		return nil, connect.NewError(connect.CodeInternal, errors.New("want validate only"))
	}
	return connect.NewResponse(&apiv1.ApplyResponse{
		Resources: []*apiv1.AppliedResource{
			{Name: "workspaces/default/sources/shop", Action: apiv1.ApplyAction_APPLY_ACTION_UPDATED, Version: "0123456789abcdef"},
			{Name: "workspaces/default/destinations/lake", Action: apiv1.ApplyAction_APPLY_ACTION_CREATED, Version: "abababababababab"},
			{Name: "workspaces/default/pipelines/shop-to-lake", Action: apiv1.ApplyAction_APPLY_ACTION_CREATED},
		},
		Dependents: []*apiv1.AppliedResource{
			{Name: "workspaces/default/pipelines/shop-to-dw", Action: apiv1.ApplyAction_APPLY_ACTION_UPDATED, Version: "fedcba9876543210"},
		},
		Plans: []*apiv1.PipelinePlan{{
			Pipeline: "workspaces/default/pipelines/shop-to-lake",
		}, {
			Pipeline: "workspaces/default/pipelines/shop-to-dw", Version: "fedcba9876543210",
			Changes: []*apiv1.PlanChange{{Field: "source.version", Before: `"01"`, After: `"02"`}, {Field: "delivery.require", After: `"at_least_once"`}},
			Streams: []*apiv1.StreamPlan{
				{Name: "orders", Level: apiv1.DeliveryLevel_DELIVERY_LEVEL_EXACTLY_ONCE, Reason: "exact replay", Write: apiv1.WriteMode_WRITE_MODE_OVERWRITE, TypeChanges: true},
				{Name: "events", Level: apiv1.DeliveryLevel_DELIVERY_LEVEL_AT_LEAST_ONCE, Reason: "superset replay, no primary key, and not collapsed", Write: apiv1.WriteMode_WRITE_MODE_APPEND},
			},
			Problems: []string{`delivery.require: stream "events" gets at_least_once`},
			Notes:    []string{"a note"},
			Warnings: []*apiv1.PlanWarning{{Stream: "logs", Message: "it has no primary key"}},
			Job:      &apiv1.JobPlan{Size: &apiv1.JobSize{Preset: "m", CpuMillis: 2000, MemoryBytes: 4 << 30}, Reason: apiv1.JobSizeReason_JOB_SIZE_REASON_HISTORY, Pool: "batch"},
		}},
	}), nil
}

// openetl apply lists the pipelines that a change gave new versions, and
// openetl plan shows their plans. A pipeline a dry run can't give a version
// shows that only the apply knows it.
func TestApplyListsDependents(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(apiv1connect.NewApplyServiceHandler(fakeApply{}))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	t.Setenv("OPENETL_API_KEY", "oetl_test")
	file := filepath.Join(t.TempDir(), "shop.yaml")
	if err := os.WriteFile(file, []byte("apiVersion: openetl.io/v1alpha1\nkind: Source\nmetadata: {name: shop}\nspec: {connector: faker}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run(t.Context(), []string{"apply", "--api-url", srv.URL, "--dry-run", "-f", file}, &out); err != nil {
		t.Fatal(err)
	}
	want := "would be updated workspaces/default/sources/shop (version 0123456789ab)\n" +
		"would be created workspaces/default/destinations/lake (version abababababab)\n" +
		"would be created workspaces/default/pipelines/shop-to-lake (version known after apply)\n" +
		"would be updated workspaces/default/pipelines/shop-to-dw (version fedcba987654), which uses what changed\n"
	if w := want + "\nwarnings for workspaces/default/pipelines/shop-to-dw:\n  logs: it has no primary key\n"; out.String() != w {
		t.Errorf("printed\n%s\nwant\n%s", out.String(), w)
	}

	out.Reset()
	err := run(t.Context(), []string{"plan", "--api-url", srv.URL, "-f", file}, &out)
	if err == nil || !strings.Contains(err.Error(), "apply would refuse it") {
		t.Errorf("a plan with problems: %v", err)
	}
	want += `
workspaces/default/pipelines/shop-to-lake (version known after apply)

workspaces/default/pipelines/shop-to-dw (version fedcba987654)
  changes:
    source.version: "01" -> "02"
    delivery.require: (unset) -> "at_least_once"
  job size: m (2 vCPU, 4 GiB), from its recent peaks, in pool batch
  streams:
    orders  overwrite  exactly_once   exact replay; columns' types change in place
    events  append     at_least_once  superset replay, no primary key, and not collapsed
  problems:
    delivery.require: stream "events" gets at_least_once
  warnings:
    logs: it has no primary key
  notes:
    a note
`
	if out.String() != want {
		t.Errorf("printed\n%s\nwant\n%s", out.String(), want)
	}
}

// A fake of what the checks after an apply call: a source that passes, a
// destination whose check the test sets, and a pipeline that uses both.
type fakeChecks struct {
	apiv1connect.UnimplementedSourceServiceHandler
	apiv1connect.UnimplementedDestinationServiceHandler
	apiv1connect.UnimplementedPipelineServiceHandler
	applied     *apiv1.ApplyResponse
	destination *apiv1.CheckResult
	problems    []string
	mu          sync.Mutex
	calls       []string
}

func (f *fakeChecks) called(what string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, what)
}

type fakeAppliedHandler struct {
	apiv1connect.UnimplementedApplyServiceHandler
	f *fakeChecks
}

func (h fakeAppliedHandler) Apply(context.Context, *connect.Request[apiv1.ApplyRequest]) (*connect.Response[apiv1.ApplyResponse], error) {
	return connect.NewResponse(h.f.applied), nil
}

func (f *fakeChecks) CheckSource(_ context.Context, r *connect.Request[apiv1.CheckSourceRequest]) (*connect.Response[apiv1.CheckSourceResponse], error) {
	f.called("check " + r.Msg.GetName() + "@" + r.Msg.GetDataPlane())
	return connect.NewResponse(&apiv1.CheckSourceResponse{Result: &apiv1.CheckResult{Ok: true}}), nil
}

func (f *fakeChecks) DiscoverSource(_ context.Context, r *connect.Request[apiv1.DiscoverSourceRequest]) (*connect.Response[apiv1.DiscoverSourceResponse], error) {
	f.called("discover " + r.Msg.GetName())
	return connect.NewResponse(&apiv1.DiscoverSourceResponse{Catalog: &apiv1.Catalog{Streams: []*apiv1.CatalogStream{{Name: "orders"}, {Name: "users"}}}}), nil
}

func (f *fakeChecks) CheckDestination(_ context.Context, r *connect.Request[apiv1.CheckDestinationRequest]) (*connect.Response[apiv1.CheckDestinationResponse], error) {
	f.called("check " + r.Msg.GetName() + "@" + r.Msg.GetDataPlane())
	return connect.NewResponse(&apiv1.CheckDestinationResponse{Result: f.destination}), nil
}

func (f *fakeChecks) GetPipeline(_ context.Context, r *connect.Request[apiv1.GetPipelineRequest]) (*connect.Response[apiv1.GetPipelineResponse], error) {
	return connect.NewResponse(&apiv1.GetPipelineResponse{Pipeline: &apiv1.Pipeline{Name: r.Msg.GetName(), Spec: &apiv1.PipelineSpec{
		DataPlane: "dp1", Source: &apiv1.PipelineSource{Ref: "shop"}, Destination: &apiv1.PipelineDestination{Ref: "lake"},
	}}}), nil
}

func (f *fakeChecks) GetPipelinePlan(_ context.Context, r *connect.Request[apiv1.GetPipelinePlanRequest]) (*connect.Response[apiv1.GetPipelinePlanResponse], error) {
	return connect.NewResponse(&apiv1.GetPipelinePlanResponse{Plan: &apiv1.PipelinePlan{Pipeline: r.Msg.GetName(), Version: "0123456789abcdef", Problems: f.problems}}), nil
}

func checksServer(t *testing.T, f *fakeChecks) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(apiv1connect.NewApplyServiceHandler(fakeAppliedHandler{f: f}))
	mux.Handle(apiv1connect.NewSourceServiceHandler(f))
	mux.Handle(apiv1connect.NewDestinationServiceHandler(f))
	mux.Handle(apiv1connect.NewPipelineServiceHandler(f))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	t.Setenv("OPENETL_API_KEY", "oetl_test")
	return srv.URL
}

func documentFile(t *testing.T) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "shop.yaml")
	if err := os.WriteFile(file, []byte("apiVersion: openetl.io/v1alpha1\nkind: Source\nmetadata: {name: shop}\nspec: {connector: faker}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return file
}

func createdPipeline() *apiv1.ApplyResponse {
	return &apiv1.ApplyResponse{
		Resources: []*apiv1.AppliedResource{
			{Name: "workspaces/default/sources/shop", Action: apiv1.ApplyAction_APPLY_ACTION_UNCHANGED, Version: "0123456789abcdef"},
			{Name: "workspaces/default/pipelines/shop-to-lake", Action: apiv1.ApplyAction_APPLY_ACTION_CREATED, Version: "abababababababab"},
		},
	}
}

// What an apply changed is checked, each connection once, on the pipeline's
// data plane, and a check that fails warns without failing the apply.
func TestApplyChecksWhatChanged(t *testing.T) {
	f := &fakeChecks{
		applied:     createdPipeline(),
		destination: &apiv1.CheckResult{Code: "AUTH_INVALID", Message: "no CREATE on schema"},
		problems:    []string{`the source has no stream "gone"`},
	}
	url := checksServer(t, f)
	var out bytes.Buffer
	if err := run(t.Context(), []string{"apply", "--api-url", url, "-f", documentFile(t)}, &out); err != nil {
		t.Fatalf("a failing check failed the apply: %v", err)
	}
	want := "unchanged workspaces/default/sources/shop (version 0123456789ab)\n" +
		"created workspaces/default/pipelines/shop-to-lake (version abababababab)\n" +
		"checked source shop\n" +
		"\nthe apply stands, but its checks found:\n" +
		"  destination lake: AUTH_INVALID: no CREATE on schema\n" +
		"  workspaces/default/pipelines/shop-to-lake: the source has no stream \"gone\"\n"
	if out.String() != want {
		t.Errorf("printed\n%s\nwant\n%s", out.String(), want)
	}
	slices.Sort(f.calls)
	if got := strings.Join(f.calls, ", "); got != "check workspaces/default/destinations/lake@dp1, check workspaces/default/sources/shop@dp1, discover workspaces/default/sources/shop" {
		t.Errorf("calls: %s", got)
	}
}

func TestApplyWithoutChangesOrWithNoCheckChecksNothing(t *testing.T) {
	unchanged := createdPipeline()
	unchanged.Resources[1].Action = apiv1.ApplyAction_APPLY_ACTION_UNCHANGED
	for name, c := range map[string]struct {
		applied *apiv1.ApplyResponse
		flags   []string
	}{
		"nothing changed": {applied: unchanged},
		"--no-check":      {applied: createdPipeline(), flags: []string{"--no-check"}},
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeChecks{applied: c.applied, destination: &apiv1.CheckResult{Ok: true}}
			url := checksServer(t, f)
			var out bytes.Buffer
			if err := run(t.Context(), append([]string{"apply", "--api-url", url, "-f", documentFile(t)}, c.flags...), &out); err != nil {
				t.Fatal(err)
			}
			if len(f.calls) != 0 || strings.Contains(out.String(), "checked") {
				t.Errorf("calls %v, printed\n%s", f.calls, out.String())
			}
		})
	}
}

func TestCheckPipeline(t *testing.T) {
	f := &fakeChecks{applied: createdPipeline(), destination: &apiv1.CheckResult{Ok: true}}
	url := checksServer(t, f)
	var out bytes.Buffer
	if err := run(t.Context(), []string{"check", "--api-url", url, "pipeline", "shop-to-lake"}, &out); err != nil {
		t.Fatal(err)
	}
	want := "source shop passed its check, and offers 2 streams\ndestination lake passed its check\n\nworkspaces/default/pipelines/shop-to-lake (version 0123456789ab)\n"
	if out.String() != want {
		t.Errorf("printed\n%s\nwant\n%s", out.String(), want)
	}

	f.destination = &apiv1.CheckResult{Code: "AUTH_INVALID", Message: "denied"}
	out.Reset()
	err := run(t.Context(), []string{"check", "--api-url", url, "pipeline", "shop-to-lake"}, &out)
	if err == nil || !strings.Contains(out.String(), "destination lake failed its check: AUTH_INVALID: denied") {
		t.Errorf("a failing check: %v, printed\n%s", err, out.String())
	}

	f.destination, f.problems = &apiv1.CheckResult{Ok: true}, []string{"a problem"}
	if err := run(t.Context(), []string{"check", "--api-url", url, "pipeline", "shop-to-lake"}, &out); err == nil {
		t.Error("a plan with problems passed")
	}
}
