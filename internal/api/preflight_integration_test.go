//go:build integration

package api_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"connectrpc.com/connect"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/internal/api"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/storetest"
)

// racingOps runs a preflight that passes, and meanwhile does what during
// does; a discover does nothing.
type racingOps struct{ during func(context.Context) }

func (o *racingOps) Run(ctx context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
	if in.Op != contract.OpPreflight {
		return contract.ConnectorOpResult{}, nil
	}
	o.during(ctx)
	passed := []contract.CheckItem{{Name: "connect", Title: "Connect", Status: "passed"}}
	return contract.ConnectorOpResult{Preflight: []contract.CheckGroup{
		{Role: "source", Stage: "check", Items: passed}, {Role: "destination", Stage: "check", Items: passed}, {Role: "sync", Stage: "plan"},
		{Role: "source", Stage: "preflight"}, {Role: "destination", Stage: "preflight"},
	}}, nil
}

// A preflight plans the version it preflighted, even when an apply replaced
// it meanwhile, so its result is of one version.
func TestPreflightPlansTheVersionItChecked(t *testing.T) {
	ctx := t.Context()
	url := storetest.NewDatabase(t)
	s, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.Bootstrap(ctx, "dp1", nil)
	if err != nil {
		t.Fatal(err)
	}
	ops := &racingOps{}
	srv := &api.Server{Store: s, Tenant: tenant, Schedules: &refreshes{}, DefaultDataPlane: "dp1", Authless: true, Logger: slog.New(slog.DiscardHandler), Ops: ops}
	h, err := srv.Handler()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	applier := apiv1connect.NewApplyServiceClient(http.DefaultClient, ts.URL)
	// typeChanges: allow gives the plan a warning, so a plan of the version
	// without it is told apart.
	apply := func(ctx context.Context, typeChanges string) string {
		t.Helper()
		res, err := applier.Apply(ctx, connect.NewRequest(&apiv1.ApplyRequest{Documents: []*apiv1.Document{{Content: []byte(`
apiVersion: openetl.io/v1alpha1
kind: Source
metadata: { name: gen }
spec: { connector: faker, config: { seed: 1, count: 10 } }
`)}, {Content: []byte(`
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata: { name: files }
spec: { connector: parquet, config: { url: "file:///tmp/openetl-preflight-test" } }
`)}, {Content: []byte(`
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata: { name: hourly }
spec:
  schedule: { every: 1h }
  source: { ref: gen }
  destination: { ref: files` + typeChanges + ` }
`)}}}))
		if err != nil {
			t.Fatal(err)
		}
		return res.Msg.GetPlans()[0].GetVersion()
	}
	checked := apply(ctx, ", typeChanges: allow")
	var replaced string
	ops.during = func(ctx context.Context) { replaced = apply(ctx, "") }

	res, err := apiv1connect.NewPipelineServiceClient(http.DefaultClient, ts.URL).PreflightPipeline(ctx,
		connect.NewRequest(&apiv1.PreflightPipelineRequest{Name: "workspaces/default/pipelines/hourly"}))
	if err != nil {
		t.Fatal(err)
	}
	if replaced == "" || replaced == checked {
		t.Fatalf("the apply during the preflight made version %q, and the preflight checked %q", replaced, checked)
	}
	out := res.Msg.GetResult()
	plan := out.GetParts()[2]
	typeChanges := slices.ContainsFunc(plan.GetItems(), func(it *apiv1.CheckItem) bool { return strings.Contains(it.GetMessage(), "typeChanges: allow") })
	if out.GetVersion() != checked || !typeChanges {
		t.Errorf("the preflight of version %s planned another: version %s, plan %v", checked, out.GetVersion(), plan)
	}
}
