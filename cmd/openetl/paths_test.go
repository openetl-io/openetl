package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/engineclient"
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// openetl plan shows each stream's way to the destination: what its source
// gives, each operator with what it needs, and where rows are exchanged. An
// aggregate exchanged on its group key reaches its writer with that one
// exchange.
func TestPlanShowsPartitioning(t *testing.T) {
	stream := func(name string, p plan.Path) *apiv1.StreamPlan {
		sp := &apiv1.StreamPlan{Name: name}
		sp.Partitioning, sp.Operators = p.API()
		return sp
	}
	key := []string{"id"}
	rename := sqlplan.Statement{At: sqlplan.Position{Line: 1, Column: 1}, Passes: map[string]string{"id": "customer_id"}}
	aggregate := plan.Operator{Kind: plan.OperatorStatement, At: sqlplan.Position{Line: 5, Column: 1},
		Need: plan.Need{Columns: []string{"k"}}, Passes: map[string]string{"k": "k"}}
	var out bytes.Buffer
	printPaths(&out, []*apiv1.StreamPlan{
		stream("orders", plan.Place(plan.SourcePartitioning(key, true, false), plan.WriterOperator(key, false))),
		stream("changes", plan.Place(plan.SourcePartitioning(key, true, true), plan.WriterOperator(key, false))),
		stream("logs", plan.Place(plan.SourcePartitioning(nil, false, false), plan.WriterOperator(nil, false))),
		stream("contacts", plan.StatementPath(plan.SourcePartitioning(key, true, false), rename, []string{"customer_id"}, false)),
		stream("hourly", plan.Place(plan.Partitioning{}, aggregate, plan.WriterOperator([]string{"window_start", "k"}, false))),
		stream("audit", plan.Place(plan.Partitioning{}, plan.WriterOperator(key, true))),
		{Name: "unplanned"},
	})
	want := `  partitioning:
    orders    split by id -> writer, needs id
    changes   unpartitioned -> exchange by key group on id -> writer, needs id
    logs      unpartitioned -> writer
    contacts  split by id -> statement 1:1 -> split by customer_id -> writer, needs customer_id
    hourly    unpartitioned -> exchange by key group on k -> statement 5:1, needs k -> writer, needs window_start, k
    audit     unpartitioned -> on one lane -> writer, needs id
`
	if out.String() != want {
		t.Errorf("printed\n%s\nwant\n%s", out.String(), want)
	}
}

// openetl plan of a real group window, planned by the engine: its stream is
// exchanged once, on the window's group key, and its output reaches the
// writer with no second exchange, as its key holds the group key.
func TestPlanShowsAWindowsOneExchange(t *testing.T) {
	bin := os.Getenv("OPENETL_ENGINE")
	if bin == "" {
		t.Skip("set OPENETL_ENGINE to an openetl-engine binary; make test does")
	}
	sql := "INSERT INTO minutes\nSELECT window_start, region, sum(amount) AS revenue\nFROM orders\nGROUP BY tumble(at, INTERVAL '1 minute'), region"
	spec := &apiv1.PipelineSpec{
		Delivery:    &apiv1.Delivery{Require: apiv1.DeliveryLevel_DELIVERY_LEVEL_AT_LEAST_ONCE},
		Source:      &apiv1.PipelineSource{Streams: []*apiv1.PipelineStream{{Name: "orders", EventTime: "at"}}},
		Destination: &apiv1.PipelineDestination{Write: apiv1.WriteMode_WRITE_MODE_MERGE},
		Transform:   &apiv1.Transform{Engine: &apiv1.Transform_Sql{Sql: &apiv1.SqlTransform{Sql: sql, Passthrough: apiv1.Passthrough_PASSTHROUGH_NONE}}},
	}
	cat := &apiv1.Catalog{Streams: []*apiv1.CatalogStream{{
		Name: "orders", PrimaryKey: []string{"id"}, SplitsByKey: true, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_FULL_REFRESH},
		Columns: []*apiv1.CatalogColumn{{Name: "id", Type: "int64"}, {Name: "region", Type: "utf8"}, {Name: "amount", Type: "decimal(12, 2)"}, {Name: "at", Type: "timestamp[us, tz=UTC]"}},
	}}}
	req, unknown := plan.SQLRequest(spec, cat, nil)
	if len(unknown) != 0 {
		t.Fatal(unknown)
	}
	a, err := engineclient.Plan(t.Context(), engineclient.Options{Binary: bin}, req)
	if err != nil || len(a.Errors) != 0 {
		t.Fatalf("%+v, %v", a, err)
	}
	caps := destination.Capabilities{Class: destination.StageAndApply, WriterLanes: 8, Scopes: []destination.Scope{destination.ScopeStream},
		WriteModes: []destination.WriteMode{destination.Append, destination.Merge}}
	plans, problems, _ := plan.PipelineSQL(spec, cat, caps, &a)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	// On one pod and on workers alike: the plan is the same, the exchange
	// crossing the workers to the keyed lanes that own each row's key group,
	// which feed their own writer lanes.
	for _, asked := range []uint32{0, 3} {
		spec.Resources = &apiv1.JobResources{Workers: asked}
		workers, onePod, warnings := plan.Workers(spec, cat, caps, false, &a)
		if workers != int(asked) || onePod != "" || len(warnings) != 0 {
			t.Fatalf("%d workers asked: %d, %q, %v", asked, workers, onePod, warnings)
		}
		var out bytes.Buffer
		printPlan(&out, &apiv1.PipelinePlan{Pipeline: "shop", Job: &apiv1.JobPlan{Workers: uint32(workers)}, Transform: plan.Transform(spec, cat, &a), Streams: plans})
		wants := []string{
			"INSERT INTO minutes  group_window  key window_start, region  exchange by region  reads orders; keeps state as operator window-",
			"minutes  split by id -> exchange by key group on region -> statement 1:1, needs region -> writer, needs window_start, region\n",
		}
		if asked > 0 {
			wants = append(wants, "workers: 3, each of the job's size, and their coordinator\n")
		}
		for _, want := range wants {
			if !strings.Contains(out.String(), want) {
				t.Errorf("%d workers: printed\n%s\nwithout %q", asked, out.String(), want)
			}
		}
		if n := strings.Count(out.String(), "exchange by key group"); n != 1 {
			t.Errorf("%d workers: %d exchanges:\n%s", asked, n, out.String())
		}
	}
}
