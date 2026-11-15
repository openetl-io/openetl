package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/engineclient"
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/sqlplan"
)

// openetl plan lists each stateful operator: where its statement is, its
// output and ID, what it is against the current version, and what it keeps.
func TestPlanShowsState(t *testing.T) {
	minute := durationpb.New(time.Minute)
	var out bytes.Buffer
	printOperators(&out, []*apiv1.OperatorPlan{
		{Id: "window-a", Output: "revenue", Line: 1, Column: 1, Window: "tumble", Size: minute, EventTime: "at", Keys: []string{"region"},
			Aggregates: []string{"sum(amount)"}, Lateness: durationpb.New(30 * time.Second), Retention: durationpb.New(90 * time.Second),
			Change: apiv1.OperatorChange_OPERATOR_CHANGE_CHANGED, Replaces: "window-b", Backfill: []string{"orders"}},
		{Id: "window-c", Output: "hops", Line: 6, Column: 1, Window: "hop", Size: durationpb.New(5 * time.Minute), Slide: minute, EventTime: "at",
			Aggregates: []string{"count(*)"}, Lateness: durationpb.New(0), Retention: durationpb.New(5 * time.Minute), Change: apiv1.OperatorChange_OPERATOR_CHANGE_KEPT},
		{Id: "window-d", Output: "gone", Change: apiv1.OperatorChange_OPERATOR_CHANGE_DROPPED},
	})
	want := `  state:
    1:1  revenue  window-a  changed  tumble 1m0s by at on region: sum(amount); keeps a window's state 1m30s of event time (lateness 30s); starts empty, in place of window-b; a backfill reads again orders
    6:1  hops     window-c  kept     hop 5m0s every 1m0s by at: count(*); keeps a window's state 5m0s of event time (lateness 0s)
    -    gone     window-d  dropped  its state is dropped
`
	if out.String() != want {
		t.Errorf("printed\n%s\nwant\n%s", out.String(), want)
	}
}

// Planned by the engine, an edit that renames and reorders keeps the
// operator, and one that changes the window replaces it.
func TestPlanShowsWhatAnEditDoesToState(t *testing.T) {
	bin := os.Getenv("OPENETL_ENGINE")
	if bin == "" {
		t.Skip("set OPENETL_ENGINE to an openetl-engine binary; make test does")
	}
	cat := &apiv1.Catalog{Streams: []*apiv1.CatalogStream{{
		Name: "orders", PrimaryKey: []string{"id"}, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_STREAM},
		Columns: []*apiv1.CatalogColumn{{Name: "id", Type: "int64"}, {Name: "region", Type: "utf8"}, {Name: "amount", Type: "decimal(12, 2)"}, {Name: "created_at", Type: "timestamp[us, tz=UTC]"}},
	}}}
	spec := func(sql string) *apiv1.PipelineSpec {
		return &apiv1.PipelineSpec{
			Mode: apiv1.PipelineMode_PIPELINE_MODE_CONTINUOUS,
			Source: &apiv1.PipelineSource{Streams: []*apiv1.PipelineStream{{Name: "orders", EventTime: "created_at",
				Watermark: &apiv1.Watermark{Delay: durationpb.New(5 * time.Second), Lateness: durationpb.New(time.Minute)}}}},
			Destination: &apiv1.PipelineDestination{Write: apiv1.WriteMode_WRITE_MODE_MERGE},
			Transform:   &apiv1.Transform{Engine: &apiv1.Transform_Sql{Sql: &apiv1.SqlTransform{Sql: sql}}},
		}
	}
	planned := func(sql string) (*apiv1.PipelineSpec, *sqlplan.Answer) {
		sp := spec(sql)
		req, unknown := plan.SQLRequest(sp, cat, nil)
		if len(unknown) != 0 {
			t.Fatal(unknown)
		}
		a, err := engineclient.Plan(t.Context(), engineclient.Options{Binary: bin}, req)
		if err != nil || len(a.Errors) != 0 {
			t.Fatalf("%s: %+v, %v", sql, a, err)
		}
		return sp, &a
	}
	_, before := planned("INSERT INTO revenue_by_minute\nSELECT window_start, region, sum(amount) AS revenue, count(*) AS orders\nFROM orders\n" +
		"GROUP BY tumble(created_at, INTERVAL '1 minute'), region;\n\n" +
		"INSERT INTO orders_by_hour\nSELECT window_start, count(*) AS orders\nFROM orders\nGROUP BY tumble(created_at, INTERVAL '1 hour')")
	applied := plan.SQLPlan(before, &apiv1.SqlPlan{})
	sp, after := planned("INSERT INTO revenue_by_minute\nSELECT count(*) AS n, sum(o.amount) AS revenue, o.region, window_start AS minute\nFROM orders AS o\n" +
		"GROUP BY tumble(o.created_at, INTERVAL '60 seconds'), o.region;\n\n" +
		"INSERT INTO orders_by_hour\nSELECT window_start, count(*) AS orders\nFROM orders\nGROUP BY tumble(created_at, INTERVAL '30 minutes')")
	ops, warnings, notes := plan.Operators(sp, cat, after, applied, map[string]bool{"orders": true})
	var out bytes.Buffer
	printOperators(&out, ops)
	t.Logf("\n%s", out.String())
	for _, w := range warnings {
		t.Log(w.GetMessage())
	}
	if len(ops) != 2 || ops[0].GetChange() != apiv1.OperatorChange_OPERATOR_CHANGE_KEPT || ops[0].GetId() != applied.GetOperators()[0] ||
		ops[1].GetChange() != apiv1.OperatorChange_OPERATOR_CHANGE_CHANGED || ops[1].GetReplaces() != applied.GetOperators()[1] ||
		len(warnings) != 1 || len(notes) != 0 || !strings.Contains(out.String(), "revenue_by_minute  window-") {
		t.Errorf("%v\n%v\n%v", ops, warnings, notes)
	}
}
