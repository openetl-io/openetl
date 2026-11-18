package main

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/engineclient"
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/destination"
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
		{Id: "over-e", Output: "totals", Line: 9, Column: 1, Class: "over", EventTime: "at", Keys: []string{"region"},
			Aggregates: []string{"sum(amount) range unbounded", "lag(amount, 1)"}, Change: apiv1.OperatorChange_OPERATOR_CHANGE_NEW},
	})
	want := `  state:
    1:1  revenue  window-a  changed  tumble 1m0s by at on region: sum(amount); keeps a window's state 1m30s of event time (lateness 30s); starts empty, in place of window-b; a backfill reads again orders
    6:1  hops     window-c  kept     hop 5m0s every 1m0s by at: count(*); keeps a window's state 5m0s of event time (lateness 0s)
    -    gone     window-d  dropped  its state is dropped
    9:1  totals   over-e    new      over by at on region: sum(amount) range unbounded, lag(amount, 1); keeps each key's frame, and its rows until the watermark passes them; starts empty
`
	if out.String() != want {
		t.Errorf("printed\n%s\nwant\n%s", out.String(), want)
	}
}

// A dedupe's line says which row it keeps, how long it remembers a key and
// what it guarantees.
func TestPlanShowsDedupes(t *testing.T) {
	var out bytes.Buffer
	dedupe := func(id, keep, guarantee string, retention *durationpb.Duration) *apiv1.OperatorPlan {
		return &apiv1.OperatorPlan{Id: id, Output: id, Line: 1, Column: 1, Class: "over", EventTime: "at", Keys: []string{"event_id"}, Keep: keep,
			Guarantee: guarantee, Retention: retention, Change: apiv1.OperatorChange_OPERATOR_CHANGE_KEPT}
	}
	printOperators(&out, []*apiv1.OperatorPlan{
		dedupe("a", "first", "exactly_once_within", durationpb.New(24*time.Hour)),
		dedupe("b", "first", "exactly_once", nil),
		dedupe("c", "first", "", nil),
		dedupe("d", "latest", "exactly_once", nil),
	})
	want := `  state:
    1:1  a  a  kept  dedupe first by at on event_id; remembers a key 24h0m0s of event time from its first row: exactly once within 24h0m0s of event time
    1:1  b  b  kept  dedupe first by at on event_id; remembers every key: exactly_once, and its state grows with the number of keys
    1:1  c  c  kept  dedupe first by at on event_id; its stream sets no dedupe.retention
    1:1  d  d  kept  dedupe latest by at on event_id; keeps each key's latest row's time, and writes it as an upsert: exactly_once, and its state grows with the number of keys
`
	if out.String() != want {
		t.Errorf("printed\n%s\nwant\n%s", out.String(), want)
	}
}

// Planned by the engine, a dedupe that keeps each key's first row is a
// problem until its stream sets dedupe.retention, and then reports it and
// what it guarantees; one that keeps the latest is merged.
func TestPlanOfADedupeThroughTheEngine(t *testing.T) {
	bin := os.Getenv("OPENETL_ENGINE")
	if bin == "" {
		t.Skip("set OPENETL_ENGINE to an openetl-engine binary; make test does")
	}
	cat := &apiv1.Catalog{Streams: []*apiv1.CatalogStream{{
		Name: "events", SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_STREAM},
		Columns: []*apiv1.CatalogColumn{{Name: "event_id", Type: "utf8"}, {Name: "kind", Type: "utf8"}, {Name: "at", Type: "timestamp[us, tz=UTC]"}},
	}}}
	sql := "INSERT INTO first_events\nSELECT event_id, kind, at FROM events\nQUALIFY row_number() OVER (PARTITION BY event_id ORDER BY at) = 1;\n" +
		"INSERT INTO latest_events\nSELECT event_id, kind, at FROM events\nQUALIFY row_number() OVER (PARTITION BY event_id ORDER BY at DESC) = 1"
	planned := func(d *apiv1.Dedupe) ([]*apiv1.OperatorPlan, []error, []*apiv1.StreamPlan) {
		sp := &apiv1.PipelineSpec{
			Mode:        apiv1.PipelineMode_PIPELINE_MODE_CONTINUOUS,
			Source:      &apiv1.PipelineSource{Streams: []*apiv1.PipelineStream{{Name: "events", EventTime: "at", Dedupe: d}}},
			Destination: &apiv1.PipelineDestination{},
			Transform:   &apiv1.Transform{Engine: &apiv1.Transform_Sql{Sql: &apiv1.SqlTransform{Sql: sql, Passthrough: apiv1.Passthrough_PASSTHROUGH_NONE}}},
		}
		req, unknown := plan.SQLRequest(sp, cat, nil)
		if len(unknown) != 0 {
			t.Fatal(unknown)
		}
		a, err := engineclient.Plan(t.Context(), engineclient.Options{Binary: bin}, req)
		if err != nil || len(a.Errors) != 0 {
			t.Fatalf("%+v, %v", a, err)
		}
		caps := destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopeStream},
			WriteModes: []destination.WriteMode{destination.Append, destination.Merge}}
		streams, problems, _ := plan.PipelineSQL(sp, cat, caps, &a)
		ops, _, _ := plan.Operators(sp, cat, &a, nil, nil, nil)
		return ops, problems, streams
	}
	_, problems, _ := planned(nil)
	if err := errors.Join(problems...); err == nil || !strings.Contains(err.Error(),
		`transform.sql:1:1: the dedupe writing "first_events", ROW_NUMBER() … = 1 in ascending order of the event time, keeps each key's first row, and stream "events" sets no dedupe.retention`) {
		t.Errorf("without a retention: %v", err)
	}
	ops, problems, streams := planned(&apiv1.Dedupe{Retention: durationpb.New(24 * time.Hour)})
	if len(problems) != 0 {
		t.Errorf("problems %v", problems)
	}
	var out bytes.Buffer
	printOperators(&out, ops)
	for _, want := range []string{
		"first_events   dedupe-", "dedupe first by at on event_id; remembers a key 24h0m0s of event time from its first row: exactly once within 24h0m0s of event time",
		"latest_events  dedupe-", "dedupe latest by at on event_id; keeps each key's latest row's time, and writes it as an upsert: exactly_once",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("printed\n%s\nwithout %q", out.String(), want)
		}
	}
	// Over a superset stream, each output is written once a key: within the
	// retention, or for good.
	got := map[string]string{}
	for _, s := range streams {
		got[s.GetName()] = fmt.Sprintf("%v %v %s", s.GetWrite(), s.GetLevel(), s.GetReason())
	}
	want := map[string]string{
		"first_events": "WRITE_MODE_APPEND DELIVERY_LEVEL_EXACTLY_ONCE exactly once within 24h0m0s of event time: a dedupe's output over superset inputs, " +
			"which remembers a key 24h0m0s from its first row and writes a repeat after that again",
		"latest_events": "WRITE_MODE_MERGE DELIVERY_LEVEL_EXACTLY_ONCE a dedupe's output over superset inputs, which remembers every key, so its state grows with the number of keys",
	}
	if !maps.Equal(got, want) {
		t.Errorf("streams %v, want %v", got, want)
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
	ops, warnings, notes := plan.Operators(sp, cat, after, applied, map[string]bool{"orders": true}, nil)
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
