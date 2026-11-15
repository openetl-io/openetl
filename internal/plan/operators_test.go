package plan

import (
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/sqlplan"
)

var windowCatalog = &apiv1.Catalog{Streams: []*apiv1.CatalogStream{
	{Name: "orders", SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_STREAM, apiv1.SyncMode_SYNC_MODE_FULL_REFRESH}},
	{Name: "clicks", SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_FULL_REFRESH}},
}}

func windowSpec(lateness time.Duration) *apiv1.PipelineSpec {
	return &apiv1.PipelineSpec{
		Mode: apiv1.PipelineMode_PIPELINE_MODE_CONTINUOUS,
		Source: &apiv1.PipelineSource{Streams: []*apiv1.PipelineStream{
			{Name: "orders", EventTime: "at", Watermark: &apiv1.Watermark{Lateness: durationpb.New(lateness)}},
			{Name: "clicks", EventTime: "at"},
		}},
		Transform: &apiv1.Transform{Engine: &apiv1.Transform_Sql{Sql: &apiv1.SqlTransform{Sql: "…"}}},
	}
}

func window(line uint32, output, input, id string) sqlplan.Statement {
	return sqlplan.Statement{At: sqlplan.Position{Line: line, Column: 1}, Output: output, Class: "group_window", Inputs: []string{input}, Operator: id,
		State: &sqlplan.State{Window: "tumble", SizeMicros: 60_000_000, EventTime: "at", Keys: []string{"region"}, Aggregates: []string{"sum(amount)", "count(*)"}}}
}

// Against the current version, an operator it has is kept, one in place of
// its operator writing the same output is changed, one writing an output it
// had no operator for is new, and one of its operators no statement keeps
// is dropped. A new or changed operator over a stream the pipeline has read
// needs a backfill, unless each sync reads the stream whole.
func TestOperatorsAgainstTheCurrentVersion(t *testing.T) {
	applied := &apiv1.SqlPlan{
		Outputs:   []*apiv1.SqlOutput{{Name: "kept", Operator: "window-k"}, {Name: "edited", Operator: "window-old"}, {Name: "gone", Operator: "window-gone"}, {Name: "rows"}},
		Operators: []string{"window-k", "window-old", "window-gone"},
	}
	a := &sqlplan.Answer{Statements: []sqlplan.Statement{
		window(1, "kept", "orders", "window-k"),
		window(3, "edited", "orders", "window-new"),
		{At: sqlplan.Position{Line: 5, Column: 1}, Output: "rows", Class: "row_wise", Inputs: []string{"orders"}},
		window(6, "fresh", "orders", "window-fresh"),
		window(8, "clicks_by_minute", "clicks", "window-c"),
	}}
	read := map[string]bool{"orders": true, "clicks": true}
	ops, warnings, notes := Operators(windowSpec(30*time.Second), windowCatalog, a, applied, read)
	type row struct {
		id, output, replaces string
		change               apiv1.OperatorChange
		backfill             []string
	}
	want := []row{
		{"window-k", "kept", "", apiv1.OperatorChange_OPERATOR_CHANGE_KEPT, nil},
		{"window-new", "edited", "window-old", apiv1.OperatorChange_OPERATOR_CHANGE_CHANGED, []string{"orders"}},
		{"window-fresh", "fresh", "", apiv1.OperatorChange_OPERATOR_CHANGE_NEW, []string{"orders"}},
		{"window-c", "clicks_by_minute", "", apiv1.OperatorChange_OPERATOR_CHANGE_NEW, nil},
		{"window-gone", "gone", "", apiv1.OperatorChange_OPERATOR_CHANGE_DROPPED, nil},
	}
	if len(ops) != len(want) {
		t.Fatalf("%d operators, want %d: %v", len(ops), len(want), ops)
	}
	for i, w := range want {
		op := ops[i]
		if op.GetId() != w.id || op.GetOutput() != w.output || op.GetReplaces() != w.replaces || op.GetChange() != w.change || !slices.Equal(op.GetBackfill(), w.backfill) {
			t.Errorf("operator %d = %v, want %+v", i, op, w)
		}
	}
	k := ops[0]
	if k.GetLine() != 1 || k.GetInput() != "orders" || k.GetWindow() != "tumble" || k.GetSize().AsDuration() != time.Minute || k.GetSlide() != nil ||
		k.GetLateness().AsDuration() != 30*time.Second || k.GetRetention().AsDuration() != 90*time.Second ||
		!slices.Equal(k.GetKeys(), []string{"region"}) || !slices.Equal(k.GetAggregates(), []string{"sum(amount)", "count(*)"}) || k.GetEventTime() != "at" {
		t.Errorf("kept = %v", k)
	}
	if g := ops[4]; g.GetLine() != 0 || g.GetWindow() != "" {
		t.Errorf("dropped = %v", g)
	}
	if len(warnings) != 2 ||
		!strings.Contains(warnings[0].GetMessage(), "transform.sql:3:1: the group window writing edited computes or keeps something else now, so it's a new operator, window-new, in place of window-old") ||
		!strings.Contains(warnings[0].GetMessage(), "openetl reset --stream orders") || warnings[0].GetStream() != "edited" ||
		!strings.Contains(warnings[1].GetMessage(), "transform.sql:6:1: the group window writing fresh is new") {
		t.Errorf("warnings = %v", warnings)
	}
	if len(notes) != 1 || notes[0] != "transform.sql: no statement keeps operator window-gone, which wrote gone, now, so the next sync drops its state" {
		t.Errorf("notes = %q", notes)
	}
}

// For a new pipeline every operator is new, with nothing to build. Without
// the current version's plan, what each is to it isn't known.
func TestOperatorsOfANewPipelineAndAnUnplannedVersion(t *testing.T) {
	a := &sqlplan.Answer{Statements: []sqlplan.Statement{window(1, "kept", "orders", "window-k")}}
	ops, warnings, notes := Operators(windowSpec(0), windowCatalog, a, &apiv1.SqlPlan{}, nil)
	if len(ops) != 1 || ops[0].GetChange() != apiv1.OperatorChange_OPERATOR_CHANGE_NEW || len(ops[0].GetBackfill()) != 0 || len(warnings) != 0 || len(notes) != 0 {
		t.Errorf("new pipeline: %v %v %v", ops, warnings, notes)
	}
	ops, warnings, notes = Operators(windowSpec(0), windowCatalog, a, nil, map[string]bool{"orders": true})
	if len(ops) != 1 || ops[0].GetChange() != apiv1.OperatorChange_OPERATOR_CHANGE_UNSPECIFIED || len(ops[0].GetBackfill()) != 0 || len(warnings) != 0 || len(notes) != 0 {
		t.Errorf("unplanned: %v %v %v", ops, warnings, notes)
	}
}

// An operator that the current version dropped, from an earlier one, is
// new, and the plan says the next sync restores its state if the
// checkpoint still holds it: no sync of the current version dropped it.
func TestOperatorsSayAnEarlierOperatorMayRestore(t *testing.T) {
	applied := &apiv1.SqlPlan{Outputs: []*apiv1.SqlOutput{{Name: "other", Operator: "window-o"}}, Operators: []string{"window-o"}, Dropped: []string{"window-a"}}
	a := &sqlplan.Answer{Statements: []sqlplan.Statement{window(1, "other", "orders", "window-o"), window(3, "back", "orders", "window-a")}}
	ops, warnings, _ := Operators(windowSpec(0), windowCatalog, a, applied, nil)
	if len(ops) != 2 || ops[1].GetChange() != apiv1.OperatorChange_OPERATOR_CHANGE_NEW || !ops[1].GetEarlier() || ops[0].GetEarlier() {
		t.Fatalf("%v", ops)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0].GetMessage(), "if no sync has run since, the next sync restores its state from then") {
		t.Errorf("%v", warnings)
	}
}

// A version drops the operators the version before it kept or dropped that
// it doesn't keep, so a sync drops their state even after a version that
// never ran; one it keeps again isn't dropped.
func TestSQLPlanDropsWhatTheVersionBeforeKept(t *testing.T) {
	prev := &apiv1.SqlPlan{Operators: []string{"window-a", "window-b"}, Dropped: []string{"window-c", "window-z"}}
	a := &sqlplan.Answer{Statements: []sqlplan.Statement{window(1, "x", "orders", "window-b"), window(2, "y", "orders", "window-z")}}
	p := SQLPlan(a, prev)
	if !slices.Equal(p.GetOperators(), []string{"window-b", "window-z"}) || !slices.Equal(p.GetDropped(), []string{"window-a", "window-c"}) ||
		p.GetOutputs()[0].GetOperator() != "window-b" {
		t.Errorf("%v", p)
	}
}
