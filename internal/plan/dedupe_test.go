package plan

import (
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// A dedupe that keeps each key's first row needs its stream's
// dedupe.retention and reads as OVER does; one that keeps each key's
// latest needs merge, takes any read, and no lateness. A retention that no
// first-row dedupe takes is a problem too.
func TestDedupeProblems(t *testing.T) {
	first := sqlplan.Statement{At: sqlplan.Position{Line: 3, Column: 1}, Output: "firsts", Class: "over", Inputs: []string{"events"}, Operator: "dedupe-1",
		Need: []string{"id"}, Passes: map[string]string{"id": "id"}, Key: []string{"id"}, State: &sqlplan.State{EventTime: "at", Keys: []string{"id"}, Keep: "first"}}
	latest := first
	latest.Output, latest.State = "latests", &sqlplan.State{EventTime: "at", Keys: []string{"id"}, Keep: "latest", Guarantee: "exactly_once"}
	retention := &apiv1.Dedupe{Retention: durationpb.New(24 * time.Hour)}
	compared := 0
	for _, c := range []struct {
		name     string
		st       sqlplan.Statement
		dedupe   *apiv1.Dedupe
		read     apiv1.SyncMode
		cursor   string
		lateness time.Duration
		mode     destination.WriteMode
		want     string
	}{
		{name: "first with a retention", st: first, dedupe: retention, read: apiv1.SyncMode_SYNC_MODE_STREAM, mode: destination.Append},
		{name: "first unbounded, merged", st: first, dedupe: &apiv1.Dedupe{Unbounded: true}, read: apiv1.SyncMode_SYNC_MODE_STREAM, mode: destination.Merge},
		{name: "first read whole", st: first, dedupe: retention, read: apiv1.SyncMode_SYNC_MODE_FULL_REFRESH, mode: destination.Overwrite},
		{name: "first without a retention", st: first, read: apiv1.SyncMode_SYNC_MODE_STREAM, mode: destination.Append,
			want: `transform.sql:3:1: the dedupe writing "firsts", ROW_NUMBER() … = 1 in ascending order of the event time, keeps each key's first row, ` +
				`and stream "events" sets no dedupe.retention, how long it remembers a key: set source.streams[].dedupe.retention to a duration of event time`},
		{name: "first read by another cursor", st: first, dedupe: retention, read: apiv1.SyncMode_SYNC_MODE_INCREMENTAL, cursor: "id", mode: destination.Append,
			want: `the dedupe writing "firsts" reads stream "events" with incremental by cursor "id", not by its eventTime "at", so each sync's read would start its keys again`},
		{name: "first with lateness", st: first, dedupe: retention, read: apiv1.SyncMode_SYNC_MODE_STREAM, lateness: time.Minute, mode: destination.Append,
			want: `stream "events" sets watermark.lateness, which the dedupe writing "firsts" doesn't take: a row that comes behind the watermark is quarantined as late`},
		{name: "latest merged", st: latest, read: apiv1.SyncMode_SYNC_MODE_STREAM, mode: destination.Merge},
		{name: "latest read by another cursor", st: latest, read: apiv1.SyncMode_SYNC_MODE_INCREMENTAL, cursor: "id", mode: destination.Merge},
		{name: "latest appended", st: latest, read: apiv1.SyncMode_SYNC_MODE_STREAM, mode: destination.Append,
			want: `transform.sql:3:1: the dedupe writing "latests", ROW_NUMBER() … = 1 in descending order of the event time, keeps each key's latest row, ` +
				`as upserts and deletes that only a merge applies, and the output is written with append: set destination.write: merge`},
		{name: "latest overwritten", st: latest, read: apiv1.SyncMode_SYNC_MODE_FULL_REFRESH, mode: destination.Overwrite, want: "and the output is written with overwrite"},
		{name: "latest with lateness", st: latest, read: apiv1.SyncMode_SYNC_MODE_STREAM, lateness: time.Minute, mode: destination.Merge,
			want: `stream "events" sets watermark.lateness, which the dedupe writing "latests" doesn't take: it writes each key's latest row as it comes`},
		{name: "a retention latest doesn't take", st: latest, dedupe: retention, read: apiv1.SyncMode_SYNC_MODE_STREAM, mode: destination.Merge,
			want: `stream "events" sets dedupe.retention, which only a dedupe that keeps each key's first row takes`},
	} {
		ps := &apiv1.PipelineStream{Name: "events", EventTime: "at", Dedupe: c.dedupe, Watermark: &apiv1.Watermark{Lateness: durationpb.New(c.lateness)}}
		in := Input{Name: "events", Read: c.read, Cursor: c.cursor, Spec: ps}
		outs := []Output{{Name: c.st.Output, Mode: c.mode, PrimaryKey: []string{"id"}}}
		got := errors.Join(windowProblems(&sqlplan.Answer{Statements: []sqlplan.Statement{c.st}}, []Input{in}, outs, warehouse)...)
		switch {
		case c.want == "" && got != nil:
			t.Errorf("%s: %v", c.name, got)
		case c.want != "" && (got == nil || !strings.Contains(got.Error(), c.want)):
			t.Errorf("%s: %v, want %q", c.name, got, c.want)
		}
		compared++
	}
	if compared != 12 {
		t.Errorf("%d cases compared", compared)
	}
	day := (24 * time.Hour).Microseconds()
	if r := DedupeRetention(&apiv1.PipelineStream{Dedupe: retention}); r == nil || *r != day {
		t.Errorf("a day's retention is %v", r)
	}
	if r := DedupeRetention(&apiv1.PipelineStream{Dedupe: &apiv1.Dedupe{Unbounded: true}}); r == nil || *r != 1<<63-1 {
		t.Errorf("unbounded is %v", r)
	}
	if r := DedupeRetention(&apiv1.PipelineStream{}); r != nil {
		t.Errorf("none is %v", *r)
	}
}

// A latest-row dedupe's output is merged where the destination merges and
// destination.write is unset, whatever the read; a first-row dedupe's isn't.
func TestDedupeOutputsMerge(t *testing.T) {
	a := &sqlplan.Answer{Statements: []sqlplan.Statement{
		{Output: "latests", Inputs: []string{"events"}, Key: []string{"id"}, State: &sqlplan.State{Keep: "latest"}},
		{Output: "firsts", Inputs: []string{"events"}, Key: []string{"id"}, State: &sqlplan.State{Keep: "first"}},
	}}
	in := Input{Name: "events", Read: apiv1.SyncMode_SYNC_MODE_FULL_REFRESH, Spec: &apiv1.PipelineStream{Name: "events"}}
	outs, _ := SQLOutputs(&apiv1.PipelineSpec{}, warehouse, a, []Input{in})
	if outs[0].Mode != destination.Merge || outs[1].Mode != destination.Overwrite {
		t.Errorf("modes %v and %v", outs[0].Mode, outs[1].Mode)
	}
	appends := destination.Capabilities{Class: destination.StageAndApply, WriteModes: []destination.WriteMode{destination.Append}}
	outs, _ = SQLOutputs(&apiv1.PipelineSpec{}, appends, a, []Input{in})
	if outs[0].Mode != destination.Append {
		t.Errorf("without merge, %v", outs[0].Mode)
	}
}

// A dedupe's operator has which row it keeps, its retention and what it
// guarantees, and a changed one says its keys start again.
func TestDedupeOperators(t *testing.T) {
	day := (24 * time.Hour).Microseconds()
	dedupe := func(id, keep string, retention *int64, guarantee string) sqlplan.Statement {
		return sqlplan.Statement{At: sqlplan.Position{Line: 2, Column: 1}, Output: "firsts", Class: "over", Inputs: []string{"orders"}, Operator: id,
			State: &sqlplan.State{EventTime: "at", Keys: []string{"id"}, Keep: keep, RetentionMicros: retention, Guarantee: guarantee}}
	}
	applied := &apiv1.SqlPlan{Outputs: []*apiv1.SqlOutput{{Name: "firsts", Operator: "dedupe-old"}}, Operators: []string{"dedupe-old"}}
	a := &sqlplan.Answer{Statements: []sqlplan.Statement{dedupe("dedupe-new", "first", &day, "exactly_once_within")}}
	ops, warnings, _ := Operators(windowSpec(0), windowCatalog, a, applied, map[string]bool{"orders": true}, nil)
	if len(ops) != 1 {
		t.Fatalf("operators %v", ops)
	}
	op := ops[0]
	if op.GetKeep() != "first" || op.GetGuarantee() != "exactly_once_within" || op.GetRetention().AsDuration() != 24*time.Hour ||
		op.GetWindow() != "" || op.GetLateness() != nil || op.GetEventTime() != "at" || len(op.GetAggregates()) != 0 {
		t.Errorf("operator %v", op)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0].GetMessage(), "transform.sql:2:1: the dedupe writing firsts computes or keeps something else now") ||
		!strings.Contains(warnings[0].GetMessage(), "it remembers only the keys of rows read from then on, so a key it kept before is written again") {
		t.Errorf("warnings = %v", warnings)
	}
	a = &sqlplan.Answer{Statements: []sqlplan.Statement{dedupe("dedupe-new", "latest", nil, "exactly_once")}}
	ops, _, _ = Operators(windowSpec(0), windowCatalog, a, nil, nil, nil)
	if op := ops[0]; op.GetKeep() != "latest" || op.GetGuarantee() != "exactly_once" || op.GetRetention() != nil {
		t.Errorf("operator %v", op)
	}
}
