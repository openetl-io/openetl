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
		{name: "first with a retention under a microsecond", st: first, dedupe: &apiv1.Dedupe{Retention: durationpb.New(time.Nanosecond)}, read: apiv1.SyncMode_SYNC_MODE_STREAM,
			mode: destination.Append, want: `stream "events" sets dedupe.retention 1ns, under the microsecond that event time counts in: set 1µs or more, or unbounded`},
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
	if compared != 13 {
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

// A first-row dedupe with a retention, over a stream that can repeat rows,
// is exactly_once_within, which the default require refuses with what to
// set; merged, it's effectively_once; unbounded, latest-row and over an
// exact stream, exactly_once. An output that replaces its stream takes that
// stream's require.
func TestDedupeLevels(t *testing.T) {
	day := (24 * time.Hour).Microseconds()
	statement := func(output, keep string, retention *int64, guarantee string) sqlplan.Statement {
		return sqlplan.Statement{At: sqlplan.Position{Line: 1, Column: 1}, Output: output, Class: "over", Inputs: []string{"events"}, Operator: "dedupe-" + output,
			Need: []string{"event_id"}, Passes: map[string]string{"event_id": "event_id"}, Key: []string{"event_id"}, Replaces: output == "events",
			State: &sqlplan.State{EventTime: "at", Keys: []string{"event_id"}, Keep: keep, RetentionMicros: retention, Guarantee: guarantee}}
	}
	first := statement("events_once", "first", &day, "exactly_once_within")
	cat := func(replay apiv1.Replay) *apiv1.Catalog {
		return &apiv1.Catalog{Streams: []*apiv1.CatalogStream{{Name: "events", Replay: replay, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_STREAM},
			Columns: []*apiv1.CatalogColumn{{Name: "event_id", Type: "utf8"}, {Name: "at", Type: "timestamp[us, tz=UTC]"}}}}}
	}
	spec := func(st *apiv1.PipelineStream, require apiv1.DeliveryLevel, write apiv1.WriteMode) *apiv1.PipelineSpec {
		st.Name, st.EventTime = "events", "at"
		return &apiv1.PipelineSpec{
			Mode:        apiv1.PipelineMode_PIPELINE_MODE_CONTINUOUS,
			Source:      &apiv1.PipelineSource{Streams: []*apiv1.PipelineStream{st}},
			Destination: &apiv1.PipelineDestination{Write: write},
			Delivery:    &apiv1.Delivery{Require: require},
			Transform:   &apiv1.Transform{Engine: &apiv1.Transform_Sql{Sql: &apiv1.SqlTransform{Sql: "…", Passthrough: apiv1.Passthrough_PASSTHROUGH_NONE}}},
		}
	}
	retention := func() *apiv1.Dedupe { return &apiv1.Dedupe{Retention: durationpb.New(24 * time.Hour)} }
	unset, merge := apiv1.WriteMode_WRITE_MODE_UNSPECIFIED, apiv1.WriteMode_WRITE_MODE_MERGE
	none, within := apiv1.DeliveryLevel_DELIVERY_LEVEL_UNSPECIFIED, apiv1.DeliveryLevel_DELIVERY_LEVEL_EXACTLY_ONCE_WITHIN
	superset, exact := apiv1.Replay_REPLAY_SUPERSET, apiv1.Replay_REPLAY_EXACT
	const remembers = "a dedupe's output over superset inputs, which remembers a key 24h0m0s of event time from its first row, "
	compared := 0
	for _, c := range []struct {
		name    string
		st      sqlplan.Statement
		ps      *apiv1.PipelineStream
		require apiv1.DeliveryLevel
		write   apiv1.WriteMode
		replay  apiv1.Replay
		level   apiv1.DeliveryLevel
		reason  string
		problem string
	}{
		{name: "the default refuses a retention", st: first, ps: &apiv1.PipelineStream{Dedupe: retention()}, require: none, write: unset, replay: superset,
			level: within, reason: remembers + "and writes a repeat after that again",
			problem: `delivery.require: stream "events_once" gets exactly_once_within (` + remembers + `and writes a repeat after that again), below exactly_once: ` +
				`to accept that a repeat 24h0m0s or more of event time after a key's first row is written again, set delivery.require: exactly_once_within; ` +
				`to remember every key, set source.streams[events].dedupe.retention: unbounded`},
		{name: "effectively_once refuses it too", st: first, ps: &apiv1.PipelineStream{Dedupe: retention()}, require: apiv1.DeliveryLevel_DELIVERY_LEVEL_EFFECTIVELY_ONCE,
			write: unset, replay: superset, level: within, reason: remembers + "and writes a repeat after that again", problem: "below effectively_once: to accept that"},
		{name: "exactly_once_within takes it", st: first, ps: &apiv1.PipelineStream{Dedupe: retention()}, require: within, write: unset, replay: superset,
			level: within, reason: remembers + "and writes a repeat after that again"},
		{name: "at_least_once takes it", st: first, ps: &apiv1.PipelineStream{Dedupe: retention()}, require: apiv1.DeliveryLevel_DELIVERY_LEVEL_AT_LEAST_ONCE,
			write: unset, replay: superset, level: within, reason: remembers + "and writes a repeat after that again"},
		{name: "merged", st: first, ps: &apiv1.PipelineStream{Dedupe: retention()}, require: apiv1.DeliveryLevel_DELIVERY_LEVEL_EFFECTIVELY_ONCE, write: merge, replay: superset,
			level: apiv1.DeliveryLevel_DELIVERY_LEVEL_EFFECTIVELY_ONCE, reason: remembers + "written with merge, which merges a repeat after that onto its key's row"},
		{name: "unbounded", st: statement("events_once", "first", nil, "exactly_once"), ps: &apiv1.PipelineStream{Dedupe: &apiv1.Dedupe{Unbounded: true}}, require: none,
			write: unset, replay: superset, level: apiv1.DeliveryLevel_DELIVERY_LEVEL_EXACTLY_ONCE,
			reason: "a dedupe's output over superset inputs, which remembers every key, so its state grows with the number of keys"},
		{name: "latest", st: statement("events_once", "latest", nil, "exactly_once"), ps: &apiv1.PipelineStream{}, require: none, write: merge, replay: superset,
			level: apiv1.DeliveryLevel_DELIVERY_LEVEL_EXACTLY_ONCE, reason: "a dedupe's output over superset inputs, which remembers every key, so its state grows with the number of keys"},
		{name: "exact", st: first, ps: &apiv1.PipelineStream{Dedupe: retention()}, require: none, write: unset, replay: exact,
			level: apiv1.DeliveryLevel_DELIVERY_LEVEL_EXACTLY_ONCE, reason: "a transform's output over exact inputs"},
		{name: "replacing its stream", st: statement("events", "first", &day, "exactly_once_within"), ps: &apiv1.PipelineStream{Dedupe: retention()}, require: none,
			write: unset, replay: superset, level: within, reason: remembers + "and writes a repeat after that again",
			problem: "set source.streams[events].require: exactly_once_within; to remember every key, set source.streams[events].dedupe.retention: unbounded"},
		{name: "taking its stream's require", st: statement("events", "first", &day, "exactly_once_within"), ps: &apiv1.PipelineStream{Dedupe: retention(), Require: within},
			require: none, write: unset, replay: superset, level: within, reason: remembers + "and writes a repeat after that again"},
	} {
		plans, problems, _ := PipelineSQL(spec(c.ps, c.require, c.write), cat(c.replay), warehouse, &sqlplan.Answer{Statements: []sqlplan.Statement{c.st}})
		if len(plans) != 1 || plans[0].GetLevel() != c.level || plans[0].GetReason() != c.reason {
			t.Errorf("%s: plans %v, want %v (%s)", c.name, plans, c.level, c.reason)
		}
		err := errors.Join(problems...)
		switch {
		case c.problem == "" && err != nil:
			t.Errorf("%s: %v", c.name, err)
		case c.problem != "" && (len(problems) != 1 || !strings.Contains(err.Error(), c.problem)):
			t.Errorf("%s: %v, want %q", c.name, err, c.problem)
		}
		compared++
	}
	if compared != 10 {
		t.Errorf("%d cases compared", compared)
	}
}
