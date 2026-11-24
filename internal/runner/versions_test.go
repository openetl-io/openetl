package runner

import (
	"cmp"
	"maps"
	"slices"
	"testing"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

var (
	dedupes = capsDest{caps: destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopeStream}, DedupeAtApply: true,
		WriteModes: []destination.WriteMode{destination.Append, destination.Merge}}}
	read = Stream{Name: "s", Replay: source.Superset, Mode: destination.Append, PrimaryKey: []string{"id"}, Read: source.Incremental, CursorField: "at"}
)

// flinkCode is a Flink job no test of versions runs.
type flinkCode struct{ Flink }

// The versions of a stream's records are kept where the destination
// dedupes it by _openetl_id against what it has, and its reads give a
// record again at their cursor's floor.
func TestVersionsAreKeptOfStreamsDedupedAgainstTheTarget(t *testing.T) {
	plain := destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopeStream}}
	for _, tc := range []struct {
		name string
		edit func(*Job)
		want bool
	}{
		{"deduped at apply", func(*Job) {}, true},
		{"without state", func(j *Job) { j.StateURL = "" }, false},
		{"under at_least_once", func(j *Job) { j.Settings.Require = delivery.AtLeastOnce }, false},
		{"merged", func(j *Job) { j.Streams[0].Mode = destination.Merge }, false},
		{"exact", func(j *Job) { j.Streams[0].Replay = source.Exact }, false},
		{"without a key", func(j *Job) { j.Streams[0].PrimaryKey = nil }, false},
		{"read whole", func(j *Job) { j.Streams[0].Read, j.Streams[0].CursorField = source.FullRefresh, "" }, false},
		{"into a destination that doesn't dedupe", func(j *Job) { j.Destination = capsDest{caps: plain} }, false},
		{"past a transform", func(j *Job) { j.SQL = &SQLTransform{} }, false},
		// An output named as its input, as one a transform doesn't declare,
		// isn't the input.
		{"past Spark code", func(j *Job) { j.Transform, j.Outputs = sparkCode{}, []Stream{read} }, false},
		{"past Flink code", func(j *Job) { j.Flink, j.Outputs = flinkCode{}, []Stream{read} }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j := Job{Destination: dedupes, StateURL: "file:///state", Streams: []Stream{read}, Settings: delivery.Settings{Require: delivery.ExactlyOnce}}
			tc.edit(&j)
			got := j.versions()
			if want := map[string]string{"s": VersionsOperator("s")}; tc.want != maps.Equal(got, want) || !tc.want && got != nil {
				t.Errorf("versions %v", got)
			}
		})
	}
}

// An attempt's engine restores the versions of the streams that keep them
// alone, and a redo's those of its epoch's own record.
func TestTheVersionsAnAttemptRestores(t *testing.T) {
	snap := func(op, data string) checkpoint.Snapshot {
		return checkpoint.Snapshot{Operator: op, Data: []byte(data)}
	}
	restored := []checkpoint.Snapshot{snap("w", "before"), snap(VersionsOperator("a"), "before"), snap(VersionsOperator("b"), "before")}
	versions := map[string]string{"a": VersionsOperator("a")}
	names := func(ss []checkpoint.Snapshot) []string {
		var out []string
		for _, s := range ss {
			out = append(out, s.Operator+" "+string(s.Data))
		}
		slices.Sort(out)
		return out
	}
	if got := names(versionsState(restored, versions, restored)); !slices.Equal(got, []string{"versions.a before", "w before"}) {
		t.Errorf("an attempt restores %q", got)
	}
	own := []checkpoint.Snapshot{snap("w", "own"), snap(VersionsOperator("a"), "own")}
	if got := names(versionsState(restored, versions, own)); !slices.Equal(got, []string{"versions.a own", "w before"}) {
		t.Errorf("a redo restores %q", got)
	}
	if got := versionsState(nil, nil, nil); got != nil {
		t.Errorf("no state restores %v", got)
	}
}

// A reset keeps the versions of the streams it reads again.
func TestARestartKeepsTheVersions(t *testing.T) {
	j := Job{Streams: []Stream{{Name: "a"}}}
	rec := &checkpoint.Record{Streams: map[string]checkpoint.StreamState{"a": {ReadStart: 5}}, Versions: []checkpoint.Snapshot{{Operator: VersionsOperator("a")}}}
	cp := j.restart(rec, []string{"a"})
	if len(cp.Versions) != 1 || cp.Streams["a"].ReadStart != 0 {
		t.Errorf("restarted %+v", cp)
	}
}

// On more than one writer lane, a stream whose versions are kept is
// exchanged by key group before its writer, also where its source splits it
// by key, so that each writer lane gets the rows of the key groups whose
// versions it keeps.
func TestAStreamWhoseVersionsAreKeptIsExchangedByKeyGroup(t *testing.T) {
	split := read
	split.Partitioning = plan.SourcePartitioning(split.PrimaryKey, true, false)
	j := Job{Destination: dedupes, StateURL: "file:///state", Streams: []Stream{split}, Settings: delivery.Settings{Require: delivery.ExactlyOnce}}
	if got := j.exchanges(2); !slices.Equal(got["s"], []string{"id"}) {
		t.Errorf("on two writer lanes, exchanges %v", got)
	}
	if got := j.exchanges(1); got != nil {
		t.Errorf("on one writer lane, exchanges %v", got)
	}
	j.Settings.Require = delivery.AtLeastOnce
	if got := j.exchanges(2); got != nil {
		t.Errorf("without versions, exchanges %v", got)
	}
}

// The plan and the runner agree on the streams whose versions are kept, over
// one set of pipelines: a stream the plan has the writer lanes keep the
// versions of needs its rows by key group, which its source's splits by key
// don't meet, so its path shows an exchange before the writer; the runner's
// job, built from the pipeline as the runner builds it, keeps them. The plan
// can't see a deployment without checkpoint storage, so every job has state.
// A paused stream keeps none while it's paused, and those the plan says once
// it resumes.
func TestThePlanAndTheRunnerKeepTheSameVersions(t *testing.T) {
	type pipeline struct {
		keyless, noCursor, exact, merge, paused, plainDest bool
		read                                               apiv1.SyncMode
		require                                            apiv1.DeliveryLevel
		transform                                          string
	}
	for _, tc := range []struct {
		name string
		p    pipeline
		want bool
	}{
		{"keyed, read with incremental, deduped at apply", pipeline{}, true},
		{"an exact source read with incremental, which reads its floor again", pipeline{exact: true}, true},
		{"paused", pipeline{paused: true}, true},
		{"under at_least_once", pipeline{require: apiv1.DeliveryLevel_DELIVERY_LEVEL_AT_LEAST_ONCE}, false},
		{"merged", pipeline{merge: true}, false},
		{"read with full_refresh", pipeline{read: apiv1.SyncMode_SYNC_MODE_FULL_REFRESH}, false},
		{"without a key", pipeline{keyless: true}, false},
		{"without a cursor", pipeline{noCursor: true}, false},
		{"into a destination that doesn't dedupe", pipeline{plainDest: true}, false},
		{"passed through a SQL transform", pipeline{transform: "sql"}, false},
		{"read by Spark code", pipeline{transform: "spark"}, false},
		{"read by Flink code", pipeline{transform: "flink"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caps := destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopePipeline, destination.ScopeStream},
				DedupeAtApply: !tc.p.plainDest, WriterLanes: 2, WriteModes: []destination.WriteMode{destination.Append, destination.Merge}}
			cs := &apiv1.CatalogStream{Name: "s", PrimaryKey: []string{"id"}, CursorField: "at", SplitsByKey: true, Replay: apiv1.Replay_REPLAY_SUPERSET,
				SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_INCREMENTAL, apiv1.SyncMode_SYNC_MODE_FULL_REFRESH}}
			if tc.p.keyless {
				cs.PrimaryKey = nil
			}
			if tc.p.noCursor {
				cs.CursorField = ""
			}
			if tc.p.exact {
				cs.Replay = apiv1.Replay_REPLAY_EXACT
			}
			ps := &apiv1.PipelineStream{Name: "s", Sync: tc.p.read, Require: tc.p.require}
			spec := &apiv1.PipelineSpec{Source: &apiv1.PipelineSource{Streams: []*apiv1.PipelineStream{ps}}}
			if tc.p.merge {
				spec.Destination = &apiv1.PipelineDestination{Write: apiv1.WriteMode_WRITE_MODE_MERGE}
			}
			var answer *sqlplan.Answer
			switch tc.p.transform {
			case "sql":
				spec.Transform = &apiv1.Transform{Engine: &apiv1.Transform_Sql{Sql: &apiv1.SqlTransform{Sql: "…", Passthrough: apiv1.Passthrough_PASSTHROUGH_ALL}}}
				answer = &sqlplan.Answer{}
			case "spark":
				spec.Transform = &apiv1.Transform{Engine: &apiv1.Transform_Spark{Spark: &apiv1.SparkTransform{}}}
			case "flink":
				spec.Transform = &apiv1.Transform{Engine: &apiv1.Transform_Flink{Flink: &apiv1.FlinkTransform{}}}
			}

			plans, _, _ := plan.PipelineSQL(spec, &apiv1.Catalog{Streams: []*apiv1.CatalogStream{cs}}, caps, answer)
			planned := slices.ContainsFunc(plans, func(sp *apiv1.StreamPlan) bool {
				return sp.GetName() == "s" && sp.GetPartitioning().GetKind() == apiv1.PartitioningKind_PARTITIONING_KIND_SPLITS &&
					slices.ContainsFunc(sp.GetOperators(), func(op *apiv1.PlanOperator) bool {
						return op.GetKind() == apiv1.PlanOperatorKind_PLAN_OPERATOR_KIND_WRITER && op.GetExchange()
					})
			})

			read := plan.ReadMode(spec, cs.GetSyncModes(), ps)
			s := Stream{Name: "s", Replay: plan.Replay(catalogReplay(cs.GetReplay()), read), Read: source.SyncMode(read), PrimaryKey: cs.GetPrimaryKey(),
				CursorField: cmp.Or(ps.GetCursor(), cs.GetCursorField()), Require: plan.Level(ps.GetRequire()),
				Mode: plan.StreamMode(spec, caps, read, len(cs.GetPrimaryKey()) > 0)}
			j := Job{Destination: capsDest{caps: caps}, StateURL: "file:///state", Streams: []Stream{s}, Settings: plan.Settings(spec, caps)}
			switch tc.p.transform {
			case "sql":
				j.SQL, j.Outputs = &SQLTransform{}, []Stream{s}
			case "spark":
				j.Transform, j.Outputs = sparkCode{}, []Stream{s}
			case "flink":
				j.Flink, j.Outputs = flinkCode{}, []Stream{s}
			}
			if tc.p.paused {
				paused := j
				paused.Streams, paused.Paused = nil, []Stream{s}
				if got := paused.versions(); got != nil {
					t.Errorf("a paused stream's job keeps versions %v", got)
				}
			}
			kept := j.versions()["s"] != ""

			if planned != tc.want || kept != tc.want {
				t.Errorf("the plan keeps versions: %v, the runner: %v, want %v", planned, kept, tc.want)
			}
		})
	}
}

// catalogReplay is a catalog's replay as a source gives it.
func catalogReplay(r apiv1.Replay) source.Replay {
	if r == apiv1.Replay_REPLAY_EXACT {
		return source.Exact
	}
	return source.Superset
}
