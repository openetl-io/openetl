package runner

import (
	"maps"
	"slices"
	"testing"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// The versions of a stream's records are kept where the destination
// dedupes it by _openetl_id against what it has, and its reads give a
// record again at their cursor's floor.
func TestVersionsAreKeptOfStreamsDedupedAgainstTheTarget(t *testing.T) {
	dedupes := capsDest{caps: destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopeStream}, DedupeAtApply: true,
		WriteModes: []destination.WriteMode{destination.Append, destination.Merge}}}
	plain := destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopeStream}}
	read := Stream{Name: "s", Replay: source.Superset, Mode: destination.Append, PrimaryKey: []string{"id"}, Read: source.Incremental, CursorField: "at"}
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
	rec := &checkpoint.Record{Streams: map[string]checkpoint.StreamState{"a": {ReadStart: 5}}, Snapshots: []checkpoint.Snapshot{{Operator: VersionsOperator("a")}}}
	cp := j.restart(rec, []string{"a"})
	if len(cp.Snapshots) != 1 || cp.Streams["a"].ReadStart != 0 {
		t.Errorf("restarted %+v", cp)
	}
}
