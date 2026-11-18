package runner

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// Rows read again after a restore from a forced barrier get less than
// planned only where require lets the barrier lower the stream.
func TestLowered(t *testing.T) {
	postgres := capsDest{caps: destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopeStream}, DedupeAtApply: true}}
	keyed := []string{"id"}
	for _, c := range []struct {
		name    string
		stream  Stream
		require delivery.Level
		want    string
	}{
		{"exact, at least once", Stream{Replay: source.Exact, Mode: destination.Append, PrimaryKey: keyed}, delivery.AtLeastOnce, "{at_least_once exactly_once}"},
		{"exact, deduped after the restore", Stream{Replay: source.Exact, Mode: destination.Append, PrimaryKey: keyed}, delivery.ExactlyOnce, "none"},
		{"exact, merge", Stream{Replay: source.Exact, Mode: destination.Merge, PrimaryKey: keyed}, delivery.EffectivelyOnce, "{effectively_once exactly_once}"},
		{"superset, at least once", Stream{Replay: source.Superset, Mode: destination.Append, PrimaryKey: keyed}, delivery.AtLeastOnce, "none"},
		{"exact without a key, its own require", Stream{Replay: source.Exact, Mode: destination.Append, Require: delivery.AtLeastOnce}, delivery.ExactlyOnce, "{at_least_once exactly_once}"},
	} {
		c.stream.Name = "s"
		j := Job{Destination: postgres, Settings: delivery.Settings{Require: c.require}, Streams: []Stream{c.stream}}
		got := "none"
		if l, ok := j.Lowered()["s"]; ok {
			got = fmt.Sprintf("{%v %v}", l.Level, l.Planned)
		}
		if got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
}

// A stream a SQL transform passes through is delivered as it would be
// without the transform: keyed, read with superset replay and appended to a
// destination that dedupes at apply, it's exactly once, as the default
// require needs. A statement's output over it isn't deduped.
// A job checks a dedupe's output as the plan does: with a retention it's
// exactly_once_within, and read with full_refresh and appended, every
// sync writes its keys again.
func TestAJobChecksADedupesLevel(t *testing.T) {
	events := Stream{Name: "events", Replay: source.Superset, Read: source.Stream, Mode: destination.Append, PrimaryKey: []string{"id"}}
	compared := 0
	for _, c := range []struct {
		name    string
		read    source.SyncMode
		require delivery.Level
		want    string
	}{
		{"with a retention", source.Stream, delivery.ExactlyOnce,
			"gets exactly_once_within (a dedupe's output over superset inputs, which remembers a key 1h0m0s of event time from its first row, and writes a repeat after that again), " +
				"below exactly_once: to accept that a key is written again by a row 1h0m0s or more of event time after its first row, set delivery.require: exactly_once_within; " +
				"to remember every key from then on, set source.streams[events].dedupe.retention: unbounded"},
		{"accepted", source.Stream, delivery.ExactlyOnceWithin, ""},
		{"read whole, appended", source.FullRefresh, delivery.ExactlyOnceWithin, "gets at_least_once (a dedupe's output over superset inputs read with full_refresh"},
	} {
		in := events
		in.Read = c.read
		out := in
		out.Name, out.From, out.Transformed, out.Deduped, out.Remembers = "events_once", "events", true, true, time.Hour
		j := Job{
			Owner:       "a",
			Log:         checkpoint.NewLog(checkpoint.NewMemStore(), "o", "p"),
			Source:      struct{ source.Reader }{},
			Dataflow:    Passthrough{},
			Streams:     []Stream{in},
			Outputs:     []Stream{out},
			SQL:         &SQLTransform{SQL: "INSERT INTO events_once SELECT id FROM events"},
			Destination: capsDest{caps: destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopeStream}}},
			Settings:    delivery.Settings{Require: c.require, Scope: destination.ScopeStream},
		}
		err := j.validate()
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%s: %v", c.name, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("%s: %v, want %q", c.name, err, c.want)
		}
		compared++
	}
	if compared != 3 {
		t.Errorf("%d cases compared", compared)
	}
}

func TestAStreamPassedThroughKeepsItsGuarantee(t *testing.T) {
	users := Stream{Name: "users", Replay: source.Superset, Mode: destination.Append, PrimaryKey: []string{"id"}}
	for _, transformed := range []bool{false, true} {
		out := users
		out.From, out.Transformed = "users", transformed
		j := Job{
			Owner:       "a",
			Log:         checkpoint.NewLog(checkpoint.NewMemStore(), "o", "p"),
			Source:      struct{ source.Reader }{},
			Dataflow:    Passthrough{},
			Streams:     []Stream{users},
			Outputs:     []Stream{out},
			SQL:         &SQLTransform{SQL: "INSERT INTO c SELECT id FROM users"},
			Destination: capsDest{caps: destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopeStream}, DedupeAtApply: true}},
			Settings:    delivery.Settings{Require: delivery.ExactlyOnce, Scope: destination.ScopeStream},
		}
		err := j.validate()
		switch {
		case !transformed && err != nil:
			t.Errorf("passed through: %v", err)
		case transformed && (err == nil || !strings.Contains(err.Error(), "at_least_once")):
			t.Errorf("a statement's output: %v", err)
		}
	}
}
