package delivery

import (
	"strings"
	"testing"
	"time"

	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

var (
	postgres = destination.Capabilities{Class: destination.StageAndApply, DedupeAtApply: true,
		Scopes: []destination.Scope{destination.ScopePipeline, destination.ScopeStream}, MinCommitInterval: time.Second}
	parquet = destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopeStream}}
	kafka   = destination.Capabilities{Class: destination.TransactionalStream}
	webhook = destination.Capabilities{Class: destination.DeliverAndAck}
)

func TestPlanStream(t *testing.T) {
	const (
		exact    = source.Exact
		superset = source.Superset
		appnd    = destination.Append
		merge    = destination.Merge
		once     = ExactlyOnce
		least    = AtLeastOnce
	)
	for _, tc := range []struct {
		name    string
		caps    destination.Capabilities
		replay  source.Replay
		mode    destination.WriteMode
		keyed   bool
		require Level
		want    Level
		dedupe  bool
	}{
		{"postgres append exact", postgres, exact, appnd, true, once, once, false},
		{"postgres append exact unkeyed", postgres, exact, appnd, false, once, once, false},
		{"postgres append superset", postgres, superset, appnd, true, once, once, true},
		{"postgres append superset, at least once", postgres, superset, appnd, true, least, least, false},
		{"postgres append superset, effectively once", postgres, superset, appnd, true, EffectivelyOnce, once, true},
		{"postgres append superset unkeyed", postgres, superset, appnd, false, once, least, false},
		{"postgres merge exact", postgres, exact, merge, true, once, once, false},
		{"postgres merge superset", postgres, superset, merge, true, once, EffectivelyOnce, false},
		{"parquet append exact", parquet, exact, appnd, true, once, once, false},
		{"parquet append superset", parquet, superset, appnd, true, once, least, false},
		{"iceberg merge superset", parquet, superset, merge, true, once, EffectivelyOnce, false},
		{"kafka exact", kafka, exact, appnd, false, once, once, false},
		{"kafka superset", kafka, superset, appnd, true, once, least, false},
		{"webhook exact", webhook, exact, appnd, true, once, least, false},
		{"webhook superset", webhook, superset, merge, true, once, least, false},
	} {
		p := PlanStream(tc.caps, Stream{Replay: tc.replay, Mode: tc.mode, Keyed: tc.keyed}, tc.require)
		if p.Level != tc.want || p.Dedupe != tc.dedupe {
			t.Errorf("%s: level %v, dedupe %v; want %v, %v", tc.name, p.Level, p.Dedupe, tc.want, tc.dedupe)
		}
	}
}

func TestUndeclaredReplayIsSuperset(t *testing.T) {
	p := PlanStream(postgres, Stream{Mode: destination.Append, Keyed: true}, ExactlyOnce)
	if p.Level != ExactlyOnce || !p.Dedupe || p.Why != "superset replay, deduped at apply" {
		t.Errorf("PlanStream = %+v", p)
	}
}

func TestForcedBarriers(t *testing.T) {
	keyed := Stream{Replay: source.Exact, Mode: destination.Append, Keyed: true}
	unkeyed := Stream{Replay: source.Exact, Mode: destination.Append}
	for _, tc := range []struct {
		name     string
		s        Stream
		require  Level
		canForce bool
		dedupe   bool // after a restore from a forced barrier
	}{
		{"keyed, exactly once", keyed, ExactlyOnce, true, true},
		{"keyed, at least once", keyed, AtLeastOnce, true, false},
		{"unkeyed, exactly once", unkeyed, ExactlyOnce, false, false},
		{"unkeyed, at least once", unkeyed, AtLeastOnce, true, false},
	} {
		if got := CanForce(postgres, tc.s, tc.require); got != tc.canForce {
			t.Errorf("%s: CanForce = %v", tc.name, got)
		}
		if got := ShouldDedupe(postgres, tc.s, tc.require, true); got != tc.dedupe {
			t.Errorf("%s: ShouldDedupe after a forced restore = %v", tc.name, got)
		}
		if ShouldDedupe(postgres, tc.s, tc.require, false) {
			t.Errorf("%s: an exact stream is deduped without a forced restore", tc.name)
		}
	}
}

func TestCheck(t *testing.T) {
	ok := Settings{Require: ExactlyOnce, Scope: destination.ScopePipeline, CommitInterval: time.Minute}
	streams := []Stream{
		{Name: "orders", Replay: source.Exact, Mode: destination.Append, Keyed: true},
		{Name: "logs", Replay: source.Exact, Mode: destination.Append},
		{Name: "events", Replay: source.Superset, Mode: destination.Append, Keyed: true},
	}
	if err := Check(postgres, ok, streams); err != nil {
		t.Errorf("Check: %v", err)
	}
	untimed := ok
	untimed.CommitInterval = 0
	if err := Check(postgres, untimed, streams); err != nil {
		t.Errorf("Check with no commit interval: %v", err)
	}

	bad := ok
	bad.CommitInterval = 100 * time.Millisecond
	err := Check(postgres, bad, append(streams,
		Stream{Name: "views", Replay: source.Superset, Mode: destination.Append},
		Stream{Name: "users", Replay: source.Superset, Mode: destination.Merge}))
	for _, want := range []string{
		`delivery.commit.interval: 100ms is below the destination's minimum, 1s`,
		`delivery.require: stream "views" gets at_least_once (superset replay, and no primary key to dedupe on), below exactly_once`,
		`destination.write: merge needs a primary key, and stream "users" has none`,
	} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Check = %v, want it to say %q", err, want)
		}
	}
	for _, name := range []string{"orders", "logs", "events"} {
		if strings.Contains(err.Error(), `"`+name+`"`) {
			t.Errorf("Check failed %s: %v", name, err)
		}
	}

	err = Check(parquet, ok, streams)
	for _, want := range []string{
		`delivery.commit.scope: the destination doesn't support pipeline`,
		`stream "events" gets at_least_once (superset replay, and the destination can't dedupe), below exactly_once`,
	} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Check = %v, want it to say %q", err, want)
		}
	}

	least := ok
	least.Require = AtLeastOnce
	least.Scope = destination.ScopeStream
	if err := Check(parquet, least, streams); err != nil {
		t.Errorf("Check at least once: %v", err)
	}
}
