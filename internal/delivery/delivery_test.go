package delivery

import (
	"strings"
	"testing"
	"time"

	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

func TestStreamLevel(t *testing.T) {
	postgres := destination.Capabilities{Class: destination.StageAndApply, DedupeAtApply: true}
	parquet := destination.Capabilities{Class: destination.StageAndApply}
	kafka := destination.Capabilities{Class: destination.TransactionalStream}
	webhook := destination.Capabilities{Class: destination.DeliverAndAck}
	const (
		exact    = source.Exact
		superset = source.Superset
		appnd    = destination.Append
		merge    = destination.Merge
		auto     = DedupeAuto
		none     = DedupeNone
	)
	for _, tc := range []struct {
		name   string
		caps   destination.Capabilities
		replay source.Replay
		mode   destination.WriteMode
		dedupe Dedupe
		want   Level
	}{
		{"postgres append exact", postgres, exact, appnd, auto, ExactlyOnce},
		{"postgres append superset auto", postgres, superset, appnd, auto, ExactlyOnce},
		{"postgres append superset none", postgres, superset, appnd, none, AtLeastOnce},
		{"postgres merge exact", postgres, exact, merge, auto, ExactlyOnce},
		{"postgres merge superset", postgres, superset, merge, none, EffectivelyOnce},
		{"parquet append exact", parquet, exact, appnd, auto, ExactlyOnce},
		{"parquet append superset", parquet, superset, appnd, auto, AtLeastOnce},
		{"iceberg merge superset", parquet, superset, merge, auto, EffectivelyOnce},
		{"kafka exact", kafka, exact, appnd, auto, ExactlyOnce},
		{"kafka superset", kafka, superset, appnd, auto, AtLeastOnce},
		{"webhook exact", webhook, exact, appnd, auto, AtLeastOnce},
		{"webhook superset", webhook, superset, merge, auto, AtLeastOnce},
	} {
		if got := StreamLevel(tc.caps, tc.replay, tc.mode, tc.dedupe); got != tc.want {
			t.Errorf("%s: level = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestUndeclaredReplayIsSuperset(t *testing.T) {
	postgres := destination.Capabilities{Class: destination.StageAndApply, DedupeAtApply: true}
	if l, why := Explain(postgres, 0, destination.Append, DedupeNone); l != AtLeastOnce || why != "superset replay with dedupe: none" {
		t.Errorf("Explain = %v, %q", l, why)
	}
	if !ShouldDedupe(postgres, 0, destination.Append, DedupeAuto, false) {
		t.Error("an undeclared replay isn't deduped")
	}
	if ShouldDedupe(postgres, source.Exact, destination.Append, DedupeAuto, false) {
		t.Error("an exact stream is deduped without a forced restore")
	}
	if !ShouldDedupe(postgres, source.Exact, destination.Append, DedupeAuto, true) {
		t.Error("an exact stream isn't deduped after a forced restore")
	}
}

func TestCheck(t *testing.T) {
	postgres := destination.Capabilities{Class: destination.StageAndApply, DedupeAtApply: true,
		Scopes: []destination.Scope{destination.ScopePipeline, destination.ScopeStream}, MinCommitInterval: time.Second}
	parquet := destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopeStream}}
	ok := Settings{Require: ExactlyOnce, Scope: destination.ScopePipeline, Dedupe: DedupeAuto, CommitInterval: time.Minute}
	streams := []Stream{
		{Name: "orders", Replay: source.Exact, Mode: destination.Append},
		{Name: "events", Replay: source.Superset, Mode: destination.Append},
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
	bad.Dedupe = DedupeNone
	bad.CommitInterval = 100 * time.Millisecond
	err := Check(postgres, bad, streams)
	for _, want := range []string{
		`delivery.commit.interval: 100ms is below the destination's minimum, 1s`,
		`delivery.require: stream "events" gets at_least_once (superset replay with dedupe: none), below exactly_once`,
	} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Check = %v, want it to say %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "orders") {
		t.Errorf("Check failed an exact stream: %v", err)
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
}
