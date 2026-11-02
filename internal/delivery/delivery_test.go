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
		Scopes: []destination.Scope{destination.ScopePipeline, destination.ScopeStream}, MinCommitInterval: time.Second,
		WriteModes: []destination.WriteMode{destination.Append, destination.Overwrite, destination.Merge}}
	parquet = destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopeStream}}
	kafka   = destination.Capabilities{Class: destination.TransactionalStream}
	webhook = destination.Capabilities{Class: destination.DeliverAndAck}
)

const (
	none   = destination.DedupeNone
	epoch  = destination.DedupeEpoch
	target = destination.DedupeTarget
)

func ptr[T any](v T) *T { return &v }

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
		dedupe  destination.Dedupe
	}{
		{"postgres append exact", postgres, exact, appnd, true, once, once, none},
		{"postgres append exact unkeyed", postgres, exact, appnd, false, once, once, none},
		{"postgres append superset", postgres, superset, appnd, true, once, once, target},
		{"postgres append superset, at least once", postgres, superset, appnd, true, least, least, none},
		{"postgres append superset, effectively once", postgres, superset, appnd, true, EffectivelyOnce, once, target},
		{"postgres append superset unkeyed", postgres, superset, appnd, false, once, once, target},
		{"postgres append superset unkeyed, at least once", postgres, superset, appnd, false, least, least, none},
		{"postgres merge exact", postgres, exact, merge, true, once, once, none},
		{"postgres merge superset", postgres, superset, merge, true, once, EffectivelyOnce, none},
		{"parquet append exact", parquet, exact, appnd, true, once, once, none},
		{"parquet append superset", parquet, superset, appnd, true, once, least, none},
		{"parquet append superset unkeyed", parquet, superset, appnd, false, once, least, none},
		{"iceberg merge superset", parquet, superset, merge, true, once, EffectivelyOnce, none},
		{"kafka exact", kafka, exact, appnd, false, once, once, none},
		{"kafka superset", kafka, superset, appnd, true, once, least, none},
		{"webhook exact", webhook, exact, appnd, true, once, least, none},
		{"webhook superset", webhook, superset, merge, true, once, least, none},
	} {
		p := PlanStream(tc.caps, Stream{Replay: tc.replay, Mode: tc.mode, Keyed: tc.keyed}, tc.require)
		if p.Level != tc.want || p.Dedupe != tc.dedupe {
			t.Errorf("%s: level %v, dedupe %v; want %v, %v", tc.name, p.Level, p.Dedupe, tc.want, tc.dedupe)
		}
	}
}

func TestUndeclaredReplayIsSuperset(t *testing.T) {
	p := PlanStream(postgres, Stream{Mode: destination.Append, Keyed: true}, ExactlyOnce)
	if p.Level != ExactlyOnce || p.Dedupe != target || p.Why != "superset replay, deduped at apply" {
		t.Errorf("PlanStream = %+v", p)
	}
}

// The rows of ADR-0016's table for superset streams without a primary key,
// with the require set by the pipeline or by the stream.
func TestCollapse(t *testing.T) {
	for _, tc := range []struct {
		name     string
		collapse *bool
		require  Level // the stream's; 0: the pipeline's, exactly once
		want     Level
		dedupe   destination.Dedupe
		why      string
	}{
		{"default", nil, 0, ExactlyOnce, target, "superset replay, no primary key: collapsed within each epoch and against the target"},
		{"default, at least once", nil, AtLeastOnce, AtLeastOnce, none, "superset replay, no primary key, and not collapsed"},
		{"collapse, at least once", ptr(true), AtLeastOnce, AtLeastOnce, epoch, "superset replay, no primary key: collapsed within each epoch"},
		{"collapse, effectively once", ptr(true), EffectivelyOnce, ExactlyOnce, target, "superset replay, no primary key: collapsed within each epoch and against the target"},
		{"no collapse", ptr(false), 0, AtLeastOnce, none, "superset replay, no primary key, and not collapsed"},
		{"no collapse, at least once", ptr(false), AtLeastOnce, AtLeastOnce, none, "superset replay, no primary key, and not collapsed"},
	} {
		s := Stream{Name: "events", Replay: source.Superset, Mode: destination.Append, Require: tc.require, Collapse: tc.collapse}
		p := PlanStream(postgres, s, ExactlyOnce)
		if p.Level != tc.want || p.Dedupe != tc.dedupe || p.Why != tc.why || p.Collapse != (tc.dedupe != none) {
			t.Errorf("%s: PlanStream = %+v", tc.name, p)
		}
	}

	keyed := Stream{Replay: source.Superset, Mode: destination.Append, Keyed: true, Collapse: ptr(false)}
	if p := PlanStream(postgres, keyed, ExactlyOnce); p.Dedupe != target || p.Collapse {
		t.Errorf("keyed with collapse: false: PlanStream = %+v", p)
	}
	exact := Stream{Replay: source.Exact, Mode: destination.Append, Collapse: ptr(true)}
	if p := PlanStream(postgres, exact, ExactlyOnce); p.Level != ExactlyOnce || p.Dedupe != none || !p.Collapse {
		t.Errorf("exact with collapse: true: PlanStream = %+v", p)
	}
	if p := PlanStream(parquet, Stream{Replay: source.Superset, Mode: destination.Append}, AtLeastOnce); p.Collapse {
		t.Errorf("parquet: PlanStream = %+v", p)
	}
}

func TestStreamRequire(t *testing.T) {
	s := Stream{Replay: source.Superset, Mode: destination.Append, Keyed: true, Require: AtLeastOnce}
	if p := PlanStream(postgres, s, ExactlyOnce); p.Level != AtLeastOnce || p.Dedupe != none {
		t.Errorf("at_least_once under an exactly_once pipeline: PlanStream = %+v", p)
	}
	s.Require = ExactlyOnce
	if p := PlanStream(postgres, s, AtLeastOnce); p.Level != ExactlyOnce || p.Dedupe != target {
		t.Errorf("exactly_once under an at_least_once pipeline: PlanStream = %+v", p)
	}
}

func TestForcedBarriers(t *testing.T) {
	keyed := Stream{Replay: source.Exact, Mode: destination.Append, Keyed: true}
	unkeyed := Stream{Replay: source.Exact, Mode: destination.Append}
	collapsing := Stream{Replay: source.Exact, Mode: destination.Append, Collapse: ptr(true)}
	ownLeast := Stream{Replay: source.Exact, Mode: destination.Append, Require: AtLeastOnce}
	for _, tc := range []struct {
		name     string
		s        Stream
		require  Level
		canForce bool
		dedupe   destination.Dedupe // after a restore from a forced barrier
	}{
		{"keyed, exactly once", keyed, ExactlyOnce, true, target},
		{"keyed, at least once", keyed, AtLeastOnce, true, none},
		{"unkeyed, exactly once", unkeyed, ExactlyOnce, false, none},
		{"unkeyed, at least once", unkeyed, AtLeastOnce, true, none},
		{"unkeyed, collapse, exactly once", collapsing, ExactlyOnce, true, target},
		{"unkeyed, collapse, at least once", collapsing, AtLeastOnce, true, epoch},
		{"unkeyed, its own at least once", ownLeast, ExactlyOnce, true, none},
	} {
		if got := CanForce(postgres, tc.s, tc.require); got != tc.canForce {
			t.Errorf("%s: CanForce = %v", tc.name, got)
		}
		if got := Dedupe(postgres, tc.s, tc.require, true); got != tc.dedupe {
			t.Errorf("%s: Dedupe after a forced restore = %v", tc.name, got)
		}
		if got := Dedupe(postgres, tc.s, tc.require, false); got != none {
			t.Errorf("%s: an exact stream is deduped without a forced restore: %v", tc.name, got)
		}
	}
	superset := Stream{Replay: source.Superset, Mode: destination.Append}
	if !CanForce(postgres, superset, ExactlyOnce) {
		t.Error("a superset stream without a key that collapses against the target can't be forced")
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
		Stream{Name: "views", Replay: source.Superset, Mode: destination.Append, Collapse: ptr(false)},
		Stream{Name: "clicks", Replay: source.Superset, Mode: destination.Append, Keyed: true, Require: AtLeastOnce},
		Stream{Name: "users", Replay: source.Superset, Mode: destination.Merge}))
	for _, want := range []string{
		`delivery.commit.interval: 100ms is below the destination's minimum, 1s`,
		`delivery.require: stream "views" gets at_least_once (superset replay, no primary key, and not collapsed), below exactly_once`,
		`destination.write: merge needs a primary key, and stream "users" has none`,
	} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Check = %v, want it to say %q", err, want)
		}
	}
	for _, name := range []string{"orders", "logs", "events", "clicks"} {
		if strings.Contains(err.Error(), `"`+name+`"`) {
			t.Errorf("Check failed %s: %v", name, err)
		}
	}
	if strings.Contains(err.Error(), "doesn't support merge") {
		t.Errorf("Check refused merge into a destination that has it: %v", err)
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
	probs := Problems(parquet, least, []Stream{
		{Name: "users", Replay: source.Exact, Mode: destination.Overwrite, Keyed: true},
		{Name: "orders", Replay: source.Exact, Mode: destination.Overwrite, Keyed: true},
	})
	if len(probs) != 1 || probs[0].Error() != "destination.write: the destination doesn't support overwrite" {
		t.Errorf("Problems with overwrite into an append-only destination = %v, want one", probs)
	}

	err = Check(parquet, least, []Stream{
		{Name: "views", Replay: source.Superset, Mode: destination.Append, Collapse: ptr(true)},
		{Name: "logs", Replay: source.Exact, Mode: destination.Append, Collapse: ptr(false)},
		{Name: "events", Replay: source.Superset, Mode: destination.Append, Keyed: true, Collapse: ptr(true)},
		{Name: "orders", Replay: source.Superset, Mode: destination.Append, Keyed: true, Require: ExactlyOnce},
	})
	for _, want := range []string{
		`source.streams[views].collapse: the destination can't dedupe at apply, so it can't collapse rows`,
		`source.streams[orders].require: stream "orders" gets at_least_once (superset replay, and the destination can't dedupe), below exactly_once`,
	} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Check = %v, want it to say %q", err, want)
		}
	}
	for _, name := range []string{"logs", "events"} {
		if err != nil && strings.Contains(err.Error(), "["+name+"]") {
			t.Errorf("Check failed %s: %v", name, err)
		}
	}
}

func TestWarnings(t *testing.T) {
	settings := Settings{Require: ExactlyOnce, Scope: destination.ScopePipeline}
	streams := []Stream{
		{Name: "orders", Replay: source.Superset, Mode: destination.Append, Keyed: true},
		{Name: "keyed", Replay: source.Superset, Mode: destination.Append, Keyed: true, Collapse: ptr(true)},
		{Name: "logs", Replay: source.Exact, Mode: destination.Append},
		{Name: "big", Replay: source.Exact, Mode: destination.Append, Collapse: ptr(true)},
		{Name: "events", Replay: source.Superset, Mode: destination.Append},
		{Name: "pages", Replay: source.Superset, Mode: destination.Append, Require: AtLeastOnce, Collapse: ptr(true)},
		{Name: "views", Replay: source.Superset, Mode: destination.Append, Require: AtLeastOnce},
		{Name: "failing", Replay: source.Superset, Mode: destination.Append, Collapse: ptr(false)},
	}
	got := map[string]string{}
	for _, w := range Warnings(postgres, settings, streams) {
		if _, ok := got[w.Stream]; ok {
			t.Errorf("two warnings for %s", w.Stream)
		}
		got[w.Stream] = w.Message
	}
	for name, want := range map[string]string{
		"keyed":  "collapse is ignored",
		"big":    "lets barriers be forced on it",
		"events": "within each epoch and against the target",
		"pages":  "within each epoch only",
		"views":  "doesn't collapse, so every row lands",
	} {
		if !strings.Contains(got[name], want) {
			t.Errorf("warning for %s = %q, want it to say %q", name, got[name], want)
		}
		delete(got, name)
	}
	if len(got) > 0 {
		t.Errorf("unexpected warnings: %v", got)
	}
	if w := Warnings(parquet, settings, streams[2:]); len(w) != 0 {
		t.Errorf("parquet: %v", w)
	}
}
