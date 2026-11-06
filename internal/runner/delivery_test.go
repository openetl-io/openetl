package runner

import (
	"fmt"
	"testing"

	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
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
