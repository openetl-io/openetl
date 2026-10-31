package delivery

import (
	"testing"

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
