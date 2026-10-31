// Package delivery computes each stream's delivery guarantee, and checks a
// pipeline's spec.delivery against its destination and streams.
package delivery

import (
	"errors"
	"fmt"
	"time"

	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// Level is a delivery guarantee. Higher is stronger.
type Level int

const (
	AtLeastOnce Level = iota + 1
	// Repeats are keyed and guarded by _openetl_seq, so the final state is
	// the same.
	EffectivelyOnce
	ExactlyOnce
)

func (l Level) String() string {
	switch l {
	case AtLeastOnce:
		return "at_least_once"
	case EffectivelyOnce:
		return "effectively_once"
	case ExactlyOnce:
		return "exactly_once"
	default:
		return "unknown"
	}
}

// Settings are a pipeline's spec.delivery, with defaults filled in.
type Settings struct {
	// The lowest level the pipeline accepts. Each stream reaches it the
	// cheapest way it can, and a barrier is placed without a resume point
	// only where every stream stays at Require.
	Require Level
	// 0 disables.
	CommitInterval time.Duration
	// Bytes read since the last barrier; 0 disables.
	CommitMaxBytes int64
	Scope          destination.Scope
	// How long a barrier waits for a resume point before it is forced; 0
	// waits forever.
	ResumePointTimeout time.Duration
}

// Stream is what sets one stream's level. A zero Replay counts as superset.
type Stream struct {
	Name   string
	Replay source.Replay
	Mode   destination.WriteMode
	// A stream without a primary key is never deduped, so rows the source
	// holds twice land twice.
	Keyed bool
}

// Plan is how a stream meets a required level.
type Plan struct {
	Level Level
	// Append and overwrite writes are deduped on _openetl_id at apply.
	Dedupe bool
	// The inputs that set Level.
	Why string
}

// PlanStream returns the level a stream gets under require. It dedupes at
// apply only when the commit protocol alone falls short of require.
func PlanStream(caps destination.Capabilities, s Stream, require Level) Plan {
	switch {
	case caps.Class == destination.DeliverAndAck:
		return Plan{Level: AtLeastOnce, Why: "the destination delivers and acknowledges"}
	case s.Replay == source.Exact && (caps.Class == destination.StageAndApply || caps.Class == destination.TransactionalStream):
		return Plan{Level: ExactlyOnce, Why: "exact replay"}
	case caps.Class == destination.StageAndApply && s.Mode == destination.Merge:
		return Plan{Level: EffectivelyOnce, Why: "superset replay, written with merge"}
	case caps.Class != destination.StageAndApply || !caps.DedupeAtApply:
		return Plan{Level: AtLeastOnce, Why: "superset replay, and the destination can't dedupe"}
	case !s.Keyed:
		return Plan{Level: AtLeastOnce, Why: "superset replay, and no primary key to dedupe on"}
	case require > AtLeastOnce:
		return Plan{Level: ExactlyOnce, Dedupe: true, Why: "superset replay, deduped at apply"}
	default:
		return Plan{Level: AtLeastOnce, Why: "superset replay, not deduped under at_least_once"}
	}
}

// ShouldDedupe reports whether a stream is deduped at apply in this attempt.
// After a restore from a forced barrier, it counts as superset.
func ShouldDedupe(caps destination.Capabilities, s Stream, require Level, restoredFromForced bool) bool {
	if restoredFromForced {
		s.Replay = source.Superset
	}
	return PlanStream(caps, s, require).Dedupe
}

// CanForce reports whether a barrier can be placed on a stream without a
// resume point: whether the stream, counted as superset, still meets
// require.
func CanForce(caps destination.Capabilities, s Stream, require Level) bool {
	s.Replay = source.Superset
	return PlanStream(caps, s, require).Level >= require
}

// Check runs the plan checks on a pipeline's settings and streams, and
// returns every failure.
func Check(caps destination.Capabilities, s Settings, streams []Stream) error {
	var fails []error
	if !caps.SupportsScope(s.Scope) {
		fails = append(fails, fmt.Errorf("delivery.commit.scope: the destination doesn't support %s", scopeName(s.Scope)))
	}
	if s.CommitInterval > 0 && s.CommitInterval < caps.MinCommitInterval {
		fails = append(fails, fmt.Errorf("delivery.commit.interval: %v is below the destination's minimum, %v", s.CommitInterval, caps.MinCommitInterval))
	}
	for _, st := range streams {
		if st.Mode == destination.Merge && !st.Keyed {
			fails = append(fails, fmt.Errorf("destination.write: merge needs a primary key, and stream %q has none", st.Name))
			continue
		}
		if p := PlanStream(caps, st, s.Require); p.Level < s.Require {
			fails = append(fails, fmt.Errorf("delivery.require: stream %q gets %v (%s), below %v", st.Name, p.Level, p.Why, s.Require))
		}
	}
	return errors.Join(fails...)
}

func scopeName(s destination.Scope) string {
	switch s {
	case destination.ScopePipeline:
		return "pipeline"
	case destination.ScopeStream:
		return "stream"
	default:
		return fmt.Sprintf("scope %d", s)
	}
}
