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

type Dedupe int

const (
	DedupeAuto Dedupe = iota + 1
	DedupeNone
)

// Settings are a pipeline's spec.delivery, with defaults filled in.
type Settings struct {
	// A barrier is placed without a resume point only where every stream stays
	// at Require.
	Require Level
	// 0 disables.
	CommitInterval time.Duration
	// Bytes read since the last barrier; 0 disables.
	CommitMaxBytes int64
	Scope          destination.Scope
	// How long a barrier waits for a resume point before it is forced; 0
	// waits forever.
	ResumePointTimeout time.Duration
	Dedupe             Dedupe
}

// Stream is what sets one stream's level. A zero Replay counts as superset.
type Stream struct {
	Name   string
	Replay source.Replay
	Mode   destination.WriteMode
}

func StreamLevel(caps destination.Capabilities, replay source.Replay, mode destination.WriteMode, dedupe Dedupe) Level {
	l, _ := Explain(caps, replay, mode, dedupe)
	return l
}

// Explain returns a stream's level and the inputs that set it.
func Explain(caps destination.Capabilities, replay source.Replay, mode destination.WriteMode, dedupe Dedupe) (Level, string) {
	switch {
	case caps.Class == destination.DeliverAndAck:
		return AtLeastOnce, "the destination delivers and acknowledges"
	case replay == source.Exact && (caps.Class == destination.StageAndApply || caps.Class == destination.TransactionalStream):
		return ExactlyOnce, "exact replay"
	case caps.Class == destination.StageAndApply && mode == destination.Merge:
		return EffectivelyOnce, "superset replay, written with merge"
	case caps.Class == destination.StageAndApply && caps.DedupeAtApply && dedupe == DedupeAuto:
		return ExactlyOnce, "superset replay, deduped at apply"
	case caps.Class == destination.StageAndApply && caps.DedupeAtApply:
		return AtLeastOnce, "superset replay with dedupe: none"
	default:
		return AtLeastOnce, "superset replay, and the destination can't dedupe"
	}
}

// ShouldDedupe reports whether a stream's append and overwrite writes are
// deduped at apply: superset streams always, exact streams only after a
// restore from a forced barrier.
func ShouldDedupe(caps destination.Capabilities, replay source.Replay, mode destination.WriteMode, dedupe Dedupe, restoredFromForced bool) bool {
	return caps.Class == destination.StageAndApply && caps.DedupeAtApply && dedupe == DedupeAuto &&
		mode != destination.Merge && (replay != source.Exact || restoredFromForced)
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
		if l, why := Explain(caps, st.Replay, st.Mode, s.Dedupe); l < s.Require {
			fails = append(fails, fmt.Errorf("delivery.require: stream %q gets %v (%s), below %v", st.Name, l, why, s.Require))
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
