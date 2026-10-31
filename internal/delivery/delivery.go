// Package delivery computes each stream's delivery guarantee.
package delivery

import (
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

func StreamLevel(caps destination.Capabilities, replay source.Replay, mode destination.WriteMode, dedupe Dedupe) Level {
	switch caps.Class {
	case destination.DeliverAndAck:
		return AtLeastOnce
	case destination.TransactionalStream:
		if replay == source.Exact {
			return ExactlyOnce
		}
		return AtLeastOnce
	case destination.StageAndApply:
		switch {
		case replay == source.Exact:
			return ExactlyOnce
		case mode == destination.Merge:
			return EffectivelyOnce
		case caps.DedupeAtApply && dedupe == DedupeAuto:
			return ExactlyOnce
		default:
			return AtLeastOnce
		}
	default:
		return AtLeastOnce
	}
}

// ShouldDedupe reports whether a stream's append and overwrite writes are
// deduped at apply: superset streams always, exact streams only after a
// restore from a forced barrier.
func ShouldDedupe(caps destination.Capabilities, replay source.Replay, mode destination.WriteMode, dedupe Dedupe, restoredFromForced bool) bool {
	return caps.Class == destination.StageAndApply && caps.DedupeAtApply && dedupe == DedupeAuto &&
		mode != destination.Merge && (replay == source.Superset || restoredFromForced)
}
