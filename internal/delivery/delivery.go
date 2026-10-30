// Package delivery computes each stream's delivery guarantee from its sink
// class, replay, write mode and dedupe setting (ADR-0016).
package delivery

import (
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// Level is a delivery guarantee. Higher is stronger.
type Level int

const (
	// AtLeastOnce: a record version can land more than once, with the same
	// _openetl_id each time.
	AtLeastOnce Level = iota + 1
	// EffectivelyOnce: a version can be applied more than once, but each apply
	// is keyed and guarded by _openetl_seq, so the destination ends up the same.
	EffectivelyOnce
	// ExactlyOnce: each record version is applied once.
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

// Dedupe is the pipeline's dedupe-at-apply setting.
type Dedupe int

const (
	DedupeAuto Dedupe = iota + 1
	DedupeNone
)

// StreamLevel returns the guarantee a stream gets.
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

// ShouldDedupe reports whether the sink should dedupe a stream's append and
// overwrite writes at apply. Superset streams always can repeat records; an
// exact stream repeats them only after a restore from a checkpoint whose
// barrier was forced on it.
func ShouldDedupe(caps destination.Capabilities, replay source.Replay, mode destination.WriteMode, dedupe Dedupe, restoredFromForced bool) bool {
	return caps.Class == destination.StageAndApply && caps.DedupeAtApply && dedupe == DedupeAuto &&
		mode != destination.Merge && (replay == source.Superset || restoredFromForced)
}
