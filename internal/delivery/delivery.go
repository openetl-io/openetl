// Package delivery computes each stream's delivery guarantee, and checks a
// pipeline's spec.delivery against its destination and streams.
package delivery

import (
	"cmp"
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
	// The lowest level the pipeline accepts from streams that don't set
	// their own. Each stream reaches its level the cheapest way it can, and a
	// barrier is placed without a resume point only where the stream stays
	// at it.
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
	// A stream without a primary key hashes its content into _openetl_id,
	// so a repeat and a row the source holds twice get the same ID.
	Keyed bool
	// Overrides the pipeline's require; 0: the pipeline's.
	Require Level
	// Whether rows with the same content become one row, for a stream
	// without a primary key. Nil: only for superset streams that require
	// more than at least once.
	Collapse *bool
}

// settled fills in the stream's require and collapse.
func (s Stream) settled(require Level) Stream {
	s.Require = cmp.Or(s.Require, require)
	if s.Collapse == nil {
		c := s.Replay != source.Exact && s.Require > AtLeastOnce
		s.Collapse = &c
	}
	return s
}

// Forced is the stream after a restore from a forced barrier: counted as
// superset, but with the collapse its declared replay gave it.
func (s Stream) Forced(require Level) Stream {
	s = s.settled(require)
	s.Replay = source.Superset
	return s
}

// Plan is how a stream meets a required level.
type Plan struct {
	Level Level
	// Which append and overwrite rows the apply skips.
	Dedupe destination.Dedupe
	// Rows with the same content become one row. An exact stream collapses
	// only after a restore from a forced barrier.
	Collapse bool
	// The inputs that set Level.
	Why string
}

// canDedupe reports whether the destination can skip rows by _openetl_id at
// apply, which merge doesn't need.
func canDedupe(caps destination.Capabilities, mode destination.WriteMode) bool {
	return caps.Class == destination.StageAndApply && caps.DedupeAtApply && mode != destination.Merge
}

// PlanStream returns the level a stream gets under the pipeline's require,
// unless it sets its own. It dedupes at apply only when the commit protocol
// alone falls short of require, and a stream without a primary key only if
// it collapses.
func PlanStream(caps destination.Capabilities, s Stream, require Level) Plan {
	s = s.settled(require)
	collapse := !s.Keyed && *s.Collapse && canDedupe(caps, s.Mode)
	p := Plan{Level: AtLeastOnce, Collapse: collapse}
	switch {
	case caps.Class == destination.DeliverAndAck:
		p.Collapse, p.Why = false, "the destination delivers and acknowledges"
	case s.Replay == source.Exact && (caps.Class == destination.StageAndApply || caps.Class == destination.TransactionalStream):
		p.Level, p.Why = ExactlyOnce, "exact replay"
	case caps.Class == destination.StageAndApply && s.Mode == destination.Merge:
		p.Level, p.Why = EffectivelyOnce, "superset replay, written with merge"
	case !canDedupe(caps, s.Mode):
		p.Why = "superset replay, and the destination can't dedupe"
	case !s.Keyed && !collapse:
		p.Why = "superset replay, no primary key, and not collapsed"
	case !s.Keyed && s.Require == AtLeastOnce:
		p.Dedupe, p.Why = destination.DedupeEpoch, "superset replay, no primary key: collapsed within each epoch"
	case !s.Keyed:
		p.Level, p.Dedupe, p.Why = ExactlyOnce, destination.DedupeTarget, "superset replay, no primary key: collapsed within each epoch and against the target"
	case s.Require > AtLeastOnce:
		p.Level, p.Dedupe, p.Why = ExactlyOnce, destination.DedupeTarget, "superset replay, deduped at apply"
	default:
		p.Why = "superset replay, not deduped under at_least_once"
	}
	return p
}

// Dedupe returns how a stream is deduped at apply in this attempt. After a
// restore from a forced barrier, it counts as superset.
func Dedupe(caps destination.Capabilities, s Stream, require Level, restoredFromForced bool) destination.Dedupe {
	if restoredFromForced {
		s = s.Forced(require)
	}
	return PlanStream(caps, s, require).Dedupe
}

// CanForce reports whether a barrier can be placed on a stream without a
// resume point: whether the stream, counted as superset, still meets its
// require.
func CanForce(caps destination.Capabilities, s Stream, require Level) bool {
	f := s.Forced(require)
	return PlanStream(caps, f, require).Level >= f.Require
}

// Check runs the plan checks on a pipeline's settings and streams, and
// returns every failure.
func Check(caps destination.Capabilities, s Settings, streams []Stream) error {
	return errors.Join(Problems(caps, s, streams)...)
}

// Problems is Check's failures, one by one.
func Problems(caps destination.Capabilities, s Settings, streams []Stream) []error {
	var fails []error
	if !caps.SupportsScope(s.Scope) {
		fails = append(fails, fmt.Errorf("delivery.commit.scope: the destination doesn't support %s", scopeName(s.Scope)))
	}
	if s.CommitInterval > 0 && s.CommitInterval < caps.MinCommitInterval {
		fails = append(fails, fmt.Errorf("delivery.commit.interval: %v is below the destination's minimum, %v", s.CommitInterval, caps.MinCommitInterval))
	}
	if caps.MaxCommitInterval > 0 && s.CommitInterval > caps.MaxCommitInterval {
		fails = append(fails, fmt.Errorf("delivery.commit.interval: %v is above the destination's maximum, %v", s.CommitInterval, caps.MaxCommitInterval))
	}
	unsupported := map[destination.WriteMode]bool{}
	for _, st := range streams {
		if err := ModeProblem(caps, st.Mode); err != nil && !unsupported[st.Mode] {
			unsupported[st.Mode] = true
			fails = append(fails, err)
		}
	}
	for _, st := range streams {
		if st.Mode == destination.Merge && !st.Keyed {
			fails = append(fails, fmt.Errorf("destination.write: merge needs a primary key, and stream %q has none", st.Name))
			continue
		}
		if !st.Keyed && st.Collapse != nil && *st.Collapse && !canDedupe(caps, st.Mode) {
			fails = append(fails, fmt.Errorf("source.streams[%s].collapse: the destination can't dedupe at apply, so it can't collapse rows", st.Name))
			continue
		}
		field := "delivery.require"
		if st.Require != 0 {
			field = fmt.Sprintf("source.streams[%s].require", st.Name)
		}
		require := cmp.Or(st.Require, s.Require)
		if p := PlanStream(caps, st, s.Require); p.Level < require {
			fails = append(fails, fmt.Errorf("%s: stream %q gets %v (%s), below %v", field, st.Name, p.Level, p.Why, require))
		}
	}
	return fails
}

// Warning is something a sync will do that the spec may not mean.
type Warning struct {
	// Empty if it's about the whole pipeline.
	Stream  string
	Message string
}

// Warnings returns a warning for each stream without a primary key that the
// destination could collapse, saying what its settings cost and what the
// others would, and for each keyed stream that sets collapse. Streams that
// fail Problems get none.
func Warnings(caps destination.Capabilities, s Settings, streams []Stream) []Warning {
	var out []Warning
	for _, st := range streams {
		if st.Keyed {
			if st.Collapse != nil {
				out = append(out, Warning{st.Name, "it has a primary key, so collapse is ignored: its IDs already tell a repeat from a row the source holds twice"})
			}
			continue
		}
		if !canDedupe(caps, st.Mode) {
			continue
		}
		set := st.settled(s.Require)
		above := set.Require > AtLeastOnce
		var msg string
		switch {
		case set.Replay == source.Exact && *set.Collapse && above:
			msg = "it has no primary key, and collapse: true lets barriers be forced on it. After a restore from a forced barrier, rows with the same content collapse into one for the rest of that attempt, within each epoch and against the target. " +
				"With collapse: false, every row lands, but nothing of it commits until it's read to the end, so a large table holds back every checkpoint and grows staging by its size."
		case set.Replay == source.Exact && *set.Collapse:
			msg = "it has no primary key, and with collapse: true, rows with the same content collapse into one within each epoch for the rest of an attempt that restores from a forced barrier. " +
				"With collapse: false, every row lands; barriers can be forced on it under at_least_once either way."
		case set.Replay == source.Exact:
			continue
		case *set.Collapse && above:
			msg = "it has no primary key, so rows with the same content collapse into one, within each epoch and against the target: repeats never land, and nor does the second copy of a row the source holds twice. " +
				"Each apply looks up every staged row in an index on _openetl_id, which costs more as the table grows. " +
				"collapse: true with require: at_least_once collapses only within each epoch, which costs less, but repeats from earlier syncs land again. " +
				"collapse: false with require: at_least_once keeps every row and costs nothing, but repeats land too."
		case *set.Collapse:
			msg = "it has no primary key, so rows with the same content collapse into one within each epoch only: the second copy of a row the source holds twice is lost when both are in one epoch, and repeats across epochs and syncs, such as the lookback window each sync reads again, still land. " +
				"It costs a DISTINCT ON over each epoch's staging. " +
				"collapse: false keeps every row and costs nothing. " +
				"require: exactly_once also collapses against the target, so repeats never land, at the cost of an index on _openetl_id and a lookup per row."
		case !above:
			msg = "it has no primary key and doesn't collapse, so every row lands: both copies of a row the source holds twice, and repeats, such as the lookback window each sync reads again. Readers can dedupe on _openetl_id. " +
				"collapse: true collapses rows with the same content within each epoch, and with require: exactly_once against the target too, so repeats never land."
		default:
			continue
		}
		out = append(out, Warning{st.Name, msg})
	}
	return out
}

// ModeProblem fails a write mode the destination doesn't support.
func ModeProblem(caps destination.Capabilities, m destination.WriteMode) error {
	if caps.SupportsMode(m) {
		return nil
	}
	return fmt.Errorf("destination.write: the destination doesn't support %s", modeName(m))
}

func modeName(m destination.WriteMode) string {
	switch m {
	case destination.Append:
		return "append"
	case destination.Overwrite:
		return "overwrite"
	case destination.Merge:
		return "merge"
	default:
		return fmt.Sprintf("write mode %d", m)
	}
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
