package runner

import (
	"math"
	"slices"
	"time"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/plan"
)

// EndOfInput is the watermark of a split whose input has ended.
const EndOfInput int64 = math.MaxInt64

// EventTime is how a stream's watermarks are made, in the engine, from its
// rows' time, and in the runner, from its splits' reads. A split's
// watermark trails the latest time it has read by Delay and never goes
// back; a split that completes, or of a continuous read that has had
// nothing to read for Idle, moves on with the clock, less Delay.
type EventTime struct {
	// Under its source name.
	Column string
	Delay  time.Duration
	// Its rows aren't read in the order of their time, as a table read by
	// key isn't: each split's watermark holds until its read ends, and is
	// the end of input then. A new read starts afresh.
	Held bool
	// Zero: plan.IdleAfter.
	Idle time.Duration
	// How long after a group window over the stream closes a row may still
	// come.
	Lateness time.Duration
}

func (e *EventTime) idle() time.Duration {
	if e.Idle > 0 {
		return e.Idle
	}
	return plan.IdleAfter
}

// idleAt is a split's watermark once it's idle at now, or has completed.
func (e *EventTime) idleAt(now time.Time) int64 {
	if e.Held {
		return EndOfInput
	}
	return now.Add(-e.Delay).UnixMicro()
}

// carryMarks gives the splits of a read that starts their watermarks from
// the read before's, prev: a split prev has keeps its own, and another
// starts at the least of prev's, so no window the last read closed opens
// again. A held stream's read starts afresh.
func carryMarks(e *EventTime, prev, splits []checkpoint.SplitState) []checkpoint.SplitState {
	if e == nil || e.Held {
		return splits
	}
	var least *int64
	for _, p := range prev {
		if p.Watermark == nil {
			return splits
		}
		if least == nil || *p.Watermark < *least {
			least = p.Watermark
		}
	}
	out := slices.Clone(splits)
	for i, sp := range out {
		if j := slices.IndexFunc(prev, func(p checkpoint.SplitState) bool { return p.ID == sp.ID }); j >= 0 {
			out[i].Watermark = prev[j].Watermark
		} else {
			out[i].Watermark = least
		}
	}
	return out
}

// floors are the least watermark of each stream with event time over the
// splits an attempt reads, which a lane past an exchange starts at; a
// stream any of whose splits has none is missing.
func floors(streams []Stream, splits map[string][]checkpoint.SplitState) map[string]int64 {
	out := map[string]int64{}
	for _, s := range streams {
		if s.EventTime == nil {
			continue
		}
		least, some := int64(math.MaxInt64), false
		for _, sp := range splits[s.Name] {
			if sp.Complete {
				continue
			}
			if sp.Watermark == nil {
				some = false
				break
			}
			least, some = min(least, *sp.Watermark), true
		}
		if some {
			out[s.Name] = least
		}
	}
	return out
}

// markSplits sets each split's watermark from the dataflow's report of
// the epoch, by stream and split, where it has one.
func markSplits(splits map[string][]checkpoint.SplitState, marks map[string]map[string]int64) {
	for name, ss := range splits {
		m := marks[name]
		if m == nil {
			continue
		}
		for i := range ss {
			if wm, ok := m[ss[i].ID]; ok {
				ss[i].Watermark = &wm
			}
		}
	}
}
