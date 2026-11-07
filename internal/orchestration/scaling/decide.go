// Package scaling is the job loop for continuous pipelines (ADR-0027 §7): it
// watches each running stream job's lanes, and rescales a job whose lanes
// call for more or fewer.
package scaling

import (
	"math"
	"time"

	"github.com/openetl-io/openetl/internal/platform/resources"
)

// DestinationBound is the note for a job whose lanes mostly wait for its
// destination, which more lanes won't help.
const DestinationBound = "destination_bound"

// Sample is a running job's lanes as its runner reported them, with their
// time and rows since it started.
type Sample struct {
	Time         time.Time
	Lanes, Limit int
	// Seconds, summed over its lanes.
	Busy, Backpressured, Idle float64
	Rows                      int64
}

// Lag is how far behind its source the job's read was at a checkpoint, at
// most over its streams, in whatever unit its source gives.
type Lag struct {
	Time  time.Time
	Value float64
}

// Windows are how long the loop watches before it acts.
type Windows struct {
	// Lag must have risen for this long before it grows a job. Zero: 5 min.
	Grow time.Duration
	// Lanes must have been under half busy for this long before it shrinks a
	// job. Zero: 30 min.
	Shrink time.Duration
	// The least time between rescales. Zero: 10 min.
	Gap time.Duration
}

func (w Windows) grow() time.Duration   { return orDefault(w.Grow, 5*time.Minute) }
func (w Windows) shrink() time.Duration { return orDefault(w.Shrink, 30*time.Minute) }
func (w Windows) gap() time.Duration    { return orDefault(w.Gap, 10*time.Minute) }

func orDefault(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

// Input is what the loop knows of a running job.
type Input struct {
	Now time.Time
	// Oldest first.
	Samples []Sample
	Lag     []Lag
	// Its size is set, by the spec or a rescale, so only a note can change.
	Fixed bool
	// The spec's bounds, as sizes; zero: none, and xl at most.
	Min, Max resources.Size
	// When it was last rescaled; zero if never.
	LastRescale time.Time
	Windows     Windows
}

// Decision is what the loop does about a job.
type Decision struct {
	// The lanes to rescale it to; 0: leave it.
	Lanes int
	// Why it doesn't rescale, if the lanes would call for it otherwise.
	Note string
	// Its lanes' busy and backpressured shares over the grow window; -1
	// before the window has passed.
	Busy, Backpressured float64
}

// headroom is how far above the input rate a job's lanes are sized.
const headroom = 1.2

// Decide is what the loop does about a job. Its lanes should keep up with
// their input with a fifth to spare: lanes = ceil(input rate × 1.2 / each
// lane's true rate), the true rate being rows over busy time, not counting
// time waiting for what's downstream. Over a window, that's 1.2 × the lanes
// busy on average. It grows a job when that's more than it has and its lag
// has risen over the grow window, and shrinks one whose lanes were under
// half busy throughout the shrink window. A job whose lanes are mostly
// backpressured waits for its destination, so it's left as it is, with a
// note if it's falling behind.
func Decide(in Input) Decision {
	d := Decision{Busy: -1, Backpressured: -1}
	if len(in.Samples) == 0 {
		return d
	}
	last := in.Samples[len(in.Samples)-1]
	recent, ok := window(in.Samples, in.Windows.grow())
	if !ok {
		return d
	}
	d.Busy, d.Backpressured = recent.busy(), recent.backpressured()
	rising := rising(in.Lag, last.Time.Add(-in.Windows.grow()))
	// Fewer lanes would write more slowly, so it isn't shrunk either.
	if d.Backpressured >= 0.5 {
		if rising {
			d.Note = DestinationBound
		}
		return d
	}
	if in.Fixed || (!in.LastRescale.IsZero() && in.Now.Sub(in.LastRescale) < in.Windows.gap()) {
		return d
	}
	lo, hi := bounds(in.Min, in.Max, last.Limit)
	if want := lanesFor(needed(last.Lanes, d.Busy), lo, hi); want > last.Lanes && rising {
		d.Lanes = want
		return d
	}
	if rising || !underHalf(in.Samples, in.Windows.shrink(), in.Windows.grow()) {
		return d
	}
	whole, _ := window(in.Samples, in.Windows.shrink())
	if want := lanesFor(needed(last.Lanes, whole.busy()), lo, hi); want < last.Lanes {
		d.Lanes = want
	}
	return d
}

// span is what a job's lanes did between two samples, in lane-seconds.
type span struct{ working, waiting, idle float64 }

func (s span) total() float64 { return s.working + s.waiting + s.idle }

// busy is the share of their time the lanes worked.
func (s span) busy() float64 {
	if s.total() <= 0 {
		return 0
	}
	return s.working / s.total()
}

// backpressured is the share they waited for what's downstream.
func (s span) backpressured() float64 {
	if s.total() <= 0 {
		return 0
	}
	return s.waiting / s.total()
}

// window is what the lanes did over the last d of samples; false if they
// don't go back that far.
func window(samples []Sample, d time.Duration) (span, bool) {
	last := samples[len(samples)-1]
	return between(samples, last.Time.Add(-d), last)
}

// between is what the lanes did from the latest sample at or before from to
// last; false if there's none.
func between(samples []Sample, from time.Time, last Sample) (span, bool) {
	var first *Sample
	for i := range samples {
		if samples[i].Time.After(from) {
			break
		}
		first = &samples[i]
	}
	if first == nil || first.Time.Equal(last.Time) {
		return span{}, false
	}
	return span{last.Busy - first.Busy, last.Backpressured - first.Backpressured, last.Idle - first.Idle}, true
}

// underHalf reports whether the lanes were under half busy in every step
// across the last d of samples.
func underHalf(samples []Sample, d, step time.Duration) bool {
	last := samples[len(samples)-1]
	if _, ok := window(samples, d); !ok {
		return false
	}
	for end := last.Time; !end.Before(last.Time.Add(-d + step)); end = end.Add(-step) {
		var at *Sample
		for i := range samples {
			if !samples[i].Time.After(end) {
				at = &samples[i]
			}
		}
		if at == nil {
			return false
		}
		s, ok := between(samples, end.Add(-step), *at)
		if !ok || s.busy() >= 0.5 {
			return false
		}
	}
	return true
}

// needed is how many lanes keep up with what lanes busy for that share of
// their time take in, with headroom.
func needed(lanes int, busy float64) int {
	return max(1, int(math.Ceil(headroom*float64(lanes)*busy-1e-9)))
}

// bounds are the fewest and most lanes the spec's min and max allow, and the
// splits a job can use (0: no limit).
func bounds(lo, hi resources.Size, limit int) (int, int) {
	if hi == (resources.Size{}) {
		hi = resources.XL.Above(lo)
	}
	most := hi.Threads()
	if limit > 0 {
		most = min(most, limit)
	}
	least := 1
	if lo != (resources.Size{}) {
		least = lo.Threads()
	}
	return least, max(least, most)
}

// lanesFor is the lanes a job wanting n gets, from lo to hi: those of the
// smallest preset with a vCPU for each, so 3 is 4, unless hi is fewer.
func lanesFor(n, lo, hi int) int {
	n = min(max(n, lo), hi)
	return min(resources.Size{CPU: int64(n) * 1000}.Smallest().Threads(), hi)
}

// rising reports whether the lag rose from since on: it goes back that far,
// ends higher than it started, and its least-squares slope is positive.
func rising(lag []Lag, since time.Time) bool {
	var pts []Lag
	var before *Lag
	for i := range lag {
		if lag[i].Time.Before(since) {
			before = &lag[i]
			continue
		}
		pts = append(pts, lag[i])
	}
	if before == nil || len(pts) == 0 {
		return false
	}
	pts = append([]Lag{*before}, pts...)
	if pts[len(pts)-1].Value <= pts[0].Value {
		return false
	}
	var n, sx, sy, sxx, sxy float64
	for _, p := range pts {
		x := p.Time.Sub(since).Seconds()
		n, sx, sy, sxx, sxy = n+1, sx+x, sy+p.Value, sxx+x*x, sxy+x*p.Value
	}
	return n*sxy-sx*sy > 0 && n*sxx-sx*sx > 0
}
