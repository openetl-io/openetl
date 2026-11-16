// Package ledger keeps what the chaos suite needs to know of one
// pipeline's checkpoint records, as its object-store proxy sees them: how
// many are committed, which attempt wrote each, and whether a later
// attempt wrote one after each kill. From that it tells whether a strict
// run's faults landed with committed state behind them and were followed
// by a later attempt.
package ledger

import (
	"fmt"
	"time"
)

// Records is one pipeline's. The zero value has seen nothing. It isn't
// safe for concurrent use.
type Records struct {
	heads int
	// HEAD swaps whose request arrived after the last resume's record: the
	// current attempt's commits.
	since int
	// The newest generation of a record whose write the proxy received,
	// and of one written.
	received, written int64
	// When its attempt was last killed, until a record shows a later one
	// went on; and the newest generation received by then.
	killed    time.Time
	killedGen int64
	// When the request of the record that ended the last kill arrived.
	resumed time.Time
	resumes []time.Duration
	// Each of the run's own faults as it landed: the records committed
	// behind it, in all and by the attempt it landed on.
	behind [][2]int
}

// Received notes a request to write a record of generation gen, as it
// arrives.
func (r *Records) Received(gen int64) { r.received = max(r.received, gen) }

// Wrote notes that the write of a record of generation gen, whose request
// arrived at `at`, succeeded, at now. It's a resume only if the request
// arrived after the last kill and gen is newer than every record received
// by then, so a record of the attempt killed, still in flight, isn't.
func (r *Records) Wrote(gen int64, at, now time.Time) {
	r.written = max(r.written, gen)
	if r.Resuming() && at.After(r.killed) && gen > r.killedGen {
		r.resumes = append(r.resumes, now.Sub(r.killed))
		r.killed, r.resumed, r.since = time.Time{}, at, 0
	}
}

// Head notes a HEAD swap, whose request arrived at `at`: a record
// committed. It's the current attempt's if it arrived no sooner than the
// record that ended the last kill.
func (r *Records) Head(at time.Time) {
	r.heads++
	if !r.Resuming() && !at.Before(r.resumed) {
		r.since++
	}
}

// Killed notes that the pipeline's attempt is killed at `at`, by another
// run's fault or one the run doesn't make a claim about: every record
// received so far is of that attempt or an older one. It's noted before
// the kill, so none of the attempt's records arrives unnoticed after.
func (r *Records) Killed(at time.Time) {
	r.killed, r.killedGen, r.since = at, r.received, 0
}

// Landed notes that one of the run's own faults that doesn't kill its
// attempt landed.
func (r *Records) Landed() { r.behind = append(r.behind, [2]int{r.heads, r.since}) }

// LandedKill notes that one of the run's own faults lands at `at` and
// kills its attempt: what's behind it, and the death, together, before the
// kill.
func (r *Records) LandedKill(at time.Time) {
	r.Landed()
	r.Killed(at)
}

// Ready reports whether a fault may land: two records are committed, the
// current attempt has committed one since the last kill, no kill waits for
// a later attempt's record, and the newest attempt that sent a record has
// written one.
func (r *Records) Ready() bool {
	return r.heads >= 2 && r.since >= 1 && !r.Resuming() && r.written == r.received
}

// Resuming reports whether a kill waits for a later attempt's record.
func (r *Records) Resuming() bool { return !r.killed.IsZero() }

// Resumes are how long each resume took, from the kill to the record.
func (r *Records) Resumes() []time.Duration { return r.resumes }

// Strict is what a strict run with `faults` faults, `kills` of which kill
// its attempt, fails of: a fault that didn't land; one that landed with
// fewer than two committed records behind it, or none committed by the
// attempt it landed on; and a kill with no record of a later attempt
// after it.
func (r *Records) Strict(faults, kills int) []string {
	var out []string
	if len(r.behind) < faults {
		out = append(out, fmt.Sprintf("%d of %d faults landed", len(r.behind), faults))
	}
	for _, b := range r.behind {
		if b[0] < 2 || b[1] < 1 {
			out = append(out, fmt.Sprintf("a fault landed with %d records committed behind it, %d by its attempt", b[0], b[1]))
		}
	}
	switch {
	case r.Resuming():
		out = append(out, "no record of a later attempt after the last kill")
	case kills > 0 && len(r.resumes) == 0:
		out = append(out, "no record of a later attempt after a kill")
	}
	return out
}
