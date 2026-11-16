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
	// The newest generation of a record whose write the proxy received,
	// and of one written.
	received, written int64
	// When its attempt was last killed, until a record shows a later one
	// went on; and the newest generation received by then.
	killed    time.Time
	killedGen int64
	resumes   []time.Duration
	// The records committed when each of the run's own faults landed.
	behind []int
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
	if !r.killed.IsZero() && at.After(r.killed) && gen > r.killedGen {
		r.resumes = append(r.resumes, now.Sub(r.killed))
		r.killed = time.Time{}
	}
}

// Head notes a HEAD swap: a record committed.
func (r *Records) Head() { r.heads++ }

// Killed notes that the pipeline's attempt is killed at `at`: every record
// received so far is of that attempt or an older one. It's noted before
// the kill, so none of the attempt's records arrives unnoticed after.
func (r *Records) Killed(at time.Time) {
	r.killed, r.killedGen = at, r.received
}

// Landed notes that one of the run's own faults landed.
func (r *Records) Landed() { r.behind = append(r.behind, r.heads) }

// Ready reports whether a fault may land: two records are committed, the
// newest attempt that sent one has written one, and no kill waits for a
// later attempt's record.
func (r *Records) Ready() bool {
	return r.heads >= 2 && r.written == r.received && r.killed.IsZero()
}

// Resuming reports whether a kill waits for a later attempt's record.
func (r *Records) Resuming() bool { return !r.killed.IsZero() }

// Resumes are how long each resume took, from the kill to the record.
func (r *Records) Resumes() []time.Duration { return r.resumes }

// Strict is what a strict run with `faults` faults, `kills` of which kill
// its attempt, fails of: a fault that didn't land, one that landed with
// fewer than two committed records behind it, and a kill with no record
// of a later attempt after it.
func (r *Records) Strict(faults, kills int) []string {
	var out []string
	if len(r.behind) < faults {
		out = append(out, fmt.Sprintf("%d of %d faults landed", len(r.behind), faults))
	}
	for _, n := range r.behind {
		if n < 2 {
			out = append(out, fmt.Sprintf("a fault landed with %d records committed behind it", n))
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
