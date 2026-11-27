// Package ledger keeps what the chaos suite needs to know of one
// pipeline's attempts, as its object-store proxy sees their checkpoint
// writes, to hold a strict run to one rule:
//
// A fault may land only when the newest attempt the ledger has seen has
// itself committed at least two records and no kill waits; and a kill is
// resumed only by a record committed by an attempt newer than every one
// the ledger had seen at the kill.
//
// An attempt is seen by its generation, as soon as a request of it
// arrives: a record's write, whose name has the generation, or a HEAD's,
// whose body has the writer's generation (a claim's too). A record is
// committed when a HEAD naming it is written, and it's the commit of the
// attempt of the record's generation. A HEAD that names no record commits
// nothing.
package ledger

import (
	"fmt"
	"slices"
	"time"
)

// Commits an attempt makes before a fault may land on it.
const Commits = 2

// Records is one pipeline's. The zero value has seen nothing. It isn't
// safe for concurrent use.
type Records struct {
	// The newest generation seen.
	newest int64
	// Records committed, by the generation that wrote them.
	commits map[int64]int
	// When the last kill came, until a newer attempt commits; and the
	// newest generation seen then.
	killed    time.Time
	killedGen int64
	resumes   []time.Duration
	// The run's own faults that landed.
	landed []landing
}

// landing is one of the run's own faults: whether the pipeline was ready
// as it landed; and for a kill, when it came, and the kill noted before
// it, which Unland puts back.
type landing struct {
	ready     bool
	at        time.Time
	before    time.Time
	beforeGen int64
}

// Seen notes a request of the attempt of generation gen, as it arrives.
func (r *Records) Seen(gen int64) { r.newest = max(r.newest, gen) }

// Committed notes that a HEAD naming a record of generation gen was
// written, at now; 0 if it names none.
func (r *Records) Committed(gen int64, now time.Time) {
	if gen == 0 {
		return
	}
	if r.commits == nil {
		r.commits = map[int64]int{}
	}
	r.commits[gen]++
	if r.Resuming() && gen > r.killedGen {
		r.resumes = append(r.resumes, now.Sub(r.killed))
		r.killed = time.Time{}
	}
}

// Killed notes that the pipeline's attempt is killed at `at`, by another
// run's fault or one this run makes no claim about. It's noted before the
// kill, so every request of the attempt killed arrives after a request it
// made before, which the ledger has seen.
func (r *Records) Killed(at time.Time) {
	r.killed, r.killedGen = at, r.newest
}

// Landed notes that one of the run's own faults that doesn't kill an
// attempt lands.
func (r *Records) Landed() { r.landed = append(r.landed, landing{ready: r.Ready()}) }

// LandedKill notes that one of the run's own faults lands at `at` and
// kills the pipeline's attempt, before it does.
func (r *Records) LandedKill(at time.Time) {
	r.landed = append(r.landed, landing{ready: r.Ready(), at: at, before: r.killed, beforeGen: r.killedGen})
	r.Killed(at)
}

// Unland retracts the kill LandedKill noted at `at`, which then found
// nothing to kill. It no longer counts as landed; and unless a kill or a
// newer attempt's commit has been noted since, the kill noted before it is
// back, or none.
func (r *Records) Unland(at time.Time) {
	i := slices.IndexFunc(r.landed, func(l landing) bool { return !l.at.IsZero() && l.at.Equal(at) })
	if i < 0 {
		return
	}
	l := r.landed[i]
	r.landed = slices.Delete(r.landed, i, i+1)
	// A kill noted after it, as a run that isn't strict may, now comes
	// after the one before it.
	for j := range r.landed {
		if r.landed[j].before.Equal(at) {
			r.landed[j].before, r.landed[j].beforeGen = l.before, l.beforeGen
		}
	}
	if r.killed.Equal(at) {
		r.killed, r.killedGen = l.before, l.beforeGen
	}
}

// Ready reports whether a fault may land now: the rule's first half.
func (r *Records) Ready() bool {
	return !r.Resuming() && r.commits[r.newest] >= Commits
}

// Resuming reports whether a kill waits for a newer attempt's commit.
func (r *Records) Resuming() bool { return !r.killed.IsZero() }

// Resumes are how long each resume took, from the kill to the commit.
func (r *Records) Resumes() []time.Duration { return r.resumes }

// Strict is what a strict run with `faults` faults, `kills` of which kill
// the pipeline's attempt, fails of: a fault that didn't land, one that
// landed when the pipeline wasn't ready, and a kill no newer attempt's
// commit followed.
func (r *Records) Strict(faults, kills int) []string {
	var out []string
	if len(r.landed) < faults {
		out = append(out, fmt.Sprintf("%d of %d faults landed", len(r.landed), faults))
	}
	for i, l := range r.landed {
		if !l.ready {
			out = append(out, fmt.Sprintf("fault %d landed before the newest attempt had committed %d records, or while a kill waited", i+1, Commits))
		}
	}
	switch {
	case r.Resuming():
		out = append(out, "no commit by a newer attempt after the last kill")
	case kills > 0 && len(r.resumes) == 0:
		out = append(out, "no commit by a newer attempt after a kill")
	}
	return out
}
