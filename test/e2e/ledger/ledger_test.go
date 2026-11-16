package ledger

import (
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func at(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }

// commit is a record of generation gen written and committed at second s.
func commit(r *Records, gen int64, s int) {
	r.Seen(gen)
	r.Committed(gen, at(s))
}

// claim is generation gen taking the pipeline: a HEAD of it naming the
// record of generation of, or none for 0.
func claim(r *Records, gen, of int64, s int) {
	r.Seen(gen)
	r.Committed(of, at(s))
}

func fails(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %q, want %d problems like %q", got, len(want), want)
	}
	for _, w := range want {
		if !slices.ContainsFunc(got, func(g string) bool { return strings.Contains(g, w) }) {
			t.Errorf("got %q, want one with %q", got, w)
		}
	}
}

// A kill after the attempt committed twice, followed by the next
// attempt's commit, passes.
func TestAKillWithStateBehindAndAResumePasses(t *testing.T) {
	var r Records
	claim(&r, 1, 0, 0)
	commit(&r, 1, 1)
	if r.Ready() {
		t.Fatal("ready after one commit")
	}
	commit(&r, 1, 2)
	if !r.Ready() {
		t.Fatal("not ready after two")
	}
	r.LandedKill(at(3))
	if r.Ready() || !r.Resuming() {
		t.Fatal("ready while the kill waits")
	}
	claim(&r, 2, 1, 4)
	if !r.Resuming() {
		t.Fatal("the next attempt's claim resumed the kill")
	}
	commit(&r, 2, 5)
	fails(t, r.Strict(1, 1))
	if got := r.Resumes(); !slices.Equal(got, []time.Duration{2 * time.Second}) {
		t.Errorf("resumes %v", got)
	}
}

// A strict run fails when its fault doesn't land.
func TestAFaultThatDidntLandFails(t *testing.T) {
	var r Records
	commit(&r, 1, 1)
	commit(&r, 1, 2)
	fails(t, r.Strict(1, 1), "0 of 1 faults landed", "no commit by a newer attempt after a kill")
	fails(t, r.Strict(1, 0), "0 of 1 faults landed")
}

// A strict run fails when its fault landed before the attempt had
// committed twice.
func TestAFaultWithoutStateBehindItFails(t *testing.T) {
	var r Records
	commit(&r, 1, 1)
	r.LandedKill(at(2))
	commit(&r, 2, 4)
	fails(t, r.Strict(1, 1), "fault 1 landed before")
}

// The killed attempt's own commits, in flight at the kill, don't resume
// it. A strict run fails while no newer attempt has committed, after one
// kill or the last of several.
func TestNoCommitOfANewerAttemptFails(t *testing.T) {
	var r Records
	commit(&r, 3, 1)
	commit(&r, 3, 2)
	r.LandedKill(at(3))
	r.Committed(3, at(4))
	if len(r.Resumes()) != 0 {
		t.Fatalf("the killed attempt's commit resumed it: %v", r.Resumes())
	}
	fails(t, r.Strict(1, 1), "no commit by a newer attempt after the last kill")

	commit(&r, 4, 6)
	commit(&r, 4, 7)
	r.LandedKill(at(8))
	fails(t, r.Strict(2, 2), "no commit by a newer attempt after the last kill")
	commit(&r, 5, 9)
	fails(t, r.Strict(2, 2))
}

// An attempt that replaces another without a kill, as a rescale's, isn't
// ready until it has committed twice itself: not at its claim, nor once its
// first record is written, nor after one commit.
func TestANewAttemptWithoutAKillIsntReadyUntilItCommits(t *testing.T) {
	var r Records
	commit(&r, 1, 1)
	commit(&r, 1, 2)
	claim(&r, 2, 1, 3)
	if r.Ready() {
		t.Fatal("ready at generation 2's claim")
	}
	r.Seen(2)
	if r.Ready() {
		t.Fatal("ready with generation 2's record written, before its HEAD")
	}
	r.Committed(2, at(4))
	if r.Ready() {
		t.Fatal("ready after one commit of generation 2")
	}
	r.LandedKill(at(5))
	commit(&r, 3, 6)
	fails(t, r.Strict(1, 1), "fault 1 landed before")
	r.Committed(2, at(5))
	r.Committed(3, at(7))
	if !r.Ready() {
		t.Fatal("not ready after generation 3 committed twice")
	}
}

// Two crash points armed at once, recorded then staged. The first kills
// attempt 1; the second may fire only once attempt 2 has committed twice.
// Had it fired at attempt 2's first record, the run fails though attempt 3
// went on.
func TestAQueuedCrashWaitsForTheNextAttemptsCommits(t *testing.T) {
	var r Records
	commit(&r, 1, 1)
	commit(&r, 1, 2)
	r.LandedKill(at(3))
	claim(&r, 2, 1, 4)
	r.Seen(2)
	if r.Ready() {
		t.Fatal("ready at the next attempt's first record")
	}
	wrong := r
	wrong.landed = slices.Clone(r.landed)
	wrong.LandedKill(at(4))
	commit(&wrong, 3, 6)
	fails(t, wrong.Strict(2, 2), "fault 2 landed before")

	r.Committed(2, at(5))
	r.Committed(2, at(6))
	if !r.Ready() {
		t.Fatal("not ready after the next attempt committed twice")
	}
	r.LandedKill(at(7))
	commit(&r, 3, 8)
	fails(t, r.Strict(2, 2))
	if got := len(r.Resumes()); got != 2 {
		t.Errorf("%d resumes", got)
	}
}

// A kill that isn't the run's own, as of another run's runner on the same
// slot, holds its faults until a newer attempt commits, and a run's writes
// stopping before then fail it.
func TestAnotherRunsKillHoldsTheFaults(t *testing.T) {
	var r Records
	commit(&r, 1, 1)
	commit(&r, 1, 2)
	r.Landed()
	r.Killed(at(3))
	if r.Ready() {
		t.Fatal("ready while another run's kill waits")
	}
	fails(t, r.Strict(1, 0), "no commit by a newer attempt after the last kill")
	commit(&r, 2, 4)
	fails(t, r.Strict(1, 0))
}

// Arbitrary histories of a few attempts: claims, records, commits arriving
// in flight past a kill, attempts replaced with and without kills, and the
// run's own faults landing at any time, or in half the histories only when
// the rule lets them, which then settle. A ground truth kept apart from
// the ledger says whether the rule held; the verdict passes exactly when
// it did.
func TestTheVerdictPassesExactlyWhenTheRuleHeld(t *testing.T) {
	type flight struct {
		gen int64
		due int
	}
	passed, failed := 0, 0
	for seed := range uint64(3000) {
		rng := rand.New(rand.NewPCG(seed, 7))
		var r Records
		// The truth: the live attempt (0: none), each attempt's commits,
		// and the generation started when the last kill came.
		live, started := int64(1), int64(1)
		commits := map[int64]int{}
		pending, pendingGen := false, int64(0)
		violated := false
		faults, kills := 0, 0
		var inFlight []flight
		r.Seen(1)
		honest := rng.IntN(2) == 0
		steps := 40 + rng.IntN(40)
		for step := 0; step < steps+20 || len(inFlight) > 0; step++ {
			now := at(step)
			// The run's writes stop at steps; its faults with them.
			settling := step >= steps
			// HEADs that were in flight land, those of dead attempts too.
			kept := inFlight[:0]
			for _, f := range inFlight {
				if f.due > step {
					kept = append(kept, f)
					continue
				}
				r.Committed(f.gen, now)
				commits[f.gen]++
				if pending && f.gen > pendingGen {
					pending = false
				}
			}
			inFlight = kept
			ready := live != 0 && !pending && commits[live] >= Commits
			n := rng.IntN(20)
			if settling && n >= 6 || honest && !ready && n >= 7 {
				n = 0
			}
			switch {
			case n < 5 && live != 0:
				// The live attempt writes a record and its HEAD, which
				// lands now or a few steps on.
				r.Seen(live)
				inFlight = append(inFlight, flight{live, step + rng.IntN(3)})
			case n < 5:
			case n == 5 && live == 0:
				// A new attempt claims the pipeline.
				started++
				live = started
				r.Seen(live)
				r.Committed(int64(rng.IntN(int(live))), now)
			case n == 5:
				// The live attempt is replaced without a kill.
				started++
				live = started
				r.Seen(live)
			case n == 6 && live != 0:
				// Another run kills it.
				r.Killed(now)
				pending, pendingGen, live = true, started, 0
			case n == 7:
				faults++
				violated = violated || !ready
				r.Landed()
			case n == 8:
				faults++
				kills++
				violated = violated || !ready
				r.LandedKill(now)
				pending, pendingGen, live = true, started, 0
			}
		}
		violated = violated || pending
		if got := r.Strict(faults, kills); (len(got) > 0) != violated {
			t.Fatalf("seed %d: the rule held: %v; the verdict: %q", seed, !violated, got)
		}
		if violated {
			failed++
		} else {
			passed++
		}
	}
	if passed < 100 || failed < 100 {
		t.Fatalf("%d histories passed, %d failed", passed, failed)
	}
}
