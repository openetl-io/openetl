package ledger

import (
	"slices"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func at(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }

// commit is a record of generation gen whose request arrives at second s,
// written and committed then.
func commit(r *Records, gen int64, s int) {
	r.Received(gen)
	r.Wrote(gen, at(s), at(s))
	r.Head(at(s))
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

// A kill after two committed records, followed by a record of the next
// attempt, passes.
func TestAKillWithStateBehindAndAResumePasses(t *testing.T) {
	var r Records
	commit(&r, 1, 1)
	if r.Ready() {
		t.Fatal("ready after one record")
	}
	commit(&r, 1, 2)
	if !r.Ready() {
		t.Fatal("not ready after two")
	}
	r.LandedKill(at(3))
	if r.Ready() || !r.Resuming() {
		t.Fatal("ready while the kill waits for a record")
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
	fails(t, r.Strict(1, 1), "0 of 1 faults landed", "no record of a later attempt after a kill")
	fails(t, r.Strict(1, 0), "0 of 1 faults landed")
}

// A strict run fails when its fault landed with fewer than two committed
// records behind it.
func TestAFaultWithoutStateBehindItFails(t *testing.T) {
	var r Records
	commit(&r, 1, 1)
	r.LandedKill(at(2))
	commit(&r, 2, 4)
	fails(t, r.Strict(1, 1), "landed with 1 records committed behind it, 1 by its attempt")

	var p Records
	commit(&p, 1, 1)
	p.Landed()
	fails(t, p.Strict(1, 0), "landed with 1 records")
}

// The killed attempt's own records don't count as a resume: one still in
// flight that's written after the kill, and one whose request arrives
// after it. A strict run with no later attempt's record fails, after one
// kill or the last of several.
func TestNoRecordOfALaterAttemptFails(t *testing.T) {
	var r Records
	commit(&r, 3, 1)
	commit(&r, 3, 2)
	r.Received(3)
	r.LandedKill(at(3))
	r.Wrote(3, at(2), at(4))
	r.Wrote(3, at(5), at(5))
	r.Head(at(5))
	if len(r.Resumes()) != 0 {
		t.Fatalf("the killed attempt's records resumed it: %v", r.Resumes())
	}
	fails(t, r.Strict(1, 1), "no record of a later attempt after the last kill")

	commit(&r, 4, 6)
	commit(&r, 4, 7)
	r.LandedKill(at(8))
	fails(t, r.Strict(2, 2), "no record of a later attempt after the last kill")
	commit(&r, 5, 9)
	fails(t, r.Strict(2, 2))
}

// A fault waits for the newest attempt that sent a record to have written
// one.
func TestNotReadyWhileAnAttemptsFirstRecordIsInFlight(t *testing.T) {
	var r Records
	commit(&r, 1, 1)
	commit(&r, 1, 2)
	r.Received(2)
	if r.Ready() {
		t.Fatal("ready with generation 2's first record in flight")
	}
	r.Wrote(2, at(3), at(3))
	r.Head(at(3))
	if !r.Ready() {
		t.Fatal("not ready once it's written")
	}
}

// Two crash points armed at once, recorded then staged. The first kills
// attempt 1. The second must wait until attempt 2 has committed a record:
// not at its first record's request, nor once that record is written but
// not committed. Had it fired at attempt 2's first request, before any
// record of it, the run fails though attempt 3 went on.
func TestAQueuedCrashWaitsForTheNextAttemptsCommit(t *testing.T) {
	var r Records
	commit(&r, 1, 1)
	commit(&r, 1, 2)
	r.Received(1)
	r.LandedKill(at(3))
	r.Wrote(1, at(3), at(3))
	r.Received(2)
	if r.Ready() {
		t.Fatal("ready at the next attempt's first record")
	}
	wrong := r
	wrong.LandedKill(at(4))
	commit(&wrong, 3, 6)
	fails(t, wrong.Strict(2, 2), "0 by its attempt")

	r.Wrote(2, at(4), at(4))
	if r.Ready() {
		t.Fatal("ready before the next attempt committed a record")
	}
	r.Head(at(4))
	if !r.Ready() {
		t.Fatal("not ready after the next attempt committed")
	}
	r.Received(2)
	r.LandedKill(at(5))
	commit(&r, 3, 7)
	fails(t, r.Strict(2, 2))
	if got := len(r.Resumes()); got != 2 {
		t.Errorf("%d resumes", got)
	}
}

// A HEAD of the attempt killed, whose request arrived before the kill,
// isn't the next attempt's commit.
func TestTheKilledAttemptsHeadIsntTheNextOnes(t *testing.T) {
	var r Records
	commit(&r, 1, 1)
	commit(&r, 1, 2)
	r.LandedKill(at(3))
	r.Head(at(2))
	r.Received(2)
	r.Wrote(2, at(4), at(4))
	if r.Ready() {
		t.Fatal("ready on the killed attempt's HEAD")
	}
	r.Head(at(4))
	if !r.Ready() {
		t.Fatal("not ready on the next attempt's HEAD")
	}
}

// A kill that isn't the run's own, as of another run's runner on the same
// slot, holds its faults until a later attempt commits, and a run's writes
// stopping before then fails it.
func TestAnotherRunsKillHoldsTheFaults(t *testing.T) {
	var r Records
	commit(&r, 1, 1)
	commit(&r, 1, 2)
	r.Landed()
	r.Killed(at(3))
	if r.Ready() {
		t.Fatal("ready while another run's kill waits")
	}
	fails(t, r.Strict(1, 0), "no record of a later attempt after the last kill")
	commit(&r, 2, 4)
	fails(t, r.Strict(1, 0))
	if !r.Ready() {
		t.Fatal("not ready after the next attempt committed")
	}
}

// Three faults, two of them kills: each kill must be followed by a later
// attempt's record, the last one included, and a fault that doesn't kill
// counts only as landed.
func TestThreeFaults(t *testing.T) {
	var r Records
	commit(&r, 1, 1)
	commit(&r, 1, 2)
	r.LandedKill(at(3))
	commit(&r, 2, 4)
	r.Landed()
	r.LandedKill(at(5))
	fails(t, r.Strict(3, 2), "no record of a later attempt after the last kill")
	commit(&r, 3, 6)
	fails(t, r.Strict(3, 2))
	fails(t, r.Strict(4, 3), "3 of 4 faults landed")
}
