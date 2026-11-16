package ledger

import (
	"slices"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func at(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }

// commit is a record of generation gen received at second s, written and
// committed.
func commit(r *Records, gen int64, s int) {
	r.Received(gen)
	r.Wrote(gen, at(s), at(s))
	r.Head()
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
	r.Killed(at(3))
	r.Landed()
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
	r.Killed(at(2))
	r.Landed()
	commit(&r, 2, 4)
	fails(t, r.Strict(1, 1), "landed with 1 records committed")
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
	r.Killed(at(3))
	r.Landed()
	r.Wrote(3, at(2), at(4))
	r.Wrote(3, at(5), at(5))
	r.Head()
	if len(r.Resumes()) != 0 {
		t.Fatalf("the killed attempt's records resumed it: %v", r.Resumes())
	}
	fails(t, r.Strict(1, 1), "no record of a later attempt after the last kill")

	commit(&r, 4, 6)
	r.Killed(at(7))
	r.Landed()
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
	if !r.Ready() {
		t.Fatal("not ready once it's written")
	}
}
