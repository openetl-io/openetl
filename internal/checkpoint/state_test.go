package checkpoint_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/openetl-io/openetl/internal/checkpoint"
)

// run writes a state object at rel below the pipeline's state, as an
// engine's lane writes a run.
func run(t *testing.T, s checkpoint.Store, rel string) string {
	t.Helper()
	key := "org/pipe/" + checkpoint.StateDir + "/" + rel
	if _, err := s.Create(t.Context(), key, []byte("run")); err != nil {
		t.Fatal(err)
	}
	return rel
}

// runs are the state objects left, below the pipeline's state.
func runs(t *testing.T, s checkpoint.Store) []string {
	t.Helper()
	prefix := "org/pipe/" + checkpoint.StateDir + "/"
	keys, err := s.List(t.Context(), prefix)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = strings.TrimPrefix(k, prefix)
	}
	return out
}

// commitNaming commits epoch n, whose record names named, by operator: the
// part of a name before its first slash.
func commitNaming(t *testing.T, s *checkpoint.Session, n uint64, named ...string) *checkpoint.Record {
	t.Helper()
	by := map[string][]string{}
	for _, r := range named {
		op, _, _ := strings.Cut(r, "/")
		by[op] = append(by[op], r)
	}
	rec := &checkpoint.Record{Epoch: n, Streams: map[string]checkpoint.StreamState{"s": {State: []byte{byte(n)}}}}
	for op, rs := range by {
		rec.Snapshots = append(rec.Snapshots, checkpoint.Snapshot{Operator: op, KeyGroups: checkpoint.KeyGroupRange{Last: 255}, Layout: 1, Data: []byte("R"), Runs: rs})
	}
	if err := s.Commit(t.Context(), rec); err != nil {
		t.Fatalf("Commit(%d): %v", n, err)
	}
	return rec
}

func lane(op string, g, n uint64) string { return fmt.Sprintf("%s/g%d/e%d/kg0-255.run", op, g, n) }

// Retention keeps every state object a kept record names, for as many
// generations as records go on naming it, and deletes the rest: the runs
// of an epoch that never committed, a generation's own runs that no record
// names, and those that only records it no longer keeps name, as a reset's
// operator's. A generation's own runs of epochs after its newest record,
// and a newer generation's, stay.
func TestPruneKeepsTheStateKeptRecordsName(t *testing.T) {
	store := checkpoint.NewMemStore()
	l := newLog(store)

	a := claim(t, l, "a")
	var named []string
	for n := uint64(1); n <= 4; n++ {
		named = append(named, run(t, store, lane("w", 1, n)))
		commitNaming(t, a, n, named...)
	}
	named = append(named, run(t, store, lane("old", 1, 4)))
	commitNaming(t, a, 5, named...)
	// Epoch 6's runs, whose record never landed.
	failed := run(t, store, lane("w", 1, 6))

	b := claim(t, l, "b")
	head, err := b.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// The attempt took a reset of old's stream: its copy of the record
	// names none of old's state, which the log's record still does.
	taken := *head
	taken.Snapshots = slices.DeleteFunc(slices.Clone(head.Snapshots), func(s checkpoint.Snapshot) bool { return s.Operator == "old" })
	if _, err := b.Prune(t.Context(), &taken, 2); err != nil {
		t.Fatal(err)
	}
	want := slices.Sorted(slices.Values(named))
	if got := runs(t, store); !slices.Equal(got, want) {
		t.Fatalf("after the restore, state %v, want %v", got, want)
	}

	// Generation 2 names the runs it restored and its own, and has
	// written epoch 9's before its record.
	own := slices.DeleteFunc(slices.Clone(named), func(r string) bool { return strings.HasPrefix(r, "old/") })
	unnamed := run(t, store, "w/g2/e6/kg0-127.run")
	var rec *checkpoint.Record
	for n := uint64(6); n <= 8; n++ {
		own = append(own, run(t, store, lane("w", 2, n)))
		rec = commitNaming(t, b, n, own...)
	}
	pending := run(t, store, lane("w", 2, 9))
	newer := run(t, store, lane("w", 3, 1))
	odd := run(t, store, "w/notes.txt")
	if _, err := b.Prune(t.Context(), rec, 2); err != nil {
		t.Fatal(err)
	}
	want = slices.Sorted(slices.Values(append(own, pending, newer, odd)))
	got := runs(t, store)
	if !slices.Equal(got, want) {
		t.Fatalf("state %v, want %v", got, want)
	}
	for _, gone := range []string{failed, unnamed, lane("old", 1, 4)} {
		if slices.Contains(got, gone) {
			t.Errorf("%s is left", gone)
		}
	}
}

// A record that won't read names nothing, so a Prune from it fails and
// deletes no state: the record before it may still be restored.
func TestPruneFromARecordThatWontReadKeepsState(t *testing.T) {
	store := checkpoint.NewMemStore()
	l := newLog(store)
	a := claim(t, l, "a")
	named := []string{run(t, store, lane("w", 1, 1))}
	commitNaming(t, a, 1, named...)
	named = append(named, run(t, store, lane("w", 1, 2)))
	head := commitNaming(t, a, 2, named...)
	b := claim(t, l, "b")
	if err := store.Delete(t.Context(), fmt.Sprintf("org/pipe/_checkpoints/%020d-g%020d.json", head.Epoch, head.Generation)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Prune(t.Context(), head, 2); !errors.Is(err, checkpoint.ErrUnreadable) {
		t.Fatalf("Prune: %v, want ErrUnreadable", err)
	}
	if got := runs(t, store); !slices.Equal(got, named) {
		t.Errorf("state %v, want %v", got, named)
	}
}

// With nothing restored, no record names any state, and every earlier
// generation's is an orphan.
func TestPruneWithNothingRestoredDeletesEarlierState(t *testing.T) {
	store := checkpoint.NewMemStore()
	l := newLog(store)
	claim(t, l, "a")
	run(t, store, lane("w", 1, 1))
	b := claim(t, l, "b")
	mine := run(t, store, lane("w", 2, 1))
	if _, err := b.Prune(t.Context(), nil, 2); err != nil {
		t.Fatal(err)
	}
	if got := runs(t, store); !slices.Equal(got, []string{mine}) {
		t.Errorf("state %v, want %v", got, []string{mine})
	}
}

// As a lane's sections die, its records stop naming their runs: a run
// stays while a kept record names any of it, as one lane after a rescale
// does when the other's part of it is dead, and goes once none does.
func TestPruneKeepsARunUntilNoKeptRecordNamesAnyOfIt(t *testing.T) {
	store := checkpoint.NewMemStore()
	l := newLog(store)
	a := claim(t, l, "a")
	r1, r2, r3 := run(t, store, lane("w", 1, 1)), run(t, store, lane("w", 1, 2)), run(t, store, lane("w", 1, 3))
	commitNaming(t, a, 1, r1)
	commitNaming(t, a, 2, r1, r2)
	rec := commitNaming(t, a, 3, r2, r3)
	b := claim(t, l, "b")
	if _, err := b.Prune(t.Context(), rec, 2); err != nil {
		t.Fatal(err)
	}
	if got, want := runs(t, store), []string{r1, r2, r3}; !slices.Equal(got, want) {
		t.Fatalf("after epoch 3, state %v, want %v: epoch 2 names %s", got, want, r1)
	}
	// Two lanes restored from epoch 3: the low one's part of r3 is all
	// that's left of r2 and r3.
	r4 := run(t, store, "w/g2/e4/kg128-255.run")
	four := &checkpoint.Record{Epoch: 4, Streams: map[string]checkpoint.StreamState{"s": {State: []byte{4}}}, Snapshots: []checkpoint.Snapshot{
		{Operator: "w", KeyGroups: checkpoint.KeyGroupRange{Last: 127}, Layout: 1, Data: []byte("R"), Runs: []string{r3}},
		{Operator: "w", KeyGroups: checkpoint.KeyGroupRange{First: 128, Last: 255}, Layout: 1, Data: []byte("R"), Runs: []string{r4}},
	}}
	if err := b.Commit(t.Context(), four); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Prune(t.Context(), four, 2); err != nil {
		t.Fatal(err)
	}
	if got, want := runs(t, store), []string{r2, r3, r4}; !slices.Equal(got, want) {
		t.Fatalf("after epoch 4, state %v, want %v", got, want)
	}
	five := commitNaming(t, b, 5, r4)
	if _, err := b.Prune(t.Context(), five, 2); err != nil {
		t.Fatal(err)
	}
	if got, want := runs(t, store), []string{r3, r4}; !slices.Equal(got, want) {
		t.Fatalf("after epoch 5, state %v, want %v", got, want)
	}
}

func merged(op string, g, n uint64) string {
	return fmt.Sprintf("%s/g%d/e%d/kg0-255.merge.run", op, g, n)
}

// A merge's output is named first by the record of the barrier it's named
// for, in place of its inputs: retention keeps the inputs while a kept
// record names them, and the output once one does. An output whose record
// never landed is an orphan, kept while it may yet be named: of its own
// generation and a later epoch than the newest record's.
func TestPruneAgreesWithMerges(t *testing.T) {
	store := checkpoint.NewMemStore()
	l := newLog(store)
	a := claim(t, l, "a")
	r1, r2, r3 := run(t, store, lane("w", 1, 1)), run(t, store, lane("w", 1, 2)), run(t, store, lane("w", 1, 3))
	commitNaming(t, a, 1, r1)
	commitNaming(t, a, 2, r1, r2)
	commitNaming(t, a, 3, r1, r2, r3)
	// Epoch 4's barrier names the merge of r1 to r3 in their place.
	m4, r4 := run(t, store, merged("w", 1, 4)), run(t, store, lane("w", 1, 4))
	rec := commitNaming(t, a, 4, m4, r4)
	// Epoch 5's merge, which the barrier of epoch 5 is about to name.
	m5 := run(t, store, merged("w", 1, 5))
	if _, err := a.Prune(t.Context(), rec, 2); err != nil {
		t.Fatal(err)
	}
	want := []string{r1, r2, r3, m4, m5, r4}
	if got := runs(t, store); !slices.Equal(got, slices.Sorted(slices.Values(want))) {
		t.Fatalf("after epoch 4, state %v, want %v", got, want)
	}
	// Epoch 5's record never lands, and its merge is an orphan.
	rec = commitNaming(t, a, 6, m4, r4)
	if _, err := a.Prune(t.Context(), rec, 2); err != nil {
		t.Fatal(err)
	}
	want = []string{m4, r4}
	if got := runs(t, store); !slices.Equal(got, slices.Sorted(slices.Values(want))) {
		t.Fatalf("after epoch 6, state %v, want %v", got, want)
	}
	// The next attempt merges what it restored; an older attempt's merge
	// that no record names goes.
	b := claim(t, l, "b")
	stale := run(t, store, merged("w", 1, 7))
	m7 := run(t, store, merged("w", 2, 7))
	rec = commitNaming(t, b, 7, m7)
	if _, err := b.Prune(t.Context(), rec, 2); err != nil {
		t.Fatal(err)
	}
	want = []string{m4, r4, m7}
	if got := runs(t, store); !slices.Equal(got, slices.Sorted(slices.Values(want))) {
		t.Fatalf("after epoch 7, state %v, want %v; %s is an orphan", got, want, stale)
	}
}

func early(op string, g, n uint64, part int) string {
	return fmt.Sprintf("%s/g%d/e%d/kg0-255.early%d.run", op, g, n, part)
}

// An early flush's run counts once the record of its epoch names it with
// the barrier's run. One whose epoch failed is an orphan: kept while it may
// yet be named, of the session's generation and an epoch after its newest
// record's, and deleted by a later attempt.
func TestPruneAgreesWithEarlyFlushes(t *testing.T) {
	store := checkpoint.NewMemStore()
	l := newLog(store)
	a := claim(t, l, "a")
	e1, r1 := run(t, store, early("w", 1, 1, 1)), run(t, store, lane("w", 1, 1))
	rec := commitNaming(t, a, 1, e1, r1)
	// Epoch 2 flushes early, and its record never lands.
	e2 := run(t, store, early("w", 1, 2, 1))
	if _, err := a.Prune(t.Context(), rec, 2); err != nil {
		t.Fatal(err)
	}
	if got, want := runs(t, store), slices.Sorted(slices.Values([]string{e1, r1, e2})); !slices.Equal(got, want) {
		t.Fatalf("after epoch 1, state %v, want %v", got, want)
	}
	b := claim(t, l, "b")
	rec = commitNaming(t, b, 2, e1, r1)
	if _, err := b.Prune(t.Context(), rec, 2); err != nil {
		t.Fatal(err)
	}
	if got, want := runs(t, store), slices.Sorted(slices.Values([]string{e1, r1})); !slices.Equal(got, want) {
		t.Fatalf("after the next attempt's epoch 2, state %v, want %v; %s is an orphan", got, want, e2)
	}
}
