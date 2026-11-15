package checkpoint_test

import (
	"fmt"
	"reflect"
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

func TestKeptSnapshotsLeaveOutOperatorsThatWroteNothing(t *testing.T) {
	snap := func(op string, first int, runs ...string) checkpoint.Snapshot {
		return checkpoint.Snapshot{Operator: op, KeyGroups: checkpoint.KeyGroupRange{First: first, Last: first + 127}, Runs: runs}
	}
	got := checkpoint.KeptSnapshots([]checkpoint.Snapshot{
		snap("a", 0, "a/g1/e1/kg0-127.run"), snap("a", 128), snap("b", 0), snap("b", 128),
	})
	want := []checkpoint.Snapshot{snap("a", 0, "a/g1/e1/kg0-127.run"), snap("a", 128)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("kept %+v, want %+v", got, want)
	}
}
