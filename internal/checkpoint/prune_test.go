package checkpoint_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/openetl-io/openetl/internal/checkpoint"
)

func records(t *testing.T, s checkpoint.Store) []string {
	t.Helper()
	keys, err := s.List(t.Context(), "org/pipe/_checkpoints/")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, k := range keys {
		if name := strings.TrimPrefix(k, "org/pipe/_checkpoints/"); name != "HEAD" {
			var e, g uint64
			if _, err := fmt.Sscanf(name, "%d-g%d.json", &e, &g); err != nil {
				t.Fatalf("record key %q: %v", k, err)
			}
			out = append(out, fmt.Sprintf("%d-g%d", e, g))
		}
	}
	return out
}

func quarantine(t *testing.T, s checkpoint.Store, g, n uint64) string {
	t.Helper()
	key := fmt.Sprintf("org/pipe/%s/%d/%d/users.jsonl", checkpoint.QuarantineDir, g, n)
	if _, err := s.Create(t.Context(), key, []byte("{}\n")); err != nil {
		t.Fatal(err)
	}
	return key
}

func exists(t *testing.T, s checkpoint.Store, key string) bool {
	t.Helper()
	keys, err := s.List(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	return slices.Contains(keys, key)
}

func prune(t *testing.T, s *checkpoint.Session, keep int) {
	t.Helper()
	restored, err := s.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prune(t.Context(), restored, keep); err != nil {
		t.Fatal(err)
	}
}

func TestPruneKeepsTheLastRecordsOfTheChain(t *testing.T) {
	store := checkpoint.NewMemStore()
	l := newLog(store)
	a := claim(t, l, "a")
	for n := uint64(1); n <= 6; n++ {
		commit(t, a, n)
	}
	q2 := quarantine(t, store, 1, 2)

	b := claim(t, l, "b")
	prune(t, b, 3)
	if got, want := records(t, store), []string{"4-g1", "5-g1", "6-g1"}; !slices.Equal(got, want) {
		t.Fatalf("records = %v, want %v", got, want)
	}
	if !exists(t, store, q2) {
		t.Error("pruning a committed record deleted its quarantined records")
	}
	// The next attempt still restores, and can fall back a record.
	if _, err := claim(t, l, "c").Load(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestPruneDeletesWhatNeverCommitted(t *testing.T) {
	ctx := t.Context()
	store := checkpoint.NewMemStore()
	l := newLog(store)
	a := claim(t, l, "a")
	commit(t, a, 1)
	commit(t, a, 2)
	kept := quarantine(t, store, 1, 2)
	// Generation 1 quarantined epoch 3's records, then lost its HEAD swap.
	orphan := quarantine(t, store, 1, 3)
	if _, err := store.Create(ctx, "org/pipe/_checkpoints/00000000000000000003-g00000000000000000001.json", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	// Generation 2 staged epoch 3 and died before its record.
	orphan2 := quarantine(t, store, 2, 3)
	claim(t, l, "b")

	c := claim(t, l, "c")
	prune(t, c, 10)
	if got, want := records(t, store), []string{"1-g1", "2-g1"}; !slices.Equal(got, want) {
		t.Fatalf("records = %v, want %v", got, want)
	}
	if exists(t, store, orphan) || exists(t, store, orphan2) {
		t.Error("quarantined records of epochs that never committed are left")
	}
	if !exists(t, store, kept) {
		t.Error("quarantined records of a committed epoch were deleted")
	}
}

func TestPruneLeavesNewerGenerationsAlone(t *testing.T) {
	ctx := t.Context()
	store := checkpoint.NewMemStore()
	l := newLog(store)
	a := claim(t, l, "a")
	commit(t, a, 1)
	zombie := claim(t, l, "zombie")
	restored, err := zombie.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// A newer attempt takes over and commits before the zombie prunes.
	b := claim(t, l, "b")
	commit(t, b, 2)
	q := quarantine(t, store, 3, 2)
	if _, err := zombie.Prune(ctx, restored, 2); err != nil {
		t.Fatal(err)
	}
	if got, want := records(t, store), []string{"1-g1", "2-g3"}; !slices.Equal(got, want) {
		t.Fatalf("records = %v, want %v", got, want)
	}
	if !exists(t, store, q) {
		t.Error("a zombie deleted a newer generation's quarantined records")
	}
}

func TestPruneAfterRewind(t *testing.T) {
	ctx := t.Context()
	store := checkpoint.NewMemStore()
	l := newLog(store)
	a := claim(t, l, "a")
	for n := uint64(1); n <= 4; n++ {
		commit(t, a, n)
	}
	b := claim(t, l, "b")
	head, err := b.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The sink lost epoch 4, so b falls back to 3.
	if err := b.Rewind(ctx, head.Previous); err != nil {
		t.Fatal(err)
	}
	restored, err := l.Record(ctx, *head.Previous)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Prune(ctx, restored, 2); err != nil {
		t.Fatal(err)
	}
	if got, want := records(t, store), []string{"2-g1", "3-g1"}; !slices.Equal(got, want) {
		t.Fatalf("records = %v, want %v", got, want)
	}
	// What the next attempt reads is there.
	c := claim(t, l, "c")
	rec, err := c.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Record(ctx, *rec.Previous); err != nil {
		t.Fatal(err)
	}
}

func TestPruneKeepsAtLeastTwo(t *testing.T) {
	s := claim(t, newLog(checkpoint.NewMemStore()), "a")
	if _, err := s.Prune(context.Background(), nil, 1); err == nil {
		t.Fatal("Prune kept 1 record")
	}
}

func held(t *testing.T, s checkpoint.Store, g, n uint64, stream string) string {
	t.Helper()
	key := fmt.Sprintf("org/pipe/%s/%d/%d/%s/part-00000.parquet", checkpoint.HeldDir, g, n, stream)
	if _, err := s.Create(t.Context(), key, []byte("PAR1")); err != nil {
		t.Fatal(err)
	}
	return key
}

func TestHeldFilesAreFoundByEpochsAndPrunedOutsideThem(t *testing.T) {
	ctx := t.Context()
	store := checkpoint.NewMemStore()
	l := newLog(store)
	a := claim(t, l, "a")
	inRange := held(t, store, 1, 2, "users")
	held(t, store, 1, 3, "users")
	other := held(t, store, 1, 2, "a%2Fb")
	orphan := held(t, store, 1, 5, "users")
	rec := &checkpoint.Record{Epoch: 3, Streams: map[string]checkpoint.StreamState{
		"users": {Holds: &checkpoint.Holds{Epochs: []checkpoint.HeldEpochs{{Generation: 1, From: 2, To: 3}}}},
		"a/b":   {},
	}}
	if err := a.Commit(ctx, rec); err != nil {
		t.Fatal(err)
	}
	files, err := l.HeldFiles(ctx, "users", rec.Streams["users"].Holds.Epochs)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"1/2/users/part-00000.parquet", "1/3/users/part-00000.parquet"}; !slices.Equal(files, want) {
		t.Fatalf("files = %v, want %v", files, want)
	}

	b := claim(t, l, "b")
	ownGen := held(t, store, b.Generation(), 9, "users")
	if _, err := b.PruneHeld(ctx, rec); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]bool{inRange: true, other: false, orphan: false, ownGen: true} {
		if exists(t, store, key) != want {
			t.Errorf("%s kept = %v, want %v", key, !want, want)
		}
	}
}
