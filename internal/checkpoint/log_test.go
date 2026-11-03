package checkpoint_test

import (
	"context"
	"errors"
	"testing"

	"github.com/unsuitable001/openetl/internal/checkpoint"
)

func newLog(s checkpoint.Store) *checkpoint.Log { return checkpoint.NewLog(s, "org", "pipe") }

func claim(t *testing.T, l *checkpoint.Log, owner string) *checkpoint.Session {
	t.Helper()
	s, err := l.Claim(t.Context(), owner)
	if err != nil {
		t.Fatalf("Claim(%s): %v", owner, err)
	}
	return s
}

func commit(t *testing.T, s *checkpoint.Session, epoch uint64) *checkpoint.Record {
	t.Helper()
	rec := &checkpoint.Record{Epoch: epoch, Streams: map[string]checkpoint.StreamState{"s": {State: []byte{byte(epoch)}}}}
	if err := s.Commit(t.Context(), rec); err != nil {
		t.Fatalf("Commit(%d): %v", epoch, err)
	}
	return rec
}

func TestClaimRaisesGeneration(t *testing.T) {
	l := newLog(checkpoint.NewMemStore())
	for want := uint64(1); want <= 3; want++ {
		if g := claim(t, l, "a").Generation(); g != want {
			t.Fatalf("generation = %d, want %d", g, want)
		}
	}
}

func TestCommitLoadAndPreviousChain(t *testing.T) {
	l := newLog(checkpoint.NewMemStore())
	s := claim(t, l, "a")
	commit(t, s, 1)
	commit(t, s, 2)

	s2 := claim(t, l, "b")
	rec, err := s2.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if rec.Epoch != 2 || rec.Generation != 1 || string(rec.Streams["s"].State) != "\x02" {
		t.Fatalf("Load = epoch %d gen %d state %q, want epoch 2 gen 1", rec.Epoch, rec.Generation, rec.Streams["s"].State)
	}
	if rec.Previous == nil || *rec.Previous != (checkpoint.RecordRef{Epoch: 1, Generation: 1}) {
		t.Fatalf("Previous = %+v, want epoch 1 gen 1", rec.Previous)
	}
	// Epoch numbering continues under the new generation.
	r3 := commit(t, s2, 3)
	if r3.Generation != 2 || *r3.Previous != rec.Ref() {
		t.Fatalf("record 3 = gen %d previous %+v", r3.Generation, r3.Previous)
	}
}

func TestReplacedSessionIsFenced(t *testing.T) {
	l := newLog(checkpoint.NewMemStore())
	old := claim(t, l, "old")
	commit(t, old, 1)
	replacement := claim(t, l, "new")

	err := old.Commit(t.Context(), &checkpoint.Record{Epoch: 2})
	if !errors.Is(err, checkpoint.ErrFenced) {
		t.Fatalf("stale Commit: err = %v, want ErrFenced", err)
	}
	if err := old.Rewind(t.Context(), nil); !errors.Is(err, checkpoint.ErrFenced) {
		t.Fatalf("stale Rewind: err = %v, want ErrFenced", err)
	}
	if err := old.Check(t.Context()); !errors.Is(err, checkpoint.ErrFenced) {
		t.Fatalf("stale Check: err = %v, want ErrFenced", err)
	}
	if err := replacement.Check(t.Context()); err != nil {
		t.Fatalf("Check: %v", err)
	}
	// The replacement still sees epoch 1, and commits on top of it.
	commit(t, replacement, 2)
	rec, err := claim(t, l, "third").Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if rec.Epoch != 2 || rec.Generation != 2 {
		t.Fatalf("HEAD record = %d-g%d, want 2-g2", rec.Epoch, rec.Generation)
	}
}

func TestRewind(t *testing.T) {
	l := newLog(checkpoint.NewMemStore())
	s := claim(t, l, "a")
	commit(t, s, 1)
	r2 := commit(t, s, 2)
	if err := s.Rewind(t.Context(), r2.Previous); err != nil {
		t.Fatal(err)
	}
	rec, err := claim(t, l, "b").Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if rec.Epoch != 1 {
		t.Fatalf("after Rewind, HEAD record = epoch %d, want 1", rec.Epoch)
	}
}

// flakyStore fails chosen operations, either before applying them or after.
type flakyStore struct {
	checkpoint.Store
	failNext   string // operation to fail: "create", "swap"
	afterApply bool   // apply the write, then report an error
}

var errTimeout = errors.New("timeout")

func (f *flakyStore) Create(ctx context.Context, key string, data []byte) (checkpoint.Version, error) {
	if f.failNext == "create" {
		f.failNext = ""
		if f.afterApply {
			_, _ = f.Store.Create(ctx, key, data)
		}
		return "", errTimeout
	}
	return f.Store.Create(ctx, key, data)
}

func (f *flakyStore) Swap(ctx context.Context, key string, data []byte, prev checkpoint.Version) (checkpoint.Version, error) {
	if f.failNext == "swap" {
		f.failNext = ""
		if f.afterApply {
			_, _ = f.Store.Swap(ctx, key, data, prev)
		}
		return "", errTimeout
	}
	return f.Store.Swap(ctx, key, data, prev)
}

func TestUnknownOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		op         string
		afterApply bool
		wantErr    bool // Commit reports an error; a retry must then succeed
	}{
		{"record write landed", "create", true, false},
		{"record write lost", "create", false, true},
		{"HEAD swap landed", "swap", true, false},
		{"HEAD swap lost", "swap", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := &flakyStore{Store: checkpoint.NewMemStore()}
			l := newLog(fs)
			s := claim(t, l, "a")
			commit(t, s, 1)

			fs.failNext, fs.afterApply = tc.op, tc.afterApply
			rec := &checkpoint.Record{Epoch: 2}
			err := s.Commit(t.Context(), rec)
			if tc.wantErr != (err != nil) {
				t.Fatalf("Commit: err = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil {
				if errors.Is(err, checkpoint.ErrFenced) {
					t.Fatalf("Commit: unexpected ErrFenced: %v", err)
				}
				if err := s.Commit(t.Context(), rec); err != nil {
					t.Fatalf("retried Commit: %v", err)
				}
			}
			commit(t, s, 3) // the session's view of HEAD is still right
			got, err := claim(t, l, "b").Load(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if got.Epoch != 3 || got.Previous.Epoch != 2 {
				t.Fatalf("HEAD record = epoch %d previous %d, want 3 and 2", got.Epoch, got.Previous.Epoch)
			}
		})
	}
}

func TestClaimWithUnknownOutcome(t *testing.T) {
	for _, landed := range []bool{true, false} {
		fs := &flakyStore{Store: checkpoint.NewMemStore()}
		l := newLog(fs)
		claim(t, l, "first")
		fs.failNext, fs.afterApply = "swap", landed
		s := claim(t, l, "second")
		// Whether the first write landed or was retried, the claim ends at 2.
		if s.Generation() != 2 {
			t.Fatalf("landed=%v: generation = %d, want 2", landed, s.Generation())
		}
		commit(t, s, 1)
	}
}

func TestProbeRejectsStoresIgnoringConditions(t *testing.T) {
	err := checkpoint.Probe(t.Context(), ignoresConditions{checkpoint.NewMemStore()}, "p/")
	if err == nil {
		t.Fatal("Probe passed a store that ignores conditional writes")
	}
}

// ignoresConditions turns every conditional write into a plain overwrite, as
// some S3-compatible stores do.
type ignoresConditions struct{ *checkpoint.MemStore }

func (s ignoresConditions) Create(ctx context.Context, key string, data []byte) (checkpoint.Version, error) {
	_ = s.Delete(ctx, key)
	return s.MemStore.Create(ctx, key, data)
}
