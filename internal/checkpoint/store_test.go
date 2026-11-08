package checkpoint_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/checkpoint/checkpointtest"
)

func TestMemStore(t *testing.T) {
	checkpointtest.TestStore(t, func(*testing.T) checkpoint.Store { return checkpoint.NewMemStore() })
}

func TestFSStore(t *testing.T) {
	checkpointtest.TestStore(t, func(t *testing.T) checkpoint.Store {
		s, err := checkpoint.NewFSStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func TestFSStoreRejectsEscapingKeys(t *testing.T) {
	s, err := checkpoint.NewFSStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"", "/abs", "a/../../etc"} {
		if _, err := s.Create(t.Context(), key, []byte("x")); err == nil {
			t.Errorf("Create(%q) succeeded, want an error", key)
		}
	}
}

// The first Swaps of a key race to create its lock file.
func TestFSStoreConcurrentFirstSwaps(t *testing.T) {
	s, err := checkpoint.NewFSStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	for i := range 50 {
		key := fmt.Sprintf("race/%d", i)
		v, err := s.Create(ctx, key, []byte("0"))
		if err != nil {
			t.Fatal(err)
		}
		const workers = 8
		start := make(chan struct{})
		errs := make(chan error, workers)
		var wg sync.WaitGroup
		for range workers {
			wg.Go(func() {
				<-start
				if _, err := s.Swap(ctx, key, []byte("1"), v); err != nil && !errors.Is(err, checkpoint.ErrPrecondition) {
					errs <- err
				}
			})
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
	}
}
