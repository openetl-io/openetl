// Package checkpointtest is the conformance suite for checkpoint.Store.
package checkpointtest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/unsuitable001/openetl/internal/checkpoint"
)

// TestStore runs the suite against empty stores from newStore, under prefix.
func TestStore(t *testing.T, newStore func(t *testing.T) checkpoint.Store) {
	const prefix = "conformance/"
	ctx := context.Background()

	t.Run("CreateThenGet", func(t *testing.T) {
		s := newStore(t)
		v, err := s.Create(ctx, prefix+"a", []byte("one"))
		must(t, err)
		data, gv, err := s.Get(ctx, prefix+"a")
		must(t, err)
		if string(data) != "one" || gv != v {
			t.Fatalf("Get = %q, %q; want %q, %q", data, gv, "one", v)
		}
	})

	t.Run("GetMissing", func(t *testing.T) {
		s := newStore(t)
		if _, _, err := s.Get(ctx, prefix+"missing"); !errors.Is(err, checkpoint.ErrNotFound) {
			t.Fatalf("Get missing: err = %v, want ErrNotFound", err)
		}
	})

	t.Run("CreateExistingFails", func(t *testing.T) {
		s := newStore(t)
		_, err := s.Create(ctx, prefix+"a", []byte("one"))
		must(t, err)
		if _, err := s.Create(ctx, prefix+"a", []byte("two")); !errors.Is(err, checkpoint.ErrPrecondition) {
			t.Fatalf("second Create: err = %v, want ErrPrecondition", err)
		}
		data, _, err := s.Get(ctx, prefix+"a")
		must(t, err)
		if string(data) != "one" {
			t.Fatalf("content after failed Create = %q, want %q", data, "one")
		}
	})

	t.Run("SwapCurrentVersion", func(t *testing.T) {
		s := newStore(t)
		v1, err := s.Create(ctx, prefix+"a", []byte("one"))
		must(t, err)
		v2, err := s.Swap(ctx, prefix+"a", []byte("two"), v1)
		must(t, err)
		data, gv, err := s.Get(ctx, prefix+"a")
		must(t, err)
		if string(data) != "two" || gv != v2 {
			t.Fatalf("Get = %q, %q; want %q, %q", data, gv, "two", v2)
		}
	})

	t.Run("SwapStaleVersionFails", func(t *testing.T) {
		s := newStore(t)
		v1, err := s.Create(ctx, prefix+"a", []byte("one"))
		must(t, err)
		_, err = s.Swap(ctx, prefix+"a", []byte("two"), v1)
		must(t, err)
		if _, err := s.Swap(ctx, prefix+"a", []byte("three"), v1); !errors.Is(err, checkpoint.ErrPrecondition) {
			t.Fatalf("stale Swap: err = %v, want ErrPrecondition", err)
		}
	})

	t.Run("SwapMissingFails", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Swap(ctx, prefix+"missing", []byte("x"), "v"); !errors.Is(err, checkpoint.ErrPrecondition) {
			t.Fatalf("Swap missing: err = %v, want ErrPrecondition", err)
		}
	})

	t.Run("ListSortedByPrefix", func(t *testing.T) {
		s := newStore(t)
		for _, k := range []string{"b/2", "a/1", "b/1", "c"} {
			_, err := s.Create(ctx, prefix+k, []byte(k))
			must(t, err)
		}
		got, err := s.List(ctx, prefix+"b/")
		must(t, err)
		if want := []string{prefix + "b/1", prefix + "b/2"}; !slices.Equal(got, want) {
			t.Fatalf("List = %v, want %v", got, want)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		s := newStore(t)
		_, err := s.Create(ctx, prefix+"a", []byte("one"))
		must(t, err)
		must(t, s.Delete(ctx, prefix+"a"))
		must(t, s.Delete(ctx, prefix+"a")) // deleting a missing key is not an error
		if _, _, err := s.Get(ctx, prefix+"a"); !errors.Is(err, checkpoint.ErrNotFound) {
			t.Fatalf("Get after Delete: err = %v, want ErrNotFound", err)
		}
	})

	t.Run("Probe", func(t *testing.T) {
		must(t, checkpoint.Probe(ctx, newStore(t), prefix))
	})

	// Concurrent increments through Swap must never lose an update.
	t.Run("ConcurrentSwaps", func(t *testing.T) {
		s := newStore(t)
		key := prefix + "counter"
		_, err := s.Create(ctx, key, []byte("0"))
		must(t, err)
		const workers, perWorker = 8, 25
		var wg sync.WaitGroup
		errs := make(chan error, workers)
		for range workers {
			wg.Go(func() {
				for done := 0; done < perWorker; {
					data, v, err := s.Get(ctx, key)
					if err != nil {
						errs <- err
						return
					}
					n, _ := strconv.Atoi(string(data))
					_, err = s.Swap(ctx, key, []byte(strconv.Itoa(n+1)), v)
					switch {
					case err == nil:
						done++
					case !errors.Is(err, checkpoint.ErrPrecondition):
						errs <- err
						return
					}
				}
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		data, _, err := s.Get(ctx, key)
		must(t, err)
		if want := fmt.Sprint(workers * perWorker); string(data) != want {
			t.Fatalf("counter = %s, want %s: an update was lost", data, want)
		}
	})
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
