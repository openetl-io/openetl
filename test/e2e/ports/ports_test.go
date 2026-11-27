package ports

import (
	"slices"
	"sync"
	"testing"
)

func always(int) bool { return true }

// With one port free, the first holder gets it and the second gets none, not
// the same port again, until the first gives it back.
func TestAPortGoesToOneHolderAtATime(t *testing.T) {
	r := &Range{First: 21000, N: 2000}
	onlyOne := func(port int) bool { return port == 21001 }
	if port, ok := r.Take(onlyOne); !ok || port != 21001 {
		t.Fatalf("first take: %d, %v; want 21001", port, ok)
	}
	if port, ok := r.Take(onlyOne); ok {
		t.Fatalf("second take: %d; want none, as 21001 is taken", port)
	}
	r.Release(21001)
	if port, ok := r.Take(onlyOne); !ok || port != 21001 {
		t.Fatalf("take after release: %d, %v; want 21001", port, ok)
	}
}

// Ports go in turn, skipping those free reports busy, and a port given back
// comes round again only after the others.
func TestPortsGoInTurn(t *testing.T) {
	r := &Range{First: 100, N: 4}
	busy := func(port int) bool { return port != 101 }
	var got []int
	for range 3 {
		port, ok := r.Take(busy)
		if !ok {
			t.Fatalf("take %d: none", len(got)+1)
		}
		got = append(got, port)
	}
	if want := []int{100, 102, 103}; !slices.Equal(got, want) {
		t.Fatalf("took %v, want %v", got, want)
	}
	if port, ok := r.Take(busy); ok {
		t.Fatalf("took %d with every free port taken", port)
	}
	r.Release(103)
	if port, _ := r.Take(always); port != 101 {
		t.Fatalf("took %d after 103 was given back, want 101, the next in turn", port)
	}
	if port, _ := r.Take(always); port != 103 {
		t.Fatalf("took %d, want 103", port)
	}
}

func TestAnEmptyRangeHasNoPorts(t *testing.T) {
	var r Range
	if port, ok := r.Take(always); ok {
		t.Fatalf("took %d from an empty range", port)
	}
}

// Holders taking at once each get a port of their own.
func TestHoldersTakingAtOnceGetDifferentPorts(t *testing.T) {
	r := &Range{First: 30000, N: 64}
	var (
		mu   sync.Mutex
		seen = map[int]bool{}
		wg   sync.WaitGroup
	)
	for range 64 {
		wg.Go(func() {
			port, ok := r.Take(always)
			mu.Lock()
			defer mu.Unlock()
			if !ok || seen[port] {
				t.Errorf("take: %d, %v; want a port no other holder has", port, ok)
			}
			seen[port] = true
		})
	}
	wg.Wait()
	if port, ok := r.Take(always); ok {
		t.Fatalf("took %d with all 64 taken", port)
	}
}
