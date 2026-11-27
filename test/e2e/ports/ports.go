// Package ports hands the chaos suite's runners TCP ports from a fixed
// range, each to one holder at a time, so two runners never get the same
// one before either listens on it.
package ports

import "sync"

// Range is the ports from First to First+N-1. A Range must not be copied
// once used.
type Range struct {
	First, N int

	mu sync.Mutex
	// The offset of the port to try first.
	next  int
	taken map[int]bool
}

// Take returns a port of the range that no holder has and that free reports
// free, and marks it taken; false if there's none. It tries the ports in
// turn from after the last one it tried, so a port given back isn't the next
// one given while others are free.
func (r *Range) Take(free func(port int) bool) (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.taken == nil {
		r.taken = map[int]bool{}
	}
	for range r.N {
		port := r.First + r.next
		r.next = (r.next + 1) % r.N
		if r.taken[port] || !free(port) {
			continue
		}
		r.taken[port] = true
		return port, true
	}
	return 0, false
}

// Release gives back a port that Take returned.
func (r *Range) Release(port int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.taken, port)
}
