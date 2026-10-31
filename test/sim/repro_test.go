package sim

import (
	"testing"
	"testing/synctest"
)

// TestReproducible runs every seed twice and compares every decision.
func TestReproducible(t *testing.T) {
	for _, kind := range sinkKinds {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			diverged := 0
			for seed := uint64(1); seed <= uint64(*seedsFlag); seed++ {
				var a, b seedResult
				synctest.Test(t, func(*testing.T) { a = runSeed(kind, seed) })
				synctest.Test(t, func(*testing.T) { b = runSeed(kind, seed) })
				i := 0
				for i < len(a.steps) && i < len(b.steps) && a.steps[i] == b.steps[i] {
					i++
				}
				if i == len(a.steps) && i == len(b.steps) {
					continue
				}
				diverged++
				if diverged <= 3 {
					lo := max(0, i-3)
					t.Logf("%s seed %d diverges at decision %d\nrun 1: %q\nrun 2: %q", kind, seed, i, a.steps[lo:min(i+1, len(a.steps))], b.steps[lo:min(i+1, len(b.steps))])
				}
			}
			if diverged > 0 {
				t.Errorf("%s: %d of %d seeds ran differently the second time", kind, diverged, *seedsFlag)
			}
		})
	}
}
