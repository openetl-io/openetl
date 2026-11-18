package runner

import (
	"fmt"
	"testing"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// Committables that hold rows of one split settle on one worker, and so do
// those that hold rows of any stream read with CDC, whose changes the
// source reads from one log: two workers redoing them at once would each
// open it.
func TestSettleGroups(t *testing.T) {
	j := &Job{Streams: []Stream{{Name: "a", Read: source.CDC}, {Name: "b", Read: source.CDC}, {Name: "s", Read: source.Incremental}}}
	covers := func(splits ...string) checkpoint.Coverage {
		var c checkpoint.Coverage
		for _, sp := range splits {
			c.Splits = append(c.Splits, checkpoint.CoveredSplit{Stream: sp[:1], ID: sp})
		}
		return c
	}
	rec := &checkpoint.Record{Coverage: []checkpoint.Coverage{covers("a"), covers("s0"), covers("b"), covers("s1"), covers("s1", "s2"), covers()}}
	rec.Committables = make([][]byte, len(rec.Coverage))
	got := fmt.Sprint(settleGroups(rec, 3, j.readsChanges))
	if want := "[[0 2 5] [1] [3 4]]"; got != want {
		t.Errorf("groups %s, want %s", got, want)
	}
	if got, want := fmt.Sprint(settleGroups(rec, 3, func(string) bool { return false })), "[[0 3 4] [1 5] [2]]"; got != want {
		t.Errorf("without CDC, groups %s, want %s", got, want)
	}
}
