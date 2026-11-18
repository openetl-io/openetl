package conformance

import (
	"slices"
	"testing"
)

// Only the splits that got new records need a resume point after them.
func TestUnresumedLooksAtSplitsWithNewRecords(t *testing.T) {
	old, added := newRow(map[string]string{"id": "1"}), newRow(map[string]string{"id": "2"})
	seen := countRows([]row{old})
	events := []event{
		{stream: "s", split: "a", batch: true, rows: []row{old}},
		{stream: "s", split: "b", batch: true, rows: []row{added}},
		{stream: "s", split: "b", resume: true},
	}
	if got := unresumed(events, seen); got != nil {
		t.Errorf("unresumed %v, want none: split a has no new records", got)
	}
	events = append(events, event{stream: "s", split: "b", batch: true, rows: []row{newRow(map[string]string{"id": "3"})}}, event{stream: "s", split: "a", resume: true})
	if got := unresumed(events, seen); !slices.Equal(got, []string{"b"}) {
		t.Errorf("unresumed %v, want [b]: a resume point of split a doesn't cover b", got)
	}
}
