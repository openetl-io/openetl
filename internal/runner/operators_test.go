package runner

import (
	"log/slog"
	"slices"
	"testing"

	"github.com/openetl-io/openetl/internal/checkpoint"
)

// An attempt drops the state of operators its version changed or dropped,
// and leaves the state of any other operator its job doesn't keep, which
// the engine refuses.
func TestWithoutDropped(t *testing.T) {
	rec := &checkpoint.Record{Epoch: 4, Snapshots: []checkpoint.Snapshot{
		{Operator: "window-kept", Runs: []string{"r1"}},
		{Operator: "window-old", Runs: []string{"r2"}},
		{Operator: "window-old", Runs: []string{"r3"}},
		{Operator: "window-drifted", Runs: []string{"r4"}},
	}}
	j := &Job{SQL: &SQLTransform{Operators: []string{"window-kept", "window-new"}, Dropped: []string{"window-old", "window-gone"}}}
	got, dropped := j.withoutDropped(rec)
	var left []string
	for _, s := range got.Snapshots {
		left = append(left, s.Operator)
	}
	if !slices.Equal(dropped, []string{"window-old"}) || !slices.Equal(left, []string{"window-kept", "window-drifted"}) || got.Epoch != 4 {
		t.Errorf("dropped %v, left %v", dropped, left)
	}
	if len(rec.Snapshots) != 4 {
		t.Errorf("the record itself changed: %v", rec.Snapshots)
	}
	if got, dropped := (&Job{SQL: &SQLTransform{Operators: []string{"window-kept"}}}).withoutDropped(rec); got != rec || dropped != nil {
		t.Errorf("without dropped operators: %v, %v", got, dropped)
	}
	if got, _ := j.withoutDropped(nil); got != nil {
		t.Errorf("without a record: %v", got)
	}
}

// A redo starts without the state of the version's dropped operators, as
// the attempt that wrote the epoch did, whether or not that attempt took a
// reset.
func TestARedoStartsWithoutTheDroppedOperators(t *testing.T) {
	j := &Job{Streams: []Stream{{Name: "a"}}, SQL: &SQLTransform{Operators: []string{"window-new"}, Dropped: []string{"window-old"},
		Expected: []ExpectedOutput{{Name: "x", Input: "a", Operator: "window-new"}}}}
	before := &checkpoint.Record{Epoch: 4, Generation: 1, Reset: "0001", Streams: map[string]checkpoint.StreamState{"a": {State: []byte("4")}},
		Snapshots: []checkpoint.Snapshot{{Operator: "window-old", Runs: []string{"window-old/g1/e4/kg0-255.run"}}}}
	ref := before.Ref()
	for _, rec := range []*checkpoint.Record{
		{Epoch: 5, Generation: 2, Previous: &ref, Reset: "0001"},
		{Epoch: 5, Generation: 2, Previous: &ref, Reset: "0002", ResetStreams: []string{"a"}},
	} {
		if got := j.redoStart(rec, before); len(got.Snapshots) != 0 {
			t.Errorf("a redo of %+v restores %v", rec, got.Snapshots)
		}
	}
	if len(before.Snapshots) != 1 {
		t.Error("the record before was changed")
	}
}

// Both restore paths, on one process and on workers, take a newer reset
// and drop the version's dropped operators.
func TestRestoring(t *testing.T) {
	rec := &checkpoint.Record{Epoch: 4, Reset: "r1", Streams: map[string]checkpoint.StreamState{"orders": {State: []byte("pos")}, "users": {State: []byte("pos")}},
		Snapshots: []checkpoint.Snapshot{{Operator: "window-kept"}, {Operator: "window-old"}}}
	j := &Job{Streams: []Stream{{Name: "orders"}, {Name: "users"}}, Resets: []Reset{{ID: "r2", Streams: []string{"users"}}},
		SQL: &SQLTransform{Operators: []string{"window-kept"}, Dropped: []string{"window-old"},
			Expected: []ExpectedOutput{{Name: "minutes", Input: "orders", Operator: "window-kept"}}}}
	got, reset, streams := j.restoring(rec, slog.New(slog.DiscardHandler))
	var left []string
	for _, s := range got.Snapshots {
		left = append(left, s.Operator)
	}
	if reset != "r2" || !slices.Equal(streams, []string{"users"}) || got.Streams["users"].State != nil || string(got.Streams["orders"].State) != "pos" || !slices.Equal(left, []string{"window-kept"}) {
		t.Errorf("reset %q, streams %v, operators %v", reset, got.Streams, left)
	}
}
