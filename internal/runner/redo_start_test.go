package runner

import (
	"slices"
	"testing"

	"github.com/openetl-io/openetl/internal/checkpoint"
)

// A redo of an attempt's first epoch starts from the record before it as
// that attempt restored it: without the positions, watermarks and state of
// what the resets it took dropped. A later epoch starts from the record
// before it as it is.
func TestARedoStartsFromTheRecordBeforeAsItsAttemptRestoredIt(t *testing.T) {
	j := &Job{Streams: []Stream{{Name: "a"}, {Name: "b"}}, SQL: &SQLTransform{
		Operators: []string{"window-a", "window-b"},
		Expected:  []ExpectedOutput{{Name: "x", Input: "a", Operator: "window-a"}, {Name: "y", Input: "b", Operator: "window-b"}},
	}}
	mark := int64(70)
	before := &checkpoint.Record{Epoch: 4, Generation: 1, Reset: "0001",
		Streams: map[string]checkpoint.StreamState{"a": {State: []byte("4"), Watermark: &mark}, "b": {State: []byte("4"), Watermark: &mark}},
		Snapshots: []checkpoint.Snapshot{{Operator: "window-a", Runs: []string{"window-a/g1/e4/kg0-255.run"}},
			{Operator: "window-b", Runs: []string{"window-b/g1/e4/kg0-255.run"}}, {Operator: "window-old", Runs: []string{"window-old/g1/e2/kg0-255.run"}}}}
	ref := before.Ref()

	same := &checkpoint.Record{Epoch: 5, Generation: 1, Previous: &ref, Reset: "0001"}
	if got := j.redoStart(same, before); got != before {
		t.Errorf("a redo of a later epoch starts from %+v, want the record before as it is", got)
	}

	first := &checkpoint.Record{Epoch: 5, Generation: 2, Previous: &ref, Reset: "0002", ResetStreams: []string{"a"}}
	got := j.redoStart(first, before)
	var ops []string
	for _, s := range got.Snapshots {
		ops = append(ops, s.Operator)
	}
	if !slices.Equal(ops, []string{"window-b"}) {
		t.Errorf("a redo of the attempt's first epoch restores state of %v, want window-b's alone", ops)
	}
	if a := got.Streams["a"]; a.State != nil || a.Watermark != nil {
		t.Errorf("stream a starts at %+v, want its start", a)
	}
	if b := got.Streams["b"]; string(b.State) != "4" || b.Watermark == nil {
		t.Errorf("stream b starts at %+v, want where the record before has it", b)
	}
	if len(before.Snapshots) != 3 || before.Streams["a"].Watermark == nil {
		t.Error("the record before was changed")
	}
}
