package runner

import (
	"slices"
	"testing"

	"github.com/openetl-io/openetl/internal/checkpoint"
)

// An attempt drops the state of operators its version changed or dropped,
// and leaves the state of any other operator its job doesn't keep, which
// the engine refuses.
func TestWithoutDropped(t *testing.T) {
	rec := &checkpoint.Record{Epoch: 4, Snapshots: []checkpoint.Snapshot{
		{Operator: "window-kept", URL: "mem://1"},
		{Operator: "window-old", URL: "mem://2"},
		{Operator: "window-old", URL: "mem://3"},
		{Operator: "window-drifted", URL: "mem://4"},
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
