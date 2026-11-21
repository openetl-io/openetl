package runner

import (
	"context"
	"testing"

	"github.com/openetl-io/openetl/internal/platform/errs"
)

// A checkpoint the sinks completed while a stream had no reader ends no
// epoch: its record would move that stream's resume points past rows the
// job never read.
func TestFlinkCutNeedsEveryStreamRead(t *testing.T) {
	cut := &flinkCut{c: 7, placed: true, ready: &Ready{}, barriers: 1}
	s := &flinkSegment{
		streams:     map[string]Stream{"a": {}, "b": {}},
		outputs:     map[string]Stream{"out": {}},
		readers:     map[string]bool{"a": true},
		parallelism: map[string]int32{"out": 1},
		cuts:        map[int64]*flinkCut{7: cut},
		order:       []int64{7},
	}
	err := s.progress(context.Background())
	if errs.CodeOf(err) != errs.ConfigInvalid {
		t.Fatalf("progress: %v, want a ConfigInvalid error", err)
	}
	if len(s.order) != 1 {
		t.Errorf("the checkpoint was decided")
	}
}
