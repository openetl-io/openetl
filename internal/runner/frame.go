package runner

import (
	"context"
	"encoding/json"

	"github.com/apache/arrow-go/v18/arrow"
)

type FrameKind uint8

const (
	// Into the dataflow only.
	FrameRows FrameKind = iota + 1
	FrameBatch
	// Ends Epoch on one stream.
	FrameBarrier
)

// Frame is one unit on a job's data path. Barriers travel in band, ordered
// with their stream's records.
type Frame struct {
	Kind   FrameKind
	Stream string
	Rows   []json.RawMessage
	Batch  arrow.RecordBatch
	Epoch  uint64
}

// Plan is what a dataflow needs to know about one attempt.
type Plan struct {
	Pipeline   string
	Owner      string
	Generation uint64
	// The first barrier's epoch.
	FirstEpoch uint64
	Streams    []Stream
}

// Dataflow carries frames from a job's sources to its sink: the engine, or
// an in-process stand-in. Run keeps each stream's frames in order, turns rows
// into batches, forwards every barrier, and closes out once in is drained.
// The receiver of a batch releases it.
type Dataflow interface {
	Run(ctx context.Context, plan Plan, in <-chan Frame, out chan<- Frame) error
}
