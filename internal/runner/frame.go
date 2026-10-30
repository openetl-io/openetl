package runner

import (
	"context"
	"encoding/json"

	"github.com/apache/arrow-go/v18/arrow"
)

// FrameKind is the kind of a frame on a job's data path.
type FrameKind uint8

const (
	// FrameRows carries raw JSON rows for one stream, into the dataflow.
	FrameRows FrameKind = iota + 1
	// FrameBatch carries an Arrow batch for one stream, in either direction.
	FrameBatch
	// FrameBarrier marks the end of epoch Epoch on one stream. Everything
	// before it on that stream belongs to the epoch.
	FrameBarrier
)

// Frame is one unit on a job's data path. Control frames travel in band with
// the data, so a barrier stays ordered with its stream's records.
type Frame struct {
	Kind   FrameKind
	Stream string
	Rows   []json.RawMessage
	Batch  arrow.RecordBatch
	Epoch  uint64
}

// Dataflow carries frames from a job's sources to its sink. In production it
// is the engine process; in tests it can run in process.
//
// Run reads in until it is closed, and writes what it produces to out. It must
// keep each stream's frames in order, turn rows into Arrow batches, and
// forward every barrier. It closes out when it has drained in. The receiver
// of a batch on out releases it.
type Dataflow interface {
	Run(ctx context.Context, in <-chan Frame, out chan<- Frame) error
}
