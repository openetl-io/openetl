package runner

import (
	"context"
	"encoding/json"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/unsuitable001/openetl/sdk/go/destination"
)

type FrameKind uint8

const (
	// Into the dataflow only.
	FrameRows FrameKind = iota + 1
	FrameBatch
	// Ends Epoch on one stream.
	FrameBarrier
	// Out of the dataflow only, with an engine sink: every stream's Epoch is
	// staged, as Staged describes.
	FrameStaged
)

// Frame is one unit on a job's data path. Barriers travel in band, ordered
// with their stream's records.
type Frame struct {
	Kind   FrameKind
	Stream string
	Rows   []json.RawMessage
	Batch  arrow.RecordBatch
	Epoch  uint64
	Staged destination.Staged
}

// Plan is what a dataflow needs to know about one attempt.
type Plan struct {
	Pipeline   string
	Owner      string
	Generation uint64
	// The first barrier's epoch.
	FirstEpoch uint64
	Streams    []Stream
	// Zero: out feeds the destination's Writer.
	Sink SinkPlan
}

// SinkPlan says where an engine sink writes.
type SinkPlan struct {
	// Parquet files under this file:// URL.
	ParquetURL string
	// The engine drops every batch; its epochs arrive staged with nothing.
	Discard bool
}

// EngineSink is a destination whose data the engine writes. Its Writer only
// commits and recovers.
type EngineSink interface {
	destination.Destination
	EngineSink() SinkPlan
}

// Dataflow carries frames from a job's sources to its sink: the engine, or
// an in-process stand-in. Run keeps each stream's frames in order, turns rows
// into batches, forwards every barrier, and closes out once in is drained.
// With an engine sink, out carries one FrameStaged per epoch, in order,
// instead. The receiver of a batch releases it.
type Dataflow interface {
	Run(ctx context.Context, plan Plan, in <-chan Frame, out chan<- Frame) error
}
