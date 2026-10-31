package runner

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/sdk/go/destination"
)

type FrameKind uint8

const (
	// Into the dataflow only.
	FrameRows FrameKind = iota + 1
	FrameBatch
	// Ends Epoch on one stream.
	FrameBarrier
	// Out of the dataflow only: its part of Epoch is durable, as Ready
	// describes.
	FrameReady
)

// Frame is one unit on a job's data path. Barriers travel in band, ordered
// with their stream's records.
type Frame struct {
	Kind   FrameKind
	Stream string
	Rows   []json.RawMessage
	Batch  arrow.RecordBatch
	Epoch  uint64
	Ready  *Ready
}

// Ready is the dataflow's part of an epoch, recorded in its checkpoint.
type Ready struct {
	// What an engine sink staged.
	Staged     destination.Staged
	Snapshots  []checkpoint.Snapshot
	Quarantine []checkpoint.QuarantineFile
	// Each stream's schema at its barrier, as an Arrow IPC stream holding
	// only the schema.
	Schemas map[string][]byte
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
	Sink   SinkPlan
	Naming Naming
	// Where the dataflow keeps records it can't take; empty: they fail it.
	QuarantineURL string
	// Each stream's read start, in microseconds since the Unix epoch.
	ReadStarts map[string]int64
	// From the restored checkpoint, as Ready reported them.
	Schemas   map[string][]byte
	Snapshots []checkpoint.Snapshot
}

// Naming is how the dataflow names tables and columns in the destination.
type Naming struct {
	KeepSource bool
	// In bytes; 0 for no limit.
	MaxLength int
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
// Once every stream has crossed barrier N, out also carries one FrameReady
// for N, in epoch order. With an engine sink, out carries only those. The
// receiver of a batch releases it.
type Dataflow interface {
	Run(ctx context.Context, plan Plan, in <-chan Frame, out chan<- Frame) error
}

// Passthrough is a dataflow without the engine. Convert turns rows into
// batches; with none, rows are an error. It reports no schemas.
type Passthrough struct {
	Convert func(stream string, rows []json.RawMessage) (arrow.RecordBatch, error)
}

func (p Passthrough) Run(ctx context.Context, plan Plan, in <-chan Frame, out chan<- Frame) error {
	defer close(out)
	crossed := make(map[uint64]int)
	send := func(f Frame) error {
		select {
		case out <- f:
			return nil
		case <-ctx.Done():
			if f.Batch != nil {
				f.Batch.Release()
			}
			return ctx.Err()
		}
	}
	for {
		var f Frame
		select {
		case fr, ok := <-in:
			if !ok {
				return nil
			}
			f = fr
		case <-ctx.Done():
			return ctx.Err()
		}
		if f.Kind == FrameRows {
			if p.Convert == nil {
				return fmt.Errorf("runner: rows for %s, and the dataflow can't convert them", f.Stream)
			}
			batch, err := p.Convert(f.Stream, f.Rows)
			if err != nil {
				return err
			}
			f = Frame{Kind: FrameBatch, Stream: f.Stream, Batch: batch}
		}
		if err := send(f); err != nil {
			return err
		}
		if f.Kind != FrameBarrier {
			continue
		}
		if crossed[f.Epoch]++; crossed[f.Epoch] < len(plan.Streams) {
			continue
		}
		delete(crossed, f.Epoch)
		if err := send(Frame{Kind: FrameReady, Epoch: f.Epoch, Ready: &Ready{}}); err != nil {
			return err
		}
	}
}
