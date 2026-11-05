package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/sdk/go/arrowipc"
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
	// Out of the dataflow only, with Plan.ReportSchemas: Stream's schema is
	// Schema from its next batch on, and from now if it had none.
	FrameSchema
	// Out of the dataflow only: Batch holds patches of held cells
	// (ADR-0025), for the destination's Patcher.
	FramePatch
)

// Frame is one unit on a job's data path. Barriers travel in band, ordered
// with their stream's records.
type Frame struct {
	Kind   FrameKind
	Stream string
	Rows   []json.RawMessage
	Batch  arrow.RecordBatch
	// A batch into the dataflow, in place of Batch, as an external source
	// encoded it: an Arrow IPC stream holding one batch.
	IPC   []byte
	Epoch uint64
	Ready *Ready
	// An Arrow IPC stream holding only the schema.
	Schema []byte
}

// Ready is the dataflow's part of an epoch, recorded in its checkpoint.
type Ready struct {
	// What an engine sink staged.
	Staged destination.Staged
	// What a staged sink staged, by stream; streams without rows are missing.
	Files      map[string][]destination.StagedFile
	Snapshots  []checkpoint.Snapshot
	Quarantine []checkpoint.QuarantineFile
	// Each stream's schema at its barrier, as an Arrow IPC stream holding
	// only the schema. With a transform, its inputs'.
	Schemas map[string][]byte
	// With a transform, its outputs' schemas, as Schemas, and the rows each
	// output had.
	OutputSchemas map[string][]byte
	OutputRows    map[string]uint64
	// Where columns are held whole: each stream's schema as the destination
	// gets it, as Schemas.
	Delivered map[string][]byte
	// The streams that hold, at their barriers.
	Holds map[string]*ReadyHolds
}

// ReadyHolds are a stream's holds at its barrier.
type ReadyHolds struct {
	// Every column held whole, without Since.
	Holding []checkpoint.ColumnHold
	// The epoch's held cells by column, without Since.
	Cells map[string]checkpoint.CellHold
	// Below Plan.HeldURL.
	Files []destination.StagedFile
	// Nil without a replay.
	Replayed *ReplayProgress
}

type ReplayProgress struct {
	File           int
	Row, StillHeld uint64
	Done           bool
}

// Plan is what a dataflow needs to know about one attempt.
type Plan struct {
	Org        string
	Pipeline   string
	Owner      string
	Generation uint64
	// The first barrier's epoch.
	FirstEpoch uint64
	Streams    []Stream
	// Zero: out feeds the destination's Writer.
	Sink   SinkPlan
	Naming Naming
	Policy SchemaPolicy
	// Where the dataflow keeps records it can't take; empty: they fail it.
	QuarantineURL string
	// Infinite dates and timestamps are quarantined instead of clamped.
	QuarantineInfinity bool
	// Each stream's read start, in microseconds since the Unix epoch.
	ReadStarts map[string]int64
	// From the restored checkpoint, as Ready reported them.
	Schemas   map[string][]byte
	Snapshots []checkpoint.Snapshot
	// out carries FrameSchema frames. An empty batch establishes its
	// stream's schema without rows.
	ReportSchemas bool
	// Where held values go; empty: nothing is held.
	HeldURL string
	// How streams hold; those that hold nothing are missing.
	Holds map[string]*HoldPlan
}

// SchemaPolicy is how the dataflow takes changes to a stream's established
// schema.
type SchemaPolicy int

const (
	// AllowAll adds columns and widens types; a type that isn't a widening
	// goes to a new column.
	AllowAll SchemaPolicy = iota
	// AllowColumns is AllowAll, but fails on a type that isn't a widening.
	AllowColumns
	// Block fails on any change.
	Block
	// IgnoreNew drops new columns and quarantines values that don't fit.
	IgnoreNew
)

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
	// Parquet files below this URL, for a DeliveryStaged destination to load
	// (Staging.URL), or a transform's inputs (Transform.URL).
	StagedURL string
	// With StagedURL, each epoch's files go in this directory of it.
	StagedDir string
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
// and encoded batches into batches, forwards every barrier, and closes out once in is drained.
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
	// Stage stages a stream's batches in epoch e, for a plan with
	// Sink.StagedURL. It must not keep them.
	Stage func(ctx context.Context, e destination.Epoch, stream string, batches []arrow.RecordBatch) ([]destination.StagedFile, error)
}

func (p Passthrough) Run(ctx context.Context, plan Plan, in <-chan Frame, out chan<- Frame) error {
	defer close(out)
	staging := plan.Sink.StagedURL != ""
	if staging && p.Stage == nil {
		return errors.New("runner: the plan stages its epochs, and the dataflow can't")
	}
	held := map[string][]arrow.RecordBatch{}
	files := map[uint64]map[string][]destination.StagedFile{}
	defer func() {
		for _, bs := range held {
			for _, b := range bs {
				b.Release()
			}
		}
	}()
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
		switch {
		case f.Kind == FrameRows:
			if p.Convert == nil {
				return fmt.Errorf("runner: rows for %s, and the dataflow can't convert them", f.Stream)
			}
			batch, err := p.Convert(f.Stream, f.Rows)
			if err != nil {
				return err
			}
			f = Frame{Kind: FrameBatch, Stream: f.Stream, Batch: batch}
		case f.IPC != nil:
			batch, err := arrowipc.DecodeBatch(f.IPC, memory.DefaultAllocator)
			if err != nil {
				return fmt.Errorf("runner: a batch for %s: %w", f.Stream, err)
			}
			f = Frame{Kind: FrameBatch, Stream: f.Stream, Batch: batch}
		}
		switch {
		case !staging:
			if err := send(f); err != nil {
				return err
			}
		case f.Kind == FrameBatch:
			held[f.Stream] = append(held[f.Stream], f.Batch)
		case f.Kind == FrameBarrier:
			fs, err := p.Stage(ctx, destination.Epoch{Generation: plan.Generation, N: f.Epoch}, f.Stream, held[f.Stream])
			for _, b := range held[f.Stream] {
				b.Release()
			}
			delete(held, f.Stream)
			if err != nil {
				return fmt.Errorf("runner: stage %s in epoch %d: %w", f.Stream, f.Epoch, err)
			}
			if len(fs) > 0 {
				if files[f.Epoch] == nil {
					files[f.Epoch] = map[string][]destination.StagedFile{}
				}
				files[f.Epoch][f.Stream] = fs
			}
		}
		if f.Kind != FrameBarrier {
			continue
		}
		if crossed[f.Epoch]++; crossed[f.Epoch] < len(plan.Streams) {
			continue
		}
		delete(crossed, f.Epoch)
		if err := send(Frame{Kind: FrameReady, Epoch: f.Epoch, Ready: &Ready{Files: files[f.Epoch]}}); err != nil {
			return err
		}
		delete(files, f.Epoch)
	}
}
