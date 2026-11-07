package runner

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
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
	// Out of the dataflow only: Batch holds patches of held cells,
	// for the destination's Patcher.
	FramePatch
)

// Frame is one unit on a job's data path. Barriers travel in band, ordered
// with their stream's records.
type Frame struct {
	Kind FrameKind
	// Into the dataflow, the lane the frame came in on; out of it, the
	// writer lane it goes to.
	Lane   int
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
	// What an engine sink or a staged sink staged, by writer lane; nil where
	// the destination's writers stage.
	Lanes      []LaneReady
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

// LaneReady is a writer lane's part of an epoch.
type LaneReady struct {
	// An engine sink's committable.
	Staged destination.Staged
	// What a staged sink staged, by stream; streams without rows are missing.
	Files map[string][]destination.StagedFile
}

// Files are what a staged sink staged on every lane, by stream; nil if
// none.
func (r *Ready) Files() map[string][]destination.StagedFile {
	var out map[string][]destination.StagedFile
	for _, l := range r.Lanes {
		for stream, fs := range l.Files {
			if out == nil {
				out = map[string][]destination.StagedFile{}
			}
			out[stream] = append(out[stream], fs...)
		}
	}
	return out
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
	// The attempt's lanes and writer lanes, at most as many; 0
	// is one. A stream's frames go from lane i to writer lane i mod
	// WriterLanes, or for one in KeyGrouped, each row to the writer lane
	// owning its key's group: writer lane w owns the w-th of WriterLanes
	// contiguous ranges of the KeyGroups.
	Lanes, WriterLanes int
	KeyGroups          int
	KeyGrouped         map[string]bool
}

func (p Plan) lanes() int       { return max(p.Lanes, 1) }
func (p Plan) writerLanes() int { return max(p.WriterLanes, 1) }

// WriterLane is the writer lane that owns key group g.
func (p Plan) WriterLane(g int) int {
	return g * p.writerLanes() / cmp.Or(p.KeyGroups, checkpoint.KeyGroups)
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
// an in-process stand-in. Run keeps each lane's frames of a stream in order,
// turns rows and encoded batches into batches, sends them to their writer
// lanes, and closes out once in is drained. A writer lane passes a stream's
// barrier N once every lane that feeds it has, holding back what those that
// passed it first send after it. Once every writer lane has passed barrier N
// on every stream, out also carries one FrameReady for N, in epoch order.
// With an engine sink, out carries only those. The receiver of a batch
// releases it.
type Dataflow interface {
	Run(ctx context.Context, plan Plan, in <-chan Frame, out chan<- Frame) error
}

// Passthrough is a dataflow without the engine. Convert turns rows into
// batches; with none, rows are an error. It reports no schemas, and routes
// by key group with a hash of its own: FNV-1a over each key value's text,
// not openetl.kg.v1.
type Passthrough struct {
	Convert func(stream string, rows []json.RawMessage) (arrow.RecordBatch, error)
	// Stage stages a stream's batches of writer lane lane in epoch e, for a
	// plan with Sink.StagedURL. It must not keep them.
	Stage func(ctx context.Context, e destination.Epoch, lane int, stream string, batches []arrow.RecordBatch) ([]destination.StagedFile, error)
}

// passItem is a frame from one lane to one writer lane, or the lane's
// barrier.
type passItem struct {
	f       Frame
	barrier bool
}

// passLane is a stream on one writer lane: its epoch, and what each lane
// feeding it sent, which waits there until the writer lane is at the same
// barrier.
type passLane struct {
	epoch  uint64
	queues map[int][]passItem
	// Staged sinks: the epoch's batches.
	held []arrow.RecordBatch
}

func (p Passthrough) Run(ctx context.Context, plan Plan, in <-chan Frame, out chan<- Frame) error {
	defer close(out)
	staging := plan.Sink.StagedURL != ""
	if staging && p.Stage == nil {
		return errors.New("runner: the plan stages its epochs, and the dataflow can't")
	}
	lanes, writers := plan.lanes(), plan.writerLanes()
	if writers > lanes {
		return fmt.Errorf("runner: %d writer lanes for %d lanes", writers, lanes)
	}
	streams := map[string]*Stream{}
	feeds := map[string][][]int{} // by stream and writer lane: the lanes feeding it
	pass := map[string][]*passLane{}
	for i := range plan.Streams {
		s := &plan.Streams[i]
		streams[s.Name] = s
		feeds[s.Name] = make([][]int, writers)
		pass[s.Name] = make([]*passLane, writers)
		for w := range writers {
			pass[s.Name][w] = &passLane{epoch: plan.FirstEpoch, queues: map[int][]passItem{}}
			for l := range lanes {
				if plan.KeyGrouped[s.Name] || l%writers == w {
					feeds[s.Name][w] = append(feeds[s.Name][w], l)
				}
			}
		}
	}
	defer func() {
		for _, ps := range pass {
			for _, pl := range ps {
				for _, b := range pl.held {
					b.Release()
				}
				for _, q := range pl.queues {
					for _, it := range q {
						release(it.f)
					}
				}
			}
		}
	}()
	files := map[uint64][]LaneReady{}
	crossed := make(map[uint64]int)
	send := func(f Frame) error {
		select {
		case out <- f:
			return nil
		case <-ctx.Done():
			release(f)
			return ctx.Err()
		}
	}
	// deliver sends what's queued for the stream's writer lane w up to the
	// barrier every lane feeding it is at, and passes it, as long as they
	// all are.
	deliver := func(name string, w int) error {
		pl := pass[name][w]
		for {
			at := true
			for _, l := range feeds[name][w] {
				q := pl.queues[l]
				for len(q) > 0 && !q[0].barrier {
					f := q[0].f
					q = q[1:]
					if staging {
						pl.held = append(pl.held, f.Batch)
					} else if err := send(f); err != nil {
						pl.queues[l] = q
						return err
					}
				}
				pl.queues[l] = q
				if len(q) > 0 && q[0].f.Epoch != pl.epoch {
					return fmt.Errorf("runner: lane %d passed barrier %d of %s, which is at %d", l, q[0].f.Epoch, name, pl.epoch)
				}
				at = at && len(q) > 0
			}
			if !at {
				return nil
			}
			for _, l := range feeds[name][w] {
				pl.queues[l] = pl.queues[l][1:]
			}
			n := pl.epoch
			pl.epoch++
			if staging {
				fs, err := p.Stage(ctx, destination.Epoch{Generation: plan.Generation, N: n}, w, name, pl.held)
				for _, b := range pl.held {
					b.Release()
				}
				pl.held = nil
				if err != nil {
					return fmt.Errorf("runner: stage %s in epoch %d: %w", name, n, err)
				}
				if files[n] == nil {
					files[n] = make([]LaneReady, writers)
				}
				if len(fs) > 0 {
					if files[n][w].Files == nil {
						files[n][w].Files = map[string][]destination.StagedFile{}
					}
					files[n][w].Files[name] = fs
				}
			} else if err := send(Frame{Kind: FrameBarrier, Lane: w, Stream: name, Epoch: n}); err != nil {
				return err
			}
			if crossed[n]++; crossed[n] < len(plan.Streams)*writers {
				continue
			}
			delete(crossed, n)
			ready := &Ready{}
			if staging {
				ready.Lanes = files[n]
				if ready.Lanes == nil {
					ready.Lanes = make([]LaneReady, writers)
				}
				delete(files, n)
			}
			if err := send(Frame{Kind: FrameReady, Epoch: n, Ready: ready}); err != nil {
				return err
			}
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
		s := streams[f.Stream]
		if s == nil || f.Lane < 0 || f.Lane >= lanes {
			release(f)
			return fmt.Errorf("runner: a frame for %s on lane %d, which the plan doesn't have", f.Stream, f.Lane)
		}
		lane := f.Lane
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
		to := map[int]Frame{}
		switch {
		case f.Kind == FrameBarrier:
			for w := range writers {
				if slices.Contains(feeds[s.Name][w], lane) {
					to[w] = f
				}
			}
		case f.Kind != FrameBatch:
			release(f)
			return fmt.Errorf("runner: unexpected frame kind %d into the dataflow", f.Kind)
		case plan.KeyGrouped[s.Name]:
			parts, err := byWriterLane(plan, s.PrimaryKey, f.Batch)
			f.Batch.Release()
			if err != nil {
				return fmt.Errorf("runner: route %s: %w", f.Stream, err)
			}
			for w, b := range parts {
				to[w] = Frame{Kind: FrameBatch, Stream: f.Stream, Batch: b}
			}
		default:
			to[lane%writers] = f
		}
		for _, w := range slices.Sorted(maps.Keys(to)) {
			g := to[w]
			g.Lane = w
			pl := pass[s.Name][w]
			pl.queues[lane] = append(pl.queues[lane], passItem{f: g, barrier: g.Kind == FrameBarrier})
			if err := deliver(s.Name, w); err != nil {
				return err
			}
		}
	}
}

// byWriterLane splits a batch's rows by the writer lane owning their key's
// group.
func byWriterLane(plan Plan, key []string, b arrow.RecordBatch) (map[int]arrow.RecordBatch, error) {
	cols := make([]arrow.Array, len(key))
	for i, k := range key {
		idx := b.Schema().FieldIndices(k)
		if len(idx) == 0 {
			return nil, fmt.Errorf("the key column %q is missing", k)
		}
		cols[i] = b.Column(idx[0])
	}
	groups := cmp.Or(plan.KeyGroups, checkpoint.KeyGroups)
	rows := map[int][]int{}
	for r := range int(b.NumRows()) {
		h := fnv.New64a()
		for _, c := range cols {
			_, _ = h.Write([]byte(c.ValueStr(r)))
			_, _ = h.Write([]byte{0})
		}
		w := plan.WriterLane(int(h.Sum64() % uint64(groups))) //nolint:gosec // small
		rows[w] = append(rows[w], r)
	}
	out := make(map[int]arrow.RecordBatch, len(rows))
	for w, rs := range rows {
		parts := make([]arrow.RecordBatch, len(rs))
		for i, r := range rs {
			parts[i] = b.NewSlice(int64(r), int64(r)+1)
		}
		cols := make([]arrow.Array, b.NumCols())
		for c := range cols {
			arrs := make([]arrow.Array, len(parts))
			for i, p := range parts {
				arrs[i] = p.Column(c)
			}
			a, err := array.Concatenate(arrs, memory.DefaultAllocator)
			if err != nil {
				return nil, err
			}
			cols[c] = a
		}
		for _, p := range parts {
			p.Release()
		}
		out[w] = array.NewRecordBatch(b.Schema(), cols, int64(len(rs)))
		for _, a := range cols {
			a.Release()
		}
	}
	return out, nil
}
