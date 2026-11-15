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
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/platform/ints"
	"github.com/openetl-io/openetl/internal/runner/attempt"
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
	// Into the dataflow only, for a stream with event time: one of its
	// splits joins Lane, the first of its splits' on it, or its watermark
	// is raised. The splits a lane reads join it before anything else of
	// the stream on it; a split a split adds joins its lane.
	FrameSplit
)

// Frame is one unit on a job's data path. Barriers travel in band, ordered
// with their stream's records.
type Frame struct {
	Kind FrameKind
	// Into the dataflow, the lane the frame came in on; out of it, the
	// writer lane it goes to.
	Lane   int
	Stream string
	// Into the dataflow, of a stream with event time: the split a
	// FrameRows, FrameBatch or FrameSplit is of.
	Split string
	// A FrameSplit's: the split's watermark from here is at least this, in
	// microseconds since the Unix epoch; nil: unchanged.
	Watermark *int64
	Rows      []json.RawMessage
	Batch     arrow.RecordBatch
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
	// With a transform, its outputs' schemas, as Schemas, and from one the
	// runner runs, the rows each output had.
	OutputSchemas map[string][]byte
	OutputRows    map[string]uint64
	// Where columns are held whole: each of the destination's streams'
	// schema as the destination gets it, as Schemas.
	Delivered map[string][]byte
	// The destination's streams that hold, at their barriers.
	Holds map[string]*ReadyHolds
	// Each split's watermark at its lane's barrier, by stream with event
	// time and split.
	Watermarks map[string]map[string]int64
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

// ReadyHolds are a stream's holds at its barrier, as a worker's share of
// an epoch carries them too.
type ReadyHolds = attempt.Holds

type ReplayProgress = attempt.ReplayProgress

// Plan is what a dataflow needs to know about one attempt.
type Plan struct {
	Org        string
	Pipeline   string
	Owner      string
	Generation uint64
	// The first barrier's epoch.
	FirstEpoch uint64
	Streams    []Stream
	// SQL the engine runs between the normalizer's halves; nil for none.
	// With it, Streams are its inputs, Outputs the destination's streams, and
	// OutputSchemas what each output's schema starts as, as Schemas.
	SQL           *SQLTransform
	Outputs       []Stream
	OutputSchemas map[string][]byte
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
	// What each stream's schema starts as: the restored checkpoint's, as
	// Ready reported them, or one a Flink job's batches have changed it to.
	Schemas   map[string][]byte
	Snapshots []checkpoint.Snapshot
	// out carries FrameSchema frames. An empty batch establishes its
	// stream's schema without rows.
	ReportSchemas bool
	// Where held values go; empty: nothing is held.
	HeldURL string
	// How the destination's streams hold; those that hold nothing are
	// missing.
	Holds map[string]*HoldPlan
	// The attempt's lanes and writer lanes, at most as many; 0 is one. The
	// lanes and writer lanes own the KeyGroups as plan.NewLayout lays them
	// out. A stream's frames go from a lane to the writer lane whose range
	// holds the lane's, or for one of the destination's streams in
	// Exchanges, each row to the writer lane owning the key group of its
	// values of those columns, which are of its primary key.
	Lanes, WriterLanes int
	KeyGroups          int
	Exchanges          map[string][]string
	// A redo's: of the destination's streams named, only the rows whose
	// key's group is in the range, by the hash Exchanges route by; others
	// pass whole.
	KeyGroupRanges map[string]checkpoint.KeyGroupRange
	// A worker's: where its streams' schemas are decided, for an engine
	// that runs the attempt with others'; nil: the dataflow decides.
	SchemaOwner SchemaOwner
	// A worker's: the attempt's index of its first lane, which names its
	// lanes' quarantine files; the worker whose first lane is the attempt's
	// first patches in released holds.
	FirstLane int
	// A worker's: the attempt's index of its first writer lane, and the
	// attempt's writer lanes, which name held files; 0: WriterLanes.
	FirstWriterLane, AttemptWriterLanes int
	// By stream with event time: the least watermark of the splits the
	// attempt reads, every worker's, which a lane past an exchange starts
	// at; missing where one has none.
	Floors map[string]int64
	// A worker's, where the workers' engines exchange rows between them:
	// then its lanes and writer lanes are its part of the attempt's layout
	// (plan.NewWorkerLayout), and Exchanges send rows to the attempt's
	// writer lanes, whichever worker has them. Nil: an exchange stays in
	// the engine.
	Mesh *Mesh
}

// Mesh is the attempt's engines, one per worker (openetl.engine.v1.Mesh).
type Mesh struct {
	// This worker's.
	Engine  int
	Engines []MeshEngine
	// Where this worker's engine listens for the others, host:port.
	Listen string
	// The attempt's certificate, as DER, which every engine presents and is
	// the only one each trusts, its PKCS #8 key, and its secret.
	Certificate, Key, Secret []byte
}

type MeshEngine struct {
	// Where the others reach it, host:port.
	Address            string
	Lanes, WriterLanes int
}

// Meshing is a Dataflow whose engines, one per worker of an attempt,
// exchange rows with each other's, as Plan.Mesh says.
type Meshing interface {
	Dataflow
	Meshes() bool
}

// meshes says the dataflow's engines exchange rows between workers.
func meshes(d Dataflow) bool {
	m, ok := d.(Meshing)
	return ok && m.Meshes()
}

// SchemaOwner decides the schemas of the destination's streams, with the
// columns they hold whole, and of the streams a SQL transform reads, for
// the dataflows of the workers of one attempt. A dataflow that changes a
// stream's schema, or what it holds, proposes it from the version it had,
// the restored schema being version 0, and goes on with it once an update
// gives it the next version with that schema; otherwise it shapes by the
// newer one.
type SchemaOwner interface {
	Propose(p SchemaProposal) error
	// Each stream's versions, in order: answers to proposals, and changes
	// other workers made.
	Updates() <-chan SchemaUpdate
}

// SchemaProposal is a change a dataflow made to a stream, from version
// Base of it.
type SchemaProposal struct {
	Stream string
	// A stream a SQL transform reads, whose versions are apart from those
	// of the destination's stream of the same name.
	Input bool
	Base  uint64
	// An Arrow IPC stream holding only the schema.
	Schema []byte
	// One of the destination's streams that holds: the columns it holds
	// whole, and the schema the destination gets, as Schema; nil where
	// that's Schema.
	Holding   []checkpoint.ColumnHold
	Delivered []byte
}

type SchemaUpdate struct {
	Stream  string
	Input   bool
	Version uint64
	// An Arrow IPC stream holding only the schema.
	Schema  []byte
	Holding []checkpoint.ColumnHold
}

func (p Plan) lanes() int       { return max(p.Lanes, 1) }
func (p Plan) writerLanes() int { return max(p.WriterLanes, 1) }

func (p Plan) keyGroups() int { return cmp.Or(p.KeyGroups, checkpoint.KeyGroups) }

// WriterLane is the writer lane that owns key group g.
func (p Plan) WriterLane(g int) int {
	return g * p.writerLanes() / p.keyGroups()
}

// layout is the key groups the plan's lanes and writer lanes own: with a
// mesh, the worker's part of the attempt's.
func (p Plan) layout() (plan.Layout, error) {
	m := p.Mesh
	if m == nil {
		return plan.NewLayout(p.keyGroups(), p.lanes(), p.writerLanes())
	}
	lanes, writers := make([]int, len(m.Engines)), make([]int, len(m.Engines))
	firstLane, firstWriter := 0, 0
	for i, e := range m.Engines {
		lanes[i], writers[i] = e.Lanes, e.WriterLanes
		if i < m.Engine {
			firstLane, firstWriter = firstLane+e.Lanes, firstWriter+e.WriterLanes
		}
	}
	l, err := plan.NewWorkerLayout(p.keyGroups(), lanes, writers)
	if err != nil {
		return plan.Layout{}, err
	}
	return l.Part(firstLane, p.lanes(), firstWriter, p.writerLanes())
}

// laidOut is the plan's layout; empty for a plan no layout fits, which a
// dataflow refuses.
func (p Plan) laidOut() plan.Layout {
	l, err := p.layout()
	if err != nil {
		return plan.Layout{}
	}
	return l
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
	if plan.Mesh != nil && len(plan.Mesh.Engines) > 1 {
		return errors.New("runner: the plan exchanges rows between engines, and the dataflow can't")
	}
	lanes, writers := plan.lanes(), plan.writerLanes()
	if writers > lanes {
		return fmt.Errorf("runner: %d writer lanes for %d lanes", writers, lanes)
	}
	layout, err := plan.layout()
	if err != nil {
		return err
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
				if plan.Exchanges[s.Name] != nil || layout.WriterOf[l] == w {
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
		// Without the engine, nothing has a watermark.
		if f.Kind == FrameSplit {
			continue
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
		if r, ok := plan.KeyGroupRanges[s.Name]; ok && f.Kind == FrameBatch {
			kept, err := inKeyGroups(plan, s.PrimaryKey, f.Batch, r)
			f.Batch.Release()
			if err != nil {
				return fmt.Errorf("runner: keep %s's key groups: %w", f.Stream, err)
			}
			if kept == nil {
				continue
			}
			f.Batch = kept
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
		case plan.Exchanges[s.Name] != nil:
			parts, err := byWriterLane(plan, plan.Exchanges[s.Name], f.Batch)
			f.Batch.Release()
			if err != nil {
				return fmt.Errorf("runner: route %s: %w", f.Stream, err)
			}
			for w, b := range parts {
				to[w] = Frame{Kind: FrameBatch, Stream: f.Stream, Batch: b}
			}
		default:
			to[layout.WriterOf[lane]] = f
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

// keyGroupsOf is the key group of each of a batch's rows, by Passthrough's
// hash.
func keyGroupsOf(plan Plan, key []string, b arrow.RecordBatch) ([]int, error) {
	cols := make([]arrow.Array, len(key))
	for i, k := range key {
		idx := b.Schema().FieldIndices(k)
		if len(idx) == 0 {
			return nil, fmt.Errorf("the key column %q is missing", k)
		}
		cols[i] = b.Column(idx[0])
	}
	groups := uint64(max(cmp.Or(plan.KeyGroups, checkpoint.KeyGroups), 1))
	out := make([]int, b.NumRows())
	for r := range out {
		h := fnv.New64a()
		for _, c := range cols {
			_, _ = h.Write([]byte(c.ValueStr(r)))
			_, _ = h.Write([]byte{0})
		}
		// Below groups, so it fits.
		g, _ := ints.Int64(h.Sum64() % groups)
		out[r] = int(g)
	}
	return out, nil
}

// byWriterLane splits a batch's rows by the writer lane owning their key's
// group.
func byWriterLane(plan Plan, key []string, b arrow.RecordBatch) (map[int]arrow.RecordBatch, error) {
	groups, err := keyGroupsOf(plan, key, b)
	if err != nil {
		return nil, err
	}
	rows := map[int][]int{}
	for r, g := range groups {
		w := plan.WriterLane(g)
		rows[w] = append(rows[w], r)
	}
	out := make(map[int]arrow.RecordBatch, len(rows))
	for w, rs := range rows {
		if out[w], err = takeRows(b, rs); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// inKeyGroups is a batch's rows whose key's group is in r; nil if none is.
func inKeyGroups(plan Plan, key []string, b arrow.RecordBatch, r checkpoint.KeyGroupRange) (arrow.RecordBatch, error) {
	groups, err := keyGroupsOf(plan, key, b)
	if err != nil {
		return nil, err
	}
	var rows []int
	for i, g := range groups {
		if g >= r.First && g <= r.Last {
			rows = append(rows, i)
		}
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return takeRows(b, rows)
}

// takeRows is a batch of b's rows rs, in that order.
func takeRows(b arrow.RecordBatch, rs []int) (arrow.RecordBatch, error) {
	parts := make([]arrow.RecordBatch, len(rs))
	for i, r := range rs {
		parts[i] = b.NewSlice(int64(r), int64(r)+1)
	}
	defer func() {
		for _, p := range parts {
			p.Release()
		}
	}()
	cols := make([]arrow.Array, b.NumCols())
	defer func() {
		for _, a := range cols {
			if a != nil {
				a.Release()
			}
		}
	}()
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
	return array.NewRecordBatch(b.Schema(), cols, int64(len(rs))), nil
}
