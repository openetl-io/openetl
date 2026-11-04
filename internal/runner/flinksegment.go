package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bridgev1 "github.com/unsuitable001/openetl/gen/openetl/bridge/v1"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/sdk/go/arrowipc"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

type metricOption = metric.MeasurementOption

// errTaken: a restarted task's call came while the segment still had its
// predecessor's, so it waits for the next segment.
var errTaken = errors.New("runner: the segment's split or sink is taken")

// The bytes the read queues hold before the first dataflow waits, unless a
// barrier is on its way out of it: holding that back could keep a reader
// from a checkpoint another reader's backlog waits on.
const flinkQueueBytes = 64 << 20

// flinkSegment is one generation's dataflow around the Flink job: the source
// into the first engine job, its output to the readers through the bridge,
// the sinks' output into the second engine job, and the destination. Its
// epochs end at Flink's checkpoints: the enumerator says one started, the
// first dataflow's barrier for it is placed at each stream's next resume
// point, and each reader takes it there.
type flinkSegment struct {
	a        *flinkAttempt
	c        *coordinator
	restored *checkpoint.Record
	learn    bool
	streams  map[string]Stream
	outputs  map[string]Stream
	// Some input is a changelog, so a merged output needs _openetl_op.
	changelog bool

	// Set by the loop; read by the attempt once the segment ended.
	head, headPrev *checkpoint.Record
	headSt         *epochState
	headCommitted  bool
	hello          *coordMsg

	g          *errgroup.Group
	gctx       context.Context
	cancel     context.CancelCauseFunc
	ended      chan struct{}
	resultOnce sync.Once
	result     error

	learned  chan struct{}
	toSecond chan Frame
	ev       chan segEvent
	tasks    chan flinkTask

	mu              sync.Mutex
	cond            *sync.Cond
	closing         bool
	queues          map[string]*readQueue
	queued          int
	cutC            map[uint64]int64 // the checkpoint each first-dataflow barrier ends
	out             map[uint64]int   // streams whose barrier k came out of the first dataflow
	inFlight        int              // barriers placed whose every stream isn't out yet
	known           map[string][]byte
	deployedSchemas map[string]*arrow.Schema
	sinks           map[sinkKey]bool
	outRows         map[string]uint64

	// The loop's own.
	jobID       string
	helloSeen   bool
	pendingHi   *coordMsg
	readers     map[string]bool
	parallelism map[string]int32
	cuts        map[int64]*flinkCut
	order       []int64 // started checkpoints not yet decided
	waiting     []int64 // started checkpoints whose barrier isn't requested
	byK         map[uint64]*flinkCut
	placing     bool
	nextK       uint64
	nextN       uint64
	carry       []*flinkCut
	epochs      map[uint64]*flinkEpoch
	byC         map[int64]uint64 // recorded checkpoints waiting for their outcome
	aborted     map[int64]bool   // checkpoints aborted while the job stops
	lastN       uint64
	committedN  uint64
	commitReady bool
	recording   bool
	committing  bool
	idleHeld    []chan error
	stopped     bool
}

type sinkKey struct {
	output  string
	subtask int32
}

type readQueue struct {
	items  []readItem
	reader bool
}

type readItem struct {
	batch   []byte
	barrier int64
}

type flinkCut struct {
	c        int64
	k        uint64
	placed   bool
	states   map[string][]byte
	forced   []string
	complete map[string]bool
	rows     map[string]uint64
	ready    *Ready
	barriers int
	typ      bridgev1.BarrierType
	replies  []chan error
	// Aborted while the job stops: the readers pass over its barrier, so
	// its rows go into the next record.
	aborted bool
}

type flinkEpoch struct {
	n         uint64
	c         int64
	typ       bridgev1.BarrierType
	inputs    []*flinkCut
	outRows   map[string]uint64
	replies   []chan error
	requested time.Time
	staged    bool
	desc      destination.Staged
	second    *Ready
	st        *epochState
}

type segEventKind uint8

const (
	segSinkHello segEventKind = iota + 1
	segSinkBarrier
	segDropped
	segReader
	segInputReady
	segSchema
	segDeployed
	segRecorded
	segCommitted
)

type segEvent struct {
	kind        segEventKind
	sink        sinkKey
	parallelism int32
	stream      string
	checkpoint  int64
	typ         bridgev1.BarrierType
	reply       chan error
	epoch       uint64
	ready       *Ready
	rec         *checkpoint.Record
	err         error
	job         string
	jobDone     chan error
}

type flinkTask struct {
	record bool
	n      uint64
	st     *epochState
	rec    *checkpoint.Record
	start  time.Time
}

func newFlinkSegment(a *flinkAttempt, restored *checkpoint.Record, learn bool) *flinkSegment {
	s := &flinkSegment{
		a: a, restored: restored, learn: learn,
		streams: map[string]Stream{}, outputs: map[string]Stream{},
		ended: make(chan struct{}), learned: make(chan struct{}),
		toSecond: make(chan Frame, 16), ev: make(chan segEvent, 64), tasks: make(chan flinkTask, 2),
		queues: map[string]*readQueue{}, cutC: map[uint64]int64{}, out: map[uint64]int{}, known: map[string][]byte{},
		sinks: map[sinkKey]bool{}, outRows: map[string]uint64{},
		readers: map[string]bool{}, parallelism: map[string]int32{},
		cuts: map[int64]*flinkCut{}, byK: map[uint64]*flinkCut{}, epochs: map[uint64]*flinkEpoch{}, byC: map[int64]uint64{}, aborted: map[int64]bool{},
	}
	s.cond = sync.NewCond(&s.mu)
	for _, st := range a.job.Streams {
		s.streams[st.Name] = st
		s.queues[st.Name] = &readQueue{}
		s.changelog = s.changelog || st.Read == source.CDC
	}
	for _, o := range a.job.Outputs {
		s.outputs[o.Name] = o
	}
	a.mu.Lock()
	for name, raw := range a.schemas {
		s.known[name] = raw
	}
	if !learn {
		s.deployedSchemas = decodeAll(a.schemas)
	}
	a.mu.Unlock()
	s.head = restored
	s.headCommitted = true
	if len(s.known) == len(s.streams) {
		close(s.learned)
	}
	return s
}

func decodeAll(raw map[string][]byte) map[string]*arrow.Schema {
	out := make(map[string]*arrow.Schema, len(raw))
	for name, b := range raw {
		if sc, err := decodeSchema(b); err == nil {
			out[name] = sc
		}
	}
	return out
}

// begin starts the segment's goroutines; run waits for them.
func (s *flinkSegment) begin(ctx context.Context) error {
	a, job := s.a, s.a.job
	if s.restored != nil && s.restored.Previous != nil {
		prev, err := job.Log.Record(ctx, *s.restored.Previous)
		if err != nil && !errors.Is(err, checkpoint.ErrNotFound) {
			return err
		}
		s.headPrev = prev
	}
	c := &coordinator{job: job, sess: a.sess, w: a.w, log: a.log, res: a.res, pending: map[uint64]*epochState{},
		applied: map[string]*arrow.Schema{}, reported: a.held.reported, reportedOutputs: a.held.reportedOutputs, measures: a.held.measures,
		events: make(chan event, 64)}
	c.afterPrune = func(ctx context.Context, head *checkpoint.Record) { a.pruneFlink(ctx, head) }
	if s.restored != nil {
		_ = applySchemas(ctx, c.w, c.applied, outputSchemas(s.restored))
		if len(s.restored.Files) > 0 {
			c.lastStaged = destination.Epoch{Generation: s.restored.Generation, N: s.restored.Epoch}
		}
	}
	s.c = c
	gctx, cancel := context.WithCancelCause(ctx)
	g, gctx := errgroup.WithContext(gctx)
	s.g, s.gctx, s.cancel = g, gctx, cancel
	context.AfterFunc(gctx, func() {
		s.mu.Lock()
		s.closing = true
		s.cond.Broadcast()
		s.mu.Unlock()
	})

	restored := s.restored
	states := map[string][]byte{}
	schemas := map[string][]byte{}
	restoredForced := map[string]bool{}
	now := time.Now().UnixMicro()
	c.readStarts = make(map[string]int64, len(job.Streams))
	for _, st := range job.Streams {
		c.readStarts[st.Name] = now
		if restored != nil {
			if rs := restored.Streams[st.Name]; rs.ReadStart != 0 && !rs.Complete {
				c.readStarts[st.Name] = rs.ReadStart
			}
		}
	}
	if restored != nil {
		c.committed = restored.Epoch
		for name, st := range restored.Streams {
			states[name] = st.State
			if st.Schema != nil {
				schemas[name] = st.Schema
			}
		}
		for _, name := range restored.Forced {
			restoredForced[name] = true
		}
		c.ackStates = states
	}
	first := c.committed + 1
	s.nextK, s.nextN, s.lastN, s.committedN = first, first, c.committed, c.committed

	toFlow := make(chan Frame, 16)
	in1 := make(chan Frame, 16)
	out1 := make(chan Frame, 16)
	fromSecond := make(chan Frame, 16)
	ends := &readEnds{at: map[string]uint64{}}
	c.pump = newSourcePump(gctx, job, c.committed, states, restoredForced, job.canForce(), map[string]bool{}, ends, toFlow, c.events)
	plan := Plan{
		Org: job.Org, Pipeline: job.Pipeline, Owner: job.Owner, Generation: a.sess.Generation(), FirstEpoch: first,
		Streams: job.Streams,
		Naming:  Naming{KeepSource: job.KeepSourceNames, MaxLength: job.Destination.Capabilities().MaxIdentifierLength},
		Policy:  job.SchemaPolicy, QuarantineURL: job.QuarantineURL, QuarantineInfinity: job.QuarantineInfinity,
		ReadStarts: c.readStarts, Schemas: schemas, ReportSchemas: true,
	}
	second := plan
	second.Streams, second.Schemas, second.ReportSchemas = job.Outputs, outputSchemas(restored), false
	if restored != nil {
		second.Snapshots = restored.Snapshots
	}
	if plan.QuarantineURL != "" {
		second.QuarantineURL = plan.QuarantineURL + "/" + checkpoint.QuarantineOutputs
	}
	second.ReadStarts = map[string]int64{}
	var start int64
	for _, st := range job.Streams {
		if t := c.readStarts[st.Name]; start == 0 || t < start {
			start = t
		}
	}
	starts := map[string]time.Time{}
	for _, o := range job.Outputs {
		second.ReadStarts[o.Name] = start
		starts[o.Name] = time.UnixMicro(start).UTC()
	}
	var staging Staging
	if es, ok := job.Destination.(EngineSink); ok {
		second.Sink = es.EngineSink()
	}
	if job.staged() {
		staging = job.Staging
		second.Sink = SinkPlan{StagedURL: staging.URL()}
	}
	sink := newSinkPump(c.w, a.sess.Generation(), first, second.Sink != SinkPlan{}, staging, job.Outputs, starts, lastEnd{ends: ends}, fromSecond, c.events)

	req := source.ReadRequest{Pipeline: job.Pipeline, Continuous: true}
	for _, st := range job.Streams {
		req.Streams = append(req.Streams, source.ConfiguredStream{Name: st.Name, SyncMode: st.Read, State: states[st.Name]})
	}
	g.Go(func() error {
		if err := job.Source.Read(gctx, req, c.pump); err != nil {
			return fmt.Errorf("runner: read: %w", err)
		}
		return c.pump.readDone()
	})
	g.Go(c.pump.run)
	g.Go(func() error { return s.prime(gctx, toFlow, in1) })
	g.Go(func() error { return job.Dataflow.Run(gctx, plan, in1, out1) })
	g.Go(func() error { return s.feed(gctx, out1) })
	g.Go(func() error { return job.Dataflow.Run(gctx, second, s.toSecond, fromSecond) })
	g.Go(func() error { return sink.run(gctx) })
	g.Go(func() error { return s.committer(gctx) })
	g.Go(func() error {
		defer close(s.tasks)
		err := s.loop(gctx)
		if err == nil {
			err = errors.New("runner: the segment ended")
		}
		s.setResult(err)
		return err
	})
	go func() {
		s.setResult(g.Wait())
		close(s.ended)
	}()
	return nil
}

func (s *flinkSegment) setResult(err error) {
	s.resultOnce.Do(func() { s.result = err })
}

// run waits for the segment to end, and says why.
func (s *flinkSegment) run(ctx context.Context) error {
	select {
	case <-s.ended:
	case <-ctx.Done():
		s.cancel(ctx.Err())
		<-s.ended
	}
	return s.result
}

// stop ends the segment with err, and waits for it.
func (s *flinkSegment) stop(err error) {
	s.setResult(err)
	s.cancel(err)
	<-s.ended
}

// open says whether the segment takes new calls.
func (s *flinkSegment) open() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.closing
}

// deployed tells the segment the job is running, as jobID.
func (s *flinkSegment) deployed(jobID string) {
	s.mu.Lock()
	s.deployedSchemas = decodeAll(s.a.schemasCopy())
	s.mu.Unlock()
	s.event(segEvent{kind: segDeployed, job: jobID, jobDone: s.a.jobDone})
}

func (a *flinkAttempt) schemasCopy() map[string][]byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	return maps.Clone(a.schemas)
}

// learnedSchemas are the schemas the first dataflow gave each stream so far.
func (s *flinkSegment) learnedSchemas() map[string][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.known)
}

func (s *flinkSegment) unlearned() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for name := range s.streams {
		if s.known[name] == nil {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

func (s *flinkSegment) event(ev segEvent) bool {
	select {
	case s.ev <- ev:
		return true
	case <-s.ended:
		return false
	case <-s.gctx.Done():
		return false
	}
}

// prime gives the first dataflow an empty batch of each stream when the
// segment learns schemas, so it says each declared stream's schema before
// any rows, then passes on the source's frames.
func (s *flinkSegment) prime(ctx context.Context, from <-chan Frame, to chan<- Frame) error {
	defer close(to)
	if s.learn {
		for _, st := range s.a.job.Streams {
			f := Frame{Kind: FrameRows, Stream: st.Name, Rows: []json.RawMessage{}}
			if sc := st.Declared(); sc != nil {
				b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
				f = Frame{Kind: FrameBatch, Stream: st.Name, Batch: b.NewRecordBatch()}
				b.Release()
			}
			if err := send(ctx, to, f); err != nil {
				return err
			}
		}
	}
	for {
		select {
		case f, ok := <-from:
			if !ok {
				return nil
			}
			if err := send(ctx, to, f); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// feed queues the first dataflow's output for the readers, and reports its
// epochs to the loop.
func (s *flinkSegment) feed(ctx context.Context, from <-chan Frame) error {
	for {
		var f Frame
		select {
		case fr, ok := <-from:
			if !ok {
				return nil
			}
			f = fr
		case <-ctx.Done():
			return ctx.Err()
		}
		switch f.Kind {
		case FrameSchema:
			if err := s.schema(f.Stream, f.Schema, nil); err != nil {
				return err
			}
		case FrameBatch:
			err := s.schema(f.Stream, nil, f.Batch.Schema())
			if err != nil || f.Batch.NumRows() == 0 {
				f.Batch.Release()
				if err != nil {
					return err
				}
				continue
			}
			b, err := arrowipc.EncodeBatch(f.Batch)
			f.Batch.Release()
			if err != nil {
				return fmt.Errorf("runner: encode a batch of %s for the Flink job: %w", f.Stream, err)
			}
			if err := s.push(ctx, f.Stream, readItem{batch: b}); err != nil {
				return err
			}
		case FrameBarrier:
			s.mu.Lock()
			c, ok := s.cutC[f.Epoch]
			s.mu.Unlock()
			if !ok {
				return fmt.Errorf("runner: the first dataflow sent barrier %d, which no checkpoint asked for", f.Epoch)
			}
			if err := s.push(ctx, f.Stream, readItem{barrier: c}); err != nil {
				return err
			}
			s.mu.Lock()
			if s.out[f.Epoch]++; s.out[f.Epoch] == len(s.streams) {
				delete(s.out, f.Epoch)
				s.inFlight--
				s.cond.Broadcast()
			}
			s.mu.Unlock()
		case FrameReady:
			if !s.event(segEvent{kind: segInputReady, epoch: f.Epoch, ready: f.Ready}) {
				return ctx.Err()
			}
		default:
			release(f)
			return fmt.Errorf("runner: the first dataflow sent a frame of kind %d", f.Kind)
		}
	}
}

// schema learns a stream's schema, from raw or sc, or checks it against the
// one the job was deployed with.
func (s *flinkSegment) schema(stream string, raw []byte, sc *arrow.Schema) error {
	var err error
	if sc == nil {
		if sc, err = decodeSchema(raw); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deployedSchemas == nil {
		if raw == nil {
			if raw, err = encodeSchema(sc); err != nil {
				return err
			}
		}
		learned := len(s.known) == len(s.streams)
		s.known[stream] = raw
		if !learned && len(s.known) == len(s.streams) {
			close(s.learned)
		}
		return nil
	}
	if d := s.deployedSchemas[stream]; d != nil && sameSchema(d, sc) {
		return nil
	}
	if raw == nil {
		if raw, err = encodeSchema(sc); err != nil {
			return err
		}
	}
	return &schemaChanged{stream: stream, schema: raw}
}

func encodeSchema(sc *arrow.Schema) ([]byte, error) {
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	rec := b.NewRecordBatch()
	defer rec.Release()
	return arrowipc.EncodeBatch(rec)
}

// push queues an item for a stream's reader. It waits while the queues are
// full, unless a barrier is on its way out of the first dataflow.
func (s *flinkSegment) push(ctx context.Context, stream string, it readItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.queued >= flinkQueueBytes && s.inFlight == 0 && !s.closing {
		s.cond.Wait()
	}
	if s.closing {
		return context.Cause(ctx)
	}
	q := s.queues[stream]
	q.items = append(q.items, it)
	s.queued += len(it.batch)
	s.cond.Broadcast()
	return nil
}

// serveRead streams a split's queue to its reader.
func (s *flinkSegment) serveRead(st grpc.ServerStreamingServer[bridgev1.ReadResponse], req *bridgev1.ReadRequest) error {
	ctx := st.Context()
	s.mu.Lock()
	q := s.queues[req.GetStream()]
	taken := q.reader
	q.reader = true
	s.mu.Unlock()
	if taken {
		s.event(segEvent{kind: segDropped, stream: req.GetStream(), err: errTaken})
		select {
		case <-s.ended:
		case <-ctx.Done():
			return ctx.Err()
		}
		return errTaken
	}
	s.event(segEvent{kind: segReader, stream: req.GetStream()})
	stopWaking := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer stopWaking()
	var err error
	defer func() { s.event(segEvent{kind: segDropped, stream: req.GetStream(), err: err}) }()
	for {
		s.mu.Lock()
		for len(q.items) == 0 && !s.closing && ctx.Err() == nil {
			s.cond.Wait()
		}
		if s.closing || ctx.Err() != nil {
			s.mu.Unlock()
			if err = ctx.Err(); err == nil {
				err = status.Error(codes.Unavailable, "runner: the segment ended")
			}
			return err
		}
		it := q.items[0]
		q.items = q.items[1:]
		s.queued -= len(it.batch)
		s.cond.Broadcast()
		s.mu.Unlock()
		res := &bridgev1.ReadResponse{}
		if it.batch != nil {
			res.Message = &bridgev1.ReadResponse_Batch{Batch: it.batch}
		} else {
			res.Message = &bridgev1.ReadResponse_Barrier{Barrier: &bridgev1.Barrier{Checkpoint: it.barrier}}
		}
		if err = st.Send(res); err != nil {
			return err
		}
	}
}

// serveWrite takes a sink subtask's batches into the second dataflow, and
// its barriers to the loop.
func (s *flinkSegment) serveWrite(st grpc.BidiStreamingServer[bridgev1.WriteRequest, bridgev1.WriteResponse], h *bridgev1.SinkHello) error {
	ctx := st.Context()
	key := sinkKey{output: h.GetOutput(), subtask: h.GetSubtask()}
	s.mu.Lock()
	taken := s.sinks[key]
	s.sinks[key] = true
	s.mu.Unlock()
	if taken {
		s.event(segEvent{kind: segDropped, sink: key, err: errTaken})
		select {
		case <-s.ended:
		case <-ctx.Done():
			return ctx.Err()
		}
		return errTaken
	}
	if !s.event(segEvent{kind: segSinkHello, sink: key, parallelism: h.GetParallelism()}) {
		return status.Error(codes.Unavailable, "runner: the segment ended")
	}
	var err error
	defer func() { s.event(segEvent{kind: segDropped, sink: key, err: err}) }()
	if err = st.Send(&bridgev1.WriteResponse{Ack: &bridgev1.Ack{}}); err != nil {
		return err
	}
	o := s.outputs[key.output]
	for {
		var req *bridgev1.WriteRequest
		if req, err = st.Recv(); err != nil {
			return err
		}
		switch m := req.GetMessage().(type) {
		case *bridgev1.WriteRequest_Batch:
			var b arrow.RecordBatch
			if b, err = arrowipc.DecodeBatch(m.Batch, memory.DefaultAllocator); err != nil {
				return status.Errorf(codes.InvalidArgument, "runner: a batch of %s: %v", key.output, err)
			}
			ob, oerr := outputBatch(b, o, s.changelog)
			b.Release()
			if oerr != nil {
				err = status.Error(codes.InvalidArgument, oerr.Error())
				return err
			}
			s.mu.Lock()
			s.outRows[key.output] += uint64(ob.NumRows()) //nolint:gosec // never negative
			s.mu.Unlock()
			if err = send(s.gctx, s.toSecond, Frame{Kind: FrameBatch, Stream: key.output, Batch: ob}); err != nil {
				return status.Error(codes.Unavailable, "runner: the segment ended")
			}
		case *bridgev1.WriteRequest_Barrier:
			reply := make(chan error, 1)
			if !s.event(segEvent{kind: segSinkBarrier, sink: key, checkpoint: m.Barrier.GetCheckpoint(), typ: m.Barrier.GetType(), reply: reply}) {
				err = status.Error(codes.Unavailable, "runner: the segment ended")
				return err
			}
			select {
			case rerr := <-reply:
				if rerr != nil {
					err = status.Error(codes.Aborted, rerr.Error())
					return err
				}
			case <-s.ended:
				err = status.Error(codes.Aborted, "runner: the segment ended")
				return err
			case <-ctx.Done():
				err = ctx.Err()
				return err
			}
			if err = st.Send(&bridgev1.WriteResponse{Ack: &bridgev1.Ack{Checkpoint: m.Barrier.GetCheckpoint()}}); err != nil {
				return err
			}
		default:
			err = status.Error(codes.InvalidArgument, "runner: a second hello")
			return err
		}
	}
}

// committer writes records and commits epochs, one at a time, in order.
func (s *flinkSegment) committer(ctx context.Context) error {
	for t := range s.tasks {
		if t.record {
			rec, err := s.c.record(ctx, t.n, t.st)
			if err != nil {
				return err
			}
			if !s.event(segEvent{kind: segRecorded, epoch: t.n, rec: rec}) {
				return ctx.Err()
			}
			continue
		}
		if err := s.c.apply(ctx, t.n, t.st, t.rec, t.start); err != nil {
			return err
		}
		if !s.event(segEvent{kind: segCommitted, epoch: t.n}) {
			return ctx.Err()
		}
		s.c.acknowledge(ctx, t.st, t.rec)
	}
	return nil
}

func (s *flinkSegment) loop(ctx context.Context) (err error) {
	defer func() {
		// Held barriers are refused, so the sinks' snapshots throw.
		cause := errors.New("runner: the segment ended")
		for _, cut := range s.cuts {
			refuse(cut.replies, cause)
		}
		for _, ep := range s.epochs {
			refuse(ep.replies, cause)
		}
		refuse(s.idleHeld, cause)
	}()
	var drain <-chan struct{}
	if !s.a.draining {
		drain = s.a.job.Drain
	}
	var jobDone chan error
	var connect <-chan time.Time
	for {
		select {
		case ev := <-s.c.events:
			err = s.pumped(ev)
		case ev := <-s.ev:
			if ev.kind == segDeployed {
				s.jobID, jobDone = ev.job, ev.jobDone
				if !s.learn {
					s.helloSeen = true
				}
				if s.helloSeen {
					connect = s.connectTimer()
				}
				if m := s.pendingHi; m != nil {
					s.pendingHi = nil
					err = s.coordinate(*m)
					if s.helloSeen && connect == nil {
						connect = s.connectTimer()
					}
				}
				break
			}
			err = s.handle(ev)
		case m := <-s.a.coord:
			had := s.helloSeen
			err = s.coordinate(m)
			if !had && s.helloSeen {
				connect = s.connectTimer()
			}
		case <-drain:
			drain = nil
			s.a.draining = true
			if s.jobID == "" {
				// Drained before the job was deployed.
				return ErrDrained
			}
			err = s.drain(ctx)
		case jerr := <-jobDone:
			jobDone <- jerr
			jobDone = nil
			if s.a.stopDir == "" || jerr != nil {
				return s.a.jobEnded(jerr)
			}
		case serr := <-s.a.stopDone:
			if serr != nil {
				return fmt.Errorf("runner: stop the Flink job with a savepoint: %w", serr)
			}
			s.stopped, s.a.stopped = true, true
			// The savepoint is complete, whether or not its outcome reached
			// the enumerator before the job finished.
			if n, ok := s.byC[s.stopCheckpoint()]; ok && n == s.lastN {
				s.commitReady = true
			}
		case <-connect:
			connect = nil
			if err := s.connected(); err != nil {
				return err
			}
		case <-ctx.Done():
			return context.Cause(ctx)
		}
		if err != nil {
			return err
		}
		if err := s.progress(ctx); err != nil {
			return err
		}
		if s.stopped && s.committedN == s.lastN && !s.recording && !s.committing {
			s.a.log.Info("drained with a savepoint", "epoch", s.lastN, "savepoint", s.a.stopDir)
			return ErrDrained
		}
	}
}

func refuse(replies []chan error, err error) {
	for _, r := range replies {
		select {
		case r <- err:
		default:
		}
	}
}

func (s *flinkSegment) connectTimer() <-chan time.Time {
	return time.After(cmp0(s.a.job.FlinkConnectTimeout, defaultFlinkConnectTimeout))
}

func cmp0(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

// connected fails a job that doesn't read every stream or write every
// output.
func (s *flinkSegment) connected() error {
	var missing []string
	for name := range s.streams {
		if !s.readers[name] {
			missing = append(missing, "stream "+name)
		}
	}
	for name := range s.outputs {
		if _, ok := s.parallelism[name]; !ok {
			missing = append(missing, "output "+name)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	slices.Sort(missing)
	return errs.New(errs.ConfigInvalid, "runner: the Flink job has run for %s without reading or writing %v; every stream needs openetl-flink's source and every output its sink", cmp0(s.a.job.FlinkConnectTimeout, defaultFlinkConnectTimeout), missing)
}

func (s *flinkSegment) stopCheckpoint() int64 {
	for c, n := range s.byC {
		if n == s.lastN {
			return c
		}
	}
	return -1
}

// drain stops the job with a savepoint, which ends the segment's last
// epoch.
func (s *flinkSegment) drain(ctx context.Context) error {
	a := s.a
	a.stopDir = a.job.Flink.SavepointDir(flinkSavepointName(a.sess.Generation()))
	a.stopDone = make(chan error, 1)
	fjob, dir, done := a.fjob, a.stopDir, a.stopDone
	a.log.Info("draining: stopping the Flink job with a savepoint", "dir", dir)
	go func() { done <- fjob.Stop(context.WithoutCancel(ctx), dir) }()
	return nil
}

// pumped handles the source pump's and the sink pump's events.
func (s *flinkSegment) pumped(ev event) error {
	switch ev.kind {
	case evPlaced:
		cut := s.byK[ev.epoch]
		if cut == nil || cut.placed {
			return fmt.Errorf("runner: unexpected placement of barrier %d", ev.epoch)
		}
		cut.placed, cut.states, cut.forced, cut.complete, cut.rows = true, ev.states, ev.forced, ev.complete, ev.rows
		s.placing = false
	case evStaged:
		ep := s.epochs[ev.epoch]
		if ep == nil || ep.staged {
			return fmt.Errorf("runner: unexpected staging of epoch %d", ev.epoch)
		}
		ep.staged, ep.desc = true, ev.staged
	case evReady:
		ep := s.epochs[ev.epoch]
		if ep == nil || ep.second != nil {
			return fmt.Errorf("runner: unexpected readiness of epoch %d", ev.epoch)
		}
		ep.second = ev.ready
	case evThreshold, evReadDone:
	default:
		return fmt.Errorf("runner: unexpected event %d", ev.kind)
	}
	return nil
}

func (s *flinkSegment) handle(ev segEvent) error {
	switch ev.kind {
	case segReader:
		s.readers[ev.stream] = true
	case segSinkHello:
		if p, ok := s.parallelism[ev.sink.output]; ok && p != ev.parallelism {
			return errs.New(errs.ConfigInvalid, "runner: output %q's sinks run with parallelism %d and %d", ev.sink.output, p, ev.parallelism)
		}
		s.parallelism[ev.sink.output] = ev.parallelism
	case segDropped:
		if s.a.stopDir != "" && !errors.Is(ev.err, errTaken) {
			return nil
		}
		what := "output " + ev.sink.output
		if ev.stream != "" {
			what = "stream " + ev.stream
		}
		s.a.log.Info("a Flink task's bridge call ended; the job is restarting", "call", what, "err", ev.err)
		return errRestarting
	case segInputReady:
		cut := s.byK[ev.epoch]
		if cut == nil || cut.ready != nil {
			return fmt.Errorf("runner: unexpected readiness of barrier %d in the first dataflow", ev.epoch)
		}
		cut.ready = ev.ready
	case segSinkBarrier:
		if s.aborted[ev.checkpoint] {
			ev.reply <- nil
			return nil
		}
		cut := s.cuts[ev.checkpoint]
		if cut == nil {
			ev.reply <- fmt.Errorf("runner: a barrier for checkpoint %d, which didn't start", ev.checkpoint)
			return nil
		}
		if cut.barriers > 0 && cut.typ != ev.typ {
			return fmt.Errorf("runner: checkpoint %d's barriers have types %v and %v", cut.c, cut.typ, ev.typ)
		}
		cut.barriers++
		cut.typ = ev.typ
		cut.replies = append(cut.replies, ev.reply)
	case segRecorded:
		ep := s.epochs[ev.epoch]
		s.recording = false
		s.headPrev, s.head, s.headSt, s.headCommitted = s.head, ev.rec, ep.st, false
		s.lastN = ev.epoch
		s.byC[ep.c] = ev.epoch
		delete(s.epochs, ev.epoch)
		refuse(ep.replies, nil)
		if s.stopped && ep.typ == bridgev1.BarrierType_BARRIER_TYPE_SYNC_SAVEPOINT {
			s.commitReady = true
		}
	case segCommitted:
		s.committing = false
		s.committedN, s.headCommitted = ev.epoch, true
		s.c.committed = ev.epoch
		s.a.res.Committed++
		s.a.res.Last = ev.epoch
		if s.a.job.OnCommit != nil {
			s.a.job.OnCommit(ev.epoch)
		}
	default:
		return fmt.Errorf("runner: unexpected segment event %d", ev.kind)
	}
	return nil
}

// coordinate handles an enumerator message, and answers it.
func (s *flinkSegment) coordinate(m coordMsg) error {
	if m.call != s.a.currentCall() {
		if m.reply != nil {
			m.reply <- errors.New("runner: a call of an enumerator that's gone")
		}
		return nil
	}
	if m.ended {
		if s.a.stopDir != "" {
			return nil
		}
		s.a.log.Info("the Flink job's enumerator disconnected; the job is restarting")
		return errRestarting
	}
	switch r := m.req.GetMessage().(type) {
	case *bridgev1.CoordinateRequest_Hello:
		switch {
		case s.jobID == "":
			s.pendingHi = &m
			return nil
		case s.helloSeen:
			// Restarted tasks: the attempt answers it once it has decided
			// where they restored.
			s.hello = &m
			return errRestarting
		case r.Hello.GetJobId() != s.jobID:
			m.reply <- errors.New("runner: another job's enumerator")
			return errs.New(errs.Internal, "runner: the enumerator of Flink job %s connected, and this attempt deployed %s", r.Hello.GetJobId(), s.jobID)
		}
		s.helloSeen = true
		m.reply <- nil
	case *bridgev1.CoordinateRequest_Started:
		if !s.helloSeen {
			m.reply <- errors.New("runner: a checkpoint before hello")
			return nil
		}
		c := r.Started.GetCheckpoint()
		if _, ok := s.cuts[c]; ok {
			m.reply <- fmt.Errorf("runner: checkpoint %d started twice", c)
			return nil
		}
		s.cuts[c] = &flinkCut{c: c}
		s.order = append(s.order, c)
		s.waiting = append(s.waiting, c)
		m.reply <- nil
	case *bridgev1.CoordinateRequest_Outcome:
		c := r.Outcome.GetCheckpoint()
		if !r.Outcome.GetCompleted() {
			if s.abortWhileStopping(c) {
				m.reply <- nil
				return nil
			}
			// Ending the call fails the job, and the restore decides C.
			m.reply <- fmt.Errorf("runner: checkpoint %d was aborted", c)
			s.a.log.Info("a Flink checkpoint was aborted; the job is restarting", "checkpoint", c)
			return errRestarting
		}
		if n, ok := s.byC[c]; ok {
			delete(s.byC, c)
			if n != s.lastN || s.committedN != n-1 {
				m.reply <- errors.New("runner: out of order")
				return fmt.Errorf("runner: checkpoint %d of epoch %d completed, and epoch %d is the last recorded, %d the last committed", c, n, s.lastN, s.committedN)
			}
			s.commitReady = true
		}
		m.reply <- nil
	default:
		m.reply <- errors.New("runner: an empty message")
	}
	return nil
}

// abortWhileStopping takes an abort that stopping with a savepoint caused,
// which the job carries on from, if nothing ended an epoch at C yet: its
// barriers are released, and its rows go into the next record.
func (s *flinkSegment) abortWhileStopping(c int64) bool {
	if s.a.stopDir == "" {
		return false
	}
	if _, ok := s.byC[c]; ok {
		return false
	}
	for _, ep := range s.epochs {
		if ep.c == c {
			return false
		}
	}
	s.aborted[c] = true
	if cut := s.cuts[c]; cut != nil {
		cut.aborted = true
		refuse(cut.replies, nil)
		cut.replies = nil
	}
	s.a.log.Info("a Flink checkpoint was aborted by the stop; its rows go into the next record", "checkpoint", c)
	return true
}

// progress requests the next barrier, decides checkpoints whose barriers
// have all arrived, records and commits what's ready, and releases idle
// checkpoints' barriers.
func (s *flinkSegment) progress(ctx context.Context) error {
	if !s.placing && len(s.waiting) > 0 {
		c := s.waiting[0]
		s.waiting = s.waiting[1:]
		cut := s.cuts[c]
		cut.k = s.nextK
		s.nextK++
		s.byK[cut.k] = cut
		s.mu.Lock()
		s.cutC[cut.k] = c
		s.inFlight++
		s.mu.Unlock()
		s.placing = true
		if err := s.c.pump.requestBarrier(cut.k); err != nil {
			return err
		}
	}
	for len(s.order) > 0 {
		cut := s.cuts[s.order[0]]
		if !cut.placed || cut.ready == nil || !cut.aborted && !s.allSinks(cut.barriers) {
			break
		}
		s.order = s.order[1:]
		delete(s.cuts, cut.c)
		delete(s.byK, cut.k)
		if err := s.decide(cut); err != nil {
			return err
		}
	}
	if s.recording || s.committing {
		return nil
	}
	if ep := s.epochs[s.lastN+1]; ep != nil && ep.staged && ep.second != nil && s.committedN == s.lastN {
		ep.st = s.epochState(ep)
		s.recording = true
		return s.task(ctx, flinkTask{record: true, n: ep.n, st: ep.st})
	}
	if s.committedN == s.lastN {
		refuse(s.idleHeld, nil)
		s.idleHeld = nil
		return nil
	}
	if s.commitReady {
		s.commitReady = false
		s.committing = true
		return s.task(ctx, flinkTask{n: s.lastN, st: s.headSt, rec: s.head, start: time.Now()})
	}
	return nil
}

func (s *flinkSegment) task(ctx context.Context, t flinkTask) error {
	select {
	case s.tasks <- t:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (s *flinkSegment) allSinks(barriers int) bool {
	if len(s.parallelism) < len(s.outputs) {
		return false
	}
	want := 0
	for _, p := range s.parallelism {
		want += int(p)
	}
	return barriers == want
}

// decide ends an epoch at a checkpoint whose every barrier has arrived. A
// savepoint taken by hand, or a checkpoint the stop aborted, ends none: what
// it covers goes into the next epoch. So does a checkpoint with no rows since the last record, whose
// barriers are released once that record's epoch is committed.
func (s *flinkSegment) decide(cut *flinkCut) error {
	if cut.aborted || cut.typ == bridgev1.BarrierType_BARRIER_TYPE_SAVEPOINT || cut.typ == bridgev1.BarrierType_BARRIER_TYPE_SYNC_SAVEPOINT && s.a.stopDir == "" {
		s.carry = append(s.carry, cut)
		refuse(cut.replies, nil)
		return nil
	}
	inputs := append(s.carry, cut)
	s.carry = nil
	s.mu.Lock()
	outRows := s.outRows
	s.outRows = map[string]uint64{}
	s.mu.Unlock()
	if cut.typ == bridgev1.BarrierType_BARRIER_TYPE_CHECKPOINT && s.idle(inputs, outRows) {
		s.idleHeld = append(s.idleHeld, cut.replies...)
		return nil
	}
	n := s.nextN
	s.nextN++
	s.epochs[n] = &flinkEpoch{n: n, c: cut.c, typ: cut.typ, inputs: inputs, outRows: outRows, replies: cut.replies, requested: time.Now()}
	frames := make([]Frame, 0, len(s.outputs))
	for _, o := range s.a.job.Outputs {
		frames = append(frames, Frame{Kind: FrameBarrier, Stream: o.Name, Epoch: n})
	}
	// Every sink is held at its barrier, so their batches are in already.
	s.g.Go(func() error {
		for _, f := range frames {
			if err := send(s.gctx, s.toSecond, f); err != nil {
				return err
			}
		}
		return nil
	})
	return nil
}

func (s *flinkSegment) idle(inputs []*flinkCut, outRows map[string]uint64) bool {
	for _, rows := range outRows {
		if rows > 0 {
			return false
		}
	}
	var last map[string]*arrow.Schema
	if s.head != nil {
		last = map[string]*arrow.Schema{}
		for name, st := range s.head.Streams {
			if st.Schema == nil {
				continue
			}
			if sc, err := decodeSchema(st.Schema); err == nil {
				last[name] = sc
			}
		}
	}
	for _, cut := range inputs {
		for _, rows := range cut.rows {
			if rows > 0 {
				return false
			}
		}
		if len(cut.ready.Quarantine) > 0 || len(cut.forced) > 0 || len(cut.complete) > 0 {
			return false
		}
		changed, err := changedSchemas(last, cut.ready.Schemas)
		if err != nil || len(changed) > 0 {
			return false
		}
	}
	return true
}

// epochState is what the record of ep holds: its last cut's resume points,
// every cut's rows and quarantine, and the second dataflow's report.
func (s *flinkSegment) epochState(ep *flinkEpoch) *epochState {
	st := &epochState{requested: ep.requested, placed: true, staged: true, ready: true,
		states: map[string][]byte{}, complete: map[string]bool{}, rows: map[string]uint64{}, desc: ep.desc}
	second := ep.second
	flow := &Ready{Staged: second.Staged, Files: second.Files, Snapshots: second.Snapshots, Schemas: map[string][]byte{},
		OutputSchemas: second.Schemas, OutputRows: ep.outRows}
	forced := map[string]bool{}
	for _, cut := range ep.inputs {
		maps.Copy(st.states, cut.states)
		maps.Copy(st.complete, cut.complete)
		for name, rows := range cut.rows {
			st.rows[name] += rows
		}
		for _, f := range cut.forced {
			forced[f] = true
		}
		maps.Copy(flow.Schemas, cut.ready.Schemas)
		flow.Quarantine = append(flow.Quarantine, cut.ready.Quarantine...)
	}
	for _, q := range second.Quarantine {
		q.Output = true
		flow.Quarantine = append(flow.Quarantine, q)
	}
	st.forced = slices.Sorted(maps.Keys(forced))
	st.flow = flow
	st.flink = &checkpoint.FlinkCheckpoint{Job: s.jobID, Checkpoint: ep.c, From: s.a.from}
	if ep.typ == bridgev1.BarrierType_BARRIER_TYPE_SYNC_SAVEPOINT {
		st.flink.Savepoint = s.a.stopDir
	}
	return st
}

// commitHead commits HEAD's epoch, if the segment recorded it and didn't
// commit it, under the generation that recorded it.
func (s *flinkSegment) commitHead(ctx context.Context) error {
	if s.headCommitted || s.headSt == nil {
		return nil
	}
	if err := s.c.apply(ctx, s.head.Epoch, s.headSt, s.head, time.Now()); err != nil {
		return err
	}
	s.headCommitted = true
	s.a.res.Committed++
	s.a.res.Last = s.head.Epoch
	if s.a.job.OnCommit != nil {
		s.a.job.OnCommit(s.head.Epoch)
	}
	s.c.acknowledge(ctx, s.headSt, s.head)
	return nil
}
