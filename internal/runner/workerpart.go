package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"golang.org/x/sync/errgroup"

	attemptv1 "github.com/openetl-io/openetl/gen/openetl/attempt/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/ints"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/runner/attempt"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// errFinished: the coordinator ended the attempt as finished.
var errFinished = errors.New("runner: the attempt finished")

// RunWorker runs a worker's part of an attempt another runner coordinates
// (Job.Workers). It joins at t, and answers the coordinator's calls: the
// first worker settles the restored epoch, splits the reads that start, and
// applies schemas and commits for every worker. Once assigned, it reads its
// splits, runs its lanes, writes its writer lanes with its own destination
// writers, sends its share of each epoch once its part is durable, commits
// at the coordinator's word, and acknowledges to its source only the
// positions of records the coordinator sends. It returns nil once the
// coordinator ends the attempt as finished. The job is the coordinator's,
// whose Log, Sync, Paused, Resets, Rewind, KeepRecords, Drain, Workers and
// On callbacks it doesn't use.
func RunWorker(ctx context.Context, job Job, t attempt.Target, opts attempt.Options) error {
	if err := job.validateRun(false); err != nil {
		return err
	}
	if err := job.OnWorkers(1); err != nil {
		return err
	}
	log := job.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log = log.With("pipeline", job.Pipeline, "owner", job.Owner, "generation", t.Generation, "worker", t.Worker)
	w, err := attempt.Join(ctx, t, opts)
	if err != nil {
		return err
	}
	p := &workerPart{job: &job, t: t, w: w, log: log, mine: map[checkpoint.SplitRef]bool{}, schemas: newSchemaRelay(w)}
	defer p.schemas.close()
	err = p.run(ctx)
	if errors.Is(err, errFinished) {
		err = nil
	}
	if err != nil {
		w.Fail(err)
	}
	_ = w.Close()
	return err
}

type workerPart struct {
	job *Job
	t   attempt.Target
	w   attempt.Worker
	log *slog.Logger
	// The writer of the attempt's first writer lane, which settles, applies
	// schemas and commits; the first worker's, once it settled. In ws once
	// the worker is assigned.
	first   destination.Writer
	firstWs bool
	// By the worker's writer lane: its writer, and its index in the
	// attempt.
	ws     []destination.Writer
	global []int
	// The splits the worker reads, the assignment's and those its source
	// added, and the streams no worker reads.
	mu      sync.Mutex
	mine    map[checkpoint.SplitRef]bool
	unread  map[string]bool
	schemas *schemaRelay
}

func (p *workerPart) run(ctx context.Context) error {
	defer func() {
		for i, w := range p.ws {
			if err := w.Close(context.WithoutCancel(ctx)); err != nil {
				p.log.Warn("closing a destination lane failed", "lane", p.global[i], "err", err)
			}
		}
		if p.first != nil && !p.firstWs {
			_ = p.first.Close(context.WithoutCancel(ctx))
		}
	}()
	for {
		var req *attemptv1.AttendResponse
		select {
		case r, ok := <-p.w.Requests():
			if !ok {
				return p.over()
			}
			req = r
		case <-ctx.Done():
			return ctx.Err()
		}
		switch x := req.GetMessage().(type) {
		case *attemptv1.AttendResponse_Call:
			if err := p.call(ctx, x.Call); err != nil {
				return err
			}
		case *attemptv1.AttendResponse_Commit:
			if err := p.commit(ctx, x.Commit); err != nil {
				return err
			}
		case *attemptv1.AttendResponse_Assignment:
			return p.attend(ctx, x.Assignment)
		default:
			return errs.New(errs.Internal, "runner: the coordinator sent %T before the assignment", req.GetMessage())
		}
	}
}

// over is why the coordinator ended the worker's part: errFinished if the
// attempt finished.
func (p *workerPart) over() error {
	if err := p.w.Err(); err != nil {
		return err
	}
	return errFinished
}

// call answers a call.
func (p *workerPart) call(ctx context.Context, c *attemptv1.Call) error {
	ans := &attemptv1.Answer{Id: c.GetId()}
	var err error
	switch x := c.GetCall().(type) {
	case *attemptv1.Call_Recover:
		var r destination.Recovery
		r, err = p.recover(ctx, x.Recover)
		ans.Answer = &attemptv1.Answer_Recovered{Recovered: &protocolv1.WriteRecovered{Recovery: ints.Enum[protocolv1.Recovery](r)}}
	case *attemptv1.Call_SplitRead:
		var s *attemptv1.ReadSplits
		s, err = p.splitRead(ctx, x.SplitRead)
		ans.Answer = &attemptv1.Answer_ReadSplits{ReadSplits: s}
	case *attemptv1.Call_ApplySchema:
		var a destination.SchemaApplied
		a, err = p.applySchema(ctx, x.ApplySchema)
		ans.Answer = &attemptv1.Answer_SchemaApplied{SchemaApplied: schemaAppliedToProto(a)}
	default:
		err = errs.New(errs.Internal, "runner: a call of %T", c.GetCall())
	}
	if err != nil {
		ans.Answer, ans.Error = nil, answerOf(err)
	}
	return p.w.Answer(ans)
}

// recover opens the writer of the attempt's first writer lane, raises the
// destination's fence and settles the epoch of the record HEAD points to,
// as a single runner's lane 0 does.
func (p *workerPart) recover(ctx context.Context, r *attemptv1.Recover) (destination.Recovery, error) {
	if p.first != nil {
		return 0, errs.New(errs.Internal, "runner: asked to recover again")
	}
	forced := map[string]bool{}
	for _, s := range r.GetForced() {
		forced[s] = true
	}
	w, err := p.job.Destination.Open(ctx, p.job.openConfig(p.t.Generation, forced, 0))
	if err != nil {
		return 0, fmt.Errorf("runner: open destination: %w", err)
	}
	p.first = w
	var e destination.Epoch
	var staged []destination.Staged
	if len(r.GetRecord()) > 0 {
		var rec checkpoint.Record
		if err := json.Unmarshal(r.GetRecord(), &rec); err != nil {
			return 0, errs.Wrap(errs.Internal, err, "runner: the record to recover")
		}
		e, staged = destination.Epoch{Generation: rec.Generation, N: rec.Epoch}, committables(&rec)
		// A redone epoch needs its schemas in place, and can't change.
		a, err := applySchemas(ctx, w, map[string]*arrow.Schema{}, restoredSchemas(&rec))
		if err == nil && len(a.Refused) > 0 {
			err = errs.Wrap(errs.SchemaDrift, &RefusedError{Columns: []RefusedColumn{{RefusedColumn: a.Refused[0]}}}, "runner: recovering a recorded epoch")
		}
		if err != nil {
			return 0, err
		}
	}
	rec, err := w.Recover(ctx, p.t.Generation, e, staged)
	if err != nil {
		return 0, fmt.Errorf("runner: recover epoch %d: %w", e.N, err)
	}
	return rec, nil
}

// splitRead splits the reads that start.
func (p *workerPart) splitRead(ctx context.Context, r *attemptv1.SplitRead) (*attemptv1.ReadSplits, error) {
	out := &attemptv1.ReadSplits{Splits: map[string]*protocolv1.Splits{}, Resplits: p.job.canResplit(ctx)}
	for _, er := range r.GetStreams() {
		i := slices.IndexFunc(p.job.Streams, func(s Stream) bool { return s.Name == er.GetStream() })
		if i < 0 {
			return nil, errs.New(errs.Internal, "runner: splits of stream %q, which the job doesn't read", er.GetStream())
		}
		var ended []source.Split
		if er.Ended != nil {
			ended = make([]source.Split, 0, len(er.GetEnded().GetSplits()))
			for _, sp := range er.GetEnded().GetSplits() {
				ended = append(ended, source.Split{ID: sp.GetId(), State: sp.GetState()})
			}
		}
		splits, err := p.job.newSplits(ctx, p.job.Streams[i], ended)
		if err != nil {
			return nil, err
		}
		ss := &protocolv1.Splits{}
		for _, sp := range splits {
			ss.Splits = append(ss.Splits, &protocolv1.Split{Id: sp.ID, State: sp.State})
		}
		out.Splits[er.GetStream()] = ss
	}
	return out, nil
}

func (p *workerPart) applySchema(ctx context.Context, m *protocolv1.WriteApplySchema) (destination.SchemaApplied, error) {
	if p.first == nil {
		return destination.SchemaApplied{}, errs.New(errs.Internal, "runner: worker %d hasn't the attempt's first writer lane, which applies schemas", p.t.Worker)
	}
	sa, ok := p.first.(destination.SchemaApplier)
	if !ok {
		return destination.SchemaApplied{}, nil
	}
	streams := make([]destination.StreamSchema, 0, len(m.GetStreams()))
	for _, s := range m.GetStreams() {
		schema, err := arrowipc.DecodeSchema(s.GetArrowSchema())
		if err != nil || schema == nil {
			return destination.SchemaApplied{}, errs.New(errs.Internal, "runner: schema for %s: %v", s.GetName(), err)
		}
		streams = append(streams, destination.StreamSchema{Name: s.GetName(), Schema: schema})
	}
	a, err := sa.ApplySchema(ctx, streams)
	if err != nil {
		return a, fmt.Errorf("apply schemas: %w", err)
	}
	return a, nil
}

// commit commits what a record names, on the attempt's first writer lane,
// and answers once it has, or failed.
func (p *workerPart) commit(ctx context.Context, req *attemptv1.Commit) error {
	c := req.GetCommit()
	answer := &attemptv1.Committed{Committed: &protocolv1.WriteCommitted{Epoch: c.GetEpoch()}, WriterLanes: req.GetWriterLanes()}
	var err error
	switch {
	case p.first == nil:
		err = errs.New(errs.Internal, "runner: worker %d hasn't the attempt's first writer lane, which commits", p.t.Worker)
	case len(req.GetWriterLanes()) > 0 && len(req.GetWriterLanes()) != len(c.GetStaged()):
		err = errs.New(errs.Internal, "runner: a commit of %d of %d writer lanes, which this destination doesn't take", len(req.GetWriterLanes()), len(c.GetStaged()))
	default:
		staged := make([]destination.Staged, len(c.GetStaged()))
		for i, s := range c.GetStaged() {
			staged[i] = s
		}
		var done destination.Committed
		done, err = p.first.Commit(ctx, destination.Epoch{Generation: c.GetEpoch().GetGeneration(), N: c.GetEpoch().GetN()}, staged)
		answer.Committed.CollapsedRows = done.Collapsed
	}
	if err != nil {
		p.log.Warn("committing failed", "epoch", c.GetEpoch().GetN(), "err", err)
		answer.Committed.Error = answerOf(err)
	}
	return p.w.Committed(answer)
}

// acknowledge acknowledges to the source the positions of a committed
// record of the splits the worker reads; the first worker's also of the
// streams no worker reads.
func (p *workerPart) acknowledge(ctx context.Context, pos *attemptv1.Positions) {
	ack, ok := p.job.Source.(source.Acknowledger)
	if !ok {
		return
	}
	splits := map[string][]checkpoint.SplitState{}
	p.mu.Lock()
	for _, sp := range pos.GetSplits() {
		ref := checkpoint.SplitRef{Stream: sp.GetStream(), ID: sp.GetSplit().GetId()}
		if p.mine[ref] || p.t.Worker == 0 && p.unread[ref.Stream] {
			splits[ref.Stream] = append(splits[ref.Stream], splitStateOf(sp))
		}
	}
	p.mu.Unlock()
	if len(splits) == 0 {
		return
	}
	states, split := acknowledged(splits)
	if err := ack.Committed(ctx, states, split); err != nil {
		p.log.Warn("upstream acknowledgement failed; the next commit retries it", "epoch", pos.GetEpoch().GetN(), "err", err)
	}
}

// attend runs the worker's part of the attempt as assigned.
func (p *workerPart) attend(ctx context.Context, a *attemptv1.Assignment) error {
	job := *p.job
	var restored *checkpoint.Record
	if len(a.GetRestored()) > 0 {
		restored = &checkpoint.Record{}
		if err := json.Unmarshal(a.GetRestored(), restored); err != nil {
			return errs.Wrap(errs.Internal, err, "runner: the assignment's record")
		}
	}
	me := ints.Uint32(p.t.Worker)
	firstLane, lanes := -1, map[uint32]bool{}
	for i, l := range a.GetLanes() {
		if l.GetWorker() == me {
			if firstLane < 0 {
				firstLane = i
			}
			lanes[ints.Uint32(i)] = true
		}
	}
	for w, l := range a.GetWriterLanes() {
		if l.GetWorker() == me {
			p.global = append(p.global, w)
		}
	}
	if len(lanes) == 0 || len(p.global) == 0 || p.t.Worker == 0 && p.global[0] != 0 {
		return errs.New(errs.Internal, "runner: worker %d's assignment has %d lanes and writer lanes %v", p.t.Worker, len(lanes), p.global)
	}
	splits := map[string][]checkpoint.SplitState{}
	readers := map[string]map[uint32]bool{}
	for _, s := range a.GetSplits() {
		name := s.GetSplit().GetStream()
		if readers[name] == nil {
			readers[name] = map[uint32]bool{}
		}
		readers[name][a.GetLanes()[s.GetLane()].GetWorker()] = true
		if lanes[s.GetLane()] {
			splits[name] = append(splits[name], splitStateOf(s.GetSplit()))
			p.mine[checkpoint.SplitRef{Stream: name, ID: s.GetSplit().GetSplit().GetId()}] = true
		}
	}
	// A stream read by several workers ends its read on none of them, as
	// one worker can't know that the others' splits have.
	spread := map[string]bool{}
	for name, ws := range readers {
		spread[name] = len(ws) > 1
	}
	done, forced := map[string]bool{}, map[string]bool{}
	for _, s := range a.GetDone() {
		done[s] = true
	}
	p.unread = done
	for _, s := range a.GetForced() {
		forced[s] = true
	}
	job.Lanes, job.writers = len(lanes), len(p.global)
	gen := p.t.Generation
	for _, gw := range p.global {
		if gw == 0 && p.first != nil {
			p.ws, p.firstWs = append(p.ws, p.first), true
			continue
		}
		w, err := job.Destination.Open(ctx, job.openConfig(gen, forced, gw))
		if err != nil {
			return fmt.Errorf("runner: open destination lane %d: %w", gw, err)
		}
		p.ws = append(p.ws, w)
	}

	readStarts, schemas := maps.Clone(a.GetReadStarts()), map[string][]byte{}
	if readStarts == nil {
		readStarts = map[string]int64{}
	}
	if restored != nil {
		for name, st := range restored.Streams {
			if st.Schema != nil {
				schemas[name] = st.Schema
			}
			if _, ok := readStarts[name]; !ok {
				readStarts[name] = st.ReadStart
			}
		}
	}
	plan := job.plan(gen, a.GetFirstEpoch(), restored, readStarts, schemas, nil, nil)
	plan.SchemaOwner, plan.FirstLane = p.schemas, firstLane

	g, gctx := errgroup.WithContext(ctx)
	events := make(chan event, 64)
	toFlow, fromFlow := make(chan Frame, 16), make(chan Frame, 16)
	ends := newReadEnds()
	pump := newSourcePump(gctx, &job, a.GetFirstEpoch()-1, splits, job.canForce(), done, ends, toFlow, events)
	pump.worker, pump.splitLanes = true, map[checkpoint.SplitRef]int{}
	pump.maxBytes.Store(clampBytes(a.GetMaxBytes()))
	starts, sinkEnds := job.sinkStarts(readStarts, done, ends)
	sinkEnds = spreadEnds{readEnder: sinkEnds, spread: spread}
	outs := make([]chan Frame, len(p.ws))
	for k, w := range p.ws {
		outs[k] = make(chan Frame, 16)
		sink := newSinkPump(w, k, gen, a.GetFirstEpoch(), plan.Sink != SinkPlan{}, nil, job.sinkStreams(), starts, sinkEnds, outs[k], events)
		g.Go(func() error { return sink.run(gctx) })
	}
	// Only the streams with splits here: another worker reads the rest.
	req := job.readRequest(splits, done)
	req.Streams = slices.DeleteFunc(req.Streams, func(cs source.ConfiguredStream) bool { return splits[cs.Name] == nil })
	g.Go(func() error {
		if len(req.Streams) > 0 {
			if err := job.Source.Read(gctx, req, pump); err != nil {
				return fmt.Errorf("runner: read: %w", err)
			}
		}
		return pump.readDone()
	})
	g.Go(pump.run)
	g.Go(func() error { return job.Dataflow.Run(gctx, plan, toFlow, fromFlow) })
	g.Go(func() error { return demux(gctx, fromFlow, outs) })
	g.Go(func() error {
		p.reportUsage(gctx, job.Meter)
		return nil
	})
	bg := newFifo[*attemptv1.AttendResponse]()
	bgDone := make(chan struct{})
	g.Go(func() error {
		defer close(bgDone)
		return p.background(gctx, bg)
	})
	g.Go(func() error {
		err := p.loop(gctx, a, plan, pump, events, bg)
		bg.close()
		if errors.Is(err, errFinished) {
			// What the coordinator sent before the end is done first.
			select {
			case <-bgDone:
			case <-gctx.Done():
			}
		}
		return err
	})
	return g.Wait()
}

// usageEvery is how often a worker tells its coordinator what it has used
// and moved, for the coordinator's reports.
const usageEvery = 5 * time.Second

// reportUsage tells the coordinator what m says, now and then, until ctx
// ends.
func (p *workerPart) reportUsage(ctx context.Context, m *resources.Meter) {
	if m == nil {
		return
	}
	t := time.NewTicker(usageEvery)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-ctx.Done():
			return
		}
		if err := p.w.Report(usageOf(m)); err != nil {
			return
		}
	}
}

// clampBytes is a byte threshold as the pump keeps it.
func clampBytes(b uint64) int64 {
	if b > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(b)
}

// spreadEnds ends no read of a stream several workers read.
type spreadEnds struct {
	readEnder
	spread map[string]bool
}

func (s spreadEnds) endsAt(stream string, epoch uint64) bool {
	return !s.spread[stream] && s.readEnder.endsAt(stream, epoch)
}

// background commits, acknowledges and answers calls, in the order the
// coordinator asked, until q is closed and empty.
func (p *workerPart) background(ctx context.Context, q *fifo[*attemptv1.AttendResponse]) error {
	for {
		req, ok := q.next(ctx)
		if !ok {
			return nil
		}
		var err error
		switch x := req.GetMessage().(type) {
		case *attemptv1.AttendResponse_Commit:
			err = p.commit(ctx, x.Commit)
		case *attemptv1.AttendResponse_Positions:
			p.acknowledge(ctx, x.Positions)
		case *attemptv1.AttendResponse_Call:
			err = p.call(ctx, x.Call)
		}
		if err != nil {
			return err
		}
	}
}

// workerEpoch is the worker's part of an epoch as it comes together.
type workerEpoch struct {
	placed     bool
	splits     map[string][]checkpoint.SplitState
	rows       map[string]uint64
	splitLanes map[checkpoint.SplitRef]int
	descs      []destination.Staged
	stagedOn   []bool
	staged     int
	ready      *Ready
}

// loop takes the coordinator's barriers and the pumps' events, sends the
// worker's signals and shares, and queues the rest for the background.
func (p *workerPart) loop(ctx context.Context, a *attemptv1.Assignment, plan Plan, pump *sourcePump, events <-chan event, bg *fifo[*attemptv1.AttendResponse]) error {
	gen, next := p.t.Generation, a.GetFirstEpoch()
	last := next - 1
	epochs := map[uint64]*workerEpoch{}
	signal := func(after uint64, s *attemptv1.Signal) error {
		s.After = after
		return p.w.Signal(s)
	}
	for {
		select {
		case req, ok := <-p.w.Requests():
			if !ok {
				return p.over()
			}
			switch x := req.GetMessage().(type) {
			case *attemptv1.AttendResponse_Barrier:
				n := x.Barrier.GetEpoch().GetN()
				if b := x.Barrier.GetMaxBytes(); b > 0 {
					pump.maxBytes.Store(clampBytes(b))
				}
				epochs[n] = &workerEpoch{descs: make([]destination.Staged, len(p.ws)), stagedOn: make([]bool, len(p.ws))}
				last = n
				if err := pump.requestBarrier(n); err != nil {
					return err
				}
			case *attemptv1.AttendResponse_Schema:
				p.schemas.update(x.Schema)
			case *attemptv1.AttendResponse_Commit, *attemptv1.AttendResponse_Positions, *attemptv1.AttendResponse_Call:
				bg.push(req)
			default:
				return errs.New(errs.Internal, "runner: the coordinator sent %T", req.GetMessage())
			}
			continue
		case ev := <-events:
			e := epochs[ev.epoch]
			var err error
			switch ev.kind {
			case evPlaced:
				if e == nil || e.placed {
					return fmt.Errorf("runner: unexpected placement of epoch %d", ev.epoch)
				}
				e.placed, e.splits, e.rows, e.splitLanes = true, ev.splits, ev.rows, ev.splitLanes
				p.mu.Lock()
				for name, ss := range ev.splits {
					for _, sp := range ss {
						p.mine[checkpoint.SplitRef{Stream: name, ID: sp.ID}] = true
					}
				}
				p.mu.Unlock()
				err = signal(ev.epoch, &attemptv1.Signal{Signal: &attemptv1.Signal_Placed{Placed: &attemptv1.Placed{}}})
			case evStaged:
				if e == nil || ev.lane >= len(e.stagedOn) || e.stagedOn[ev.lane] {
					return fmt.Errorf("runner: unexpected staging of epoch %d on lane %d", ev.epoch, ev.lane)
				}
				e.descs[ev.lane], e.stagedOn[ev.lane] = ev.staged, true
				e.staged++
			case evReady:
				if e == nil || e.ready != nil {
					return fmt.Errorf("runner: unexpected readiness of epoch %d", ev.epoch)
				}
				e.ready = ev.ready
			case evThreshold:
				err = signal(last, &attemptv1.Signal{Signal: &attemptv1.Signal_Full{Full: &attemptv1.Full{}}})
			case evEmitted:
				err = signal(ev.epoch, &attemptv1.Signal{Signal: &attemptv1.Signal_Emitted{Emitted: &attemptv1.Emitted{}}})
			case evReadDone:
				err = signal(ev.epoch, &attemptv1.Signal{Signal: &attemptv1.Signal_ReadDone{ReadDone: &attemptv1.ReadDone{Active: ev.active}}})
			}
			if err != nil {
				return err
			}
			// Shares go in epoch order, once the worker's part is durable:
			// placed, pre-committed on every writer lane, and ready.
			for e := epochs[next]; e != nil && e.placed && e.staged == len(p.ws) && e.ready != nil; e = epochs[next] {
				if m := p.job.Meter; m != nil {
					if err := p.w.Report(usageOf(m)); err != nil {
						return err
					}
				}
				if err := p.w.Share(p.share(gen, next, e, plan)); err != nil {
					return err
				}
				delete(epochs, next)
				next++
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// share is the worker's share of epoch n.
func (p *workerPart) share(gen, n uint64, e *workerEpoch, plan Plan) *attemptv1.Share {
	epoch := &protocolv1.Epoch{Generation: gen, N: n}
	s := &attemptv1.Share{Epoch: epoch, Rows: e.rows, Ready: attempt.Ready{Schemas: e.ready.Schemas, Snapshots: e.ready.Snapshots, Quarantine: e.ready.Quarantine}.EpochReady(epoch)}
	for _, name := range slices.Sorted(maps.Keys(e.splits)) {
		for _, sp := range e.splits[name] {
			s.Positions = append(s.Positions, positionOf(name, sp))
		}
	}
	refs := slices.SortedFunc(maps.Keys(e.splitLanes), func(a, b checkpoint.SplitRef) int {
		return cmpRefs(a, b)
	})
	writers := len(p.ws)
	for k, gw := range p.global {
		c := &attemptv1.Committable{WriterLane: ints.Uint32(gw), Staged: e.descs[k]}
		for _, ref := range refs {
			lane := e.splitLanes[ref]
			r := &attemptv1.SplitRef{Stream: ref.Stream, Split: ref.ID}
			switch {
			case plan.KeyGrouped[ref.Stream]:
				first, last := writerKeyGroups(plan, k)
				r.KeyGroups = attempt.KeyGroups(first, last)
			case lane == laneAll:
			case lane < 0 || lane%writers != k:
				continue
			}
			c.Splits = append(c.Splits, r)
		}
		s.Committables = append(s.Committables, c)
	}
	return s
}

func cmpRefs(a, b checkpoint.SplitRef) int {
	if c := strings.Compare(a.Stream, b.Stream); c != 0 {
		return c
	}
	return strings.Compare(a.ID, b.ID)
}

// writerKeyGroups are the key groups writer lane w of plan owns.
func writerKeyGroups(plan Plan, w int) (first, last int) {
	groups, writers := max(plan.KeyGroups, 1), plan.writerLanes()
	if plan.KeyGroups == 0 {
		groups = checkpoint.KeyGroups
	}
	return (w*groups + writers - 1) / writers, ((w+1)*groups+writers-1)/writers - 1
}

// schemaRelay is a worker's way to its streams' schema owner, the
// coordinator.
type schemaRelay struct {
	w       attempt.Worker
	updates *fifo[SchemaUpdate]
	out     chan SchemaUpdate
	once    sync.Once
	stop    context.CancelFunc
	stopped context.Context
}

func newSchemaRelay(w attempt.Worker) *schemaRelay {
	ctx, stop := context.WithCancel(context.Background())
	return &schemaRelay{w: w, updates: newFifo[SchemaUpdate](), out: make(chan SchemaUpdate), stop: stop, stopped: ctx}
}

func (r *schemaRelay) Propose(stream string, base uint64, schema []byte) error {
	return r.w.Propose(&attemptv1.SchemaProposal{Stream: stream, Base: base, ArrowSchema: schema})
}

func (r *schemaRelay) Updates() <-chan SchemaUpdate {
	r.once.Do(func() {
		go func() {
			for {
				u, ok := r.updates.next(r.stopped)
				if !ok {
					return
				}
				select {
				case r.out <- u:
				case <-r.stopped.Done():
					return
				}
			}
		}()
	})
	return r.out
}

func (r *schemaRelay) update(s *attemptv1.StreamSchema) {
	r.updates.push(SchemaUpdate{Stream: s.GetStream(), Version: s.GetVersion(), Schema: s.GetArrowSchema()})
}

func (r *schemaRelay) close() { r.stop() }

// fifo is an unbounded queue with one reader.
type fifo[T any] struct {
	mu     sync.Mutex
	items  []T
	closed bool
	wake   chan struct{}
}

func newFifo[T any]() *fifo[T] { return &fifo[T]{wake: make(chan struct{}, 1)} }

func (q *fifo[T]) push(v T) {
	q.mu.Lock()
	q.items = append(q.items, v)
	q.mu.Unlock()
	q.poke()
}

func (q *fifo[T]) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.poke()
}

func (q *fifo[T]) poke() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// next waits for the next item; false once the queue is closed and empty,
// or ctx is done.
func (q *fifo[T]) next(ctx context.Context) (T, bool) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			v := q.items[0]
			q.items = q.items[1:]
			q.mu.Unlock()
			return v, true
		}
		closed := q.closed
		q.mu.Unlock()
		var zero T
		if closed {
			return zero, false
		}
		select {
		case <-q.wake:
		case <-ctx.Done():
			return zero, false
		}
	}
}
