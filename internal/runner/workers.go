package runner

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	attemptv1 "github.com/openetl-io/openetl/gen/openetl/attempt/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/ints"
	"github.com/openetl-io/openetl/internal/runner/attempt"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// Workers runs an attempt on workers, each a runner of its own, that the
// attempt's runner coordinates. The coordinator claims the pipeline, lays
// the attempt out, triggers barriers, merges the workers' shares of each
// epoch, writes the record, and says when to commit. It runs no lanes and
// no connector, and holds no destination credentials: it calls on the
// source's and the destination's capabilities alone, and workers settle,
// split reads, apply schemas and commit at its word. Each stream's schema
// has one owner, the coordinator, which every worker's engine asks.
type Workers struct {
	// How many; at least one.
	Count int
	// Where the workers reach the coordinator.
	Server  *attempt.Server
	Options attempt.Options
	// Starts a worker for each target, each running RunWorker with the job,
	// and returns once they're started. Called once the attempt is claimed.
	Launch func(ctx context.Context, targets []attempt.Target) error
}

// endTimeout bounds how long ending an attempt waits for its workers to
// close their calls.
const endTimeout = 30 * time.Second

// OnWorkers says why the job can't run on workers; nil if it can. What
// needs rows of one key to meet across workers, or one writer, stays on one
// pod.
func (j *Job) OnWorkers(workers int) error {
	caps := j.Destination.Capabilities()
	_, engineSink := j.Destination.(EngineSink)
	switch {
	case workers < 1:
		return errs.New(errs.ConfigInvalid, "runner: %d workers", workers)
	case j.ownCode():
		return errs.New(errs.ConfigInvalid, "runner: a Spark or Flink transform runs on one pod")
	case j.SQL != nil:
		return errs.New(errs.ConfigInvalid, "runner: a SQL transform runs on one pod")
	case caps.Lanes() < workers:
		return errs.New(errs.ConfigInvalid, "runner: the destination takes %d writer lanes, fewer than %d workers", caps.Lanes(), workers)
	case caps.Class == destination.TransactionalStream:
		return errs.New(errs.ConfigInvalid, "runner: a transactional destination's writer lanes commit on their own, which only one pod recovers")
	case j.staged():
		return errs.New(errs.ConfigInvalid, "runner: a destination that loads staged epochs runs on one pod")
	case engineSink:
		return errs.New(errs.ConfigInvalid, "runner: a destination the engine writes runs on one pod")
	}
	for _, s := range j.Streams {
		if j.holdsOn(s) {
			return errs.New(errs.ConfigInvalid, "runner: stream %q holds columns, which runs on one pod", s.Name)
		}
	}
	return nil
}

// fleet is a coordinator's workers.
type fleet struct {
	job *Job
	co  attempt.Coordinator
	n   int
	// The attempt's writer lanes, and each split's place in the
	// assignment, which the record keeps.
	writers int
	order   map[checkpoint.SplitRef]int
	// The bytes an epoch reads before the coordinator asks for a barrier,
	// each worker's share of it its threshold.
	maxBytes atomic.Int64
	schemas  *schemaOwner
	events   chan event
	// Barriers requested, for the gatherer, in order.
	requested chan uint64
	// Owned by the loop.
	emitted bool
	// By worker: its read ended, and it emitted after the last barrier
	// it was asked for before.
	done, act []bool
	// Owned by the inbox: workers that placed each barrier.
	placed map[uint64]int
	// Owned by the committer: the record whose positions the workers get
	// next.
	acking *checkpoint.RecordRef
	log    *slog.Logger
}

func (f *fleet) perWorker() uint64 {
	b := f.maxBytes.Load()
	if b <= 0 {
		return 0
	}
	n := int64(f.n)
	return ints.Uint64((b + n - 1) / n)
}

func (f *fleet) barrier(ctx context.Context, n uint64) error {
	f.emitted = false
	if err := f.co.Barrier(n, f.perWorker()); err != nil {
		return err
	}
	select {
	case f.requested <- n:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// coverReadEnds is a barrier requested: it's placed after every ended
// read's last emit.
func (f *fleet) coverReadEnds() {
	for i, d := range f.done {
		f.act[i] = f.act[i] && !d
	}
}

func (f *fleet) active() bool { return slices.Contains(f.act, true) }

// readEnded records a worker's read ended, and says if every worker's has.
func (f *fleet) readEnded(worker int, active bool) bool {
	f.done[worker], f.act[worker] = true, active
	return !slices.Contains(f.done, false)
}

// acknowledge sends the workers the positions of the record before rec,
// states, and takes rec's next.
func (f *fleet) acknowledge(states map[string][]checkpoint.SplitState, rec *checkpoint.Record) error {
	at := f.acking
	ref := rec.Ref()
	f.acking = &ref
	if at == nil || states == nil {
		return nil
	}
	p := &attemptv1.Positions{Epoch: &protocolv1.Epoch{Generation: at.Generation, N: at.Epoch}}
	for _, name := range slices.Sorted(maps.Keys(states)) {
		for _, sp := range states[name] {
			p.Splits = append(p.Splits, positionOf(name, sp))
		}
	}
	return f.co.Acknowledge(p)
}

// watch fails the attempt's dataflow once a worker fails it.
func (f *fleet) watch(ctx context.Context) error {
	select {
	case <-f.co.Done():
		return cmp.Or(f.co.Err(), attempt.ErrOver)
	case <-ctx.Done():
		return nil
	}
}

// inbox takes the workers' signals and schema proposals.
func (f *fleet) inbox(ctx context.Context) error {
	for {
		var from attempt.From
		select {
		case m, ok := <-f.co.Inbox():
			if !ok {
				return nil
			}
			from = m
		case <-ctx.Done():
			return nil
		}
		if p := from.Proposal; p != nil {
			if err := f.schemas.propose(from.Worker, p); err != nil {
				return err
			}
			continue
		}
		s := from.Signal
		var ev event
		switch x := s.GetSignal().(type) {
		case *attemptv1.Signal_Placed:
			if f.placed[s.GetAfter()]++; f.placed[s.GetAfter()] < f.n {
				continue
			}
			delete(f.placed, s.GetAfter())
			ev = event{kind: evPlaced, epoch: s.GetAfter()}
		case *attemptv1.Signal_Emitted:
			ev = event{kind: evEmitted, epoch: s.GetAfter()}
		case *attemptv1.Signal_Full:
			ev = event{kind: evThreshold}
		case *attemptv1.Signal_ReadDone:
			ev = event{kind: evReadDone, lane: from.Worker, epoch: s.GetAfter(), active: x.ReadDone.GetActive()}
		default:
			continue
		}
		select {
		case f.events <- ev:
		case <-ctx.Done():
			return nil
		}
	}
}

// gather takes every worker's share of each epoch requested, in order, and
// hands them on merged.
func (f *fleet) gather(ctx context.Context) error {
	for {
		var n uint64
		select {
		case n = <-f.requested:
		case <-ctx.Done():
			return nil
		}
		shares, err := f.co.Shares(ctx, n)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		sh, err := f.merge(shares)
		if err != nil {
			return fmt.Errorf("runner: epoch %d: %w", n, err)
		}
		select {
		case f.events <- event{kind: evShared, epoch: n, shared: sh}:
		case <-ctx.Done():
			return nil
		}
	}
}

// shared is an epoch as every worker's shares give it.
type shared struct {
	splits   map[string][]checkpoint.SplitState
	rows     map[string]uint64
	descs    []destination.Staged
	coverage []checkpoint.Coverage
	flow     *Ready
}

// merge is the epoch shares, one per worker, give. Each stream's schema is
// its owner's newest, which holds every worker's.
func (f *fleet) merge(shares []*attemptv1.Share) (*shared, error) {
	sh := &shared{
		splits: map[string][]checkpoint.SplitState{}, rows: map[string]uint64{},
		descs: make([]destination.Staged, f.writers), coverage: make([]checkpoint.Coverage, f.writers), flow: &Ready{},
	}
	for _, s := range shares {
		for _, p := range s.GetPositions() {
			sh.splits[p.GetStream()] = append(sh.splits[p.GetStream()], splitStateOf(p))
		}
		for name, rows := range s.GetRows() {
			sh.rows[name] += rows
		}
		for _, c := range s.GetCommittables() {
			w := int(c.GetWriterLane())
			if w >= f.writers {
				return nil, fmt.Errorf("a committable of writer lane %d, of %d", w, f.writers)
			}
			sh.descs[w] = c.GetStaged()
			sh.coverage[w] = coverageOf(c.GetSplits())
		}
		if err := mergeReady(sh.flow, s); err != nil {
			return nil, err
		}
	}
	for name, ss := range sh.splits {
		// Splits an epoch added come after the assignment's, in the order
		// their workers shared them.
		slices.SortStableFunc(ss, func(a, b checkpoint.SplitState) int {
			return f.place(name, a.ID) - f.place(name, b.ID)
		})
	}
	maps.Copy(sh.flow.Schemas, f.schemas.taken())
	return sh, nil
}

func (f *fleet) place(stream, id string) int {
	if i, ok := f.order[checkpoint.SplitRef{Stream: stream, ID: id}]; ok {
		return i
	}
	return len(f.order)
}

// mergeReady adds a worker's share of its dataflow's Ready to r.
func mergeReady(r *Ready, s *attemptv1.Share) error {
	got, err := attempt.ReadyOf(s.GetReady())
	if err != nil {
		return err
	}
	if r.Schemas == nil {
		r.Schemas = map[string][]byte{}
	}
	r.Snapshots = append(r.Snapshots, got.Snapshots...)
	r.Quarantine = append(r.Quarantine, got.Quarantine...)
	for name, s := range got.Schemas {
		if _, ok := r.Schemas[name]; !ok {
			r.Schemas[name] = s
		}
	}
	return nil
}

func positionOf(stream string, sp checkpoint.SplitState) *attemptv1.SplitPosition {
	return &attemptv1.SplitPosition{Stream: stream, Split: &protocolv1.Split{Id: sp.ID, State: sp.State}, Complete: sp.Complete, Forced: sp.Forced, Reread: sp.Reread}
}

func splitStateOf(p *attemptv1.SplitPosition) checkpoint.SplitState {
	return checkpoint.SplitState{ID: p.GetSplit().GetId(), State: p.GetSplit().GetState(), Complete: p.GetComplete(), Forced: p.GetForced(), Reread: p.GetReread()}
}

func coverageOf(refs []*attemptv1.SplitRef) checkpoint.Coverage {
	var c checkpoint.Coverage
	for _, r := range refs {
		cs := checkpoint.CoveredSplit{Stream: r.GetStream(), ID: r.GetSplit()}
		if kg := r.GetKeyGroups(); kg != nil {
			cs.KeyGroups = &checkpoint.KeyGroupRange{First: int(kg.GetFirst()), Last: int(kg.GetLast())}
		}
		c.Splits = append(c.Splits, cs)
	}
	return c
}

// schemaOwner is each stream's one schema across the workers: it takes a
// change proposed from the version it holds, and sends every worker the
// new one; a proposal from an older version gets the proposer the one it
// holds.
type schemaOwner struct {
	co      attempt.Coordinator
	mu      sync.Mutex
	streams map[string]*ownedSchema
}

type ownedSchema struct {
	version uint64
	schema  []byte
}

func newSchemaOwner(co attempt.Coordinator, restored map[string][]byte) *schemaOwner {
	o := &schemaOwner{co: co, streams: map[string]*ownedSchema{}}
	for name, s := range restored {
		o.streams[name] = &ownedSchema{schema: s}
	}
	return o
}

func (o *schemaOwner) propose(worker int, p *attemptv1.SchemaProposal) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	s := o.streams[p.GetStream()]
	if s == nil {
		s = &ownedSchema{}
		o.streams[p.GetStream()] = s
	}
	to := worker
	if p.GetBase() == s.version {
		s.version++
		s.schema = p.GetArrowSchema()
		to = -1
	}
	return o.co.Schema(to, &attemptv1.StreamSchema{Stream: p.GetStream(), Version: s.version, ArrowSchema: s.schema})
}

// taken are the schemas of the streams that changed in the attempt.
func (o *schemaOwner) taken() map[string][]byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := map[string][]byte{}
	for name, s := range o.streams {
		if s.version > 0 {
			out[name] = s.schema
		}
	}
	return out
}

// remoteWriter is the attempt's first writer lane, on worker 0, as the
// coordinator reaches it: it applies schemas and commits there.
type remoteWriter struct{ f *fleet }

var (
	_ destination.Writer        = remoteWriter{}
	_ destination.SchemaApplier = remoteWriter{}
)

var errOnWorkers = errs.New(errs.Internal, "runner: a coordinator writes nothing; its workers do")

func (remoteWriter) Write(context.Context, destination.Epoch, string, arrow.RecordBatch) error {
	return errOnWorkers
}

func (remoteWriter) PreCommit(context.Context, destination.Epoch) (destination.Staged, error) {
	return nil, errOnWorkers
}

func (remoteWriter) Recover(context.Context, uint64, destination.Epoch, []destination.Staged) (destination.Recovery, error) {
	return 0, errOnWorkers
}

func (remoteWriter) Close(context.Context) error { return nil }

func (r remoteWriter) Commit(ctx context.Context, e destination.Epoch, staged []destination.Staged) (destination.Committed, error) {
	req := &attemptv1.Commit{Commit: &protocolv1.WriteCommit{Epoch: &protocolv1.Epoch{Generation: e.Generation, N: e.N}}}
	for _, s := range staged {
		req.Commit.Staged = append(req.Commit.Staged, s)
	}
	ans, err := r.f.co.Commit(ctx, 0, req)
	if err != nil {
		return destination.Committed{}, err
	}
	done := destination.Committed{Collapsed: ans.GetCollapsedRows()}
	return done, answerErr(ans.GetError())
}

func (r remoteWriter) ApplySchema(ctx context.Context, streams []destination.StreamSchema) (destination.SchemaApplied, error) {
	req := &protocolv1.WriteApplySchema{}
	for _, s := range streams {
		b, err := arrowipc.EncodeSchema(s.Schema)
		if err != nil {
			return destination.SchemaApplied{}, fmt.Errorf("stream %s schema: %w", s.Name, err)
		}
		req.Streams = append(req.Streams, &protocolv1.StreamSchema{Name: s.Name, ArrowSchema: b})
	}
	ans, err := r.f.co.Call(ctx, 0, &attemptv1.Call{Call: &attemptv1.Call_ApplySchema{ApplySchema: req}})
	if err != nil {
		return destination.SchemaApplied{}, err
	}
	if err := answerErr(ans.GetError()); err != nil {
		return destination.SchemaApplied{}, err
	}
	return schemaAppliedOf(ans.GetSchemaApplied()), nil
}

// answerErr is a worker's error as the coordinator acts on it: a fenced
// destination is destination.ErrFenced.
func answerErr(p *protocolv1.Error) error {
	err := errs.FromProto(p)
	if errs.CodeOf(err) == errs.Fenced {
		return fmt.Errorf("%w: %w", destination.ErrFenced, err)
	}
	return err
}

// answerOf is how a worker sends err: fenced is FENCED.
func answerOf(err error) *protocolv1.Error {
	if errors.Is(err, destination.ErrFenced) && errs.CodeOf(err) != errs.Fenced {
		err = errs.Wrap(errs.Fenced, err, "")
	}
	return errs.ToProto(err)
}

func schemaAppliedOf(p *protocolv1.WriteSchemaApplied) destination.SchemaApplied {
	var a destination.SchemaApplied
	for _, r := range p.GetRefused() {
		a.Refused = append(a.Refused, destination.RefusedColumn{Stream: r.GetStream(), Column: r.GetColumn(), Reason: r.GetReason(), FromType: r.GetFromType(), ToType: r.GetToType(), Fix: r.GetFix()})
	}
	if len(p.GetLimits()) > 0 {
		a.Limits = make(map[string]destination.TableLimits, len(p.GetLimits()))
		for name, l := range p.GetLimits() {
			a.Limits[name] = destination.TableLimits{NotNull: l.GetNotNull(), Unholdable: l.GetUnholdable(), Fixed: l.GetFixed(), Columns: int(l.GetColumns()), MaxColumns: int(l.GetMaxColumns())}
		}
	}
	return a
}

func schemaAppliedToProto(a destination.SchemaApplied) *protocolv1.WriteSchemaApplied {
	p := &protocolv1.WriteSchemaApplied{}
	for _, r := range a.Refused {
		p.Refused = append(p.Refused, &protocolv1.RefusedColumn{Stream: r.Stream, Column: r.Column, Reason: r.Reason, FromType: r.FromType, ToType: r.ToType, Fix: r.Fix})
	}
	if len(a.Limits) > 0 {
		p.Limits = make(map[string]*protocolv1.TableLimits, len(a.Limits))
		for name, l := range a.Limits {
			p.Limits[name] = &protocolv1.TableLimits{NotNull: l.NotNull, Unholdable: l.Unholdable, Fixed: l.Fixed, Columns: ints.Uint32(l.Columns), MaxColumns: ints.Uint32(l.MaxColumns)}
		}
	}
	return p
}

// coordinate runs the claimed attempt on its workers.
func (j *Job) coordinate(ctx, rctx context.Context, span trace.Span, sess *checkpoint.Session, at loaded, res Result, log *slog.Logger) (_ Result, err error) {
	if err := j.OnWorkers(j.Workers.Count); err != nil {
		return res, err
	}
	srv, n := j.Workers.Server, j.Workers.Count
	token := rand.Text()
	co, err := srv.Open(attempt.Attempt{ID: j.Owner, Generation: res.Generation, Workers: n, Token: token}, j.Workers.Options)
	if err != nil {
		return res, err
	}
	defer func() {
		ectx, cancel := context.WithTimeout(context.WithoutCancel(ctx), endTimeout)
		defer cancel()
		cause := err
		if errors.Is(err, ErrDrained) {
			cause = nil
		}
		if eerr := co.End(ectx, cause); eerr != nil {
			log.Warn("the workers didn't close their calls", "err", eerr)
		}
	}()
	targets := make([]attempt.Target, n)
	for i := range targets {
		targets[i] = attempt.Target{Address: srv.Address(), Certificate: srv.Certificate(), Token: token, Attempt: j.Owner, Generation: res.Generation, Worker: i}
	}
	if err := j.Workers.Launch(rctx, targets); err != nil {
		return res, fmt.Errorf("runner: launch the workers: %w", err)
	}
	f := &fleet{job: j, co: co, n: n, requested: make(chan uint64, 64), done: make([]bool, n), act: make([]bool, n), placed: map[uint64]int{}, log: log}
	f.maxBytes.Store(j.Settings.CommitMaxBytes)

	// Settled before anything is assigned. After a rewind, the restored
	// record's epoch is applied, and the destination may have dropped its
	// markers since, so only the fence is raised.
	recovering := at.head
	if at.rewound != nil {
		recovering = nil
	}
	settle := &attemptv1.Recover{Forced: slices.Sorted(maps.Keys(at.forced))}
	var e destination.Epoch
	applied := map[string]*arrow.Schema{}
	if recovering != nil {
		e = destination.Epoch{Generation: recovering.Generation, N: recovering.Epoch}
		if settle.Record, err = json.Marshal(recovering); err != nil {
			return res, err
		}
	}
	ans, err := co.Call(rctx, 0, &attemptv1.Call{Call: &attemptv1.Call_Recover{Recover: settle}})
	if err != nil {
		return res, fmt.Errorf("runner: recover epoch %d: %w", e.N, err)
	}
	if err := answerErr(ans.GetError()); err != nil {
		return res, fmt.Errorf("runner: recover epoch %d: %w", e.N, err)
	}
	res.Recovery = destination.Recovery(ans.GetRecovered().GetRecovery())
	if recovering != nil {
		// The worker applied them.
		if _, err := changedRaw(applied, restoredSchemas(recovering)); err != nil {
			return res, err
		}
	}
	restored := at.head
	if res.Recovery == destination.Lost {
		if at.prevErr != nil {
			return res, errs.Wrap(errs.CheckpointCorrupt, at.prevErr, fmt.Sprintf("runner: the destination lost epoch %d, and the checkpoint before it can't be read", e.N))
		}
		// Safe because the epoch never became visible, and upstream is
		// acknowledged at most up to the previous record.
		if err := sess.RewindTo(rctx, at.prev); err != nil {
			return res, err
		}
		restored = at.prev
	}
	restored, reset, resetStreams := j.reset(restored)
	if len(resetStreams) > 0 {
		log.Info("reading streams again from the start", "streams", resetStreams, "reset", reset)
	}
	if restored != nil {
		res.Restored = restored.Epoch
	}
	log.Debug("attempt recovered", "recovery", res.Recovery.String(), "restored", res.Restored, "workers", n)
	done := j.doneStreams(restored)
	splits, resplits, err := f.readSplits(rctx, restored, done)
	if err != nil {
		return res, err
	}
	a, err := j.layout(n, splits, done, resplits)
	if err != nil {
		return res, err
	}
	log.Debug("attempt laid out", "lanes", len(a.GetLanes()), "writer_lanes", len(a.GetWriterLanes()), "splits", layoutString(a))
	f.writers, f.order = len(a.GetWriterLanes()), map[checkpoint.SplitRef]int{}
	for i, s := range a.GetSplits() {
		f.order[checkpoint.SplitRef{Stream: s.GetSplit().GetStream(), ID: s.GetSplit().GetSplit().GetId()}] = i
	}
	a.Forced, a.Done = settle.GetForced(), slices.Sorted(maps.Keys(trueKeys(done)))
	if restored != nil {
		if a.Restored, err = json.Marshal(restored); err != nil {
			return res, err
		}
	}
	// Version 0 is what the workers' engines start from.
	var start map[string][]byte
	if restored != nil {
		start = map[string][]byte{}
		for name, st := range restored.Streams {
			start[name] = st.Schema
		}
	}
	f.schemas = newSchemaOwner(co, start)
	prune(rctx, sess, restored, j.keep(), j.Staging, false, log)
	span.SetAttributes(attribute.Int64("openetl.generation", ints.Int64Clamped(res.Generation)), attribute.String("openetl.recovery", res.Recovery.String()), attribute.Int64("openetl.epoch", ints.Int64Clamped(res.Restored)), attribute.Int("openetl.workers", n))
	span.End()

	c := &coordinator{job: j, sess: sess, w: remoteWriter{f}, log: log, res: &res, pending: make(map[uint64]*epochState), applied: applied, reset: reset,
		reported: map[string]*arrow.Schema{}, reportedOutputs: map[string]*arrow.Schema{}, measures: map[string]metric.MeasurementOption{}, fleet: f}
	err = c.runFleet(ctx, restored, a)
	return res, err
}

func trueKeys(m map[string]bool) map[string]bool {
	out := map[string]bool{}
	for k, v := range m {
		if v {
			out[k] = true
		}
	}
	return out
}

// restoredSchemas are a record's streams' schemas as the destination got
// them; nil without a record.
func restoredSchemas(r *checkpoint.Record) map[string][]byte {
	if r == nil {
		return nil
	}
	schemas := make(map[string][]byte, len(r.Streams))
	for name, st := range r.Streams {
		schemas[name] = st.Schema
		if st.Delivered != nil {
			schemas[name] = st.Delivered
		}
	}
	return schemas
}

// readSplits are each stream's splits as the attempt starts, as
// Job.readSplits gives them, those of reads that start from worker 0, and
// whether the source splits running splits.
func (f *fleet) readSplits(ctx context.Context, restored *checkpoint.Record, done map[string]bool) (map[string][]checkpoint.SplitState, bool, error) {
	out := make(map[string][]checkpoint.SplitState, len(f.job.Streams))
	req := &attemptv1.SplitRead{}
	for _, s := range f.job.Streams {
		prev, starts := f.job.startsRead(restored, done, s)
		if !starts {
			out[s.Name] = prev
			continue
		}
		er := &attemptv1.EndedRead{Stream: s.Name}
		if prev != nil {
			er.Ended = &protocolv1.Splits{}
			for _, sp := range prev {
				er.Ended.Splits = append(er.Ended.Splits, &protocolv1.Split{Id: sp.ID, State: sp.State})
			}
		}
		req.Streams = append(req.Streams, er)
	}
	ans, err := f.co.Call(ctx, 0, &attemptv1.Call{Call: &attemptv1.Call_SplitRead{SplitRead: req}})
	if err == nil {
		err = answerErr(ans.GetError())
	}
	if err != nil {
		return nil, false, fmt.Errorf("runner: the reads' splits: %w", err)
	}
	got := ans.GetReadSplits()
	for _, er := range req.GetStreams() {
		var splits []source.Split
		for _, sp := range got.GetSplits()[er.GetStream()].GetSplits() {
			splits = append(splits, source.Split{ID: sp.GetId(), State: sp.GetState()})
		}
		if err := checkSplits(er.GetStream(), splits); err != nil {
			return nil, false, err
		}
		for _, sp := range splits {
			out[er.GetStream()] = append(out[er.GetStream()], checkpoint.SplitState{ID: sp.ID, State: sp.State})
		}
	}
	return out, got.GetResplits(), nil
}

// layout spreads the attempt over n workers. A stream's splits go to the
// workers emitting least, but those of a stream whose key's rows meet on a
// writer lane, of an overwrite, whose read one pump ends, and of a stream
// the attempt doesn't read stay on one worker. Each worker runs as many
// lanes as the job, no more than its splits unless the source splits
// running ones, and at least one of the destination's writer lanes. Worker
// 0 has the first writer lane, which applies schemas and commits.
func (j *Job) layout(n int, splits map[string][]checkpoint.SplitState, done map[string]bool, resplits bool) (*attemptv1.Assignment, error) {
	load := make([]int, n)
	inParts := make([]bool, n)
	least := func() int { return slices.Index(load, slices.Min(load)) }
	type placed struct {
		stream string
		split  checkpoint.SplitState
		worker int
	}
	var all []placed
	for _, s := range j.Streams {
		ss := splits[s.Name]
		together := done[s.Name] || len(ss) <= 1 || s.Mode == destination.Overwrite || j.keyGrouped(s)
		w := least()
		for _, sp := range ss {
			if !together && !sp.Complete {
				w = least()
			}
			all = append(all, placed{s.Name, sp, w})
			if !sp.Complete && !done[s.Name] {
				load[w]++
				inParts[w] = inParts[w] || sp.ID != checkpoint.WholeStream
			}
		}
	}
	lanes := make([]int, n)
	total := 0
	for i := range n {
		lanes[i] = min(j.lanes(), max(load[i], 1))
		if resplits && inParts[i] {
			lanes[i] = j.lanes()
		}
		total += lanes[i]
	}
	writers := make([]int, n)
	for i := range writers {
		writers[i] = 1
	}
	for extra := j.Destination.Capabilities().Lanes() - n; extra > 0; {
		more := false
		for i := range writers {
			if extra > 0 && writers[i] < lanes[i] {
				writers[i]++
				extra--
				more = true
			}
		}
		if !more {
			break
		}
	}
	j.Meter.SetLanes(total, 0)
	a := &attemptv1.Assignment{KeyGroups: ints.Uint32(j.keyGroups())}
	first := make([]int, n)
	for i := range n {
		first[i] = len(a.Lanes)
		for range lanes[i] {
			a.Lanes = append(a.Lanes, &attemptv1.Lane{Worker: ints.Uint32(i)})
		}
	}
	for i := range n {
		for range writers[i] {
			a.WriterLanes = append(a.WriterLanes, &attemptv1.Lane{Worker: ints.Uint32(i)})
		}
	}
	next := make([]int, n)
	for _, p := range all {
		lane := first[p.worker] + next[p.worker]%lanes[p.worker]
		next[p.worker]++
		a.Splits = append(a.Splits, &attemptv1.SplitAssignment{Split: positionOf(p.stream, p.split), Lane: ints.Uint32(lane)})
	}
	return a, nil
}

// runFleet runs the attempt's epochs on its workers.
func (c *coordinator) runFleet(ctx context.Context, restored *checkpoint.Record, a *attemptv1.Assignment) error {
	f := c.fleet
	c.begin(restored)
	if restored != nil {
		ref := restored.Ref()
		f.acking = &ref
	}
	a.FirstEpoch, a.MaxBytes = c.next, f.perWorker()
	a.ReadStarts = map[string]int64{}
	for _, s := range c.job.Streams {
		if !slices.Contains(a.GetDone(), s.Name) {
			a.ReadStarts[s.Name] = c.readStarts[s.Name]
		}
	}
	g, gctx := errgroup.WithContext(ctx)
	c.events = make(chan event, 64)
	c.commits = make(chan commitReq, 1)
	f.events = c.events
	if err := f.co.Assign(gctx, a); err != nil {
		return err
	}
	// The workers' side ends with the loop, or with the error that ended it.
	sctx, stop := context.WithCancel(gctx)
	defer stop()
	g.Go(func() error { return f.watch(sctx) })
	g.Go(func() error { return f.inbox(sctx) })
	g.Go(func() error { return f.gather(sctx) })
	g.Go(func() error { return c.committer(gctx) })
	g.Go(func() error {
		defer close(c.commits)
		defer stop()
		return c.loop(gctx)
	})
	return g.Wait()
}

// layoutString describes an assignment for logs.
func layoutString(a *attemptv1.Assignment) string {
	var b strings.Builder
	for i, s := range a.GetSplits() {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s/%s→%d", s.GetSplit().GetStream(), s.GetSplit().GetSplit().GetId(), a.GetLanes()[s.GetLane()].GetWorker())
	}
	return b.String()
}
