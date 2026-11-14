package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"golang.org/x/sync/errgroup"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/ints"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// settlesByLane says rec's committables are settled one by one: the
// destination's writer lanes commit on their own, and rec says what each
// committable holds. Otherwise lane 0 settles them all.
func settlesByLane(caps destination.Capabilities, rec *checkpoint.Record) bool {
	return caps.LaneCommits && rec != nil && len(rec.Committables) > 0 && len(rec.Coverage) == len(rec.Committables)
}

// settleCommittables settles rec's committables which names, in turn, and
// says how each was.
func (j *Job) settleCommittables(ctx context.Context, gen uint64, rec *checkpoint.Record, which []int, forced map[string]bool, log *slog.Logger) ([]destination.Recovery, error) {
	out := make([]destination.Recovery, 0, len(which))
	for _, i := range which {
		if i < 0 || i >= len(rec.Committables) {
			return nil, errs.New(errs.Internal, "runner: committable %d of epoch %d, which has %d", i, rec.Epoch, len(rec.Committables))
		}
		r, err := j.settleCommittable(ctx, gen, rec, i, forced, log)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// settledAs is how an epoch settled committable by committable was: Applied
// if every one was.
func settledAs(rs []destination.Recovery) (destination.Recovery, error) {
	out := destination.Applied
	for _, r := range rs {
		switch r {
		case destination.Applied:
		case destination.Redone:
			out = destination.Redone
		default:
			return 0, errs.New(errs.Internal, "runner: a committable settled as %s", r)
		}
	}
	return out, nil
}

// settleCommittable settles committable i of rec, on a writer of generation
// gen opened as the lane that staged it, and closed: from its marker, from
// its staging, or by redoing its part of the epoch from the source.
func (j *Job) settleCommittable(ctx context.Context, gen uint64, rec *checkpoint.Record, i int, forced map[string]bool, log *slog.Logger) (_ destination.Recovery, err error) {
	w, err := j.Destination.Open(ctx, j.openConfig(gen, forced, i))
	if err != nil {
		return 0, fmt.Errorf("runner: open destination lane %d: %w", i, err)
	}
	defer func() {
		if cerr := w.Close(context.WithoutCancel(ctx)); err == nil && cerr != nil {
			err = fmt.Errorf("runner: close destination lane %d: %w", i, cerr)
		}
	}()
	if _, err := j.applyRecorded(ctx, w, map[string]*arrow.Schema{}, rec); err != nil {
		return 0, err
	}
	e := destination.Epoch{Generation: rec.Generation, N: rec.Epoch}
	r, err := w.Recover(ctx, gen, e, []destination.Staged{rec.Committables[i]})
	if err != nil {
		return 0, fmt.Errorf("runner: recover writer lane %d of epoch %d: %w", i, e.N, err)
	}
	if r != destination.Lost {
		log.Debug("writer lane settled", "lane", i, "epoch", e.N, "recovery", r.String())
		return r, nil
	}
	log.Info("redoing a writer lane's part of an epoch from the source", "lane", i, "epoch", e.N, "of_generation", e.Generation)
	start := time.Now()
	if err := j.redo(ctx, gen, w, rec, i); err != nil {
		return 0, fmt.Errorf("runner: redo writer lane %d of epoch %d from the source: %w", i, e.N, err)
	}
	log.Info("redid a writer lane's part of an epoch", "lane", i, "epoch", e.N, "took", time.Since(start))
	return destination.Redone, nil
}

// recordedSchemas are rec's schemas as the destination got them: its
// streams', or a transform's outputs', a held column at the type the
// destination took.
func (j *Job) recordedSchemas(rec *checkpoint.Record) map[string][]byte {
	if !j.transformed() {
		return restoredSchemas(rec)
	}
	schemas := outputSchemas(rec)
	for name, o := range rec.Outputs {
		if o.Delivered != nil {
			schemas[name] = o.Delivered
		}
	}
	return schemas
}

// applyRecorded applies rec's schemas on w: an epoch recovered or redone
// needs them in place, and can't change them.
func (j *Job) applyRecorded(ctx context.Context, w destination.Writer, applied map[string]*arrow.Schema, rec *checkpoint.Record) (destination.SchemaApplied, error) {
	a, err := applySchemas(ctx, w, applied, j.recordedSchemas(rec))
	if err == nil && len(a.Refused) > 0 {
		err = errs.Wrap(errs.SchemaDrift, &RefusedError{Columns: []RefusedColumn{{RefusedColumn: a.Refused[0]}}}, "runner: recovering a recorded epoch")
	}
	return a, err
}

// redoRead is a covered split read again: from where its part of the epoch
// began, to where the record has it, and the rows it emitted on the way; 0
// if not known.
type redoRead struct {
	stream, id string
	from       []byte
	to         checkpoint.SplitState
	rows       uint64
}

// redoReads are what committable c of rec holds, read again: its splits,
// but those whose part of the epoch was empty, and of each stream the key
// groups whose rows it holds, where not all. A stream grouped by another
// key than the pipeline's now can't be read again as the epoch had it.
func (j *Job) redoReads(rec *checkpoint.Record, i int) ([]redoRead, map[string]checkpoint.KeyGroupRange, error) {
	var reads []redoRead
	ranges := map[string]checkpoint.KeyGroupRange{}
	whole := map[string]bool{}
	for _, cs := range rec.Coverage[i].Splits {
		k := slices.IndexFunc(j.Streams, func(s Stream) bool { return s.Name == cs.Stream })
		if k < 0 {
			return nil, nil, errs.New(errs.ConfigInvalid, "runner: epoch %d holds rows of stream %q, which the pipeline no longer reads, so writer lane %d's part can't be written again: read the stream again until the epoch is settled", rec.Epoch, cs.Stream, i)
		}
		if key := j.Streams[k].PrimaryKey; cs.KeyGroups != nil && !slices.Equal(cs.Key, key) {
			return nil, nil, errs.New(errs.ConfigInvalid, "runner: epoch %d split stream %q between writer lanes by its primary key %s, and the pipeline now keys it by %s, so writer lane %d's part can't be written again as it was: apply the pipeline with the key %s until the epoch is settled, then change it",
				rec.Epoch, cs.Stream, keyName(cs.Key), keyName(key), i, keyName(cs.Key))
		}
		splits := rec.Splits(cs.Stream)
		at := slices.IndexFunc(splits, func(sp checkpoint.SplitState) bool { return sp.ID == cs.ID })
		if at < 0 {
			return nil, nil, errs.New(errs.CheckpointCorrupt, "runner: epoch %d's record holds rows of %s, and has no position for it", rec.Epoch, splitName(cs.Stream, cs.ID))
		}
		r, ranged := ranges[cs.Stream]
		switch {
		case cs.KeyGroups == nil && ranged, cs.KeyGroups != nil && (whole[cs.Stream] || ranged && r != *cs.KeyGroups):
			return nil, nil, errs.New(errs.Internal, "runner: a committable of epoch %d holds rows of stream %q in more than one range of key groups", rec.Epoch, cs.Stream)
		case cs.KeyGroups == nil:
			whole[cs.Stream] = true
		default:
			ranges[cs.Stream] = *cs.KeyGroups
		}
		to := splits[at]
		if !to.Complete && bytes.Equal(cs.From, to.State) {
			// No resume point since: what it emitted after one is read again
			// from the record.
			continue
		}
		reads = append(reads, redoRead{stream: cs.Stream, id: cs.ID, from: cs.From, to: to, rows: cs.Rows})
	}
	return reads, ranges, nil
}

// keyName is a primary key as a message names it.
func keyName(key []string) string {
	if len(key) == 0 {
		return "(none)"
	}
	return "(" + strings.Join(key, ", ") + ")"
}

// redo reads committable i's part of rec's epoch again from the source, as
// rec's coverage names it, and writes and commits it on w as that epoch's:
// each covered split from where its part began up to its position in rec,
// and the rows of the key groups the committable holds. The dataflow shapes
// rows by rec's schemas, and runs as generation gen, which names what it
// quarantines.
func (j *Job) redo(ctx context.Context, gen uint64, w destination.Writer, rec *checkpoint.Record, i int) error {
	if j.ownCode() || j.staged() {
		return errs.New(errs.Internal, "runner: only the destination's own writers redo an epoch")
	}
	if kg := rec.KeyGroupCount(); kg != j.keyGroups() {
		return errs.New(errs.ConfigInvalid, "runner: the record has %d key groups, and the pipeline %d", kg, j.keyGroups())
	}
	reads, ranges, err := j.redoReads(rec, i)
	if err != nil {
		return err
	}
	rj := *j
	rj.Lanes, rj.writers = 1, 1
	readStarts, schemas := map[string]int64{}, map[string][]byte{}
	for name, st := range rec.Streams {
		readStarts[name] = st.ReadStart
		if st.Schema != nil {
			schemas[name] = st.Schema
		}
	}
	plan := rj.plan(gen, rec.Epoch, nil, readStarts, schemas, outputSchemas(rec), nil)
	plan.KeyGroupRanges, plan.FirstLane = ranges, i
	if plan.Sink != (SinkPlan{}) {
		return errs.New(errs.Internal, "runner: only the destination's own writers redo an epoch")
	}
	// A reader that tracks reads gets their starts, and the end of each read
	// the record ended.
	starts, ends := map[string]time.Time{}, newReadEnds()
	for _, r := range reads {
		st := rec.Streams[r.stream]
		starts[r.stream] = time.UnixMicro(st.ReadStart).UTC()
		if st.Complete {
			ends.set(r.stream, rec.Epoch)
		}
	}

	g, gctx := errgroup.WithContext(ctx)
	toFlow, fromFlow, lane := make(chan Frame, 16), make(chan Frame, 16), make(chan Frame, 16)
	// The epoch's staging and readiness, which wait here.
	events := make(chan event, 4)
	sink := newSinkPump(w, 0, rec.Generation, rec.Epoch, false, nil, j.sinkStreams(), starts, ends, lane, events)
	g.Go(func() error { return sink.run(gctx) })
	g.Go(func() error { return j.Dataflow.Run(gctx, plan, toFlow, fromFlow) })
	g.Go(func() error { return demux(gctx, fromFlow, []chan Frame{lane}) })
	g.Go(func() error {
		defer close(toFlow)
		if err := j.replay(gctx, rec.Epoch, reads, toFlow); err != nil {
			return err
		}
		for _, s := range plan.Streams {
			if err := send(gctx, toFlow, Frame{Kind: FrameBarrier, Stream: s.Name, Epoch: rec.Epoch}); err != nil {
				return err
			}
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}
	var staged destination.Staged
	got, ready := false, false
	for len(events) > 0 {
		switch ev := <-events; ev.kind {
		case evStaged:
			staged, got = ev.staged, true
		case evReady:
			ready = true
		}
	}
	if !got || !ready {
		return errs.New(errs.Internal, "runner: the dataflow ended without epoch %d staged and ready", rec.Epoch)
	}
	e := destination.Epoch{Generation: rec.Generation, N: rec.Epoch}
	err = retry(ctx, func() error {
		_, err := w.Commit(ctx, e, []destination.Staged{staged})
		return err
	}, destination.ErrFenced)
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// replay reads each of reads again, as many at once as the job has lanes,
// and sends what each emits up to its position on lane 0 of out.
func (j *Job) replay(ctx context.Context, n uint64, reads []redoRead, out chan<- Frame) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(j.lanes())
	for _, r := range reads {
		g.Go(func() error { return j.replaySplit(gctx, n, r, out) })
	}
	return g.Wait()
}

// replaySplit reads one split again from where its part of epoch n began,
// on a read of its own, which ends once the split emits the resume point
// the record has for it, or completes where the record has it complete.
// The read asks the source to end there. Only that split's emits go on: a
// split it adds is read on a read of its own, if the committable holds it.
func (j *Job) replaySplit(ctx context.Context, n uint64, r redoRead, out chan<- Frame) error {
	k := slices.IndexFunc(j.Streams, func(s Stream) bool { return s.Name == r.stream })
	s := j.Streams[k]
	rctx, stop := context.WithCancel(ctx)
	defer stop()
	em := &redoEmitter{ctx: rctx, stop: stop, read: r, out: out}
	cs := source.ConfiguredStream{Name: s.Name, SyncMode: s.Read, Cursor: s.CursorField, Columns: s.columns()}
	if r.id == checkpoint.WholeStream {
		cs.State, cs.Until = r.from, r.to.State
	} else {
		cs.Splits = []source.Split{{ID: r.id, State: r.from, Until: r.to.State}}
	}
	// Never continuous: the source ends the read once it has caught up with
	// where it was when the read started, which is past the position.
	req := source.ReadRequest{Pipeline: j.Pipeline, Streams: []source.ConfiguredStream{cs}, Lanes: 1, Continuous: false}
	err := j.Source.Read(rctx, req, em)
	reached, eerr := em.result()
	switch {
	case reached:
		return nil
	case eerr != nil:
		return eerr
	case ctx.Err() != nil:
		return ctx.Err()
	case err != nil:
		return fmt.Errorf("runner: read %s again: %w", splitName(r.stream, r.id), err)
	}
	return errs.New(errs.PositionLost, "runner: the source's read of %s ended before the position epoch %d's record has for it, so its part of the epoch can't be read again as it was: a redo needs a source that replays the same rows and resume points from a position", splitName(r.stream, r.id), n)
}

// redoEmitter takes what a read of one split emits again, and passes on
// its rows until it reaches the record's position for the split.
type redoEmitter struct {
	ctx  context.Context
	stop context.CancelFunc
	read redoRead
	out  chan<- Frame
	mu   sync.Mutex
	// The rows passed on so far.
	rows uint64
	// It reached the position, or failed to.
	done bool
	err  error
}

var _ source.Forwarder = (*redoEmitter)(nil)

func (e *redoEmitter) result() (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.done && e.err == nil, e.err
}

func (e *redoEmitter) Rows(stream string, rows []json.RawMessage) error {
	return e.Split(stream, source.WholeStream).Rows(rows)
}

func (e *redoEmitter) Records(stream string, batch arrow.RecordBatch) error {
	return e.Split(stream, source.WholeStream).Records(batch)
}

func (e *redoEmitter) Forward(stream string, batch arrowipc.Encoded) error {
	return e.Split(stream, source.WholeStream).(source.SplitForwarder).Forward(batch)
}

func (e *redoEmitter) ResumePoint(stream string, state []byte) error {
	return e.Split(stream, source.WholeStream).ResumePoint(state)
}

func (e *redoEmitter) Complete(stream string, next []byte) error {
	return e.Split(stream, source.WholeStream).Complete(next)
}

func (e *redoEmitter) Split(stream, id string) source.SplitEmitter {
	return redoSplit{e: e, mine: stream == e.read.stream && id == e.read.id}
}

// open says the split's rows go on: it's the one read, and short of its
// position.
func (e *redoEmitter) open(mine bool) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return mine && !e.done
}

// send passes f on, with its rows. As the ordinary read's emitter does, it
// keeps what the source handed it, and leaves a batch the source's when it
// fails.
func (e *redoEmitter) send(f Frame, rows uint64) error {
	f.Stream = e.read.stream
	select {
	case e.out <- f:
	case <-e.ctx.Done():
		return e.ctx.Err()
	}
	e.mu.Lock()
	e.rows += rows
	e.mu.Unlock()
	return nil
}

// at takes a resume point or the split's completion, and ends the read
// there if it's the record's position: the resume point it has, or, for a
// split that completed in the epoch, a completion whose next read starts at
// the same state. Fewer rows on the way than the epoch had are a loss, but
// where its barrier was forced: rows after its last resume point are read
// again anyway.
func (e *redoEmitter) at(mine, complete bool, state []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	to, name := e.read.to, splitName(e.read.stream, e.read.id)
	const need = "a redo needs a source that replays the same rows and resume points from a position"
	switch {
	case !mine || e.done:
		return
	case to.Complete && complete && !bytes.Equal(state, to.State):
		e.err = errs.New(errs.PositionLost, "runner: %s completed again at another state than the epoch's: %s", name, need)
	case complete && !to.Complete:
		e.err = errs.New(errs.PositionLost, "runner: %s completed again before the position the record has for it: %s", name, need)
	case !complete && (to.Complete || !bytes.Equal(state, to.State)):
		return
	case e.read.rows > 0 && !to.Forced && e.rows < e.read.rows:
		e.err = errs.New(errs.PositionLost, "runner: %s returned %d rows again up to the position the record has for it, where the epoch had %d: %s", name, e.rows, e.read.rows, need)
	}
	e.done = true
	e.stop()
}

// redoSplit is one split's emitter in a read again.
type redoSplit struct {
	e    *redoEmitter
	mine bool
}

var _ source.SplitForwarder = redoSplit{}

func (s redoSplit) Rows(rows []json.RawMessage) error {
	if !s.e.open(s.mine) {
		return nil
	}
	return s.e.send(Frame{Kind: FrameRows, Rows: rows}, uint64(len(rows)))
}

func (s redoSplit) Records(batch arrow.RecordBatch) error {
	if !s.e.open(s.mine) {
		batch.Release()
		return nil
	}
	return s.e.send(Frame{Kind: FrameBatch, Batch: batch}, ints.Uint64(max(batch.NumRows(), 0)))
}

func (s redoSplit) Forward(batch arrowipc.Encoded) error {
	if !s.e.open(s.mine) {
		return nil
	}
	return s.e.send(Frame{Kind: FrameBatch, IPC: batch.Bytes}, ints.Uint64(max(batch.Rows, 0)))
}

func (s redoSplit) ResumePoint(state []byte, _ ...source.Split) error {
	s.e.at(s.mine, false, state)
	return nil
}

func (s redoSplit) Complete(next []byte, _ ...source.Split) error {
	s.e.at(s.mine, true, next)
	return nil
}

// settleGroups spreads rec's committables over n workers, those that hold
// rows of one split on one worker, which reads it again for each in turn.
func settleGroups(rec *checkpoint.Record, n int) [][]uint32 {
	parent := make([]int, len(rec.Committables))
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(i int) int {
		if parent[i] != i {
			parent[i] = find(parent[i])
		}
		return parent[i]
	}
	first := map[checkpoint.SplitRef]int{}
	for i, c := range rec.Coverage {
		for _, s := range c.Splits {
			ref := checkpoint.SplitRef{Stream: s.Stream, ID: s.ID}
			if j, ok := first[ref]; ok {
				parent[find(i)] = find(j)
			} else {
				first[ref] = i
			}
		}
	}
	out := make([][]uint32, n)
	worker := map[int]int{}
	for i := range rec.Committables {
		root := find(i)
		w, ok := worker[root]
		if !ok {
			w = len(worker) % n
			worker[root] = w
		}
		out[w] = append(out[w], ints.Uint32(i))
	}
	return out
}
