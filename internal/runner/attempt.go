// Package runner runs a pipeline's jobs: it hosts the connectors, drives the
// dataflow, and coordinates checkpoints.
package runner

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"golang.org/x/sync/errgroup"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

type Stream struct {
	Name        string
	Replay      source.Replay
	Mode        destination.WriteMode
	PrimaryKey  []string
	CursorField string
	// Declared logical types under source names; nil: inferred.
	Schema *arrow.Schema
	// Decimal columns that arrive as another type instead, by source name:
	// String or Float64.
	Overrides map[string]arrow.DataType
}

func (s Stream) delivery() delivery.Stream {
	return delivery.Stream{Name: s.Name, Replay: s.Replay, Mode: s.Mode, Keyed: len(s.PrimaryKey) > 0}
}

// Declared is Schema with Overrides applied.
func (s Stream) Declared() *arrow.Schema {
	if s.Schema == nil || len(s.Overrides) == 0 {
		return s.Schema
	}
	fields := s.Schema.Fields()
	for i, f := range fields {
		if t, ok := s.Overrides[f.Name]; ok {
			fields[i] = arrow.Field{Name: f.Name, Type: t, Nullable: f.Nullable}
		}
	}
	md := s.Schema.Metadata()
	return arrow.NewSchema(fields, &md)
}

func (s Stream) checkOverrides() error {
	for _, name := range slices.Sorted(maps.Keys(s.Overrides)) {
		if t := s.Overrides[name]; !arrow.TypeEqual(t, arrow.BinaryTypes.String) && !arrow.TypeEqual(t, arrow.PrimitiveTypes.Float64) {
			return fmt.Errorf("stream %q: column %q can't be overridden to %s, only to string or float64", s.Name, name, t)
		}
		var fs []arrow.Field
		if s.Schema != nil {
			fs, _ = s.Schema.FieldsByName(name)
		}
		if len(fs) != 1 || fs[0].Type.ID() != arrow.DECIMAL128 {
			return fmt.Errorf("stream %q: column %q has a type override, but the source doesn't declare it a decimal", s.Name, name)
		}
	}
	return nil
}

type Job struct {
	// Unique per attempt.
	Owner string
	// Identifies the sync. Its attempts don't read again the streams one of
	// them completed. Empty: every attempt reads every stream.
	Sync        string
	Pipeline    string
	Log         *checkpoint.Log
	Source      source.Reader
	Streams     []Stream
	Destination destination.Destination
	// The destination's schema or dataset; empty for its default.
	Namespace string
	// Tables and columns keep their source names instead of normalized ones.
	KeepSourceNames bool
	SchemaPolicy    SchemaPolicy
	// Where the dataflow keeps records it can't take, as Plan says.
	QuarantineURL string
	// Infinite dates and timestamps are quarantined instead of clamped.
	QuarantineInfinity bool
	Dataflow           Dataflow
	Settings           delivery.Settings
	Logger             *slog.Logger
	// Called on the coordinator's goroutine after each epoch commits. It
	// must not block.
	OnCommit func(epoch uint64)
	// Called after an epoch commits with the schemas that changed since the
	// last call, the attempt's first holding every stream's. Each is an
	// Arrow IPC stream holding only the schema. It must not block.
	OnSchemas func(e destination.Epoch, schemas map[string][]byte)
	// Called after an epoch commits, and after OnSchemas, with its record,
	// which it must not modify. It must not block.
	OnCheckpoint func(rec *checkpoint.Record)
	// Closing it stops the attempt at its next checkpoint with ErrDrained,
	// unless the read has already ended, in which case it finishes.
	Drain <-chan struct{}
	// Records kept along the checkpoint chain, at least 2. Zero: 10.
	KeepRecords int
}

// ErrDrained: the attempt stopped at a checkpoint because Job.Drain closed.
var ErrDrained = errors.New("runner: drained at a checkpoint")

type Result struct {
	Generation uint64
	Recovery   destination.Recovery
	// The epoch the attempt resumed from; 0 from the beginning.
	Restored uint64
	// Epochs committed by this attempt.
	Committed int
	// The last epoch committed, or Restored.
	Last uint64
}

const defaultKeepRecords = 10

// commitAttempts bounds in-process retries of the idempotent commit steps.
// Recovery in the next attempt finishes what still fails.
const commitAttempts = 4

// RunAttempt runs one attempt of a bounded sync: claim, recover HEAD's epoch,
// then read until every stream is complete, committing along the way.
func RunAttempt(ctx context.Context, job Job) (res Result, err error) {
	if err := job.validate(); err != nil {
		return res, err
	}
	log := job.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log = log.With("pipeline", job.Pipeline, "owner", job.Owner)
	caps := job.Destination.Capabilities()

	sess, err := job.Log.Claim(ctx, job.Owner)
	if err != nil {
		return res, err
	}
	res.Generation = sess.Generation()
	log = log.With("generation", res.Generation)

	head, err := sess.Load(ctx)
	if err != nil {
		return res, err
	}
	// The previous record is needed if the sink reports HEAD's epoch lost.
	var prev *checkpoint.Record
	if head != nil && head.Previous != nil {
		if prev, err = job.Log.Record(ctx, *head.Previous); err != nil {
			return res, err
		}
	}

	// Either record may become the restore point, so dedupe conservatively.
	forced := make(map[string]bool)
	for _, r := range []*checkpoint.Record{head, prev} {
		if r != nil {
			for _, s := range r.Forced {
				forced[s] = true
			}
		}
	}
	cfg := destination.OpenConfig{Pipeline: job.Pipeline, Generation: res.Generation, Scope: job.Settings.Scope, Namespace: job.Namespace}
	canForce := make(map[string]bool, len(job.Streams))
	for _, s := range job.Streams {
		cfg.Streams = append(cfg.Streams, destination.Stream{
			Name:       s.Name,
			Mode:       s.Mode,
			PrimaryKey: s.PrimaryKey,
			Dedupe:     delivery.ShouldDedupe(caps, s.delivery(), job.Settings.Require, forced[s.Name]),
		})
		canForce[s.Name] = delivery.CanForce(caps, s.delivery(), job.Settings.Require)
	}
	w, err := job.Destination.Open(ctx, cfg)
	if err != nil {
		return res, fmt.Errorf("runner: open destination: %w", err)
	}
	defer func() {
		if cerr := w.Close(context.WithoutCancel(ctx)); err == nil && cerr != nil {
			err = fmt.Errorf("runner: close destination: %w", cerr)
		}
	}()

	var e destination.Epoch
	var staged destination.Staged
	applied := map[string]*arrow.Schema{}
	if head != nil {
		e, staged = destination.Epoch{Generation: head.Generation, N: head.Epoch}, head.Staged
		schemas := make(map[string][]byte, len(head.Streams))
		for name, st := range head.Streams {
			schemas[name] = st.Schema
		}
		// A redone epoch needs its schemas in place.
		if err := applySchemas(ctx, w, applied, schemas); err != nil {
			return res, err
		}
	}
	if res.Recovery, err = w.Recover(ctx, res.Generation, e, staged); err != nil {
		return res, fmt.Errorf("runner: recover epoch %d: %w", e.N, err)
	}
	restored := head
	if res.Recovery == destination.Lost {
		// Safe because the epoch never became visible, and upstream is
		// acknowledged at most up to the previous record.
		var to *checkpoint.RecordRef
		if prev != nil {
			ref := prev.Ref()
			to = &ref
		}
		if err := sess.Rewind(ctx, to); err != nil {
			return res, err
		}
		restored = prev
	}
	if restored != nil {
		res.Restored = restored.Epoch
	}
	log.Debug("attempt recovered", "recovery", res.Recovery.String(), "restored", res.Restored)
	if n, err := sess.Prune(ctx, restored, cmp.Or(job.KeepRecords, defaultKeepRecords)); err != nil {
		log.Warn("pruning the checkpoint log failed", "deleted", n, "err", err)
	} else if n > 0 {
		log.Debug("pruned the checkpoint log", "deleted", n)
	}

	c := &coordinator{job: &job, sess: sess, w: w, log: log, res: &res, pending: make(map[uint64]*epochState), applied: applied, reported: map[string]*arrow.Schema{}}
	return res, c.run(ctx, restored, canForce)
}

func (j *Job) validate() error {
	switch {
	case j.Owner == "":
		return errors.New("runner: job has no owner")
	case j.Log == nil || j.Source == nil || j.Destination == nil || j.Dataflow == nil:
		return errors.New("runner: job is missing its log, source, destination or dataflow")
	case len(j.Streams) == 0:
		return errors.New("runner: job has no streams")
	case j.KeepRecords < 0 || j.KeepRecords == 1:
		return fmt.Errorf("runner: the job keeps %d checkpoint records, not at least 2", j.KeepRecords)
	case j.Settings.Require == 0 || j.Settings.Scope == 0:
		return errors.New("runner: job settings need Require and Scope")
	}
	seen := make(map[string]bool)
	streams := make([]delivery.Stream, 0, len(j.Streams))
	for _, s := range j.Streams {
		if seen[s.Name] {
			return errs.New(errs.ConfigInvalid, "runner: stream %q configured twice", s.Name)
		}
		seen[s.Name] = true
		if err := s.checkOverrides(); err != nil {
			return errs.Wrap(errs.ConfigInvalid, err, "runner")
		}
		streams = append(streams, s.delivery())
	}
	return errs.Wrap(errs.ConfigInvalid, delivery.Check(j.Destination.Capabilities(), j.Settings, streams), "runner: plan checks")
}

type eventKind uint8

const (
	evPlaced    eventKind = iota + 1 // every stream crossed barrier epoch; complete lists the streams it ended
	evStaged                         // the sink staged epoch
	evReady                          // the dataflow's part of epoch is durable
	evThreshold                      // CommitMaxBytes was reached
	evReadDone                       // Read returned; active says whether anything emitted after barrier epoch is uncommitted
	evCommitted                      // epoch is visible in the destination
)

type event struct {
	kind     eventKind
	epoch    uint64
	states   map[string][]byte
	forced   []string
	complete map[string]bool
	rows     map[string]uint64
	staged   destination.Staged
	ready    *Ready
	active   bool
}

type epochState struct {
	placed, staged, ready bool
	states                map[string][]byte
	forced                []string
	complete              map[string]bool
	rows                  map[string]uint64
	desc                  destination.Staged
	flow                  *Ready
}

type commitReq struct {
	n  uint64
	st *epochState
}

// coordinator requests barriers and commits each epoch once it is placed and
// staged. Commits run one at a time, in epoch order, on the committer's
// goroutine, so barriers keep being requested while an epoch commits.
type coordinator struct {
	job  *Job
	sess *checkpoint.Session
	w    destination.Writer
	log  *slog.Logger
	res  *Result

	events      chan event
	commits     chan commitReq
	pump        *sourcePump
	next        uint64 // the next epoch to request
	outstanding bool   // a barrier is being placed
	want        bool   // a commit trigger fired
	readDone    bool
	active      bool // emitted after the last barrier, so a final epoch is needed
	committing  bool
	draining    bool
	drainEpoch  uint64 // the barrier requested after draining began; 0 until then
	pending     map[uint64]*epochState
	committed   uint64                   // the last epoch committed or restored
	ackStates   map[string][]byte        // owned by the committer: states of the committed epoch, acked after the next commit
	applied     map[string]*arrow.Schema // owned by the committer: each stream's schema last applied
	reported    map[string]*arrow.Schema // owned by the committer: each stream's schema last passed to OnSchemas
	readStarts  map[string]int64
}

func (c *coordinator) run(ctx context.Context, restored *checkpoint.Record, canForce map[string]bool) error {
	states := map[string][]byte{}
	schemas := map[string][]byte{}
	restoredForced := map[string]bool{}
	now := time.Now().UnixMicro()
	c.readStarts = make(map[string]int64, len(c.job.Streams))
	for _, s := range c.job.Streams {
		c.readStarts[s.Name] = now
		if restored == nil {
			continue
		}
		if st := restored.Streams[s.Name]; st.ReadStart != 0 && !st.Complete {
			c.readStarts[s.Name] = st.ReadStart
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
	c.next = c.committed + 1
	c.res.Last = c.committed

	g, gctx := errgroup.WithContext(ctx)
	toFlow := make(chan Frame, 16)
	fromFlow := make(chan Frame, 16)
	c.events = make(chan event, 64)
	c.commits = make(chan commitReq, 1)
	done := map[string]bool{}
	if restored != nil && c.job.Sync != "" && restored.Sync == c.job.Sync {
		for name, st := range restored.Streams {
			done[name] = st.Complete
		}
	}
	c.pump = newSourcePump(gctx, c.job, c.committed, states, restoredForced, canForce, done, toFlow, c.events)
	plan := Plan{
		Pipeline:   c.job.Pipeline,
		Owner:      c.job.Owner,
		Generation: c.sess.Generation(),
		FirstEpoch: c.next,
		Streams:    c.job.Streams,
		Naming: Naming{
			KeepSource: c.job.KeepSourceNames,
			MaxLength:  c.job.Destination.Capabilities().MaxIdentifierLength,
		},
		Policy:             c.job.SchemaPolicy,
		QuarantineURL:      c.job.QuarantineURL,
		QuarantineInfinity: c.job.QuarantineInfinity,
		ReadStarts:         c.readStarts,
		Schemas:            schemas,
	}
	if restored != nil {
		plan.Snapshots = restored.Snapshots
	}
	if es, ok := c.job.Destination.(EngineSink); ok {
		plan.Sink = es.EngineSink()
	}
	sink := newSinkPump(c.w, c.sess.Generation(), c.next, plan.Sink != SinkPlan{}, c.job.Streams, fromFlow, c.events)

	req := source.ReadRequest{}
	for _, s := range c.job.Streams {
		if !done[s.Name] {
			req.Streams = append(req.Streams, source.ConfiguredStream{Name: s.Name, State: states[s.Name]})
		}
	}
	g.Go(func() error {
		if len(req.Streams) > 0 {
			if err := c.job.Source.Read(gctx, req, c.pump); err != nil {
				return fmt.Errorf("runner: read: %w", err)
			}
		}
		return c.pump.readDone()
	})
	g.Go(c.pump.run)
	g.Go(func() error { return c.job.Dataflow.Run(gctx, plan, toFlow, fromFlow) })
	g.Go(func() error { return sink.run(gctx) })
	g.Go(func() error { return c.committer(gctx) })
	g.Go(func() error {
		defer close(c.commits)
		return c.loop(gctx)
	})
	return g.Wait()
}

func (c *coordinator) loop(ctx context.Context) error {
	var tick <-chan time.Time
	if d := c.job.Settings.CommitInterval; d > 0 {
		t := time.NewTicker(d)
		defer t.Stop()
		tick = t.C
	}
	drain := c.job.Drain
	for {
		// Events first, so decisions don't depend on select's random choice.
		select {
		case ev := <-c.events:
			if err := c.handle(ctx, ev); err != nil {
				return err
			}
			continue
		default:
		}
		if c.finished() {
			c.pump.stop()
			return nil
		}
		if c.drained() {
			c.log.Info("drained", "epoch", c.committed)
			return ErrDrained
		}
		if c.shouldRequest() {
			c.outstanding, c.want = true, false
			c.active = c.active && !c.readDone // after Read, this barrier covers everything
			n := c.next
			c.next++
			if c.draining && c.drainEpoch == 0 {
				c.drainEpoch = n
			}
			c.pending[n] = &epochState{}
			if err := c.pump.requestBarrier(n); err != nil {
				return err
			}
			continue
		}
		select {
		case ev := <-c.events:
			if err := c.handle(ctx, ev); err != nil {
				return err
			}
		case <-tick:
			c.want = true
		case <-drain:
			drain = nil
			c.draining, c.want = true, true
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// One epoch staging, one waiting to commit.
const maxInFlight = 2

func (c *coordinator) shouldRequest() bool {
	if c.outstanding || c.next-1-c.committed >= maxInFlight {
		return false
	}
	if c.readDone {
		return c.active
	}
	return c.want && c.drainEpoch == 0
}

func (c *coordinator) finished() bool {
	return c.readDone && !c.active && c.settled()
}

func (c *coordinator) drained() bool {
	return c.drainEpoch != 0 && !c.readDone && c.settled()
}

// settled: no barrier is being placed and every epoch requested has committed.
func (c *coordinator) settled() bool {
	return !c.outstanding && !c.committing && c.committed == c.next-1
}

func (c *coordinator) handle(ctx context.Context, ev event) error {
	switch ev.kind {
	case evThreshold:
		c.want = true
		return nil
	case evReadDone:
		// A barrier requested after the pump's last one is placed after Read
		// ended, so it covers everything.
		c.readDone, c.active = true, ev.active && ev.epoch == c.next-1
		return nil
	case evPlaced:
		st := c.pending[ev.epoch]
		if st == nil || st.placed {
			return fmt.Errorf("runner: unexpected placement of epoch %d", ev.epoch)
		}
		st.placed, st.states, st.forced, st.complete, st.rows = true, ev.states, ev.forced, ev.complete, ev.rows
		c.outstanding = false
	case evStaged:
		st := c.pending[ev.epoch]
		if st == nil || st.staged {
			return fmt.Errorf("runner: unexpected staging of epoch %d", ev.epoch)
		}
		st.staged, st.desc = true, ev.staged
	case evReady:
		st := c.pending[ev.epoch]
		if st == nil || st.ready {
			return fmt.Errorf("runner: unexpected readiness of epoch %d", ev.epoch)
		}
		st.ready, st.flow = true, ev.ready
	case evCommitted:
		if !c.committing || ev.epoch != c.committed+1 {
			return fmt.Errorf("runner: unexpected commit of epoch %d", ev.epoch)
		}
		c.committing = false
		c.committed = ev.epoch
		c.res.Committed++
		c.res.Last = ev.epoch
		delete(c.pending, ev.epoch)
		if c.job.OnCommit != nil {
			c.job.OnCommit(ev.epoch)
		}
	}
	n := c.committed + 1
	st := c.pending[n]
	if c.committing || st == nil || !st.placed || !st.staged || !st.ready {
		return nil
	}
	c.committing = true
	select {
	case c.commits <- commitReq{n: n, st: st}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *coordinator) committer(ctx context.Context) error {
	for req := range c.commits {
		if err := c.commit(ctx, req.n, req.st); err != nil {
			return err
		}
	}
	return nil
}

// commit records the checkpoint, makes the epoch visible, reports it to the
// loop, then acknowledges the previous epoch upstream.
func (c *coordinator) commit(ctx context.Context, n uint64, st *epochState) error {
	rec := &checkpoint.Record{
		Epoch:      n,
		Streams:    make(map[string]checkpoint.StreamState, len(st.states)),
		Staged:     st.desc,
		Snapshots:  st.flow.Snapshots,
		Quarantine: st.flow.Quarantine,
		Forced:     st.forced,
		Sync:       c.job.Sync,
		CreatedAt:  time.Now().UTC(),
	}
	for name, state := range st.states {
		rec.Streams[name] = checkpoint.StreamState{
			State:     state,
			Schema:    st.flow.Schemas[name],
			ReadStart: c.readStarts[name],
			Complete:  st.complete[name],
			Rows:      st.rows[name],
		}
	}
	slices.Sort(rec.Forced)
	err := retry(ctx, func() error { return c.sess.Commit(ctx, rec) }, checkpoint.ErrFenced)
	if err != nil {
		return fmt.Errorf("runner: record checkpoint %d: %w", n, err)
	}
	if err := retry(ctx, func() error { return applySchemas(ctx, c.w, c.applied, st.flow.Schemas) }, nil); err != nil {
		return fmt.Errorf("runner: epoch %d: %w", n, err)
	}
	e := destination.Epoch{Generation: c.sess.Generation(), N: n}
	if err := retry(ctx, func() error { return c.w.Commit(ctx, e, st.desc) }, destination.ErrFenced); err != nil {
		return fmt.Errorf("runner: commit epoch %d: %w", n, err)
	}
	c.log.Debug("epoch committed", "epoch", n, "forced", st.forced)
	if c.job.OnSchemas != nil {
		changed, err := changedSchemas(c.reported, st.flow.Schemas)
		if err != nil {
			return fmt.Errorf("runner: epoch %d: %w", n, err)
		}
		if len(changed) > 0 {
			raw := make(map[string][]byte, len(changed))
			for _, s := range changed {
				raw[s.Name] = st.flow.Schemas[s.Name]
				c.reported[s.Name] = s.Schema
			}
			c.job.OnSchemas(e, raw)
		}
	}
	if c.job.OnCheckpoint != nil {
		c.job.OnCheckpoint(rec)
	}
	select {
	case c.events <- event{kind: evCommitted, epoch: n}:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Trail by one checkpoint, so recovery can fall back one record.
	if ack, ok := c.job.Source.(source.Acknowledger); ok && c.ackStates != nil {
		if err := ack.Committed(ctx, c.ackStates); err != nil {
			c.log.Warn("upstream acknowledgement failed; the next commit retries it", "epoch", n-1, "err", err)
		}
	}
	c.ackStates = st.states
	return nil
}

func retry(ctx context.Context, f func() error, fatal error) error {
	backoff := 100 * time.Millisecond
	var err error
	for i := range commitAttempts {
		if err = f(); err == nil || errors.Is(err, fatal) || ctx.Err() != nil {
			return err
		}
		if i == commitAttempts-1 {
			break
		}
		t := time.NewTimer(backoff)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
		backoff *= 2
	}
	return err
}

// applySchemas applies the schemas that differ from those in applied, if the
// writer has a schema, and records them there.
func applySchemas(ctx context.Context, w destination.Writer, applied map[string]*arrow.Schema, schemas map[string][]byte) error {
	sa, ok := w.(destination.SchemaApplier)
	if !ok {
		return nil
	}
	changed, err := changedSchemas(applied, schemas)
	if err != nil || len(changed) == 0 {
		return err
	}
	if err := sa.ApplySchema(ctx, changed); err != nil {
		return fmt.Errorf("apply schemas: %w", err)
	}
	for _, s := range changed {
		applied[s.Name] = s.Schema
	}
	return nil
}

// changedSchemas decodes the schemas that differ from those in known, sorted
// by stream.
func changedSchemas(known map[string]*arrow.Schema, schemas map[string][]byte) ([]destination.StreamSchema, error) {
	var changed []destination.StreamSchema
	for name, b := range schemas {
		if b == nil {
			continue
		}
		s, err := decodeSchema(b)
		if err != nil {
			return nil, fmt.Errorf("stream %s schema: %w", name, err)
		}
		if prev := known[name]; prev == nil || !sameSchema(prev, s) {
			changed = append(changed, destination.StreamSchema{Name: name, Schema: s})
		}
	}
	slices.SortFunc(changed, func(a, b destination.StreamSchema) int { return strings.Compare(a.Name, b.Name) })
	return changed, nil
}

func decodeSchema(b []byte) (*arrow.Schema, error) {
	r, err := ipc.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer r.Release()
	return r.Schema(), nil
}

// sameSchema compares fields and metadata; the engine encodes metadata in
// no particular order.
func sameSchema(a, b *arrow.Schema) bool {
	if !slices.EqualFunc(a.Fields(), b.Fields(), func(x, y arrow.Field) bool { return x.Equal(y) }) {
		return false
	}
	am, bm := a.Metadata(), b.Metadata()
	if am.Len() != bm.Len() {
		return false
	}
	for i, k := range am.Keys() {
		if v, ok := bm.GetValue(k); !ok || v != am.Values()[i] {
			return false
		}
	}
	return true
}
