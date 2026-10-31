// Package runner runs a pipeline's jobs: it hosts the connectors, drives the
// dataflow, and coordinates checkpoints.
package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
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
}

// Settings are a pipeline's spec.delivery.
type Settings struct {
	// A barrier is placed without a resume point only where every stream stays
	// at Require.
	Require delivery.Level
	// 0 disables.
	CommitInterval time.Duration
	// Bytes read since the last barrier; 0 disables.
	CommitMaxBytes int64
	Scope          destination.Scope
	// How long a barrier waits for a resume point before it is forced; 0
	// waits forever.
	ResumePointTimeout time.Duration
	Dedupe             delivery.Dedupe
}

type Job struct {
	// Unique per attempt.
	Owner       string
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
	Dataflow      Dataflow
	Settings      Settings
	Logger        *slog.Logger
	// Called on the coordinator's goroutine after each epoch commits. It
	// must not block.
	OnCommit func(epoch uint64)
}

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
			Dedupe:     delivery.ShouldDedupe(caps, s.Replay, s.Mode, job.Settings.Dedupe, forced[s.Name]),
		})
		canForce[s.Name] = delivery.StreamLevel(caps, source.Superset, s.Mode, job.Settings.Dedupe) >= job.Settings.Require
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

	c := &coordinator{job: &job, sess: sess, w: w, log: log, res: &res, pending: make(map[uint64]*epochState), applied: applied}
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
	case j.Settings.Require == 0 || j.Settings.Scope == 0 || j.Settings.Dedupe == 0:
		return errors.New("runner: job settings need Require, Scope and Dedupe")
	}
	caps := j.Destination.Capabilities()
	if !caps.SupportsScope(j.Settings.Scope) {
		return errs.New(errs.ConfigInvalid, "runner: the destination doesn't support commit scope %d", j.Settings.Scope)
	}
	seen := make(map[string]bool)
	for _, s := range j.Streams {
		if seen[s.Name] {
			return errs.New(errs.ConfigInvalid, "runner: stream %q configured twice", s.Name)
		}
		seen[s.Name] = true
		if lvl := delivery.StreamLevel(caps, s.Replay, s.Mode, j.Settings.Dedupe); lvl < j.Settings.Require {
			return errs.New(errs.ConfigInvalid, "runner: stream %q gets %v, below the required %v", s.Name, lvl, j.Settings.Require)
		}
	}
	return nil
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
	staged   destination.Staged
	ready    *Ready
	active   bool
}

type epochState struct {
	placed, staged, ready bool
	states                map[string][]byte
	forced                []string
	complete              map[string]bool
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
	pending     map[uint64]*epochState
	committed   uint64                   // the last epoch committed or restored
	ackStates   map[string][]byte        // owned by the committer: states of the committed epoch, acked after the next commit
	applied     map[string]*arrow.Schema // owned by the committer: each stream's schema last applied
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
	c.pump = newSourcePump(gctx, c.job, c.committed, states, restoredForced, canForce, toFlow, c.events)
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
		Policy:        c.job.SchemaPolicy,
		QuarantineURL: c.job.QuarantineURL,
		ReadStarts:    c.readStarts,
		Schemas:       schemas,
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
		req.Streams = append(req.Streams, source.ConfiguredStream{Name: s.Name, State: states[s.Name]})
	}
	g.Go(func() error {
		if err := c.job.Source.Read(gctx, req, c.pump); err != nil {
			return fmt.Errorf("runner: read: %w", err)
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
		if c.shouldRequest() {
			c.outstanding, c.want = true, false
			c.active = c.active && !c.readDone // after Read, this barrier covers everything
			n := c.next
			c.next++
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
	return (c.want && !c.readDone) || (c.readDone && c.active)
}

func (c *coordinator) finished() bool {
	return c.readDone && !c.active && !c.outstanding && !c.committing && c.committed == c.next-1
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
		st.placed, st.states, st.forced, st.complete = true, ev.states, ev.forced, ev.complete
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
		CreatedAt:  time.Now().UTC(),
	}
	for name, state := range st.states {
		rec.Streams[name] = checkpoint.StreamState{
			State:     state,
			Schema:    st.flow.Schemas[name],
			ReadStart: c.readStarts[name],
			Complete:  st.complete[name],
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
	var changed []destination.StreamSchema
	for name, b := range schemas {
		if b == nil {
			continue
		}
		s, err := decodeSchema(b)
		if err != nil {
			return fmt.Errorf("stream %s schema: %w", name, err)
		}
		if prev := applied[name]; prev == nil || !sameSchema(prev, s) {
			changed = append(changed, destination.StreamSchema{Name: name, Schema: s})
		}
	}
	if len(changed) == 0 {
		return nil
	}
	slices.SortFunc(changed, func(a, b destination.StreamSchema) int { return strings.Compare(a.Name, b.Name) })
	if err := sa.ApplySchema(ctx, changed); err != nil {
		return fmt.Errorf("apply schemas: %w", err)
	}
	for _, s := range changed {
		applied[s.Name] = s.Schema
	}
	return nil
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
