// Package runner runs a pipeline's jobs: it hosts the connectors, drives the
// dataflow, and coordinates checkpoints.
package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

type Stream struct {
	Name       string
	Replay     source.Replay
	Mode       destination.WriteMode
	PrimaryKey []string
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
	Dataflow    Dataflow
	Settings    Settings
	Logger      *slog.Logger
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
	cfg := destination.OpenConfig{Pipeline: job.Pipeline, Generation: res.Generation, Scope: job.Settings.Scope}
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
	if head != nil {
		e, staged = destination.Epoch{Generation: head.Generation, N: head.Epoch}, head.Staged
	}
	if res.Recovery, err = w.Recover(ctx, res.Generation, e, staged); err != nil {
		return res, fmt.Errorf("runner: recover epoch %d: %w", e.N, err)
	}
	restored := head
	if res.Recovery == destination.Lost {
		// Safe because acknowledgements trail by one checkpoint.
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

	c := &coordinator{job: &job, sess: sess, w: w, log: log, res: &res, pending: make(map[uint64]*epochState)}
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
		return fmt.Errorf("runner: the destination doesn't support commit scope %d", j.Settings.Scope)
	}
	seen := make(map[string]bool)
	for _, s := range j.Streams {
		if seen[s.Name] {
			return fmt.Errorf("runner: stream %q configured twice", s.Name)
		}
		seen[s.Name] = true
		if lvl := delivery.StreamLevel(caps, s.Replay, s.Mode, j.Settings.Dedupe); lvl < j.Settings.Require {
			return fmt.Errorf("runner: stream %q gets %v, below the required %v", s.Name, lvl, j.Settings.Require)
		}
	}
	return nil
}

type eventKind uint8

const (
	evPlaced    eventKind = iota + 1 // every stream crossed barrier epoch
	evStaged                         // the sink staged epoch
	evThreshold                      // CommitMaxBytes was reached
	evReadDone                       // Read returned; active says whether anything is uncommitted
)

type event struct {
	kind   eventKind
	epoch  uint64
	states map[string][]byte
	forced []string
	staged destination.Staged
	active bool
}

type epochState struct {
	placed, staged bool
	states         map[string][]byte
	forced         []string
	desc           destination.Staged
}

// coordinator requests barriers and commits each epoch once it is placed and
// staged.
type coordinator struct {
	job  *Job
	sess *checkpoint.Session
	w    destination.Writer
	log  *slog.Logger
	res  *Result

	events      chan event
	pump        *sourcePump
	next        uint64 // the next epoch to request
	outstanding bool   // a barrier is being placed
	want        bool   // a commit trigger fired
	readDone    bool
	active      bool // emitted after the last barrier, so a final epoch is needed
	pending     map[uint64]*epochState
	committed   uint64            // the last epoch committed or restored
	ackStates   map[string][]byte // states of the committed epoch, acked after the next commit
}

func (c *coordinator) run(ctx context.Context, restored *checkpoint.Record, canForce map[string]bool) error {
	states := map[string][]byte{}
	restoredForced := map[string]bool{}
	if restored != nil {
		c.committed = restored.Epoch
		for name, st := range restored.Streams {
			states[name] = st.State
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
	c.pump = newSourcePump(gctx, c.job, states, restoredForced, canForce, toFlow, c.events)
	sink := newSinkPump(c.w, c.sess.Generation(), c.next, c.job.Streams, fromFlow, c.events)

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
	g.Go(func() error { return c.job.Dataflow.Run(gctx, toFlow, fromFlow) })
	g.Go(func() error { return sink.run(gctx) })
	g.Go(func() error { return c.loop(gctx) })
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
	return c.readDone && !c.active && !c.outstanding && c.committed == c.next-1
}

func (c *coordinator) handle(ctx context.Context, ev event) error {
	switch ev.kind {
	case evThreshold:
		c.want = true
		return nil
	case evReadDone:
		c.readDone, c.active = true, ev.active
		return nil
	case evPlaced:
		st := c.pending[ev.epoch]
		if st == nil || st.placed {
			return fmt.Errorf("runner: unexpected placement of epoch %d", ev.epoch)
		}
		st.placed, st.states, st.forced = true, ev.states, ev.forced
		c.outstanding = false
	case evStaged:
		st := c.pending[ev.epoch]
		if st == nil || st.staged {
			return fmt.Errorf("runner: unexpected staging of epoch %d", ev.epoch)
		}
		st.staged, st.desc = true, ev.staged
	}
	for {
		n := c.committed + 1
		st := c.pending[n]
		if st == nil || !st.placed || !st.staged {
			return nil
		}
		if err := c.commit(ctx, n, st); err != nil {
			return err
		}
		delete(c.pending, n)
	}
}

// commit records the checkpoint, makes the epoch visible, then acknowledges
// the previous epoch upstream.
func (c *coordinator) commit(ctx context.Context, n uint64, st *epochState) error {
	rec := &checkpoint.Record{
		Epoch:     n,
		Streams:   make(map[string]checkpoint.StreamState, len(st.states)),
		Staged:    st.desc,
		Forced:    st.forced,
		CreatedAt: time.Now().UTC(),
	}
	for name, state := range st.states {
		rec.Streams[name] = checkpoint.StreamState{State: state}
	}
	slices.Sort(rec.Forced)
	err := retry(ctx, func() error { return c.sess.Commit(ctx, rec) }, checkpoint.ErrFenced)
	if err != nil {
		return fmt.Errorf("runner: record checkpoint %d: %w", n, err)
	}
	e := destination.Epoch{Generation: c.sess.Generation(), N: n}
	if err := retry(ctx, func() error { return c.w.Commit(ctx, e, st.desc) }, destination.ErrFenced); err != nil {
		return fmt.Errorf("runner: commit epoch %d: %w", n, err)
	}
	c.committed = n
	c.res.Committed++
	c.res.Last = n
	c.log.Debug("epoch committed", "epoch", n, "forced", st.forced)

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
