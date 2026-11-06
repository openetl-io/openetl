package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/util"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/sdk/go/arrowipc"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

type emitKind uint8

const (
	emitRows emitKind = iota + 1
	emitBatch
	emitResume
	emitComplete
	emitDone // Read returned without error
)

type emit struct {
	kind    emitKind
	stream  string
	split   string
	rows    []json.RawMessage
	batch   arrow.RecordBatch
	encoded arrowipc.Encoded // an emitBatch without batch
	state   []byte
	added   []source.Split
}

// splitPos is where one of a stream's splits is in the read.
type splitPos struct {
	id   string
	lane int
	// state at the last resume point, or restored
	resume   []byte
	rows     uint64 // emitted since it last placed a barrier
	active   bool   // emitted since it last placed a barrier
	dirty    bool   // emitted since the last resume point
	complete bool
	placed   bool // the pending barrier is placed
	// It joined the stream after placing the pending barrier would have:
	// it isn't in that epoch.
	later bool
	// resume came from a forced barrier. Every checkpoint recording it must
	// list it as forced until its next real resume point.
	forced bool
	// resume came from a restore of a forced barrier, and the split hasn't
	// reached a resume point since, so what it emits is read again.
	rereading bool
	// It emitted while rereading since the last barrier.
	reread bool
}

type streamPos struct {
	splits []*splitPos // in the order they joined
	byID   map[string]*splitPos
	// A forced barrier keeps the stream at or above require.
	canForce bool
	// The barrier that ends the read is placed.
	ended bool
	// By lane: the pending barrier is on it, and what the stream's splits on
	// it emitted after placing the barrier before it was.
	lanes []laneHold
	// Splits a held emit adds, by the lane it's held on: what they emit
	// waits behind it.
	adding map[string]int
}

type laneHold struct {
	placed bool
	held   []emit
}

// readEnds records the barrier that ends each stream's read, and the last
// barrier placed on every split, for the sink pumps: a writer lane can pass
// a stream's barrier before the source has placed it on every lane, so they
// wait for the whole epoch to be placed before they end reads in it.
type readEnds struct {
	mu     sync.Mutex
	at     map[string]uint64
	placed uint64
	wake   chan struct{}
}

func newReadEnds() *readEnds {
	return &readEnds{at: map[string]uint64{}, wake: make(chan struct{})}
}

func (r *readEnds) set(stream string, epoch uint64) {
	r.mu.Lock()
	r.at[stream] = epoch
	r.mu.Unlock()
}

func (r *readEnds) setPlaced(epoch uint64) {
	r.mu.Lock()
	r.placed = epoch
	close(r.wake)
	r.wake = make(chan struct{})
	r.mu.Unlock()
}

func (r *readEnds) endsAt(stream string, epoch uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.at[stream] == epoch
}

func (r *readEnds) waitPlaced(ctx context.Context, epoch uint64) error {
	for {
		r.mu.Lock()
		placed, wake := r.placed, r.wake
		r.mu.Unlock()
		if placed >= epoch {
			return nil
		}
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type readEnder interface {
	endsAt(stream string, epoch uint64) bool
	// waitPlaced returns once barrier epoch is placed on every split.
	waitPlaced(ctx context.Context, epoch uint64) error
}

// lastEnd ends a transform's outputs' reads in the epoch where the last of
// the inputs read ends.
type lastEnd struct {
	ends   *readEnds
	inputs []string
}

func (l lastEnd) endsAt(_ string, epoch uint64) bool {
	l.ends.mu.Lock()
	defer l.ends.mu.Unlock()
	last := uint64(0)
	for _, s := range l.inputs {
		at, ok := l.ends.at[s]
		if !ok {
			return false
		}
		last = max(last, at)
	}
	return len(l.inputs) > 0 && last == epoch
}

// Without inputs, as a Flink job's outputs, no read ends, and the sink's
// epochs aren't the source's barriers.
func (l lastEnd) waitPlaced(ctx context.Context, epoch uint64) error {
	if len(l.inputs) == 0 {
		return nil
	}
	return l.ends.waitPlaced(ctx, epoch)
}

// sourcePump is the connector's Emitter. It places each requested barrier on
// every split right after that split's next resume point, and forwards a
// stream's barrier on a lane once every split of the stream on the lane has
// placed it. What a split emits after placing it waits until then. Only run
// touches its state.
type sourcePump struct {
	ctx      context.Context
	in       chan emit
	barriers chan uint64
	stopCh   chan struct{}
	out      chan<- Frame
	events   chan<- event
	ends     *readEnds
	lanes    int

	order   []string // stream names, in configuration order
	streams map[string]*streamPos
	known   map[string]bool // read by Emitter methods on the connector's goroutine

	maxBytes      int64
	forceAfter    time.Duration
	requested     uint64 // the last barrier requested
	pending       uint64 // epoch being placed; 0 if none
	unplaced      int    // splits that haven't placed the pending barrier
	splits        map[string][]checkpoint.SplitState
	rows          map[string]uint64
	bytes         int64
	thresholdSent bool
	done          bool
	forceTimer    *time.Timer
	forceC        <-chan time.Time
	// Counts what the source emitted, for the coordinator to skip epochs
	// that would hold nothing.
	emitted atomic.Uint64
	// Nil: progress is dropped.
	progress *Progress
}

var (
	_ source.Forwarder  = (*sourcePump)(nil)
	_ source.Progresser = (*sourcePump)(nil)
)

// splits are each stream's at the start of the read, its restored ones if it
// continues one; done are the streams that completed earlier in the sync,
// which aren't read.
func newSourcePump(ctx context.Context, job *Job, restoredEpoch uint64, splits map[string][]checkpoint.SplitState, canForce, done map[string]bool, ends *readEnds, out chan<- Frame, events chan<- event) *sourcePump {
	p := &sourcePump{
		ctx:        ctx,
		requested:  restoredEpoch,
		in:         make(chan emit),
		barriers:   make(chan uint64, 1),
		stopCh:     make(chan struct{}),
		out:        out,
		events:     events,
		ends:       ends,
		lanes:      job.lanes(),
		streams:    make(map[string]*streamPos, len(job.Streams)),
		known:      make(map[string]bool, len(job.Streams)),
		maxBytes:   job.Settings.CommitMaxBytes,
		forceAfter: job.Settings.ResumePointTimeout,
		progress:   job.Progress,
	}
	for _, s := range job.Streams {
		p.order = append(p.order, s.Name)
		st := &streamPos{byID: map[string]*splitPos{}, canForce: canForce[s.Name], ended: done[s.Name], lanes: make([]laneHold, p.lanes), adding: map[string]int{}}
		for i, sp := range splits[s.Name] {
			pos := &splitPos{id: sp.ID, lane: i % p.lanes, resume: sp.State, complete: sp.Complete, forced: sp.Forced, rereading: sp.Forced}
			st.splits = append(st.splits, pos)
			st.byID[sp.ID] = pos
		}
		p.streams[s.Name] = st
		p.known[s.Name] = true
	}
	return p
}

func (p *sourcePump) Rows(stream string, rows []json.RawMessage) error {
	return p.send(emit{kind: emitRows, stream: stream, rows: rows})
}

func (p *sourcePump) Records(stream string, batch arrow.RecordBatch) error {
	return p.send(emit{kind: emitBatch, stream: stream, batch: batch})
}

func (p *sourcePump) Forward(stream string, batch arrowipc.Encoded) error {
	return p.send(emit{kind: emitBatch, stream: stream, encoded: batch})
}

func (p *sourcePump) ResumePoint(stream string, state []byte) error {
	return p.send(emit{kind: emitResume, stream: stream, state: state})
}

func (p *sourcePump) Complete(stream string, next []byte) error {
	return p.send(emit{kind: emitComplete, stream: stream, state: next})
}

func (p *sourcePump) Split(stream, id string) source.SplitEmitter {
	return pumpSplit{p: p, stream: stream, id: id}
}

// pumpSplit emits on one split.
type pumpSplit struct {
	p          *sourcePump
	stream, id string
}

var _ source.SplitForwarder = pumpSplit{}

func (s pumpSplit) Rows(rows []json.RawMessage) error {
	return s.p.send(emit{kind: emitRows, stream: s.stream, split: s.id, rows: rows})
}

func (s pumpSplit) Records(batch arrow.RecordBatch) error {
	return s.p.send(emit{kind: emitBatch, stream: s.stream, split: s.id, batch: batch})
}

func (s pumpSplit) Forward(batch arrowipc.Encoded) error {
	return s.p.send(emit{kind: emitBatch, stream: s.stream, split: s.id, encoded: batch})
}

func (s pumpSplit) ResumePoint(state []byte, added ...source.Split) error {
	return s.p.send(emit{kind: emitResume, stream: s.stream, split: s.id, state: state, added: added})
}

func (s pumpSplit) Complete(next []byte, added ...source.Split) error {
	return s.p.send(emit{kind: emitComplete, stream: s.stream, split: s.id, state: next, added: added})
}

// Progress doesn't go through the pump: it's for metrics, not ordered with
// what the source emits.
func (p *sourcePump) Progress(r source.Progress) error {
	if r.Stream != "" && !p.known[r.Stream] {
		return fmt.Errorf("runner: source reported progress for unknown stream %q", r.Stream)
	}
	if p.progress != nil {
		p.progress.set(r)
	}
	return nil
}

func (p *sourcePump) readDone() error { return p.send(emit{kind: emitDone}) }

func (p *sourcePump) send(e emit) error {
	if e.kind != emitDone && !p.known[e.stream] {
		return fmt.Errorf("runner: source emitted unknown stream %q", e.stream)
	}
	select {
	case p.in <- e:
		return nil
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

// requestBarrier is called only when no other barrier is being placed.
func (p *sourcePump) requestBarrier(n uint64) error {
	select {
	case p.barriers <- n:
		return nil
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

func (p *sourcePump) stop() { close(p.stopCh) }

func (p *sourcePump) run() error {
	defer close(p.out)
	for {
		// Barrier requests first, so ready requests and emits keep one order.
		select {
		case n := <-p.barriers:
			if err := p.request(n); err != nil {
				return err
			}
			continue
		default:
		}
		select {
		case n := <-p.barriers:
			if err := p.request(n); err != nil {
				return err
			}
		case e := <-p.in:
			if err := p.handle(e); err != nil {
				return err
			}
		case <-p.forceC:
			if err := p.forceWaiting(); err != nil {
				return err
			}
		case <-p.stopCh:
			return nil
		case <-p.ctx.Done():
			return p.ctx.Err()
		}
	}
}

func (p *sourcePump) handle(e emit) error {
	if e.kind == emitDone {
		return p.finish()
	}
	p.emitted.Add(1)
	return p.route(e)
}

// route applies an emit, or holds it until its split's stream has the
// pending barrier on the split's lane.
func (p *sourcePump) route(e emit) error {
	st := p.streams[e.stream]
	sp := st.byID[e.split]
	lane := -1
	switch l, adding := st.adding[e.split]; {
	case sp == nil && adding:
		lane = l
	case sp == nil:
		return fmt.Errorf("runner: source emitted on %s, which it doesn't have", splitName(e.stream, e.split))
	case p.pending != 0 && sp.placed && !st.lanes[sp.lane].placed:
		lane = sp.lane
	}
	if lane < 0 {
		return p.apply(e.stream, st, sp, e)
	}
	st.lanes[lane].held = append(st.lanes[lane].held, e)
	for _, a := range e.added {
		st.adding[a.ID] = lane
	}
	return nil
}

func (p *sourcePump) apply(name string, st *streamPos, sp *splitPos, e emit) error {
	if sp.complete {
		return fmt.Errorf("runner: source emitted on %s after completing it", splitName(name, sp.id))
	}
	switch e.kind {
	case emitRows, emitBatch:
		f := Frame{Kind: FrameRows, Lane: sp.lane, Stream: name, Rows: e.rows}
		sp.rows += uint64(len(e.rows))
		switch {
		case e.batch != nil:
			f = Frame{Kind: FrameBatch, Lane: sp.lane, Stream: name, Batch: e.batch}
			p.bytes += util.TotalRecordSize(e.batch)
			sp.rows += uint64(e.batch.NumRows()) //nolint:gosec // never negative
		case e.kind == emitBatch:
			f = Frame{Kind: FrameBatch, Lane: sp.lane, Stream: name, IPC: e.encoded.Bytes}
			p.bytes += e.encoded.Size
			sp.rows += uint64(e.encoded.Rows) //nolint:gosec // never negative
		}
		for _, r := range e.rows {
			p.bytes += int64(len(r))
		}
		sp.dirty, sp.active = true, true
		sp.reread = sp.reread || sp.rereading
		if err := p.forward(f); err != nil {
			return err
		}
		if p.maxBytes > 0 && p.bytes >= p.maxBytes && !p.thresholdSent {
			p.thresholdSent = true
			return p.notify(event{kind: evThreshold})
		}
		return nil
	case emitComplete:
		sp.complete = true
	}
	// A resume point, which completion also is.
	before := sp.placed
	sp.resume, sp.dirty, sp.forced, sp.rereading, sp.active = e.state, false, false, false, true
	added, err := p.add(name, st, e.added, p.pending != 0 && before)
	if err != nil {
		return err
	}
	if p.pending == 0 {
		return nil
	}
	if !sp.placed {
		if err := p.place(name, sp, false); err != nil {
			return err
		}
	}
	// Splits it adds with the barrier start at it, so they place it at once.
	for _, a := range added {
		if !a.placed && p.pending != 0 {
			if err := p.place(name, a, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// add adds splits to a stream, each to the lane with the fewest of the
// stream's open splits. later: they join after the pending barrier.
func (p *sourcePump) add(name string, st *streamPos, splits []source.Split, later bool) ([]*splitPos, error) {
	var out []*splitPos
	for _, s := range splits {
		if st.byID[s.ID] != nil {
			return nil, fmt.Errorf("runner: source added %s, which the stream already has", splitName(name, s.ID))
		}
		open := make([]int, p.lanes)
		for _, sp := range st.splits {
			if !sp.complete {
				open[sp.lane]++
			}
		}
		pos := &splitPos{id: s.ID, lane: slices.Index(open, slices.Min(open)), resume: s.State, placed: later, later: later}
		st.splits = append(st.splits, pos)
		st.byID[s.ID] = pos
		delete(st.adding, s.ID)
		if p.pending != 0 && !later {
			p.unplaced++
		}
		out = append(out, pos)
	}
	return out, nil
}

func splitName(stream, split string) string {
	if split == source.WholeStream {
		return fmt.Sprintf("stream %q", stream)
	}
	return fmt.Sprintf("split %q of stream %q", split, stream)
}

// finish runs when Read has returned: every waiting split is placed now.
func (p *sourcePump) finish() error {
	p.done = true
	if p.pending != 0 {
		if err := p.placeRemaining(); err != nil {
			return err
		}
	}
	active := false
	for _, st := range p.streams {
		active = active || slices.ContainsFunc(st.splits, func(sp *splitPos) bool { return sp.active })
	}
	return p.notify(event{kind: evReadDone, active: active, epoch: p.requested})
}

func (p *sourcePump) request(n uint64) error {
	if p.pending != 0 {
		return fmt.Errorf("runner: barrier %d requested while %d is being placed", n, p.pending)
	}
	p.requested, p.pending, p.unplaced = n, n, 0
	p.splits = make(map[string][]checkpoint.SplitState, len(p.order))
	p.rows = make(map[string]uint64, len(p.order))
	p.bytes, p.thresholdSent = 0, false
	for _, st := range p.streams {
		for _, sp := range st.splits {
			sp.placed, sp.later = false, false
			p.unplaced++
		}
		for l := range st.lanes {
			st.lanes[l].placed = false
		}
	}
	// Lanes without splits of a stream are at its barrier.
	for _, name := range p.order {
		for l := range p.lanes {
			if !slices.ContainsFunc(p.streams[name].splits, func(sp *splitPos) bool { return sp.lane == l }) {
				if err := p.placeLane(name, l); err != nil {
					return err
				}
			}
		}
	}
	if p.done {
		return p.placeRemaining()
	}
	// Nothing emitted since its last resume point: it's at one.
	for _, name := range p.order {
		for _, sp := range slices.Clone(p.streams[name].splits) {
			if !sp.dirty && !sp.placed && p.pending != 0 {
				if err := p.place(name, sp, false); err != nil {
					return err
				}
			}
		}
	}
	if p.pending != 0 && p.forceAfter > 0 {
		p.forceTimer = time.NewTimer(p.forceAfter)
		p.forceC = p.forceTimer.C
	}
	return nil
}

func (p *sourcePump) placeRemaining() error {
	for _, name := range p.order {
		st := p.streams[name]
		for _, sp := range slices.Clone(st.splits) {
			if p.pending == 0 || sp.placed {
				continue
			}
			if !sp.dirty {
				if err := p.place(name, sp, false); err != nil {
					return err
				}
				continue
			}
			if !st.canForce {
				return fmt.Errorf("runner: %s ended without a final resume point, and placing its barrier without one would lower it below the pipeline's required delivery level", splitName(name, sp.id))
			}
			if err := p.place(name, sp, true); err != nil {
				return err
			}
		}
	}
	return nil
}

// forceWaiting runs at resumePointTimeout and forces the barrier on every
// waiting split that can take it.
func (p *sourcePump) forceWaiting() error {
	p.forceC = nil
	for _, name := range p.order {
		st := p.streams[name]
		for _, sp := range slices.Clone(st.splits) {
			if p.pending != 0 && !sp.placed && sp.dirty && st.canForce {
				if err := p.place(name, sp, true); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (p *sourcePump) place(name string, sp *splitPos, forced bool) error {
	if p.pending == 0 {
		return errors.New("runner: placing a barrier with none pending")
	}
	st := p.streams[name]
	sp.placed, sp.active = true, false
	sp.forced = sp.forced || forced
	p.splits[name] = append(p.splits[name], checkpoint.SplitState{ID: sp.id, State: sp.resume, Complete: sp.complete && !sp.forced, Forced: sp.forced, Reread: sp.reread})
	p.rows[name] += sp.rows
	sp.rows, sp.reread = 0, false
	p.unplaced--
	n := p.pending
	if p.unplaced == 0 {
		// Before the last lane's barrier goes out, so a sink that sees it
		// knows which reads end in the epoch.
		for _, s := range p.order {
			at := p.splits[s]
			if st := p.streams[s]; !st.ended && !slices.ContainsFunc(at, func(sp checkpoint.SplitState) bool { return !sp.Complete }) {
				st.ended = true
				p.ends.set(s, n)
			}
		}
		p.ends.setPlaced(n)
	}
	if !slices.ContainsFunc(st.splits, func(o *splitPos) bool { return o.lane == sp.lane && !o.placed }) {
		if err := p.placeLane(name, sp.lane); err != nil {
			return err
		}
	}
	if p.unplaced > 0 || p.pending != n {
		return nil
	}
	splits := p.splits
	for name, ss := range splits {
		slices.SortStableFunc(ss, func(a, b checkpoint.SplitState) int { return p.joined(name, a.ID) - p.joined(name, b.ID) })
	}
	ev := event{kind: evPlaced, epoch: n, splits: splits, rows: p.rows}
	p.pending = 0
	if p.forceTimer != nil {
		p.forceTimer.Stop()
		p.forceTimer, p.forceC = nil, nil
	}
	return p.notify(ev)
}

// joined is the order a split joined its stream in.
func (p *sourcePump) joined(stream, id string) int {
	return slices.IndexFunc(p.streams[stream].splits, func(sp *splitPos) bool { return sp.id == id })
}

// placeLane forwards the stream's pending barrier on the lane, then what
// its splits there emitted after placing it.
func (p *sourcePump) placeLane(name string, lane int) error {
	st := p.streams[name]
	l := &st.lanes[lane]
	if l.placed {
		return nil
	}
	l.placed = true
	if err := p.forward(Frame{Kind: FrameBarrier, Lane: lane, Stream: name, Epoch: p.pending}); err != nil {
		return err
	}
	held := l.held
	l.held = nil
	for _, e := range held {
		if err := p.route(e); err != nil {
			return err
		}
	}
	return nil
}

func (p *sourcePump) forward(f Frame) error {
	select {
	case p.out <- f:
		return nil
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

func (p *sourcePump) notify(ev event) error {
	select {
	case p.events <- ev:
		return nil
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}
