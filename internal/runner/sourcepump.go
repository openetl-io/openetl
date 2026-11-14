package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/util"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/source"
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
	id string
	// Set when it first emits, so the splits being read keep to lanes of
	// their own; -1 before.
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
	// When it first emitted, and whether the source refused to split it.
	started time.Time
	unsplit bool
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
	// Its frames go to the lanes in turn: order doesn't matter to it, so all
	// of them can do its work. Its splits share lanes[0], and its barrier goes
	// on every lane at once.
	inTurn bool
	turn   int
}

type laneHold struct {
	placed bool
	held   []emit
}

// readEnds records the barrier that ends each stream's read, for the sink
// pumps. It's set before the last of the stream's lanes passes that barrier,
// so that lane's writer knows. A writer never waits for it: that would hold
// up what the source emits for its lane, and with it what the source emits
// after, which can be the very barrier it would be waiting for.
type readEnds struct {
	mu sync.Mutex
	at map[string]uint64
}

func newReadEnds() *readEnds {
	return &readEnds{at: map[string]uint64{}}
}

func (r *readEnds) set(stream string, epoch uint64) {
	r.mu.Lock()
	r.at[stream] = epoch
	r.mu.Unlock()
}

func (r *readEnds) endsAt(stream string, epoch uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.at[stream] == epoch
}

type readEnder interface {
	endsAt(stream string, epoch uint64) bool
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
	// By lane: the splits emitting on it.
	load []int
	// Asked to split a straggler when a lane has nothing to do.
	resplitter  source.Resplitter
	resplitting chan resplitAnswer
	log         *slog.Logger

	order   []string // stream names, in configuration order
	streams map[string]*streamPos
	known   map[string]bool // read by Emitter methods on the connector's goroutine

	maxBytes      atomic.Int64
	meter         *resources.Meter
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
	// A worker's: it reports its first emit after each barrier requested,
	// and the lanes of the splits each epoch got rows from.
	worker     bool
	emitTold   bool
	splitLanes map[checkpoint.SplitRef]int
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
		load:       make([]int, job.lanes()),
		streams:    make(map[string]*streamPos, len(job.Streams)),
		known:      make(map[string]bool, len(job.Streams)),
		meter:      job.Meter,
		forceAfter: job.Settings.ResumePointTimeout,
		progress:   job.Progress,
		log:        job.Logger,
	}
	if p.log == nil {
		p.log = slog.New(slog.DiscardHandler)
	}
	if job.canResplit(ctx) {
		p.resplitter, _ = job.Source.(source.Resplitter)
	}
	p.maxBytes.Store(job.Settings.CommitMaxBytes)
	for _, s := range job.Streams {
		p.order = append(p.order, s.Name)
		st := &streamPos{byID: map[string]*splitPos{}, canForce: canForce[s.Name], ended: done[s.Name], lanes: make([]laneHold, p.lanes), adding: map[string]int{}, inTurn: job.inTurn(s)}
		for _, sp := range splits[s.Name] {
			pos := &splitPos{id: sp.ID, lane: st.firstLane(), resume: sp.State, complete: sp.Complete, forced: sp.Forced, rereading: sp.Forced}
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
	var resplit <-chan time.Time
	if p.resplitter != nil && p.lanes > 1 {
		t := time.NewTicker(resplitEvery)
		defer t.Stop()
		resplit = t.C
	}
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
		case <-resplit:
			p.resplit()
		case a := <-p.resplitting:
			p.resplitting = nil
			a.sp.unsplit = !a.accepted
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
	if p.worker && !p.emitTold {
		p.emitTold = true
		if err := p.notify(event{kind: evEmitted, epoch: p.requested}); err != nil {
			return err
		}
	}
	return p.route(e)
}

func (st *streamPos) firstLane() int {
	if st.inTurn {
		return 0
	}
	return -1
}

// assign gives a split that's starting to emit the lane emitting the least.
func (p *sourcePump) assign(sp *splitPos) {
	sp.lane = slices.Index(p.load, slices.Min(p.load))
	p.load[sp.lane]++
	sp.started = time.Now()
}

// How often a lane with nothing to do asks for a straggler to be split.
const resplitEvery = 5 * time.Second

type resplitAnswer struct {
	sp       *splitPos
	accepted bool
}

// resplit asks the source to split the split that's been reading longest, if
// a lane has nothing to do, and no split is waiting to start. Once it adds a
// split, a lane takes it when it starts.
func (p *sourcePump) resplit() {
	if p.done || p.resplitting != nil || slices.Min(p.load) > 0 {
		return
	}
	var stream string
	var longest *splitPos
	for _, name := range p.order {
		st := p.streams[name]
		if st.ended || st.inTurn {
			continue
		}
		for _, sp := range st.splits {
			if sp.complete || sp.unsplit {
				continue
			}
			if sp.lane < 0 {
				return
			}
			if longest == nil || sp.started.Before(longest.started) {
				stream, longest = name, sp
			}
		}
	}
	if longest == nil {
		return
	}
	id, answer := longest.id, make(chan resplitAnswer, 1)
	p.resplitting = answer
	sp := longest
	go func() {
		accepted, err := p.resplitter.Resplit(p.ctx, stream, id)
		if err != nil && p.ctx.Err() == nil {
			p.log.Warn("asking to split a split failed", "stream", stream, "split", id, "err", err)
		}
		answer <- resplitAnswer{sp: sp, accepted: accepted}
	}()
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
	default:
		if sp.lane < 0 && !sp.complete && (e.kind == emitRows || e.kind == emitBatch) {
			p.assign(sp)
		}
		if p.pending != 0 && sp.placed && sp.lane >= 0 && !st.lanes[sp.lane].placed {
			lane = sp.lane
		}
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
		lane := sp.lane
		if st.inTurn {
			lane = st.turn % p.lanes
			st.turn++
		}
		f := Frame{Kind: FrameRows, Lane: lane, Stream: name, Rows: e.rows}
		rows, bytes := int64(len(e.rows)), int64(0)
		switch {
		case e.batch != nil:
			f = Frame{Kind: FrameBatch, Lane: lane, Stream: name, Batch: e.batch}
			rows += e.batch.NumRows()
			bytes += util.TotalRecordSize(e.batch)
		case e.kind == emitBatch:
			f = Frame{Kind: FrameBatch, Lane: lane, Stream: name, IPC: e.encoded.Bytes}
			rows += e.encoded.Rows
			bytes += e.encoded.Size
		}
		for _, r := range e.rows {
			bytes += int64(len(r))
		}
		sp.rows += uint64(rows)
		p.bytes += bytes
		p.meter.Moved(rows, bytes)
		sp.dirty, sp.active = true, true
		sp.reread = sp.reread || sp.rereading
		if err := p.forward(f); err != nil {
			return err
		}
		if limit := p.maxBytes.Load(); limit > 0 && p.bytes >= limit && !p.thresholdSent {
			p.thresholdSent = true
			return p.notify(event{kind: evThreshold})
		}
		return nil
	case emitComplete:
		sp.complete = true
		if sp.lane >= 0 && !st.inTurn {
			p.load[sp.lane]--
		}
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

// add adds splits to a stream. later: they join after the pending barrier.
func (p *sourcePump) add(name string, st *streamPos, splits []source.Split, later bool) ([]*splitPos, error) {
	var out []*splitPos
	for _, s := range splits {
		if st.byID[s.ID] != nil {
			return nil, fmt.Errorf("runner: source added %s, which the stream already has", splitName(name, s.ID))
		}
		pos := &splitPos{id: s.ID, lane: st.firstLane(), resume: s.State, placed: later, later: later}
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
	if p.worker {
		p.emitTold, p.splitLanes = false, map[checkpoint.SplitRef]int{}
	}
	for _, st := range p.streams {
		for _, sp := range st.splits {
			sp.placed, sp.later = false, false
			p.unplaced++
		}
		for l := range st.lanes {
			st.lanes[l].placed = false
		}
	}
	// A read that has ended places what's left, and a split with nothing
	// emitted since its last resume point is at one. Before lanes get their
	// barriers, so the lane that gets a stream's last one knows if its read
	// ended.
	if p.done {
		if err := p.placeRemaining(); err != nil {
			return err
		}
	} else {
		for _, name := range p.order {
			for _, sp := range slices.Clone(p.streams[name].splits) {
				if !sp.dirty && !sp.placed && p.pending != 0 {
					if err := p.place(name, sp, false); err != nil {
						return err
					}
				}
			}
		}
	}
	if p.pending == 0 {
		return nil
	}
	// Lanes without splits of a stream still to place it are at its barrier.
	for _, name := range p.order {
		if err := p.placeFreeLanes(name); err != nil {
			return err
		}
	}
	// A worker may have no splits to place.
	if p.worker && p.pending != 0 && p.unplaced == 0 {
		return p.placed()
	}
	if p.pending != 0 && p.forceAfter > 0 {
		p.forceTimer = time.NewTimer(p.forceAfter)
		p.forceC = p.forceTimer.C
	}
	return nil
}

// placeFreeLanes forwards the stream's pending barrier on every lane none of
// its unplaced splits are on.
func (p *sourcePump) placeFreeLanes(name string) error {
	st := p.streams[name]
	for l := range p.lanes {
		if st.inTurn && l > 0 {
			break
		}
		if !slices.ContainsFunc(st.splits, func(sp *splitPos) bool { return sp.lane == l && !sp.placed }) {
			if err := p.placeLane(name, l); err != nil {
				return err
			}
		}
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
	if p.worker && sp.rows > 0 {
		lane := sp.lane
		if st.inTurn {
			lane = laneAll
		}
		p.splitLanes[checkpoint.SplitRef{Stream: name, ID: sp.id}] = lane
	}
	sp.rows, sp.reread = 0, false
	p.unplaced--
	n := p.pending
	// Before the last lane's barrier goes out, so the sink that sees it knows
	// the read ended in the epoch.
	if !st.ended && !slices.ContainsFunc(st.splits, func(o *splitPos) bool { return !o.placed }) &&
		!slices.ContainsFunc(p.splits[name], func(sp checkpoint.SplitState) bool { return !sp.Complete }) {
		st.ended = true
		p.ends.set(name, n)
	}
	if sp.lane >= 0 && !slices.ContainsFunc(st.splits, func(o *splitPos) bool { return o.lane == sp.lane && !o.placed }) {
		if err := p.placeLane(name, sp.lane); err != nil {
			return err
		}
	}
	if p.unplaced > 0 || p.pending != n {
		return nil
	}
	for _, s := range p.order {
		if err := p.placeFreeLanes(s); err != nil {
			return err
		}
	}
	if p.unplaced > 0 || p.pending != n {
		return nil
	}
	return p.placed()
}

// placed reports the pending barrier placed on every split.
func (p *sourcePump) placed() error {
	splits := p.splits
	for name, ss := range splits {
		slices.SortStableFunc(ss, func(a, b checkpoint.SplitState) int { return p.joined(name, a.ID) - p.joined(name, b.ID) })
	}
	ev := event{kind: evPlaced, epoch: p.pending, splits: splits, rows: p.rows, splitLanes: p.splitLanes}
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
	if st.inTurn {
		for each := range p.lanes {
			if err := p.forward(Frame{Kind: FrameBarrier, Lane: each, Stream: name, Epoch: p.pending}); err != nil {
				return err
			}
		}
	} else if err := p.forward(Frame{Kind: FrameBarrier, Lane: lane, Stream: name, Epoch: p.pending}); err != nil {
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
