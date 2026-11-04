package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/util"

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
	rows    []json.RawMessage
	batch   arrow.RecordBatch
	encoded arrowipc.Encoded // an emitBatch without batch
	state   []byte
}

type streamPos struct {
	resume   []byte // state at the last resume point, or restored
	rows     uint64 // emitted since the last barrier
	dirty    bool   // emitted since the last resume point
	active   bool   // emitted since the last barrier
	complete bool
	placed   bool // the pending barrier is placed
	// resume came from a forced barrier. Every checkpoint recording it must
	// list the stream as forced until the next real resume point.
	forced bool
	// A forced barrier keeps the stream at or above require.
	canForce bool
	// The barrier that ends the read is placed.
	ended bool
}

// readEnds records the barrier that ends each stream's read, for the sink
// pump, which sees it come out of the dataflow. The source pump records it
// before forwarding the barrier.
type readEnds struct {
	mu sync.Mutex
	at map[string]uint64
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
// every stream right after that stream's next resume point. Only run touches
// its state.
type sourcePump struct {
	ctx      context.Context
	in       chan emit
	barriers chan uint64
	stopCh   chan struct{}
	out      chan<- Frame
	events   chan<- event
	ends     *readEnds

	order   []string // stream names, in configuration order
	streams map[string]*streamPos
	known   map[string]bool // read by Emitter methods on the connector's goroutine

	maxBytes      int64
	forceAfter    time.Duration
	requested     uint64 // the last barrier requested
	pending       uint64 // epoch being placed; 0 if none
	unplaced      int
	states        map[string][]byte
	forced        []string
	complete      map[string]bool
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

// Streams in done completed earlier in the sync, and aren't read.
func newSourcePump(ctx context.Context, job *Job, restoredEpoch uint64, restored map[string][]byte, restoredForced, canForce, done map[string]bool, ends *readEnds, out chan<- Frame, events chan<- event) *sourcePump {
	p := &sourcePump{
		ctx:        ctx,
		requested:  restoredEpoch,
		in:         make(chan emit),
		barriers:   make(chan uint64, 1),
		stopCh:     make(chan struct{}),
		out:        out,
		events:     events,
		ends:       ends,
		streams:    make(map[string]*streamPos, len(job.Streams)),
		known:      make(map[string]bool, len(job.Streams)),
		maxBytes:   job.Settings.CommitMaxBytes,
		forceAfter: job.Settings.ResumePointTimeout,
		progress:   job.Progress,
	}
	for _, s := range job.Streams {
		p.order = append(p.order, s.Name)
		p.streams[s.Name] = &streamPos{resume: restored[s.Name], forced: restoredForced[s.Name], canForce: canForce[s.Name], complete: done[s.Name], ended: done[s.Name]}
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
	st := p.streams[e.stream]
	if st.complete {
		return fmt.Errorf("runner: source emitted on stream %q after completing it", e.stream)
	}
	p.emitted.Add(1)
	switch e.kind {
	case emitRows, emitBatch:
		f := Frame{Kind: FrameRows, Stream: e.stream, Rows: e.rows}
		st.rows += uint64(len(e.rows))
		switch {
		case e.batch != nil:
			f = Frame{Kind: FrameBatch, Stream: e.stream, Batch: e.batch}
			p.bytes += util.TotalRecordSize(e.batch)
			st.rows += uint64(e.batch.NumRows()) //nolint:gosec // never negative
		case e.kind == emitBatch:
			f = Frame{Kind: FrameBatch, Stream: e.stream, IPC: e.encoded.Bytes}
			p.bytes += e.encoded.Size
			st.rows += uint64(e.encoded.Rows) //nolint:gosec // never negative
		}
		for _, r := range e.rows {
			p.bytes += int64(len(r))
		}
		st.dirty, st.active = true, true
		if err := p.forward(f); err != nil {
			return err
		}
		if p.maxBytes > 0 && p.bytes >= p.maxBytes && !p.thresholdSent {
			p.thresholdSent = true
			return p.notify(event{kind: evThreshold})
		}
	case emitResume:
		st.resume, st.dirty, st.active, st.forced = e.state, false, true, false
		if p.pending != 0 && !st.placed {
			return p.place(e.stream, false)
		}
	case emitComplete:
		// A resume point that a record must also show as complete.
		st.resume, st.dirty, st.complete, st.active, st.forced = e.state, false, true, true, false
		if p.pending != 0 && !st.placed {
			return p.place(e.stream, false)
		}
	}
	return nil
}

// finish runs when Read has returned: every waiting stream is placed now.
func (p *sourcePump) finish() error {
	p.done = true
	if p.pending != 0 {
		if err := p.placeRemaining(); err != nil {
			return err
		}
	}
	active := false
	for _, st := range p.streams {
		active = active || st.active
	}
	return p.notify(event{kind: evReadDone, active: active, epoch: p.requested})
}

func (p *sourcePump) request(n uint64) error {
	if p.pending != 0 {
		return fmt.Errorf("runner: barrier %d requested while %d is being placed", n, p.pending)
	}
	p.requested, p.pending, p.unplaced = n, n, len(p.order)
	p.states = make(map[string][]byte, len(p.order))
	p.forced = nil
	p.complete = make(map[string]bool)
	p.rows = make(map[string]uint64, len(p.order))
	p.bytes, p.thresholdSent = 0, false
	for _, st := range p.streams {
		st.placed = false
	}
	if p.done {
		return p.placeRemaining()
	}
	// Nothing emitted since its last resume point: it's at one.
	for _, name := range p.order {
		if !p.streams[name].dirty {
			if err := p.place(name, false); err != nil {
				return err
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
		if p.pending == 0 || st.placed {
			continue
		}
		if !st.dirty {
			if err := p.place(name, false); err != nil {
				return err
			}
			continue
		}
		if !st.canForce {
			return fmt.Errorf("runner: stream %q ended without a final resume point, and placing its barrier without one would lower it below the pipeline's required delivery level", name)
		}
		if err := p.place(name, true); err != nil {
			return err
		}
	}
	return nil
}

// forceWaiting runs at resumePointTimeout and forces the barrier on every
// waiting stream that can take it.
func (p *sourcePump) forceWaiting() error {
	p.forceC = nil
	for _, name := range p.order {
		st := p.streams[name]
		if p.pending != 0 && !st.placed && st.dirty && st.canForce {
			if err := p.place(name, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *sourcePump) place(name string, forced bool) error {
	if p.pending == 0 {
		return errors.New("runner: placing a barrier with none pending")
	}
	st := p.streams[name]
	if st.complete && !st.forced && !forced && !st.ended {
		st.ended = true
		p.ends.set(name, p.pending)
	}
	if err := p.forward(Frame{Kind: FrameBarrier, Stream: name, Epoch: p.pending}); err != nil {
		return err
	}
	st.placed, st.active = true, false
	st.forced = st.forced || forced
	p.states[name] = st.resume
	p.rows[name], st.rows = st.rows, 0
	if st.forced {
		p.forced = append(p.forced, name)
	} else if st.complete {
		p.complete[name] = true
	}
	p.unplaced--
	if p.unplaced > 0 {
		return nil
	}
	ev := event{kind: evPlaced, epoch: p.pending, states: p.states, forced: p.forced, complete: p.complete, rows: p.rows}
	p.pending = 0
	if p.forceTimer != nil {
		p.forceTimer.Stop()
		p.forceTimer, p.forceC = nil, nil
	}
	return p.notify(ev)
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
