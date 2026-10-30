package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/util"
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
	kind   emitKind
	stream string
	rows   []json.RawMessage
	batch  arrow.RecordBatch
	state  []byte
}

// streamPos is the source pump's view of one stream.
type streamPos struct {
	resume   []byte // state at the last resume point (or the restored state)
	dirty    bool   // records emitted since the last resume point
	active   bool   // anything emitted since this stream's last barrier
	complete bool
	placed   bool // barrier for the pending epoch is placed
	// forced: resume was captured by a forced barrier, so records after it
	// may already be applied. Every checkpoint that records this state must
	// say so, until the stream reaches a real resume point.
	forced bool
	// canForce: placing a barrier without a resume point, which makes the
	// stream repeat records after a restore, keeps it at or above require.
	canForce bool
}

// sourcePump sits between a source connector and the dataflow. It is the
// connector's Emitter, and it places each requested barrier on every stream
// right after that stream's next resume point.
//
// One goroutine (run) owns all state. The connector's Emitter calls and the
// coordinator's barrier requests reach it over channels.
type sourcePump struct {
	ctx      context.Context
	in       chan emit
	barriers chan uint64
	stopCh   chan struct{}
	out      chan<- Frame
	events   chan<- event

	order   []string // stream names, in configuration order
	streams map[string]*streamPos
	known   map[string]bool // read by Emitter methods on the connector's goroutine

	maxBytes      int64
	forceAfter    time.Duration
	pending       uint64 // epoch being placed; 0 if none
	unplaced      int
	states        map[string][]byte
	forced        []string
	bytes         int64
	thresholdSent bool
	done          bool
	forceTimer    *time.Timer
	forceC        <-chan time.Time
}

func newSourcePump(ctx context.Context, job *Job, restored map[string][]byte, restoredForced, canForce map[string]bool, out chan<- Frame, events chan<- event) *sourcePump {
	p := &sourcePump{
		ctx:        ctx,
		in:         make(chan emit),
		barriers:   make(chan uint64, 1),
		stopCh:     make(chan struct{}),
		out:        out,
		events:     events,
		streams:    make(map[string]*streamPos, len(job.Streams)),
		known:      make(map[string]bool, len(job.Streams)),
		maxBytes:   job.Settings.CommitMaxBytes,
		forceAfter: job.Settings.ResumePointTimeout,
	}
	for _, s := range job.Streams {
		p.order = append(p.order, s.Name)
		p.streams[s.Name] = &streamPos{resume: restored[s.Name], forced: restoredForced[s.Name], canForce: canForce[s.Name]}
		p.known[s.Name] = true
	}
	return p
}

// Emitter methods, called on the connector's goroutine.

func (p *sourcePump) Rows(stream string, rows []json.RawMessage) error {
	return p.send(emit{kind: emitRows, stream: stream, rows: rows})
}

func (p *sourcePump) Records(stream string, batch arrow.RecordBatch) error {
	return p.send(emit{kind: emitBatch, stream: stream, batch: batch})
}

func (p *sourcePump) ResumePoint(stream string, state []byte) error {
	return p.send(emit{kind: emitResume, stream: stream, state: state})
}

func (p *sourcePump) Complete(stream string) error {
	return p.send(emit{kind: emitComplete, stream: stream})
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

// requestBarrier asks the pump to place barrier n. The coordinator calls it
// only when no other barrier is being placed.
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
		// Barrier requests first, so that a request and an emit that are both
		// ready are always handled in the same order.
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
	switch e.kind {
	case emitRows, emitBatch:
		f := Frame{Kind: FrameRows, Stream: e.stream, Rows: e.rows}
		if e.kind == emitBatch {
			f = Frame{Kind: FrameBatch, Stream: e.stream, Batch: e.batch}
			p.bytes += util.TotalRecordSize(e.batch)
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
		st.complete = true
		if p.pending != 0 && !st.placed && !st.dirty {
			return p.place(e.stream, false)
		}
	}
	return nil
}

// finish runs when Read has returned. No stream will emit again, so every
// stream still waiting for a resume point is placed now.
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
	return p.notify(event{kind: evReadDone, active: active})
}

func (p *sourcePump) request(n uint64) error {
	if p.pending != 0 {
		return fmt.Errorf("runner: barrier %d requested while %d is being placed", n, p.pending)
	}
	p.pending, p.unplaced = n, len(p.order)
	p.states = make(map[string][]byte, len(p.order))
	p.forced = nil
	p.bytes, p.thresholdSent = 0, false
	for _, st := range p.streams {
		st.placed = false
	}
	if p.done {
		return p.placeRemaining()
	}
	// A stream with nothing emitted since its last resume point is at one:
	// streams not started yet, idle streams and complete streams.
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

// forceWaiting runs when resumePointTimeout has passed: it places the barrier
// on every waiting stream that can take a forced barrier. The others keep
// waiting for a resume point.
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
	if err := p.forward(Frame{Kind: FrameBarrier, Stream: name, Epoch: p.pending}); err != nil {
		return err
	}
	st := p.streams[name]
	st.placed, st.active = true, false
	st.forced = st.forced || forced
	p.states[name] = st.resume
	if st.forced {
		p.forced = append(p.forced, name)
	}
	p.unplaced--
	if p.unplaced > 0 {
		return nil
	}
	ev := event{kind: evPlaced, epoch: p.pending, states: p.states, forced: p.forced}
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
