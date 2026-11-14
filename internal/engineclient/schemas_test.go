package engineclient

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/runner/attempt"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// sharedOwner is one stream's schema owner for several engines, each of
// which reaches it through a view.
type sharedOwner struct {
	mu      sync.Mutex
	version uint64
	schema  []byte
	views   []*ownerView
}

type ownerView struct {
	o       *sharedOwner
	updates chan runner.SchemaUpdate
}

func (o *sharedOwner) view() *ownerView {
	v := &ownerView{o: o, updates: make(chan runner.SchemaUpdate, 64)}
	o.views = append(o.views, v)
	return v
}

func (o *sharedOwner) newest() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.version
}

func (v *ownerView) Propose(stream string, base uint64, schema []byte) error {
	o := v.o
	o.mu.Lock()
	defer o.mu.Unlock()
	to := []*ownerView{v}
	if base == o.version {
		o.version++
		o.schema = schema
		to = o.views
	}
	for _, w := range to {
		w.updates <- runner.SchemaUpdate{Stream: stream, Version: o.version, Schema: o.schema}
	}
	return nil
}

func (v *ownerView) Updates() <-chan runner.SchemaUpdate { return v.updates }

// enginePair is two engines of one attempt, whose stream s has one owner.
type enginePair struct {
	t       *testing.T
	ctx     context.Context
	owner   *sharedOwner
	engines [2]pairedEngine
}

type pairedEngine struct {
	in  chan runner.Frame
	out chan runner.Frame
	err chan error
}

// startPair starts the engines; without an engine binary the test skips.
func startPair(t *testing.T) *enginePair {
	opts := options(t, memory.NewGoAllocator())
	p := &enginePair{t: t, ctx: testContext(t), owner: &sharedOwner{}}
	for i := range p.engines {
		pl := plan("s")
		pl.SchemaOwner, pl.FirstLane = p.owner.view(), i
		e := pairedEngine{in: make(chan runner.Frame), out: make(chan runner.Frame, 64), err: make(chan error, 1)}
		p.engines[i] = e
		go func() { e.err <- (&Dataflow{Options: opts}).Run(p.ctx, pl, e.in, e.out) }()
	}
	return p
}

// write sends engine i a row of s, and returns the schema of the batch it
// writes of it.
func (p *enginePair) write(i int, row string) *arrow.Schema {
	p.t.Helper()
	select {
	case p.engines[i].in <- runner.Frame{Kind: runner.FrameRows, Stream: "s", Rows: rows(row)}:
	case <-p.ctx.Done():
		p.t.Fatal(p.ctx.Err())
	}
	for {
		select {
		case f := <-p.engines[i].out:
			if f.Kind != runner.FrameBatch {
				continue
			}
			s := f.Batch.Schema()
			f.Batch.Release()
			return s
		case <-p.ctx.Done():
			p.t.Fatal(p.ctx.Err())
			return nil
		}
	}
}

// until writes rows to engine i until it writes a batch whose schema has
// what ok wants: it shapes by the owner's newest version once it has it.
func (p *enginePair) until(i int, row string, ok func(*arrow.Schema) bool) *arrow.Schema {
	p.t.Helper()
	for {
		if s := p.write(i, row); ok(s) {
			return s
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-p.ctx.Done():
			p.t.Fatalf("engine %d never wrote the schema wanted; the owner is at version %d", i, p.owner.newest())
		}
	}
}

// end ends both engines' jobs, which must succeed.
func (p *enginePair) end() {
	p.t.Helper()
	for i, e := range p.engines {
		e.in <- runner.Frame{Kind: runner.FrameBarrier, Stream: "s", Epoch: 5}
		close(e.in)
		for f := range e.out {
			if f.Batch != nil {
				f.Batch.Release()
			}
		}
		if err := <-e.err; err != nil {
			p.t.Fatalf("engine %d: %v", i, err)
		}
	}
}

func columnNames(s *arrow.Schema) []string {
	var names []string
	for _, f := range s.Fields() {
		names = append(names, f.Name)
	}
	return names
}

// Two engines that run one attempt shape a stream by one owner's schema:
// each adds a column the other hasn't seen, one change is taken first, the
// other is proposed again from it, and both write the same columns.
func TestEnginesShapeByOneOwner(t *testing.T) {
	p := startPair(t)
	p.write(0, `{"id": 1, "a": "x"}`)
	p.write(1, `{"id": 2, "b": true}`)
	both := func(s *arrow.Schema) bool {
		return len(s.FieldIndices("a")) == 1 && len(s.FieldIndices("b")) == 1
	}
	got := [][]string{
		columnNames(p.until(0, `{"id": 3}`, both)),
		columnNames(p.until(1, `{"id": 4}`, both)),
	}
	p.end()
	if !slices.Equal(got[0], got[1]) || p.owner.newest() != 2 {
		t.Fatalf("the engines wrote %v and %v, the owner at version %d", got[0], got[1], p.owner.newest())
	}
}

// One engine sees integers past ±2^53 in a column, another a float in it:
// the owner learns of the first, so the column becomes text in both, as in
// one engine, and not a float that can't hold those integers.
func TestEnginesShareWhatTheySawOfBigIntegers(t *testing.T) {
	p := startPair(t)
	xIs := func(typ arrow.DataType) func(*arrow.Schema) bool {
		return func(s *arrow.Schema) bool {
			i := s.FieldIndices("x")
			return len(i) == 1 && arrow.TypeEqual(s.Field(i[0]).Type, typ)
		}
	}
	p.until(0, `{"id": 1, "x": 1}`, xIs(arrow.PrimitiveTypes.Int64))
	p.until(0, `{"id": 2, "x": 9007199254740993}`, xIs(arrow.PrimitiveTypes.Int64))
	p.until(1, `{"id": 3, "x": 1.5}`, xIs(arrow.BinaryTypes.String))
	p.until(0, `{"id": 4, "x": 5}`, xIs(arrow.BinaryTypes.String))
	p.end()
	p.owner.mu.Lock()
	defer p.owner.mu.Unlock()
	s, err := arrowipc.DecodeSchema(p.owner.schema)
	if err != nil {
		t.Fatal(err)
	}
	if !xIs(arrow.BinaryTypes.String)(s) {
		t.Fatalf("the owner's schema is %v", s)
	}
}

// refusingFlow runs the engine with a schema owner that refuses every
// proposal.
type refusingFlow struct{ df runner.Dataflow }

type refusingOwner struct{ runner.SchemaOwner }

func (refusingOwner) Propose(string, uint64, []byte) error {
	return errors.New("the owner refused")
}

func (f refusingFlow) Run(ctx context.Context, plan runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	plan.SchemaOwner = refusingOwner{plan.SchemaOwner}
	return f.df.Run(ctx, plan, in, out)
}

// A proposal the owner can't take ends the worker's job, and with it the
// attempt, at once, with why.
func TestARefusedProposalEndsTheAttempt(t *testing.T) {
	opts := options(t, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	srv, err := attempt.Listen(ctx, attempt.ListenConfig{Address: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	dest := &laneDest{lanes: 16, staged: map[uint64]map[int][]int64{}, byLane: map[int64]int{}, ends: map[string]uint64{}}
	j := runner.Job{
		Pipeline: "p", Owner: "o", Log: checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"),
		Source:      &laneSource{splits: 2, chunks: 2, rows: 10},
		Streams:     []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append}},
		Destination: dest, Dataflow: refusingFlow{&Dataflow{Options: opts}},
		Settings: delivery.Settings{Require: delivery.ExactlyOnce, CommitMaxBytes: 1 << 10, Scope: destination.ScopePipeline},
	}
	timings := attempt.Options{Heartbeat: 100 * time.Millisecond, Timeout: 10 * time.Second}
	ended := make(chan error, 1)
	worker := j
	worker.Log = nil
	j.Workers = &runner.Workers{Count: 1, Server: srv, Options: timings, Launch: func(_ context.Context, targets []attempt.Target) error {
		go func() { ended <- runner.RunWorker(ctx, worker, targets[0], timings) }()
		return nil
	}}
	start := time.Now()
	_, err = runner.RunAttempt(ctx, j)
	took := time.Since(start)
	if err == nil || !strings.Contains(err.Error(), "the owner refused") {
		t.Fatalf("the attempt: %v", err)
	}
	if werr := <-ended; werr == nil || !strings.Contains(werr.Error(), "the owner refused") {
		t.Fatalf("the worker: %v", werr)
	}
	if took > 10*time.Second {
		t.Errorf("the attempt took %s to end", took)
	}
}

// An attempt on three workers, each with an engine of its own, lands every
// row once, and its records hold the schema the workers' engines shaped by,
// with a column only one worker's rows have.
func TestAnAttemptOnWorkersThroughTheEngine(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	srv, err := attempt.Listen(ctx, attempt.ListenConfig{Address: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
	dest := &laneDest{lanes: 16, staged: map[uint64]map[int][]int64{}, byLane: map[int64]int{}, ends: map[string]uint64{}}
	opts := options(t, nil)
	job := func() runner.Job {
		return runner.Job{
			Pipeline: "p", Owner: "o", Sync: "s1", Log: log,
			Source:      &laneSource{splits: 6, chunks: 10, rows: 100, extra: "4"},
			Streams:     []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append}},
			Destination: dest, Dataflow: &Dataflow{Options: opts}, Lanes: 2,
			Settings: delivery.Settings{Require: delivery.ExactlyOnce, CommitMaxBytes: 16 << 10, Scope: destination.ScopePipeline},
		}
	}
	timings := attempt.Options{Heartbeat: 100 * time.Millisecond, Timeout: 10 * time.Second}
	var workers sync.WaitGroup
	j := job()
	j.Workers = &runner.Workers{Count: 3, Server: srv, Options: timings, Launch: func(_ context.Context, targets []attempt.Target) error {
		for _, tg := range targets {
			wj := job()
			wj.Log = nil
			workers.Go(func() {
				if err := runner.RunWorker(ctx, wj, tg, timings); err != nil {
					t.Errorf("worker %d: %v", tg.Worker, err)
				}
			})
		}
		return nil
	}}
	res, err := runner.RunAttempt(ctx, j)
	workers.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if res.Committed < 2 {
		t.Errorf("committed %d epochs, want several", res.Committed)
	}
	seen := map[int64]bool{}
	for _, id := range dest.applied {
		if seen[id] {
			t.Fatalf("row %d applied twice", id)
		}
		seen[id] = true
	}
	if want := 6 * 10 * 100; len(seen) != want {
		t.Errorf("applied %d rows, want %d", len(seen), want)
	}
	head, err := log.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := arrowipc.DecodeSchema(head.Streams["s"].Schema)
	if err != nil {
		t.Fatal(err)
	}
	if len(schema.FieldIndices("extra")) != 1 || len(schema.FieldIndices("id")) != 1 {
		t.Errorf("the record's schema is %v", schema)
	}
}
