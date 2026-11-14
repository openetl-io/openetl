package engineclient

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

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

// Two engines that run one attempt shape a stream by one owner's schema:
// each adds a column the other hasn't seen, one change is taken first, the
// other is proposed again from it, and both write the same columns.
func TestEnginesShapeByOneOwner(t *testing.T) {
	mem := memory.NewGoAllocator()
	ctx := testContext(t)
	owner := &sharedOwner{}
	type engine struct {
		in  chan runner.Frame
		out chan runner.Frame
		err chan error
	}
	engines := make([]engine, 2)
	for i := range engines {
		p := plan("s")
		p.SchemaOwner, p.FirstLane = owner.view(), i
		e := engine{in: make(chan runner.Frame), out: make(chan runner.Frame, 64), err: make(chan error, 1)}
		engines[i] = e
		go func() { e.err <- (&Dataflow{Options: options(t, mem)}).Run(ctx, p, e.in, e.out) }()
	}
	send := func(i int, f runner.Frame) {
		t.Helper()
		select {
		case engines[i].in <- f:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	columns := func(i int) []string {
		t.Helper()
		for {
			select {
			case f := <-engines[i].out:
				if f.Kind != runner.FrameBatch {
					continue
				}
				var names []string
				for _, fd := range f.Batch.Schema().Fields() {
					names = append(names, fd.Name)
				}
				f.Batch.Release()
				return names
			case <-ctx.Done():
				t.Fatal(ctx.Err())
				return nil
			}
		}
	}
	send(0, runner.Frame{Kind: runner.FrameRows, Stream: "s", Rows: rows(`{"id": 1, "a": "x"}`)})
	send(1, runner.Frame{Kind: runner.FrameRows, Stream: "s", Rows: rows(`{"id": 2, "b": true}`)})
	columns(0)
	columns(1)
	for owner.newest() < 2 {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("the owner has version %d", owner.newest())
		}
	}
	var got [][]string
	for i := range engines {
		send(i, runner.Frame{Kind: runner.FrameRows, Stream: "s", Rows: rows(fmt.Sprintf(`{"id": %d}`, 3+i))})
		got = append(got, columns(i))
		send(i, runner.Frame{Kind: runner.FrameBarrier, Stream: "s", Epoch: 5})
		close(engines[i].in)
	}
	for i, e := range engines {
		for f := range e.out {
			if f.Batch != nil {
				f.Batch.Release()
			}
		}
		if err := <-e.err; err != nil {
			t.Fatalf("engine %d: %v", i, err)
		}
	}
	if !slices.Equal(got[0], got[1]) || !slices.Contains(got[0], "a") || !slices.Contains(got[0], "b") || owner.newest() != 2 {
		t.Fatalf("the engines wrote %v and %v, the owner at version %d", got[0], got[1], owner.newest())
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
