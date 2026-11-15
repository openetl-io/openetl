package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// seen is a frame into the dataflow, as markFlow saw it.
type seen struct {
	kind      runner.FrameKind
	lane      int
	split     string
	watermark *int64
	epoch     uint64
	at        time.Time
}

// markFlow is Passthrough, keeping what reaches it and reporting marks as
// every split's watermark in each epoch.
type markFlow struct {
	marks map[string]int64

	mu     sync.Mutex
	frames []seen
	plans  []runner.Plan
}

func (f *markFlow) Run(ctx context.Context, plan runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	f.mu.Lock()
	f.plans, f.frames = append(f.plans, plan), nil
	f.mu.Unlock()
	mid, passed := make(chan runner.Frame), make(chan runner.Frame)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(mid)
		for fr := range in {
			f.mu.Lock()
			f.frames = append(f.frames, seen{kind: fr.Kind, lane: fr.Lane, split: fr.Split, watermark: fr.Watermark, epoch: fr.Epoch, at: time.Now()})
			f.mu.Unlock()
			select {
			case mid <- fr:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})
	g.Go(func() error { return runner.Passthrough{Convert: idRows}.Run(gctx, plan, mid, passed) })
	g.Go(func() error {
		defer close(out)
		for fr := range passed {
			if fr.Kind == runner.FrameReady && f.marks != nil {
				fr.Ready.Watermarks = map[string]map[string]int64{"s": maps.Clone(f.marks)}
			}
			select {
			case out <- fr:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})
	return g.Wait()
}

func (f *markFlow) seen() []seen {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.frames)
}

// threeSplits reads splits 0 to 2 of s, each a row, a resume point and its
// completion.
type threeSplits struct{}

func (threeSplits) Splits(context.Context, source.ConfiguredStream, []source.Split) ([]source.Split, error) {
	return []source.Split{{ID: "0"}, {ID: "1"}, {ID: "2"}}, nil
}

func (threeSplits) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	return source.ReadSplits(ctx, req, out, func(_ context.Context, _ source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		if err := e.Rows([]json.RawMessage{json.RawMessage(`{"id": ` + sp.ID + `}`)}); err != nil {
			return err
		}
		if err := e.ResumePoint([]byte("1")); err != nil {
			return err
		}
		return e.Complete([]byte("2"))
	})
}

func timedJob(log *checkpoint.Log, owner, sync string, flow runner.Dataflow, held bool) runner.Job {
	return runner.Job{
		Owner: owner, Pipeline: "p", Sync: sync, Log: log, Source: threeSplits{}, Lanes: 2,
		Streams:     []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append, Schema: idSchema, EventTime: &runner.EventTime{Column: "at", Delay: 5 * time.Second, Held: held}}},
		Destination: &countingDest{},
		Dataflow:    flow,
		Settings:    delivery.Settings{Require: delivery.ExactlyOnce, CommitMaxBytes: 1, Scope: destination.ScopePipeline},
	}
}

func marked(ss []checkpoint.SplitState) map[string]int64 {
	out := map[string]int64{}
	for _, sp := range ss {
		if sp.Watermark != nil {
			out[sp.ID] = *sp.Watermark
		}
	}
	return out
}

// Each split of a stream with event time joins the lane it's read on
// before anything else of the stream reaches the dataflow, with the
// watermark it starts at; its rows are named by it; it moves on as an idle
// split does as it completes. Records keep what the dataflow reports, and
// the next read starts each split from it.
func TestSplitsJoinTheirLanesAndRecordsKeepTheirWatermarks(t *testing.T) {
	ctx := t.Context()
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
	flow := &markFlow{marks: map[string]int64{"0": 100, "1": 200, "2": 300}}
	before := time.Now()
	if _, err := runner.RunAttempt(ctx, timedJob(log, "a1", "s1", flow, false)); err != nil {
		t.Fatal(err)
	}
	after := time.Now()
	frames := flow.seen()
	joined := map[string]int{}
	for _, f := range frames {
		switch {
		case f.kind == runner.FrameSplit && f.watermark == nil:
			joined[f.split] = f.lane
		case f.kind == runner.FrameSplit:
			if _, ok := joined[f.split]; !ok {
				t.Fatalf("split %s moved before it joined", f.split)
			}
			lo, hi := before.Add(-5*time.Second).UnixMicro(), after.Add(-5*time.Second).UnixMicro()
			if *f.watermark < lo || *f.watermark > hi {
				t.Errorf("split %s completed at %d, not the clock less the delay", f.split, *f.watermark)
			}
		case f.kind == runner.FrameRows:
			if l, ok := joined[f.split]; !ok || l != f.lane {
				t.Fatalf("split %q's rows on lane %d, which it didn't join (%v)", f.split, f.lane, joined)
			}
		}
	}
	if !maps.Equal(joined, map[string]int{"0": 0, "1": 1, "2": 0}) {
		t.Fatalf("splits joined lanes %v", joined)
	}
	if _, ok := flow.plans[0].Floors["s"]; ok {
		t.Errorf("a read without watermarks has a floor: %v", flow.plans[0].Floors)
	}
	head, err := log.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := marked(head.Splits("s")); !maps.Equal(got, flow.marks) {
		t.Fatalf("the record's watermarks %v, want %v", got, flow.marks)
	}

	// The next sync's read starts each split at its own.
	if _, err := runner.RunAttempt(ctx, timedJob(log, "a2", "s2", flow, false)); err != nil {
		t.Fatal(err)
	}
	starts := map[string]int64{}
	for _, f := range flow.seen() {
		if f.kind == runner.FrameSplit && f.watermark != nil {
			if _, ok := starts[f.split]; !ok {
				starts[f.split] = *f.watermark
			}
		}
	}
	if !maps.Equal(starts, flow.marks) {
		t.Errorf("the next read's splits start at %v, want %v", starts, flow.marks)
	}
	if f, ok := flow.plans[1].Floors["s"]; !ok || f != 100 {
		t.Errorf("floors %v, want s at 100", flow.plans[1].Floors)
	}
}

// A held stream's split ends its input as it completes, and its next read
// starts afresh.
func TestAHeldStreamsSplitsEndAsTheyComplete(t *testing.T) {
	ctx := t.Context()
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
	flow := &markFlow{marks: map[string]int64{"0": runner.EndOfInput, "1": runner.EndOfInput, "2": runner.EndOfInput}}
	for i, sync := range []string{"s1", "s2"} {
		if _, err := runner.RunAttempt(ctx, timedJob(log, "a"+strconv.Itoa(i), sync, flow, true)); err != nil {
			t.Fatal(err)
		}
		ends := 0
		for _, f := range flow.seen() {
			if f.kind != runner.FrameSplit || f.watermark == nil {
				continue
			}
			if *f.watermark != runner.EndOfInput {
				t.Fatalf("sync %s: split %s at %d", sync, f.split, *f.watermark)
			}
			ends++
		}
		if ends != 3 {
			t.Errorf("sync %s: %d splits ended", sync, ends)
		}
		if len(flow.plans[i].Floors) != 0 {
			t.Errorf("sync %s: floors %v", sync, flow.plans[i].Floors)
		}
	}
}

// quietSource reads a and b of s on one lane, in a continuous read: a a
// row and a resume point, then nothing; b a row, then, once gate closes, a
// resume point, then nothing.
type quietSource struct{ gate chan struct{} }

func (quietSource) Splits(context.Context, source.ConfiguredStream, []source.Split) ([]source.Split, error) {
	return []source.Split{{ID: "a"}, {ID: "b"}}, nil
}

func (q quietSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	for _, sp := range req.Streams[0].Splits {
		e := out.Split("s", sp.ID)
		if err := e.Rows([]json.RawMessage{json.RawMessage(`{"id": 1}`)}); err != nil {
			return err
		}
		if sp.ID == "a" {
			if err := e.ResumePoint([]byte("a1")); err != nil {
				return err
			}
		}
	}
	select {
	case <-q.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := out.Split("s", "b").ResumePoint([]byte("b1")); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

// A split of a continuous read with nothing to read for its idle time moves
// on with the clock, as an event of the split: while it waits at a barrier
// for another split of its lane, its moves wait behind the lane's barrier.
func TestAnIdleSplitsMovesWaitBehindItsBarrier(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		gate, drain := make(chan struct{}), make(chan struct{})
		flow := &markFlow{}
		j := runner.Job{
			Owner: "a1", Pipeline: "p", Continuous: true, Log: checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"),
			Source:      quietSource{gate: gate},
			Streams:     []runner.Stream{{Name: "s", Replay: source.Exact, Read: source.Stream, Mode: destination.Append, Schema: idSchema, EventTime: &runner.EventTime{Column: "at", Idle: time.Second}}},
			Destination: &countingDest{},
			Dataflow:    flow,
			Drain:       drain,
			Settings:    delivery.Settings{Require: delivery.AtLeastOnce, CommitInterval: 2 * time.Second, Scope: destination.ScopePipeline, ResumePointTimeout: time.Hour},
		}
		done := make(chan error, 1)
		go func() {
			_, err := runner.RunAttempt(ctx, j)
			done <- err
		}()
		// The first barrier goes out at 2s: a places it, and b, without a
		// resume point, holds the lane.
		time.Sleep(4 * time.Second)
		close(gate)
		time.Sleep(time.Second)
		close(drain)
		if err := <-done; err != nil && !errors.Is(err, runner.ErrDrained) {
			t.Fatal(err)
		}

		frames := flow.seen()
		barrier := slices.IndexFunc(frames, func(f seen) bool { return f.kind == runner.FrameBarrier })
		if barrier < 0 {
			t.Fatal("no barrier")
		}
		var before, held int
		for i, f := range frames {
			if f.kind != runner.FrameSplit || f.split != "a" || f.watermark == nil {
				continue
			}
			switch {
			case i < barrier:
				before++
			case *f.watermark < frames[barrier].at.UnixMicro():
				held++
			}
		}
		if before == 0 {
			t.Error("a never moved before the barrier")
		}
		if held == 0 {
			t.Errorf("none of a's moves waited behind the barrier at %v: %+v", frames[barrier].at, frames)
		}
	})
}

// handOver reads split a of s, which completes adding b, which reads a row
// and completes.
type handOver struct{}

func (handOver) Splits(context.Context, source.ConfiguredStream, []source.Split) ([]source.Split, error) {
	return []source.Split{{ID: "a"}}, nil
}

func (handOver) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	return source.ReadSplits(ctx, req, out, func(_ context.Context, _ source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		if err := e.Rows([]json.RawMessage{json.RawMessage(`{"id": 1}`)}); err != nil {
			return err
		}
		if sp.ID == "a" {
			return e.Complete([]byte("a1"), source.Split{ID: "b"})
		}
		return e.Complete([]byte("b1"))
	})
}

// A split that completes adding others has them join its lane before it
// moves on, so the lane waits for them.
func TestASplitsSuccessorsJoinBeforeItMovesOn(t *testing.T) {
	flow := &markFlow{}
	j := timedJob(checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"), "a1", "s1", flow, false)
	j.Source = handOver{}
	if _, err := runner.RunAttempt(t.Context(), j); err != nil {
		t.Fatal(err)
	}
	frames := flow.seen()
	joined := slices.IndexFunc(frames, func(f seen) bool { return f.kind == runner.FrameSplit && f.split == "b" })
	moved := slices.IndexFunc(frames, func(f seen) bool {
		return f.kind == runner.FrameSplit && f.split == "a" && f.watermark != nil
	})
	if joined < 0 || moved < 0 || joined > moved || frames[joined].lane != frames[moved].lane {
		t.Fatalf("b joined at %d, a moved on at %d: %+v", joined, moved, frames)
	}
}

// lateRowSource is quietSource whose a reads another row at 2.9s, while it
// waits at the first barrier for b.
type lateRowSource struct{ gate chan struct{} }

func (lateRowSource) Splits(context.Context, source.ConfiguredStream, []source.Split) ([]source.Split, error) {
	return []source.Split{{ID: "a"}, {ID: "b"}}, nil
}

func (q lateRowSource) Read(ctx context.Context, _ source.ReadRequest, out source.Emitter) error {
	a, b := out.Split("s", "a"), out.Split("s", "b")
	row := []json.RawMessage{json.RawMessage(`{"id": 1}`)}
	if err := a.Rows(row); err != nil {
		return err
	}
	if err := a.ResumePoint([]byte("a1")); err != nil {
		return err
	}
	if err := b.Rows(row); err != nil {
		return err
	}
	time.Sleep(2900 * time.Millisecond)
	if err := a.Rows(row); err != nil {
		return err
	}
	if err := a.ResumePoint([]byte("a2")); err != nil {
		return err
	}
	select {
	case <-q.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := b.ResumePoint([]byte("b1")); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

// A row a split reads while it waits at a barrier keeps it from being idle,
// though the row waits behind the barrier too: no idle move of the split
// comes after it before the idle time has passed since it was read.
func TestARowHeldBehindABarrierKeepsItsSplitBusy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		start := time.Now()
		gate, drain := make(chan struct{}), make(chan struct{})
		flow := &markFlow{}
		j := runner.Job{
			Owner: "a1", Pipeline: "p", Continuous: true, Log: checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"),
			Source:      lateRowSource{gate: gate},
			Streams:     []runner.Stream{{Name: "s", Replay: source.Exact, Read: source.Stream, Mode: destination.Append, Schema: idSchema, EventTime: &runner.EventTime{Column: "at", Idle: time.Second}}},
			Destination: &countingDest{},
			Dataflow:    flow,
			Drain:       drain,
			Settings:    delivery.Settings{Require: delivery.AtLeastOnce, CommitInterval: 2 * time.Second, Scope: destination.ScopePipeline, ResumePointTimeout: time.Hour},
		}
		done := make(chan error, 1)
		go func() {
			_, err := runner.RunAttempt(ctx, j)
			done <- err
		}()
		time.Sleep(3500 * time.Millisecond)
		close(gate)
		time.Sleep(time.Second)
		close(drain)
		if err := <-done; err != nil && !errors.Is(err, runner.ErrDrained) {
			t.Fatal(err)
		}

		frames := flow.seen()
		barrier := slices.IndexFunc(frames, func(f seen) bool { return f.kind == runner.FrameBarrier })
		if barrier < 0 {
			t.Fatal("no barrier")
		}
		row := slices.IndexFunc(frames[barrier:], func(f seen) bool { return f.kind == runner.FrameRows && f.split == "a" })
		if row < 0 {
			t.Fatal("a's row didn't follow the barrier")
		}
		read := start.Add(2900 * time.Millisecond)
		for _, f := range frames[barrier+row:] {
			if f.kind == runner.FrameSplit && f.split == "a" && f.watermark != nil && *f.watermark < read.Add(time.Second).UnixMicro() {
				t.Fatalf("a moved to %v after its row read at %v: %+v", time.UnixMicro(*f.watermark), read, frames)
			}
		}
	})
}
