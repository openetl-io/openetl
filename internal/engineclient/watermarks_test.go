package engineclient

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// Rows' times in these tests: microseconds since the Unix epoch, from 2023.
const base int64 = 1_700_000_000_000_000

func at(us int64) string { return time.UnixMicro(us).UTC().Format(time.RFC3339Nano) }

// timedStream is s, of ids and times at, whose watermark trails at by 50µs.
func timedStream() runner.Stream {
	return runner.Stream{
		Name: "s", Replay: source.Exact, Mode: destination.Append,
		Schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		}, nil),
		EventTime: &runner.EventTime{Column: "at", Delay: 50 * time.Microsecond},
	}
}

const timedSQL = "INSERT INTO kept SELECT id, at FROM s WHERE id % 2 = 0"

func timedExpected(ctx context.Context, t *testing.T) []runner.ExpectedOutput {
	t.Helper()
	declared, err := arrowipc.EncodeSchema(timedStream().Schema)
	if err != nil {
		t.Fatal(err)
	}
	a, err := Plan(ctx, planEngine(t), sqlplan.Request{SQL: timedSQL, Streams: []sqlplan.Stream{{Name: "s", DeclaredSchema: declared}}})
	if err != nil || len(a.Errors) != 0 {
		t.Fatal(a.Errors, err)
	}
	return expected(t, a)
}

func mark(v int64) *int64 { return &v }

// The engine takes each batch to be of the split the runner says, and
// reports each split's watermark at its lane's barrier: the latest time it
// read less the delay, or what the runner raised it to.
func TestEngineReportsEachSplitsWatermark(t *testing.T) {
	ctx := testContext(t)
	df := &Dataflow{Options: options(t, nil)}
	p := plan()
	p.Streams = []runner.Stream{timedStream()}
	p.Outputs = []runner.Stream{{Name: "kept", Transformed: true, From: "s"}, {Name: "s", Transformed: true, From: "s"}}
	p.SQL = &runner.SQLTransform{SQL: timedSQL, Expected: timedExpected(ctx, t)}
	p.Floors = map[string]int64{"s": base}
	row := func(id, us int64) string { return fmt.Sprintf(`{"id": %d, "at": %q}`, id, at(us)) }
	got, err := run(ctx, df, p,
		runner.Frame{Kind: runner.FrameSplit, Stream: "s", Split: "a"},
		runner.Frame{Kind: runner.FrameSplit, Stream: "s", Split: "b", Watermark: mark(base + 10)},
		runner.Frame{Kind: runner.FrameRows, Stream: "s", Split: "a", Rows: rows(row(1, base+100), row(2, base+300))},
		runner.Frame{Kind: runner.FrameRows, Stream: "s", Split: "b", Rows: rows(row(3, base+120))},
		runner.Frame{Kind: runner.FrameRows, Stream: "s", Split: "a", Rows: rows(row(4, base+200))},
		runner.Frame{Kind: runner.FrameBarrier, Stream: "s", Epoch: 5},
		runner.Frame{Kind: runner.FrameSplit, Stream: "s", Split: "b", Watermark: mark(runner.EndOfInput)},
		runner.Frame{Kind: runner.FrameBarrier, Stream: "s", Epoch: 6},
	)
	defer release(got)
	if err != nil {
		t.Fatal(err)
	}
	ready := got[""]
	if len(ready) != 2 {
		t.Fatalf("ready: %+v", ready)
	}
	want := []map[string]int64{{"a": base + 250, "b": base + 70}, {"a": base + 250, "b": runner.EndOfInput}}
	for i, r := range ready {
		if !maps.Equal(r.Ready.Watermarks["s"], want[i]) {
			t.Errorf("epoch %d's watermarks: %v, want %v", r.Epoch, r.Ready.Watermarks, want[i])
		}
	}
}

// A SQL transform with event time runs on workers, each with an engine of
// its own. Every record holds each split's watermark, which never goes back
// along the records, through a lost worker, a restore on more workers and
// one on a single runner. A split that completes moves on as an idle one
// does, so the last record has every split past the time the run started,
// less the delay.
func TestWatermarksOnWorkersThroughTheEngine(t *testing.T) {
	opts := options(t, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	exp := timedExpected(ctx, t)
	started := time.Now()
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
	dest := newKeyDest(16)
	const splits, chunks, perChunk = 4, 8, 10
	src := func(upTo int) *splitSource {
		return &splitSource{splits: splits, chunks: chunks, rows: perChunk, upTo: upTo, row: func(split, chunk int, id int64) string {
			// Split 2 starts late, so early records have it without a
			// watermark.
			if split == 2 && chunk == 0 && id%100 == 0 {
				time.Sleep(200 * time.Millisecond)
			}
			return fmt.Sprintf(`{"id": %d, "at": %q}`, id, at(base+int64(split*7+chunk*1000)+id%100))
		}}
	}
	var mu sync.Mutex
	var records []*checkpoint.Record
	job := func(owner string, upTo int) func() runner.Job {
		return func() runner.Job {
			return runner.Job{
				Pipeline: "p", Owner: owner, Sync: "s1", Log: log, Source: src(upTo),
				Streams:     []runner.Stream{timedStream()},
				Outputs:     []runner.Stream{{Name: "kept", Replay: source.Exact, Mode: destination.Append, Transformed: true, From: "s"}},
				SQL:         &runner.SQLTransform{SQL: timedSQL, Expected: exp},
				Destination: dest, Dataflow: &Dataflow{Options: opts}, Lanes: 2, Logger: opts.Logger.With("job", owner),
				Settings: delivery.Settings{Require: delivery.AtLeastOnce, CommitMaxBytes: 4 << 10, CommitInterval: 20 * time.Millisecond, Scope: destination.ScopePipeline},
				OnCheckpoint: func(rec *checkpoint.Record, _ map[string]uint64) {
					mu.Lock()
					records = append(records, rec)
					mu.Unlock()
				},
			}
		}
	}
	srv := listen(t)

	p := &inProcess{ctx: ctx, mk: job("a1", 3)}
	j := p.mk()
	p.on(&j, srv, 2)
	var once sync.Once
	onCheckpoint := j.OnCheckpoint
	j.OnCheckpoint = func(rec *checkpoint.Record, c map[string]uint64) {
		onCheckpoint(rec, c)
		once.Do(func() { p.kill(1) })
	}
	if _, err := runner.RunAttempt(ctx, j); err == nil {
		t.Fatal("the attempt went on without its worker")
	}
	p.wait()

	p.mk = job("a2", 6)
	j = p.mk()
	p.on(&j, srv, 3)
	drainOnceRecorded(&j, func(rec *checkpoint.Record) bool {
		onCheckpoint(rec, nil)
		for _, sp := range rec.Splits("s") {
			if sp.State == nil || string(sp.State) < "5" {
				return false
			}
		}
		return true
	})
	if _, err := runner.RunAttempt(ctx, j); !errors.Is(err, runner.ErrDrained) {
		t.Fatalf("three workers: %v", err)
	}
	for i, err := range p.wait() {
		if err != nil {
			t.Errorf("worker %d of three: %v", i, err)
		}
	}
	if _, err := runner.RunAttempt(ctx, job("a3", 0)()); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(records) < 3 {
		t.Fatalf("%d records", len(records))
	}
	// A split without a watermark is one that hasn't read a time yet; once
	// it has one, every later record keeps it.
	last := map[string]int64{}
	for _, rec := range records {
		for _, sp := range rec.Splits("s") {
			prev, had := last[sp.ID]
			if sp.Watermark == nil {
				if had {
					t.Fatalf("record %d lost split %s's watermark %d: %+v", rec.Epoch, sp.ID, prev, sp)
				}
				continue
			}
			if had && *sp.Watermark < prev {
				t.Fatalf("split %s's watermark went back from %d to %d in record %d", sp.ID, prev, *sp.Watermark, rec.Epoch)
			}
			last[sp.ID] = *sp.Watermark
		}
	}
	final := records[len(records)-1]
	floor := started.Add(-50 * time.Microsecond).UnixMicro()
	for _, sp := range final.Splits("s") {
		n, _ := strconv.Atoi(sp.ID)
		if !sp.Complete || sp.Watermark == nil || *sp.Watermark < floor {
			t.Errorf("split %d at the end: complete %v, watermark %v, below the run's start %d", n, sp.Complete, sp.Watermark, floor)
		}
	}
}
