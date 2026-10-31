package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// The source emits its second page only once Commit(1) has started, and
// Commit(1) returns only once PreCommit(2) has run, so barrier 2 must be
// requested and placed while epoch 1 is committing.
func TestBarrierRequestedWhileCommitting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		dest := &blockingDest{committing1: make(chan struct{}), preCommitted2: make(chan struct{})}
		res, err := runner.RunAttempt(ctx, runner.Job{
			Owner:       "a1",
			Pipeline:    "p",
			Log:         checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"),
			Source:      pagedSource{pages: 2, after1: dest.committing1},
			Streams:     []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append}},
			Destination: dest,
			Dataflow:    runner.Passthrough{Convert: columnless},
			Settings: delivery.Settings{
				Require:        delivery.ExactlyOnce,
				CommitMaxBytes: 1,
				Scope:          destination.ScopePipeline,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if res.Committed < 2 {
			t.Fatalf("committed %d epochs, want at least 2", res.Committed)
		}
		if dest.rows != 2 {
			t.Fatalf("wrote %d rows, want 2", dest.rows)
		}
	})
}

// pagedSource emits one row per page, each followed by a resume point, and
// waits for after1 after the first.
type pagedSource struct {
	pages  int
	after1 <-chan struct{}
}

func (s pagedSource) Read(ctx context.Context, _ source.ReadRequest, out source.Emitter) error {
	for i := range s.pages {
		if i == 1 {
			select {
			case <-s.after1:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := out.Rows("s", []json.RawMessage{json.RawMessage(`{}`)}); err != nil {
			return err
		}
		if err := out.ResumePoint("s", fmt.Appendf(nil, "%d", i+1)); err != nil {
			return err
		}
	}
	return out.Complete("s")
}

var empty = arrow.NewSchema(nil, nil)

func columnless(_ string, rows []json.RawMessage) (arrow.RecordBatch, error) {
	return array.NewRecordBatch(empty, nil, int64(len(rows))), nil
}

type blockingDest struct {
	committing1, preCommitted2 chan struct{}
	rows                       int64
}

func (d *blockingDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopePipeline}}
}

func (d *blockingDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *blockingDest) Write(_ context.Context, _ destination.Epoch, _ string, b arrow.RecordBatch) error {
	d.rows += b.NumRows()
	return nil
}

func (d *blockingDest) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	if e.N == 2 {
		close(d.preCommitted2)
	}
	return nil, nil
}

func (d *blockingDest) Commit(ctx context.Context, e destination.Epoch, _ destination.Staged) error {
	if e.N != 1 {
		return nil
	}
	close(d.committing1)
	select {
	case <-d.preCommitted2:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *blockingDest) Recover(context.Context, uint64, destination.Epoch, destination.Staged) (destination.Recovery, error) {
	return destination.Applied, nil
}

func (d *blockingDest) Close(context.Context) error { return nil }

// Each record keeps what the dataflow reported for its epoch, and the next
// attempt's dataflow gets it back.
func TestRecordsCarryTheDataflowsReport(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		var plans []runner.Plan
		var last uint64
		var gen uint64
		for _, pages := range []int{2, 3} {
			committed := make(chan uint64, 16)
			res, err := runner.RunAttempt(ctx, job(log, fmt.Sprintf("a%d", pages), ackSource{pages: pages, committed: committed, acks: new([]string)}, reportingFlow{plans: &plans}, committed))
			if err != nil {
				t.Fatal(err)
			}
			last, gen = res.Last, res.Generation
		}
		rec, err := log.Record(ctx, checkpoint.RecordRef{Epoch: last, Generation: gen})
		if err != nil {
			t.Fatal(err)
		}
		if got, want := string(rec.Streams["s"].Schema), fmt.Sprintf("schema-%d", last); got != want {
			t.Errorf("record %d has schema %q, want %q", last, got, want)
		}
		if len(rec.Snapshots) != 1 || rec.Snapshots[0].URL != fmt.Sprintf("mem://chk-%d", last) {
			t.Errorf("record %d has snapshots %+v", last, rec.Snapshots)
		}

		first := plans[1]
		restored := first.FirstEpoch - 1
		if got, want := string(first.Schemas["s"]), fmt.Sprintf("schema-%d", restored); got != want {
			t.Errorf("the second attempt restored schema %q, want %q", got, want)
		}
		if len(first.Snapshots) != 1 || first.Snapshots[0].URL != fmt.Sprintf("mem://chk-%d", restored) {
			t.Errorf("the second attempt restored snapshots %+v", first.Snapshots)
		}
	})
}

// Each record counts the rows its epoch read, and OnCheckpoint gets every
// record in order, across attempts. A page's row is in its own epoch, and
// an epoch that only completes the stream has none.
func TestRecordsCountRows(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		var epochs []uint64
		var rows uint64
		for _, pages := range []int{2, 3} {
			committed := make(chan uint64, 16)
			j := job(log, fmt.Sprintf("a%d", pages), ackSource{pages: pages, committed: committed, acks: new([]string)}, runner.Passthrough{Convert: columnless}, committed)
			j.OnCheckpoint = func(rec *checkpoint.Record) {
				epochs = append(epochs, rec.Epoch)
				if n := rec.Streams["s"].Rows; n > 1 {
					t.Errorf("epoch %d read %d rows, want at most 1", rec.Epoch, n)
				}
				rows += rec.Streams["s"].Rows
			}
			if _, err := runner.RunAttempt(ctx, j); err != nil {
				t.Fatal(err)
			}
		}
		for i, n := range epochs {
			if n != uint64(i)+1 {
				t.Fatalf("OnCheckpoint got epochs %v, want 1, 2, ... in order", epochs)
			}
		}
		if rows != 3 {
			t.Fatalf("records count %d rows, want 3", rows)
		}
	})
}

// After Commit(N), the source acknowledges record N-1's states, including the
// restored record after a new attempt's first commit.
func TestAcknowledgementsTrailByOneRecord(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		var acks []string
		var records []string
		for _, pages := range []int{3, 5} {
			committed := make(chan uint64, 16)
			var epochs []uint64
			j := job(log, fmt.Sprintf("a%d", pages), ackSource{pages: pages, committed: committed, acks: &acks}, runner.Passthrough{Convert: columnless}, committed)
			j.OnCommit = func(n uint64) {
				epochs = append(epochs, n)
				committed <- n
			}
			res, err := runner.RunAttempt(ctx, j)
			if err != nil {
				t.Fatal(err)
			}
			for _, n := range epochs {
				rec, err := log.Record(ctx, checkpoint.RecordRef{Epoch: n, Generation: res.Generation})
				if err != nil {
					t.Fatal(err)
				}
				records = append(records, string(rec.Streams["s"].State))
			}
		}
		if len(records) < 4 {
			t.Fatalf("only %d records", len(records))
		}
		if want := records[:len(records)-1]; !slices.Equal(acks, want) {
			t.Fatalf("acknowledged %q, want %q", acks, want)
		}
	})
}

// A stream's read start survives a failed attempt, so rows re-read after it
// keep their IDs, and starts again once the stream has completed.
func TestReadStartLastsUntilTheStreamCompletes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		var plans []runner.Plan
		run := func(src ackSource) error {
			committed := make(chan uint64, 16)
			src.committed, src.acks = committed, new([]string)
			_, err := runner.RunAttempt(ctx, job(log, fmt.Sprintf("a%d", len(plans)), src, reportingFlow{plans: &plans}, committed))
			time.Sleep(time.Second)
			return err
		}
		if err := run(ackSource{pages: 4, failAfter: 2}); err == nil {
			t.Fatal("the first attempt succeeded")
		}
		if err := run(ackSource{pages: 4}); err != nil {
			t.Fatal(err)
		}
		if err := run(ackSource{pages: 4}); err != nil {
			t.Fatal(err)
		}
		start := func(i int) int64 { return plans[i].ReadStarts["s"] }
		if start(0) == 0 || start(1) != start(0) {
			t.Errorf("the second attempt read from %d, want the first's %d", start(1), start(0))
		}
		if start(2) <= start(1) {
			t.Errorf("after the stream completed, the next read started at %d, not after %d", start(2), start(1))
		}
	})
}

func job(log *checkpoint.Log, owner string, src source.Reader, flow runner.Dataflow, committed chan<- uint64) runner.Job {
	return runner.Job{
		Owner:       owner,
		Pipeline:    "p",
		Log:         log,
		Source:      src,
		Streams:     []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append}},
		Destination: &countingDest{},
		Dataflow:    flow,
		Settings: delivery.Settings{
			Require:        delivery.ExactlyOnce,
			CommitMaxBytes: 1,
			Scope:          destination.ScopePipeline,
		},
		OnCommit: func(n uint64) { committed <- n },
	}
}

// ackSource emits pages from its state up to pages, one row and a resume
// point each. It waits for a commit after each page but the last, and keeps
// what it's asked to acknowledge.
type ackSource struct {
	pages     int
	committed <-chan uint64
	acks      *[]string
	// Fail once page failAfter has committed; 0 never fails.
	failAfter int
}

func (s ackSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	from := 0
	if st := req.Streams[0].State; st != nil {
		from, _ = strconv.Atoi(string(st))
	}
	for i := from; i < s.pages; i++ {
		if err := out.Rows("s", []json.RawMessage{json.RawMessage(`{}`)}); err != nil {
			return err
		}
		if err := out.ResumePoint("s", strconv.AppendInt(nil, int64(i+1), 10)); err != nil {
			return err
		}
		if i+1 == s.pages {
			break
		}
		select {
		case <-s.committed:
		case <-ctx.Done():
			return ctx.Err()
		}
		if i+1 == s.failAfter {
			return errors.New("the source broke")
		}
	}
	return out.Complete("s")
}

func (s ackSource) Committed(_ context.Context, states map[string][]byte) error {
	*s.acks = append(*s.acks, string(states["s"]))
	return nil
}

// reportingFlow reports a schema for s and one snapshot in each epoch, and
// keeps the plans it ran.
type reportingFlow struct{ plans *[]runner.Plan }

func (f reportingFlow) Run(ctx context.Context, plan runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	defer close(out)
	*f.plans = append(*f.plans, plan)
	mid := make(chan runner.Frame)
	errc := make(chan error, 1)
	go func() { errc <- runner.Passthrough{Convert: columnless}.Run(ctx, plan, in, mid) }()
	for fr := range mid {
		if fr.Kind == runner.FrameReady {
			fr.Ready.Schemas = map[string][]byte{"s": fmt.Appendf(nil, "schema-%d", fr.Epoch)}
			fr.Ready.Snapshots = []checkpoint.Snapshot{{Operator: "op", URL: fmt.Sprintf("mem://chk-%d", fr.Epoch)}}
		}
		select {
		case out <- fr:
		case <-ctx.Done():
		}
	}
	return <-errc
}

type countingDest struct{ rows int64 }

func (d *countingDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopePipeline}}
}

func (d *countingDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *countingDest) Write(_ context.Context, _ destination.Epoch, _ string, b arrow.RecordBatch) error {
	d.rows += b.NumRows()
	return nil
}

func (d *countingDest) PreCommit(context.Context, destination.Epoch) (destination.Staged, error) {
	return nil, nil
}

func (d *countingDest) Commit(context.Context, destination.Epoch, destination.Staged) error {
	return nil
}

func (d *countingDest) Recover(context.Context, uint64, destination.Epoch, destination.Staged) (destination.Recovery, error) {
	return destination.Applied, nil
}

func (d *countingDest) Close(context.Context) error { return nil }

// A barrier requested just as Read returns covers everything, so no empty
// epoch follows it. Read's end and the commit tick coincide; which the pump
// sees first varies, so this runs many times.
func TestNoEmptyFinalEpoch(t *testing.T) {
	for i := range 200 {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			j := job(checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"), "a1", lateSource{after: time.Second}, runner.Passthrough{Convert: columnless}, make(chan uint64, 16))
			j.Settings.CommitMaxBytes = 0
			j.Settings.CommitInterval = time.Second
			res, err := runner.RunAttempt(ctx, j)
			if err != nil {
				t.Fatal(err)
			}
			if res.Committed != 1 {
				t.Fatalf("run %d committed %d epochs, want 1", i, res.Committed)
			}
		})
	}
}

// lateSource completes its stream, then returns after a while.
type lateSource struct{ after time.Duration }

func (s lateSource) Read(_ context.Context, _ source.ReadRequest, out source.Emitter) error {
	if err := out.Rows("s", []json.RawMessage{json.RawMessage(`{}`)}); err != nil {
		return err
	}
	if err := out.ResumePoint("s", []byte("1")); err != nil {
		return err
	}
	if err := out.Complete("s"); err != nil {
		return err
	}
	time.Sleep(s.after)
	return nil
}
