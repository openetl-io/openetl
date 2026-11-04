package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/arrowipc"
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
	return out.Complete("s", nil)
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

func (d *blockingDest) Commit(ctx context.Context, e destination.Epoch, _ destination.Staged) (destination.Committed, error) {
	if e.N != 1 {
		return destination.Committed{}, nil
	}
	close(d.committing1)
	select {
	case <-d.preCommitted2:
		return destination.Committed{}, nil
	case <-ctx.Done():
		return destination.Committed{}, ctx.Err()
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

// An attempt keeps the last records of the chain it restored, and the ones
// it commits.
func TestAttemptsPruneTheLog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		store := checkpoint.NewMemStore()
		log := checkpoint.NewLog(store, "org", "p")
		var res runner.Result
		for i, pages := range []int{3, 6, 9} {
			committed := make(chan uint64, 16)
			j := job(log, fmt.Sprintf("a%d", i), ackSource{pages: pages, committed: committed, acks: new([]string)}, runner.Passthrough{Convert: columnless}, committed)
			// Above each attempt's commits (3, or 4 if the completion has an
			// epoch of its own), so only attempts' starts prune.
			j.KeepRecords = 5
			var err error
			if res, err = runner.RunAttempt(ctx, j); err != nil {
				t.Fatal(err)
			}
		}
		keys, err := store.List(ctx, "org/p/_checkpoints/")
		if err != nil {
			t.Fatal(err)
		}
		var older, newer []uint64
		for _, k := range keys {
			var e, g uint64
			if _, err := fmt.Sscanf(strings.TrimPrefix(k, "org/p/_checkpoints/"), "%d-g%d.json", &e, &g); err != nil {
				continue
			}
			switch {
			case e > res.Restored && g == res.Generation:
				newer = append(newer, e)
			case e <= res.Restored:
				older = append(older, e)
			default:
				t.Errorf("record %d-g%d is left", e, g)
			}
		}
		if r := res.Restored; !slices.Equal(older, []uint64{r - 4, r - 3, r - 2, r - 1, r}) || len(newer) != res.Committed {
			t.Fatalf("records %v up to the restored %d and %v after, want the last 5 and the %d committed", older, r, newer, res.Committed)
		}
	})
}

// A job prunes every KeepRecords commits, so a long one keeps few records.
func TestJobsPruneAsTheyGo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		store := checkpoint.NewMemStore()
		committed := make(chan uint64, 64)
		j := job(checkpoint.NewLog(store, "org", "p"), "a1", ackSource{pages: 20, committed: committed, acks: new([]string)}, runner.Passthrough{Convert: columnless}, committed)
		j.KeepRecords = 3
		res, err := runner.RunAttempt(ctx, j)
		if err != nil {
			t.Fatal(err)
		}
		keys, err := store.List(ctx, "org/p/_checkpoints/")
		if err != nil {
			t.Fatal(err)
		}
		var epochs []uint64
		for _, k := range keys {
			var e, g uint64
			if _, err := fmt.Sscanf(strings.TrimPrefix(k, "org/p/_checkpoints/"), "%d-g%d.json", &e, &g); err == nil {
				epochs = append(epochs, e)
			}
		}
		// The last prune kept 3, and the commits since are left.
		var want []uint64
		for e := res.Last - uint64(3+res.Committed%3) + 1; e <= res.Last; e++ {
			want = append(want, e)
		}
		if res.Committed < 20 || !slices.Equal(epochs, want) {
			t.Fatalf("after %d commits, records %v, want %v", res.Committed, epochs, want)
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
			j.OnCheckpoint = func(rec *checkpoint.Record, _ map[string]uint64) {
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

// The pump counts a forwarded batch's rows without decoding it, and
// Passthrough decodes it.
func TestForwardedBatches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		j := job(checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"), "a1", encodedSource{pages: 3}, runner.Passthrough{}, make(chan uint64, 16))
		var rows uint64
		j.OnCheckpoint = func(rec *checkpoint.Record, _ map[string]uint64) { rows += rec.Streams["s"].Rows }
		if _, err := runner.RunAttempt(ctx, j); err != nil {
			t.Fatal(err)
		}
		if d := j.Destination.(*countingDest); d.rows != 6 || rows != 6 {
			t.Fatalf("wrote %d rows and recorded %d, want 6", d.rows, rows)
		}
	})
}

// encodedSource forwards pages of two rows, each followed by a resume point.
type encodedSource struct{ pages int }

func (s encodedSource) Read(_ context.Context, _ source.ReadRequest, out source.Emitter) error {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues([]int64{1, 2}, nil)
	col := b.NewArray()
	defer col.Release()
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{col}, 2)
	defer rec.Release()
	payload, err := arrowipc.EncodeBatch(rec)
	if err != nil {
		return err
	}
	enc, err := arrowipc.Inspect(payload)
	if err != nil {
		return err
	}
	for i := range s.pages {
		if err := out.(source.Forwarder).Forward("s", enc); err != nil {
			return err
		}
		if err := out.ResumePoint("s", fmt.Appendf(nil, "%d", i+1)); err != nil {
			return err
		}
	}
	return out.Complete("s", nil)
}

// ackSource emits pages from its state up to pages, one row and a resume
// point each, and its next sync carries on from there. It waits for a commit
// after each page but the last, and keeps what it's asked to acknowledge.
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
	return out.Complete("s", strconv.AppendInt(nil, int64(s.pages), 10))
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

func (d *countingDest) Commit(context.Context, destination.Epoch, destination.Staged) (destination.Committed, error) {
	return destination.Committed{}, nil
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
	if err := out.Complete("s", nil); err != nil {
		return err
	}
	time.Sleep(s.after)
	return nil
}

// Draining stops the attempt at a checkpoint covering what it had read, and
// the next attempt resumes from there.
func TestDrainStopsAtACheckpoint(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		dest := &countingDest{}
		drain := make(chan struct{})
		j := job(log, "a1", slowSource{pages: 10}, runner.Passthrough{Convert: columnless}, make(chan uint64, 16))
		j.Destination, j.Drain = dest, drain
		j.Settings.CommitMaxBytes = 1 << 40
		go func() {
			time.Sleep(3500 * time.Millisecond)
			close(drain)
		}()
		res, err := runner.RunAttempt(ctx, j)
		if !errors.Is(err, runner.ErrDrained) {
			t.Fatalf("got %v, want ErrDrained", err)
		}
		if res.Committed != 1 || dest.rows != 3 {
			t.Fatalf("committed %d epochs and %d rows, want 1 and 3", res.Committed, dest.rows)
		}

		j = job(log, "a2", slowSource{pages: 10}, runner.Passthrough{Convert: columnless}, make(chan uint64, 16))
		j.Destination = dest
		if _, err := runner.RunAttempt(ctx, j); err != nil {
			t.Fatal(err)
		}
		if dest.rows != 10 {
			t.Fatalf("wrote %d rows, want 10", dest.rows)
		}
	})
}

// slowSource emits pages a second apart from its state, one row and a resume
// point each.
type slowSource struct{ pages int }

func (s slowSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	from := 0
	if st := req.Streams[0].State; st != nil {
		from, _ = strconv.Atoi(string(st))
	}
	for i := from; i < s.pages; i++ {
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := out.Rows("s", []json.RawMessage{json.RawMessage(`{}`)}); err != nil {
			return err
		}
		if err := out.ResumePoint("s", strconv.AppendInt(nil, int64(i+1), 10)); err != nil {
			return err
		}
	}
	return out.Complete("s", nil)
}

// A stream completed earlier in the sync isn't read again, and the next sync
// reads it whole.
func TestCompletedStreamsWaitForTheNextSync(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		read := map[string]int{}
		attempt := func(sync string, fail bool) error {
			committed := make(chan uint64, 16)
			j := job(log, fmt.Sprintf("a%d", len(read)), fullSource{pages: 2, committed: committed, read: read, fail: fail}, runner.Passthrough{Convert: columnless}, committed)
			j.Sync = sync
			j.Streams = []runner.Stream{{Name: "a", Replay: source.Exact, Mode: destination.Append}, {Name: "b", Replay: source.Exact, Mode: destination.Append}}
			_, err := runner.RunAttempt(ctx, j)
			time.Sleep(time.Second)
			return err
		}
		if err := attempt("s1", true); err == nil {
			t.Fatal("the first attempt succeeded")
		}
		if err := attempt("s1", false); err != nil {
			t.Fatal(err)
		}
		if read["a"] != 2 || read["b"] != 2 {
			t.Fatalf("the first sync read %v, want 2 rows of each", read)
		}
		if err := attempt("s2", false); err != nil {
			t.Fatal(err)
		}
		if err := attempt("s2", false); err != nil {
			t.Fatal(err)
		}
		if read["a"] != 4 || read["b"] != 4 {
			t.Fatalf("after the second sync, read %v, want 4 rows of each", read)
		}
	})
}

// fullSource reads each stream's pages from its state, one row and a resume
// point each, waiting for a commit after each, and completes it as a full
// refresh. With fail, it fails once b's first page has committed.
type fullSource struct {
	pages     int
	committed <-chan uint64
	read      map[string]int
	fail      bool
}

func (s fullSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	for _, cs := range req.Streams {
		from := 0
		if cs.State != nil {
			from, _ = strconv.Atoi(string(cs.State))
		}
		for i := from; i < s.pages; i++ {
			if err := out.Rows(cs.Name, []json.RawMessage{json.RawMessage(`{}`)}); err != nil {
				return err
			}
			s.read[cs.Name]++
			if err := out.ResumePoint(cs.Name, strconv.AppendInt(nil, int64(i+1), 10)); err != nil {
				return err
			}
			select {
			case <-s.committed:
			case <-ctx.Done():
				return ctx.Err()
			}
			if s.fail && cs.Name == "b" {
				return errors.New("the source broke")
			}
		}
		if err := out.Complete(cs.Name, nil); err != nil {
			return err
		}
	}
	return nil
}

// Each attempt starts the reads it does, a retry continuing its sync's read
// with the same start, and ends each read once, after its last batch and
// before its epoch's PreCommit.
func TestReadsStartAndEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		dest := &readsDest{}
		attempts := 0
		attempt := func(sync string, fail bool) error {
			attempts++
			dest.log = append(dest.log, "attempt")
			committed := make(chan uint64, 16)
			j := job(log, fmt.Sprintf("a%d", attempts), fullSource{pages: 2, committed: committed, read: map[string]int{}, fail: fail}, runner.Passthrough{Convert: columnless}, committed)
			j.Sync, j.Destination = sync, dest
			j.Streams = []runner.Stream{{Name: "a", Replay: source.Exact, Mode: destination.Overwrite}, {Name: "b", Replay: source.Exact, Mode: destination.Overwrite}}
			_, err := runner.RunAttempt(ctx, j)
			time.Sleep(time.Second)
			return err
		}
		if err := attempt("s1", true); err == nil {
			t.Fatal("the first attempt succeeded")
		}
		if err := attempt("s1", false); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if err := attempt("s2", false); err != nil {
			t.Fatal(err)
		}

		if len(dest.starts) != 3 {
			t.Fatalf("StartReads called %d times, want once an attempt", len(dest.starts))
		}
		first, retry, next := dest.starts[0], dest.starts[1], dest.starts[2]
		if len(first) != 2 || len(retry) != 1 || len(next) != 2 {
			t.Fatalf("reads started: %v, %v, %v; want a and b, then only b, then both", first, retry, next)
		}
		if !retry["b"].Equal(first["b"]) || !next["a"].After(first["a"]) || !next["b"].After(first["b"]) {
			t.Errorf("starts %v, %v, %v: the retry should continue b's read, and the next sync start new ones", first, retry, next)
		}
		ends := strings.Join(slices.DeleteFunc(slices.Clone(dest.log), func(l string) bool { return !strings.HasPrefix(l, "end") && l != "attempt" }), " ")
		if !regexp.MustCompile(`^attempt end \d+ a attempt end \d+ b attempt (end \d+ [ab] ){2}$`).MatchString(ends + " ") {
			t.Errorf("reads ended: %s; want a in the first attempt, b in the retry, and both in the next sync", ends)
		}
		for i, l := range dest.log {
			var n uint64
			var s string
			if _, err := fmt.Sscanf(l, "end %d %s", &n, &s); err != nil {
				continue
			}
			rest := dest.log[i+1:]
			pre := slices.Index(rest, fmt.Sprintf("precommit %d", n))
			if pre < 0 {
				t.Errorf("%q: no PreCommit of its epoch follows", l)
			}
			for _, later := range rest {
				var m uint64
				var ws string
				if _, err := fmt.Sscanf(later, "write %d %s", &m, &ws); err == nil && ws == s && m <= n {
					t.Errorf("%q: a write in epoch %d follows it", l, m)
				}
			}
		}
	})
}

// readsDest logs writes, reads' ends and pre-commits, and each attempt's
// read starts.
type readsDest struct {
	countingDest
	log    []string
	starts []map[string]time.Time
}

func (d *readsDest) Capabilities() destination.Capabilities {
	c := d.countingDest.Capabilities()
	c.WriteModes = []destination.WriteMode{destination.Overwrite}
	return c
}

func (d *readsDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *readsDest) Write(ctx context.Context, e destination.Epoch, s string, b arrow.RecordBatch) error {
	d.log = append(d.log, fmt.Sprintf("write %d %s", e.N, s))
	return d.countingDest.Write(ctx, e, s, b)
}

func (d *readsDest) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	d.log = append(d.log, fmt.Sprintf("precommit %d", e.N))
	return nil, nil
}

func (d *readsDest) StartReads(_ context.Context, starts map[string]time.Time) error {
	d.starts = append(d.starts, starts)
	return nil
}

func (d *readsDest) EndRead(_ context.Context, e destination.Epoch, s string) error {
	d.log = append(d.log, fmt.Sprintf("end %d %s", e.N, s))
	return nil
}

// A continuous job commits on its interval while its source emits, commits
// nothing while it idles, and stops at a checkpoint when drained. The next
// job carries on from there.
func TestContinuousJob(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
		defer cancel()
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		dest := &countingDest{}
		run := func(owner string, pages int) runner.Result {
			drain := make(chan struct{})
			j := job(log, owner, endlessSource{pages: pages}, runner.Passthrough{Convert: columnless}, make(chan uint64, 64))
			j.Destination, j.Drain = dest, drain
			j.Settings.CommitMaxBytes = 1 << 40
			j.Settings.CommitInterval = 2200 * time.Millisecond
			time.AfterFunc(time.Minute, func() { close(drain) })
			res, err := runner.RunAttempt(ctx, j)
			if !errors.Is(err, runner.ErrDrained) {
				t.Fatalf("%s: got %v, want ErrDrained", owner, err)
			}
			return res
		}
		// Rows at 1 to 5 s land in epochs at 2.2, 4.4 and 6.6 s, and the
		// drain's epoch at a minute holds nothing.
		if res := run("a1", 5); res.Committed != 4 || dest.rows != 5 {
			t.Fatalf("committed %d epochs and %d rows, want 4 and 5", res.Committed, dest.rows)
		}
		if res := run("a2", 8); res.Restored != 4 || dest.rows != 8 {
			t.Fatalf("restored %d and wrote %d rows, want 4 and 8", res.Restored, dest.rows)
		}
	})
}

// An attempt that fails once another attempt has claimed the pipeline was
// fenced, whatever it failed on, as a source connection the other took over.
// One that fails while it holds the pipeline wasn't.
func TestFailureAfterAnotherClaimIsFenced(t *testing.T) {
	for _, claimed := range []bool{false, true} {
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		lost := errors.New("terminating connection due to administrator command")
		src := failingSource{err: lost, before: func() {
			if claimed {
				if _, err := log.Claim(t.Context(), "a2"); err != nil {
					t.Error(err)
				}
			}
		}}
		_, err := runner.RunAttempt(t.Context(), job(log, "a1", src, runner.Passthrough{Convert: columnless}, make(chan uint64, 64)))
		if !errors.Is(err, lost) {
			t.Errorf("claimed by another: %v; got %v, want the source's error", claimed, err)
		}
		if errors.Is(err, checkpoint.ErrFenced) != claimed {
			t.Errorf("claimed by another: %v; got %v", claimed, err)
		}
	}
}

type failingSource struct {
	err    error
	before func()
}

func (s failingSource) Read(context.Context, source.ReadRequest, source.Emitter) error {
	s.before()
	return s.err
}

// endlessSource emits a row and a resume point a second from its state, up
// to pages, then idles until it's cancelled, as a quiet stream does.
type endlessSource struct{ pages int }

func (s endlessSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	i := 0
	if st := req.Streams[0].State; st != nil {
		i, _ = strconv.Atoi(string(st))
	}
	for ; ; i++ {
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
		if i >= s.pages {
			continue
		}
		if err := out.Rows("s", []json.RawMessage{json.RawMessage(`{}`)}); err != nil {
			return err
		}
		if err := out.ResumePoint("s", strconv.AppendInt(nil, int64(i+1), 10)); err != nil {
			return err
		}
	}
}

// Each record says where its sync started and what the sync has committed so
// far, across the sync's attempts, so the last one holds the whole sync.
func TestRecordsCarryTheSyncSoFar(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		read := map[string]int{}
		attempts := 0
		attempt := func(sync string, fail bool) *checkpoint.Record {
			committed := make(chan uint64, 16)
			attempts++
			j := job(log, fmt.Sprintf("a%d", attempts), fullSource{pages: 2, committed: committed, read: read, fail: fail}, runner.Passthrough{Convert: columnless}, committed)
			j.Sync = sync
			j.Streams = []runner.Stream{{Name: "a", Replay: source.Exact, Mode: destination.Append}, {Name: "b", Replay: source.Exact, Mode: destination.Append}}
			_, _ = runner.RunAttempt(ctx, j)
			time.Sleep(time.Second)
			head, err := log.Latest(ctx)
			if err != nil {
				t.Fatal(err)
			}
			return head
		}
		check := func(head *checkpoint.Record, sync string, start checkpoint.RecordRef, a, b uint64) {
			t.Helper()
			if head.Sync != sync || head.SyncStart == nil || *head.SyncStart != start || head.SyncRows["a"] != a || head.SyncRows["b"] != b {
				t.Errorf("HEAD %d-g%d: sync %s from %+v with %v; want %s from %+v with a: %d, b: %d", head.Epoch, head.Generation, head.Sync, head.SyncStart, head.SyncRows, sync, start, a, b)
			}
		}
		check(attempt("s1", true), "s1", checkpoint.RecordRef{}, 2, 1)
		first := attempt("s1", false)
		check(first, "s1", checkpoint.RecordRef{}, 2, 2)
		check(attempt("s2", false), "s2", first.Ref(), 2, 2)
		if head, err := log.Latest(ctx); err != nil || head == nil {
			t.Fatalf("Latest: %v, %v", head, err)
		}
	})
}
