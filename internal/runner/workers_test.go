package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/runner/attempt"
	"github.com/openetl-io/openetl/internal/runner/flinktest"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// The attempt protocol's timings in these tests.
var workerOpts = attempt.Options{Heartbeat: 50 * time.Millisecond, Timeout: 2 * time.Second, JoinTimeout: 10 * time.Second}

// rangeSource reads stream "s" in splits s0, s1 and so on, split i holding
// ids i*rows+1 to (i+1)*rows, a page at a time with a resume point after
// each. Its state is the last id it emitted.
type rangeSource struct {
	splits, rows, page int
	// Set: each split waits for it after its first page.
	gate <-chan struct{}
	// Between pages.
	pause time.Duration
	// Set: a split that has read its rows keeps its read open until it's
	// cancelled, instead of completing.
	open bool
	// Set: a read from a state pages a row short first, so that reading
	// again misses the resume points of the first read.
	shift bool
	// Set: a split's read stops at its Until, and completes there.
	until bool
	// Rows each split has past its rows, as when its upstream moved on
	// after a read completed it: their ids go on from its last.
	grown int
	// Set: a split's read stops at its CompleteAt, and completes there.
	completeAt bool
	// Set: a split's last page goes with its completion, with no resume
	// point between, as a source's that flushes what it holds as it
	// completes.
	flushes bool
	// What the worker acknowledged, and read, and each split's read as
	// "split@state" where it began, and as "split@state" where it was asked
	// to complete.
	mu          sync.Mutex
	acked       map[string]string
	read        []string
	from        []string
	completions []string
}

func (s *rangeSource) Splits(_ context.Context, _ source.ConfiguredStream, _ []source.Split) ([]source.Split, error) {
	out := make([]source.Split, s.splits)
	for i := range out {
		out[i] = source.Split{ID: fmt.Sprintf("s%d", i)}
	}
	return out, nil
}

func (s *rangeSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	return source.ReadSplits(ctx, req, out, func(ctx context.Context, _ source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		i, err := strconv.Atoi(strings.TrimPrefix(sp.ID, "s"))
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.read = append(s.read, sp.ID)
		s.from = append(s.from, sp.ID+"@"+string(sp.State))
		if sp.CompleteAt != nil {
			s.completions = append(s.completions, sp.ID+"@"+string(sp.CompleteAt))
		}
		s.mu.Unlock()
		next, end := i*s.rows+1, (i+1)*s.rows+s.grown
		if sp.State != nil {
			last, err := strconv.Atoi(string(sp.State))
			if err != nil {
				return err
			}
			next = last + 1
		}
		if s.until && sp.Until != nil {
			u, err := strconv.Atoi(string(sp.Until))
			if err != nil {
				return err
			}
			end = min(end, u)
		}
		if s.completeAt && sp.CompleteAt != nil {
			c, err := strconv.Atoi(string(sp.CompleteAt))
			if err != nil {
				return err
			}
			end = min(end, c)
		}
		for first := true; next <= end; first = false {
			page := s.page
			if first && s.shift && sp.State != nil {
				page = max(page-1, 1)
			}
			var rows []json.RawMessage
			for ; len(rows) < page && next <= end; next++ {
				rows = append(rows, json.RawMessage(fmt.Sprintf(`{"id":%d}`, next)))
			}
			if err := e.Rows(rows); err != nil {
				return err
			}
			if s.flushes && next > end {
				break
			}
			if err := e.ResumePoint([]byte(strconv.Itoa(next - 1))); err != nil {
				return err
			}
			if first && s.gate != nil {
				select {
				case <-s.gate:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			select {
			case <-time.After(s.pause):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if s.open {
			<-ctx.Done()
			return ctx.Err()
		}
		return e.Complete([]byte(strconv.Itoa(end)))
	})
}

func (s *rangeSource) Committed(_ context.Context, _ map[string][]byte, splits map[string][]source.Split) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.acked == nil {
		s.acked = map[string]string{}
	}
	for _, sp := range splits["s"] {
		s.acked[sp.ID] = string(sp.State)
	}
	return nil
}

// noSource is the coordinator's: it never reads.
type noSource struct{ t *testing.T }

func (s noSource) Read(context.Context, source.ReadRequest, source.Emitter) error {
	s.t.Error("the coordinator read the source")
	return errors.New("the coordinator reads nothing")
}

// laneStore is a stage-and-apply destination as every worker's writers see
// it: each writer lane stages its part of an epoch, and a commit applies
// every lane's staging with a marker, behind a fence.
type laneStore struct {
	mu      sync.Mutex
	fence   uint64
	writing map[stageKey][]int64
	staged  map[stageKey][]int64
	marked  map[destination.Epoch]bool
	rows    []int64
	commits map[destination.Epoch]int
	// Called before and after a commit applies; an error fails it.
	before, after func(e destination.Epoch) error
	// Called as a writer starts to recover; an error fails it.
	recovering func(ctx context.Context) error
}

type stageKey struct {
	E    destination.Epoch
	Lane int
}

func newLaneStore() *laneStore {
	return &laneStore{writing: map[stageKey][]int64{}, staged: map[stageKey][]int64{}, marked: map[destination.Epoch]bool{}, commits: map[destination.Epoch]int{}}
}

// stagingDest is the destination in a runner: the coordinator's opens nothing.
type stagingDest struct {
	s           *laneStore
	t           *testing.T
	coordinator bool
}

func (d stagingDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopePipeline}, WriterLanes: 16}
}

func (d stagingDest) Open(_ context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	if d.coordinator {
		d.t.Error("the coordinator opened the destination")
		return nil, errors.New("the coordinator holds no credentials")
	}
	return &stagingWriter{s: d.s, lane: cfg.Lane}, nil
}

type stagingWriter struct {
	s    *laneStore
	lane int
}

func (w *stagingWriter) Write(_ context.Context, e destination.Epoch, _ string, b arrow.RecordBatch) error {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	k := stageKey{e, w.lane}
	w.s.writing[k] = append(w.s.writing[k], b.Column(b.Schema().FieldIndices("id")[0]).(*array.Int64).Int64Values()...)
	return nil
}

func (w *stagingWriter) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	if e.Generation < w.s.fence {
		return nil, destination.ErrFenced
	}
	k := stageKey{e, w.lane}
	w.s.staged[k] = w.s.writing[k]
	delete(w.s.writing, k)
	return json.Marshal(k)
}

func (w *stagingWriter) Commit(_ context.Context, e destination.Epoch, staged []destination.Staged) (destination.Committed, error) {
	s := w.s
	s.mu.Lock()
	s.commits[e]++
	before, after := s.before, s.after
	s.mu.Unlock()
	if before != nil {
		if err := before(e); err != nil {
			return destination.Committed{}, err
		}
	}
	s.mu.Lock()
	if e.Generation < s.fence {
		s.mu.Unlock()
		return destination.Committed{}, destination.ErrFenced
	}
	err := s.apply(e, staged)
	s.mu.Unlock()
	if err == nil && after != nil {
		err = after(e)
	}
	return destination.Committed{}, err
}

// apply makes e's staging visible once; s.mu is held.
func (s *laneStore) apply(e destination.Epoch, staged []destination.Staged) error {
	if s.marked[e] {
		return nil
	}
	var rows []int64
	for _, c := range staged {
		var k stageKey
		if err := json.Unmarshal(c, &k); err != nil {
			return err
		}
		got, ok := s.staged[k]
		if !ok {
			return fmt.Errorf("lane %d's staging of epoch %d is gone", k.Lane, k.E.N)
		}
		rows = append(rows, got...)
	}
	s.rows = append(s.rows, rows...)
	s.marked[e] = true
	return nil
}

func (w *stagingWriter) Recover(ctx context.Context, gen uint64, e destination.Epoch, staged []destination.Staged) (destination.Recovery, error) {
	s := w.s
	s.mu.Lock()
	recovering := s.recovering
	s.mu.Unlock()
	if recovering != nil {
		if err := recovering(ctx); err != nil {
			return 0, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fence = max(s.fence, gen)
	switch {
	case e.IsZero() || s.marked[e]:
		return destination.Applied, nil
	case s.apply(e, staged) == nil:
		return destination.Redone, nil
	}
	return destination.Lost, nil
}

func (w *stagingWriter) Close(context.Context) error { return nil }

// check fails unless the destination holds ids 1 to n once each.
func (s *laneStore) check(t *testing.T, n int) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	got := slices.Sorted(slices.Values(s.rows))
	if len(got) != n {
		t.Fatalf("%d rows, want %d: %v", len(got), n, got)
	}
	for i, id := range got {
		if id != int64(i+1) {
			t.Fatalf("row %d is id %d: %v", i, id, got)
		}
	}
}

// pool runs a pipeline's attempts, on workers in this process.
type pool struct {
	t     *testing.T
	log   *checkpoint.Log
	store *laneStore
	srv   *attempt.Server
	// Each worker's source, by attempt.
	mu      sync.Mutex
	sources map[string][]*rangeSource
	cancels map[string][]context.CancelFunc
	done    map[string]chan error
	// Makes each attempt's workers' sources; reader, if set, in its place.
	source func() *rangeSource
	reader func() source.Reader
	// The workers' dataflow; nil: Passthrough.
	flow runner.Dataflow
	// The destination, the coordinator's or a worker's; nil: store's.
	dest func(coordinator bool) destination.Destination
	// Changes each attempt's job, the coordinator's and its workers'.
	change func(*runner.Job)
}

func (p *pool) destination(coordinator bool) destination.Destination {
	if p.dest != nil {
		return p.dest(coordinator)
	}
	return stagingDest{s: p.store, t: p.t, coordinator: coordinator}
}

func newPool(t *testing.T, source func() *rangeSource) *pool {
	return &pool{
		t: t, log: checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"), store: newLaneStore(), srv: listen(t),
		sources: map[string][]*rangeSource{}, cancels: map[string][]context.CancelFunc{}, done: map[string]chan error{}, source: source,
	}
}

func listen(t *testing.T) *attempt.Server {
	t.Helper()
	s, err := attempt.Listen(t.Context(), attempt.ListenConfig{Address: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

// job is the pipeline's job for attempt owner, without workers.
func (p *pool) job(owner string, src source.Reader, dest destination.Destination) runner.Job {
	j := runner.Job{
		Owner: owner, Sync: "sync", Pipeline: "p", Log: p.log, Source: src,
		Streams:     []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append}},
		Destination: dest,
		Dataflow:    runner.Passthrough{Convert: idRows},
		Settings:    delivery.Settings{Require: delivery.ExactlyOnce, CommitMaxBytes: 1, Scope: destination.ScopePipeline},
		Lanes:       2,
	}
	if p.change != nil {
		p.change(&j)
	}
	return j
}

// workers is the pipeline's job for attempt owner on n workers, which
// launch in this process.
func (p *pool) workers(owner string, n int) runner.Job {
	j := p.job(owner, noSource{p.t}, p.destination(true))
	j.Workers = &runner.Workers{Count: n, Server: p.srv, Options: workerOpts, Launch: func(ctx context.Context, targets []attempt.Target) error {
		done := make(chan error, len(targets))
		p.mu.Lock()
		p.done[owner] = done
		for _, tg := range targets {
			var src source.Reader
			if p.reader != nil {
				src = p.reader()
			} else {
				rs := p.source()
				p.sources[owner] = append(p.sources[owner], rs)
				src = rs
			}
			wctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
			p.cancels[owner] = append(p.cancels[owner], cancel)
			wj := p.job(owner, src, p.destination(false))
			wj.Log = nil
			if p.flow != nil {
				wj.Dataflow = p.flow
			}
			go func() {
				defer cancel()
				done <- runner.RunWorker(wctx, wj, tg, workerOpts)
			}()
		}
		p.mu.Unlock()
		return nil
	}}
	return j
}

// kill stops worker i of attempt owner.
func (p *pool) kill(owner string, i int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cancels[owner][i]()
}

// wait waits for attempt owner's workers to end, and returns how they did.
func (p *pool) wait(owner string) []error {
	p.t.Helper()
	p.mu.Lock()
	done, n := p.done[owner], len(p.cancels[owner])
	p.mu.Unlock()
	var errs []error
	for range n {
		select {
		case err := <-done:
			errs = append(errs, err)
		case <-time.After(time.Minute):
			p.t.Fatalf("attempt %s's workers didn't end", owner)
		}
	}
	return errs
}

func (p *pool) head() *checkpoint.Record {
	p.t.Helper()
	rec, err := p.log.Latest(p.t.Context())
	if err != nil {
		p.t.Fatal(err)
	}
	return rec
}

func ranges(splits, rows, page int) func() *rangeSource {
	return func() *rangeSource {
		return &rangeSource{splits: splits, rows: rows, page: page, pause: 2 * time.Millisecond}
	}
}

// Workers read their splits and write their committables, and the
// coordinator, which reads and writes nothing, commits every row once.
func TestWorkersFanOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	// Each split's pages past its first wait for the first commit, so the
	// rows take several epochs however fast they're read.
	gate := make(chan struct{})
	p := newPool(t, func() *rangeSource {
		s := ranges(6, 20, 5)()
		s.gate = gate
		return s
	})
	j := p.workers("a1", 3)
	var once sync.Once
	j.OnCommit = func(uint64) { once.Do(func() { close(gate) }) }
	// Each epoch's rows, by split, and the splits its committables hold.
	rows, held := map[string]uint64{}, map[string]bool{}
	var checkpoints []*checkpoint.Record
	j.OnCheckpoint = func(rec *checkpoint.Record, _, _ map[string]uint64) {
		checkpoints = append(checkpoints, rec)
		rows["s"] += rec.Streams["s"].Rows
		for _, c := range rec.Coverage {
			for _, s := range c.Splits {
				held[s.ID] = true
			}
		}
	}
	res, err := runner.RunAttempt(ctx, j)
	if err != nil {
		t.Fatal(err)
	}
	for i, err := range p.wait("a1") {
		if err != nil {
			t.Errorf("worker %d: %v", i, err)
		}
	}
	p.store.check(t, 120)
	if res.Committed < 2 {
		t.Errorf("%d epochs committed", res.Committed)
	}
	rec := p.head()
	if !rec.Streams["s"].Complete || len(rec.Splits("s")) != 6 {
		t.Fatalf("the record has %+v", rec.Streams["s"])
	}
	// Each worker read two splits, and acknowledged only its own.
	read := map[string]int{}
	for i, src := range p.sources["a1"] {
		src.mu.Lock()
		if len(src.read) != 2 {
			t.Errorf("worker %d read %v", i, src.read)
		}
		for id := range src.acked {
			if !slices.Contains(src.read, id) {
				t.Errorf("worker %d acknowledged split %s, which it didn't read", i, id)
			}
		}
		for _, id := range src.read {
			read[id]++
		}
		src.mu.Unlock()
	}
	if len(read) != 6 {
		t.Errorf("splits read: %v", read)
	}
	// Each committable says what it holds, and every split is held by one.
	for _, r := range checkpoints {
		if len(r.Coverage) != len(r.Committables) || len(r.Committables) < 3 {
			t.Fatalf("record %d: %d coverages of %d committables", r.Epoch, len(r.Coverage), len(r.Committables))
		}
	}
	if len(held) != 6 || rows["s"] != 120 {
		t.Errorf("committables held splits %v, and records counted %d rows", held, rows["s"])
	}
}

// A worker lost mid-epoch fails the attempt, and the next one, on other
// workers, restores from the record and lands every row once.
func TestAWorkerLostMidEpoch(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	gate := make(chan struct{})
	p := newPool(t, func() *rangeSource {
		s := ranges(6, 20, 5)()
		s.gate = gate
		return s
	})
	j := p.workers("a1", 3)
	var once sync.Once
	j.OnCommit = func(uint64) { once.Do(func() { p.kill("a1", 1) }) }
	if _, err := runner.RunAttempt(ctx, j); err == nil {
		t.Fatal("the attempt went on without its worker")
	}
	p.wait("a1")
	close(gate)
	res, err := runner.RunAttempt(ctx, p.workers("a2", 2))
	if err != nil {
		t.Fatal(err)
	}
	p.wait("a2")
	if res.Restored == 0 {
		t.Error("the second attempt restored nothing")
	}
	p.store.check(t, 120)
}

// A worker stopped mid-epoch, as a pod is on a node drain, fails the attempt
// as transient, whatever its part failed with as it stopped: it's retried
// like a worker lost.
func TestAStoppedWorkerFailsTheAttemptAsTransient(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	gate := make(chan struct{})
	defer close(gate)
	p := newPool(t, func() *rangeSource {
		s := ranges(6, 20, 5)()
		s.gate = gate
		return s
	})
	j := p.workers("a1", 3)
	var once sync.Once
	j.OnCommit = func(uint64) { once.Do(func() { p.kill("a1", 1) }) }
	_, err := runner.RunAttempt(ctx, j)
	if errs.CodeOf(err) != errs.Transient || !strings.Contains(fmt.Sprint(err), "worker 2") {
		t.Fatalf("the attempt failed with %v (%s), want worker 2 transient", err, errs.CodeOf(err))
	}
	ended := p.wait("a1")
	if !slices.ContainsFunc(ended, func(err error) bool {
		return errs.CodeOf(err) == errs.Transient && strings.Contains(err.Error(), "stopped")
	}) {
		t.Errorf("the workers ended with %v, want the stopped one transient", ended)
	}
}

// A worker stopped while it answers a call, here settling the restored
// epoch, fails the attempt as transient too: its answer says it stopped.
func TestAWorkerStoppedInACallFailsTheAttemptAsTransient(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	p := newPool(t, ranges(6, 20, 5))
	p.store.mu.Lock()
	p.store.recovering = func(ctx context.Context) error {
		p.kill("a1", 0)
		<-ctx.Done()
		return ctx.Err()
	}
	p.store.mu.Unlock()
	_, err := runner.RunAttempt(ctx, p.workers("a1", 2))
	if errs.CodeOf(err) != errs.Transient || !strings.Contains(fmt.Sprint(err), "stopped") {
		t.Fatalf("the attempt failed with %v (%s), want the stopped worker's transient", err, errs.CodeOf(err))
	}
	p.wait("a1")
}

// A worker that fails before it joins, as when its connectors don't start,
// joins only to say so: the attempt fails at once with the worker's error
// and its code, not once the workers' time to join is up; one stopped as it
// starts, as its pod is, says so as transient. The worker joining after
// that, as a late join would, is refused.
func TestAWorkerThatFailsBeforeItJoinsFailsTheAttemptAtOnce(t *testing.T) {
	for name, tc := range map[string]struct {
		err     error
		stopped bool
		want    errs.Code
	}{
		"transient":      {err: errs.New(errs.Transient, "connector: the source didn't start: the database is restarting"), want: errs.Transient},
		"config invalid": {err: errs.New(errs.ConfigInvalid, "connector: the source didn't start: it has no host"), want: errs.ConfigInvalid},
		"stopped":        {err: errs.Wrap(errs.Internal, context.Canceled, "connector: the source didn't start"), stopped: true, want: errs.Transient},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			p := newPool(t, ranges(6, 20, 5))
			j := p.workers("a1", 2)
			j.Workers.Options.JoinTimeout = time.Hour
			launch := j.Workers.Launch
			var late attempt.Target
			reported := make(chan error, 1)
			j.Workers.Launch = func(ctx context.Context, targets []attempt.Target) error {
				late = targets[1]
				tg := targets[1]
				var err error
				if tg.Listen, tg.Exchange, tg.Held, err = attempt.ExchangeAddresses(ctx, "", ""); err != nil {
					return err
				}
				wctx, stop := context.WithCancel(context.WithoutCancel(ctx))
				if tc.stopped {
					stop()
				}
				go func() {
					defer stop()
					reported <- runner.FailBeforeJoining(wctx, tg, workerOpts, tc.err)
				}()
				return launch(ctx, targets[:1])
			}
			began := time.Now()
			_, err := runner.RunAttempt(ctx, j)
			took := time.Since(began)
			switch msg := fmt.Sprint(err); {
			case errs.CodeOf(err) != tc.want || !strings.Contains(msg, "worker 2 failed: ") || !strings.Contains(msg, tc.err.Error()):
				t.Fatalf("the attempt failed with %v (%s), want worker 2's error, %s", err, errs.CodeOf(err), tc.want)
			case took > 10*time.Second:
				t.Errorf("the attempt took %s to fail", took)
			case tc.stopped && !strings.Contains(msg, "the worker was stopped"):
				t.Errorf("the attempt failed with %v, want the worker stopped", err)
			}
			if err := <-reported; errs.CodeOf(err) != tc.want || !strings.Contains(err.Error(), tc.err.Error()) {
				t.Errorf("the worker reported %v (%s)", err, errs.CodeOf(err))
			}
			p.wait("a1")
			err = runner.RunWorker(ctx, p.job("a1", ranges(6, 20, 5)(), p.destination(false)), late, workerOpts)
			if !errors.Is(err, attempt.ErrRefused) || errs.CodeOf(err) != errs.Fenced {
				t.Fatalf("worker 2 joining after the attempt failed: %v (%s), want refused", err, errs.CodeOf(err))
			}
		})
	}
}

// A worker lost between record N and its commit fails the attempt. The
// next one's workers settle epoch N from its staging, or find its marker
// if the commit happened, before anything is assigned.
func TestAWorkerLostBetweenTheRecordAndItsCommit(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprintf("applied=%v", applied), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			p := newPool(t, ranges(6, 40, 5))
			var once sync.Once
			lose := func(e destination.Epoch) error {
				var err error
				if e.N == 2 {
					once.Do(func() {
						p.kill("a1", 0)
						err = errors.New("worker 0 is gone")
					})
				}
				return err
			}
			p.store.mu.Lock()
			if applied {
				p.store.after = lose
			} else {
				p.store.before = lose
			}
			p.store.mu.Unlock()
			if _, err := runner.RunAttempt(ctx, p.workers("a1", 3)); err == nil {
				t.Fatal("the attempt went on without its worker")
			}
			p.wait("a1")
			head := p.head()
			p.store.mu.Lock()
			p.store.before, p.store.after = nil, nil
			marked := p.store.marked[destination.Epoch{Generation: head.Generation, N: head.Epoch}]
			p.store.mu.Unlock()
			if head.Epoch != 2 || marked != applied {
				t.Fatalf("HEAD is epoch %d, applied %v", head.Epoch, marked)
			}
			res, err := runner.RunAttempt(ctx, p.workers("a2", 2))
			if err != nil {
				t.Fatal(err)
			}
			p.wait("a2")
			want := destination.Redone
			if applied {
				want = destination.Applied
			}
			if res.Recovery != want || res.Restored != 2 {
				t.Errorf("recovered %s, restoring %d; want %s, 2", res.Recovery, res.Restored, want)
			}
			p.store.check(t, 240)
		})
	}
}

// A coordinator lost fails the attempt: its workers stop, and the next
// attempt restores from the record.
func TestTheCoordinatorLost(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	p := newPool(t, ranges(6, 40, 5))
	srv := listen(t)
	j := p.workers("a1", 3)
	j.Workers.Server = srv
	cctx, lose := context.WithCancel(ctx)
	defer lose()
	var once sync.Once
	j.OnCommit = func(uint64) {
		once.Do(func() {
			go func() {
				srv.Close()
				lose()
			}()
		})
	}
	if _, err := runner.RunAttempt(cctx, j); err == nil {
		t.Fatal("the attempt went on without its coordinator")
	}
	for i, err := range p.wait("a1") {
		if err == nil {
			t.Errorf("worker %d finished without its coordinator", i)
		}
	}
	res, err := runner.RunAttempt(ctx, p.workers("a2", 3))
	if err != nil {
		t.Fatal(err)
	}
	p.wait("a2")
	if res.Restored == 0 {
		t.Error("the second attempt restored nothing")
	}
	p.store.check(t, 240)
}

// A commit that fails is asked for again: one that failed before it applied
// applies then, and one that applied and failed to say so applies once.
func TestCommitRequestsRepeat(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	p := newPool(t, ranges(6, 40, 5))
	failed := map[string]bool{}
	failOnce := func(when string, n uint64) func(destination.Epoch) error {
		return func(e destination.Epoch) error {
			if e.N != n || failed[when] {
				return nil
			}
			failed[when] = true
			return fmt.Errorf("epoch %d failed %s it applied", n, when)
		}
	}
	p.store.before, p.store.after = failOnce("before", 3), failOnce("after", 2)
	res, err := runner.RunAttempt(ctx, p.workers("a1", 3))
	if err != nil {
		t.Fatal(err)
	}
	for i, err := range p.wait("a1") {
		if err != nil {
			t.Errorf("worker %d: %v", i, err)
		}
	}
	p.store.check(t, 240)
	p.store.mu.Lock()
	defer p.store.mu.Unlock()
	for _, n := range []uint64{2, 3} {
		if c := p.store.commits[destination.Epoch{Generation: res.Generation, N: n}]; c < 2 {
			t.Errorf("epoch %d was asked to commit %d times", n, c)
		}
	}
}

// drains has an attempt stop at the checkpoint after its first commit.
func drains(j *runner.Job) {
	ch := make(chan struct{})
	var once sync.Once
	j.Drain, j.OnCommit = ch, func(uint64) { once.Do(func() { close(ch) }) }
}

// drainsHolding drains j once a commit leaves s holding n rows.
func drainsHolding(j *runner.Job, s *laneStore, n int) {
	ch := make(chan struct{})
	var once sync.Once
	j.Drain, j.OnCommit = ch, func(uint64) {
		s.mu.Lock()
		held := len(s.rows)
		s.mu.Unlock()
		if held >= n {
			once.Do(func() { close(ch) })
		}
	}
}

// Attempts on one runner, three workers and one restore from each other's
// records, whatever the layout that wrote them. Each but the last keeps its
// read open, so it ends only by draining.
func TestRescalingBetweenAttempts(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	p := newPool(t, func() *rangeSource {
		s := ranges(6, 100, 5)()
		s.open = true
		return s
	})
	single := p.job("a1", p.source(), stagingDest{s: p.store, t: t})
	drains(&single)
	last, err := runner.RunAttempt(ctx, single)
	if !errors.Is(err, runner.ErrDrained) {
		t.Fatalf("the single runner: %v", err)
	}
	for i, n := range []int{3, 1} {
		owner := fmt.Sprintf("w%d", i)
		j := p.workers(owner, n)
		drains(&j)
		res, err := runner.RunAttempt(ctx, j)
		if !errors.Is(err, runner.ErrDrained) {
			t.Fatalf("%d workers: %v", n, err)
		}
		p.wait(owner)
		if res.Restored != last.Last || res.Committed == 0 {
			t.Fatalf("%d workers restored %d, after %d, and committed %d", n, res.Restored, last.Last, res.Committed)
		}
		last = res
	}
	p.source = ranges(6, 100, 5)
	res, err := runner.RunAttempt(ctx, p.job("a2", p.source(), stagingDest{s: p.store, t: t}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Restored != last.Last {
		t.Errorf("the single runner restored %d, after %d", res.Restored, last.Last)
	}
	p.store.check(t, 600)
}

// proposingFlow passes rows through once its worker has had the stream's
// owner take a schema of its own, proposing again from each newer version
// it hears of, as an engine's lanes do.
type proposingFlow struct {
	mu *sync.Mutex
	// Each worker's version, by its first lane.
	taken map[int]uint64
}

func (f proposingFlow) Run(ctx context.Context, plan runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	o := plan.SchemaOwner
	if o == nil {
		return errors.New("no schema owner")
	}
	mine, err := arrowipc.EncodeSchema(arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: fmt.Sprintf("lane%d", plan.FirstLane), Type: arrow.PrimitiveTypes.Int64},
	}, nil))
	if err != nil {
		return err
	}
	var base uint64
	for taken := false; !taken; {
		if err := o.Propose(runner.SchemaProposal{Stream: "s", Base: base, Schema: mine}); err != nil {
			return err
		}
		for {
			var u runner.SchemaUpdate
			select {
			case u = <-o.Updates():
			case <-ctx.Done():
				return ctx.Err()
			}
			// An owner's answer, and its change sent to every worker, can
			// both bring a version.
			if u.Version <= base {
				continue
			}
			taken = u.Version == base+1 && slices.Equal(u.Schema, mine)
			base = u.Version
			break
		}
	}
	f.mu.Lock()
	f.taken[plan.FirstLane] = base
	f.mu.Unlock()
	return runner.Passthrough{Convert: idRows}.Run(ctx, plan, in, out)
}

// Each stream's schema has one owner across workers: of three workers that
// change it from one version, one's change is taken, and the others propose
// theirs again from it, and the record holds the newest.
func TestOneSchemaOwnerAcrossWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	p := newPool(t, ranges(3, 20, 5))
	flow := proposingFlow{mu: &sync.Mutex{}, taken: map[int]uint64{}}
	p.flow = flow
	if _, err := runner.RunAttempt(ctx, p.workers("a1", 3)); err != nil {
		t.Fatal(err)
	}
	p.wait("a1")
	p.store.check(t, 60)
	if versions := slices.Sorted(maps.Values(flow.taken)); !slices.Equal(versions, []uint64{1, 2, 3}) {
		t.Fatalf("the workers' changes were taken as versions %v", versions)
	}
	schema, err := arrowipc.DecodeSchema(p.head().Streams["s"].Schema)
	if err != nil {
		t.Fatal(err)
	}
	for lane, v := range flow.taken {
		if v == 3 && (schema.NumFields() != 2 || schema.Field(1).Name != fmt.Sprintf("lane%d", lane)) {
			t.Fatalf("the record's schema is %v, not version 3, lane %d's", schema, lane)
		}
	}
}

// aheadFlow passes rows through on each worker of an attempt whose stream s
// has one owner, and records the schema of s each worker's plan restored,
// by attempt. The worker with the attempt's later lanes runs ahead. In
// attempt a1, before its share of epoch 1 goes, it proposes s's first
// columns, as a lane past barrier 1 would, and its share waits for the
// owner to take them; it then fails at its share of epoch 2, once epoch 1
// is committed.
type aheadFlow struct {
	mu       *sync.Mutex
	restored map[string][][]byte
	// Closed once a1 has committed epoch 1.
	committed <-chan struct{}
}

func (f aheadFlow) Run(ctx context.Context, plan runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	f.mu.Lock()
	f.restored[plan.Owner] = append(f.restored[plan.Owner], plan.Schemas["s"])
	f.mu.Unlock()
	ahead := plan.FirstLane > 0
	propose := func() error {
		o := plan.SchemaOwner
		if err := o.Propose(runner.SchemaProposal{Stream: "s", Schema: xSchema(arrow.PrimitiveTypes.Int64)}); err != nil {
			return err
		}
		for {
			select {
			case u := <-o.Updates():
				if u.Stream == "s" && u.Version > 0 {
					return nil
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	mid := make(chan runner.Frame)
	done := make(chan error, 1)
	go func() { done <- runner.Passthrough{Convert: idRows}.Run(ctx, plan, in, mid) }()
	defer close(out)
	for fr := range mid {
		if fr.Kind == runner.FrameReady && ahead && plan.Owner == "a1" {
			if fr.Epoch == 1 {
				if err := propose(); err != nil {
					return err
				}
			} else {
				select {
				case <-f.committed:
				case <-ctx.Done():
					return ctx.Err()
				}
				return errors.New("the worker that ran ahead is lost")
			}
		}
		select {
		case out <- fr:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return <-done
}

// A change the owner takes before it merges the first epoch with columns is
// part of the stream's first schema, whatever epoch's rows made it: the
// protocol's one exception. A worker whose lane runs ahead proposes s's
// first columns before its share of epoch 1 goes, and the record of epoch 1
// has them. So an attempt restored from it starts s with them, on every
// worker, as the lanes after epoch 1 shaped by them, and its records keep
// them.
func TestAChangeBeforeTheFirstMergeIsTheFirstSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	committed := make(chan struct{})
	// Each split's pages past its first wait for epoch 1's commit, so the
	// worker that runs ahead has a share of epoch 2 to fail at.
	p := newPool(t, func() *rangeSource {
		return &rangeSource{splits: 2, rows: 30, page: 5, pause: 2 * time.Millisecond, gate: committed}
	})
	flow := aheadFlow{mu: &sync.Mutex{}, restored: map[string][][]byte{}, committed: committed}
	p.flow = flow
	j := p.workers("a1", 2)
	var once sync.Once
	j.OnCommit = func(n uint64) {
		if n == 1 {
			once.Do(func() { close(committed) })
		}
	}
	if _, err := runner.RunAttempt(ctx, j); err == nil || !strings.Contains(err.Error(), "the worker that ran ahead is lost") {
		t.Fatalf("the attempt ended with %v, not the loss of the worker that ran ahead", err)
	}
	p.wait("a1")
	x := xSchema(arrow.PrimitiveTypes.Int64)
	if head := p.head(); head.Epoch != 1 || !slices.Equal(head.Streams["s"].Schema, x) {
		t.Fatalf("HEAD is epoch %d, with s's schema %q", head.Epoch, head.Streams["s"].Schema)
	}
	var mu sync.Mutex
	recorded := map[uint64][]byte{}
	j = p.workers("a2", 2)
	j.OnCheckpoint = func(rec *checkpoint.Record, _, _ map[string]uint64) {
		mu.Lock()
		recorded[rec.Epoch] = rec.Streams["s"].Schema
		mu.Unlock()
	}
	if _, err := runner.RunAttempt(ctx, j); err != nil {
		t.Fatal(err)
	}
	p.wait("a2")
	flow.mu.Lock()
	restored := flow.restored["a2"]
	flow.mu.Unlock()
	if len(restored) != 2 || slices.ContainsFunc(restored, func(s []byte) bool { return !slices.Equal(s, x) }) {
		t.Errorf("the second attempt's workers restored s's schema as %q", restored)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(recorded) == 0 || slices.ContainsFunc(slices.Collect(maps.Values(recorded)), func(s []byte) bool { return !slices.Equal(s, x) }) {
		t.Errorf("the second attempt's records have s's schemas %v", recorded)
	}
	p.store.check(t, 60)
}

// holdingFlow plays the engine of each worker of an attempt whose stream s
// holds. Unless s holds x already, worker 1 starts holding it, proposing the
// change to the coordinator, and every worker waits until it hears that x
// is held before it passes rows through. Each reports a held cell and a
// held file at every barrier, and the first worker, where a release is
// patched in, the replay's end.
type holdingFlow struct {
	mu *sync.Mutex
	// Each worker's hold plan, and whether it heard x is held, by its first
	// lane.
	plans map[int]*runner.HoldPlan
	heard map[int]bool
}

var heldX = func() checkpoint.ColumnHold {
	kept, err := arrowipc.EncodeSchema(arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil))
	if err != nil {
		panic(err)
	}
	return checkpoint.ColumnHold{Column: "x", Delivered: kept, Reason: "held", Cause: &checkpoint.HoldCause{Kind: checkpoint.HoldPolicy, Policy: "block"}}
}()

func xSchema(t arrow.DataType) []byte {
	b, err := arrowipc.EncodeSchema(arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "x", Type: t, Nullable: true}}, nil))
	if err != nil {
		panic(err)
	}
	return b
}

func (f holdingFlow) Run(ctx context.Context, plan runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	hp, o := plan.Holds["s"], plan.SchemaOwner
	if hp == nil || plan.HeldURL == "" || o == nil {
		return fmt.Errorf("worker %d: holds %v at %q, owner %v", plan.FirstLane, hp, plan.HeldURL, o)
	}
	f.mu.Lock()
	f.plans[plan.FirstLane] = hp
	f.mu.Unlock()
	if !slices.ContainsFunc(hp.Holding, func(c checkpoint.ColumnHold) bool { return c.Column == "x" }) && hp.Replay == nil {
		if plan.FirstLane > 0 {
			if err := o.Propose(runner.SchemaProposal{Stream: "s", Schema: xSchema(arrow.BinaryTypes.String), Holding: []checkpoint.ColumnHold{heldX}, Delivered: xSchema(arrow.PrimitiveTypes.Int64)}); err != nil {
				return err
			}
		}
		for heard := false; !heard; {
			select {
			case u := <-o.Updates():
				heard = !u.Input && u.Stream == "s" && slices.ContainsFunc(u.Holding, func(c checkpoint.ColumnHold) bool { return c.Column == "x" })
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		f.mu.Lock()
		f.heard[plan.FirstLane] = true
		f.mu.Unlock()
	}
	mid := make(chan runner.Frame)
	done := make(chan error, 1)
	go func() { done <- runner.Passthrough{Convert: idRows}.Run(ctx, plan, in, mid) }()
	defer close(out)
	for fr := range mid {
		if fr.Kind == runner.FrameReady {
			h := &runner.ReadyHolds{
				Cells: map[string]checkpoint.CellHold{"y": {Cells: 1, Reason: "range"}},
				Files: []destination.StagedFile{{Path: fmt.Sprintf("lane%d/%d", plan.FirstLane, fr.Epoch), Rows: 1}},
			}
			if hp.Replay != nil && plan.FirstLane == 0 {
				h.Replayed = &runner.ReplayProgress{Done: true}
			}
			fr.Ready.Holds = map[string]*runner.ReadyHolds{"s": h}
		}
		select {
		case out <- fr:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return <-done
}

// Holds run on workers: the coordinator gives every worker the holds the
// attempt starts with, and owns what each stream holds. A column one worker
// holds is held on all, and the record keeps it once, with the schema the
// destination gets, and every worker's held cells and files. Released, the
// hold ends once the first worker has patched it in.
func TestHoldsOnWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	p := newPool(t, ranges(4, 20, 5))
	p.dest = func(coordinator bool) destination.Destination {
		return holdingStagingDest{stagingDest{s: p.store, t: t, coordinator: coordinator}}
	}
	p.change = func(j *runner.Job) {
		j.HeldURL = "file:///held"
		j.Streams[0].PrimaryKey, j.Streams[0].Holds = []string{"id"}, &runner.StreamHolds{}
		j.Streams[0].Partitioning = plan.SourcePartitioning([]string{"id"}, true, false)
	}
	flow := holdingFlow{mu: &sync.Mutex{}, plans: map[int]*runner.HoldPlan{}, heard: map[int]bool{}}
	p.flow = flow
	shares := 0
	j := p.workers("a1", 2)
	j.OnCheckpoint = func(_ *checkpoint.Record, _, _ map[string]uint64) { shares += 2 }
	if _, err := runner.RunAttempt(ctx, j); err != nil {
		t.Fatal(err)
	}
	for i, err := range p.wait("a1") {
		if err != nil {
			t.Errorf("worker %d: %v", i, err)
		}
	}
	p.store.check(t, 80)
	if len(flow.plans) != 2 || len(flow.heard) != 2 {
		t.Fatalf("hold plans %v, heard x held %v, of 2 workers", flow.plans, flow.heard)
	}
	head := p.head()
	h, st := head.Streams["s"].Holds, head.Streams["s"]
	switch {
	case h == nil || len(h.Columns) != 1 || h.Columns[0].Column != "x" || !slices.Equal(h.Columns[0].Delivered, heldX.Delivered):
		t.Fatalf("holds %+v", h)
	case !slices.Equal(st.Schema, xSchema(arrow.BinaryTypes.String)) || !slices.Equal(st.Delivered, xSchema(arrow.PrimitiveTypes.Int64)):
		t.Fatal("the record's schemas aren't the owner's")
	case h.Cells["y"].Cells != uint64(shares) || len(h.Epochs) == 0:
		t.Fatalf("held cells %+v in epochs %v, want %d", h.Cells, h.Epochs, shares)
	}

	// Released: every worker gets the replay, and the hold ends.
	p.source = ranges(4, 20, 5)
	clear(flow.plans)
	p.change = func(j *runner.Job) {
		j.HeldURL = "file:///held"
		j.Streams[0].PrimaryKey, j.Streams[0].Holds = []string{"id"}, &runner.StreamHolds{}
		j.Streams[0].Partitioning = plan.SourcePartitioning([]string{"id"}, true, false)
		j.Releases = []runner.Release{{Stream: "s", Column: "x", Since: h.Columns[0].Since}}
	}
	if _, err := runner.RunAttempt(ctx, p.workers("a2", 2)); err != nil {
		t.Fatal(err)
	}
	p.wait("a2")
	for lane, hp := range flow.plans {
		if hp.Replay == nil || slices.ContainsFunc(hp.Holding, func(c checkpoint.ColumnHold) bool { return c.Column == "x" }) {
			t.Errorf("the worker of lane %d starts with %+v", lane, hp)
		}
	}
	if h := p.head().Streams["s"].Holds; h != nil && (len(h.Columns) > 0 || h.Replay != nil) {
		t.Errorf("the hold didn't end: %+v", h)
	}
}

// changeLog is a source's one log of changes for every stream it reads
// with CDC, as a Postgres replication slot is: a read holds it until it
// ends, and another read of it meanwhile fails. Stream a's rows are ids 1
// to rows, b's the next rows.
type changeLog struct {
	rows int
	mu   sync.Mutex
	held bool
}

type changeReader struct{ log *changeLog }

func (r changeReader) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	l := r.log
	l.mu.Lock()
	if l.held {
		l.mu.Unlock()
		return errs.New(errs.Transient, "the change log is held by another read")
	}
	l.held = true
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		l.held = false
		l.mu.Unlock()
	}()
	for _, cs := range req.Streams {
		if cs.State != nil {
			continue
		}
		first := 1
		if cs.Name == "b" {
			first += l.rows
		}
		rows := make([]json.RawMessage, l.rows)
		for i := range rows {
			rows[i] = json.RawMessage(fmt.Sprintf(`{"id":%d}`, first+i))
		}
		if err := out.Rows(cs.Name, rows); err != nil {
			return err
		}
		if err := out.ResumePoint(cs.Name, []byte("read")); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

// Every stream a job reads with CDC is read on one worker, since the source
// reads their changes from one log, which one read holds: two streams'
// changes read on two workers would fight over it.
func TestStreamsReadWithCDCShareOneWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	p := newPool(t, ranges(1, 1, 1))
	log := &changeLog{rows: 20}
	p.reader = func() source.Reader { return changeReader{log} }
	p.change = func(j *runner.Job) {
		j.Streams = []runner.Stream{
			{Name: "a", Replay: source.Exact, Read: source.CDC, Mode: destination.Append},
			{Name: "b", Replay: source.Exact, Read: source.CDC, Mode: destination.Append},
		}
	}
	j := p.workers("a1", 2)
	drainsHolding(&j, p.store, 40)
	if _, err := runner.RunAttempt(ctx, j); !errors.Is(err, runner.ErrDrained) {
		t.Fatalf("the attempt: %v", err)
	}
	p.wait("a1")
	p.store.check(t, 40)
}

// Workers without a server to join or a way to launch them are refused
// before the attempt claims the pipeline.
func TestWorkersNeedAServerAndALaunch(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	for name, w := range map[string]*runner.Workers{
		"no launch": {Count: 2, Server: listen(t), Options: workerOpts},
		"no server": {Count: 2, Options: workerOpts, Launch: func(context.Context, []attempt.Target) error { return nil }},
	} {
		store := checkpoint.NewMemStore()
		p := newPool(t, ranges(2, 1, 1))
		j := p.job("a1", noSource{t}, stagingDest{s: p.store, t: t, coordinator: true})
		j.Log, j.Workers = checkpoint.NewLog(store, "org", "p"), w
		if _, err := runner.RunAttempt(ctx, j); errs.CodeOf(err) != errs.ConfigInvalid {
			t.Fatalf("%s: %v", name, err)
		}
		if keys, err := store.List(ctx, ""); err != nil || len(keys) > 0 {
			t.Fatalf("%s: the attempt wrote %v (%v)", name, keys, err)
		}
	}
}

// A Flink job given workers is refused before its own path starts it.
func TestAFlinkJobIsRefusedWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	bridge := flinktest.NewBridge()
	launched := false
	_, err := runner.RunAttempt(ctx, runner.Job{
		Owner: "a1", Pipeline: "p", Continuous: true, Log: checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"),
		Source:      countingSource{streams: []string{"s"}, rows: 1, page: 1},
		Streams:     []runner.Stream{{Name: "s", Replay: source.Exact, Read: source.Incremental, Mode: destination.Append, Schema: idSchema}},
		Outputs:     []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append, Transformed: true}},
		Destination: stagingDest{s: newLaneStore(), t: t},
		Dataflow:    runner.Passthrough{Convert: idRows},
		Settings:    delivery.Settings{Require: delivery.ExactlyOnce, Scope: destination.ScopePipeline, CommitInterval: 10 * time.Millisecond},
		Flink:       flinktest.NewCluster(bridge),
		Bridge:      bridge,
		Workers: &runner.Workers{Count: 2, Server: listen(t), Options: workerOpts, Launch: func(context.Context, []attempt.Target) error {
			launched = true
			return nil
		}},
	})
	if errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "Spark or Flink") || launched {
		t.Fatalf("a Flink job on workers: %v, launched %v", err, launched)
	}
}

// meshingPassthrough is Passthrough as a dataflow whose engines exchange
// rows between workers.
type meshingPassthrough struct{ runner.Passthrough }

func (meshingPassthrough) Meshes() bool { return true }

// What only one pod runs is refused workers; a SQL transform, into a
// destination whose lanes commit on their own too, and holds aren't, but
// into such a destination. A group window takes its rows from every worker, so it runs on workers
// whose engines exchange rows between them, and on one pod otherwise.
func TestWhatRunsOnOnePod(t *testing.T) {
	p := newPool(t, ranges(1, 1, 1))
	for name, c := range map[string]struct {
		workers int
		change  func(*runner.Job)
		onePod  bool
	}{
		"more workers than writer lanes": {17, func(*runner.Job) {}, true},
		"one writer lane":                {2, func(j *runner.Job) { j.Destination = &countingDest{} }, true},
		"a SQL transform": {2, func(j *runner.Job) {
			j.SQL = &runner.SQLTransform{}
			j.Outputs = j.Streams
		}, false},
		"a SQL transform into lane commits": {2, func(j *runner.Job) {
			j.SQL = &runner.SQLTransform{}
			j.Outputs = j.Streams
			j.Destination = txnDest{s: newTxnStore(t)}
		}, false},
		"a group window into lane commits": {2, func(j *runner.Job) {
			j.SQL = &runner.SQLTransform{Operators: []string{"window-1"}}
			j.Outputs = j.Streams
			j.Destination = txnDest{s: newTxnStore(t)}
			j.Dataflow = meshingPassthrough{}
		}, false},
		"a group window": {2, func(j *runner.Job) {
			j.SQL = &runner.SQLTransform{Operators: []string{"window-1"}}
			j.Outputs = j.Streams
			j.Dataflow = meshingPassthrough{}
		}, false},
		"a group window on engines that don't exchange rows": {2, func(j *runner.Job) {
			j.SQL = &runner.SQLTransform{Operators: []string{"window-1"}}
			j.Outputs = j.Streams
		}, true},
		"a group window on one worker": {1, func(j *runner.Job) {
			j.SQL = &runner.SQLTransform{Operators: []string{"window-1"}}
			j.Outputs = j.Streams
		}, false},
		"holds": {2, func(j *runner.Job) {
			j.Destination = holdingStagingDest{stagingDest{s: p.store, t: t}}
			j.HeldURL = "file:///held"
			j.Streams[0].PrimaryKey, j.Streams[0].Holds = []string{"id"}, &runner.StreamHolds{Cells: true}
		}, false},
		// A redo of a lost writer lane's part of an epoch doesn't hold what
		// the epoch held.
		"holds into lane commits": {2, func(j *runner.Job) {
			j.Destination = holdingTxnDest{txnDest{s: newTxnStore(t)}}
			j.HeldURL = "file:///held"
			j.Streams[0].PrimaryKey, j.Streams[0].Holds = []string{"id"}, &runner.StreamHolds{}
		}, true},
		"lane commits that could hold, of a stream that doesn't": {2, func(j *runner.Job) {
			j.Destination = holdingTxnDest{txnDest{s: newTxnStore(t)}}
			j.HeldURL = "file:///held"
		}, false},
	} {
		j := p.job("a1", p.source(), stagingDest{s: p.store, t: t})
		c.change(&j)
		if err := j.OnWorkers(c.workers); (err != nil) != c.onePod {
			t.Errorf("%s on %d workers: %v", name, c.workers, err)
		}
	}
	j := p.job("a1", p.source(), stagingDest{s: p.store, t: t})
	if err := j.OnWorkers(16); err != nil {
		t.Error(err)
	}
}

// holdingTxnDest is txnDest taking holds.
type holdingTxnDest struct{ txnDest }

func (d holdingTxnDest) Capabilities() destination.Capabilities {
	c := d.txnDest.Capabilities()
	c.WriteModes = []destination.WriteMode{destination.Append, destination.Merge}
	c.Holds = destination.Holds{ByID: true, ByKey: true, Guarded: true}
	return c
}

// holdingStagingDest is stagingDest taking holds.
type holdingStagingDest struct{ stagingDest }

func (d holdingStagingDest) Capabilities() destination.Capabilities {
	c := d.stagingDest.Capabilities()
	c.WriteModes = []destination.WriteMode{destination.Append, destination.Merge}
	c.Holds = destination.Holds{ByID: true, ByKey: true, Guarded: true}
	return c
}
