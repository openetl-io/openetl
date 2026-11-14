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
	// What the worker acknowledged, and read.
	mu    sync.Mutex
	acked map[string]string
	read  []string
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
		s.mu.Unlock()
		next, end := i*s.rows+1, (i+1)*s.rows
		if sp.State != nil {
			last, err := strconv.Atoi(string(sp.State))
			if err != nil {
				return err
			}
			next = last + 1
		}
		for first := true; next <= end; first = false {
			var rows []json.RawMessage
			for ; len(rows) < s.page && next <= end; next++ {
				rows = append(rows, json.RawMessage(fmt.Sprintf(`{"id":%d}`, next)))
			}
			if err := e.Rows(rows); err != nil {
				return err
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

func (w *stagingWriter) Recover(_ context.Context, gen uint64, e destination.Epoch, staged []destination.Staged) (destination.Recovery, error) {
	s := w.s
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
	// Makes each attempt's workers' sources.
	source func() *rangeSource
	// The workers' dataflow; nil: Passthrough.
	flow runner.Dataflow
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
	return runner.Job{
		Owner: owner, Sync: "sync", Pipeline: "p", Log: p.log, Source: src,
		Streams:     []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append}},
		Destination: dest,
		Dataflow:    runner.Passthrough{Convert: idRows},
		Settings:    delivery.Settings{Require: delivery.ExactlyOnce, CommitMaxBytes: 1, Scope: destination.ScopePipeline},
		Lanes:       2,
	}
}

// workers is the pipeline's job for attempt owner on n workers, which
// launch in this process.
func (p *pool) workers(owner string, n int) runner.Job {
	j := p.job(owner, noSource{p.t}, stagingDest{s: p.store, t: p.t, coordinator: true})
	j.Workers = &runner.Workers{Count: n, Server: p.srv, Options: workerOpts, Launch: func(ctx context.Context, targets []attempt.Target) error {
		done := make(chan error, len(targets))
		p.mu.Lock()
		p.done[owner] = done
		for _, tg := range targets {
			src := p.source()
			wctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
			p.sources[owner] = append(p.sources[owner], src)
			p.cancels[owner] = append(p.cancels[owner], cancel)
			wj := p.job(owner, src, stagingDest{s: p.store, t: p.t})
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
	p := newPool(t, ranges(6, 20, 5))
	j := p.workers("a1", 3)
	// Each epoch's rows, by split, and the splits its committables hold.
	rows, held := map[string]uint64{}, map[string]bool{}
	var checkpoints []*checkpoint.Record
	j.OnCheckpoint = func(rec *checkpoint.Record, _ map[string]uint64) {
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
		if err := o.Propose("s", base, mine); err != nil {
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

// What only one pod runs is refused workers.
func TestWhatRunsOnOnePod(t *testing.T) {
	p := newPool(t, ranges(1, 1, 1))
	for name, c := range map[string]struct {
		workers int
		change  func(*runner.Job)
	}{
		"more workers than writer lanes": {17, func(*runner.Job) {}},
		"one writer lane":                {2, func(j *runner.Job) { j.Destination = &countingDest{} }},
		"a SQL transform": {2, func(j *runner.Job) {
			j.SQL = &runner.SQLTransform{}
			j.Outputs = j.Streams
		}},
	} {
		j := p.job("a1", p.source(), stagingDest{s: p.store, t: t})
		c.change(&j)
		if err := j.OnWorkers(c.workers); err == nil {
			t.Errorf("%s: runs on %d workers", name, c.workers)
		}
	}
	j := p.job("a1", p.source(), stagingDest{s: p.store, t: t})
	if err := j.OnWorkers(16); err != nil {
		t.Error(err)
	}
}
