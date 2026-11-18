package engineclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	partition "github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/runner/attempt"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// The attempt protocol's timings in these tests.
var workerTimings = attempt.Options{Heartbeat: 100 * time.Millisecond, Timeout: 10 * time.Second}

// splitSource reads stream "s" in splits "0" to splits-1, a chunk of rows
// at a time with a resume point after each: split k's chunk c holds ids
// k*10000+c*100+r for r below rows, each as row makes it. Its state is the
// next chunk. With upTo, a split has only that many chunks so far, and a
// read that has read them stays open until it's cancelled, every split
// read up to there.
type splitSource struct {
	splits, chunks, rows int
	upTo                 int
	row                  func(split, chunk int, id int64) string
	// Called before each chunk of a split but its first; nil: nothing.
	before func(ctx context.Context, split, chunk int) error
	// Splits that have no chunks yet, whatever upTo says.
	held map[int]bool
}

func (s *splitSource) Splits(context.Context, source.ConfiguredStream, []source.Split) ([]source.Split, error) {
	out := make([]source.Split, s.splits)
	for i := range out {
		out[i] = source.Split{ID: strconv.Itoa(i)}
	}
	return out, nil
}

func (s *splitSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	var open atomic.Bool
	err := source.ReadSplits(ctx, req, out, func(ctx context.Context, _ source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		split, err := strconv.Atoi(sp.ID)
		if err != nil {
			return err
		}
		from := 0
		if sp.State != nil {
			if from, err = strconv.Atoi(string(sp.State)); err != nil {
				return err
			}
		}
		upTo := s.chunks
		if s.upTo > 0 {
			upTo = min(s.upTo, s.chunks)
		}
		if s.held[split] {
			upTo = 0
		}
		for c := from; c < upTo; c++ {
			if c > 0 && s.before != nil {
				if err := s.before(ctx, split, c); err != nil {
					return err
				}
			}
			rows := make([]json.RawMessage, s.rows)
			for r := range rows {
				rows[r] = json.RawMessage(s.row(split, c, int64(split*10000+c*100+r)))
			}
			if err := e.Rows(rows); err != nil {
				return err
			}
			if err := e.ResumePoint([]byte(strconv.Itoa(c + 1))); err != nil {
				return err
			}
		}
		if upTo < s.chunks {
			// Its lane goes to a split waiting for one: ReadSplits reads only
			// as many at a time as the read has lanes.
			open.Store(true)
			return nil
		}
		return e.Complete([]byte(strconv.Itoa(s.chunks)))
	})
	if err != nil || !open.Load() {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

// keyDest is a stage-and-apply destination of several writer lanes that
// takes holds and patches. Each lane stages its part of an epoch, its rows
// and patches, and a commit applies every lane's staging once, with a
// marker, behind a fence.
type keyDest struct {
	lanes int

	mu      sync.Mutex
	fence   uint64
	writing map[stageAt][]destRow
	staged  map[stageAt][]destRow
	marked  map[destination.Epoch]bool
	rows    []destRow
	patched []destRow
	// The type each column was delivered at, by stream and column.
	types map[string]map[string][]string
	// Closed and replaced at every commit.
	committed chan struct{}
}

type stageAt struct {
	E    destination.Epoch
	Lane int
}

// destRow is a row as the destination got it: its stream, whether it's a
// patch, and its columns' values but the metadata columns', with
// _openetl_held's.
type destRow struct {
	stream string
	patch  bool
	cols   map[string]string
}

func (r destRow) id() int64 {
	id, _ := strconv.ParseInt(r.cols["id"], 10, 64)
	return id
}

func newKeyDest(lanes int) *keyDest {
	return &keyDest{lanes: lanes, writing: map[stageAt][]destRow{}, staged: map[stageAt][]destRow{}, marked: map[destination.Epoch]bool{},
		types: map[string]map[string][]string{}, committed: make(chan struct{})}
}

func (d *keyDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{
		Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopePipeline}, WriterLanes: d.lanes,
		WriteModes: []destination.WriteMode{destination.Append, destination.Merge, destination.Overwrite},
		Holds:      destination.Holds{ByID: true, ByKey: true, Guarded: true},
	}
}

func (d *keyDest) Open(_ context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	return &keyWriter{d: d, lane: cfg.Lane}, nil
}

// waitFor waits until ok says yes of what the destination applied.
func (d *keyDest) waitFor(ctx context.Context, ok func(rows []destRow) bool) error {
	for {
		d.mu.Lock()
		done, ch := ok(d.rows), d.committed
		d.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// applied are the rows of stream applied, but patches.
func (d *keyDest) applied(stream string) []destRow {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []destRow
	for _, r := range d.rows {
		if r.stream == stream && !r.patch {
			out = append(out, r)
		}
	}
	return out
}

type keyWriter struct {
	d    *keyDest
	lane int
}

var _ destination.Patcher = (*keyWriter)(nil)

func rowsOf(stream string, patch bool, b arrow.RecordBatch) []destRow {
	out := make([]destRow, b.NumRows())
	for i := range out {
		out[i] = destRow{stream: stream, patch: patch, cols: map[string]string{}}
	}
	for c, f := range b.Schema().Fields() {
		if strings.HasPrefix(f.Name, "_openetl_") && f.Name != "_openetl_held" {
			continue
		}
		col := b.Column(c)
		for i := range out {
			v := "null"
			if col.IsValid(i) {
				v = col.ValueStr(i)
			}
			out[i].cols[f.Name] = v
		}
	}
	return out
}

func (w *keyWriter) stage(e destination.Epoch, rows []destRow) {
	w.d.mu.Lock()
	defer w.d.mu.Unlock()
	k := stageAt{e, w.lane}
	w.d.writing[k] = append(w.d.writing[k], rows...)
}

func (w *keyWriter) Write(_ context.Context, e destination.Epoch, stream string, b arrow.RecordBatch) error {
	w.d.mu.Lock()
	if w.d.types[stream] == nil {
		w.d.types[stream] = map[string][]string{}
	}
	for _, f := range b.Schema().Fields() {
		if t := f.Type.String(); !slices.Contains(w.d.types[stream][f.Name], t) {
			w.d.types[stream][f.Name] = append(w.d.types[stream][f.Name], t)
		}
	}
	w.d.mu.Unlock()
	w.stage(e, rowsOf(stream, false, b))
	return nil
}

func (w *keyWriter) Patch(_ context.Context, e destination.Epoch, stream string, b arrow.RecordBatch) error {
	w.stage(e, rowsOf(stream, true, b))
	return nil
}

func (w *keyWriter) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	w.d.mu.Lock()
	defer w.d.mu.Unlock()
	if e.Generation < w.d.fence {
		return nil, destination.ErrFenced
	}
	k := stageAt{e, w.lane}
	w.d.staged[k] = w.d.writing[k]
	delete(w.d.writing, k)
	return json.Marshal(k)
}

// apply makes e's staging visible once; d.mu is held.
func (d *keyDest) apply(e destination.Epoch, staged []destination.Staged) error {
	if d.marked[e] {
		return nil
	}
	var rows []destRow
	for _, c := range staged {
		var k stageAt
		if err := json.Unmarshal(c, &k); err != nil {
			return err
		}
		got, ok := d.staged[k]
		if !ok {
			return fmt.Errorf("lane %d's staging of epoch %d is gone", k.Lane, k.E.N)
		}
		rows = append(rows, got...)
	}
	for _, r := range rows {
		if r.patch {
			d.patched = append(d.patched, r)
		}
	}
	d.rows = append(d.rows, rows...)
	d.marked[e] = true
	close(d.committed)
	d.committed = make(chan struct{})
	return nil
}

func (w *keyWriter) Commit(_ context.Context, e destination.Epoch, staged []destination.Staged) (destination.Committed, error) {
	w.d.mu.Lock()
	defer w.d.mu.Unlock()
	if e.Generation < w.d.fence {
		return destination.Committed{}, destination.ErrFenced
	}
	return destination.Committed{}, w.d.apply(e, staged)
}

func (w *keyWriter) Recover(_ context.Context, gen uint64, e destination.Epoch, staged []destination.Staged) (destination.Recovery, error) {
	w.d.mu.Lock()
	defer w.d.mu.Unlock()
	w.d.fence = max(w.d.fence, gen)
	switch {
	case e.IsZero() || w.d.marked[e]:
		return destination.Applied, nil
	case w.d.apply(e, staged) == nil:
		return destination.Redone, nil
	}
	return destination.Lost, nil
}

func (w *keyWriter) Close(context.Context) error { return nil }

// inProcess runs an attempt's workers in this process, each a runner with
// an engine of its own running mk's job.
type inProcess struct {
	ctx context.Context
	mk  func() runner.Job

	mu      sync.Mutex
	cancels []context.CancelFunc
	errs    []error
	ended   sync.WaitGroup
}

// on makes j run on n workers, which join srv.
func (p *inProcess) on(j *runner.Job, srv *attempt.Server, n int) {
	j.Workers = &runner.Workers{Count: n, Server: srv, Options: workerTimings, Launch: func(_ context.Context, targets []attempt.Target) error {
		for _, tg := range targets {
			wctx, cancel := context.WithCancel(p.ctx)
			wj := p.mk()
			wj.Log = nil
			p.mu.Lock()
			p.cancels = append(p.cancels, cancel)
			p.mu.Unlock()
			p.ended.Go(func() {
				defer cancel()
				err := runner.RunWorker(wctx, wj, tg, workerTimings)
				p.mu.Lock()
				p.errs = append(p.errs, err)
				p.mu.Unlock()
			})
		}
		return nil
	}}
}

// kill stops the i-th worker launched.
func (p *inProcess) kill(i int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cancels[i]()
}

// wait waits for every worker launched to end, and says how they did.
func (p *inProcess) wait() []error {
	p.ended.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	errs := p.errs
	p.errs, p.cancels = nil, nil
	return errs
}

func listen(t *testing.T) *attempt.Server {
	t.Helper()
	srv, err := attempt.Listen(t.Context(), attempt.ListenConfig{Address: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)
	return srv
}

// drainOnceRecorded has j stop at the checkpoint after the first committed
// record that ok says yes of.
func drainOnceRecorded(j *runner.Job, ok func(*checkpoint.Record) bool) {
	ch := make(chan struct{})
	var once sync.Once
	j.Drain = ch
	j.OnCheckpoint = func(rec *checkpoint.Record, _, _ map[string]uint64) {
		if ok(rec) {
			once.Do(func() { close(ch) })
		}
	}
}

func fieldNames(raw []byte) ([]string, error) {
	s, err := arrowipc.DecodeSchema(raw)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, f := range s.Fields() {
		if !strings.HasPrefix(f.Name, "_openetl_") {
			out = append(out, f.Name+":"+f.Type.String())
		}
	}
	return out, nil
}

const kept = "INSERT INTO kept SELECT *, id % 7 AS r FROM s WHERE id % 2 = 0"

// A SQL transform runs on workers, each with an engine of its own: a worker
// lost mid-epoch fails the attempt, and the next, on other workers, then on
// one runner, restores from the record and lands every row the statement
// keeps once. A column only one worker's rows have reaches the statement's
// output, and the record holds it in the stream's schema and the output's,
// as the coordinator owns both.
func TestSQLOnWorkersThroughTheEngine(t *testing.T) {
	opts := options(t, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	declared := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	raw, err := arrowipc.EncodeSchema(declared)
	if err != nil {
		t.Fatal(err)
	}
	a, err := Plan(ctx, planEngine(t), sqlplan.Request{SQL: kept, Streams: []sqlplan.Stream{{Name: "s", DeclaredSchema: raw}}})
	if err != nil || len(a.Errors) != 0 {
		t.Fatal(a.Errors, err)
	}
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
	dest := newKeyDest(16)
	src := func(upTo int, held map[int]bool) *splitSource {
		return &splitSource{splits: 6, chunks: 8, rows: 20, upTo: upTo, held: held, row: func(split, _ int, id int64) string {
			if split == 5 {
				return fmt.Sprintf(`{"id": %d, "extra": "e%d"}`, id, id)
			}
			return fmt.Sprintf(`{"id": %d}`, id)
		}}
	}
	job := func(owner string, upTo int, held map[int]bool) func() runner.Job {
		return func() runner.Job {
			return runner.Job{
				Pipeline: "p", Owner: owner, Sync: "s1", Log: log, Source: src(upTo, held),
				Streams:     []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append, Schema: declared}},
				Outputs:     []runner.Stream{{Name: "kept", Replay: source.Exact, Mode: destination.Append, Transformed: true, From: "s"}},
				SQL:         &runner.SQLTransform{SQL: kept, Expected: expected(t, a)},
				Destination: dest, Dataflow: &Dataflow{Options: opts}, Lanes: 2, Logger: opts.Logger.With("job", owner),
				Settings: delivery.Settings{Require: delivery.AtLeastOnce, CommitMaxBytes: 4 << 10, CommitInterval: 20 * time.Millisecond, Scope: destination.ScopePipeline},
			}
		}
	}
	srv := listen(t)

	// Three workers, one of which is lost after the first commit, with some
	// of the rows to read, and none yet of the split with extra.
	p := &inProcess{ctx: ctx, mk: job("a1", 3, map[int]bool{5: true})}
	j := p.mk()
	p.on(&j, srv, 3)
	var once sync.Once
	j.OnCommit = func(uint64) { once.Do(func() { p.kill(1) }) }
	if _, err := runner.RunAttempt(ctx, j); err == nil {
		t.Fatal("the attempt went on without its worker")
	}
	p.wait()
	sawExtra := func(raw []byte) bool {
		names, err := fieldNames(raw)
		return err == nil && slices.Contains(names, "extra:utf8")
	}
	if head, err := log.Latest(ctx); err != nil || head == nil || sawExtra(head.Streams["s"].Schema) {
		t.Fatalf("after three workers, the record is %+v (%v), and should have no extra", head, err)
	}

	// Two workers, with more to read, drained once a record holds what
	// worker 1, which reads the split with extra, saw; then one runner, to
	// the end.
	p.mk = job("a2", 6, nil)
	j = p.mk()
	p.on(&j, srv, 2)
	drainOnceRecorded(&j, func(rec *checkpoint.Record) bool {
		return sawExtra(rec.Streams["s"].Schema) && sawExtra(rec.Outputs["kept"].Schema)
	})
	res, err := runner.RunAttempt(ctx, j)
	if !errors.Is(err, runner.ErrDrained) {
		t.Fatalf("two workers: %v", err)
	}
	for i, err := range p.wait() {
		if err != nil {
			t.Errorf("worker %d of two: %v", i, err)
		}
	}
	if res.Restored == 0 || res.Committed == 0 {
		t.Fatalf("two workers restored %d and committed %d", res.Restored, res.Committed)
	}
	// Worker 1 reads the split with extra, and the coordinator records what
	// it saw.
	head, err := log.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	in, err := fieldNames(head.Streams["s"].Schema)
	if err != nil {
		t.Fatal(err)
	}
	out, err := fieldNames(head.Outputs["kept"].Schema)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(in, "extra:utf8") || !slices.Contains(out, "extra:utf8") || !slices.Contains(out, "r:int64") {
		t.Errorf("the record's schemas: stream %v, output %v", in, out)
	}
	last, err := runner.RunAttempt(ctx, job("a3", 0, nil)())
	if err != nil {
		t.Fatal(err)
	}
	if last.Restored != res.Last {
		t.Errorf("one runner restored %d, after %d", last.Restored, res.Last)
	}

	got := map[int64]destRow{}
	for _, r := range dest.applied("kept") {
		if _, ok := got[r.id()]; ok {
			t.Fatalf("row %d landed twice", r.id())
		}
		got[r.id()] = r
	}
	want := 0
	for k := range 6 {
		for c := range 8 {
			for i := range 20 {
				id := int64(k*10000 + c*100 + i)
				if id%2 != 0 {
					continue
				}
				want++
				r, ok := got[id]
				switch {
				case !ok:
					t.Fatalf("row %d is missing", id)
				case r.cols["r"] != strconv.FormatInt(id%7, 10):
					t.Fatalf("row %d: %v", id, r.cols)
				case k == 5 && r.cols["extra"] != fmt.Sprintf("e%d", id):
					t.Fatalf("row %d of the split with extra: %v", id, r.cols)
				}
			}
		}
	}
	if len(got) != want {
		t.Fatalf("%d rows landed, want %d", len(got), want)
	}
}

// The value column x of the stream's rows: an integer, but text in split
// 1's chunk 1.
func heldRow(split, chunk int, id int64) string {
	if split == 1 && chunk == 1 {
		return fmt.Sprintf(`{"id": %d, "x": "t%d"}`, id, id)
	}
	return fmt.Sprintf(`{"id": %d, "x": %d}`, id, id)
}

// Holds on workers, each with an engine of its own: the coordinator owns
// what each stream holds. x changes type in one worker's rows, under a
// schema policy that holds it, and is held on every worker: no worker
// delivers it at its new type, and rows every worker reads after the hold
// hold it. The record holds it once, with the type the destination has.
// Released, on other workers, its held values are patched in once each, by
// worker 0 alone, from the files every worker wrote.
func TestHoldsOnWorkersThroughTheEngine(t *testing.T) {
	opts := options(t, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	root := t.TempDir()
	store, err := checkpoint.NewFSStore(root)
	if err != nil {
		t.Fatal(err)
	}
	log := checkpoint.NewLog(store, "org", "p")
	dest := newKeyDest(16)
	heldURL := "file://" + filepath.Join(root, "org", "p", checkpoint.HeldDir)
	// Chunk 1 waits for the first commit, which sets the schema the policy
	// guards; chunk 2 for the hold, so every worker's chunk 2 comes after.
	held := func(rows []destRow) bool {
		return slices.ContainsFunc(rows, func(r destRow) bool { return strings.Contains(r.cols["_openetl_held"], "x") })
	}
	src := &splitSource{splits: 4, chunks: 3, rows: 10, row: heldRow, before: func(ctx context.Context, _, chunk int) error {
		if chunk == 1 {
			return dest.waitFor(ctx, func(rows []destRow) bool { return len(rows) > 0 })
		}
		return dest.waitFor(ctx, held)
	}}
	job := func(owner string) func() runner.Job {
		return func() runner.Job {
			return runner.Job{
				Pipeline: "p", Owner: owner, Log: log, Source: src, HeldURL: heldURL, SchemaPolicy: runner.Block,
				Streams: []runner.Stream{{
					Name: "s", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"id"}, Partitioning: partition.SourcePartitioning([]string{"id"}, true, false),
					Holds: &runner.StreamHolds{},
				}},
				Destination: dest, Dataflow: &Dataflow{Options: opts}, Lanes: 2,
				Settings: delivery.Settings{Require: delivery.AtLeastOnce, CommitMaxBytes: 1, Scope: destination.ScopePipeline},
			}
		}
	}
	srv := listen(t)
	p := &inProcess{ctx: ctx, mk: job("a1")}
	j := p.mk()
	p.on(&j, srv, 2)
	if _, err := runner.RunAttempt(ctx, j); err != nil {
		t.Fatal(err)
	}
	for i, err := range p.wait() {
		if err != nil {
			t.Errorf("worker %d: %v", i, err)
		}
	}
	if types := dest.types["s"]["x"]; !slices.Equal(types, []string{"int64"}) {
		t.Errorf("x was delivered as %v", types)
	}
	heldIDs, after := map[int64]bool{}, map[int]int{}
	for _, r := range dest.applied("s") {
		id := r.id()
		chunk, split := int(id%10000)/100, int(id/10000)
		isHeld := strings.Contains(r.cols["_openetl_held"], "x")
		switch {
		case chunk == 2 && (!isHeld || r.cols["x"] != "null"):
			t.Errorf("worker of split %d delivered row %d after the hold: %v", split, id, r.cols)
		case split == 1 && chunk == 1 && !isHeld:
			t.Errorf("row %d, whose x changed type, isn't held: %v", id, r.cols)
		}
		if isHeld {
			heldIDs[id] = true
		}
		if chunk == 2 {
			after[split]++
		}
	}
	if len(after) != 4 || len(heldIDs) < 50 {
		t.Fatalf("rows after the hold, by split: %v; %d rows held", after, len(heldIDs))
	}
	head, err := log.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h := head.Streams["s"].Holds
	if h == nil || len(h.Columns) != 1 || h.Columns[0].Column != "x" || len(h.Epochs) == 0 {
		t.Fatalf("holds = %+v", h)
	}
	kept, err := fieldNames(h.Columns[0].Delivered)
	if err != nil || !slices.Equal(kept, []string{"x:int64"}) {
		t.Fatalf("x is held at %v (%v), want int64", kept, err)
	}
	stream, err := fieldNames(head.Streams["s"].Schema)
	if err != nil || !slices.Contains(stream, "x:utf8") {
		t.Fatalf("the stream's schema: %v (%v)", stream, err)
	}
	delivered, err := fieldNames(head.Streams["s"].Delivered)
	if err != nil || !slices.Contains(delivered, "x:int64") {
		t.Fatalf("the schema the destination gets: %v (%v)", delivered, err)
	}

	// Released, the held values are patched in by worker 0, once each, from
	// both workers' files; three workers run it.
	p.mk = func() runner.Job {
		j := job("a2")()
		j.Source = &splitSource{splits: 4, chunks: 3, rows: 10, row: heldRow}
		j.Releases = []runner.Release{{Stream: "s", Column: "x", Since: h.Columns[0].Since}}
		return j
	}
	j = p.mk()
	p.on(&j, srv, 3)
	if _, err := runner.RunAttempt(ctx, j); err != nil {
		t.Fatal(err)
	}
	for i, err := range p.wait() {
		if err != nil {
			t.Errorf("worker %d of the release: %v", i, err)
		}
	}
	dest.mu.Lock()
	patched := map[int64]int{}
	for _, r := range dest.patched {
		patched[r.id()]++
		if want := "t" + strconv.FormatInt(r.id(), 10); r.id()/10000 == 1 && int(r.id()%10000)/100 == 1 && r.cols["x"] != want {
			t.Errorf("patch of row %d: %v", r.id(), r.cols)
		}
	}
	dest.mu.Unlock()
	if !maps.Equal(patched, func() map[int64]int {
		m := map[int64]int{}
		for id := range heldIDs {
			m[id] = 1
		}
		return m
	}()) {
		t.Errorf("patched %v, want each of the %d held rows once", patched, len(heldIDs))
	}
	head, err = log.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h := head.Streams["s"].Holds; h != nil && len(h.Columns) > 0 {
		t.Errorf("the hold didn't end: %+v", h)
	}
}
