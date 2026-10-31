package engineclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	enginev1 "github.com/unsuitable001/openetl/gen/openetl/engine/v1"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/internal/enginesink"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// The engine's frame tests check the same bytes.
func TestFrameHeader(t *testing.T) {
	h := make([]byte, headerLen)
	putHeader(h, enginev1.FrameKind_FRAME_KIND_BATCH, 3, 5)
	if want := []byte{2, 0, 0, 0, 3, 0, 0, 0, 5, 0, 0, 0}; !bytes.Equal(h, want) {
		t.Fatalf("header = %v, want %v", h, want)
	}
}

func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	fw := newFrameWriter(&buf)
	if err := fw.write(enginev1.FrameKind_FRAME_KIND_ROWS, 7, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := fw.write(enginev1.FrameKind_FRAME_KIND_END_OF_STREAM, 7, nil); err != nil {
		t.Fatal(err)
	}
	if err := fw.flush(); err != nil {
		t.Fatal(err)
	}
	full := slices.Clone(buf.Bytes())

	fr := newFrameReader(&buf)
	kind, stream, payload, err := fr.next()
	if err != nil || kind != enginev1.FrameKind_FRAME_KIND_ROWS || stream != 7 || string(payload) != "abc" {
		t.Fatalf("first frame = %v %d %q %v", kind, stream, payload, err)
	}
	if kind, _, payload, err = fr.next(); err != nil || kind != enginev1.FrameKind_FRAME_KIND_END_OF_STREAM || len(payload) != 0 {
		t.Fatalf("second frame = %v %q %v", kind, payload, err)
	}
	if _, _, _, err = fr.next(); !errors.Is(err, io.EOF) {
		t.Fatalf("after the last frame: %v, want io.EOF", err)
	}

	for _, n := range []int{5, headerLen, 14} {
		_, _, _, err := newFrameReader(bytes.NewReader(full[:n])).next()
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("truncated to %d bytes: %v, want io.ErrUnexpectedEOF", n, err)
		}
	}
	bad := slices.Clone(full)
	bad[0] = 99
	if _, _, _, err := newFrameReader(bytes.NewReader(bad)).next(); err == nil {
		t.Error("unknown frame kind accepted")
	}
}

func engineBinary(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("OPENETL_ENGINE")
	if bin == "" {
		t.Skip("set OPENETL_ENGINE to an openetl-engine binary; make test does")
	}
	return bin
}

func options(t *testing.T, mem memory.Allocator) Options {
	return Options{
		Binary:    engineBinary(t),
		Logger:    slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelDebug})),
		Allocator: mem,
	}
}

func testContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func rows(js ...string) []json.RawMessage {
	out := make([]json.RawMessage, len(js))
	for i, j := range js {
		out[i] = json.RawMessage(j)
	}
	return out
}

func plan(streams ...string) runner.Plan {
	p := runner.Plan{Pipeline: "pipe", Owner: "a1", Generation: 3, FirstEpoch: 5}
	for _, s := range streams {
		p.Streams = append(p.Streams, runner.Stream{Name: s})
	}
	return p
}

// run feeds frames to a Dataflow and collects what comes out, per stream.
func run(ctx context.Context, df *Dataflow, p runner.Plan, frames ...runner.Frame) (map[string][]runner.Frame, error) {
	in := make(chan runner.Frame)
	out := make(chan runner.Frame)
	errc := make(chan error, 1)
	go func() { errc <- df.Run(ctx, p, in, out) }()
	go func() {
		defer close(in)
		for _, f := range frames {
			select {
			case in <- f:
			case <-ctx.Done():
				return
			}
		}
	}()
	got := map[string][]runner.Frame{}
	for f := range out {
		got[f.Stream] = append(got[f.Stream], f)
	}
	return got, <-errc
}

func release(frames map[string][]runner.Frame) {
	for _, fs := range frames {
		for _, f := range fs {
			if f.Batch != nil {
				f.Batch.Release()
			}
		}
	}
}

func extensionName(f arrow.Field) string {
	if ext, ok := f.Type.(arrow.ExtensionType); ok {
		return ext.ExtensionName()
	}
	name, _ := f.Metadata.GetValue("ARROW:extension:name")
	return name
}

func TestRunPassesFramesThrough(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	opts := options(t, mem)
	defer mem.AssertSize(t, 0)
	ctx := testContext(t)

	b := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{{Name: "k", Type: arrow.BinaryTypes.String, Nullable: true}}, nil))
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"x", "y"}, nil)
	batch := b.NewRecordBatch()
	b.Release()

	got, err := run(ctx, &Dataflow{Options: opts}, plan("a", "b"),
		runner.Frame{Kind: runner.FrameRows, Stream: "a", Rows: rows(`{"x": 1}`, `{"x": 2}`)},
		runner.Frame{Kind: runner.FrameBarrier, Stream: "a", Epoch: 5},
		runner.Frame{Kind: runner.FrameBatch, Stream: "b", Batch: batch},
		runner.Frame{Kind: runner.FrameRows, Stream: "a", Rows: rows(`{"x": 2.5, "o": {"k": [1]}}`)},
		runner.Frame{Kind: runner.FrameBarrier, Stream: "b", Epoch: 5},
	)
	defer release(got)
	if err != nil {
		t.Fatal(err)
	}

	a := got["a"]
	if len(a) != 3 || a[0].Kind != runner.FrameBatch || a[1].Kind != runner.FrameBarrier || a[1].Epoch != 5 || a[2].Kind != runner.FrameBatch {
		t.Fatalf("stream a: %+v", a)
	}
	if typ := a[0].Batch.Schema().Field(0).Type; !arrow.TypeEqual(typ, arrow.PrimitiveTypes.Int64) {
		t.Errorf("x in the first batch is %v, want int64", typ)
	}
	if n := a[0].Batch.NumRows(); n != 2 {
		t.Errorf("first batch has %d rows, want 2", n)
	}
	s := a[2].Batch.Schema()
	if typ := s.Field(0).Type; !arrow.TypeEqual(typ, arrow.PrimitiveTypes.Float64) {
		t.Errorf("x widened to %v, want float64", typ)
	}
	if name := extensionName(s.Field(1)); name != "arrow.json" {
		t.Errorf("o has extension %q, want arrow.json", name)
	}
	// A registered arrow.json type decodes as an extension array.
	col := a[2].Batch.Column(1)
	if ext, ok := col.(array.ExtensionArray); ok {
		col = ext.Storage()
	}
	if o := col.(*array.String).Value(0); o != `{"k":[1]}` {
		t.Errorf("o = %s", o)
	}

	bs := got["b"]
	if len(bs) != 2 || bs[0].Kind != runner.FrameBatch || bs[1].Kind != runner.FrameBarrier || bs[1].Epoch != 5 {
		t.Fatalf("stream b: %+v", bs)
	}
	if v := bs[0].Batch.Column(0).(*array.String); v.Len() != 2 || v.Value(0) != "x" || v.Value(1) != "y" {
		t.Errorf("stream b's batch changed: %v", v)
	}

	ready := got[""]
	if len(ready) != 1 || ready[0].Kind != runner.FrameReady || ready[0].Epoch != 5 {
		t.Fatalf("ready: %+v", ready)
	}
	// Each schema is the stream's at its barrier: a's x hadn't widened yet.
	want := map[string]arrow.DataType{"a": arrow.PrimitiveTypes.Int64, "b": arrow.BinaryTypes.String}
	if len(ready[0].Ready.Schemas) != len(want) {
		t.Fatalf("schemas for %d streams, want %d", len(ready[0].Ready.Schemas), len(want))
	}
	for name, typ := range want {
		schema, err := decodeSchema(ready[0].Ready.Schemas[name])
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := schema.Field(0).Type; !arrow.TypeEqual(got, typ) {
			t.Errorf("%s's first column is %v, want %v", name, got, typ)
		}
	}
}

func decodeSchema(b []byte) (*arrow.Schema, error) {
	r, err := ipc.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer r.Release()
	return r.Schema(), nil
}

func TestRunReportsEngineErrors(t *testing.T) {
	ctx := testContext(t)
	got, err := run(ctx, &Dataflow{Options: options(t, nil)}, plan("a"),
		runner.Frame{Kind: runner.FrameRows, Stream: "a", Rows: rows(`[1]`)})
	release(got)
	if errs.CodeOf(err) != errs.DataInvalid {
		t.Fatalf("err = %v, want DATA_INVALID", err)
	}

	_, err = run(ctx, &Dataflow{Options: options(t, nil)}, plan("a", "a"))
	if errs.CodeOf(err) != errs.ConfigInvalid {
		t.Fatalf("err = %v, want CONFIG_INVALID", err)
	}
}

func TestRunReportsEngineDeaths(t *testing.T) {
	for _, tc := range []struct {
		sig  syscall.Signal
		want errs.Code
	}{
		{syscall.SIGKILL, errs.ResourceExhausted},
		{syscall.SIGTERM, errs.Internal},
	} {
		t.Run(tc.sig.String(), func(t *testing.T) {
			ctx := testContext(t)
			df := &Dataflow{Options: options(t, nil)}
			df.started = func(p *process) { _ = p.cmd.Process.Signal(tc.sig) }
			in := make(chan runner.Frame)
			out := make(chan runner.Frame, 16)
			err := df.Run(ctx, plan("a"), in, out)
			if errs.CodeOf(err) != tc.want {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

// startJob starts an engine with a job and connects both of its edges.
func startJob(t *testing.T) (p *process, in, out io.Closer) {
	ctx := testContext(t)
	p, err := start(ctx, options(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.close)
	req, _, _ := startRequest(plan("a"))
	if _, err := p.client.StartJob(ctx, req); err != nil {
		t.Fatal(err)
	}
	toEngine, err := p.dialEdge(ctx, inEdge)
	if err != nil {
		t.Fatal(err)
	}
	fromEngine, err := p.dialEdge(ctx, outEdge)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = toEngine.Close(); _ = fromEngine.Close() })
	return p, toEngine, fromEngine
}

func waitExited(t *testing.T, p *process) {
	t.Helper()
	select {
	case <-p.exited:
	case <-time.After(10 * time.Second):
		t.Fatal("the engine is still running")
	}
}

func TestEngineExitsWhenTheRunnerCloses(t *testing.T) {
	t.Run("edge", func(t *testing.T) {
		p, in, _ := startJob(t)
		_ = in.Close()
		waitExited(t, p)
	})
	t.Run("stdin", func(t *testing.T) {
		p, _, _ := startJob(t)
		_ = p.stdin.Close()
		waitExited(t, p)
	})
	// A killed runner also stops reading the engine's output, so logging
	// the exit fails.
	t.Run("killed", func(t *testing.T) {
		dir, err := os.MkdirTemp("", "oe-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
		cmd := exec.Command(engineBinary(t), "serve", "--control", dir+"/c.sock", "--data", dir+"/d.sock", "--watch-stdin")
		stdin, _ := cmd.StdinPipe()
		stdout, _ := cmd.StdoutPipe()
		stderr, _ := cmd.StderrPipe()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		ready := make([]byte, len("ready\n"))
		if _, err := io.ReadFull(stdout, ready); err != nil {
			t.Fatal(err)
		}
		_ = stdout.Close()
		_ = stderr.Close()
		_ = stdin.Close()
		var waitErr error
		exited := make(chan struct{})
		go func() {
			waitErr = cmd.Wait()
			close(exited)
		}()
		select {
		case <-exited:
			if waitErr != nil {
				t.Fatalf("the engine didn't exit cleanly: %v", waitErr)
			}
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-exited
			t.Fatal("the engine is still running")
		}
	})
}

// pagedSource emits each stream's pages with a resume point after each, and
// calls afterPage with the number of pages emitted so far.
type pagedSource struct {
	pages     map[string][][]string
	afterPage func(ctx context.Context, n int) error
}

func (s *pagedSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	n := 0
	for _, cs := range req.Streams {
		for i, page := range s.pages[cs.Name] {
			if err := out.Rows(cs.Name, rows(page...)); err != nil {
				return err
			}
			if err := out.ResumePoint(cs.Name, []byte(strconv.Itoa(i+1))); err != nil {
				return err
			}
			n++
			if err := s.afterPage(ctx, n); err != nil {
				return err
			}
		}
		if err := out.Complete(cs.Name); err != nil {
			return err
		}
	}
	return nil
}

// memDest applies each epoch's ids per stream at Commit.
type memDest struct {
	mu      sync.Mutex
	pending map[uint64]map[string][]int64
	applied map[string][]int64
	types   map[string]arrow.DataType
	commits int
	// Closed and replaced at every Commit.
	committed chan struct{}
}

func newMemDest() *memDest {
	return &memDest{
		pending:   map[uint64]map[string][]int64{},
		applied:   map[string][]int64{},
		types:     map[string]arrow.DataType{},
		committed: make(chan struct{}),
	}
}

func (d *memDest) waitCommits(ctx context.Context, n int) error {
	for {
		d.mu.Lock()
		done, ch := d.commits >= n, d.committed
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

func (d *memDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopePipeline}}
}

func (d *memDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *memDest) Write(_ context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	idx := batch.Schema().FieldIndices("id")
	if len(idx) != 1 {
		return fmt.Errorf("batch without id: %v", batch.Schema())
	}
	col, ok := batch.Column(idx[0]).(*array.Int64)
	if !ok {
		return fmt.Errorf("id is %v", batch.Column(idx[0]).DataType())
	}
	if d.pending[e.N] == nil {
		d.pending[e.N] = map[string][]int64{}
	}
	d.pending[e.N][stream] = append(d.pending[e.N][stream], col.Int64Values()...)
	d.types[stream] = batch.Schema().Field(batch.Schema().FieldIndices("name")[0]).Type
	return nil
}

func (d *memDest) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	return destination.Staged(strconv.FormatUint(e.N, 10)), nil
}

func (d *memDest) Commit(_ context.Context, e destination.Epoch, _ destination.Staged) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for stream, ids := range d.pending[e.N] {
		d.applied[stream] = append(d.applied[stream], ids...)
	}
	delete(d.pending, e.N)
	d.commits++
	close(d.committed)
	d.committed = make(chan struct{})
	return nil
}

func (d *memDest) Recover(context.Context, uint64, destination.Epoch, destination.Staged) (destination.Recovery, error) {
	return destination.Applied, nil
}

func (d *memDest) Close(context.Context) error { return nil }

func TestRunAttemptThroughTheEngine(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	opts := options(t, mem)
	defer mem.AssertSize(t, 0)
	ctx := testContext(t)

	// Each page waits for the previous one's epoch to commit, so with a
	// one-byte threshold every page is its own epoch.
	dest := newMemDest()
	src := &pagedSource{
		pages: map[string][][]string{
			"users": {
				{`{"id": 1, "name": "a"}`, `{"id": 2, "name": "b"}`},
				{`{"id": 3, "name": null}`},
				{`{"id": 4, "name": "d", "extra": true}`},
			},
			"orders": {
				{`{"id": 10, "name": "x"}`},
			},
		},
		afterPage: dest.waitCommits,
	}
	job := runner.Job{
		Owner:       "a1",
		Pipeline:    "pipe",
		Log:         checkpoint.NewLog(checkpoint.NewMemStore(), "org", "pipe"),
		Source:      src,
		Destination: dest,
		Dataflow:    &Dataflow{Options: opts},
		Settings: runner.Settings{
			Require:        delivery.ExactlyOnce,
			Scope:          destination.ScopePipeline,
			Dedupe:         delivery.DedupeAuto,
			CommitMaxBytes: 1,
		},
	}
	for _, s := range []string{"users", "orders"} {
		job.Streams = append(job.Streams, runner.Stream{Name: s, Replay: source.Exact, Mode: destination.Append})
	}

	res, err := runner.RunAttempt(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if res.Committed != 4 || res.Last != 4 {
		t.Errorf("committed %d epochs, last %d; want 4 and 4", res.Committed, res.Last)
	}
	if got := dest.applied["users"]; !slices.Equal(got, []int64{1, 2, 3, 4}) {
		t.Errorf("users applied %v", got)
	}
	if got := dest.applied["orders"]; !slices.Equal(got, []int64{10}) {
		t.Errorf("orders applied %v", got)
	}
	if typ := dest.types["users"]; !arrow.TypeEqual(typ, arrow.BinaryTypes.String) {
		t.Errorf("users.name is %v, want string", typ)
	}
	if len(dest.pending) != 0 {
		t.Errorf("epochs left unapplied: %v", dest.pending)
	}
}

// readIDs reads the id column of every Parquet file in dir, in name order.
func readIDs(t *testing.T, dir string) []int64 {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for _, name := range files {
		f, err := os.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		tbl, err := pqarrow.ReadTable(t.Context(), f, parquet.NewReaderProperties(nil), pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		for _, chunk := range tbl.Column(tbl.Schema().FieldIndices("id")[0]).Data().Chunks() {
			ids = append(ids, chunk.(*array.Int64).Int64Values()...)
		}
		tbl.Release()
	}
	return ids
}

func TestRunAttemptIntoParquet(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	opts := options(t, mem)
	defer mem.AssertSize(t, 0)
	ctx := testContext(t)

	root := t.TempDir()
	dest, err := enginesink.NewParquet([]byte(`{"url": "file://` + filepath.ToSlash(root) + `"}`))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	commits, committed := 0, make(chan struct{})
	waitCommits := func(ctx context.Context, n int) error {
		for {
			mu.Lock()
			done, ch := commits >= n, committed
			mu.Unlock()
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
	src := &pagedSource{
		pages: map[string][][]string{
			"users": {
				{`{"id": 1, "name": "a"}`, `{"id": 2, "name": "b"}`},
				{`{"id": 3, "name": null}`},
				{`{"id": 4, "name": "d", "extra": true}`},
			},
			"orders": {
				{`{"id": 10}`},
			},
		},
		afterPage: waitCommits,
	}
	job := runner.Job{
		Owner:       "a1",
		Pipeline:    "pipe",
		Log:         checkpoint.NewLog(checkpoint.NewMemStore(), "org", "pipe"),
		Source:      src,
		Destination: dest,
		Dataflow:    &Dataflow{Options: opts},
		Settings: runner.Settings{
			Require:        delivery.ExactlyOnce,
			Scope:          destination.ScopeStream,
			Dedupe:         delivery.DedupeAuto,
			CommitMaxBytes: 1,
		},
		OnCommit: func(uint64) {
			mu.Lock()
			defer mu.Unlock()
			commits++
			close(committed)
			committed = make(chan struct{})
		},
	}
	for _, s := range []string{"users", "orders"} {
		job.Streams = append(job.Streams, runner.Stream{Name: s, Replay: source.Exact, Mode: destination.Append})
	}

	res, err := runner.RunAttempt(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if res.Committed != 4 {
		t.Errorf("committed %d epochs, want 4", res.Committed)
	}
	if got := readIDs(t, filepath.Join(root, "users")); !slices.Equal(got, []int64{1, 2, 3, 4}) {
		t.Errorf("users holds %v", got)
	}
	if got := readIDs(t, filepath.Join(root, "orders")); !slices.Equal(got, []int64{10}) {
		t.Errorf("orders holds %v", got)
	}
	markers, err := filepath.Glob(filepath.Join(root, "_openetl", "loads", "*", "*"))
	if err != nil || len(markers) != 4 {
		t.Errorf("markers %v, %v; want one per stream and epoch with rows", markers, err)
	}
	if left, _ := os.ReadDir(filepath.Join(root, "_openetl", "staging")); len(left) != 0 {
		t.Errorf("staging left behind: %v", left)
	}
}

// discardDest is the engine's discard sink. It records the epochs committed.
type discardDest struct{ committed []uint64 }

func (d *discardDest) EngineSink() runner.SinkPlan { return runner.SinkPlan{Discard: true} }

func (d *discardDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopeStream}}
}

func (d *discardDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *discardDest) Write(context.Context, destination.Epoch, string, arrow.RecordBatch) error {
	return errors.New("the engine sink got a batch")
}

func (d *discardDest) PreCommit(context.Context, destination.Epoch) (destination.Staged, error) {
	return nil, errors.New("the engine sink was asked to pre-commit")
}

func (d *discardDest) Commit(_ context.Context, e destination.Epoch, s destination.Staged) error {
	if len(s) != 0 {
		return fmt.Errorf("epoch %d staged %q", e.N, s)
	}
	d.committed = append(d.committed, e.N)
	return nil
}

func (d *discardDest) Recover(context.Context, uint64, destination.Epoch, destination.Staged) (destination.Recovery, error) {
	return destination.Applied, nil
}

func (d *discardDest) Close(context.Context) error { return nil }

func TestRunAttemptIntoTheDiscardSink(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	opts := options(t, mem)
	defer mem.AssertSize(t, 0)

	dest := &discardDest{}
	job := runner.Job{
		Owner:    "a1",
		Pipeline: "pipe",
		Log:      checkpoint.NewLog(checkpoint.NewMemStore(), "org", "pipe"),
		Source: &pagedSource{
			pages:     map[string][][]string{"users": {{`{"id": 1}`}, {`{"id": 2}`}}},
			afterPage: func(context.Context, int) error { return nil },
		},
		Streams:     []runner.Stream{{Name: "users", Replay: source.Exact, Mode: destination.Append}},
		Destination: dest,
		Dataflow:    &Dataflow{Options: opts},
		Settings:    runner.Settings{Require: delivery.ExactlyOnce, Scope: destination.ScopeStream, Dedupe: delivery.DedupeAuto},
	}
	res, err := runner.RunAttempt(testContext(t), job)
	if err != nil {
		t.Fatal(err)
	}
	if res.Committed != 1 || !slices.Equal(dest.committed, []uint64{1}) {
		t.Errorf("committed %d epochs: %v; want epoch 1", res.Committed, dest.committed)
	}
}
