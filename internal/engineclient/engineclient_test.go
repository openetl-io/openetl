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
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
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
	"github.com/unsuitable001/openetl/sdk/go/arrowipc"
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

func TestRunNamesTablesAndColumns(t *testing.T) {
	ctx := testContext(t)
	p := plan("Users")
	p.Naming = runner.Naming{MaxLength: 12}
	got, err := run(ctx, &Dataflow{Options: options(t, nil)}, p,
		runner.Frame{Kind: runner.FrameRows, Stream: "Users", Rows: rows(`{"First Name": "a", "first_name": "b", "a_rather_long_name": 1}`)},
	)
	defer release(got)
	if err != nil {
		t.Fatal(err)
	}
	s := got["Users"][0].Batch.Schema()
	var names []string
	for _, f := range s.Fields() {
		names = append(names, f.Name)
	}
	if len(names) != 7 || names[0] != "first_name" || names[1] != "first_name_2" || len(names[2]) != 12 || !strings.HasPrefix(names[2], "a_r_") {
		t.Errorf("columns are %q", names)
	}
	if table, _ := s.Metadata().GetValue("openetl.table"); table != "users" {
		t.Errorf("table is %q, want users", table)
	}
	if names, _ := s.Metadata().GetValue("openetl.source_names"); !strings.Contains(names, `"first_name":"First Name"`) {
		t.Errorf("source names are %s", names)
	}
	if s.Field(0).HasMetadata() {
		t.Errorf("first_name has metadata %v", s.Field(0).Metadata)
	}
}

func TestRunAddsMetadataColumns(t *testing.T) {
	ctx := testContext(t)
	p := plan("a")
	p.Streams[0].PrimaryKey = []string{"id"}
	p.ReadStarts = map[string]int64{"a": 1_700_000_000_000_000}
	ids := func() (*arrow.Schema, []string) {
		got, err := run(ctx, &Dataflow{Options: options(t, nil)}, p,
			runner.Frame{Kind: runner.FrameRows, Stream: "a", Rows: rows(`{"id": 1, "v": "x"}`, `{"id": 2, "v": "y"}`)},
		)
		defer release(got)
		if err != nil {
			t.Fatal(err)
		}
		b := got["a"][0].Batch
		col := b.Column(b.Schema().FieldIndices("_openetl_id")[0])
		var out []string
		for i := range col.Len() {
			out = append(out, col.ValueStr(i))
		}
		seq := b.Column(b.Schema().FieldIndices("_openetl_seq")[0]).(*array.Timestamp)
		if seq.Value(0) != 1_700_000_000_000_000 {
			t.Errorf("_openetl_seq is %v, want the read start", seq.Value(0))
		}
		if op := b.Column(b.Schema().FieldIndices("_openetl_op")[0]).(*array.String); op.Value(1) != "insert" {
			t.Errorf("_openetl_op is %q", op.Value(1))
		}
		return b.Schema(), out
	}
	schema, first := ids()
	var names []string
	for _, f := range schema.Fields() {
		names = append(names, f.Name)
	}
	if want := []string{"id", "v", "_openetl_id", "_openetl_op", "_openetl_seq", "_openetl_synced_at"}; !slices.Equal(names, want) {
		t.Fatalf("columns are %q, want %q", names, want)
	}
	if ext := extensionName(schema.Field(2)); ext != "arrow.uuid" {
		t.Errorf("_openetl_id has extension %q", ext)
	}
	if first[0] == first[1] {
		t.Errorf("two rows share the ID %s", first[0])
	}
	if _, again := ids(); !slices.Equal(again, first) {
		t.Errorf("a second engine gave IDs %q, the first %q", again, first)
	}
}

func TestRunTakesAChangelogsControlColumns(t *testing.T) {
	ctx := testContext(t)
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "doc", Type: arrow.BinaryTypes.String, Nullable: true},
		source.OpField(), source.SeqField(&arrow.Decimal128Type{Precision: 38, Scale: 0}), source.UnchangedField(),
	}, nil))
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 1}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"long", ""}, []bool{true, false})
	b.Field(2).(*array.StringBuilder).AppendValues([]string{source.OpInsert, source.OpUpdate}, nil)
	b.Field(3).(*array.Decimal128Builder).AppendValues([]decimal128.Num{decimal128.FromI64(7), decimal128.FromI64(9)}, nil)
	u := b.Field(4).(*array.ListBuilder)
	u.Append(true)
	u.Append(true)
	u.ValueBuilder().(*array.StringBuilder).Append("doc")
	batch := b.NewRecordBatch()
	b.Release()
	p := plan("a")
	p.Streams[0].PrimaryKey = []string{"id"}
	p.Streams[0].Read = source.CDC
	p.Streams[0].Mode = destination.Merge
	got, err := run(ctx, &Dataflow{Options: options(t, nil)}, p, runner.Frame{Kind: runner.FrameBatch, Stream: "a", Batch: batch})
	defer release(got)
	if err != nil {
		t.Fatal(err)
	}
	out := got["a"][0].Batch
	var names []string
	for _, f := range out.Schema().Fields() {
		names = append(names, f.Name)
	}
	if want := []string{"id", "doc", "_openetl_id", "_openetl_op", "_openetl_seq", "_openetl_synced_at", "_openetl_deleted", "_openetl_unchanged"}; !slices.Equal(names, want) {
		t.Fatalf("columns are %q, want %q", names, want)
	}
	col := func(name string) arrow.Array { return out.Column(out.Schema().FieldIndices(name)[0]) }
	if op := col("_openetl_op").(*array.String); op.Value(0) != "insert" || op.Value(1) != "update" {
		t.Errorf("ops %v", op)
	}
	if seq := col("_openetl_seq").(*array.Decimal128); seq.Value(1) != decimal128.FromI64(9) {
		t.Errorf("_openetl_seq %v", seq)
	}
	if u := col("_openetl_unchanged").ValueStr(1); u != `["doc"]` {
		t.Errorf("_openetl_unchanged is %s", u)
	}
}

func TestPassthroughLeavesBatchesAsTheyCame(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	opts := options(t, mem)
	opts.Passthrough = true
	defer mem.AssertSize(t, 0)
	ctx := testContext(t)

	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "Key", Type: arrow.PrimitiveTypes.Int64}}, nil))
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	batch := b.NewRecordBatch()
	b.Release()
	p := plan("a")
	p.Streams[0].PrimaryKey = []string{"Key"}
	got, err := run(ctx, &Dataflow{Options: opts}, p, runner.Frame{Kind: runner.FrameBatch, Stream: "a", Batch: batch})
	defer release(got)
	if err != nil {
		t.Fatal(err)
	}
	if a := got["a"]; len(a) != 1 || a[0].Batch.Schema().NumFields() != 1 || a[0].Batch.Schema().Field(0).Name != "Key" {
		t.Fatalf("stream a: %+v", a)
	}
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

func TestPeakMemoryIsTheEngines(t *testing.T) {
	// On Linux the engine's rusage would include this.
	ballast := make([]byte, 256<<20)
	for i := 0; i < len(ballast); i += 4096 {
		ballast[i] = 1
	}
	var peaks []int64
	opts := options(t, nil)
	opts.PeakMemory = func(n int64) { peaks = append(peaks, n) }
	got, err := run(testContext(t), &Dataflow{Options: opts}, plan("a"),
		runner.Frame{Kind: runner.FrameRows, Stream: "a", Rows: rows(`{"x": 1}`)},
		runner.Frame{Kind: runner.FrameBarrier, Stream: "a", Epoch: 5},
	)
	release(got)
	runtime.KeepAlive(ballast)
	if err != nil {
		t.Fatal(err)
	}
	if len(peaks) != 1 {
		t.Fatalf("got %d peaks, want 1", len(peaks))
	}
	if mb := peaks[0] >> 20; mb < 1 || mb >= 128 {
		t.Errorf("peak memory is %d MiB, want the engine's own, 1-128 MiB", mb)
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
	req, _, _, err := startRequest(plan("a"))
	if err != nil {
		t.Fatal(err)
	}
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
		if err := out.Complete(cs.Name, nil); err != nil {
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

func (d *memDest) Commit(_ context.Context, e destination.Epoch, _ destination.Staged) (destination.Committed, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for stream, ids := range d.pending[e.N] {
		d.applied[stream] = append(d.applied[stream], ids...)
	}
	delete(d.pending, e.N)
	d.commits++
	close(d.committed)
	d.committed = make(chan struct{})
	return destination.Committed{}, nil
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
		Settings: delivery.Settings{
			Require:        delivery.ExactlyOnce,
			Scope:          destination.ScopePipeline,
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
	// One epoch per page, then one that records the streams completed.
	if res.Committed != 5 || res.Last != 5 {
		t.Errorf("committed %d epochs, last %d; want 5 and 5", res.Committed, res.Last)
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

// An external source's batches reach the engine as the source encoded them.
func TestRunAttemptForwardsEncodedBatches(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	opts := options(t, mem)
	defer mem.AssertSize(t, 0)
	ctx := testContext(t)

	dest := newMemDest()
	var rows uint64
	job := runner.Job{
		Owner:       "a1",
		Pipeline:    "pipe",
		Log:         checkpoint.NewLog(checkpoint.NewMemStore(), "org", "pipe"),
		Source:      encodedSource{pages: [][]int64{{1, 2}, {3}}},
		Streams:     []runner.Stream{{Name: "users", Replay: source.Exact, Mode: destination.Append}},
		Destination: dest,
		Dataflow:    &Dataflow{Options: opts},
		Settings:    delivery.Settings{Require: delivery.ExactlyOnce, Scope: destination.ScopePipeline},
		OnCheckpoint: func(rec *checkpoint.Record, _ map[string]uint64) {
			rows += rec.Streams["users"].Rows
		},
	}
	if _, err := runner.RunAttempt(ctx, job); err != nil {
		t.Fatal(err)
	}
	if got := dest.applied["users"]; !slices.Equal(got, []int64{1, 2, 3}) || rows != 3 {
		t.Errorf("applied %v; records counted %d rows", got, rows)
	}
}

// encodedSource forwards each page of ids, encoded, with a resume point
// after each.
type encodedSource struct{ pages [][]int64 }

func (s encodedSource) Read(_ context.Context, _ source.ReadRequest, out source.Emitter) error {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "name", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	for i, ids := range s.pages {
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
		b.Field(1).AppendNulls(len(ids))
		rec := b.NewRecordBatch()
		b.Release()
		payload, err := arrowipc.EncodeBatch(rec)
		rec.Release()
		if err != nil {
			return err
		}
		enc, err := arrowipc.Inspect(payload)
		if err != nil {
			return err
		}
		if err := out.(source.Forwarder).Forward("users", enc); err != nil {
			return err
		}
		if err := out.ResumePoint("users", []byte(strconv.Itoa(i+1))); err != nil {
			return err
		}
	}
	return out.Complete("users", nil)
}

func TestRunAttemptQuarantinesBadRows(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	opts := options(t, mem)
	defer mem.AssertSize(t, 0)
	ctx := testContext(t)

	dest := newMemDest()
	src := &pagedSource{
		pages:     map[string][][]string{"users": {{`{"id": 1, "name": "a"}`, `[2]`, `{"id": 3, "name": "c"}`}}},
		afterPage: dest.waitCommits,
	}
	dir := t.TempDir()
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "pipe")
	res, err := runner.RunAttempt(ctx, runner.Job{
		Owner:         "a1",
		Pipeline:      "pipe",
		Log:           log,
		Source:        src,
		Streams:       []runner.Stream{{Name: "users", Replay: source.Exact, Mode: destination.Append}},
		Destination:   dest,
		Dataflow:      &Dataflow{Options: opts},
		QuarantineURL: "file://" + dir,
		Settings: delivery.Settings{
			Require:        delivery.ExactlyOnce,
			Scope:          destination.ScopePipeline,
			CommitMaxBytes: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := dest.applied["users"]; !slices.Equal(got, []int64{1, 3}) {
		t.Errorf("users applied %v", got)
	}
	var files []checkpoint.QuarantineFile
	for n := uint64(1); n <= res.Last; n++ {
		rec, err := log.Record(ctx, checkpoint.RecordRef{Epoch: n, Generation: res.Generation})
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, rec.Quarantine...)
	}
	if len(files) != 1 || files[0].Stream != "users" || files[0].Rows != 1 {
		t.Fatalf("records list quarantine files %+v", files)
	}
	body, err := os.ReadFile(strings.TrimPrefix(files[0].URL, "file://"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"record":"[2]"`) {
		t.Errorf("the quarantine file holds %s", body)
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
		Settings: delivery.Settings{
			Require:        delivery.ExactlyOnce,
			Scope:          destination.ScopeStream,
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
	if res.Committed != 5 {
		t.Errorf("committed %d epochs, want one per page and one for completion", res.Committed)
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

func TestNanosecondTimestampsReachParquet(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	opts := options(t, mem)
	defer mem.AssertSize(t, 0)
	ctx := testContext(t)

	root := t.TempDir()
	dest, err := enginesink.NewParquet([]byte(`{"url": "file://` + filepath.ToSlash(root) + `"}`))
	if err != nil {
		t.Fatal(err)
	}
	src := &pagedSource{
		pages:     map[string][][]string{"events": {{`{"id": 1, "at": "2000-01-01T00:00:00.123456789Z"}`}}},
		afterPage: func(context.Context, int) error { return nil },
	}
	declared := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "at", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
	}, nil)
	_, err = runner.RunAttempt(ctx, runner.Job{
		Owner:       "a1",
		Pipeline:    "pipe",
		Log:         checkpoint.NewLog(checkpoint.NewMemStore(), "org", "pipe"),
		Source:      src,
		Streams:     []runner.Stream{{Name: "events", Replay: source.Exact, Mode: destination.Append, Schema: declared, NanosecondTimestamps: true}},
		Destination: dest,
		Dataflow:    &Dataflow{Options: opts},
		Settings:    delivery.Settings{Require: delivery.ExactlyOnce, Scope: destination.ScopeStream},
	})
	if err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(root, "events", "*.parquet"))
	if len(files) != 1 {
		t.Fatalf("parquet files %v", files)
	}
	f, err := os.Open(files[0])
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	tbl, err := pqarrow.ReadTable(t.Context(), f, parquet.NewReaderProperties(nil), pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Release()
	col := tbl.Column(tbl.Schema().FieldIndices("at")[0])
	if want := (&arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}); !arrow.TypeEqual(col.DataType(), want) {
		t.Fatalf("at is %s, want %s", col.DataType(), want)
	}
	if got := col.Data().Chunk(0).(*array.Timestamp).Value(0); got != 946_684_800_123_456_789 {
		t.Errorf("at = %d", got)
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

func (d *discardDest) Commit(_ context.Context, e destination.Epoch, s destination.Staged) (destination.Committed, error) {
	if len(s) != 0 {
		return destination.Committed{}, fmt.Errorf("epoch %d staged %q", e.N, s)
	}
	d.committed = append(d.committed, e.N)
	return destination.Committed{}, nil
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
		Settings:    delivery.Settings{Require: delivery.ExactlyOnce, Scope: destination.ScopeStream},
	}
	res, err := runner.RunAttempt(testContext(t), job)
	if err != nil {
		t.Fatal(err)
	}
	if res.Committed != 1 || !slices.Equal(dest.committed, []uint64{1}) {
		t.Errorf("committed %d epochs: %v; want epoch 1", res.Committed, dest.committed)
	}
}

// An empty batch in a declared schema, or no rows, gives a stream's schema
// before any row comes, for a Flink job built before the data.
func TestEmptyFramesReportTheirSchema(t *testing.T) {
	ctx := testContext(t)
	declared := arrow.NewSchema([]arrow.Field{{Name: "Id", Type: arrow.PrimitiveTypes.Int32}, {Name: "note", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	p := plan("a", "b")
	p.Streams[0].Schema = declared
	p.ReportSchemas = true
	b := array.NewRecordBuilder(memory.DefaultAllocator, declared)
	empty := b.NewRecordBatch()
	b.Release()
	got, err := run(ctx, &Dataflow{Options: options(t, nil)}, p,
		runner.Frame{Kind: runner.FrameBatch, Stream: "a", Batch: empty},
		runner.Frame{Kind: runner.FrameRows, Stream: "b", Rows: []json.RawMessage{}},
	)
	defer release(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range got["a"] {
		if f.Kind == runner.FrameBatch {
			t.Errorf("an empty batch came out: %d rows", f.Batch.NumRows())
		}
	}
	names := func(stream string) []string {
		for _, f := range got[stream] {
			if f.Kind != runner.FrameSchema {
				continue
			}
			sc, err := ipc.NewReader(bytes.NewReader(f.Schema))
			if err != nil {
				t.Fatal(err)
			}
			defer sc.Release()
			var out []string
			for _, fd := range sc.Schema().Fields() {
				out = append(out, fd.Name+":"+fd.Type.String())
			}
			return out
		}
		return nil
	}
	t.Logf("a: %v; b: %v", names("a"), names("b"))
	if a := names("a"); len(a) == 0 || a[0] != "id:int32" {
		t.Errorf("stream a's schema: %v", a)
	}
}

// holdDest takes holds on append streams, and keeps the patches' prices.
type holdDest struct {
	*memDest
	patched []string
}

func (d *holdDest) Capabilities() destination.Capabilities {
	c := d.memDest.Capabilities()
	c.Holds = destination.Holds{ByID: true, Guarded: true}
	return c
}

func (d *holdDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *holdDest) Patch(_ context.Context, _ destination.Epoch, _ string, batch arrow.RecordBatch) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	col := batch.Column(batch.Schema().FieldIndices("price")[0])
	for i := range int(batch.NumRows()) {
		d.patched = append(d.patched, col.DataType().String()+" "+col.ValueStr(i))
	}
	return nil
}

func TestRunAttemptHoldsACellAndPatchesItOnRelease(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	opts := options(t, mem)
	defer mem.AssertSize(t, 0)
	ctx := testContext(t)

	dest := &holdDest{memDest: newMemDest()}
	root := t.TempDir()
	store, err := checkpoint.NewFSStore(root)
	if err != nil {
		t.Fatal(err)
	}
	log := checkpoint.NewLog(store, "org", "pipe")
	declared := func(precision int32) *arrow.Schema {
		return arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "price", Type: &arrow.Decimal128Type{Precision: precision, Scale: 2}, Nullable: true},
		}, nil)
	}
	job := func(precision int32, pages [][]string) runner.Job {
		return runner.Job{
			Owner:       fmt.Sprintf("a%d", precision),
			Pipeline:    "pipe",
			Log:         log,
			Source:      &pagedSource{pages: map[string][][]string{"users": pages}, afterPage: dest.waitCommits},
			Destination: dest,
			Dataflow:    &Dataflow{Options: opts},
			HeldURL:     "file://" + filepath.Join(root, "org", "pipe", checkpoint.HeldDir),
			Streams: []runner.Stream{{
				Name: "users", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"id"},
				Schema: declared(precision), Holds: &runner.StreamHolds{Cells: true},
			}},
			Settings: delivery.Settings{Require: delivery.AtLeastOnce, Scope: destination.ScopePipeline, CommitMaxBytes: 1},
		}
	}
	first := job(10, [][]string{{`{"id": 1, "name": "a", "price": "1.5"}`, `{"id": 2, "name": "b", "price": "12345678901.5"}`}})
	if _, err := runner.RunAttempt(ctx, first); err != nil {
		t.Fatal(err)
	}
	if got := dest.applied["users"]; !slices.Equal(got, []int64{1, 2}) {
		t.Fatalf("applied %v: the row with a held cell is delivered", got)
	}
	head, err := log.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h := head.Streams["users"].Holds
	if h == nil || h.Cells["price"].Cells != 1 || len(h.Epochs) != 1 {
		t.Fatalf("holds = %+v", h)
	}

	// Released after the column was widened, the held value is patched in.
	second := job(20, nil)
	second.Releases = []runner.Release{{Stream: "users", Column: "price", Since: h.Cells["price"].Since}}
	if _, err := runner.RunAttempt(ctx, second); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(dest.patched, []string{"decimal(20, 2) 12345678901.5"}) {
		t.Fatalf("patched %v", dest.patched)
	}
	head, err = log.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h := head.Streams["users"].Holds; h != nil {
		t.Fatalf("the hold didn't end: %+v", h)
	}
}

// refuseDest refuses the column "extra", as a destination does a column a
// view depends on, and drops what earlier generations staged on Recover.
type refuseDest struct {
	*holdDest
	extra     map[uint64]bool
	committed bool
}

func (d *refuseDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *refuseDest) ApplySchema(_ context.Context, streams []destination.StreamSchema) (destination.SchemaApplied, error) {
	var a destination.SchemaApplied
	for _, s := range streams {
		if len(s.Schema.FieldIndices("extra")) > 0 {
			a.Refused = append(a.Refused, destination.RefusedColumn{Stream: s.Name, Column: "extra", Reason: "a view depends on it"})
		}
	}
	return a, nil
}

func (d *refuseDest) Write(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	if len(batch.Schema().FieldIndices("extra")) > 0 {
		d.mu.Lock()
		d.extra[e.N] = true
		d.mu.Unlock()
	}
	return d.memDest.Write(ctx, e, stream, batch)
}

func (d *refuseDest) Commit(ctx context.Context, e destination.Epoch, s destination.Staged) (destination.Committed, error) {
	d.mu.Lock()
	d.committed = d.committed || d.extra[e.N]
	d.mu.Unlock()
	return d.memDest.Commit(ctx, e, s)
}

func (d *refuseDest) Recover(context.Context, uint64, destination.Epoch, destination.Staged) (destination.Recovery, error) {
	d.mu.Lock()
	clear(d.pending)
	clear(d.extra)
	d.mu.Unlock()
	return destination.Applied, nil
}

func TestRunAttemptHoldsAColumnTheDestinationRefuses(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	opts := options(t, mem)
	defer mem.AssertSize(t, 0)
	ctx := testContext(t)

	dest := &refuseDest{holdDest: &holdDest{memDest: newMemDest()}, extra: map[uint64]bool{}}
	root := t.TempDir()
	store, err := checkpoint.NewFSStore(root)
	if err != nil {
		t.Fatal(err)
	}
	log := checkpoint.NewLog(store, "org", "pipe")
	res, err := runner.RunAttempt(ctx, runner.Job{
		Owner:    "a1",
		Pipeline: "pipe",
		Log:      log,
		Source: &pagedSource{pages: map[string][][]string{"users": {
			{`{"id": 1, "name": "a"}`},
			{`{"id": 2, "name": "b", "extra": true}`},
		}}, afterPage: dest.waitCommits},
		Destination: dest,
		Dataflow:    &Dataflow{Options: opts},
		HeldURL:     "file://" + filepath.Join(root, "org", "pipe", checkpoint.HeldDir),
		Streams: []runner.Stream{{
			Name: "users", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"id"},
			Holds: &runner.StreamHolds{},
		}},
		Settings: delivery.Settings{Require: delivery.AtLeastOnce, Scope: destination.ScopePipeline, CommitMaxBytes: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Generation != 2 {
		t.Errorf("generation %d: the refusal should take one more attempt", res.Generation)
	}
	if dest.committed {
		t.Error("an epoch with the refused column committed")
	}
	if got := dest.applied["users"]; len(got) == 0 || got[len(got)-1] != 2 {
		t.Errorf("applied %v", got)
	}
	head, err := log.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h := head.Streams["users"].Holds
	if h == nil || len(h.Columns) != 1 || h.Columns[0].Column != "extra" || h.Columns[0].Delivered != nil || h.Columns[0].Reason != "a view depends on it" {
		t.Fatalf("holds = %+v", h)
	}
	if len(h.Epochs) == 0 {
		t.Error("the held values' epochs aren't recorded")
	}
}

// failingCommit fails every pre-commit of the generation that first stages
// patches, so the attempt ends before it records them.
type failingCommit struct {
	*holdDest
	patches map[uint64]bool
	failGen uint64
}

func (d *failingCommit) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *failingCommit) Patch(ctx context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	d.mu.Lock()
	d.patches[e.N] = true
	d.mu.Unlock()
	return d.holdDest.Patch(ctx, e, stream, batch)
}

func (d *failingCommit) PreCommit(ctx context.Context, e destination.Epoch) (destination.Staged, error) {
	d.mu.Lock()
	if d.patches[e.N] && d.failGen == 0 {
		d.failGen = e.Generation
	}
	fail := e.Generation == d.failGen
	if fail {
		d.patched, d.patches = nil, map[uint64]bool{}
		delete(d.pending, e.N)
	}
	d.mu.Unlock()
	if fail {
		return nil, errors.New("the destination went away")
	}
	return d.memDest.PreCommit(ctx, e)
}

func TestAReleaseResumesAfterACrash(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	opts := options(t, mem)
	defer mem.AssertSize(t, 0)
	ctx := testContext(t)

	dest := &failingCommit{holdDest: &holdDest{memDest: newMemDest()}, patches: map[uint64]bool{}}
	root := t.TempDir()
	store, err := checkpoint.NewFSStore(root)
	if err != nil {
		t.Fatal(err)
	}
	log := checkpoint.NewLog(store, "org", "pipe")
	job := func(owner string, precision int32, pages [][]string) runner.Job {
		return runner.Job{
			Owner: owner, Pipeline: "pipe", Log: log, Destination: dest, Dataflow: &Dataflow{Options: opts},
			Source:  &pagedSource{pages: map[string][][]string{"users": pages}, afterPage: dest.waitCommits},
			HeldURL: "file://" + filepath.Join(root, "org", "pipe", checkpoint.HeldDir),
			Streams: []runner.Stream{{
				Name: "users", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"id"},
				Schema: arrow.NewSchema([]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
					{Name: "price", Type: &arrow.Decimal128Type{Precision: precision, Scale: 2}, Nullable: true},
				}, nil),
				Holds: &runner.StreamHolds{Cells: true},
			}},
			Settings: delivery.Settings{Require: delivery.AtLeastOnce, Scope: destination.ScopePipeline, CommitMaxBytes: 1},
		}
	}
	if _, err := runner.RunAttempt(ctx, job("a1", 10, [][]string{{`{"id": 1, "name": "a", "price": "12345678901.5"}`}})); err != nil {
		t.Fatal(err)
	}
	head, err := log.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	since := head.Streams["users"].Holds.Cells["price"].Since
	release := job("a2", 20, nil)
	release.Releases = []runner.Release{{Stream: "users", Column: "price", Since: since}}
	if _, err := runner.RunAttempt(ctx, release); err == nil {
		t.Fatal("the attempt whose patches failed to stage succeeded")
	}
	if len(dest.patched) != 0 {
		t.Fatalf("patched %v before a commit", dest.patched)
	}
	release.Owner = "a3"
	if _, err := runner.RunAttempt(ctx, release); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(dest.patched, []string{"decimal(20, 2) 12345678901.5"}) {
		t.Fatalf("patched %v after the crash", dest.patched)
	}
	if head, err = log.Latest(ctx); err != nil || head.Streams["users"].Holds != nil {
		t.Fatalf("holds %+v, %v", head.Streams["users"].Holds, err)
	}
}
