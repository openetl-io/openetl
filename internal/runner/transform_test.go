package runner_test

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// batchSource emits pages of one-row batches, with a resume point after each.
type batchSource struct{ pages map[string]int }

func (s batchSource) Read(_ context.Context, req source.ReadRequest, out source.Emitter) error {
	for _, cs := range req.Streams {
		from, _ := strconv.Atoi(string(cs.State))
		for i := from; i < s.pages[cs.Name]; i++ {
			if err := out.Records(cs.Name, ints("n", int64(i))); err != nil {
				return err
			}
			if err := out.ResumePoint(cs.Name, []byte(strconv.Itoa(i+1))); err != nil {
				return err
			}
		}
		if err := out.Complete(cs.Name, []byte(strconv.Itoa(s.pages[cs.Name]))); err != nil {
			return err
		}
	}
	return nil
}

func ints(name string, v ...int64) arrow.RecordBatch {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(v, nil)
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: name, Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{col}, int64(len(v)))
}

// fakeTransform keeps what the first dataflow stages, and outputs each
// input's rows to "out" with their stream, an op and a sequence. "none"
// gets nothing.
type fakeTransform struct {
	c *calls
	// Epoch fails in this epoch; 0 for never.
	failAt uint64
	// Leave out _openetl_seq.
	noSeq bool

	mu     sync.Mutex
	staged map[string][]arrow.RecordBatch
}

func (f *fakeTransform) stage(_ context.Context, e destination.Epoch, stream string, batches []arrow.RecordBatch) ([]destination.StagedFile, error) {
	var rows int64
	for _, b := range batches {
		b.Retain()
		rows += b.NumRows()
	}
	if rows == 0 {
		return nil, nil
	}
	p := fmt.Sprintf("%d/%d/in/%s/part-00000.parquet", e.Generation, e.N, stream)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.staged == nil {
		f.staged = map[string][]arrow.RecordBatch{}
	}
	f.staged[p] = batches
	return []destination.StagedFile{{Path: p, Rows: uint64(rows)}}, nil
}

func (f *fakeTransform) URL() string { return "mem://spark" }

func (f *fakeTransform) Start(_ context.Context, gen uint64) error {
	f.c.add("start %d", gen)
	return nil
}

func (f *fakeTransform) Epoch(_ context.Context, e destination.Epoch, inputs []runner.TransformInput, emit func(string, arrow.RecordBatch) error) error {
	f.c.add("epoch %d/%d", e.Generation, e.N)
	if e.N == f.failAt {
		return errs.New(errs.Transient, "the driver was lost")
	}
	for _, in := range inputs {
		for _, file := range in.Files {
			f.mu.Lock()
			batches := f.staged[file.Path]
			f.mu.Unlock()
			for _, b := range batches {
				if err := emit("out", f.output(in.Stream, b)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (f *fakeTransform) output(stream string, b arrow.RecordBatch) arrow.RecordBatch {
	n := b.Column(0).(*array.Int64)
	mem := memory.DefaultAllocator
	sb, ob, ib := array.NewStringBuilder(mem), array.NewStringBuilder(mem), array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	for range n.Len() {
		sb.Append(stream)
		ob.Append("insert")
		ib.Append(make([]byte, 16))
	}
	fields := []arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}, {Name: "stream", Type: arrow.BinaryTypes.String},
		{Name: "_openetl_op", Type: arrow.BinaryTypes.String}, {Name: "_openetl_id", Type: arrow.BinaryTypes.Binary}}
	cols := []arrow.Array{n, sb.NewArray(), ob.NewArray(), ib.NewArray()}
	if !f.noSeq {
		fields = append(fields, arrow.Field{Name: "_openetl_seq", Type: arrow.PrimitiveTypes.Int64})
		cols = append(cols, n)
	}
	return array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, int64(n.Len()))
}

func (f *fakeTransform) Drop(_ context.Context, e destination.Epoch) error {
	f.c.add("drop %d/%d", e.Generation, e.N)
	return nil
}

func (f *fakeTransform) Stop(context.Context) error {
	f.c.add("stop")
	return nil
}

// transformDest records what each stream gets, and tracks reads.
type transformDest struct {
	mu      sync.Mutex
	streams []destination.Stream
	rows    map[string]int64
	schemas map[string]*arrow.Schema
	starts  []string
	ends    []string
}

func (d *transformDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopePipeline},
		WriteModes: []destination.WriteMode{destination.Append, destination.Overwrite, destination.Merge}}
}

func (d *transformDest) Open(_ context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	d.streams = cfg.Streams
	return d, nil
}

func (d *transformDest) Write(_ context.Context, _ destination.Epoch, stream string, b arrow.RecordBatch) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rows[stream] += b.NumRows()
	d.schemas[stream] = b.Schema()
	return nil
}

func (d *transformDest) PreCommit(context.Context, destination.Epoch) (destination.Staged, error) {
	return nil, nil
}

func (d *transformDest) Commit(context.Context, destination.Epoch, destination.Staged) (destination.Committed, error) {
	return destination.Committed{}, nil
}

func (d *transformDest) Recover(context.Context, uint64, destination.Epoch, destination.Staged) (destination.Recovery, error) {
	return destination.Redone, nil
}

func (d *transformDest) Close(context.Context) error { return nil }

func (d *transformDest) StartReads(_ context.Context, starts map[string]time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.starts = append(d.starts, slices.Sorted(maps.Keys(starts))...)
	return nil
}

func (d *transformDest) EndRead(_ context.Context, e destination.Epoch, stream string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ends = append(d.ends, fmt.Sprintf("%s@%d", stream, e.N))
	return nil
}

func transformJob(log *checkpoint.Log, owner string, tr *fakeTransform, dst *transformDest, pages map[string]int, outputs ...runner.Stream) runner.Job {
	if dst.rows == nil {
		dst.rows, dst.schemas = map[string]int64{}, map[string]*arrow.Schema{}
	}
	return runner.Job{
		Owner:    owner,
		Pipeline: "p",
		Log:      log,
		Source:   batchSource{pages: pages},
		Streams: []runner.Stream{
			{Name: "a", Replay: source.Exact, Mode: destination.Append},
			{Name: "b", Replay: source.Exact, Mode: destination.Append},
		},
		Outputs:     outputs,
		Transform:   tr,
		Destination: dst,
		Dataflow:    runner.Passthrough{Stage: tr.stage},
		Settings:    delivery.Settings{Require: delivery.ExactlyOnce, CommitMaxBytes: 1, Scope: destination.ScopePipeline},
	}
}

var (
	outAppend = runner.Stream{Name: "out", Replay: source.Exact, Mode: destination.Append, Transformed: true}
	noneOut   = runner.Stream{Name: "none", Replay: source.Exact, Mode: destination.Append, Transformed: true}
)

// The destination gets the outputs, never the inputs. Each record keeps the
// inputs' state and the outputs' rows apart, and an epoch's files are
// dropped once its record is written. The outputs' op and sequence reach
// the second dataflow as control columns, without the IDs Spark kept.
func TestTransformedSync(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		c := &calls{}
		tr := &fakeTransform{c: c}
		dst := &transformDest{}
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		j := transformJob(log, "a1", tr, dst, map[string]int{"a": 3, "b": 1}, outAppend, noneOut)
		var recs []*checkpoint.Record
		j.OnCheckpoint = func(rec *checkpoint.Record, _ map[string]uint64) { recs = append(recs, rec) }
		res, err := runner.RunAttempt(ctx, j)
		if err != nil {
			t.Fatal(err)
		}
		if names := []string{dst.streams[0].Name, dst.streams[1].Name}; len(dst.streams) != 2 || !slices.Equal(names, []string{"out", "none"}) {
			t.Fatalf("the destination's streams: %v", dst.streams)
		}
		if dst.rows["out"] != 4 || len(dst.rows) != 1 {
			t.Errorf("rows written: %v, want 4 to out", dst.rows)
		}
		s := dst.schemas["out"]
		kind := func(name string) string {
			fs, _ := s.FieldsByName(name)
			if len(fs) != 1 {
				return "missing"
			}
			v, _ := fs[0].Metadata.GetValue(source.ControlKey)
			return v
		}
		if kind("_openetl_op") != "op" || kind("_openetl_seq") != "seq" || kind("_openetl_id") != "missing" || kind("n") != "" {
			t.Errorf("the second dataflow's schema: %v", s)
		}
		var in, out uint64
		for _, r := range recs {
			in += r.Streams["a"].Rows + r.Streams["b"].Rows
			out += r.Outputs["out"].Rows
			if _, ok := r.Outputs["none"]; !ok || len(r.Outputs) != 2 || r.Streams["out"].State != nil {
				t.Errorf("record %d: streams %v, outputs %v", r.Epoch, r.Streams, r.Outputs)
			}
		}
		if in != 4 || out != 4 {
			t.Errorf("records count %d rows in and %d out, want 4 and 4", in, out)
		}
		got := c.take()
		if got[0] != "start 1" || got[len(got)-1] != "stop" {
			t.Errorf("calls: %v", got)
		}
		for n := uint64(1); n <= res.Last; n++ {
			inOrder(t, got, fmt.Sprintf("epoch 1/%d", n), fmt.Sprintf("drop 1/%d", n))
		}
	})
}

// An epoch the transform fails isn't recorded: the next attempt reads it
// again and transforms it again, and every row lands once.
func TestTransformFailureIsRedone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		c := &calls{}
		dst := &transformDest{}
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		pages := map[string]int{"a": 4, "b": 2}
		var committed uint64
		count := func(rec *checkpoint.Record, _ map[string]uint64) { committed += rec.Outputs["out"].Rows }
		j := transformJob(log, "a1", &fakeTransform{c: c, failAt: 2}, dst, pages, outAppend)
		j.OnCheckpoint = count
		_, err := runner.RunAttempt(ctx, j)
		if errs.CodeOf(err) != errs.Transient || !strings.Contains(err.Error(), "transform epoch 2") {
			t.Fatalf("the first attempt: %v", err)
		}
		if got := c.take(); got[len(got)-1] != "stop" || slices.Contains(got, "drop 1/2") {
			t.Fatalf("the first attempt's calls: %v", got)
		}
		j = transformJob(log, "a2", &fakeTransform{c: c}, dst, pages, outAppend)
		j.OnCheckpoint = count
		res, err := runner.RunAttempt(ctx, j)
		if err != nil {
			t.Fatal(err)
		}
		if got := c.take(); !slices.Contains(got, fmt.Sprintf("epoch 2/%d", res.Restored+1)) {
			t.Errorf("the second attempt didn't transform epoch %d again: %v", res.Restored+1, got)
		}
		if committed != 6 {
			t.Errorf("committed records hold %d output rows, want 6", committed)
		}
	})
}

// A merged output needs _openetl_seq, which the transform's output lacks.
func TestMergedOutputNeedsASequence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		merged := runner.Stream{Name: "out", Replay: source.Exact, Mode: destination.Merge, PrimaryKey: []string{"n"}, Transformed: true}
		_, err := runner.RunAttempt(ctx, transformJob(checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"), "a1",
			&fakeTransform{c: &calls{}, noSeq: true}, &transformDest{}, map[string]int{"a": 1}, merged))
		if errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), `output "out" is written with merge`) {
			t.Fatalf("err = %v", err)
		}
	})
}

// An output's read starts with its inputs' and ends in the epoch where the
// last of them ends.
func TestOutputsReadEndsWithTheLastInput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		dst := &transformDest{}
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		over := runner.Stream{Name: "out", Replay: source.Exact, Mode: destination.Overwrite, Transformed: true}
		j := transformJob(log, "a1", &fakeTransform{c: &calls{}}, dst, map[string]int{"a": 1, "b": 3}, over)
		var last uint64
		j.OnCheckpoint = func(rec *checkpoint.Record, _ map[string]uint64) {
			if rec.Streams["a"].Complete || rec.Streams["b"].Complete {
				last = rec.Epoch
			}
		}
		if _, err := runner.RunAttempt(ctx, j); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(dst.starts, []string{"out"}) || !slices.Equal(dst.ends, []string{fmt.Sprintf("out@%d", last)}) {
			t.Errorf("reads started %v and ended %v, want out, ending in epoch %d", dst.starts, dst.ends, last)
		}
	})
}

// A job with a transform needs outputs, marked as such.
func TestTransformNeedsOutputs(t *testing.T) {
	ctx := context.Background()
	j := transformJob(checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"), "a1", &fakeTransform{c: &calls{}}, &transformDest{}, nil)
	if _, err := runner.RunAttempt(ctx, j); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("no outputs: %v", err)
	}
	j.Outputs = []runner.Stream{{Name: "out", Mode: destination.Append}}
	if _, err := runner.RunAttempt(ctx, j); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("an output not marked transformed: %v", err)
	}
}
