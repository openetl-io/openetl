package sim

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// sparkCluster is a Spark cluster for a transform: each
// generation's application, and the inputs the first dataflow staged, by
// path below the transform's URL. Its transform returns each input's rows
// as the output of the input's name, without field metadata, as Spark
// does. It records where the runner breaks the transform's contract.
type sparkCluster struct {
	mu sync.Mutex
	// Running or ended, by generation. An ended application transforms
	// nothing more.
	running map[uint64]bool
	files   map[string][]row
	// Generations whose files were dropped as older ones.
	dropped  map[uint64]bool
	problems []string
}

func newSparkCluster() *sparkCluster {
	return &sparkCluster{running: map[uint64]bool{}, files: map[string][]row{}, dropped: map[uint64]bool{}}
}

func (c *sparkCluster) problem(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.problems = append(c.problems, fmt.Sprintf(format, args...))
}

// sparkApp is one attempt's runner.Transform. A newer attempt's Start ends
// an older generation's application, so a zombie attempt's next epoch
// fails, as its application, given _end and killed, would.
type sparkApp struct {
	w       *world
	attempt int

	mu               sync.Mutex
	gen              uint64
	started, stopped bool
	busy             bool
	last             uint64
}

var _ runner.Transform = (*sparkApp)(nil)

func (a *sparkApp) point(ctx context.Context, op string, apply func() error) error {
	return outcome(a.w.sched.point(ctx, a.attempt, op), apply)
}

func (a *sparkApp) URL() string { return "sim://spark" }

// stage is the first dataflow's part: one file per input and epoch.
func (a *sparkApp) stage(ctx context.Context, e destination.Epoch, _ int, stream string, batches []arrow.RecordBatch) ([]destination.StagedFile, error) {
	var rows []row
	for _, b := range batches {
		rows = append(rows, rowsOf(b)...)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	p := epochDir(e) + runner.TransformInputs + "/" + stream + "/part-00000.parquet"
	c := a.w.spark
	err := a.point(ctx, "spark.stage", func() error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.files[p] = rows
		return nil
	})
	return []destination.StagedFile{{Path: p, Rows: uint64(len(rows))}}, err
}

func (a *sparkApp) Start(ctx context.Context, gen uint64) error {
	a.mu.Lock()
	if a.started {
		a.w.spark.problem("a%d started a second Spark application", a.attempt)
	}
	a.started, a.gen = true, gen
	a.mu.Unlock()
	c := a.w.spark
	// A submission whose outcome is unknown runs.
	return a.point(ctx, "spark.submit", func() error {
		c.mu.Lock()
		defer c.mu.Unlock()
		for g := range c.running {
			if g < gen {
				c.running[g] = false
				c.dropped[g] = true
				prefix := fmt.Sprintf("%d/", g)
				for p := range c.files {
					if strings.HasPrefix(p, prefix) {
						delete(c.files, p)
					}
				}
			}
		}
		c.running[gen] = true
		return nil
	})
}

// Epoch transforms the epoch, then reads each output back, a file each, so a
// driver lost or a read failing partway leaves the epoch half sent.
func (a *sparkApp) Epoch(ctx context.Context, e destination.Epoch, inputs []runner.TransformInput, emit func(string, arrow.RecordBatch) error) error {
	c := a.w.spark
	a.mu.Lock()
	switch {
	case !a.started || e.Generation != a.gen:
		c.problem("a%d sent epoch %d-g%d to Spark, and its application is generation %d (started: %v)", a.attempt, e.N, e.Generation, a.gen, a.started)
	case a.stopped:
		c.problem("a%d sent epoch %d to Spark after stopping its application", a.attempt, e.N)
	case a.busy:
		c.problem("a%d sent epoch %d to Spark while epoch %d was in it", a.attempt, e.N, a.last+1)
	case e.N <= a.last:
		c.problem("a%d sent epoch %d to Spark after epoch %d", a.attempt, e.N, a.last)
	}
	a.busy = true
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.busy = false
		a.mu.Unlock()
	}()

	var outputs [][]row
	err := a.point(ctx, "spark.epoch", func() error {
		c.mu.Lock()
		defer c.mu.Unlock()
		if !c.running[e.Generation] {
			return errs.New(errs.Transient, "the Spark transform's application ended without transforming epoch %d", e.N)
		}
		outputs = make([][]row, len(inputs))
		prefix := epochDir(e) + runner.TransformInputs + "/"
		for i, in := range inputs {
			for _, f := range in.Files {
				rows, ok := c.files[f.Path]
				switch {
				case !strings.HasPrefix(f.Path, prefix+in.Stream+"/"):
					c.problems = append(c.problems, fmt.Sprintf("a%d gave Spark %s as input %s of epoch %d-g%d", a.attempt, f.Path, in.Stream, e.N, e.Generation))
				case !ok:
					c.problems = append(c.problems, fmt.Sprintf("a%d gave Spark %s, which no one staged or which was dropped", a.attempt, f.Path))
				case uint64(len(rows)) != f.Rows:
					c.problems = append(c.problems, fmt.Sprintf("a%d says %s has %d rows; it has %d", a.attempt, f.Path, f.Rows, len(rows)))
				}
				outputs[i] = append(outputs[i], rows...)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i, in := range inputs {
		if len(outputs[i]) == 0 {
			continue
		}
		if err := a.point(ctx, "spark.read", func() error { return nil }); err != nil {
			return err
		}
		if err := emit(in.Stream, sparkBatch(outputs[i])); err != nil {
			return err
		}
	}
	a.mu.Lock()
	a.last = e.N
	a.mu.Unlock()
	return nil
}

func (a *sparkApp) Drop(ctx context.Context, e destination.Epoch) error {
	a.mu.Lock()
	if e.Generation != a.gen || e.N > a.last {
		a.w.spark.problem("a%d dropped epoch %d-g%d, which it didn't transform", a.attempt, e.N, e.Generation)
	}
	a.mu.Unlock()
	c := a.w.spark
	return a.point(ctx, "spark.drop", func() error {
		c.mu.Lock()
		defer c.mu.Unlock()
		for p := range c.files {
			if strings.HasPrefix(p, epochDir(e)) {
				delete(c.files, p)
			}
		}
		return nil
	})
}

func (a *sparkApp) Stop(ctx context.Context) error {
	a.mu.Lock()
	a.stopped = true
	gen := a.gen
	a.mu.Unlock()
	c := a.w.spark
	return a.point(ctx, "spark.stop", func() error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.running[gen] = false
		return nil
	})
}

// sparkBatch is rows as Spark writes them: the input's columns, its
// metadata columns among them, without field metadata.
func sparkBatch(rows []row) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, rowSchema)
	defer b.Release()
	for _, r := range rows {
		b.Field(0).(*array.StringBuilder).Append(r.ID)
		b.Field(1).(*array.Int64Builder).Append(r.Seq)
		b.Field(2).(*array.StringBuilder).Append(r.Key)
		b.Field(3).(*array.StringBuilder).Append(r.Value)
	}
	return b.NewRecordBatch()
}

// sparkFlow is the engine of a transformed attempt: the first dataflow
// stages the inputs for Spark, and the second computes each output row's
// _openetl_id again, as the engine's output lane does.
type sparkFlow struct {
	first, second runner.Passthrough
}

func (f sparkFlow) Run(ctx context.Context, plan runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	if plan.Sink.StagedDir == runner.TransformInputs {
		return f.first.Run(ctx, plan, in, out)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ids := make(chan runner.Frame)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(ids)
		for {
			var fr runner.Frame
			select {
			case x, ok := <-in:
				if !ok {
					return
				}
				fr = x
			case <-ctx.Done():
				return
			}
			if fr.Kind == runner.FrameBatch {
				b := withIDs(fr.Stream, fr.Batch)
				fr.Batch.Release()
				fr.Batch = b
			}
			select {
			case ids <- fr:
			case <-ctx.Done():
				if fr.Batch != nil {
					fr.Batch.Release()
				}
				return
			}
		}
	}()
	err := f.second.Run(ctx, plan, ids, out)
	cancel()
	<-done
	return err
}

// withIDs is an output's batch, from which the runner dropped Spark's
// _openetl_id, with the sim's rows' columns and IDs.
func withIDs(stream string, b arrow.RecordBatch) arrow.RecordBatch {
	col := func(name string) arrow.Array {
		idx := b.Schema().FieldIndices(name)
		if len(idx) != 1 {
			return nil
		}
		return b.Column(idx[0])
	}
	seqs, _ := col("_openetl_seq").(*array.Int64)
	keys, _ := col("key").(*array.String)
	vals, _ := col("value").(*array.String)
	rb := array.NewRecordBuilder(memory.DefaultAllocator, rowSchema)
	defer rb.Release()
	for i := range int(b.NumRows()) {
		var r row
		if seqs != nil && keys != nil && vals != nil {
			r = row{Seq: seqs.Value(i), Key: keys.Value(i), Value: vals.Value(i)}
		}
		rb.Field(0).(*array.StringBuilder).Append(recordID(stream, r.Key, r.Seq))
		rb.Field(1).(*array.Int64Builder).Append(r.Seq)
		rb.Field(2).(*array.StringBuilder).Append(r.Key)
		rb.Field(3).(*array.StringBuilder).Append(r.Value)
	}
	return rb.NewRecordBatch()
}
