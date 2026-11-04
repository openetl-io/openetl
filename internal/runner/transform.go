package runner

import (
	"context"
	"fmt"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"golang.org/x/sync/errgroup"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// Transform runs the user's code between the source and the destination
// (ADR-0022). An attempt's first dataflow stages each epoch's inputs below
// URL, at {g}/{N}/in/{stream}/; Epoch turns them into the outputs; and a
// second dataflow takes those to the destination. Epochs reach it one at a
// time, in order.
type Transform interface {
	URL() string
	// Start starts the attempt's transform once the attempt has recovered.
	Start(ctx context.Context, gen uint64) error
	// Epoch transforms epoch e, emitting each output's batches, which their
	// receiver releases.
	Epoch(ctx context.Context, e destination.Epoch, inputs []TransformInput, emit func(output string, b arrow.RecordBatch) error) error
	// Drop deletes what epoch e left, once its record is written. It's best
	// effort.
	Drop(ctx context.Context, e destination.Epoch) error
	// Stop ends the attempt's transform, whether the attempt failed or not,
	// and after a Start that failed.
	Stop(ctx context.Context) error
}

// TransformInput is one input stream's part of an epoch.
type TransformInput struct {
	Stream string
	// Below Transform.URL; none if the stream had no rows in the epoch.
	Files []destination.StagedFile
	// As Ready.Schemas has it; nil if the stream has never had a column.
	Schema []byte
}

// TransformInputs is the directory of each epoch's {g}/{N}/ below
// Transform.URL that holds its inputs.
const TransformInputs = "in"

// Metadata columns the second dataflow computes again, so it drops an
// output's.
var recomputed = []string{"_openetl_id", "_openetl_synced_at", "_openetl_deleted"}

// runTransform runs the attempt's two dataflows around its transform: in
// feeds the first, which stages the inputs; the transform turns each epoch
// the first reports ready into the outputs, which feed the second; and out
// carries the second's frames, its FrameReady with the first's report.
func (c *coordinator) runTransform(ctx context.Context, g *errgroup.Group, plan Plan, in <-chan Frame, out chan<- Frame) {
	first := plan
	first.Sink = SinkPlan{StagedURL: c.job.Transform.URL(), StagedDir: TransformInputs}
	second := plan
	second.Streams, second.Schemas = c.job.Outputs, c.outputSchemas
	if plan.QuarantineURL != "" {
		second.QuarantineURL = plan.QuarantineURL + "/" + checkpoint.QuarantineOutputs
	}
	// An output's rows without a sequence carry its first input's read start.
	second.ReadStarts = map[string]int64{}
	var start int64
	for _, s := range c.job.Streams {
		if t := plan.ReadStarts[s.Name]; start == 0 || t < start {
			start = t
		}
	}
	for _, o := range c.job.Outputs {
		second.ReadStarts[o.Name] = start
	}

	staged := make(chan Frame, 16)
	toSecond := make(chan Frame, 16)
	fromSecond := make(chan Frame, 16)
	reports := make(chan *Ready, maxInFlight+1)
	g.Go(func() error { return c.job.Dataflow.Run(ctx, first, in, staged) })
	gen := plan.Generation
	g.Go(func() error { return c.transformEpochs(ctx, gen, staged, toSecond, reports) })
	g.Go(func() error { return c.job.Dataflow.Run(ctx, second, toSecond, fromSecond) })
	g.Go(func() error { return mergeReports(ctx, fromSecond, reports, out) })
}

// transformEpochs runs the transform on each epoch the first dataflow
// reports ready, and sends the outputs' batches and barriers to the second.
func (c *coordinator) transformEpochs(ctx context.Context, gen uint64, staged <-chan Frame, to chan<- Frame, reports chan<- *Ready) error {
	defer close(to)
	defer close(reports)
	outputs := map[string]Stream{}
	for _, o := range c.job.Outputs {
		outputs[o.Name] = o
	}
	changelog := slices.ContainsFunc(c.job.Streams, func(s Stream) bool { return s.Read == source.CDC })
	for {
		var f Frame
		select {
		case fr, ok := <-staged:
			if !ok {
				return nil
			}
			f = fr
		case <-ctx.Done():
			return ctx.Err()
		}
		if f.Kind != FrameReady || f.Ready == nil {
			release(f)
			return fmt.Errorf("runner: the transform's first dataflow sent a frame of kind %d", f.Kind)
		}
		inputs := make([]TransformInput, len(c.job.Streams))
		for i, s := range c.job.Streams {
			inputs[i] = TransformInput{Stream: s.Name, Files: f.Ready.Files[s.Name], Schema: f.Ready.Schemas[s.Name]}
		}
		rows := map[string]uint64{}
		emit := func(output string, b arrow.RecordBatch) error {
			o, ok := outputs[output]
			if !ok {
				b.Release()
				return errs.New(errs.Internal, "runner: the transform emitted %q, which isn't an output", output)
			}
			ob, err := outputBatch(b, o, changelog)
			b.Release()
			if err != nil {
				return err
			}
			rows[output] += uint64(ob.NumRows()) //nolint:gosec // never negative
			return send(ctx, to, Frame{Kind: FrameBatch, Stream: output, Batch: ob})
		}
		e := destination.Epoch{Generation: gen, N: f.Epoch}
		if err := c.job.Transform.Epoch(ctx, e, inputs, emit); err != nil {
			return fmt.Errorf("runner: transform epoch %d: %w", f.Epoch, err)
		}
		r := *f.Ready
		// The inputs' files are the transform's, not the destination's.
		r.Files, r.OutputRows = nil, rows
		select {
		case reports <- &r:
		case <-ctx.Done():
			return ctx.Err()
		}
		for _, o := range c.job.Outputs {
			if err := send(ctx, to, Frame{Kind: FrameBarrier, Stream: o.Name, Epoch: f.Epoch}); err != nil {
				return err
			}
		}
	}
}

// mergeReports forwards the second dataflow's frames, adding the first's
// report of each epoch to the second's.
func mergeReports(ctx context.Context, from <-chan Frame, reports <-chan *Ready, out chan<- Frame) error {
	defer close(out)
	for {
		var f Frame
		select {
		case fr, ok := <-from:
			if !ok {
				return nil
			}
			f = fr
		case <-ctx.Done():
			return ctx.Err()
		}
		if f.Kind == FrameReady {
			var first *Ready
			select {
			case r, ok := <-reports:
				if !ok {
					return fmt.Errorf("runner: the transform's second dataflow reported epoch %d, which the first didn't", f.Epoch)
				}
				first = r
			case <-ctx.Done():
				return ctx.Err()
			}
			merged := *first
			merged.Staged, merged.Files, merged.Snapshots = f.Ready.Staged, f.Ready.Files, f.Ready.Snapshots
			merged.OutputSchemas = f.Ready.Schemas
			merged.Quarantine = slices.Clone(first.Quarantine)
			for _, q := range f.Ready.Quarantine {
				q.Output = true
				merged.Quarantine = append(merged.Quarantine, q)
			}
			f.Ready = &merged
		}
		if err := send(ctx, out, f); err != nil {
			return err
		}
	}
}

func send(ctx context.Context, ch chan<- Frame, f Frame) error {
	select {
	case ch <- f:
		return nil
	case <-ctx.Done():
		release(f)
		return ctx.Err()
	}
}

// outputBatch is an output's batch as the second dataflow takes it. Spark
// keeps no field metadata, so _openetl_op, _openetl_seq and
// _openetl_unchanged become control columns again by name, and the metadata
// columns the dataflow computes are dropped. A merged output needs a
// sequence, and an op too when an input is a changelog.
func outputBatch(b arrow.RecordBatch, o Stream, changelog bool) (arrow.RecordBatch, error) {
	schema := b.Schema()
	var fields []arrow.Field
	var cols []arrow.Array
	var op, seq bool
	for i, f := range schema.Fields() {
		switch {
		case slices.Contains(recomputed, f.Name):
			continue
		case f.Name == "_openetl_op":
			if !arrow.TypeEqual(f.Type, arrow.BinaryTypes.String) {
				return nil, errs.New(errs.ConfigInvalid, "output %q: _openetl_op is %s, not a string", o.Name, f.Type)
			}
			op, f = true, control(f, "op")
		case f.Name == "_openetl_seq":
			seq, f = true, control(f, "seq")
		case f.Name == "_openetl_unchanged":
			if lt, ok := f.Type.(*arrow.ListType); !ok || !arrow.TypeEqual(lt.Elem(), arrow.BinaryTypes.String) {
				return nil, errs.New(errs.ConfigInvalid, "output %q: _openetl_unchanged is %s, not a list of strings", o.Name, f.Type)
			}
			f = control(f, "unchanged")
		default:
			f.Metadata = arrow.Metadata{}
		}
		fields = append(fields, f)
		cols = append(cols, b.Column(i))
	}
	switch {
	case o.Mode == destination.Merge && !seq:
		return nil, errs.New(errs.ConfigInvalid, "output %q is written with merge, which orders a key's versions by _openetl_seq, and the transform's output has no _openetl_seq column", o.Name)
	case o.Mode == destination.Merge && changelog && !op:
		return nil, errs.New(errs.ConfigInvalid, "output %q is written with merge from a changelog, so it needs each row's _openetl_op, and the transform's output has no _openetl_op column", o.Name)
	}
	md := schema.Metadata()
	return array.NewRecordBatch(arrow.NewSchema(fields, &md), cols, b.NumRows()), nil
}

// control marks f a control column of kind, keeping its type.
func control(f arrow.Field, kind string) arrow.Field {
	return arrow.Field{Name: f.Name, Type: f.Type, Nullable: true, Metadata: arrow.NewMetadata([]string{source.ControlKey}, []string{kind})}
}
