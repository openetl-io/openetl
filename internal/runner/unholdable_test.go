package runner_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// keyedDest's tables take their columns as int64 only, as DynamoDB takes
// a table's key at its type: it refuses any other type, and lists the
// columns in unholdable as columns it can't hold.
type keyedDest struct {
	countingDest
	unholdable, notNull []string
	refusals            int
}

func (d *keyedDest) Capabilities() destination.Capabilities {
	c := d.countingDest.Capabilities()
	c.Holds = destination.Holds{ByID: true, Guarded: true}
	return c
}

func (d *keyedDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *keyedDest) ApplySchema(_ context.Context, streams []destination.StreamSchema) (destination.SchemaApplied, error) {
	out := destination.SchemaApplied{Limits: map[string]destination.TableLimits{}}
	for _, s := range streams {
		out.Limits[s.Name] = destination.TableLimits{Unholdable: d.unholdable, NotNull: d.notNull}
		for _, f := range s.Schema.Fields() {
			if f.Type.ID() != arrow.INT64 {
				d.refusals++
				out.Refused = append(out.Refused, destination.RefusedColumn{
					Stream: s.Name, Column: f.Name, FromType: "N", ToType: "S",
					Reason: "the table is keyed on it as N, which it can't change",
					Fix:    "aws dynamodb delete-table --table-name " + s.Name,
				})
			}
		}
	}
	return out, nil
}

// fieldsFlow is Passthrough, with a schema for s of fields and metadata md
// in each epoch, recording each attempt's hold plan for s.
type fieldsFlow struct {
	fields []arrow.Field
	md     arrow.Metadata
	plans  *[]runner.HoldPlan
}

func (f fieldsFlow) Run(ctx context.Context, plan runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	defer close(out)
	if p := plan.Holds["s"]; p != nil {
		*f.plans = append(*f.plans, *p)
	}
	schema, err := arrowipc.EncodeSchema(arrow.NewSchema(f.fields, &f.md))
	if err != nil {
		return err
	}
	mid := make(chan runner.Frame)
	errc := make(chan error, 1)
	go func() { errc <- runner.Passthrough{Convert: columnless}.Run(ctx, plan, in, mid) }()
	for fr := range mid {
		if fr.Kind == runner.FrameReady {
			fr.Ready.Schemas = map[string][]byte{"s": schema}
		}
		select {
		case out <- fr:
		case <-ctx.Done():
		}
	}
	return <-errc
}

// A refused column the engine can't hold fails the sync with the
// destination's fix, and isn't held: not one the destination lists as
// unholdable, nor the stream's key, by its source name.
func TestARefusedColumnThatCantBeHeldFailsTheSync(t *testing.T) {
	sourceNames := arrow.NewMetadata([]string{"openetl.source_names"}, []string{`{"id":"Id"}`})
	for _, tc := range []struct {
		name       string
		key        string
		col        string
		unholdable []string
		md         arrow.Metadata
		why        string
	}{
		{name: "listed", key: "id", col: "p", unholdable: []string{"p"}, why: "the destination can't hold it"},
		{name: "key", key: "id", col: "id", why: "it's part of the stream's key"},
		{name: "renamed key", key: "Id", col: "id", md: sourceNames, why: "it's part of the stream's key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
			dest := &keyedDest{unholdable: tc.unholdable}
			var plans []runner.HoldPlan
			streams := []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{tc.key}, Holds: &runner.StreamHolds{}}}
			fields := func(typ arrow.DataType) []arrow.Field {
				fs := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}
				if tc.col != "id" {
					fs = append(fs, arrow.Field{Name: tc.col, Type: arrow.PrimitiveTypes.Int64})
				}
				fs[len(fs)-1].Type = typ
				return fs
			}

			j := job(log, "a1", pagedSource{pages: 1}, fieldsFlow{fields: fields(arrow.PrimitiveTypes.Int64), md: tc.md, plans: &plans}, make(chan uint64, 64))
			j.Destination, j.HeldURL, j.Streams = dest, "mem://held", streams
			if _, err := runner.RunAttempt(t.Context(), j); err != nil {
				t.Fatalf("sync 1: %v", err)
			}

			plans = nil
			j = job(log, "a2", pagedSource{pages: 1}, fieldsFlow{fields: fields(arrow.BinaryTypes.String), md: tc.md, plans: &plans}, make(chan uint64, 64))
			j.Destination, j.HeldURL, j.Streams = dest, "mem://held", streams
			_, err := runner.RunAttempt(t.Context(), j)
			if errs.CodeOf(err) != errs.SchemaDrift {
				t.Fatalf("got %v, want SCHEMA_DRIFT", err)
			}
			for _, want := range []string{tc.why, "the table is keyed on it as N", "aws dynamodb delete-table --table-name s"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("%v doesn't say %q", err, want)
				}
			}
			if dest.refusals != 1 {
				t.Errorf("the destination refused %d times, want once: no attempt holds it", dest.refusals)
			}
			for _, p := range plans {
				if slices.ContainsFunc(p.Holding, func(c checkpoint.ColumnHold) bool { return c.Column == tc.col }) {
					t.Errorf("an attempt held %s: %+v", tc.col, p.Holding)
				}
			}
		})
	}
}

// A refused column that can't be null is held where the pipeline gives it
// a held value, by its source name.
func TestARefusedNotNullColumnWithAHeldValueIsHeld(t *testing.T) {
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
	dest := &keyedDest{notNull: []string{"amount"}}
	var plans []runner.HoldPlan
	holds := &runner.StreamHolds{Columns: map[string]runner.Held{"Amount": {Mode: runner.HeldValue, Value: "0"}}}
	streams := []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"id"}, Holds: holds}}
	md := arrow.NewMetadata([]string{"openetl.source_names"}, []string{`{"amount":"Amount"}`})
	flow := func(typ arrow.DataType) fieldsFlow {
		return fieldsFlow{fields: []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "amount", Type: typ}}, md: md, plans: &plans}
	}

	j := job(log, "a1", pagedSource{pages: 1}, flow(arrow.PrimitiveTypes.Int64), make(chan uint64, 64))
	j.Destination, j.HeldURL, j.Streams = dest, "mem://held", streams
	if _, err := runner.RunAttempt(t.Context(), j); err != nil {
		t.Fatalf("sync 1: %v", err)
	}

	plans = nil
	j = job(log, "a2", pagedSource{pages: 1}, flow(arrow.BinaryTypes.String), make(chan uint64, 64))
	j.Destination, j.HeldURL, j.Streams = dest, "mem://held", streams
	// This flow doesn't deliver a held column at the destination's type, so
	// the destination refuses it again.
	_, err := runner.RunAttempt(t.Context(), j)
	if err != nil && strings.Contains(err.Error(), "can't be held") {
		t.Fatalf("got %v, want amount held", err)
	}
	if !slices.ContainsFunc(plans, func(p runner.HoldPlan) bool {
		return slices.ContainsFunc(p.Holding, func(c checkpoint.ColumnHold) bool { return c.Column == "amount" })
	}) {
		t.Errorf("no attempt held amount: %v", err)
	}
}
