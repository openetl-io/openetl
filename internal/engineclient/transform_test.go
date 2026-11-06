package engineclient

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// passTransform reads each input's staged Parquet below dir, and outputs it
// unchanged under its name, metadata columns and all, as a transform that
// keeps them does.
type passTransform struct {
	dir string

	mu     sync.Mutex
	epochs []uint64
}

func (p *passTransform) URL() string { return "file://" + filepath.ToSlash(p.dir) }

func (p *passTransform) Start(context.Context, uint64) error { return nil }

func (p *passTransform) Epoch(ctx context.Context, e destination.Epoch, inputs []runner.TransformInput, emit func(string, arrow.RecordBatch) error) error {
	p.mu.Lock()
	p.epochs = append(p.epochs, e.N)
	p.mu.Unlock()
	for _, in := range inputs {
		for _, f := range in.Files {
			if !strings.Contains(f.Path, "/"+runner.TransformInputs+"/") {
				return &unexpected{f.Path}
			}
			pf, err := file.OpenParquetFile(filepath.Join(p.dir, filepath.FromSlash(f.Path)), false)
			if err != nil {
				return err
			}
			fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
			if err != nil {
				return err
			}
			rr, err := fr.GetRecordReader(ctx, nil, nil)
			if err != nil {
				return err
			}
			for rr.Next() {
				b := rr.RecordBatch()
				b.Retain()
				if err := emit(in.Stream, b); err != nil {
					rr.Release()
					return err
				}
			}
			rr.Release()
			if err := pf.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

type unexpected struct{ path string }

func (u *unexpected) Error() string { return "an input's file isn't in its epoch's in/: " + u.path }

func (p *passTransform) Drop(context.Context, destination.Epoch) error { return nil }

func (p *passTransform) Stop(context.Context) error { return nil }

// schemaDest is memDest that keeps each stream's last schema.
type schemaDest struct {
	*memDest
	schemas sync.Map
}

func (d *schemaDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *schemaDest) Write(ctx context.Context, e destination.Epoch, stream string, b arrow.RecordBatch) error {
	d.schemas.Store(stream, b.Schema())
	return d.memDest.Write(ctx, e, stream, b)
}

// A transform between two engines: the first stages its inputs in each
// epoch's in/, and the second takes the outputs, the inputs' metadata
// columns included, and writes every row with its metadata columns made
// again, the op and sequence it was given among them.
func TestTransformBetweenEngines(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	ctx := testContext(t)
	tr := &passTransform{dir: t.TempDir()}
	dest := &schemaDest{memDest: newMemDest()}
	src := &pagedSource{
		pages: map[string][][]string{
			"users": {
				{`{"id": 1, "name": "a"}`, `{"id": 2, "name": "b"}`},
				{`{"id": 3, "name": "c"}`},
			},
		},
		afterPage: func(context.Context, int) error { return nil },
	}
	var recs []*checkpoint.Record
	job := runner.Job{
		Owner:       "a1",
		Pipeline:    "pipe",
		Log:         checkpoint.NewLog(checkpoint.NewMemStore(), "org", "pipe"),
		Source:      src,
		Streams:     []runner.Stream{{Name: "users", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"id"}}},
		Outputs:     []runner.Stream{{Name: "users", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"id"}, Transformed: true}},
		Transform:   tr,
		Destination: dest,
		Dataflow:    &Dataflow{Options: options(t, mem)},
		Settings:    delivery.Settings{Require: delivery.ExactlyOnce, Scope: destination.ScopePipeline, CommitMaxBytes: 1},
		OnCheckpoint: func(rec *checkpoint.Record, _ map[string]uint64) {
			recs = append(recs, rec)
		},
	}
	if _, err := runner.RunAttempt(ctx, job); err != nil {
		t.Fatal(err)
	}
	if got := dest.applied["users"]; !slices.Equal(got, []int64{1, 2, 3}) {
		t.Errorf("users got %v", got)
	}
	v, _ := dest.schemas.Load("users")
	s, _ := v.(*arrow.Schema)
	if s == nil {
		t.Fatal("the destination got no batch")
	}
	var names []string
	for _, f := range s.Fields() {
		names = append(names, f.Name)
	}
	if want := []string{"id", "name", "_openetl_id", "_openetl_op", "_openetl_seq", "_openetl_synced_at"}; !slices.Equal(names, want) {
		t.Errorf("the destination's columns: %v, want %v", names, want)
	}
	var rows uint64
	for _, r := range recs {
		rows += r.Outputs["users"].Rows
		if r.Streams["users"].Schema == nil || (r.Outputs["users"].Rows > 0 && r.Outputs["users"].Schema == nil) {
			t.Errorf("record %d: input schema %d bytes, output %+v", r.Epoch, len(r.Streams["users"].Schema), r.Outputs["users"])
		}
	}
	if rows != 3 || len(tr.epochs) == 0 {
		t.Errorf("records hold %d output rows from %d epochs", rows, len(tr.epochs))
	}
}
