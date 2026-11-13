package runner_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"golang.org/x/sync/errgroup"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// sideSchema is a schema of one column, n, marked as an input's or an
// output's.
func sideSchema(t *testing.T, side, stream string) []byte {
	t.Helper()
	md := arrow.NewMetadata([]string{"side"}, []string{side + " " + stream})
	b, err := arrowipc.EncodeSchema(arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, &md))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// sqlFlow stands in for an engine that runs SQL: each input's frames go to
// the output rename names, and each epoch reports its inputs' schemas and
// its outputs' apart.
type sqlFlow struct {
	t      *testing.T
	rename map[string]string

	mu    sync.Mutex
	plans []runner.Plan
}

func (f *sqlFlow) Run(ctx context.Context, plan runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	f.mu.Lock()
	f.plans = append(f.plans, plan)
	f.mu.Unlock()
	inputs, outputs := map[string][]byte{}, map[string][]byte{}
	for _, s := range plan.Streams {
		inputs[s.Name] = sideSchema(f.t, "input", s.Name)
	}
	for _, o := range plan.Outputs {
		outputs[o.Name] = sideSchema(f.t, "output", o.Name)
	}
	inner := plan
	inner.Streams, inner.Outputs, inner.SQL = plan.Outputs, nil, nil
	renamed, passed := make(chan runner.Frame, 16), make(chan runner.Frame, 16)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(renamed)
		for fr := range in {
			fr.Stream = f.rename[fr.Stream]
			select {
			case renamed <- fr:
			case <-gctx.Done():
				if fr.Batch != nil {
					fr.Batch.Release()
				}
				return gctx.Err()
			}
		}
		return nil
	})
	g.Go(func() error { return runner.Passthrough{}.Run(gctx, inner, renamed, passed) })
	g.Go(func() error {
		defer close(out)
		for fr := range passed {
			if fr.Kind == runner.FrameReady {
				fr.Ready.Schemas, fr.Ready.OutputSchemas = maps.Clone(inputs), maps.Clone(outputs)
			}
			select {
			case out <- fr:
			case <-gctx.Done():
				if fr.Batch != nil {
					fr.Batch.Release()
				}
				return gctx.Err()
			}
		}
		return nil
	})
	return g.Wait()
}

// sqlJob reads a and b, and writes a, which replaces stream a, and c, from
// b.
func sqlJob(log *checkpoint.Log, owner string, flow *sqlFlow, dst *transformDest, pages map[string]int) runner.Job {
	if dst.rows == nil {
		dst.rows, dst.schemas = map[string]int64{}, map[string]*arrow.Schema{}
	}
	stream := func(name string, output bool) runner.Stream {
		return runner.Stream{Name: name, Replay: source.Exact, Mode: destination.Append, Transformed: output}
	}
	n := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)
	return runner.Job{
		Owner:    owner,
		Pipeline: "p",
		Log:      log,
		Source:   batchSource{pages: pages},
		Streams:  []runner.Stream{stream("a", false), stream("b", false)},
		Outputs:  []runner.Stream{stream("a", true), stream("c", true)},
		SQL: &runner.SQLTransform{
			SQL:       "INSERT INTO a SELECT n FROM a; INSERT INTO c SELECT n FROM b",
			Expected:  []runner.ExpectedOutput{{Name: "a", Schema: n}, {Name: "c", Schema: n}},
			Operators: []string{"op-1"},
		},
		Destination: dst,
		Dataflow:    flow,
		Settings:    delivery.Settings{Require: delivery.ExactlyOnce, CommitMaxBytes: 1, Scope: destination.ScopePipeline},
	}
}

func decoded(t *testing.T, b []byte) string {
	t.Helper()
	if b == nil {
		return "none"
	}
	s, err := arrowipc.DecodeSchema(b)
	if err != nil {
		t.Fatal(err)
	}
	v, _ := s.Metadata().GetValue("side")
	return v
}

// With SQL, the dataflow gets its inputs and outputs apart, and the
// destination only the outputs. Each record keeps the inputs' schemas in
// its streams and the outputs' in its outputs, schema history gets them
// apart, and the next attempt starts each from its own.
func TestSQLKeepsOutputsApart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		flow := &sqlFlow{t: t, rename: map[string]string{"a": "a", "b": "c"}}
		dst := &transformDest{}
		j := sqlJob(log, "a1", flow, dst, map[string]int{"a": 3, "b": 2})
		var recs []*checkpoint.Record
		var reported []string
		j.OnCheckpoint = func(rec *checkpoint.Record, _ map[string]uint64) { recs = append(recs, rec) }
		j.OnSchemas = func(_ destination.Epoch, schemas, outputs map[string][]byte) {
			for _, name := range slices.Sorted(maps.Keys(schemas)) {
				reported = append(reported, decoded(t, schemas[name]))
			}
			for _, name := range slices.Sorted(maps.Keys(outputs)) {
				reported = append(reported, decoded(t, outputs[name]))
			}
		}
		if _, err := runner.RunAttempt(ctx, j); err != nil {
			t.Fatal(err)
		}

		p := flow.plans[0]
		names := func(ss []runner.Stream) []string {
			out := make([]string, len(ss))
			for i, s := range ss {
				out[i] = s.Name
			}
			return out
		}
		if !slices.Equal(names(p.Streams), []string{"a", "b"}) || !slices.Equal(names(p.Outputs), []string{"a", "c"}) || p.SQL != j.SQL {
			t.Errorf("the plan: streams %v, outputs %v, SQL %v", names(p.Streams), names(p.Outputs), p.SQL)
		}
		if len(p.Schemas) != 0 || len(p.OutputSchemas) != 0 {
			t.Errorf("a first attempt starts from schemas %v and %v", p.Schemas, p.OutputSchemas)
		}
		if got := []string{dst.streams[0].Name, dst.streams[1].Name}; len(dst.streams) != 2 || !slices.Equal(got, []string{"a", "c"}) {
			t.Errorf("the destination's streams: %v", dst.streams)
		}
		if dst.rows["a"] != 3 || dst.rows["c"] != 2 || len(dst.rows) != 2 {
			t.Errorf("rows written: %v", dst.rows)
		}
		for _, r := range recs {
			got := fmt.Sprint(decoded(t, r.Streams["a"].Schema), ", ", decoded(t, r.Streams["b"].Schema), ", ",
				decoded(t, r.Outputs["a"].Schema), ", ", decoded(t, r.Outputs["c"].Schema))
			if got != "input a, input b, output a, output c" || len(r.Streams) != 2 || len(r.Outputs) != 2 {
				t.Errorf("record %d: %s, streams %v, outputs %v", r.Epoch, got, slices.Sorted(maps.Keys(r.Streams)), slices.Sorted(maps.Keys(r.Outputs)))
			}
		}
		if !slices.Equal(reported, []string{"input a", "input b", "output a", "output c"}) {
			t.Errorf("schema history got %v", reported)
		}

		if _, err := runner.RunAttempt(ctx, sqlJob(log, "a2", flow, dst, map[string]int{"a": 3, "b": 2})); err != nil {
			t.Fatal(err)
		}
		p = flow.plans[1]
		got := fmt.Sprint(decoded(t, p.Schemas["a"]), ", ", decoded(t, p.Schemas["b"]), ", ",
			decoded(t, p.OutputSchemas["a"]), ", ", decoded(t, p.OutputSchemas["c"]))
		if got != "input a, input b, output a, output c" {
			t.Errorf("the next attempt starts from %s", got)
		}
	})
}

// oldRecord is a record as written before outputs, with one stream, a,
// read up to page 1 under schema: what the next attempt restores.
func oldRecord(t *testing.T, schema []byte) *checkpoint.MemStore {
	t.Helper()
	store := checkpoint.NewMemStore()
	put := func(key, data string) {
		if _, err := store.Create(t.Context(), "org/p/_checkpoints/"+key, []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	put("HEAD", `{"format":1,"generation":1,"owner":"old","record":{"epoch":2,"generation":1},"first":true}`)
	put("00000000000000000002-g00000000000000000001.json", fmt.Sprintf(
		`{"epoch":2,"generation":1,"streams":{"a":{"state":"MQ==","schema":%q,"read_start":1762300000000000,"rows":1}},`+
			`"key_groups":256,"committables":[null],"created_at":"2026-11-01T00:00:00Z"}`,
		base64.StdEncoding.EncodeToString(schema)))
	return store
}

// A record from before outputs restores: a job without SQL starts its
// streams from it, as before, and one with SQL its inputs, with nothing
// for its outputs.
func TestARecordFromBeforeOutputsRestores(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		schema := sideSchema(t, "input", "a")
		pages := map[string]int{"a": 2, "b": 1}

		flow := &sqlFlow{t: t, rename: map[string]string{"a": "a", "b": "c"}}
		dst := &transformDest{}
		res, err := runner.RunAttempt(ctx, sqlJob(checkpoint.NewLog(oldRecord(t, schema), "org", "p"), "a2", flow, dst, pages))
		if err != nil {
			t.Fatal(err)
		}
		p := flow.plans[0]
		if res.Restored != 2 || decoded(t, p.Schemas["a"]) != "input a" || len(p.Schemas) != 1 || len(p.OutputSchemas) != 0 {
			t.Errorf("restored %d, schemas %v, outputs' %v", res.Restored, p.Schemas, p.OutputSchemas)
		}
		if dst.rows["a"] != 1 || dst.rows["c"] != 1 {
			t.Errorf("rows written: %v, want page 1 of a and page 0 of b", dst.rows)
		}

		var plans []runner.Plan
		pass := runner.Passthrough{}
		j := runner.Job{
			Owner:       "a2",
			Pipeline:    "p",
			Log:         checkpoint.NewLog(oldRecord(t, schema), "org", "p"),
			Source:      batchSource{pages: pages},
			Streams:     []runner.Stream{{Name: "a", Replay: source.Exact, Mode: destination.Append}},
			Destination: &transformDest{rows: map[string]int64{}, schemas: map[string]*arrow.Schema{}},
			Dataflow: dataflowFunc(func(ctx context.Context, p runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
				plans = append(plans, p)
				return pass.Run(ctx, p, in, out)
			}),
			Settings: delivery.Settings{Require: delivery.ExactlyOnce, CommitMaxBytes: 1, Scope: destination.ScopePipeline},
		}
		if _, err := runner.RunAttempt(ctx, j); err != nil {
			t.Fatal(err)
		}
		if p := plans[0]; decoded(t, p.Schemas["a"]) != "input a" || p.SQL != nil || p.Outputs != nil || p.OutputSchemas != nil {
			t.Errorf("without SQL: schemas %v, SQL %v, outputs %v and %v", p.Schemas, p.SQL, p.Outputs, p.OutputSchemas)
		}
	})
}

type dataflowFunc func(ctx context.Context, p runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error

func (f dataflowFunc) Run(ctx context.Context, p runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	return f(ctx, p, in, out)
}

// A pipeline has one transform, and SQL needs outputs.
func TestSQLNeedsOutputsAndNoOtherTransform(t *testing.T) {
	ctx := context.Background()
	j := sqlJob(checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"), "a1", &sqlFlow{t: t}, &transformDest{}, nil)
	j.Outputs = nil
	if _, err := runner.RunAttempt(ctx, j); errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "has no outputs") {
		t.Errorf("no outputs: %v", err)
	}
	j = sqlJob(checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"), "a1", &sqlFlow{t: t}, &transformDest{}, nil)
	j.Transform = &fakeTransform{c: &calls{}}
	if _, err := runner.RunAttempt(ctx, j); errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "one transform") {
		t.Errorf("SQL and Spark: %v", err)
	}
}

// A SQL transform runs on as many lanes as a pipeline without one, unlike
// Spark's and Flink's.
func TestSQLRunsOnSeveralLanes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		flow := &sqlFlow{t: t, rename: map[string]string{"a": "a", "b": "c"}}
		dst := &transformDest{}
		j := sqlJob(checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"), "a1", flow, dst, map[string]int{"a": 3, "b": 2})
		j.Lanes = 4
		if _, err := runner.RunAttempt(ctx, j); err != nil {
			t.Fatal(err)
		}
		if p := flow.plans[0]; p.Lanes != 2 {
			t.Errorf("lanes %d, want one per stream's split", p.Lanes)
		}
		if dst.rows["a"] != 3 || dst.rows["c"] != 2 {
			t.Errorf("rows written: %v", dst.rows)
		}
	})
}
