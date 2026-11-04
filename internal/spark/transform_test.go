package spark

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/destination"
)

type fakeSubmitter struct {
	mu     sync.Mutex
	apps   []Application
	states map[string]State
	found  map[string]string
	killed []string
	// Submit fails with it, after the cluster took the application.
	submitErr error
}

func (f *fakeSubmitter) Submit(_ context.Context, app Application) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.apps = append(f.apps, app)
	if f.submitErr != nil {
		return "", f.submitErr
	}
	id := fmt.Sprintf("driver-%d", len(f.apps))
	f.states[id] = State{State: "RUNNING"}
	return id, nil
}

func (f *fakeSubmitter) Find(_ context.Context, name string) (string, error) {
	return f.found[name], nil
}

func (f *fakeSubmitter) Status(_ context.Context, id string) (State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.states[id], nil
}

func (f *fakeSubmitter) Kill(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.killed = append(f.killed, id)
	f.states[id] = State{Done: true, Failed: true, State: "KILLED"}
	return nil
}

func (f *fakeSubmitter) Kind() string { return "standalone" }

func (f *fakeSubmitter) set(id string, st State) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states[id] = st
}

func schemaIPC(t *testing.T, s *arrow.Schema) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(s))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// driver stands in for openetl-spark's: it answers each _ready.json below
// root with one output file per output, holding each input's rows and the
// epoch, until ctx ends.
func driver(ctx context.Context, t *testing.T, root string) {
	done := map[string]bool{}
	for ctx.Err() == nil {
		readies, _ := filepath.Glob(filepath.Join(root, "*", "in", "_ready.json"))
		for _, r := range readies {
			if done[r] {
				continue
			}
			done[r] = true
			b, err := os.ReadFile(r)
			if err != nil {
				t.Error(err)
				return
			}
			var doc readyDoc
			if err := json.Unmarshal(b, &doc); err != nil {
				t.Error(err)
				return
			}
			var rows int64
			for _, in := range doc.Inputs {
				rows += int64(in.Rows)
			}
			epochDir := filepath.Dir(filepath.Dir(r))
			out := map[string]any{"version": 1, "epoch": doc.Epoch}
			var outputs []map[string]any
			for k, name := range doc.Outputs {
				rel := fmt.Sprintf("out/%d/part-00000-x.snappy.parquet", k)
				writeParquet(t, filepath.Join(epochDir, rel), rows, int64(doc.Epoch))
				outputs = append(outputs, map[string]any{"name": name, "files": []string{rel}})
			}
			out["outputs"] = outputs
			b, _ = json.Marshal(out)
			if err := os.WriteFile(filepath.Join(epochDir, "out", "_done.json"), b, 0o600); err != nil {
				t.Error(err)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func writeParquet(t *testing.T, path string, rows, epoch int64) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}, {Name: "spark_epoch", Type: arrow.PrimitiveTypes.Int64}}, nil)
	w, err := pqarrow.NewFileWriter(schema, f, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	n, e := array.NewInt64Builder(memory.DefaultAllocator), array.NewInt64Builder(memory.DefaultAllocator)
	for i := range rows {
		n.Append(i)
		e.Append(epoch)
	}
	b := array.NewRecordBatch(schema, []arrow.Array{n.NewArray(), e.NewArray()}, rows)
	if err := w.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func newTestTransform(t *testing.T, sub *fakeSubmitter) (*Transform, string) {
	t.Helper()
	store := t.TempDir()
	area, err := Open(t.Context(), store, "org", "Pipe")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = area.Close() })
	app := Application{Code: "file:///code.jar", MainClass: "com.acme.Enrich", Conf: map[string]string{"spark.executor.memory": "1g"}}
	tr, err := NewTransform(area, store, sub, app, []string{"enriched", "none"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tr.Poll, tr.Watch, tr.Alive, tr.StopGrace = 5*time.Millisecond, 20*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond
	return tr, filepath.Join(store, "org", "Pipe", Dir)
}

// Start ends an older generation's application and drops its files, then
// submits openetl-spark's driver with the user's class and the runner's
// properties. Each epoch's _ready.json lists its inputs; the outputs come
// back from the files _done.json lists; and Drop removes the epoch.
func TestTransformEpochs(t *testing.T) {
	sub := &fakeSubmitter{states: map[string]State{"old": {State: "RUNNING"}}, found: map[string]string{"openetl-pipe-g1": "old"}}
	tr, root := newTestTransform(t, sub)
	if err := os.MkdirAll(filepath.Join(root, "1", "4", "in"), 0o750); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	if err := tr.Start(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "1")); !errors.Is(err, os.ErrNotExist) || !slices.Equal(sub.killed, []string{"old"}) {
		t.Errorf("generation 1: %v; killed %v", err, sub.killed)
	}
	app := sub.apps[0]
	if app.Name != "openetl-pipe-g2" || app.MainClass != TransformDriver || app.Conf["spark.openetl.transform"] != "com.acme.Enrich" ||
		app.Conf["spark.openetl.root"] != "file://"+filepath.ToSlash(root)+"/2/" || app.Conf["spark.openetl.protocol"] != "1" ||
		app.Conf["spark.sql.parquet.outputTimestampType"] != "TIMESTAMP_MICROS" || app.Conf["spark.executor.memory"] != "1g" {
		t.Errorf("app = %+v", app)
	}
	if _, err := os.Stat(filepath.Join(root, "2", "_alive")); err != nil {
		t.Error(err)
	}
	if !strings.HasPrefix(tr.URL(), "file://") || !strings.HasSuffix(tr.URL(), "/org/Pipe/_spark/") {
		t.Errorf("URL() = %s", tr.URL())
	}

	dctx, stop := context.WithCancel(ctx)
	defer stop()
	go driver(dctx, t, filepath.Join(root, "2"))
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}}}, nil)
	e := destination.Epoch{Generation: 2, N: 5}
	inputs := []runner.TransformInput{
		{Stream: "orders", Schema: schemaIPC(t, schema), Files: []destination.StagedFile{{Path: "2/5/in/orders/part-00000.parquet", Rows: 3}}},
		{Stream: "users"},
	}
	got := map[string]int64{}
	err := tr.Epoch(ctx, e, inputs, func(name string, b arrow.RecordBatch) error {
		got[name] += b.NumRows()
		b.Release()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got["enriched"] != 3 || got["none"] != 3 {
		t.Errorf("rows = %v", got)
	}
	var ready readyDoc
	b, err := os.ReadFile(filepath.Join(root, "2", "5", "in", "_ready.json"))
	if err != nil || json.Unmarshal(b, &ready) != nil {
		t.Fatal(err)
	}
	if ready.Epoch != 5 || len(ready.Inputs) != 2 || ready.Inputs[0].Files[0] != "in/orders/part-00000.parquet" || ready.Inputs[0].Rows != 3 ||
		!strings.Contains(string(ready.Inputs[0].Schema), `"timestamp"`) || string(ready.Inputs[1].Schema) != "null" || len(ready.Inputs[1].Files) != 0 ||
		!slices.Equal(ready.Outputs, []string{"enriched", "none"}) {
		t.Errorf("_ready.json = %s", b)
	}
	if err := tr.Drop(ctx, e); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "2", "5")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("epoch 5 after Drop: %v", err)
	}

	// Stop writes _end; an application that doesn't end is killed.
	if err := tr.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "2", "_end")); err != nil || !slices.Contains(sub.killed, "driver-1") {
		t.Errorf("_end: %v, killed %v", err, sub.killed)
	}
}

// The user's exception fails the epoch with Failed, which isn't retried;
// an application that ends without either file fails it with TRANSIENT.
func TestTransformFailures(t *testing.T) {
	sub := &fakeSubmitter{states: map[string]State{}}
	tr, root := newTestTransform(t, sub)
	ctx := t.Context()
	if err := tr.Start(ctx, 1); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Stop(ctx) }()
	failed := `{"version": 1, "epoch": 1, "error": "java.lang.IllegalStateException: asked to fail", "trace": "at com.acme.Enrich"}`
	// The driver creates the file, then writes it, so the runner can read it
	// empty first.
	if err := os.WriteFile(filepath.Join(root, "1", "_failed.json"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(30 * time.Millisecond)
		_ = os.WriteFile(filepath.Join(root, "1", "_failed.json"), []byte(failed), 0o600)
	}()
	var f *Failed
	err := tr.Epoch(ctx, destination.Epoch{Generation: 1, N: 1}, nil, nil)
	if !errors.As(err, &f) || !strings.Contains(f.Message, "asked to fail") {
		t.Fatalf("err = %v", err)
	}
	if err := os.Remove(filepath.Join(root, "1", "_failed.json")); err != nil {
		t.Fatal(err)
	}
	sub.set("driver-1", State{Done: true, Failed: true, State: "FAILED", Message: "the driver's node was lost"})
	err = tr.Epoch(ctx, destination.Epoch{Generation: 1, N: 2}, nil, nil)
	if errs.CodeOf(err) != errs.Transient || !strings.Contains(err.Error(), "ended FAILED without transforming epoch 2: the driver's node was lost") {
		t.Fatalf("err = %v", err)
	}
}

// A transform whose submission fails is stopped too: its _alive stops being
// rewritten, and _end ends an application the cluster took.
func TestTransformSubmitFails(t *testing.T) {
	sub := &fakeSubmitter{states: map[string]State{}, submitErr: errs.New(errs.Transient, "timed out")}
	tr, root := newTestTransform(t, sub)
	ctx := t.Context()
	if err := tr.Start(ctx, 1); errs.CodeOf(err) != errs.Transient {
		t.Fatalf("start: %v", err)
	}
	if err := tr.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "1", "_end")); err != nil {
		t.Errorf("no _end: %v", err)
	}
	alive := filepath.Join(root, "1", "_alive")
	before, err := os.ReadFile(alive)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * tr.Alive)
	if after, _ := os.ReadFile(alive); string(after) != string(before) {
		t.Errorf("_alive was rewritten after Stop: %s, then %s", before, after)
	}
}

func TestSchemaJSON(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "price", Type: &arrow.Decimal128Type{Precision: 12, Scale: 2}, Nullable: true},
		{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: "local", Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
		{Name: "_openetl_id", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		{Name: "big", Type: arrow.PrimitiveTypes.Uint64},
	}, nil)
	b, err := SchemaJSON(schemaIPC(t, schema))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"price","type":"decimal(12,2)","nullable":true,"metadata":{}},` +
		`{"name":"at","type":"timestamp","nullable":false,"metadata":{}},{"name":"local","type":"timestamp_ntz","nullable":false,"metadata":{}},` +
		`{"name":"_openetl_id","type":"binary","nullable":false,"metadata":{}},` +
		`{"name":"tags","type":{"containsNull":true,"elementType":"string","type":"array"},"nullable":true,"metadata":{}},` +
		`{"name":"big","type":"decimal(20,0)","nullable":false,"metadata":{}}],"type":"struct"}`
	if string(b) != want {
		t.Errorf("got  %s\nwant %s", b, want)
	}
	clock := arrow.NewSchema([]arrow.Field{{Name: "opens", Type: arrow.FixedWidthTypes.Time64us}}, nil)
	if _, err := SchemaJSON(schemaIPC(t, clock)); err == nil || !strings.Contains(err.Error(), `column "opens": it's a time of day`) {
		t.Errorf("err = %v", err)
	}
}
