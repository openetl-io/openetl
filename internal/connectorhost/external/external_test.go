package external

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/util"
	"google.golang.org/protobuf/proto"

	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/sdk/go/arrowipc"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

var connectors string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "oc-test-")
	if err != nil {
		panic(err)
	}
	bin := filepath.Join(dir, "testconnector")
	if out, err := exec.Command("go", "build", "-o", bin, "./testconnector").CombinedOutput(); err != nil {
		panic(string(out))
	}
	for _, name := range []string{"source-faker", "source-typed", "source-crash", "source-env", "destination-files"} {
		if err := os.Link(bin, filepath.Join(dir, name)); err != nil {
			panic(err)
		}
	}
	connectors = dir
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func exe(name string) string { return filepath.Join(connectors, name) }

// collect is an Emitter that keeps what it gets.
type collect struct {
	mu       sync.Mutex
	rows     int
	raw      []json.RawMessage
	batches  []arrow.RecordBatch
	resumes  []string
	complete []string
}

func (c *collect) Rows(_ string, rows []json.RawMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rows += len(rows)
	c.raw = append(c.raw, rows...)
	return nil
}

func (c *collect) Records(_ string, b arrow.RecordBatch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batches = append(c.batches, b)
	return nil
}

func (c *collect) ResumePoint(stream string, state []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumes = append(c.resumes, stream+"="+string(state))
	return nil
}

func (c *collect) Complete(stream string, _ []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.complete = append(c.complete, stream)
	return nil
}

func TestSourceRawRows(t *testing.T) {
	src := NewSource(exe("source-faker"), []byte(`{"seed": 1, "count": 250, "batchSize": 100}`), Options{})
	defer func() { _ = src.Close() }()
	cat, err := src.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var streams []source.ConfiguredStream
	for _, s := range cat.Streams {
		streams = append(streams, source.ConfiguredStream{Name: s.Name})
	}
	if len(streams) != 2 || cat.Streams[0].Replay != source.Exact {
		t.Fatalf("catalog = %+v", cat)
	}
	var out collect
	if err := src.Read(t.Context(), source.ReadRequest{Streams: streams}, &out); err != nil {
		t.Fatal(err)
	}
	if out.rows != 500 || len(out.complete) != 2 || len(out.resumes) != 6 {
		t.Fatalf("rows %d, resume points %v, complete %v", out.rows, out.resumes, out.complete)
	}
	// Faker doesn't acknowledge, so this is a no-op.
	if err := src.Committed(t.Context(), map[string][]byte{"users": []byte("1")}); err != nil {
		t.Fatal(err)
	}
}

func TestSourceArrow(t *testing.T) {
	src := NewSource(exe("source-typed"), nil, Options{})
	defer func() { _ = src.Close() }()
	cat, err := src.Discover(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got := cat.Streams[0].Schema.Field(0).Type; !arrow.TypeEqual(got, extensions.NewUUIDType()) {
		t.Fatalf("declared id type = %v", got)
	}
	var out collect
	if err := src.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "things"}}}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.batches) != 1 {
		t.Fatalf("%d batches", len(out.batches))
	}
	b := out.batches[0]
	defer b.Release()
	if b.NumRows() != 3 || !arrow.TypeEqual(b.Column(0).DataType(), extensions.NewUUIDType()) || b.Column(1).(*array.Int64).Value(2) != 2 {
		t.Fatalf("batch = %v", b)
	}
	if strings.Join(out.resumes, ",") != "things=3" || len(out.complete) != 1 {
		t.Fatalf("resume points %v, complete %v", out.resumes, out.complete)
	}
}

// forward is a Forwarder that keeps what it gets.
type forward struct {
	collect
	encoded []arrowipc.Encoded
}

func (f *forward) Forward(_ string, b arrowipc.Encoded) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.encoded = append(f.encoded, b)
	return nil
}

func TestSourceForwardsArrow(t *testing.T) {
	src := NewSource(exe("source-typed"), nil, Options{})
	defer func() { _ = src.Close() }()
	var out forward
	if err := src.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "things"}}}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.batches) != 0 || len(out.encoded) != 1 {
		t.Fatalf("%d batches decoded, %d forwarded", len(out.batches), len(out.encoded))
	}
	enc := out.encoded[0]
	b, err := arrowipc.DecodeBatch(enc.Bytes, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()
	if enc.Rows != 3 || enc.Size != util.TotalRecordSize(b) || !arrow.TypeEqual(b.Column(0).DataType(), extensions.NewUUIDType()) {
		t.Fatalf("forwarded %d rows, %d bytes: %v", enc.Rows, enc.Size, b)
	}
}

func TestSourceCrash(t *testing.T) {
	src := NewSource(exe("source-crash"), nil, Options{})
	defer func() { _ = src.Close() }()
	err := src.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "s"}}}, &collect{})
	if errs.CodeOf(err) != errs.Internal || !strings.Contains(err.Error(), "exited unexpectedly") {
		t.Fatalf("err = %v", err)
	}
}

func TestSourceKilled(t *testing.T) {
	src := NewSource(exe("source-faker"), []byte(`{"count": 100000, "batchSize": 10, "delay": "10ms"}`), Options{})
	defer func() { _ = src.Close() }()
	if err := src.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	out := &killer{kill: func() { _ = src.p.cmd.Process.Kill() }}
	err := src.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "users"}}}, out)
	if errs.CodeOf(err) != errs.ResourceExhausted {
		t.Fatalf("err = %v, want RESOURCE_EXHAUSTED for a SIGKILL nobody asked for", err)
	}
}

// killer kills the connector at its first resume point.
type killer struct {
	collect
	kill func()
	once sync.Once
}

func (k *killer) ResumePoint(string, []byte) error {
	k.once.Do(k.kill)
	return nil
}

// The runner's environment holds its own credentials.
func TestConnectorsDontInheritTheEnvironment(t *testing.T) {
	t.Setenv("AWS_SECRET_ACCESS_KEY", "runner's")
	src := NewSource(exe("source-env"), nil, Options{})
	defer func() { _ = src.Close() }()
	var out collect
	if err := src.Read(t.Context(), source.ReadRequest{Streams: []source.ConfiguredStream{{Name: "env"}}}, &out); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, r := range out.raw {
		var v struct{ Name string }
		if err := json.Unmarshal(r, &v); err != nil {
			t.Fatal(err)
		}
		names = append(names, v.Name)
	}
	if slices.Contains(names, "AWS_SECRET_ACCESS_KEY") || !slices.Contains(names, "OPENETL_CONNECTOR_SOCKET") {
		t.Fatalf("the connector's environment: %v", names)
	}
}

func TestUnknownExecutable(t *testing.T) {
	src := NewSource(filepath.Join(t.TempDir(), "source-none"), nil, Options{})
	if err := src.Start(t.Context()); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Fatalf("err = %v", err)
	}
	if _, err := NewSource(exe("destination-files"), nil, Options{}).Discover(t.Context()); err == nil || !strings.Contains(err.Error(), "not a CONNECTOR_KIND_SOURCE") {
		t.Fatalf("a destination started as a source: %v", err)
	}
}

func filesConfig(dir, token string) []byte {
	b, _ := json.Marshal(map[string]string{"dir": dir, "token": token})
	return b
}

func TestDestinationSecrets(t *testing.T) {
	dir := t.TempDir()
	d := NewDestination(exe("destination-files"), filesConfig(dir, "s3cr3t"), Options{})
	defer func() { _ = d.Close() }()
	if err := d.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(d.p.config), "s3cr3t") {
		t.Fatalf("the config sent to the connector holds the secret: %s", d.p.config)
	}
	if c := d.Capabilities(); c.Class != destination.StageAndApply || c.MaxIdentifierLength != 63 || !c.SupportsScope(destination.ScopePipeline) {
		t.Fatalf("capabilities = %+v", c)
	}

	bad := NewDestination(exe("destination-files"), filesConfig(dir, "nope"), Options{})
	defer func() { _ = bad.Close() }()
	if err := bad.Check(t.Context()); errs.CodeOf(err) != errs.AuthInvalid {
		t.Fatalf("check with a wrong token: %v", err)
	}
}

func batch(n int) arrow.RecordBatch {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	for i := range n {
		b.Append(int64(i))
	}
	a := b.NewArray()
	defer a.Release()
	return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{a}, int64(n))
}

func open(t *testing.T, dir string, gen uint64) (*Destination, destination.Writer) {
	t.Helper()
	d := NewDestination(exe("destination-files"), filesConfig(dir, "s3cr3t"), Options{})
	t.Cleanup(func() { _ = d.Close() })
	w, err := d.Open(t.Context(), destination.OpenConfig{Pipeline: "p", Generation: gen, Scope: destination.ScopePipeline, CommitInterval: 3 * time.Second,
		Streams: []destination.Stream{{Name: "s", Mode: destination.Append}}})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "interval")); string(got) != "3s" {
		t.Fatalf("the connector got a commit interval of %q", got)
	}
	t.Cleanup(func() { _ = w.Close(context.Background()) })
	return d, w
}

func rows(t *testing.T, dir string) int64 {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "rows.json"))
	if err != nil {
		return 0
	}
	var m map[string]int64
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m["s"]
}

func TestDestinationCommitProtocol(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	_, w := open(t, dir, 1)
	if r, err := w.Recover(ctx, 1, destination.Epoch{}, nil); err != nil || r != destination.Applied {
		t.Fatalf("recover = %v, %v", r, err)
	}
	sa, ok := w.(destination.SchemaApplier)
	if !ok {
		t.Fatal("the writer doesn't apply schemas")
	}
	b := batch(5)
	defer b.Release()
	if err := sa.ApplySchema(ctx, []destination.StreamSchema{{Name: "s", Schema: b.Schema()}}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "schemas.json")); string(got) != `{"s":["n:int64"]}` {
		t.Fatalf("schemas = %s", got)
	}

	rt, ok := w.(destination.ReadTracker)
	if !ok {
		t.Fatal("the writer doesn't track reads")
	}
	if err := rt.StartReads(ctx, map[string]time.Time{"s": time.UnixMicro(42)}); err != nil {
		t.Fatal(err)
	}
	e1 := destination.Epoch{Generation: 1, N: 1}
	if err := w.Write(ctx, e1, "s", b); err != nil {
		t.Fatal(err)
	}
	if err := rt.EndRead(ctx, e1, "s"); err != nil {
		t.Fatal(err)
	}
	st1, err := w.PreCommit(ctx, e1)
	if err != nil {
		t.Fatal(err)
	}
	// They go unanswered, in order with the pre-commit.
	if got, _ := os.ReadFile(filepath.Join(dir, "reads.log")); string(got) != "start s 42\nend s 1-1\n" {
		t.Fatalf("reads.log = %q", got)
	}
	// Epoch 2's batches arrive while epoch 1 commits.
	e2 := destination.Epoch{Generation: 1, N: 2}
	var wg sync.WaitGroup
	wg.Go(func() {
		if _, err := w.Commit(ctx, e1, st1); err != nil {
			t.Error(err)
		}
	})
	for range 3 {
		if err := w.Write(ctx, e2, "s", b); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	st2, err := w.PreCommit(ctx, e2)
	if err != nil {
		t.Fatal(err)
	}
	if got := rows(t, dir); got != 5 {
		t.Fatalf("rows after epoch 1 = %d", got)
	}

	// A newer generation recovers epoch 2, staged but not applied, and
	// fences the old writer.
	_, w2 := open(t, dir, 2)
	if r, err := w2.Recover(ctx, 2, e2, st2); err != nil || r != destination.Redone {
		t.Fatalf("recover epoch 2 = %v, %v", r, err)
	}
	if got := rows(t, dir); got != 20 {
		t.Fatalf("rows after redoing epoch 2 = %d", got)
	}
	_, err = w.Commit(ctx, e2, st2)
	if !errors.Is(err, destination.ErrFenced) || errs.CodeOf(err) != errs.Fenced {
		t.Fatalf("the fenced writer's commit = %v", err)
	}
	if r, err := w2.Recover(ctx, 2, destination.Epoch{Generation: 1, N: 3}, destination.Staged("1-3")); err != nil || r != destination.Lost {
		t.Fatalf("recover an epoch never staged = %v, %v", r, err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDestinationStreamFailure(t *testing.T) {
	dir := t.TempDir()
	_, w := open(t, dir, 3)
	if _, err := w.Recover(t.Context(), 3, destination.Epoch{}, nil); err != nil {
		t.Fatal(err)
	}
	// Staged for an epoch nobody pre-committed: the commit fails and the
	// stream stays open.
	if _, err := w.Commit(t.Context(), destination.Epoch{Generation: 3, N: 9}, destination.Staged("3-9")); err == nil {
		t.Fatal("committing an epoch that wasn't staged succeeded")
	}
	b := batch(1)
	defer b.Release()
	if err := w.Write(t.Context(), destination.Epoch{Generation: 3, N: 1}, "s", b); err != nil {
		t.Fatal(err)
	}
	if _, err := w.PreCommit(t.Context(), destination.Epoch{Generation: 3, N: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestDescriptorSpec(t *testing.T) {
	files := NewDestination(exe("destination-files"), []byte(`{"dir": "x"}`), Options{})
	if err := files.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	spec := files.p.spec
	_ = files.Close()
	if modes := Capabilities(spec).WriteModes; !slices.Equal(modes, []destination.WriteMode{destination.Append, destination.Overwrite}) {
		t.Errorf("write modes = %v", modes)
	}

	same := NewDestination(exe("destination-files"), []byte(`{"dir": "x"}`), Options{Spec: spec})
	if err := same.Start(t.Context()); err != nil {
		t.Fatalf("the connector's own spec: %v", err)
	}
	_ = same.Close()

	host := proto.Clone(spec).(*protocolv1.SpecResponse)
	host.Egress.Tier = protocolv1.EgressTier_EGRESS_TIER_HOST
	other := NewDestination(exe("destination-files"), []byte(`{"dir": "x"}`), Options{Spec: host})
	defer func() { _ = other.Close() }()
	err := other.Start(t.Context())
	if errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "descriptor") || !strings.Contains(err.Error(), `"tier":"raw"`) {
		t.Fatalf("a descriptor on another tier: %v", err)
	}
}

func TestValidateConfig(t *testing.T) {
	schema := []byte(`{"type": "object", "required": ["dir"], "properties": {"dir": {"type": "string"}, "token": {"type": "string", "x-secret": true, "pattern": "^tok_"}}}`)
	if err := ValidateConfig("files", schema, []byte(`{"dir": "x", "token": "tok_1"}`)); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConfig("files", schema, []byte(`{"token": "tok_1"}`)); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("missing dir: %v", err)
	}
	err := ValidateConfig("files", schema, []byte(`{"dir": "x", "token": "hunter2"}`))
	if err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("a secret that fails its pattern: %v", err)
	}
	if err := ValidateConfig("files", []byte(`{"type": 3}`), []byte(`{}`)); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("a bad schema: %v", err)
	}
	if _, err := CompileSchema([]byte(`{"$ref": "http://169.254.169.254/schema.json"}`)); err == nil {
		t.Error("compiled a schema that refers to a URL")
	}
}
