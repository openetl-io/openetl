// Command testconnector is an external connector for the external package's
// tests. Its executable's name picks what it serves.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"

	"github.com/unsuitable001/openetl/connectors/native/faker"
	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/host"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

func main() {
	name := filepath.Base(os.Args[0])
	// A release asset, NAME-ROLE-OS-ARCH, serves as ROLE-NAME.
	if p := strings.Split(name, "-"); len(p) == 4 && (p[1] == "source" || p[1] == "destination") {
		name = p[1] + "-" + p[0]
	}
	switch name {
	case "source-faker":
		sdk.Serve(sdk.Connector{Name: "faker", Source: func(c []byte, _ host.Host) (source.Source, error) { return faker.New(c) }})
	case "source-typed":
		sdk.Serve(sdk.Connector{Name: "typed", Source: func([]byte, host.Host) (source.Source, error) { return typed{}, nil }})
	case "source-crash":
		sdk.Serve(sdk.Connector{Name: "crash", Source: func([]byte, host.Host) (source.Source, error) { return crash{}, nil }})
	case "source-env":
		sdk.Serve(sdk.Connector{Name: "env", Source: func([]byte, host.Host) (source.Source, error) { return env{}, nil }})
	case "source-future":
		sdk.Serve(sdk.Connector{Name: "future", RequiredFeatures: []string{"from-the-future"}, Source: func([]byte, host.Host) (source.Source, error) { return env{}, nil }})
	case "source-supports":
		sdk.Serve(sdk.Connector{Name: "supports", Source: func(_ []byte, h host.Host) (source.Source, error) { return supports{h}, nil }})
	case "destination-files":
		sdk.Serve(sdk.Connector{
			Name:         "files",
			ConfigSchema: []byte(`{"type": "object", "properties": {"dir": {"type": "string"}, "token": {"type": "string", "x-secret": true}}}`),
			Egress:       sdk.Egress{Tier: sdk.EgressRaw},
			Destination:  newFiles,
			Capabilities: destination.Capabilities{
				Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopePipeline}, MaxIdentifierLength: 63,
				WriteModes: []destination.WriteMode{destination.Append, destination.Overwrite},
			},
		})
	default:
		fmt.Fprintln(os.Stderr, "unknown connector", os.Args[0])
		os.Exit(2)
	}
}

// typed emits one Arrow batch with a uuid column, then completes.
type typed struct{}

func (typed) Discover(context.Context) (source.Catalog, error) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: extensions.NewUUIDType()}, {Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)
	return source.Catalog{Streams: []source.CatalogStream{{Name: "things", PrimaryKey: []string{"id"}, Replay: source.Exact, Schema: schema}}}, nil
}

func (t typed) Read(ctx context.Context, _ source.ReadRequest, out source.Emitter) error {
	cat, _ := t.Discover(ctx)
	b := array.NewRecordBuilder(memory.DefaultAllocator, cat.Streams[0].Schema)
	defer b.Release()
	ids := b.Field(0).(*extensions.UUIDBuilder)
	for i := range 3 {
		ids.Append(uuid.NewSHA1(uuid.NameSpaceOID, []byte(strconv.Itoa(i))))
		b.Field(1).(*array.Int64Builder).Append(int64(i))
	}
	rec := b.NewRecordBatch()
	if err := out.Records("things", rec); err != nil {
		rec.Release()
		return err
	}
	if err := out.ResumePoint("things", []byte("3")); err != nil {
		return err
	}
	return out.Complete("things", nil)
}

// crash exits after its first rows.
type crash struct{}

func (crash) Discover(context.Context) (source.Catalog, error) {
	return source.Catalog{Streams: []source.CatalogStream{{Name: "s"}}}, nil
}

func (crash) Read(_ context.Context, _ source.ReadRequest, out source.Emitter) error {
	_ = out.Rows("s", []json.RawMessage{[]byte(`{"a": 1}`)})
	os.Exit(3)
	return nil
}

// env emits the names of its environment variables, one row each.
type env struct{}

func (env) Discover(context.Context) (source.Catalog, error) {
	return source.Catalog{Streams: []source.CatalogStream{{Name: "env"}}}, nil
}

func (env) Read(_ context.Context, _ source.ReadRequest, out source.Emitter) error {
	var rows []json.RawMessage
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		b, err := json.Marshal(map[string]string{"name": name})
		if err != nil {
			return err
		}
		rows = append(rows, b)
	}
	if err := out.Rows("env", rows); err != nil {
		return err
	}
	return out.Complete("env", nil)
}

// supports emits one row: whether its host supports the feature "testing".
type supports struct{ h host.Host }

func (supports) Discover(context.Context) (source.Catalog, error) {
	return source.Catalog{Streams: []source.CatalogStream{{Name: "supports"}}}, nil
}

func (s supports) Read(_ context.Context, _ source.ReadRequest, out source.Emitter) error {
	b, err := json.Marshal(map[string]bool{"testing": host.Supports(s.h, "testing"), "other": host.Supports(s.h, "other")})
	if err != nil {
		return err
	}
	if err := out.Rows("supports", []json.RawMessage{b}); err != nil {
		return err
	}
	return out.Complete("supports", nil)
}

// files is a stage-and-apply destination in a directory: a fence file,
// staged counts per epoch, a marker per applied epoch, and each stream's
// applied row count.
type files struct {
	dir   string
	token string
}

func newFiles(config []byte, _ host.Host) (destination.Destination, error) {
	var c struct {
		Dir   string `json:"dir"`
		Token string `json:"token"`
	}
	if err := json.Unmarshal(config, &c); err != nil {
		return nil, err
	}
	if c.Dir == "" {
		return nil, errors.New("files: dir is required")
	}
	return &files{dir: c.Dir, token: c.Token}, nil
}

func (f *files) Capabilities() destination.Capabilities { return destination.Capabilities{} }

func (f *files) Check(context.Context) error {
	if f.token != "s3cr3t" {
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, fmt.Errorf("files: wrong token (%d bytes)", len(f.token)))
	}
	return nil
}

// Open writes the commit interval it's given to interval.
func (f *files) Open(_ context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	for _, d := range []string{"staged", "applied"} {
		if err := os.MkdirAll(filepath.Join(f.dir, d), 0o750); err != nil {
			return nil, err
		}
	}
	if err := os.WriteFile(filepath.Join(f.dir, "interval"), []byte(cfg.CommitInterval.String()), 0o600); err != nil {
		return nil, err
	}
	return &filesWriter{files: f, gen: cfg.Generation, pending: map[uint64]map[string]int64{}}, nil
}

type filesWriter struct {
	*files
	gen     uint64
	mu      sync.Mutex
	pending map[uint64]map[string]int64
}

// StartReads and EndRead append to reads.log.
func (w *filesWriter) StartReads(_ context.Context, starts map[string]time.Time) error {
	for name, t := range starts {
		if err := w.logRead(fmt.Sprintf("start %s %d", name, t.UnixMicro())); err != nil {
			return err
		}
	}
	return nil
}

func (w *filesWriter) EndRead(_ context.Context, e destination.Epoch, stream string) error {
	return w.logRead(fmt.Sprintf("end %s %s", stream, epochName(e)))
}

func (w *filesWriter) logRead(line string) error {
	f, err := os.OpenFile(w.path("reads.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(f, line)
	return errors.Join(err, f.Close())
}

func (w *filesWriter) path(parts ...string) string {
	return filepath.Join(append([]string{w.dir}, parts...)...)
}

func epochName(e destination.Epoch) string { return fmt.Sprintf("%d-%d", e.Generation, e.N) }

func (w *filesWriter) Write(_ context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pending[e.N] == nil {
		w.pending[e.N] = map[string]int64{}
	}
	w.pending[e.N][stream] += batch.NumRows()
	return nil
}

func (w *filesWriter) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	w.mu.Lock()
	counts := w.pending[e.N]
	delete(w.pending, e.N)
	w.mu.Unlock()
	b, err := json.Marshal(counts)
	if err != nil {
		return nil, err
	}
	name := epochName(e)
	return destination.Staged(name), os.WriteFile(w.path("staged", name), b, 0o600)
}

func (w *filesWriter) fence() uint64 {
	b, _ := os.ReadFile(w.path("fence"))
	n, _ := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	return n
}

func (w *filesWriter) Commit(_ context.Context, _ destination.Epoch, s destination.Staged) (destination.Committed, error) {
	if w.fence() > w.gen {
		return destination.Committed{}, destination.ErrFenced
	}
	return destination.Committed{}, w.apply(string(s))
}

func (w *filesWriter) apply(name string) error {
	if _, err := os.Stat(w.path("applied", name)); err == nil {
		return nil
	}
	b, err := os.ReadFile(w.path("staged", name))
	if err != nil {
		return err
	}
	var counts map[string]int64
	if err := json.Unmarshal(b, &counts); err != nil {
		return err
	}
	totals := map[string]int64{}
	if b, err := os.ReadFile(w.path("rows.json")); err == nil {
		if err := json.Unmarshal(b, &totals); err != nil {
			return err
		}
	}
	for k, n := range counts {
		totals[k] += n
	}
	if b, err = json.Marshal(totals); err != nil {
		return err
	}
	if err := os.WriteFile(w.path("rows.json"), b, 0o600); err != nil {
		return err
	}
	return os.WriteFile(w.path("applied", name), nil, 0o600)
}

func (w *filesWriter) Recover(_ context.Context, gen uint64, e destination.Epoch, s destination.Staged) (destination.Recovery, error) {
	if w.fence() > gen {
		return 0, destination.ErrFenced
	}
	if err := os.WriteFile(w.path("fence"), []byte(strconv.FormatUint(gen, 10)), 0o600); err != nil {
		return 0, err
	}
	if e.IsZero() {
		return destination.Applied, nil
	}
	if _, err := os.Stat(w.path("applied", string(s))); err == nil {
		return destination.Applied, nil
	}
	if _, err := os.Stat(w.path("staged", string(s))); err != nil {
		return destination.Lost, nil
	}
	return destination.Redone, w.apply(string(s))
}

func (w *filesWriter) ApplySchema(_ context.Context, streams []destination.StreamSchema) error {
	fields := map[string][]string{}
	for _, s := range streams {
		for _, f := range s.Schema.Fields() {
			fields[s.Name] = append(fields[s.Name], f.Name+":"+f.Type.String())
		}
	}
	b, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	return os.WriteFile(w.path("schemas.json"), b, 0o600)
}

func (w *filesWriter) Close(context.Context) error { return nil }
