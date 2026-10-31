//go:build e2e

// Package e2e runs scheduled syncs through the whole stack: the API,
// Temporal, the orchestrator, runner processes and their engines, into
// Postgres. It needs the compose stack (make deps-up) and OPENETL_ENGINE.
package e2e

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/jackc/pgx/v5"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/unsuitable001/openetl/internal/daemon"
	"github.com/unsuitable001/openetl/internal/orchestration/workflows"
	"github.com/unsuitable001/openetl/internal/platform/config"
	"github.com/unsuitable001/openetl/internal/store/storetest"
)

var env struct {
	openetld, engine string
	temporal, ns     string
	dbURL, apiURL    string
	checkpoints      string
}

func TestMain(m *testing.M) {
	code, err := setup(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e:", err)
		code = 1
	}
	os.Exit(code)
}

func setup(m *testing.M) (int, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env.engine = os.Getenv("OPENETL_ENGINE")
	if env.engine == "" {
		return 0, errors.New("set OPENETL_ENGINE to an openetl-engine binary (make e2e does)")
	}
	dir, err := os.MkdirTemp("", "openetl-e2e-")
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	env.checkpoints = filepath.Join(dir, "checkpoints")
	env.openetld = filepath.Join(dir, "openetld")
	if out, err := exec.CommandContext(ctx, "go", "build", "-o", env.openetld, "github.com/unsuitable001/openetl/cmd/openetld").CombinedOutput(); err != nil {
		return 0, fmt.Errorf("build openetld: %v\n%s", err, out)
	}

	env.temporal = cmp.Or(os.Getenv("OPENETL_TEST_TEMPORAL_ADDRESS"), "localhost:7233")
	env.ns = "openetl-e2e-" + strings.ToLower(rand.Text())[:10]
	tc, err := client.DialContext(ctx, client.Options{HostPort: env.temporal})
	if err != nil {
		return 0, err
	}
	defer tc.Close()
	if _, err := tc.WorkflowService().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace: env.ns, WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	}); err != nil {
		return 0, fmt.Errorf("register namespace: %w", err)
	}
	defer func() {
		_, _ = tc.OperatorService().DeleteNamespace(context.Background(), &operatorservice.DeleteNamespaceRequest{Namespace: env.ns})
	}()

	var drop func()
	if env.dbURL, drop, err = storetest.Create(ctx); err != nil {
		return 0, err
	}
	defer drop()

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	env.apiURL = "http://" + ln.Addr().String()
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- daemon.Run(ctx, daemon.Options{
			Config: config.Config{
				DatabaseURL: env.dbURL, TemporalAddress: env.temporal, TemporalNamespace: env.ns, DataPlane: "default",
			},
			Roles:     daemon.Roles{Server: true, Orchestrator: true},
			Listener:  ln,
			Ready:     func(string) { close(ready) },
			Workflows: workflows.Workflows{AttemptRetry: time.Second, AttemptHeartbeat: 4 * time.Second},
		})
	}()
	select {
	case <-ready:
	case err := <-done:
		return 0, fmt.Errorf("server: %w", err)
	case <-time.After(30 * time.Second):
		return 0, errors.New("the server didn't start")
	}
	code := m.Run()
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		return code, fmt.Errorf("server: %w", err)
	}
	return code, nil
}

// runner is an openetld runner process.
type runner struct {
	cmd  *exec.Cmd
	out  *syncBuffer
	done chan struct{}
}

func startRunner(t *testing.T) *runner {
	t.Helper()
	cmd := exec.Command(env.openetld, "runner",
		"--api-url", env.apiURL, "--temporal-address", env.temporal, "--temporal-namespace", env.ns,
		"--checkpoints", env.checkpoints, "--engine", env.engine, "--log-format", "text")
	r := &runner{cmd: cmd, out: &syncBuffer{}, done: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = r.out, r.out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = cmd.Wait()
		close(r.done)
	}()
	t.Cleanup(func() {
		r.kill(t)
		if t.Failed() {
			t.Logf("runner %d output:\n%s", cmd.Process.Pid, r.out.String())
		}
	})
	return r
}

func (r *runner) kill(t *testing.T) {
	t.Helper()
	_ = r.cmd.Process.Kill()
	<-r.done
}

// engine returns the PID of the runner's engine process, once it has one.
func (r *runner) engine(t *testing.T) int {
	t.Helper()
	var pid int
	eventually(t, 30*time.Second, "the runner to start an engine", func() bool {
		out, err := exec.Command("pgrep", "-P", fmt.Sprint(r.cmd.Process.Pid), "openetl-engine").Output()
		if err != nil {
			return false
		}
		_, err = fmt.Sscan(string(out), &pid)
		return err == nil
	})
	return pid
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func eventually(t *testing.T, timeout time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func apply(t *testing.T, docs ...string) []*apiv1.AppliedResource {
	t.Helper()
	req := &apiv1.ApplyRequest{}
	for i, d := range docs {
		req.Documents = append(req.Documents, &apiv1.Document{Origin: fmt.Sprintf("doc %d", i+1), Content: []byte(d)})
	}
	res, err := apiv1connect.NewApplyServiceClient(http.DefaultClient, env.apiURL).Apply(t.Context(), connect.NewRequest(req))
	if err != nil {
		t.Fatal(err)
	}
	return res.Msg.GetResources()
}

// unique makes names unique per run, so -count=n works.
func unique(name string) string { return name + "-" + strings.ToLower(rand.Text())[:6] }

func schema(name string) string { return strings.ReplaceAll(name, "-", "_") }

func pipelineDocs(name string, faker, delivery string) []string {
	return []string{
		fmt.Sprintf(`{"apiVersion": "openetl.dev/v1alpha1", "kind": "Source", "metadata": {"name": %q},
			"spec": {"connector": "faker", "config": %s}}`, name, faker),
		fmt.Sprintf(`{"apiVersion": "openetl.dev/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": %q}}}`, name, env.dbURL),
		fmt.Sprintf(`{"apiVersion": "openetl.dev/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"schedule": {"every": "1h"}, "source": {"ref": %q},
			"destination": {"ref": %q, "namespace": %q}, "delivery": %s}}`, name, name, name, schema(name), delivery),
	}
}

func query[T any](t *testing.T, q string, args ...any) (T, error) {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var v T
	err = conn.QueryRow(t.Context(), q, args...).Scan(&v)
	return v, err
}

// waitSync waits for the pipeline's latest sync to finish, and returns its
// status and stats.
func waitSync(t *testing.T, pipeline string, timeout time.Duration) (status, stats string) {
	t.Helper()
	q := `SELECT s.status || ' ' || coalesce(s.stats::text, s.error, '') FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id
		WHERE p.name = $1 ORDER BY s.start_time DESC LIMIT 1`
	eventually(t, timeout, "pipeline "+pipeline+" to finish a sync", func() bool {
		v, err := query[string](t, q, pipeline)
		if err != nil {
			return false
		}
		status, stats, _ = strings.Cut(v, " ")
		return status != "running"
	})
	return status, stats
}

// checkRows fails unless each table has want rows, each id once.
func checkRows(t *testing.T, schema string, want int, tables ...string) {
	t.Helper()
	for _, table := range tables {
		ident := pgx.Identifier{schema, table}.Sanitize()
		v, err := query[string](t, "SELECT count(*) || ' ' || count(DISTINCT id) FROM "+ident)
		if err != nil {
			t.Fatal(err)
		}
		if v != fmt.Sprintf("%d %d", want, want) {
			t.Errorf("%s: rows and distinct ids are %s, want %d each", ident, v, want)
		}
	}
}

func committedEpochs(t *testing.T, schema string) int {
	n, err := query[int](t, "SELECT count(*) FROM "+pgx.Identifier{schema, "_openetl_loads"}.Sanitize())
	if err != nil {
		return 0
	}
	return n
}

func TestScheduledSync(t *testing.T) {
	startRunner(t)
	name := unique("basic")
	res := apply(t, pipelineDocs(name, `{"seed": 1, "count": 500}`, `{}`)...)
	for _, r := range res {
		if r.GetAction() != apiv1.ApplyAction_APPLY_ACTION_CREATED {
			t.Errorf("%s: %v", r.GetName(), r.GetAction())
		}
	}
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkRows(t, schema(name), 500, "users", "purchases")

	for _, r := range apply(t, pipelineDocs(name, `{"seed": 1, "count": 500}`, `{}`)...) {
		if r.GetAction() != apiv1.ApplyAction_APPLY_ACTION_UNCHANGED {
			t.Errorf("reapplied %s: %v", r.GetName(), r.GetAction())
		}
	}
}

const slowFaker = `{"seed": 2, "count": 2000, "batchSize": 50, "delay": "50ms"}`

func TestEngineKilledMidSync(t *testing.T) {
	r := startRunner(t)
	name := unique("engine-killed")
	apply(t, pipelineDocs(name, slowFaker, `{"commit": {"interval": "1s"}}`)...)
	pid := r.engine(t)
	eventually(t, 30*time.Second, "a committed epoch", func() bool { return committedEpochs(t, schema(name)) > 0 })
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	status, stats := waitSync(t, name, 90*time.Second)
	if status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if !strings.Contains(stats, `"Generation": 2`) {
		t.Errorf("stats %s: want the second attempt to have finished", stats)
	}
	checkRows(t, schema(name), 2000, "users", "purchases")
}

func TestRunnerKilledMidSync(t *testing.T) {
	first := startRunner(t)
	name := unique("runner-killed")
	apply(t, pipelineDocs(name, slowFaker, `{"commit": {"interval": "1s"}}`)...)
	engine := first.engine(t)
	eventually(t, 30*time.Second, "a committed epoch", func() bool { return committedEpochs(t, schema(name)) > 0 })
	first.kill(t)
	eventually(t, 10*time.Second, "the orphaned engine to exit", func() bool {
		return errors.Is(syscall.Kill(engine, 0), syscall.ESRCH)
	})
	startRunner(t)
	status, stats := waitSync(t, name, 90*time.Second)
	if status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if !strings.Contains(stats, `"Generation": 2`) {
		t.Errorf("stats %s: want the second attempt to have finished", stats)
	}
	checkRows(t, schema(name), 2000, "users", "purchases")
}

func docs(name, src, dst, pipeline string) []string {
	return []string{
		fmt.Sprintf(`{"apiVersion": "openetl.dev/v1alpha1", "kind": "Source", "metadata": {"name": %q}, "spec": %s}`, name, src),
		fmt.Sprintf(`{"apiVersion": "openetl.dev/v1alpha1", "kind": "Destination", "metadata": {"name": %q}, "spec": %s}`, name, dst),
		fmt.Sprintf(`{"apiVersion": "openetl.dev/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"schedule": {"every": "1h"}, "source": {"ref": %q}, "destination": {"ref": %q}, "delivery": %s}}`, name, name, name, pipeline),
	}
}

func parquetDest(dir string) string {
	return fmt.Sprintf(`{"connector": "parquet", "config": {"url": %q}}`, "file://"+filepath.ToSlash(dir))
}

// checkParquet fails unless each stream's committed files hold want rows,
// each id once.
func checkParquet(t *testing.T, dir string, want int, streams ...string) {
	t.Helper()
	for _, stream := range streams {
		if rows, ids, files := parquetRows(t, dir, stream); rows != want || ids != want {
			t.Errorf("%s: %d rows and %d distinct ids in %d files, want %d each", stream, rows, ids, files, want)
		}
	}
}

// parquetRows counts a stream's committed rows and distinct ids.
func parquetRows(t *testing.T, dir, stream string) (rows, ids, files int) {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(dir, stream, "*.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int64]bool{}
	for _, name := range names {
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
			for i := range chunk.Len() {
				seen[chunk.(*array.Int64).Value(i)] = true
				rows++
			}
		}
		tbl.Release()
	}
	return rows, len(seen), len(names)
}

func TestPostgresToParquet(t *testing.T) {
	startRunner(t)
	name := unique("postgres-parquet")
	sch := pgx.Identifier{schema(name)}.Sanitize()
	conn, err := pgx.Connect(t.Context(), env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	for _, q := range []string{
		"CREATE SCHEMA " + sch,
		"CREATE TABLE " + sch + ".items (id bigint PRIMARY KEY, name text, price numeric(10, 2), at timestamptz)",
		"INSERT INTO " + sch + ".items SELECT i, 'item ' || i, i / 100.0, now() FROM generate_series(1, 5000) i",
		"CREATE TABLE " + sch + ".events (id bigint, kind text)",
		"INSERT INTO " + sch + ".events SELECT i, 'k' || (i % 3) FROM generate_series(1, 700) i",
	} {
		if _, err := conn.Exec(t.Context(), q); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	src := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": %q, "schema": %q, "chunkRows": 1000}}`, env.dbURL, schema(name))
	apply(t, docs(name, src, parquetDest(dir), `{}`)...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkParquet(t, dir, 5000, "items")
	checkParquet(t, dir, 700, "events")
}

func TestParquetEngineKilledMidSync(t *testing.T) {
	r := startRunner(t)
	name := unique("parquet-killed")
	dir := t.TempDir()
	apply(t, docs(name, `{"connector": "faker", "config": `+slowFaker+`}`, parquetDest(dir), `{"commit": {"interval": "1s"}}`)...)
	pid := r.engine(t)
	eventually(t, 30*time.Second, "a committed epoch", func() bool {
		m, _ := filepath.Glob(filepath.Join(dir, "_openetl", "loads", "*", "*"))
		return len(m) > 0
	})
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	status, stats := waitSync(t, name, 90*time.Second)
	if status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if !strings.Contains(stats, `"Generation": 2`) {
		t.Errorf("stats %s: want the second attempt to have finished", stats)
	}
	checkParquet(t, dir, 2000, "users", "purchases")
}
