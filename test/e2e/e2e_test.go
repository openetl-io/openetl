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
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
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
	ggcr "github.com/google/go-containerregistry/pkg/registry"
	"go.opentelemetry.io/otel/attribute"
	"github.com/jackc/pgx/v5"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/unsuitable001/openetl/internal/auth"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/daemon"
	"github.com/unsuitable001/openetl/internal/netguard"
	"github.com/unsuitable001/openetl/internal/orchestration/workflows"
	"github.com/unsuitable001/openetl/internal/platform/config"
	"github.com/unsuitable001/openetl/internal/platform/telemetry"
	"github.com/unsuitable001/openetl/internal/platform/telemetry/telemetrytest"
	"github.com/unsuitable001/openetl/internal/registry"
	"github.com/unsuitable001/openetl/internal/store/storetest"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

var env struct {
	openetld, engine string
	// The official connectors, built from openetl-connectors. Syncs run them
	// from images in an in-process registry, by their applied descriptors.
	connectors    string
	registry      string
	temporal, ns  string
	dbURL, apiURL string
	checkpoints   string
	// Runners' connector cache.
	cache string
	// An empty connectors directory, so runners don't find the official
	// connectors in OPENETL_CONNECTORS.
	noConnectors string
	// A dbt with dbt-postgres, from OPENETL_DBT; without it, dbt steps
	// aren't tested.
	dbt string
	// Send the owner key the server wrote, and the default data plane's
	// runner token.
	http, runner *http.Client
	runnerToken  string
	// The owner key's file.
	keyFile string
	// Receives every process's traces and metrics: the server's, and each
	// runner's through the OTEL_* variables they inherit.
	collector *telemetrytest.Collector
}

// The in-process registry is on loopback.
const egressAllow = "127.0.0.1/32"

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
	env.dbt = os.Getenv("OPENETL_DBT")
	env.connectors = os.Getenv("OPENETL_CONNECTORS")
	if env.connectors == "" {
		return 0, errors.New("set OPENETL_CONNECTORS to a directory with source-postgres and destination-postgres (make e2e builds them)")
	}
	dir, err := os.MkdirTemp("", "openetl-e2e-")
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	env.checkpoints = filepath.Join(dir, "checkpoints")
	env.cache = filepath.Join(dir, "cache")
	env.noConnectors = filepath.Join(dir, "no-connectors")
	env.keyFile = filepath.Join(dir, "admin.key")
	if err := os.Mkdir(env.noConnectors, 0o750); err != nil {
		return 0, err
	}
	env.openetld = filepath.Join(dir, "openetld")
	if out, err := exec.CommandContext(ctx, "go", "build", "-o", env.openetld, "github.com/unsuitable001/openetl/cmd/openetld").CombinedOutput(); err != nil {
		return 0, fmt.Errorf("build openetld: %v\n%s", err, out)
	}

	if env.collector, err = telemetrytest.Start(); err != nil {
		return 0, err
	}
	defer env.collector.Close()
	for k, v := range env.collector.Env() {
		if err := os.Setenv(k, v); err != nil {
			return 0, err
		}
	}
	stopTelemetry, err := telemetry.Start(ctx, telemetry.Options{Service: "openetld", Attributes: []attribute.KeyValue{attribute.String("openetl.role", "server")}})
	if err != nil {
		return 0, err
	}
	defer func() { _ = stopTelemetry(context.Background()) }()

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

	reg := httptest.NewServer(ggcr.New(ggcr.Logger(log.New(io.Discard, "", 0))))
	defer reg.Close()
	env.registry = strings.TrimPrefix(reg.URL, "http://")

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
				Connectors: env.noConnectors, EgressAllow: egressAllow, SecretsKey: filepath.Join(dir, "secrets.key"),
				BootstrapKeyFile: env.keyFile,
			},
			Roles:     daemon.Roles{Server: true, Orchestrator: true},
			Listener:  ln,
			Ready:     func(string) { close(ready) },
			Workflows: workflows.Workflows{AttemptRetry: time.Second, AttemptHeartbeat: 4 * time.Second, StreamHeartbeat: 4 * time.Second},
		})
	}()
	select {
	case <-ready:
	case err := <-done:
		return 0, fmt.Errorf("server: %w", err)
	case <-time.After(30 * time.Second):
		return 0, errors.New("the server didn't start")
	}
	key, err := auth.ReadKeyFile(env.keyFile)
	if err != nil {
		return 0, fmt.Errorf("the server's owner key: %w", err)
	}
	env.http = withKey(key)
	tok, err := apiv1connect.NewAccessServiceClient(env.http, env.apiURL).CreateRunnerToken(ctx, connect.NewRequest(&apiv1.CreateRunnerTokenRequest{Parent: "dataPlanes/default"}))
	if err != nil {
		return 0, err
	}
	env.runnerToken = tok.Msg.GetSecret()
	env.runner = withKey(env.runnerToken)
	if err := applyOfficialConnectors(ctx); err != nil {
		return 0, err
	}
	// The DSN of the test's database, which configs name as e2e-dsn.
	if _, err := apiv1connect.NewSecretServiceClient(env.http, env.apiURL).UpdateSecret(ctx, connect.NewRequest(&apiv1.UpdateSecretRequest{
		Secret: &apiv1.Secret{Name: "workspaces/default/secrets/e2e-dsn", Value: env.dbURL}, AllowMissing: true,
	})); err != nil {
		return 0, err
	}
	code := m.Run()
	if dir := os.Getenv("OPENETL_RECORD_HISTORIES"); dir != "" {
		if err := recordHistories(ctx, dir); err != nil {
			return code, fmt.Errorf("record histories: %w", err)
		}
	}
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		return code, fmt.Errorf("server: %w", err)
	}
	return code, nil
}

// applyOfficialConnectors packages the official connectors into the test
// registry, for this platform, and applies their descriptors.
func applyOfficialConnectors(ctx context.Context) error {
	c := &registry.Client{Policy: netguard.Policy{Allow: []netip.Prefix{netip.MustParsePrefix(egressAllow)}}}
	req := &apiv1.ApplyRequest{}
	for _, exe := range []string{"source-postgres", "destination-postgres", "destination-iceberg", "destination-s3", "source-kafka", "destination-kafka", "source-kinesis", "destination-kinesis"} {
		path := filepath.Join(env.connectors, exe)
		// Builds from before them have only the Postgres connectors; the
		// others' tests skip.
		if _, err := os.Stat(path); err != nil && !strings.HasSuffix(exe, "-postgres") {
			continue
		}
		_, name, _ := strings.Cut(exe, "-")
		doc, err := connectorhost.Package(ctx, c, env.registry+"/openetl-connectors/"+exe+":e2e", name, "0.1.0", []registry.Executable{{Platform: registry.Platform(), Path: path}})
		if err != nil {
			return fmt.Errorf("package %s: %w", exe, err)
		}
		req.Documents = append(req.Documents, &apiv1.Document{Origin: exe, Content: doc})
	}
	_, err := apiv1connect.NewApplyServiceClient(env.http, env.apiURL).Apply(ctx, connect.NewRequest(req))
	return err
}

// recordHistories writes the history of every workflow that finished, for the
// replay corpus in internal/orchestration/workflows/testdata, with this
// machine's host name replaced.
func recordHistories(ctx context.Context, dir string) error {
	host, err := os.Hostname()
	if err != nil {
		return err
	}
	tc, err := client.DialContext(ctx, client.Options{HostPort: env.temporal, Namespace: env.ns})
	if err != nil {
		return err
	}
	defer tc.Close()
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	req := &workflowservice.ListWorkflowExecutionsRequest{Namespace: env.ns, Query: "ExecutionStatus != 'Running'"}
	for {
		res, err := tc.ListWorkflow(ctx, req)
		if err != nil {
			return err
		}
		for _, w := range res.GetExecutions() {
			h := &historypb.History{}
			it := tc.GetWorkflowHistory(ctx, w.GetExecution().GetWorkflowId(), w.GetExecution().GetRunId(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
			for it.HasNext() {
				ev, err := it.Next()
				if err != nil {
					return err
				}
				h.Events = append(h.Events, ev)
			}
			replaceText(h.ProtoReflect(), host, "host")
			b, err := protojson.MarshalOptions{Indent: "  "}.Marshal(h)
			if err != nil {
				return err
			}
			status := strings.ToLower(strings.TrimPrefix(w.GetStatus().String(), "WORKFLOW_EXECUTION_STATUS_"))
			name := fmt.Sprintf("%s-%s-%s.json", w.GetType().GetName(), status, w.GetExecution().GetRunId())
			if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
				return err
			}
		}
		if len(res.GetNextPageToken()) == 0 {
			return nil
		}
		req.NextPageToken = res.GetNextPageToken()
	}
}

// replaceText replaces old with new in every string and bytes field of m,
// payloads included.
func replaceText(m protoreflect.Message, old, new string) {
	fix := func(fd protoreflect.FieldDescriptor, v protoreflect.Value) (protoreflect.Value, bool) {
		switch fd.Kind() {
		case protoreflect.StringKind:
			return protoreflect.ValueOfString(strings.ReplaceAll(v.String(), old, new)), true
		case protoreflect.BytesKind:
			return protoreflect.ValueOfBytes(bytes.ReplaceAll(v.Bytes(), []byte(old), []byte(new))), true
		case protoreflect.MessageKind, protoreflect.GroupKind:
			replaceText(v.Message(), old, new)
		}
		return v, false
	}
	var fields []protoreflect.FieldDescriptor
	m.Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		fields = append(fields, fd)
		return true
	})
	for _, fd := range fields {
		switch v := m.Get(fd); {
		case fd.IsList():
			l := v.List()
			for i := range l.Len() {
				if nv, set := fix(fd, l.Get(i)); set {
					l.Set(i, nv)
				}
			}
		case fd.IsMap():
			mv := v.Map()
			var keys []protoreflect.MapKey
			mv.Range(func(k protoreflect.MapKey, _ protoreflect.Value) bool {
				keys = append(keys, k)
				return true
			})
			for _, k := range keys {
				if nv, set := fix(fd.MapValue(), mv.Get(k)); set {
					mv.Set(k, nv)
				}
			}
		default:
			if nv, set := fix(fd, v); set {
				m.Set(fd, nv)
			}
		}
	}
}

// bearer sends a key or token with each request.
type bearer string

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+string(b))
	return http.DefaultTransport.RoundTrip(r)
}

func withKey(secret string) *http.Client { return &http.Client{Transport: bearer(secret)} }

// runner is an openetld runner process.
type runner struct {
	cmd  *exec.Cmd
	out  *syncBuffer
	done chan struct{}
}

func startRunner(t *testing.T) *runner {
	t.Helper()
	args := []string{"runner",
		"--api-url", env.apiURL, "--temporal-address", env.temporal, "--temporal-namespace", env.ns,
		"--checkpoints", env.checkpoints, "--engine", env.engine, "--cache", env.cache, "--connectors", env.noConnectors, "--egress-allow", egressAllow, "--log-format", "text"}
	if env.dbt != "" {
		args = append(args, "--dbt", env.dbt)
	}
	cmd := exec.Command(env.openetld, args...)
	cmd.Env = append(os.Environ(), "OPENETL_RUNNER_TOKEN="+env.runnerToken)
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

// engine returns the PID of the engine the runner last started for
// pipeline, once it has one. The runner also picks up streams earlier tests
// left running, so it goes by the pipeline its "engine ready" line names.
func (r *runner) engine(t *testing.T, pipeline string) int {
	t.Helper()
	id := mustQuery[string](t, "SELECT id::text FROM pipelines WHERE name = $1", pipeline)
	ready := regexp.MustCompile(`msg="engine ready" .*pipeline=` + id + ` .* pid=(\d+)`)
	var pid int
	eventually(t, 30*time.Second, "the runner to start an engine for "+pipeline, func() bool {
		m := ready.FindAllStringSubmatch(r.out.String(), -1)
		if len(m) == 0 {
			return false
		}
		pid, _ = strconv.Atoi(m[len(m)-1][1])
		return true
	})
	return pid
}

// child returns the PID of the runner's child process whose name starts
// with name, once it has one. Linux keeps 15 bytes of a name.
func (r *runner) child(t *testing.T, name string) int {
	t.Helper()
	var pid int
	eventually(t, 30*time.Second, "the runner to start "+name, func() bool {
		out, err := exec.Command("pgrep", "-P", fmt.Sprint(r.cmd.Process.Pid), "^"+name).Output()
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
	return applyAll(t, false, docs...).GetResources()
}

// applyAll applies the documents, or only validates them.
func applyAll(t *testing.T, validateOnly bool, docs ...string) *apiv1.ApplyResponse {
	t.Helper()
	req := &apiv1.ApplyRequest{ValidateOnly: validateOnly}
	for i, d := range docs {
		req.Documents = append(req.Documents, &apiv1.Document{Origin: fmt.Sprintf("doc %d", i+1), Content: []byte(d)})
	}
	res, err := apiv1connect.NewApplyServiceClient(env.http, env.apiURL).Apply(t.Context(), connect.NewRequest(req))
	if err != nil {
		t.Fatal(err)
	}
	return res.Msg
}

// setSecret stores value as a new secret, and returns its name.
func setSecret(t *testing.T, value string) string {
	t.Helper()
	name := unique("secret")
	_, err := apiv1connect.NewSecretServiceClient(env.http, env.apiURL).UpdateSecret(t.Context(), connect.NewRequest(&apiv1.UpdateSecretRequest{
		Secret: &apiv1.Secret{Name: "workspaces/default/secrets/" + name, Value: value}, AllowMissing: true,
	}))
	if err != nil {
		t.Fatal(err)
	}
	return name
}

// unique makes names unique per run, so -count=n works.
func unique(name string) string { return name + "-" + strings.ToLower(rand.Text())[:6] }

func schema(name string) string { return strings.ReplaceAll(name, "-", "_") }

func pipelineDocs(name string, faker, delivery string) []string {
	return []string{
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q},
			"spec": {"connector": "faker", "config": %s}}`, name, faker),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q},
			"spec": {"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}}`, name),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
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
	pid := r.engine(t, name)
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
	engine := first.engine(t, name)
	dst := first.child(t, "destination-pos")
	eventually(t, 30*time.Second, "a committed epoch", func() bool { return committedEpochs(t, schema(name)) > 0 })
	first.kill(t)
	eventually(t, 10*time.Second, "the orphaned engine and destination to exit", func() bool {
		return errors.Is(syscall.Kill(engine, 0), syscall.ESRCH) && errors.Is(syscall.Kill(dst, 0), syscall.ESRCH)
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
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q}, "spec": %s}`, name, src),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q}, "spec": %s}`, name, dst),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
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
	src := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "chunkRows": 1000}}`, schema(name))
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
	pid := r.engine(t, name)
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

// postgresSource binds the external Postgres source and starts it.
func postgresSource(t *testing.T, config []byte) source.Source {
	t.Helper()
	s, err := connectorhost.NewSource("postgres", config, connectorhost.Env{Connectors: env.connectors})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connectorhost.Close(s) })
	if err := connectorhost.Start(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	return s
}

// postgresDestination binds the external Postgres destination and starts
// it.
func postgresDestination(t *testing.T, config []byte) destination.Destination {
	t.Helper()
	d, err := connectorhost.NewDestination("postgres", config, connectorhost.Env{Connectors: env.connectors})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connectorhost.Close(d) })
	if err := connectorhost.Start(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	return d
}
