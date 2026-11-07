//go:build e2e

package e2e

import (
	"context"
	"crypto/rand"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/internal/auth"
	"github.com/openetl-io/openetl/internal/orchestration/dbosbackend"
	"github.com/openetl-io/openetl/internal/store/storetest"
)

// controlPlane is a control plane of its own, out of processes that can be
// killed, on a database of its own. Its server and orchestrators take flags,
// such as which backend to use.
type controlPlane struct {
	t      *testing.T
	dir    string
	addr   string
	system string
}

// separateControlPlane makes one, and points the test's helpers at it. Its API
// server is started by the caller, which then calls connect.
func separateControlPlane(t *testing.T) *controlPlane {
	t.Helper()
	ctx := t.Context()
	saved := env
	t.Cleanup(func() { env = saved })
	dbURL, drop, err := storetest.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(drop)
	t.Cleanup(func() { dropDBOS(dbURL) })
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	env.dbURL, env.apiURL, env.keyFile = dbURL, "http://"+addr, filepath.Join(dir, "admin.key")
	system, err := dbosbackend.SystemDatabaseURL(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	return &controlPlane{t: t, dir: dir, addr: addr, system: system}
}

func (c *controlPlane) server(flags ...string) *runner {
	c.t.Helper()
	return startOpenetld(c.t, nil, append([]string{"server", "--database-url", env.dbURL, "--listen", c.addr, "--secrets-key", filepath.Join(c.dir, "secrets.key"),
		"--bootstrap-key-file", env.keyFile, "--connectors", env.noConnectors, "--egress-allow", egressAllow, "--sign-in-rate", "1000",
		"--official-connectors="}, flags...)...)
}

func (c *controlPlane) orchestrator(id string, vars []string, flags ...string) *runner {
	c.t.Helper()
	return startOpenetld(c.t, vars, append([]string{"orchestrator", "--database-url", env.dbURL, "--executor-id", id,
		"--secrets-key", filepath.Join(c.dir, "secrets.key"), "--bootstrap-key-file", env.keyFile}, flags...)...)
}

// connect waits for the first server, and sets up what the test's helpers need.
func (c *controlPlane) connect() {
	c.t.Helper()
	ctx := c.t.Context()
	eventually(c.t, 60*time.Second, "the server to start", func() bool {
		key, err := auth.ReadKeyFile(env.keyFile)
		if err != nil {
			return false
		}
		env.http = withKey(key)
		tok, err := apiv1connect.NewAccessServiceClient(env.http, env.apiURL).CreateRunnerToken(ctx, connect.NewRequest(&apiv1.CreateRunnerTokenRequest{Parent: "dataPlanes/default"}))
		if err != nil {
			return false
		}
		env.runnerToken = tok.Msg.GetSecret()
		return true
	})
	env.runner = withKey(env.runnerToken)
	if err := applyOfficialConnectors(ctx); err != nil {
		c.t.Fatal(err)
	}
	if _, err := apiv1connect.NewSecretServiceClient(env.http, env.apiURL).UpdateSecret(ctx, connect.NewRequest(&apiv1.UpdateSecretRequest{
		Secret: &apiv1.Secret{Name: "workspaces/default/secrets/e2e-dsn", Value: env.dbURL}, AllowMissing: true,
	})); err != nil {
		c.t.Fatal(err)
	}
}

// executorOf is the orchestrator a workflow's row is running on, if any.
func (c *controlPlane) executorOf(row string) string {
	c.t.Helper()
	conn, err := pgx.Connect(c.t.Context(), c.system)
	if err != nil {
		c.t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var executor string
	if err := conn.QueryRow(c.t.Context(), `SELECT executor_id FROM dbos.workflow_status WHERE workflow_uuid = $1 AND status = 'PENDING'`, row).Scan(&executor); err != nil {
		return ""
	}
	return executor
}

func requireDBOS(t *testing.T) {
	t.Helper()
	if env.orchestrator != "dbos" {
		t.Skip("it's about DBOS's replicas")
	}
}

const longFaker = `{"seed": 4, "count": 20000, "batchSize": 50, "delay": "100ms"}`

// An orchestrator killed mid-sync: another recovers the sync's workflow within
// a minute, and it carries on with the same attempt.
func TestOrchestratorKilledMidSync(t *testing.T) {
	requireDBOS(t)
	cp := separateControlPlane(t)
	cp.server()
	cp.connect()
	replicas := map[string]*runner{"replica-a": cp.orchestrator("replica-a", nil), "replica-b": cp.orchestrator("replica-b", nil)}
	startRunner(t)
	name := unique("orchestrator-killed")
	// Longer than the recovery takes, so it's still running when it's recovered.
	apply(t, pipelineDocs(name, longFaker, `{"commit": {"interval": "1s"}}`)...)
	eventually(t, 60*time.Second, "a committed epoch", func() bool { return committedEpochs(t, schema(name)) > 0 })
	row := mustQuery[string](t, `SELECT s.run_id FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id WHERE p.name = $1`, name)
	var victim string
	eventually(t, 30*time.Second, "the sync's executor", func() bool {
		victim = cp.executorOf(row)
		return victim != ""
	})
	replicas[victim].kill(t)
	killed := time.Now()
	eventually(t, time.Minute, "another orchestrator to recover the sync", func() bool {
		e := cp.executorOf(row)
		return e != "" && e != victim
	})
	t.Logf("recovered after %v", time.Since(killed).Round(time.Second))
	if status, stats := waitSync(t, name, 3*time.Minute); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkRows(t, schema(name), 20000, "users", "purchases")
	if n := mustQuery[int](t, `SELECT count(*) FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id WHERE p.name = $1`, name); n != 1 {
		t.Errorf("%d syncs, want the one carrying on", n)
	}
	if n := mustQuery[int](t, `SELECT count(*) FROM attempts a JOIN pipelines p ON p.id = a.pipeline_id WHERE p.name = $1`, name); n != 1 {
		t.Errorf("%d attempts, want the one carrying on", n)
	}
}

// An upgrade with workflows of the old version in flight: the new version
// can't run them, so it cancels them and starts them again. A supervisor takes
// over its job, and a sync the next starts carries on from the checkpoint log.
func TestUpgradeWithWorkflowsInFlight(t *testing.T) {
	requireDBOS(t)
	cp := separateControlPlane(t)
	cp.server()
	cp.connect()
	old := cp.orchestrator("orchestrator-old", []string{"DBOS__APPVERSION=old"})
	startRunner(t)
	stream := unique("upgraded-stream")
	startContinuous(t, stream)
	eventually(t, 60*time.Second, "the stream's committed epoch", func() bool { return committedEpochs(t, schema(stream)) > 0 })
	scheduled := unique("upgraded-sync")
	apply(t, pipelineDocs(scheduled, longFaker, `{"commit": {"interval": "1s"}}`)...)
	eventually(t, 60*time.Second, "the sync's committed epoch", func() bool { return committedEpochs(t, schema(scheduled)) > 0 })

	old.kill(t)
	cp.orchestrator("orchestrator-new", []string{"DBOS__APPVERSION=new"})

	before := committedEpochs(t, schema(stream))
	eventually(t, time.Minute, "the sync to be started again", func() bool {
		return mustQuery[int](t, `SELECT count(*) FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id WHERE p.name = $1`, scheduled) == 2
	})
	if status, stats := waitSync(t, scheduled, 3*time.Minute); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkRows(t, schema(scheduled), 20000, "users", "purchases")

	id := pipelineID(t, stream)
	eventually(t, time.Minute, "the stream to carry on", func() bool { return committedEpochs(t, schema(stream)) > before+2 })
	if got := statuses(runs(t, stream)); got != "running" {
		t.Errorf("runs %q, want the one still running", got)
	}
	if n := mustQuery[int](t, `SELECT count(*) FROM attempts WHERE pipeline_id = $1`, id); n != 1 {
		t.Errorf("%d jobs, want the same one still running", n)
	}
	// The new version's supervisor answers for it.
	apply(t, continuousDocs(stream, `{"seed": 4, "endless": true, "batchSize": 10, "delay": "200ms"}`)...)
	eventually(t, 60*time.Second, "the run to stop and another to start", func() bool { return statuses(runs(t, stream)) == "stopped running" })
}

// Switching an install from one backend to the other and back: nothing carries
// over but the database, and the reconcile rebuilds what runs its pipelines. A
// supervisor takes over its job, and a schedule goes on. Going back to a backend
// means clearing what it had, whose workflows would otherwise wake up with an
// old view of their pipelines.
func TestSwitchingOrchestrators(t *testing.T) {
	if env.orchestrator != "temporal" {
		t.Skip("it needs Temporal, which the nightly run has")
	}
	ns := "openetl-e2e-" + strings.ToLower(rand.Text())[:10]
	tc, err := client.DialContext(t.Context(), client.Options{HostPort: env.temporal})
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Close()
	if _, err := tc.WorkflowService().RegisterNamespace(t.Context(), &workflowservice.RegisterNamespaceRequest{
		Namespace: ns, WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = tc.OperatorService().DeleteNamespace(context.Background(), &operatorservice.DeleteNamespaceRequest{Namespace: ns})
	}()
	cp := separateControlPlane(t)
	onDBOS := []string{"--orchestrator", "dbos"}
	onTemporal := []string{"--orchestrator", "temporal", "--temporal-address", env.temporal, "--temporal-namespace", ns}
	server, orchestrator := cp.server(onDBOS...), cp.orchestrator("switching", nil, onDBOS...)
	cp.connect()
	startRunner(t)
	stream := unique("switched-stream")
	startContinuous(t, stream)
	eventually(t, 60*time.Second, "the stream's committed epoch", func() bool { return committedEpochs(t, schema(stream)) > 0 })
	id := pipelineID(t, stream)
	jobs := func() int { return mustQuery[int](t, `SELECT count(*) FROM attempts WHERE pipeline_id = $1`, id) }
	scheduled := unique("switched-sync")
	apply(t, pipelineDocs(scheduled, `{"seed": 5, "count": 10}`, `{}`)...)
	if status, stats := waitSync(t, scheduled, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}

	switchTo := func(flags []string, first func()) {
		t.Helper()
		orchestrator.kill(t)
		server.kill(t)
		first()
		server, orchestrator = cp.server(flags...), cp.orchestrator("switching", nil, flags...)
		eventually(t, 60*time.Second, "the server to start again", func() bool {
			_, err := pipelines().GetPipeline(t.Context(), connect.NewRequest(&apiv1.GetPipelineRequest{Name: "workspaces/default/pipelines/" + stream}))
			return err == nil
		})
		before := committedEpochs(t, schema(stream))
		eventually(t, 2*time.Minute, "the stream to carry on", func() bool { return committedEpochs(t, schema(stream)) > before+2 })
		// What runs it now answers for it: syncing a scheduled pipeline.
		eventually(t, time.Minute, "the schedule to take a sync", func() bool {
			_, err := pipelines().SyncPipeline(t.Context(), connect.NewRequest(&apiv1.SyncPipelineRequest{Name: "workspaces/default/pipelines/" + scheduled}))
			return err == nil
		})
		if status, stats := waitSync(t, scheduled, 2*time.Minute); status != "succeeded" {
			t.Fatalf("sync %s: %s", status, stats)
		}
	}
	switchTo(onTemporal, func() {})
	if got := statuses(runs(t, stream)); got != "running" {
		t.Errorf("on Temporal: runs %q, want the one still running", got)
	}
	if n := jobs(); n != 1 {
		t.Errorf("on Temporal: %d jobs, want the same one still running", n)
	}
	// A new version shows the supervisor there answers for its job.
	apply(t, continuousDocs(stream, `{"seed": 4, "endless": true, "batchSize": 10, "delay": "200ms"}`)...)
	eventually(t, 60*time.Second, "the run to stop and another to start", func() bool { return statuses(runs(t, stream)) == "stopped running" })

	switchTo(onDBOS, func() { dropDBOS(env.dbURL) })
	if got := statuses(runs(t, stream)); got != "stopped running" {
		t.Errorf("on DBOS again: runs %q", got)
	}
	apply(t, continuousDocs(stream, endlessFaker)...)
	eventually(t, 60*time.Second, "the run to stop and another to start", func() bool { return statuses(runs(t, stream)) == "stopped stopped running" })
}

// The small install: every role in one process, on Postgres alone.
func TestDevMode(t *testing.T) {
	cp := separateControlPlane(t)
	startOpenetld(t, nil, "dev", "--database-url", env.dbURL, "--listen", cp.addr, "--secrets-key", filepath.Join(cp.dir, "secrets.key"),
		"--bootstrap-key-file", env.keyFile, "--connectors", env.noConnectors, "--egress-allow", egressAllow,
		"--checkpoints", env.checkpoints, "--cache", env.cache, "--engine", env.engine, "--official-connectors=")
	cp.connect()
	name := unique("dev-mode")
	apply(t, pipelineDocs(name, `{"seed": 5, "count": 100}`, `{}`)...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkRows(t, schema(name), 100, "users", "purchases")
}
