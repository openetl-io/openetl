// Package daemon runs openetld's roles: the API server, the orchestrator
// and the runner, alone or together.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"golang.org/x/sync/errgroup"

	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/unsuitable001/openetl/internal/api"
	"github.com/unsuitable001/openetl/internal/auth"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/checkpoint/openstore"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/engineclient"
	"github.com/unsuitable001/openetl/internal/netguard"
	cpactivities "github.com/unsuitable001/openetl/internal/orchestration/activities"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/orchestration/ops"
	"github.com/unsuitable001/openetl/internal/orchestration/schedules"
	"github.com/unsuitable001/openetl/internal/orchestration/workflows"
	"github.com/unsuitable001/openetl/internal/platform/config"
	"github.com/unsuitable001/openetl/internal/platform/temporalx"
	"github.com/unsuitable001/openetl/internal/registry"
	dpactivities "github.com/unsuitable001/openetl/internal/runner/activities"
	"github.com/unsuitable001/openetl/internal/secrets"
	"github.com/unsuitable001/openetl/internal/store"
	"github.com/unsuitable001/openetl/internal/store/db"
)

type Roles struct {
	Server, Orchestrator, Runner bool
}

type Options struct {
	Config config.Config
	Roles  Roles
	Logger *slog.Logger
	// The API server's listener. Nil: listen on Config.Listen.
	Listener net.Listener
	// Called with the API's address once it is serving.
	Ready     func(addr string)
	Workflows workflows.Workflows
}

// Run runs the roles until ctx ends or one of them fails.
func Run(ctx context.Context, o Options) error {
	log := o.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	cfg := o.Config
	tc, err := temporalx.Dial(ctx, temporalx.Options{Address: cfg.TemporalAddress, Namespace: cfg.TemporalNamespace, Logger: log})
	if err != nil {
		return err
	}
	defer tc.Close()

	var st *store.Store
	if o.Roles.Server || o.Roles.Orchestrator {
		if err := temporalx.EnsureSearchAttributes(ctx, tc, cfg.TemporalNamespace, contract.SearchAttributes); err != nil {
			return err
		}
		if st, err = store.Open(ctx, cfg.DatabaseURL); err != nil {
			return err
		}
		defer st.Close()
		if err := st.Migrate(ctx); err != nil {
			return err
		}
	}

	g, gctx := errgroup.WithContext(ctx)
	if o.Roles.Server {
		ln := o.Listener
		if ln == nil {
			var lc net.ListenConfig
			if ln, err = lc.Listen(ctx, "tcp", cfg.Listen); err != nil {
				return fmt.Errorf("daemon: listen: %w", err)
			}
		}
		if o.Roles.Runner {
			// The runner in this process calls the API it serves.
			cfg.APIURL = "http://" + ln.Addr().String()
		}
		// A new org has no owner, and nor does one from before API keys.
		tenant, err := st.Bootstrap(ctx, cfg.DataPlane, func(q *db.Queries, org uuid.UUID) error {
			owners, err := q.LockOrgOwners(ctx, org)
			if err != nil || len(owners) > 0 {
				return err
			}
			return writeOwnerKey(ctx, q, org, cfg.BootstrapKeyFile, "openetld bootstrap", log)
		})
		if err != nil {
			_ = ln.Close()
			return err
		}
		if o.Roles.Runner && cfg.RunnerToken == "" && cfg.TokenFile == "" {
			// The runner in this process gets a token for as long as it runs.
			token, id, err := store.CreateRunnerToken(ctx, st.Queries, tenant.Org, cfg.DataPlane)
			if err != nil {
				_ = ln.Close()
				return err
			}
			defer func() {
				_, _ = st.DeleteRunnerToken(context.WithoutCancel(ctx), db.DeleteRunnerTokenParams{OrgID: tenant.Org, DataPlaneID: cfg.DataPlane, ID: id})
			}()
			cfg.RunnerToken = token.Secret
		}
		allow, err := netguard.ParsePrefixes(cfg.EgressAllow)
		if err != nil {
			_ = ln.Close()
			return fmt.Errorf("daemon: --egress-allow: %w", err)
		}
		keyring, err := openKeyring(ctx, cfg.SecretsKey, st)
		if err != nil {
			_ = ln.Close()
			return err
		}
		if keyring != nil {
			defer func() { _ = keyring.Close() }()
		}
		srv := &api.Server{
			Store: st, Tenant: tenant, Schedules: &schedules.Schedules{Client: tc}, Ops: &ops.Ops{Client: tc}, Logs: &ops.Ops{Client: tc},
			DefaultDataPlane: cfg.DataPlane, Connectors: connectorhost.Env{Connectors: connectorsDir(cfg)},
			Registry: &registry.Client{Policy: netguard.Policy{Allow: allow}},
			Secrets:  keyring,
			Logger:   log.With("role", "server"),
		}
		g.Go(func() error { return serve(gctx, ln, srv.Handler(), o.Ready, log) })
	}
	if o.Roles.Orchestrator {
		g.Go(func() error { return orchestrate(gctx, tc, st, o.Workflows) })
	}
	if o.Roles.Runner {
		g.Go(func() error { return runRunner(gctx, cfg, tc, log.With("role", "runner")) })
	}
	return g.Wait()
}

// writeOwnerKey makes the bootstrap principal an owner with a new key, and
// writes the key to path. method is what the audit log calls it.
func writeOwnerKey(ctx context.Context, q *db.Queries, org uuid.UUID, path, method string, log *slog.Logger) error {
	if path == "" {
		return errors.New("daemon: no bootstrap key file to write the owner key to")
	}
	key, err := store.EnsureOwner(ctx, q, org, method)
	if err != nil {
		return err
	}
	if err := auth.WriteKeyFile(path, key); err != nil {
		return fmt.Errorf("daemon: write the owner key: %w", err)
	}
	log.Info("wrote an owner key", "principal", "principals/"+store.BootstrapPrincipal, "file", path)
	return nil
}

// BootstrapKey gives the bootstrap principal a new owner key, for an
// operator who has lost every owner's.
func BootstrapKey(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return err
	}
	org, err := st.FirstOrg(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("daemon: there's no org yet; openetld server creates it, with an owner key, on its first start")
	}
	if err != nil {
		return err
	}
	return st.Tx(ctx, func(q *db.Queries) error {
		return writeOwnerKey(ctx, q, org.ID, cfg.BootstrapKeyFile, "openetld bootstrap-key", log)
	})
}

// openKeyring opens the secrets key, if there is one, and checks that it
// unwraps every org's data key.
func openKeyring(ctx context.Context, ref string, st *store.Store) (*secrets.Keyring, error) {
	if ref == "" {
		return nil, nil
	}
	orgs, err := st.ListOrgs(ctx)
	if err != nil {
		return nil, err
	}
	return secrets.Open(ctx, ref, st, orgs)
}

func serve(ctx context.Context, ln net.Listener, h http.Handler, ready func(string), log *slog.Logger) error {
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
	srv.Protocols = new(http.Protocols)
	srv.Protocols.SetHTTP1(true)
	srv.Protocols.SetUnencryptedHTTP2(true)
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	log.Info("API serving", "addr", ln.Addr().String())
	if ready != nil {
		ready(ln.Addr().String())
	}
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		return err
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func orchestrate(ctx context.Context, tc client.Client, st *store.Store, wf workflows.Workflows) error {
	sched := &schedules.Schedules{Client: tc}
	if err := sched.EnsureRetention(ctx, contract.RetentionInput{CheckpointHistory: contract.DefaultCheckpointHistory}); err != nil {
		return err
	}
	w := worker.New(tc, contract.OrchestratorQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(wf.Sync, workflow.RegisterOptions{Name: contract.SyncWorkflow})
	w.RegisterWorkflowWithOptions(wf.ConnectorOp, workflow.RegisterOptions{Name: contract.ConnectorOpWorkflow})
	w.RegisterWorkflowWithOptions(wf.Retention, workflow.RegisterOptions{Name: contract.RetentionWorkflow})
	w.RegisterWorkflowWithOptions(wf.ReadLogs, workflow.RegisterOptions{Name: contract.ReadLogsWorkflow})
	acts := &cpactivities.Activities{Store: st}
	w.RegisterActivityWithOptions(acts.StartSync, activity.RegisterOptions{Name: contract.StartSync})
	w.RegisterActivityWithOptions(acts.FinishSync, activity.RegisterOptions{Name: contract.FinishSync})
	w.RegisterActivityWithOptions(acts.DropMirrorPartitions, activity.RegisterOptions{Name: contract.DropMirrorPartitions})
	return runWorker(ctx, w)
}

func runRunner(ctx context.Context, cfg config.Config, tc client.Client, log *slog.Logger) error {
	cps, err := openstore.Open(ctx, cfg.Checkpoints)
	if err != nil {
		return err
	}
	if err := checkpoint.Probe(ctx, cps, ""); err != nil {
		return fmt.Errorf("daemon: the checkpoint store fails the conditional-write probe, so the runner won't start: %w", err)
	}
	allow, err := netguard.ParsePrefixes(cfg.EgressAllow)
	if err != nil {
		return fmt.Errorf("daemon: --egress-allow: %w", err)
	}
	connectors := connectorhost.Env{Egress: netguard.Policy{Allow: allow}, Connectors: connectorsDir(cfg), Logger: log.With("component", "connector")}
	token := cfg.RunnerToken
	if cfg.TokenFile != "" {
		if token, err = auth.ReadKeyFile(cfg.TokenFile); err != nil {
			return fmt.Errorf("daemon: --token-file: %w", err)
		}
	}
	if token == "" {
		return errors.New("daemon: the runner needs its data plane's token, in --token-file or OPENETL_RUNNER_TOKEN; openetl dataplane token create makes one")
	}
	if !auth.IsRunnerToken(token) {
		return errors.New("daemon: the runner's token isn't a runner token; openetl dataplane token create makes one")
	}
	apiClient := dataplanev1connect.NewDataPlaneServiceClient(http.DefaultClient, cfg.APIURL, auth.WithBearer(token)) //nolint:forbidigo // the operator configures the API's address
	versions := &connectorhost.Fetcher{
		Artifact: dpactivities.Artifacts(apiClient),
		Registry: &registry.Client{Policy: netguard.Policy{Allow: allow}},
		Cache:    cfg.Cache,
	}
	sync := &dpactivities.Sync{
		API:         apiClient,
		Checkpoints: cps,
		Storage:     cfg.Checkpoints,
		Engine:      engineclient.Options{Binary: cfg.Engine, Logger: log.With("component", "engine")},
		Logger:      log,
		Connectors:  connectors,
		Versions:    versions,
		Logs:        cps,
	}
	w := worker.New(tc, contract.BatchQueue(cfg.DataPlane), worker.Options{
		MaxConcurrentActivityExecutionSize: 4,
		// Attempts drain to a checkpoint for up to this long.
		WorkerStopTimeout: 2 * time.Minute,
	})
	w.RegisterActivityWithOptions(sync.RunSyncAttempt, activity.RegisterOptions{Name: contract.RunSyncAttempt})
	dbt := &dpactivities.Dbt{API: apiClient, Artifacts: versions, Executable: cfg.Dbt, Logger: log, Logs: cps}
	w.RegisterActivityWithOptions(dbt.RunDbt, activity.RegisterOptions{Name: contract.RunDbt})
	connectorOps := &dpactivities.Ops{API: apiClient, Connectors: connectors, Versions: versions}
	w.RegisterActivityWithOptions(connectorOps.CheckConnector, activity.RegisterOptions{Name: contract.CheckConnector})
	w.RegisterActivityWithOptions(connectorOps.DiscoverSource, activity.RegisterOptions{Name: contract.DiscoverSource})
	logs := worker.New(tc, contract.LogsQueue(cfg.DataPlane), worker.Options{MaxConcurrentActivityExecutionSize: 8})
	logs.RegisterActivityWithOptions((&dpactivities.Logs{Store: cps}).ReadLogs, activity.RegisterOptions{Name: contract.ReadLogs})
	log.Info("runner polling", "queues", []string{contract.BatchQueue(cfg.DataPlane), contract.LogsQueue(cfg.DataPlane)}, "checkpoints", cfg.Checkpoints)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return runWorker(gctx, w) })
	g.Go(func() error { return runWorker(gctx, logs) })
	return g.Wait()
}

// connectorsDir is where external connectors are: connectors/ next to
// openetld unless configured.
func connectorsDir(cfg config.Config) string {
	if cfg.Connectors != "" {
		return cfg.Connectors
	}
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	return filepath.Join(filepath.Dir(exe), "connectors")
}

func runWorker(ctx context.Context, w worker.Worker) error {
	if err := w.Start(); err != nil {
		return err
	}
	<-ctx.Done()
	w.Stop()
	return nil
}
