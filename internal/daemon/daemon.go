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
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
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
	"github.com/unsuitable001/openetl/internal/bridge"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/checkpoint/openstore"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/connectorhost/external"
	"github.com/unsuitable001/openetl/internal/engineclient"
	"github.com/unsuitable001/openetl/internal/flink"
	"github.com/unsuitable001/openetl/internal/netguard"
	cpactivities "github.com/unsuitable001/openetl/internal/orchestration/activities"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/orchestration/dispatch"
	"github.com/unsuitable001/openetl/internal/orchestration/ops"
	"github.com/unsuitable001/openetl/internal/orchestration/schedules"
	"github.com/unsuitable001/openetl/internal/orchestration/workflows"
	"github.com/unsuitable001/openetl/internal/platform/config"
	"github.com/unsuitable001/openetl/internal/platform/resources"
	"github.com/unsuitable001/openetl/internal/platform/temporalx"
	dpactivities "github.com/unsuitable001/openetl/internal/runner/activities"
	dpworker "github.com/unsuitable001/openetl/internal/runner/worker"
	"github.com/unsuitable001/openetl/internal/secrets"
	"github.com/unsuitable001/openetl/internal/secretstore"
	"github.com/unsuitable001/openetl/internal/store"
	"github.com/unsuitable001/openetl/internal/store/db"
	"github.com/unsuitable001/openetl/internal/webhooks"
	"github.com/unsuitable001/openetl/internal/webui"
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
	// The dispatcher's timings; zero means production's.
	Dispatch dispatch.Options
	// Changes the server's notifier before it starts, for tests.
	Webhooks func(*webhooks.Notifier)
}

// Run runs the roles until ctx ends or one of them fails.
func Run(ctx context.Context, o Options) error {
	log := o.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	cfg := o.Config
	var (
		tc   client.Client
		st   *store.Store
		work *dispatch.Dispatcher
		err  error
	)
	// A runner reaches only the API (ADR-0028).
	if o.Roles.Server || o.Roles.Orchestrator {
		tc, err = temporalx.Dial(ctx, temporalx.Options{Address: cfg.TemporalAddress, Namespace: cfg.TemporalNamespace, Logger: log})
		if err != nil {
			return err
		}
		defer tc.Close()
		if err := temporalx.EnsureSearchAttributes(ctx, tc, cfg.TemporalNamespace, temporalx.SearchAttributes); err != nil {
			return err
		}
		if st, err = store.Open(ctx, cfg.DatabaseURL); err != nil {
			return err
		}
		defer st.Close()
		if err := st.Migrate(ctx); err != nil {
			return err
		}
		work = &dispatch.Dispatcher{Store: st, Waker: &ops.Waker{Client: tc}, Options: o.Dispatch, Logger: log.With("component", "dispatch")}
	}

	g, gctx := errgroup.WithContext(ctx)
	// A runner in this process drains through the API, so it stops first.
	cpctx := gctx
	if o.Roles.Runner && (o.Roles.Server || o.Roles.Orchestrator) {
		var stop context.CancelFunc
		cpctx, stop = context.WithCancel(context.WithoutCancel(gctx))
		defer stop()
		g.Go(func() error {
			defer stop()
			return runRunner(gctx, cfg, log.With("role", "runner"))
		})
	} else if o.Roles.Runner {
		g.Go(func() error { return runRunner(gctx, cfg, log.With("role", "runner")) })
	}
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
		lagging, err := time.ParseDuration(cfg.LaggingAfter)
		if cfg.LaggingAfter != "" && (err != nil || lagging <= 0) {
			_ = ln.Close()
			return fmt.Errorf("daemon: --lagging-after %q isn't a positive duration", cfg.LaggingAfter)
		}
		srv := &api.Server{
			Store: st, Tenant: tenant, Schedules: &schedules.Schedules{Client: tc}, Ops: &ops.Ops{Client: tc}, Logs: &ops.Ops{Client: tc}, Dispatch: work,
			DefaultDataPlane: cfg.DataPlane, Connectors: connectorhost.Env{Connectors: connectorsDir(cfg)},
			Downloads:    (&netguard.Policy{Allow: allow}).Client(),
			Secrets:      keyring,
			LaggingAfter: lagging,
			Logger:       log.With("role", "server"),
		}
		if err := configureSignIn(srv, cfg, ln.Addr(), log); err != nil {
			_ = ln.Close()
			return err
		}
		h, err := srv.Handler()
		if err != nil {
			_ = ln.Close()
			return err
		}
		reg, err := srv.RegisterMetrics()
		if err != nil {
			_ = ln.Close()
			return err
		}
		defer func() { _ = reg.Unregister() }()
		if cfg.Web != "" {
			if h, err = webui.Handler(cfg.Web, h); err != nil {
				_ = ln.Close()
				return err
			}
		}
		g.Go(func() error { return work.Listen(cpctx) })
		g.Go(func() error { return serve(cpctx, ln, h, o.Ready, log) })
		notifier := &webhooks.Notifier{
			Store: st, Org: tenant.Org, Secrets: keyring, Client: webhooks.Client(netguard.Policy{Allow: allow}),
			Logger: log.With("role", "server", "component", "webhooks"),
		}
		if o.Webhooks != nil {
			o.Webhooks(notifier)
		}
		g.Go(func() error { return notifier.Run(cpctx) })
	}
	if o.Roles.Orchestrator {
		reg, err := work.RegisterMetrics()
		if err != nil {
			return err
		}
		defer func() { _ = reg.Unregister() }()
		g.Go(func() error { return orchestrate(cpctx, cfg, tc, st, work, o.Workflows) })
		g.Go(func() error { return work.Run(cpctx) })
	}
	return g.Wait()
}

// configureSignIn sets the server's sign-in from cfg (ADR-0020). It refuses
// --auth none on an address that isn't loopback, unless that's allowed.
func configureSignIn(srv *api.Server, cfg config.Config, addr net.Addr, log *slog.Logger) error {
	host, port := "localhost", ""
	loopback := false
	if a, ok := addr.(*net.TCPAddr); ok {
		loopback, port = a.IP.IsLoopback(), strconv.Itoa(a.Port)
		if !loopback && !a.IP.IsUnspecified() {
			host = a.IP.String()
		}
	}
	srv.Authless = cfg.Auth == "none"
	switch {
	case srv.Authless && !loopback && !cfg.AuthNoneAllowRemote:
		return fmt.Errorf("daemon: --auth none on %s, which isn't a loopback address, would make anyone who can reach it an owner; add --auth-none-allow-remote if that's what you want", addr)
	case srv.Authless && !loopback:
		log.Warn("sign-in is off on an address that isn't loopback: anyone who can reach this server acts as an owner", "listen", addr.String())
	case srv.Authless:
		log.Warn("sign-in is off: anyone who can reach this server acts as an owner", "listen", addr.String())
	}
	if cfg.SignInRate < 0 {
		return fmt.Errorf("daemon: --sign-in-rate is %d; it can't be negative", cfg.SignInRate)
	}
	srv.SignInRate = cfg.SignInRate
	srv.PublicURL = cfg.PublicURL
	if srv.PublicURL == "" {
		srv.PublicURL = "http://" + net.JoinHostPort(host, port)
	}
	u, err := url.Parse(srv.PublicURL)
	if err != nil || u.Scheme != "http" && u.Scheme != "https" || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("daemon: --public-url %q isn't an http or https URL", cfg.PublicURL)
	}
	if ip := net.ParseIP(u.Hostname()); u.Scheme == "http" && !srv.Authless && u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		log.Warn("browsers reach this server over plain http, so passwords and session cookies cross the network unencrypted; put TLS in front of it and set --public-url to its https URL", "public_url", srv.PublicURL)
	}
	for _, d := range []struct {
		flag, value string
		dst         *time.Duration
	}{{"session-idle-timeout", cfg.SessionIdleTimeout, &srv.SessionIdle}, {"session-max-age", cfg.SessionMaxAge, &srv.SessionMaxAge}} {
		if d.value == "" {
			continue
		}
		v, err := time.ParseDuration(d.value)
		if err != nil || v <= 0 {
			return fmt.Errorf("daemon: --%s %q isn't a positive duration", d.flag, d.value)
		}
		*d.dst = v
	}
	return nil
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

func orchestrate(ctx context.Context, cfg config.Config, tc client.Client, st *store.Store, work *dispatch.Dispatcher, wf workflows.Workflows) error {
	sched := &schedules.Schedules{Client: tc}
	retention := contract.RetentionInput{
		CheckpointHistory: contract.DefaultCheckpointHistory,
		AuditHistory:      time.Duration(cfg.AuditRetentionDays) * 24 * time.Hour,
	}
	if err := sched.EnsureRetention(ctx, retention); err != nil {
		return err
	}
	w := worker.New(tc, contract.OrchestratorQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(wf.Sync, workflow.RegisterOptions{Name: contract.SyncWorkflow})
	w.RegisterWorkflowWithOptions(wf.StreamSupervisor, workflow.RegisterOptions{Name: contract.StreamSupervisorWorkflow})
	w.RegisterWorkflowWithOptions(wf.ConnectorOp, workflow.RegisterOptions{Name: contract.ConnectorOpWorkflow})
	w.RegisterWorkflowWithOptions(wf.Retention, workflow.RegisterOptions{Name: contract.RetentionWorkflow})
	w.RegisterWorkflowWithOptions(wf.ReadLogs, workflow.RegisterOptions{Name: contract.ReadLogsWorkflow})
	acts := &cpactivities.Activities{Store: st, Schedules: sched, Dispatch: work}
	w.RegisterActivityWithOptions(acts.StartSync, activity.RegisterOptions{Name: contract.StartSync})
	w.RegisterActivityWithOptions(acts.FinishSync, activity.RegisterOptions{Name: contract.FinishSync})
	w.RegisterActivityWithOptions(acts.GetStreamState, activity.RegisterOptions{Name: contract.GetStreamState})
	w.RegisterActivityWithOptions(acts.StartRun, activity.RegisterOptions{Name: contract.StartRun})
	w.RegisterActivityWithOptions(acts.EnqueueWork, activity.RegisterOptions{Name: contract.EnqueueWork})
	w.RegisterActivityWithOptions(acts.GetWork, activity.RegisterOptions{Name: contract.GetWork})
	w.RegisterActivityWithOptions(acts.CancelWork, activity.RegisterOptions{Name: contract.CancelWork})
	w.RegisterActivityWithOptions(acts.DropMirrorPartitions, activity.RegisterOptions{Name: contract.DropMirrorPartitions})
	w.RegisterActivityWithOptions(acts.AuditPartitions, activity.RegisterOptions{Name: contract.AuditPartitions})
	return runWorker(ctx, w)
}

func runRunner(ctx context.Context, cfg config.Config, log *slog.Logger) error {
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
	capacity, err := runnerCapacity(cfg, log)
	if err != nil {
		return err
	}
	otel, err := otelconnect.NewInterceptor()
	if err != nil {
		return err
	}
	apiClient := dataplanev1connect.NewDataPlaneServiceClient(http.DefaultClient, cfg.APIURL, auth.WithBearer(token), connect.WithInterceptors(otel)) //nolint:forbidigo // the operator configures the API's address
	versions := &connectorhost.Fetcher{
		Artifact: dpactivities.Artifacts(apiClient),
		Client:   (&netguard.Policy{Allow: allow}).Client(),
		Cache:    cfg.Cache,
	}
	stores := &secretstore.Reader{}
	defer func() { _ = stores.Close() }()
	sync := &dpactivities.Sync{
		API:         apiClient,
		Checkpoints: cps,
		Storage:     cfg.Checkpoints,
		Engine:      engineclient.Options{Binary: cfg.Engine, Logger: log.With("component", "engine")},
		Logger:      log,
		Connectors:  connectors,
		Versions:    versions,
		Secrets:     stores,
		Logs:        cps,
		HTTP:        (&netguard.Policy{Allow: allow}).Client(),
	}
	dbt := &dpactivities.Dbt{API: apiClient, Artifacts: versions, Secrets: stores, Executable: cfg.Dbt, Logger: log, Logs: cps}
	sparkSteps := &dpactivities.Spark{
		API: apiClient, Secrets: stores, Checkpoints: cps, Storage: cfg.Checkpoints,
		HTTP: (&netguard.Policy{Allow: allow}).Client(), SparkSubmit: cfg.SparkSubmit, Logger: log, Logs: cps,
	}
	sync.Spark = sparkSteps
	if cfg.BridgeListen != "" {
		br, err := bridge.Listen(ctx, cfg.BridgeListen, cfg.BridgeAdvertise)
		if err != nil {
			return err
		}
		defer br.Close()
		sync.Bridge = br
		log.Info("serving the bridge to Flink jobs", "listen", cfg.BridgeListen, "advertise", br.Address())
	}
	connectorOps := &dpactivities.Ops{
		API: apiClient, Connectors: connectors, Versions: versions, Secrets: stores, Storage: cfg.Checkpoints,
		Flink: flink.Options{Logger: log.With("component", "flink"), HTTP: (&netguard.Policy{Allow: allow}).Client()},
	}
	every, err := time.ParseDuration(cfg.ReportInterval)
	if cfg.ReportInterval != "" && (err != nil || every <= 0) {
		return fmt.Errorf("daemon: --report-interval %q isn't a positive duration", cfg.ReportInterval)
	}
	host, _ := os.Hostname()
	w := &dpworker.Worker{
		API: apiClient, ID: fmt.Sprintf("%s:%d", host, os.Getpid()), Release: external.Release(), Protocol: external.Protocol,
		Features: external.Features, Launchers: []string{dispatch.LauncherSubprocess}, Platform: connectorhost.Platform(),
		Pool: cfg.Pool, Capacity: capacity, ReportEvery: every, Logger: log,
		Kinds: map[string]dpworker.Kind{
			// Attempts drain to a checkpoint for up to this long.
			contract.RunSyncAttempt: {Handler: dpworker.Handle(sync.RunSyncAttempt), Checkpoints: true, StopTimeout: 2 * time.Minute},
			// A Flink job's drain takes a savepoint, which takes longer with
			// large state.
			contract.RunStreamJob:   {Handler: dpworker.Handle(sync.RunStreamJob), Checkpoints: true, StopTimeout: 10 * time.Minute},
			contract.RunDbt:         {Handler: dpworker.Handle(dbt.RunDbt)},
			contract.RunSpark:       {Handler: dpworker.Handle(sparkSteps.RunSpark)},
			contract.CheckConnector: {Handler: dpworker.Handle(connectorOps.CheckConnector)},
			contract.DiscoverSource: {Handler: dpworker.Handle(connectorOps.DiscoverSource)},
			contract.TeardownSource: {Handler: dpworker.Handle(connectorOps.TeardownSource)},
			contract.TeardownFlink:  {Handler: dpworker.Handle(connectorOps.TeardownFlink)},
			contract.ReadLogs:       {Handler: dpworker.Handle((&dpactivities.Logs{Store: cps}).ReadLogs)},
		},
	}
	log.Info("runner polling", "id", w.ID, "data_plane", cfg.DataPlane, "pool", cfg.Pool, "capacity", capacity.String(), "checkpoints", cfg.Checkpoints)
	return w.Run(ctx)
}

// runnerCapacity is what the runner places on itself: what --capacity says,
// or what its cgroup or host has, less --reserve.
func runnerCapacity(cfg config.Config, log *slog.Logger) (resources.Size, error) {
	if cfg.Capacity != "" {
		c, err := resources.Parse(cfg.Capacity)
		if err != nil {
			return c, fmt.Errorf("daemon: --capacity: %w", err)
		}
		return c, nil
	}
	reserve, err := resources.Parse(cfg.Reserve)
	if cfg.Reserve == "" {
		reserve, err = resources.Size{}, nil
	}
	if err != nil {
		return reserve, fmt.Errorf("daemon: --reserve: %w", err)
	}
	host, err := resources.Detect()
	if err != nil {
		return host, fmt.Errorf("daemon: %w", err)
	}
	c := host.Sub(reserve)
	if !resources.Default.Fits(c) {
		log.Warn("this runner is smaller than a job's default size, so it can't take one", "capacity", c.String(), "size", resources.Default.String())
	}
	return c, nil
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
