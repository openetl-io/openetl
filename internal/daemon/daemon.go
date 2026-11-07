// Package daemon runs openetld's roles: the API server, the orchestrator
// and the runner, alone or together.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
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
	"golang.org/x/sync/errgroup"

	"github.com/openetl-io/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/openetl-io/openetl/internal/api"
	"github.com/openetl-io/openetl/internal/auth"
	"github.com/openetl-io/openetl/internal/bridge"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/checkpoint/openstore"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/connectorhost/external"
	"github.com/openetl-io/openetl/internal/engineclient"
	"github.com/openetl-io/openetl/internal/flink"
	"github.com/openetl-io/openetl/internal/netguard"
	"github.com/openetl-io/openetl/internal/orchestration"
	cpactivities "github.com/openetl-io/openetl/internal/orchestration/activities"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/dbosbackend"
	"github.com/openetl-io/openetl/internal/orchestration/dispatch"
	"github.com/openetl-io/openetl/internal/orchestration/ops"
	"github.com/openetl-io/openetl/internal/orchestration/scaling"
	"github.com/openetl-io/openetl/internal/orchestration/schedules"
	"github.com/openetl-io/openetl/internal/orchestration/temporalbackend"
	"github.com/openetl-io/openetl/internal/orchestration/workflows"
	"github.com/openetl-io/openetl/internal/platform/config"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/platform/temporalx"
	dpactivities "github.com/openetl-io/openetl/internal/runner/activities"
	"github.com/openetl-io/openetl/internal/runner/pods"
	dpworker "github.com/openetl-io/openetl/internal/runner/worker"
	"github.com/openetl-io/openetl/internal/secrets"
	"github.com/openetl-io/openetl/internal/secretstore"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/internal/webhooks"
	"github.com/openetl-io/openetl/internal/webui"
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
	// The job loop's; zero means production's.
	Scaling scaling.Options
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
		backend orchestration.Backend
		st      *store.Store
		work    *dispatch.Dispatcher
		err     error
	)
	// A runner reaches only the API.
	if o.Roles.Server || o.Roles.Orchestrator {
		backend, err = openBackend(ctx, cfg, log)
		if err != nil {
			return err
		}
		defer backend.Close()
		if st, err = store.Open(ctx, cfg.DatabaseURL); err != nil {
			return err
		}
		defer st.Close()
		if err := st.Migrate(ctx); err != nil {
			return err
		}
		work = &dispatch.Dispatcher{Store: st, Waker: &ops.Waker{Backend: backend}, Options: o.Dispatch, Logger: log.With("component", "dispatch")}
	}

	g, gctx := errgroup.WithContext(ctx)
	// A runner in this process drains through the API, so it stops first.
	cpctx := gctx
	stopControlPlane := func() {}
	if o.Roles.Runner && (o.Roles.Server || o.Roles.Orchestrator) {
		var stop context.CancelFunc
		cpctx, stop = context.WithCancel(context.WithoutCancel(gctx))
		defer stop()
		stopControlPlane = stop
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
			Store: st, Tenant: tenant, Schedules: &schedules.Schedules{Backend: backend}, Ops: &ops.Ops{Backend: backend}, Logs: &ops.Ops{Backend: backend}, Dispatch: work,
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
	// After the server's part, which gives a runner in this process the API's
	// address and a token.
	if o.Roles.Runner {
		g.Go(func() error {
			defer stopControlPlane()
			return runRunner(gctx, cfg, log.With("role", "runner"))
		})
	}
	if o.Roles.Orchestrator {
		reg, err := work.RegisterMetrics()
		if err != nil {
			return err
		}
		defer func() { _ = reg.Unregister() }()
		g.Go(func() error { return orchestrate(cpctx, cfg, backend, st, work, o.Workflows, log) })
		g.Go(func() error { return work.Run(cpctx) })
		jobs := &scaling.Loop{Store: st, Options: o.Scaling, Logger: log.With("component", "scaling")}
		g.Go(func() error { return jobs.Run(cpctx) })
	}
	return g.Wait()
}

// configureSignIn sets the server's sign-in from cfg. It refuses
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

func openBackend(ctx context.Context, cfg config.Config, log *slog.Logger) (orchestration.Backend, error) {
	switch cfg.Orchestrator {
	case "temporal":
		return temporalbackend.Open(ctx, temporalx.Options{Address: cfg.TemporalAddress, Namespace: cfg.TemporalNamespace, Logger: log})
	case "", "dbos":
		url := cfg.DBOSDatabaseURL
		if url == "" {
			var err error
			if url, err = dbosbackend.SystemDatabaseURL(cfg.DatabaseURL); err != nil {
				return nil, fmt.Errorf("daemon: --dbos-database-url: %w", err)
			}
		}
		return dbosbackend.Open(ctx, dbosbackend.Options{DatabaseURL: url, ExecutorID: cfg.ExecutorID, Logger: log.With("component", "dbos")})
	}
	return nil, fmt.Errorf("daemon: --orchestrator: %q is neither dbos nor temporal", cfg.Orchestrator)
}

func orchestrate(ctx context.Context, cfg config.Config, backend orchestration.Backend, st *store.Store, work *dispatch.Dispatcher, wf workflows.Workflows, log *slog.Logger) error {
	sched := &schedules.Schedules{Backend: backend}
	retention := contract.RetentionInput{
		CheckpointHistory: contract.DefaultCheckpointHistory,
		AuditHistory:      time.Duration(cfg.AuditRetentionDays) * 24 * time.Hour,
	}
	if err := sched.EnsureRetention(ctx, retention); err != nil {
		return err
	}
	reconciler := &schedules.Reconciler{Store: st, Schedules: sched, Backend: backend, Logger: log.With("component", "reconcile")}
	go reconciler.Run(ctx)
	acts := &cpactivities.Activities{Store: st, Schedules: sched, Dispatch: work, Logger: log.With("component", "steps")}
	return backend.Run(ctx, wf.All(), acts.Steps())
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
	var capacity resources.Size
	if cfg.Launcher == "" || cfg.Attempt != "" {
		if capacity, err = runnerCapacity(cfg, log); err != nil {
			return err
		}
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
	var cgroups *resources.Cgroups
	if cfg.CgroupRoot != "" {
		if cgroups, err = resources.OpenCgroups(cfg.CgroupRoot); err != nil {
			return fmt.Errorf("daemon: --cgroup-root: %w", err)
		}
	}
	host, _ := os.Hostname()
	w := &dpworker.Worker{
		API: apiClient, ID: fmt.Sprintf("%s:%d", host, os.Getpid()), Release: external.Release(), Protocol: external.Protocol,
		Features: external.Features, Launchers: []string{dispatch.LauncherSubprocess}, Platform: connectorhost.Platform(),
		Pool: cfg.Pool, Capacity: capacity, Cgroups: cgroups, ReportEvery: every, Logger: log,
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
	switch {
	case cfg.Attempt != "":
		if cfg.Placement <= 0 || cfg.Placement > math.MaxInt32 {
			return errors.New("daemon: --attempt needs its --placement, which the launcher sets")
		}
		w.Attempt = &dpworker.Attempt{ID: cfg.Attempt, Placement: int32(cfg.Placement)}
		log.Info("runner running one attempt", "id", w.ID, "attempt", cfg.Attempt, "placement", cfg.Placement)
		return w.Run(ctx)
	case cfg.Launcher != "":
		launcher, limits, err := podLauncher(ctx, cfg)
		if err != nil {
			return err
		}
		if c, ok := launcher.(interface{ Close() }); ok {
			defer c.Close()
		}
		w.Launcher, w.Pods = launcher, limits
		log.Info("pod launcher polling", "id", w.ID, "launcher", cfg.Launcher, "data_plane", cfg.DataPlane, "pool", cfg.Pool,
			"pod_min_size", limits.Min.String(), "max_pods", limits.MaxPods, "checkpoints", cfg.Checkpoints)
		return w.Run(ctx)
	}
	log.Info("runner polling", "id", w.ID, "data_plane", cfg.DataPlane, "pool", cfg.Pool, "capacity", capacity.String(), "checkpoints", cfg.Checkpoints)
	return w.Run(ctx)
}

// podLauncher is the runner's pod launcher, and the jobs it takes.
func podLauncher(ctx context.Context, cfg config.Config) (pods.Launcher, dpworker.PodLimits, error) {
	var limits dpworker.PodLimits
	var err error
	if limits.Min, err = sizeFlag(cfg.PodMinSize); err != nil {
		return nil, limits, fmt.Errorf("daemon: --pod-min-size: %w", err)
	}
	if cfg.PodMaxSize != "" {
		if limits.Max, err = sizeFlag(cfg.PodMaxSize); err != nil {
			return nil, limits, fmt.Errorf("daemon: --pod-max-size: %w", err)
		}
	}
	cpu, err := strconv.ParseFloat(cfg.MaxPodCPU, 64)
	if err != nil || cpu < 0 {
		return nil, limits, fmt.Errorf("daemon: --max-pod-cpu %q isn't a number of vCPUs", cfg.MaxPodCPU)
	}
	limits.MaxPods, limits.MaxCPU = max(0, cfg.MaxPods), int64(cpu*1000)
	reserve, err := resources.Parse(cfg.Reserve)
	if err != nil {
		return nil, limits, fmt.Errorf("daemon: --reserve: %w", err)
	}
	switch cfg.Launcher {
	case "kubernetes":
		if cfg.PodStreamSpot != "prefer" && cfg.PodStreamSpot != "avoid" {
			return nil, limits, fmt.Errorf("daemon: --pod-stream-spot %q is neither prefer nor avoid", cfg.PodStreamSpot)
		}
		k, err := pods.OpenKubernetes(ctx, pods.KubernetesOptions{Kubeconfig: cfg.Kubeconfig, Namespace: cfg.PodNamespace, Template: cfg.PodTemplate})
		if err != nil {
			return nil, limits, err
		}
		if k.Scratch, err = resources.ParseMemory(cfg.PodScratch); err != nil {
			return nil, limits, fmt.Errorf("daemon: --pod-scratch: %w", err)
		}
		k.DataPlane, k.Pool, k.Reserve, k.StreamsAvoidSpot = cfg.DataPlane, cfg.Pool, reserve, cfg.PodStreamSpot == "avoid"
		return k, limits, nil
	case "process":
		exe, err := os.Executable()
		if err != nil {
			return nil, limits, err
		}
		// Its own flags, but with the attempt in the environment, which makes
		// each process a pod.
		return &pods.Processes{Command: append([]string{exe}, os.Args[1:]...), Environ: os.Environ(), Pool: cfg.Pool}, limits, nil
	}
	return nil, limits, fmt.Errorf("daemon: --launcher %q is neither kubernetes nor process", cfg.Launcher)
}

// sizeFlag reads a size given as a preset or CPU,MEMORY.
func sizeFlag(v string) (resources.Size, error) {
	if p, ok := resources.Preset(v); ok {
		return p, nil
	}
	return resources.Parse(v)
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
