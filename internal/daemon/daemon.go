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
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"golang.org/x/sync/errgroup"

	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/unsuitable001/openetl/internal/api"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/checkpoint/openstore"
	"github.com/unsuitable001/openetl/internal/engineclient"
	cpactivities "github.com/unsuitable001/openetl/internal/orchestration/activities"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/orchestration/ops"
	"github.com/unsuitable001/openetl/internal/orchestration/schedules"
	"github.com/unsuitable001/openetl/internal/orchestration/workflows"
	"github.com/unsuitable001/openetl/internal/platform/config"
	"github.com/unsuitable001/openetl/internal/platform/temporalx"
	dpactivities "github.com/unsuitable001/openetl/internal/runner/activities"
	"github.com/unsuitable001/openetl/internal/store"
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
		tenant, err := st.Bootstrap(ctx, cfg.DataPlane)
		if err != nil {
			_ = ln.Close()
			return err
		}
		srv := &api.Server{
			Store: st, Tenant: tenant, Schedules: &schedules.Schedules{Client: tc}, Ops: &ops.Ops{Client: tc},
			DefaultDataPlane: cfg.DataPlane, Logger: log.With("role", "server"),
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
	w := worker.New(tc, contract.OrchestratorQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(wf.Sync, workflow.RegisterOptions{Name: contract.SyncWorkflow})
	w.RegisterWorkflowWithOptions(wf.ConnectorOp, workflow.RegisterOptions{Name: contract.ConnectorOpWorkflow})
	acts := &cpactivities.Activities{Store: st}
	w.RegisterActivityWithOptions(acts.StartSync, activity.RegisterOptions{Name: contract.StartSync})
	w.RegisterActivityWithOptions(acts.FinishSync, activity.RegisterOptions{Name: contract.FinishSync})
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
	apiClient := dataplanev1connect.NewDataPlaneServiceClient(http.DefaultClient, cfg.APIURL)
	sync := &dpactivities.Sync{
		API:         apiClient,
		Checkpoints: cps,
		Storage:     cfg.Checkpoints,
		Engine:      engineclient.Options{Binary: cfg.Engine, Logger: log.With("component", "engine")},
		Logger:      log,
	}
	w := worker.New(tc, contract.BatchQueue(cfg.DataPlane), worker.Options{
		MaxConcurrentActivityExecutionSize: 4,
		// Attempts drain to a checkpoint for up to this long.
		WorkerStopTimeout: 2 * time.Minute,
	})
	w.RegisterActivityWithOptions(sync.RunSyncAttempt, activity.RegisterOptions{Name: contract.RunSyncAttempt})
	connectorOps := &dpactivities.Ops{API: apiClient}
	w.RegisterActivityWithOptions(connectorOps.CheckConnector, activity.RegisterOptions{Name: contract.CheckConnector})
	w.RegisterActivityWithOptions(connectorOps.DiscoverSource, activity.RegisterOptions{Name: contract.DiscoverSource})
	log.Info("runner polling", "queue", contract.BatchQueue(cfg.DataPlane), "checkpoints", cfg.Checkpoints)
	return runWorker(ctx, w)
}

func runWorker(ctx context.Context, w worker.Worker) error {
	if err := w.Start(); err != nil {
		return err
	}
	<-ctx.Done()
	w.Stop()
	return nil
}
