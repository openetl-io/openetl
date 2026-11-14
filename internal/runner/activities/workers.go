package activities

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/engineclient"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/runner/attempt"
	"github.com/openetl-io/openetl/internal/runner/worker"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// coordinate makes the attempt's runner the coordinator of its workers, whose
// pods its launcher made: it serves them the attempt protocol, and tells the
// API where they join once it has claimed the pipeline. It closes the
// connectors the job started to discover and set up, and keeps only the
// destination's capabilities: workers read, write and commit. A job that
// can't run on workers fails with worker.OnePod, and is placed again on one
// pod.
func (a *Sync) coordinate(ctx context.Context, job *runner.Job, workers int, log *slog.Logger) (*attempt.Server, error) {
	if err := job.OnWorkers(workers); err != nil {
		var onePod *runner.OnePodError
		if errors.As(err, &onePod) {
			return nil, &worker.OnePod{Why: onePod.Why}
		}
		return nil, err
	}
	srv, err := attempt.Listen(ctx, attempt.ListenConfig{Address: cmp.Or(a.AttemptListen, "127.0.0.1:0"), Advertise: a.AttemptAdvertise})
	if err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "serve the attempt's workers")
	}
	caps := job.Destination.Capabilities()
	connectorhost.Close(job.Source)
	connectorhost.Close(job.Destination)
	job.Source, job.Destination = coordinatorSource{}, coordinatorDestination{caps}
	wk := worker.From(ctx)
	job.Workers = &runner.Workers{Count: workers, Server: srv, Launch: func(ctx context.Context, targets []attempt.Target) error {
		t := targets[0]
		req := &dataplanev1.OpenWorkersRequest{
			Runner: wk.Runner, Id: wk.ID, Address: t.Address, Certificate: t.Certificate, Token: t.Token, Generation: t.Generation, Owner: t.Attempt,
		}
		var res *connect.Response[dataplanev1.OpenWorkersResponse]
		var err error
		for wait := time.Second; ; wait *= 2 {
			if res, err = a.API.OpenWorkers(ctx, connect.NewRequest(req)); err == nil || wait > 8*time.Second {
				break
			}
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		switch {
		case err != nil:
			return errs.Wrap(errs.Transient, err, "tell the API where the workers join")
		case !res.Msg.GetAccepted():
			return errs.New(errs.Fenced, "the attempt isn't this runner's to run any more")
		}
		log.Info("the workers join here", "workers", workers, "address", t.Address)
		return nil
	}}
	return srv, nil
}

// coordinatorSource is a coordinator's source: its workers read.
type coordinatorSource struct{}

func (coordinatorSource) Read(context.Context, source.ReadRequest, source.Emitter) error {
	return errs.New(errs.Internal, "runner: a coordinator reads nothing; its workers do")
}

// coordinatorDestination is a coordinator's destination: the capabilities
// the destination declared, and no writer; its workers write.
type coordinatorDestination struct{ caps destination.Capabilities }

func (d coordinatorDestination) Capabilities() destination.Capabilities { return d.caps }

func (coordinatorDestination) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return nil, errs.New(errs.Internal, "runner: a coordinator opens no destination; its workers do")
}

// RunWorker runs one worker's part of an attempt that another runner
// coordinates, in a pod of the worker's own: w is the attempt, of size each
// worker's, and where the worker joins. It reads, writes and commits what
// the coordinator assigns, with its own connectors and engine.
func (a *Sync) RunWorker(ctx context.Context, w *dataplanev1.Work) error {
	j := w.GetJoin()
	if j == nil {
		return errs.New(errs.Internal, "runner: a worker with nowhere to join")
	}
	// A stream job's input has the same fields.
	var in contract.AttemptInput
	if err := json.Unmarshal(w.GetInput(), &in); err != nil {
		return errs.Wrap(errs.Internal, err, "the attempt's input")
	}
	log := a.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log = log.With("pipeline", in.PipelineID, "sync", in.SyncID, "version", in.Version, "attempt", in.Attempt, "worker", j.GetWorker())
	spec, err := syncSpec(ctx, a.API, in.OrgID, in.PipelineID, in.Version)
	if err != nil {
		return err
	}
	if err := readSecrets(ctx, a.Secrets, spec.GetSource(), spec.GetSourceSecrets()); err != nil {
		return err
	}
	if err := readSecrets(ctx, a.Secrets, spec.GetDestination(), spec.GetDestinationSecrets()); err != nil {
		return err
	}
	env, err := connectorEnv(ctx, a.Connectors, a.Versions, spec.GetSourceConnector(), spec.GetDestinationConnector())
	if err != nil {
		return err
	}
	release, err := leaseAWS(ctx, a.AWSRoles, &env, spec.GetWorkspace(), spec.GetPipelineName(),
		awsConnection{"source", spec.GetSourceName(), spec.GetSource().GetAws()}, awsConnection{"destination", spec.GetDestinationName(), spec.GetDestination().GetAws()})
	defer release()
	if err != nil {
		return err
	}
	env.Logger = log.With("component", "connector")
	meter, size := resources.NewMeter(), resources.Size{CPU: w.GetSize().GetCpuMillis(), Memory: w.GetSize().GetMemoryBytes()}
	env.Meter, env.Size = meter, size
	checkpoints := checkpoint.NewLog(a.Checkpoints, spec.GetOrgId(), spec.GetPipelineId())
	planner := a.Engine
	planner.Logger = log.With("component", "engine")
	plans := func(ctx context.Context, req sqlplan.Request) (sqlplan.Answer, error) {
		return engineclient.Plan(ctx, planner, req)
	}
	job, err := bindJob(ctx, spec, env, withRestored(plans, checkpoints), false)
	defer connectorhost.Close(job.Source)
	defer connectorhost.Close(job.Destination)
	if err != nil {
		return err
	}
	job.Owner, job.Sync, job.Org, job.Log = j.GetOwner(), in.RunKey, spec.GetOrgId(), checkpoints
	if a.Storage != "" {
		if job.QuarantineURL, err = pipelineURL(a.Storage, spec.GetOrgId(), spec.GetPipelineId(), checkpoint.QuarantineDir); err != nil {
			return errs.Wrap(errs.ConfigInvalid, err, "quarantine location")
		}
		if job.HeldURL, err = pipelineURL(a.Storage, spec.GetOrgId(), spec.GetPipelineId(), checkpoint.HeldDir); err != nil {
			return errs.Wrap(errs.ConfigInvalid, err, "held values' location")
		}
	}
	if size != (resources.Size{}) {
		job.Lanes = size.Threads()
	}
	engine := a.Engine
	engine.Logger = log.With("component", "engine")
	engine.Meter, engine.Size = meter, size
	job.Meter, job.Dataflow, job.Logger = meter, &engineclient.Dataflow{Options: engine}, log
	t := attempt.Target{
		Address: j.GetAddress(), Certificate: j.GetCertificate(), Token: j.GetToken(),
		Attempt: j.GetOwner(), Generation: j.GetGeneration(), Worker: int(j.GetWorker()),
	}
	err = runner.RunWorker(ctx, job, t, attempt.Options{})
	if err == nil {
		log.Info("the worker's part of the attempt finished")
	}
	return err
}
