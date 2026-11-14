// Package activities holds the work a runner runs, which workflows send
// through the API.
package activities

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/openetl-io/openetl/internal/awsroles"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/connectorhost/hostservices"
	"github.com/openetl-io/openetl/internal/engineclient"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/ints"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/runner/worker"
	"github.com/openetl-io/openetl/internal/secretstore"
	"github.com/openetl-io/openetl/internal/spark"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/internal/staging"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

type Sync struct {
	API         dataplanev1connect.DataPlaneServiceClient
	Checkpoints checkpoint.Store
	// Where Checkpoints is: a directory, or an s3://, gs:// or azblob://
	// URL. Quarantined records go next to the checkpoints.
	Storage string
	Engine  engineclient.Options
	Logger  *slog.Logger
	// What the connectors get.
	Connectors connectorhost.Env
	// Nil: connector versions can't be fetched.
	Versions *connectorhost.Fetcher
	// Nil: secrets in cloud secret stores can't be read.
	Secrets *secretstore.Reader
	// Where attempts' logs go, next to the checkpoints. Nil: attempts keep
	// none.
	Logs checkpoint.Store
	// How long syncs' logs are kept. Zero: contract.DefaultLogHistory.
	LogHistory time.Duration
	// Runs pipelines' Spark transforms. Nil: they fail.
	Spark *Spark
	// Serves pipelines' Flink jobs. Nil: they fail.
	Bridge runner.Bridge
	// Reaches Flink clusters' REST APIs, and fetches https:// code. Nil: a
	// default client.
	HTTP *http.Client
	// Assumes the aws roles that sources and destinations name. Nil: none
	// can be.
	AWSRoles *awsroles.Broker
}

// RunSyncAttempt runs one attempt of a sync. When it's asked to stop, or the
// runner is stopping, the attempt stops at its next checkpoint, and fails
// with DRAINING if it wasn't asked to.
func (a *Sync) RunSyncAttempt(ctx context.Context, in contract.AttemptInput) (contract.AttemptResult, error) {
	log := a.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	work := worker.From(ctx)
	attempt := int(in.Attempt)
	log, done := attemptLog(ctx, a.Logs, log.With("pipeline", in.PipelineID, "sync", in.SyncID, "version", in.Version, "attempt", attempt), in.OrgID, in.PipelineID, in.SyncID, 0, attempt)
	defer done()
	if err := work.LateStart(); err != nil {
		log.Warn("sync attempt not started", "err", err)
		return contract.AttemptResult{}, err
	}
	if attempt == 1 {
		defer pruneLogs(ctx, a.Logs, a.LogHistory, in.OrgID, in.PipelineID, log)()
	}
	res, err := a.run(ctx, attemptParams{
		org: in.OrgID, pipeline: in.PipelineID, version: in.Version,
		sync: in.RunKey, drain: work.Stopping(), onCommit: work.SetEpoch,
	}, log)
	countAttempt(ctx, in.OrgID, in.PipelineID, "sync", attempt, work.Cancelled(), err)
	var failed *spark.Failed
	switch {
	case errors.As(err, &failed):
		log.Warn("sync attempt failed: the Spark transform failed", "err", err)
		a.reportFailure(ctx, in.PipelineID, in.RunKey, attempt, err, log)
		return res, &contract.Failure{Code: contract.SparkFailed, Message: err.Error()}
	case errors.Is(err, runner.ErrDrained) && work.Cancelled():
		log.Info("sync attempt stopped", "generation", res.Generation, "committed", res.Committed, "last", res.Last)
		return res, worker.ErrCancelled
	case errors.Is(err, runner.ErrDrained):
		log.Info("sync attempt drained", "generation", res.Generation, "committed", res.Committed, "last", res.Last)
		return res, errs.Wrap(errs.Draining, err, "")
	case err != nil:
		log.Warn("sync attempt failed", "code", errs.CodeOf(err).String(), "err", err)
		a.reportFailure(ctx, in.PipelineID, in.RunKey, attempt, err, log)
	default:
		log.Info("sync attempt finished", "generation", res.Generation, "recovery", res.Recovery, "restored", res.Restored, "committed", res.Committed)
	}
	return res, err
}

// RunStreamJob runs a continuous pipeline's job until it is asked to stop. A
// job doesn't stop when reports fail, since an API outage would then stop
// every job, and fencing makes a job that's been replaced harmless. When a
// report's answer says to stop, the job stops at its next checkpoint and
// reports itself cancelled. When the runner stops, it fails with DRAINING
// instead.
func (a *Sync) RunStreamJob(ctx context.Context, in contract.StreamJobInput) (contract.AttemptResult, error) {
	log := a.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	work := worker.From(ctx)
	attempt := int(in.Attempt)
	log, done := attemptLog(ctx, a.Logs, log.With("pipeline", in.PipelineID, "sync", in.SyncID, "version", in.Version, "attempt", attempt), in.OrgID, in.PipelineID, in.SyncID, 0, attempt)
	defer done()
	if err := work.LateStart(); err != nil {
		log.Warn("stream job not started", "err", err)
		return contract.AttemptResult{}, err
	}
	if attempt == 1 {
		defer pruneLogs(ctx, a.Logs, a.LogHistory, in.OrgID, in.PipelineID, log)()
	}
	log.Info("stream job starting")
	res, err := a.run(ctx, attemptParams{
		org: in.OrgID, pipeline: in.PipelineID, version: in.Version,
		sync: in.RunKey, drain: work.Stopping(), onCommit: work.SetEpoch,
	}, log)
	cancelled := work.Cancelled()
	countAttempt(ctx, in.OrgID, in.PipelineID, "stream", attempt, cancelled, err)
	switch {
	case errors.Is(err, runner.ErrDrained) && cancelled:
		log.Info("stream job stopped", "generation", res.Generation, "committed", res.Committed, "last", res.Last)
		return res, worker.ErrCancelled
	case errors.Is(err, runner.ErrDrained):
		log.Info("stream job drained", "generation", res.Generation, "committed", res.Committed, "last", res.Last)
		return res, errs.Wrap(errs.Draining, err, "")
	case err == nil:
		err = errs.New(errs.ContractChanged, "the source ended every stream, and a continuous pipeline's streams don't end")
	case cancelled:
		log.Info("stream job stopped", "err", err)
		return res, worker.ErrCancelled
	}
	log.Warn("stream job failed", "code", errs.CodeOf(err).String(), "err", err)
	a.reportFailure(ctx, in.PipelineID, in.RunKey, attempt, err, log)
	if failed := (*runner.FlinkFailed)(nil); errors.As(err, &failed) {
		return res, &contract.Failure{Code: contract.FlinkFailed, Message: err.Error()}
	}
	return res, err
}

// reportFailure tells the control plane an attempt failed, which it otherwise
// sees only once retries run out. It's best effort.
func (a *Sync) reportFailure(ctx context.Context, pipeline, run string, attempt int, err error, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, rerr := a.API.ReportAttemptFailure(ctx, connect.NewRequest(&dataplanev1.ReportAttemptFailureRequest{
		PipelineId: pipeline, RunId: run, Attempt: ints.Int32(attempt),
		Code: errs.CodeOf(err).String(), Message: err.Error(), Time: timestamppb.Now(),
	}))
	if rerr != nil {
		log.Warn("reporting the failed attempt failed", "err", rerr)
	}
}

type attemptParams struct {
	org, pipeline, version string
	// The attempt's sync, which the checkpoint mirror joins on: the
	// workflow's run for a sync, the run's key for a stream job.
	sync string
	// Closing it stops the attempt at its next checkpoint.
	drain <-chan struct{}
	// Told each epoch the attempt commits, for reports.
	onCommit func(uint64)
}

// run runs an attempt on ctx, which carries the work.
func (a *Sync) run(ctx context.Context, p attemptParams, log *slog.Logger) (contract.AttemptResult, error) {
	var out contract.AttemptResult
	spec, err := syncSpec(ctx, a.API, p.org, p.pipeline, p.version)
	if err != nil {
		return out, err
	}
	if err := readSecrets(ctx, a.Secrets, spec.GetSource(), spec.GetSourceSecrets()); err != nil {
		return out, err
	}
	if err := readSecrets(ctx, a.Secrets, spec.GetDestination(), spec.GetDestinationSecrets()); err != nil {
		return out, err
	}
	env, err := connectorEnv(ctx, a.Connectors, a.Versions, spec.GetSourceConnector(), spec.GetDestinationConnector())
	if err != nil {
		return out, err
	}
	release, err := leaseAWS(ctx, a.AWSRoles, &env, spec.GetWorkspace(), spec.GetPipelineName(),
		awsConnection{"source", spec.GetSourceName(), spec.GetSource().GetAws()}, awsConnection{"destination", spec.GetDestinationName(), spec.GetDestination().GetAws()})
	defer release()
	if err != nil {
		return out, err
	}
	env.Logger = log.With("component", "connector")
	meter, size := worker.From(ctx).Meter, worker.From(ctx).Size
	env.Meter, env.Size = meter, size
	planner := a.Engine
	planner.Logger = log.With("component", "engine")
	checkpoints := checkpoint.NewLog(a.Checkpoints, spec.GetOrgId(), spec.GetPipelineId())
	plans := func(ctx context.Context, req sqlplan.Request) (sqlplan.Answer, error) {
		return engineclient.Plan(ctx, planner, req)
	}
	job, err := buildJob(ctx, spec, env, withRestored(plans, checkpoints))
	defer connectorhost.Close(job.Source)
	defer connectorhost.Close(job.Destination)
	if err != nil {
		return out, err
	}
	job.Owner = fmt.Sprintf("%s/%s", worker.From(ctx).ID, rand.Text()[:8])
	job.Sync = p.sync
	job.Org = spec.GetOrgId()
	job.Progress = runner.NewProgress(attribute.String("openetl.org", spec.GetOrgId()), attribute.String("openetl.pipeline", spec.GetPipelineId()))
	job.Log = checkpoints
	if a.Storage != "" {
		if job.QuarantineURL, err = pipelineURL(a.Storage, spec.GetOrgId(), spec.GetPipelineId(), checkpoint.QuarantineDir); err != nil {
			return out, errs.Wrap(errs.ConfigInvalid, err, "quarantine location")
		}
		// Next to the checkpoints, where the log finds them for a release.
		if job.HeldURL, err = pipelineURL(a.Storage, spec.GetOrgId(), spec.GetPipelineId(), checkpoint.HeldDir); err != nil {
			return out, errs.Wrap(errs.ConfigInvalid, err, "held values' location")
		}
	}
	if job.Destination.Capabilities().Delivery == destination.DeliveryStaged {
		area, err := openStaging(ctx, a.Storage, job, log)
		if err != nil {
			return out, err
		}
		defer func() { _ = area.Close() }()
		job.Staging = area
	}
	if spec.GetPipeline().GetTransform().GetSpark() != nil {
		if a.Spark == nil {
			return out, errs.New(errs.ConfigInvalid, "this runner can't run Spark transforms")
		}
		names := make([]string, len(job.Outputs))
		for i, o := range job.Outputs {
			names[i] = o.Name
		}
		tr, area, err := a.Spark.transform(ctx, spec, names, log)
		if err != nil {
			return out, err
		}
		defer func() { _ = area.Close() }()
		job.Transform = tr
	}
	if spec.GetPipeline().GetTransform().GetFlink() != nil {
		fl, err := a.flinkCluster(ctx, spec, log)
		if err != nil {
			return out, err
		}
		defer func() { _ = fl.Close() }()
		job.Flink, job.Bridge = fl, a.Bridge
	}
	if size != (resources.Size{}) && job.Transform == nil && job.Flink == nil {
		job.Lanes = size.Threads()
	}
	engine := a.Engine
	engine.Logger = log.With("component", "engine")
	engine.Meter, engine.Size = meter, size
	job.Meter = meter
	job.Dataflow = &engineclient.Dataflow{Options: engine}
	job.Logger = log
	job.Drain = p.drain
	job.OnCommit = p.onCommit
	reports := newReporter(ctx, a.API, spec.GetPipelineId(), p.sync, log)
	reports.progress, reports.lowered, reports.meter = job.Progress, job.Lowered(), meter
	reports.reads, reports.paused = streamReads(&job)
	job.OnSchemas, job.OnCheckpoint, job.OnRewind = reports.schemas, reports.checkpoint, reports.rewound
	r, err := runner.RunAttempt(ctx, job)
	reports.close(5 * time.Second)

	out = contract.AttemptResult{Generation: r.Generation, Restored: r.Restored, Committed: r.Committed, Last: r.Last}
	if r.Recovery != 0 {
		out.Recovery = r.Recovery.String()
	}
	if errors.Is(err, checkpoint.ErrFenced) || errors.Is(err, destination.ErrFenced) {
		err = errs.Wrap(errs.Fenced, err, "")
	}
	return out, err
}

// syncSpec fetches a pipeline version, with its source and destination.
func syncSpec(ctx context.Context, api dataplanev1connect.DataPlaneServiceClient, org, pipeline, version string) (*dataplanev1.GetSyncSpecResponse, error) {
	resp, err := api.GetSyncSpec(ctx, connect.NewRequest(&dataplanev1.GetSyncSpecRequest{PipelineId: pipeline, Version: version}))
	if err != nil {
		code := errs.Transient
		if c := connect.CodeOf(err); c == connect.CodeNotFound || c == connect.CodeInvalidArgument {
			code = errs.ConfigInvalid
		}
		return nil, errs.Wrap(code, err, "fetch the sync spec")
	}
	if resp.Msg.GetOrgId() != org {
		return nil, errs.New(errs.ConfigInvalid, "pipeline %s belongs to another org", pipeline)
	}
	return resp.Msg, nil
}

// readSecrets sets each config field that names a secret in a cloud secret
// store to its value, read with the runner's cloud identity.
func readSecrets(ctx context.Context, r *secretstore.Reader, cfg *apiv1.ConnectorConfig, external []*dataplanev1.ExternalSecret) error {
	for _, e := range external {
		if _, ok := connectorhost.ConfigValue(cfg.GetConfig(), e.GetField()); !ok {
			return errs.New(errs.Internal, "config.%s names %s, but the config has no such field", e.GetField(), e.GetSecret())
		}
		if r == nil {
			return errs.New(errs.ConfigInvalid, "config.%s names %s, which is in a cloud secret store, and this runner can't read those", e.GetField(), e.GetSecret())
		}
		v, err := r.Read(ctx, e.GetRef())
		if err != nil {
			return fmt.Errorf("config.%s names %s: %w", e.GetField(), e.GetSecret(), err)
		}
		connectorhost.SetConfigValue(cfg.Config, e.GetField(), structpb.NewStringValue(v))
	}
	return nil
}

// openStaging opens the pipeline's staging for a destination that loads
// staged epochs, and serves it to the destination's host services.
func openStaging(ctx context.Context, storage string, job runner.Job, log *slog.Logger) (*staging.Area, error) {
	dst, ok := job.Destination.(interface{ SetStaging(hostservices.Staging) })
	switch {
	case storage == "":
		return nil, errs.New(errs.ConfigInvalid, "the destination loads staged epochs, and this runner has no object store to stage them in")
	case !ok:
		return nil, errs.New(errs.ConfigInvalid, "the destination loads staged epochs, and only an external destination can")
	}
	area, err := staging.Open(ctx, storage, job.Org, job.Pipeline, staging.Options{Logger: log})
	if err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "staging location")
	}
	dst.SetStaging(area)
	return area, nil
}

// quarantineURL is where a pipeline's quarantined records go:
// {org}/{pipeline}/_quarantine below the checkpoint store.
func pipelineURL(storage, org, pipeline, dir string) (string, error) {
	rel := path.Join(org, pipeline, dir)
	if !strings.Contains(storage, "://") {
		abs, err := filepath.Abs(storage)
		if err != nil {
			return "", err
		}
		return (&url.URL{Scheme: "file", Path: filepath.ToSlash(abs) + "/" + rel}).String(), nil
	}
	u, err := url.Parse(storage)
	if err != nil {
		return "", err
	}
	u.Path = path.Join("/", u.Path, rel)
	return u.String(), nil
}
