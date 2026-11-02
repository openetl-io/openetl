// Package activities holds the data plane's activities, which run on a
// runner.
package activities

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/engineclient"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/platform/temporalx"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/internal/secretstore"
	"github.com/unsuitable001/openetl/sdk/go/destination"
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
}

// RunSyncAttempt runs one attempt of a sync. Heartbeats carry the last
// committed epoch, as a progress hint only. When the worker stops, the
// attempt stops at its next checkpoint and fails with DRAINING, whose
// details are the attempt number.
func (a *Sync) RunSyncAttempt(ctx context.Context, in contract.AttemptInput) (contract.AttemptResult, error) {
	log := a.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	info := activity.GetInfo(ctx)
	attempt := int(in.Earlier + info.Attempt)
	log, done := attemptLog(ctx, a.Logs, log.With("pipeline", in.PipelineID, "sync", in.SyncID, "version", in.Version, "attempt", attempt), in.OrgID, in.PipelineID, in.SyncID, 0, attempt)
	defer done()
	if err := lateStart(info); err != nil {
		log.Warn("sync attempt not started", "err", err)
		return contract.AttemptResult{}, temporalx.ApplicationError(err)
	}
	if attempt == 1 {
		defer pruneLogs(ctx, a.Logs, a.LogHistory, in.OrgID, in.PipelineID, log)()
	}
	res, err := a.run(ctx, ctx, attemptParams{
		org: in.OrgID, pipeline: in.PipelineID, version: in.Version,
		sync: info.WorkflowExecution.RunID, drain: activity.GetWorkerStopChannel(ctx),
	}, log)
	switch {
	case errors.Is(err, runner.ErrDrained):
		log.Info("sync attempt drained", "generation", res.Generation, "committed", res.Committed, "last", res.Last)
		return res, temporal.NewApplicationErrorWithOptions(err.Error(), errs.Draining.String(), temporal.ApplicationErrorOptions{Details: []any{info.Attempt}})
	case err != nil:
		log.Warn("sync attempt failed", "code", errs.CodeOf(err).String(), "err", err)
	default:
		log.Info("sync attempt finished", "generation", res.Generation, "recovery", res.Recovery, "restored", res.Restored, "committed", res.Committed)
	}
	return res, temporalx.ApplicationError(err)
}

// streamDrain is how long a stream job has to stop at a checkpoint once
// it's asked to, as long as the worker's stop timeout.
const streamDrain = 2 * time.Minute

// RunStreamJob runs a continuous pipeline's job until it is asked to stop.
// Heartbeats carry the last committed epoch. A job doesn't stop when
// heartbeats fail, since a Temporal outage would then stop every job, and
// fencing makes a job Temporal replaced harmless. When the activity is
// cancelled, or Temporal says it no longer exists, the job stops at its next
// checkpoint and the activity reports itself cancelled. When the worker
// stops, it fails with DRAINING instead, whose details are the attempt
// number.
func (a *Sync) RunStreamJob(ctx context.Context, in contract.StreamJobInput) (contract.AttemptResult, error) {
	log := a.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	info := activity.GetInfo(ctx)
	attempt := int(in.Earlier + info.Attempt)
	log, done := attemptLog(ctx, a.Logs, log.With("pipeline", in.PipelineID, "sync", in.SyncID, "version", in.Version, "attempt", attempt), in.OrgID, in.PipelineID, in.SyncID, 0, attempt)
	defer done()
	if err := lateStart(info); err != nil {
		log.Warn("stream job not started", "err", err)
		return contract.AttemptResult{}, temporalx.ApplicationError(err)
	}
	if attempt == 1 {
		defer pruneLogs(ctx, a.Logs, a.LogHistory, in.OrgID, in.PipelineID, log)()
	}

	// The job outlives the activity's context by up to streamDrain.
	jobCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
		case <-activity.GetWorkerStopChannel(ctx):
		case <-jobCtx.Done():
			return
		}
		close(stop)
		t := time.NewTimer(streamDrain)
		defer t.Stop()
		select {
		case <-t.C:
			log.Warn("the job didn't stop at a checkpoint in time")
			cancel()
		case <-jobCtx.Done():
		}
	}()
	log.Info("stream job starting")
	res, err := a.run(jobCtx, ctx, attemptParams{
		org: in.OrgID, pipeline: in.PipelineID, version: in.Version,
		sync: in.RunKey, drain: stop,
	}, log)
	switch {
	case errors.Is(err, runner.ErrDrained) && ctx.Err() != nil:
		log.Info("stream job stopped", "generation", res.Generation, "committed", res.Committed, "last", res.Last)
		return res, temporal.NewCanceledError()
	case errors.Is(err, runner.ErrDrained):
		log.Info("stream job drained", "generation", res.Generation, "committed", res.Committed, "last", res.Last)
		return res, temporal.NewApplicationErrorWithOptions(err.Error(), errs.Draining.String(), temporal.ApplicationErrorOptions{Details: []any{info.Attempt}})
	case err == nil:
		err = errs.New(errs.ContractChanged, "the source ended every stream, and a continuous pipeline's streams don't end")
	case ctx.Err() != nil:
		log.Info("stream job stopped", "err", err)
		return res, temporal.NewCanceledError()
	}
	log.Warn("stream job failed", "code", errs.CodeOf(err).String(), "err", err)
	if errs.CodeOf(err) == errs.Fenced {
		// Retried, unlike a sync's: see the supervisor.
		return res, temporal.NewApplicationErrorWithOptions(err.Error(), errs.Fenced.String(), temporal.ApplicationErrorOptions{Cause: err})
	}
	return res, temporalx.ApplicationError(err)
}

type attemptParams struct {
	org, pipeline, version string
	// The attempt's sync, which the checkpoint mirror joins on: the
	// workflow's run for a sync, the run's key for a stream job.
	sync string
	// Closing it stops the attempt at its next checkpoint.
	drain <-chan struct{}
}

// run runs an attempt on ctx; actx is the activity's, for heartbeats.
func (a *Sync) run(ctx, actx context.Context, p attemptParams, log *slog.Logger) (contract.AttemptResult, error) {
	var out contract.AttemptResult
	info := activity.GetInfo(actx)
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
	env.Logger = log.With("component", "connector")
	job, err := buildJob(ctx, spec, env)
	defer connectorhost.Close(job.Source)
	defer connectorhost.Close(job.Destination)
	if err != nil {
		return out, err
	}
	job.Owner = fmt.Sprintf("%s/%s/%d/%s", info.WorkflowExecution.RunID, info.ActivityID, info.Attempt, rand.Text()[:8])
	job.Sync = p.sync
	job.Log = checkpoint.NewLog(a.Checkpoints, spec.GetOrgId(), spec.GetPipelineId())
	if a.Storage != "" {
		if job.QuarantineURL, err = quarantineURL(a.Storage, spec.GetOrgId(), spec.GetPipelineId()); err != nil {
			return out, errs.Wrap(errs.ConfigInvalid, err, "quarantine location")
		}
	}
	engine := a.Engine
	engine.Logger = log.With("component", "engine")
	job.Dataflow = &engineclient.Dataflow{Options: engine}
	job.Logger = log
	job.Drain = p.drain

	var last atomic.Uint64
	job.OnCommit = last.Store
	reports := newReporter(ctx, a.API, spec.GetPipelineId(), p.sync, log)
	job.OnSchemas, job.OnCheckpoint = reports.schemas, reports.checkpoint
	stop := heartbeats(actx, func() any { return last.Load() })
	r, err := runner.RunAttempt(ctx, job)
	stop()
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
		if _, ok := cfg.GetConfig().GetFields()[e.GetField()]; !ok {
			return errs.New(errs.Internal, "config.%s names %s, but the config has no such field", e.GetField(), e.GetSecret())
		}
		if r == nil {
			return errs.New(errs.ConfigInvalid, "config.%s names %s, which is in a cloud secret store, and this runner can't read those", e.GetField(), e.GetSecret())
		}
		v, err := r.Read(ctx, e.GetRef())
		if err != nil {
			return fmt.Errorf("config.%s names %s: %w", e.GetField(), e.GetSecret(), err)
		}
		cfg.Config.Fields[e.GetField()] = structpb.NewStringValue(v)
	}
	return nil
}

// quarantineURL is where a pipeline's quarantined records go:
// {org}/{pipeline}/_quarantine below the checkpoint store.
func quarantineURL(storage, org, pipeline string) (string, error) {
	rel := path.Join(org, pipeline, checkpoint.QuarantineDir)
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
