package activities

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/dbt"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/platform/temporalx"
)

// Dbt runs pipelines' dbt steps against their destinations.
type Dbt struct {
	API       dataplanev1connect.DataPlaneServiceClient
	Artifacts *connectorhost.Fetcher
	// Empty: dbt on PATH.
	Executable string
	// Where projects are unpacked while they run. Empty: the system's
	// temporary directory.
	Work   string
	Logger *slog.Logger
	// Where steps' logs go. Nil: steps keep none.
	Logs checkpoint.Store
}

// RunDbt runs a dbt step's command on the project's bundle. A command that
// fails is a non-retryable DBT_FAILED error.
func (a *Dbt) RunDbt(ctx context.Context, in contract.DbtInput) (contract.DbtResult, error) {
	log := a.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	attempt := int(activity.GetInfo(ctx).Attempt)
	log = log.With("pipeline", in.PipelineID, "sync", in.SyncID, "step", in.StepNumber, "project", in.Step.Project, "digest", in.Step.Digest, "attempt", attempt)
	log, done := attemptLog(ctx, a.Logs, log, in.OrgID, in.PipelineID, in.SyncID, in.StepNumber, attempt)
	defer done()
	stop := heartbeats(ctx, func() any { return nil })
	res, err := a.run(ctx, in, log)
	stop()
	var f *dbt.Failure
	switch {
	case errors.As(err, &f):
		log.Warn("dbt failed", "err", f)
		return res, temporal.NewNonRetryableApplicationError(f.Error(), contract.DbtFailed, nil)
	case err != nil:
		log.Warn("dbt step failed", "code", errs.CodeOf(err).String(), "err", err)
	default:
		log.Info("dbt finished", "nodes", res.Nodes, "seconds", res.Seconds)
	}
	return res, temporalx.ApplicationError(err)
}

func (a *Dbt) run(ctx context.Context, in contract.DbtInput, log *slog.Logger) (contract.DbtResult, error) {
	res := contract.DbtResult{Project: in.Step.Project, Digest: in.Step.Digest, Command: in.Step.Command}
	spec, err := syncSpec(ctx, a.API, in.OrgID, in.PipelineID, in.Version)
	if err != nil {
		return res, err
	}
	dst := spec.GetDestination()
	cfg, err := protojson.Marshal(dst.GetConfig())
	if err != nil {
		return res, err
	}
	name, _ := connectorhost.SplitRef(dst.GetConnector())
	target, err := dbt.DestinationTarget(name, cfg, spec.GetPipeline().GetDestination().GetNamespace(), in.Step.Schema)
	if err != nil {
		return res, errs.Wrap(errs.ConfigInvalid, err, "dbt project "+in.Step.Project)
	}
	if a.Artifacts == nil {
		return res, errs.New(errs.ConfigInvalid, "this runner can't fetch artifacts")
	}
	bundle, err := a.Artifacts.Get(ctx, in.Step.Digest)
	if err != nil {
		return res, err
	}
	work, err := os.MkdirTemp(a.Work, "openetl-dbt-")
	if err != nil {
		return res, errs.Wrap(errs.Internal, err, "dbt work directory")
	}
	defer func() { _ = os.RemoveAll(work) }()
	r, err := dbt.Exec(ctx, bundle, work, dbt.Invocation{
		Executable: a.Executable, Command: in.Step.Command, Select: in.Step.Select, Exclude: in.Step.Exclude,
		Target: target, Logger: log.With("component", "dbt"),
	})
	if r != nil {
		res.Nodes, res.Seconds = r.Nodes, r.Elapsed.Seconds()
	}
	if errors.Is(err, dbt.ErrStart) {
		return res, errs.Wrap(errs.ConfigInvalid, err, "install dbt-core and the destination's adapter, such as dbt-postgres, on the runner, or point --dbt at it")
	}
	return res, err
}
