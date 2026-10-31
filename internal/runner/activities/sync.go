// Package activities holds the data plane's activities, which run on a
// runner.
package activities

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/activity"

	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/engineclient"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/platform/temporalx"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/destination"
)

type Sync struct {
	API         dataplanev1connect.DataPlaneServiceClient
	Checkpoints checkpoint.Store
	Engine      engineclient.Options
	Logger      *slog.Logger
}

// RunSyncAttempt runs one attempt of a sync. Heartbeats carry the last
// committed epoch, as a progress hint only.
func (a *Sync) RunSyncAttempt(ctx context.Context, in contract.AttemptInput) (contract.AttemptResult, error) {
	log := a.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	info := activity.GetInfo(ctx)
	log = log.With("pipeline", in.PipelineID, "version", in.Version, "attempt", info.Attempt)
	res, err := a.run(ctx, in, info, log)
	if err != nil {
		log.Warn("sync attempt failed", "code", errs.CodeOf(err).String(), "err", err)
	} else {
		log.Info("sync attempt finished", "generation", res.Generation, "recovery", res.Recovery, "restored", res.Restored, "committed", res.Committed)
	}
	return res, temporalx.ApplicationError(err)
}

func (a *Sync) run(ctx context.Context, in contract.AttemptInput, info activity.Info, log *slog.Logger) (contract.AttemptResult, error) {
	var out contract.AttemptResult
	resp, err := a.API.GetSyncSpec(ctx, connect.NewRequest(&dataplanev1.GetSyncSpecRequest{PipelineId: in.PipelineID, Version: in.Version}))
	if err != nil {
		code := errs.Transient
		if c := connect.CodeOf(err); c == connect.CodeNotFound || c == connect.CodeInvalidArgument {
			code = errs.ConfigInvalid
		}
		return out, errs.Wrap(code, err, "fetch the sync spec")
	}
	spec := resp.Msg
	if spec.GetOrgId() != in.OrgID {
		return out, errs.New(errs.ConfigInvalid, "pipeline %s belongs to another org", in.PipelineID)
	}
	job, err := buildJob(ctx, spec)
	if err != nil {
		return out, err
	}
	job.Owner = fmt.Sprintf("%s/%s/%d/%s", info.WorkflowExecution.RunID, info.ActivityID, info.Attempt, rand.Text()[:8])
	job.Log = checkpoint.NewLog(a.Checkpoints, spec.GetOrgId(), spec.GetPipelineId())
	job.Dataflow = &engineclient.Dataflow{Options: a.Engine}
	job.Logger = log

	var last atomic.Uint64
	job.OnCommit = last.Store
	every := 30 * time.Second
	if t := info.HeartbeatTimeout / 4; t > 0 && t < every {
		every = t
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				activity.RecordHeartbeat(ctx, last.Load())
			case <-stop:
				return
			}
		}
	})
	r, err := runner.RunAttempt(ctx, job)
	close(stop)
	wg.Wait()

	out = contract.AttemptResult{Generation: r.Generation, Restored: r.Restored, Committed: r.Committed, Last: r.Last}
	if r.Recovery != 0 {
		out.Recovery = r.Recovery.String()
	}
	if errors.Is(err, checkpoint.ErrFenced) || errors.Is(err, destination.ErrFenced) {
		err = errs.Wrap(errs.Fenced, err, "")
	}
	return out, err
}
