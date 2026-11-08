package workflows

import (
	"cmp"
	"errors"
	"time"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
)

// RetentionProbe asks the sources of pipelines that aren't running how much
// of what they keep for them the pipelines have used, one pipeline at a
// time, on a runner of its data plane, which reports what it found to the
// API. A probe that fails is left until the next run, and so are the rest
// of its data plane's once no runner took one.
func (w Workflows) RetentionProbe(ctx orchestration.Context, _ struct{}) (*contract.RetentionProbeResult, error) {
	in := contract.ListRetentionProbesInput{Before: ctx.Now().Add(-contract.RetentionProbeEvery), Max: contract.RetentionProbeMax}
	var probes []contract.ConnectorOpInput
	if err := ctx.Step(contract.ListRetentionProbes, cpStep, in, &probes); err != nil {
		return nil, err
	}
	b := &inbox{ctx: ctx}
	var res contract.RetentionProbeResult
	unreachable := map[string]bool{}
	var n int32
	for _, p := range probes {
		if unreachable[p.DataPlane] {
			res.Failed++
			continue
		}
		n++
		work := contract.WorkInput{
			ID: workID(ctx, contract.OpRetention, n), OrgID: p.OrgID, DataPlane: p.DataPlane, Light: contract.LightOps,
			Kind: contract.ProbeRetention, Input: p, Heartbeat: cmp.Or(w.AttemptHeartbeat, time.Minute), Pickup: contract.OpPickup,
			Timeout: 2 * time.Minute,
		}
		err := b.sendWork(work)
		var out contract.WorkOutcome
		if err == nil {
			out, err = b.awaitWork(contract.WorkRef{OrgID: work.OrgID, ID: work.ID})
		}
		if err == nil {
			err = outcomeError(out)
		}
		switch {
		case errors.Is(err, orchestration.ErrStopped):
			return &res, err
		case err != nil:
			res.Failed++
			if orchestration.CodeOf(err) == contract.NoRunner {
				unreachable[p.DataPlane] = true
			}
			ctx.Logger().Warn("a retention probe failed", "pipeline", p.PipelineID, "err", err)
		default:
			res.Probed++
		}
	}
	return &res, nil
}
