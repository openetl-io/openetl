package workflows

import (
	"cmp"
	"time"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
)

// ConnectorOp runs Check, Discover or Teardown on a runner of the data
// plane, or cancels a deleted pipeline's Flink jobs. A teardown retries
// until the operation times out, since the pipeline it cleans up after is
// gone.
func (w Workflows) ConnectorOp(ctx orchestration.Context, in contract.ConnectorOpInput) (*contract.ConnectorOpResult, error) {
	work := contract.WorkInput{
		OrgID: in.OrgID, DataPlane: in.DataPlane, Light: contract.LightOps, Kind: contract.CheckConnector, Input: in,
		Heartbeat: cmp.Or(w.AttemptHeartbeat, time.Minute), Pickup: contract.OpPickup, Timeout: 2 * time.Minute,
	}
	policy := retryPolicy{wait: time.Second, maxWait: 100 * time.Second, attempts: 3}
	switch in.Op {
	case contract.OpDiscover:
		work.Kind, work.Timeout = contract.DiscoverSource, 10*time.Minute
	case contract.OpTeardown:
		work.Kind = contract.TeardownSource
		policy = retryPolicy{wait: time.Second, maxWait: time.Minute}
	case contract.OpTeardownFlink:
		// The jobs' runner may have died, and its replacement may take a
		// while; the operation waits for the jobs' deployments to go.
		work.Kind, work.Pickup, work.Timeout = contract.TeardownFlink, 5*time.Minute, 10*time.Minute
		policy = retryPolicy{wait: time.Second, maxWait: time.Minute}
	}
	var res contract.ConnectorOpResult
	if err := (&inbox{ctx: ctx}).runWork(policy, &res, func(n int32) contract.WorkInput {
		w := work
		w.ID = workID(ctx, in.Op, n)
		return w
	}); err != nil {
		return nil, err
	}
	return &res, nil
}
