// Package ops runs connector operations through ConnectorOpWorkflow.
package ops

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
)

type Ops struct {
	Client client.Client
}

// Run starts the operation and waits for its result.
func (o *Ops) Run(ctx context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
	var res contract.ConnectorOpResult
	run, err := o.Client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       contract.OpWorkflowID(uuid.NewString()),
		TaskQueue:                contract.OrchestratorQueue,
		WorkflowExecutionTimeout: 15 * time.Minute,
	}, contract.ConnectorOpWorkflow, in)
	if err != nil {
		return res, fmt.Errorf("ops: start %s: %w", in.Op, err)
	}
	return res, run.Get(ctx, &res)
}
