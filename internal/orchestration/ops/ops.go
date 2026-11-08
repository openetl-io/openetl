// Package ops runs connector operations through ConnectorOpWorkflow, and
// reads syncs' logs through ReadLogsWorkflow.
package ops

import (
	"cmp"
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
)

type Ops struct {
	Backend orchestration.Backend
}

// Run starts the operation and waits for its result.
func (o *Ops) Run(ctx context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
	var res contract.ConnectorOpResult
	run, err := o.start(ctx, in)
	if err != nil {
		return res, err
	}
	return res, run.Get(ctx, &res)
}

func (o *Ops) start(ctx context.Context, in contract.ConnectorOpInput) (orchestration.Run, error) {
	id := contract.OpWorkflowID(cmp.Or(in.OpID, uuid.NewString()))
	if in.Try > 0 {
		id = fmt.Sprintf("%s/%d", id, in.Try)
	}
	run, err := o.Backend.Start(ctx, orchestration.StartOptions{
		ID:       id,
		Workflow: contract.ConnectorOpWorkflow,
		Input:    in,
		Timeout:  contract.OpTimeout,
		Attributes: orchestration.Attributes{
			Org: in.OrgID, Workspace: in.WorkspaceID, DataPlane: in.DataPlane, Connectors: []string{in.Connector},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("ops: start %s: %w", in.Op, err)
	}
	return run, nil
}

// ReadLogs reads a page of a sync's log on its data plane, and waits for it.
func (o *Ops) ReadLogs(ctx context.Context, in contract.ReadLogsInput) (contract.ReadLogsResult, error) {
	var res contract.ReadLogsResult
	run, err := o.Backend.Start(ctx, orchestration.StartOptions{
		ID:         contract.LogsWorkflowID(uuid.NewString()),
		Workflow:   contract.ReadLogsWorkflow,
		Input:      in,
		Timeout:    2 * time.Minute,
		Attributes: orchestration.Attributes{Org: in.OrgID, Pipeline: in.PipelineID, DataPlane: in.DataPlane},
	})
	if err != nil {
		return res, fmt.Errorf("ops: read a log: %w", err)
	}
	return res, run.Get(ctx, &res)
}

// Waker wakes workflows waiting for work they sent to a data plane.
type Waker struct {
	Backend orchestration.Backend
}

// Wake tells the workflow, if it's still running.
func (w *Waker) Wake(ctx context.Context, workflowID, workID string) error {
	return w.Backend.Send(ctx, workflowID, orchestration.Message{Kind: contract.WakeMessage, Work: workID})
}
