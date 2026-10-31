// Package activities holds the control plane's activities, which run on
// cp.orchestrator.
package activities

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/activity"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/platform/temporalx"
	"github.com/unsuitable001/openetl/internal/store"
	"github.com/unsuitable001/openetl/internal/store/db"
)

type Activities struct {
	Store *store.Store
}

// StartSync records a running sync of the pipeline's current version.
// Retries of one workflow run record it once.
func (a *Activities) StartSync(ctx context.Context, in contract.SyncInput) (contract.StartSyncResult, error) {
	res, err := a.startSync(ctx, in)
	return res, temporalx.ApplicationError(err)
}

func (a *Activities) startSync(ctx context.Context, in contract.SyncInput) (contract.StartSyncResult, error) {
	var res contract.StartSyncResult
	org, pipeline, err := ids(in.OrgID, in.PipelineID)
	if err != nil {
		return res, err
	}
	p, err := a.Store.GetPipelineByID(ctx, db.GetPipelineByIDParams{OrgID: org, ID: pipeline})
	if errors.Is(err, pgx.ErrNoRows) {
		return res, errs.New(errs.ConfigInvalid, "pipeline %s no longer exists", in.PipelineID)
	}
	if err != nil {
		return res, err
	}
	run := activity.GetInfo(ctx).WorkflowExecution
	id, err := a.Store.StartSync(ctx, db.StartSyncParams{
		OrgID:      org,
		ID:         uuid.New(),
		PipelineID: pipeline,
		Version:    p.Version,
		WorkflowID: run.ID,
		RunID:      run.RunID,
	})
	if err != nil {
		return res, err
	}
	return contract.StartSyncResult{SyncID: id.String(), Version: p.Version, DataPlane: p.DataPlaneID}, nil
}

func (a *Activities) FinishSync(ctx context.Context, in contract.FinishSyncInput) error {
	org, id, err := ids(in.OrgID, in.SyncID)
	if err != nil {
		return temporalx.ApplicationError(err)
	}
	params := db.FinishSyncParams{OrgID: org, ID: id, Status: "succeeded"}
	if in.Error != "" {
		params.Status, params.Error = "failed", &in.Error
	}
	if in.Result != nil {
		if params.Stats, err = json.Marshal(in.Result); err != nil {
			return err
		}
	}
	return temporalx.ApplicationError(a.Store.FinishSync(ctx, params))
}

func ids(a, b string) (uuid.UUID, uuid.UUID, error) {
	x, err := uuid.Parse(a)
	if err != nil {
		return x, x, errs.Wrap(errs.ConfigInvalid, err, fmt.Sprintf("id %q", a))
	}
	y, err := uuid.Parse(b)
	if err != nil {
		return x, y, errs.Wrap(errs.ConfigInvalid, err, fmt.Sprintf("id %q", b))
	}
	return x, y, nil
}
