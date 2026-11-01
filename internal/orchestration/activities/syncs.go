// Package activities holds the control plane's activities, which run on
// cp.orchestrator.
package activities

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/activity"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/platform/temporalx"
	"github.com/unsuitable001/openetl/internal/store"
	"github.com/unsuitable001/openetl/internal/store/db"
)

type Activities struct {
	Store *store.Store
}

// StartSync records a running sync of the pipeline's current version, unless
// the pipeline is paused. Retries of one workflow run record it once.
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
	if p.Paused {
		return contract.StartSyncResult{Paused: true}, nil
	}
	connectors, err := a.connectors(ctx, p)
	if err != nil {
		return res, err
	}
	steps, err := a.postSync(ctx, p)
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
	return contract.StartSyncResult{
		SyncID: id.String(), Version: p.Version, DataPlane: p.DataPlaneID,
		WorkspaceID: p.WorkspaceID.String(), Connectors: connectors, PostSync: steps,
	}, nil
}

// postSync resolves the pipeline version's post-sync steps: each dbt
// project to its current bundle.
func (a *Activities) postSync(ctx context.Context, p db.Pipeline) ([]contract.PostSyncStep, error) {
	v, err := a.Store.GetPipelineVersion(ctx, db.GetPipelineVersionParams{OrgID: p.OrgID, PipelineID: p.ID, Version: p.Version})
	if err != nil {
		return nil, err
	}
	var spec apiv1.PipelineSpec
	if err := protojson.Unmarshal(v.Spec, &spec); err != nil {
		return nil, err
	}
	var steps []contract.PostSyncStep
	for _, s := range spec.GetPostSync() {
		d := s.GetDbt()
		proj, err := a.Store.GetDbtProject(ctx, db.GetDbtProjectParams{OrgID: p.OrgID, WorkspaceID: p.WorkspaceID, Name: d.GetProject()})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errs.New(errs.ConfigInvalid, "dbt project %q no longer exists", d.GetProject())
		}
		if err != nil {
			return nil, err
		}
		command := d.GetCommand()
		if command == apiv1.DbtCommand_DBT_COMMAND_UNSPECIFIED {
			command = apiv1.DbtCommand_DBT_COMMAND_BUILD
		}
		steps = append(steps, contract.PostSyncStep{Dbt: &contract.DbtStep{
			Project: d.GetProject(), Digest: proj.Digest,
			Command: strings.ToLower(strings.TrimPrefix(command.String(), "DBT_COMMAND_")),
			Select:  d.GetSelect(), Exclude: d.GetExclude(), Schema: d.GetSchema(),
		}})
	}
	return steps, nil
}

// connectors names the pipeline's source and destination connectors.
func (a *Activities) connectors(ctx context.Context, p db.Pipeline) ([]string, error) {
	src, err := a.Store.GetSourceByID(ctx, db.GetSourceByIDParams{OrgID: p.OrgID, ID: p.SourceID})
	if err != nil {
		return nil, err
	}
	dst, err := a.Store.GetDestinationByID(ctx, db.GetDestinationByIDParams{OrgID: p.OrgID, ID: p.DestinationID})
	if err != nil {
		return nil, err
	}
	var names []string
	for _, spec := range [][]byte{src.Spec, dst.Spec} {
		var c apiv1.ConnectorConfig
		if err := protojson.Unmarshal(spec, &c); err != nil {
			return nil, err
		}
		names = append(names, c.GetConnector())
	}
	return names, nil
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
		stats := struct {
			*contract.AttemptResult
			PostSync []contract.PostSyncResult `json:",omitempty"`
		}{in.Result, in.PostSync}
		if params.Stats, err = json.Marshal(stats); err != nil {
			return err
		}
	}
	return temporalx.ApplicationError(a.Store.FinishSync(ctx, params))
}

func (a *Activities) DropMirrorPartitions(ctx context.Context, in contract.DropMirrorPartitionsInput) ([]string, error) {
	dropped, err := a.Store.DropMirrorPartitions(ctx, in.Before)
	return dropped, temporalx.ApplicationError(err)
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
