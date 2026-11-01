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
	v, err := a.Store.GetSyncSpec(ctx, db.GetSyncSpecParams{OrgID: org, PipelineID: pipeline, Version: p.Version})
	if err != nil {
		return res, err
	}
	connectors, err := connectorNames(v)
	if err != nil {
		return res, err
	}
	steps, err := postSync(v)
	if err != nil {
		return res, err
	}
	run := activity.GetInfo(ctx).WorkflowExecution
	// A UUIDv7 holds when the sync started, which data planes prune its log
	// by.
	sync, err := uuid.NewV7()
	if err != nil {
		return res, err
	}
	id, err := a.Store.StartSync(ctx, db.StartSyncParams{
		OrgID:      org,
		ID:         sync,
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

// postSync is the pipeline version's post-sync steps, with the bundles it
// pins.
func postSync(v db.GetSyncSpecRow) ([]contract.PostSyncStep, error) {
	var spec apiv1.PipelineSpec
	if err := protojson.Unmarshal(v.PipelineSpec, &spec); err != nil {
		return nil, err
	}
	var bundles map[string]string
	if err := json.Unmarshal(v.Dbt, &bundles); err != nil {
		return nil, err
	}
	var steps []contract.PostSyncStep
	for _, s := range spec.GetPostSync() {
		d := s.GetDbt()
		digest, ok := bundles[d.GetProject()]
		if !ok {
			return nil, errs.New(errs.ConfigInvalid, "pipeline version %s pins no bundle of dbt project %q", v.Version, d.GetProject())
		}
		command := d.GetCommand()
		if command == apiv1.DbtCommand_DBT_COMMAND_UNSPECIFIED {
			command = apiv1.DbtCommand_DBT_COMMAND_BUILD
		}
		steps = append(steps, contract.PostSyncStep{Dbt: &contract.DbtStep{
			Project: d.GetProject(), Digest: digest,
			Command: strings.ToLower(strings.TrimPrefix(command.String(), "DBT_COMMAND_")),
			Select:  d.GetSelect(), Exclude: d.GetExclude(), Schema: d.GetSchema(),
		}})
	}
	return steps, nil
}

// connectorNames names the pipeline version's source and destination
// connectors.
func connectorNames(v db.GetSyncSpecRow) ([]string, error) {
	var names []string
	for _, spec := range [][]byte{v.SourceSpec, v.DestinationSpec} {
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
