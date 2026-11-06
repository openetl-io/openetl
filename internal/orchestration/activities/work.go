package activities

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/dispatch"
	"github.com/openetl-io/openetl/internal/orchestration/sizing"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/spec"
	"github.com/openetl-io/openetl/internal/store/db"
)

// EnqueueWork sends a workflow's work to its data plane, and places it if
// it's an attempt with room waiting. A pipeline's attempt is sized here, not in
// its workflow, so a size adds nothing to the workflow's history.
func (a *Activities) EnqueueWork(ctx context.Context, in contract.WorkInput) error {
	needs, err := a.prepare(ctx, &in)
	if err != nil {
		return err
	}
	return a.Dispatch.Enqueue(ctx, in, needs)
}

// prepare sizes a pipeline's attempt, and returns what a runner must have to
// run its connector versions.
func (a *Activities) prepare(ctx context.Context, in *contract.WorkInput) ([]dispatch.Need, error) {
	if in.PipelineID == "" {
		return nil, nil
	}
	org, pipeline, err := ids(in.OrgID, in.PipelineID)
	if err != nil {
		return nil, err
	}
	v, err := a.Store.GetSyncSpec(ctx, db.GetSyncSpecParams{OrgID: org, PipelineID: pipeline, Version: in.Version})
	if err != nil {
		return nil, err
	}
	var needs []dispatch.Need
	var hints []string
	for _, r := range []struct{ role, digest string }{{"source", v.SourceConnector}, {"destination", v.DestinationConnector}} {
		if r.digest == "" {
			continue
		}
		role, err := a.role(ctx, org, r.digest, r.role)
		if err != nil {
			return nil, err
		}
		if role != nil {
			needs = append(needs, dispatch.NeedOf(role))
			hints = append(hints, role.Resources.Hint)
		}
	}
	if in.Size != (resources.Size{}) {
		return needs, nil
	}
	var pipelineSpec apiv1.PipelineSpec
	if err := protojson.Unmarshal(v.PipelineSpec, &pipelineSpec); err != nil {
		return nil, err
	}
	sp := sizing.SpecOf(pipelineSpec.GetResources())
	history, err := sizing.History(ctx, a.Store.Queries, org, pipeline)
	if err != nil {
		return nil, err
	}
	runners, err := a.Dispatch.Capacities(ctx, org, v.DataPlaneID, sp.Pool)
	if err != nil {
		return nil, err
	}
	c := sizing.Choose(sizing.Input{Spec: sp, Hint: sizing.LargestHint(hints...), History: history, Runners: runners})
	in.Size, in.SizeReason, in.Pool = c.Size, string(c.Reason), sp.Pool
	return needs, nil
}

func (a *Activities) role(ctx context.Context, org uuid.UUID, digest, role string) (*spec.Role, error) {
	artifact, err := a.Store.GetArtifact(ctx, db.GetArtifactParams{OrgID: org, Digest: digest})
	if err != nil {
		return nil, err
	}
	ver, err := connectorhost.ParseVersion(artifact.Content)
	if err != nil {
		return nil, err
	}
	r := ver.Roles[role]
	if r == nil {
		return nil, nil
	}
	return r.Role, nil
}

// GetWork reports whether work finished, and how.
func (a *Activities) GetWork(ctx context.Context, ref contract.WorkRef) (contract.WorkStatus, error) {
	return a.Dispatch.Status(ctx, ref)
}

// CancelWork stops work.
func (a *Activities) CancelWork(ctx context.Context, ref contract.WorkRef) error {
	return a.Dispatch.Cancel(ctx, ref)
}
