package activities

import (
	"context"

	"github.com/google/uuid"

	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/orchestration/dispatch"
	"github.com/unsuitable001/openetl/internal/platform/temporalx"
	"github.com/unsuitable001/openetl/internal/store/db"
)

// EnqueueWork sends a workflow's work to its data plane, and places it if
// it's an attempt with room waiting.
func (a *Activities) EnqueueWork(ctx context.Context, in contract.WorkInput) error {
	needs, err := a.needs(ctx, in)
	if err != nil {
		return temporalx.ApplicationError(err)
	}
	return temporalx.ApplicationError(a.Dispatch.Enqueue(ctx, in, needs))
}

// needs is what a runner must have to run an attempt's connector versions.
func (a *Activities) needs(ctx context.Context, in contract.WorkInput) ([]dispatch.Need, error) {
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
	for _, r := range []struct{ role, digest string }{{"source", v.SourceConnector}, {"destination", v.DestinationConnector}} {
		if r.digest == "" {
			continue
		}
		need, err := a.need(ctx, org, r.digest, r.role)
		if err != nil {
			return nil, err
		}
		if need != nil {
			needs = append(needs, *need)
		}
	}
	return needs, nil
}

func (a *Activities) need(ctx context.Context, org uuid.UUID, digest, role string) (*dispatch.Need, error) {
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
	n := dispatch.NeedOf(r.Role)
	return &n, nil
}

// GetWork reports whether work finished, and how.
func (a *Activities) GetWork(ctx context.Context, ref contract.WorkRef) (contract.WorkStatus, error) {
	st, err := a.Dispatch.Status(ctx, ref)
	return st, temporalx.ApplicationError(err)
}

// CancelWork stops work.
func (a *Activities) CancelWork(ctx context.Context, ref contract.WorkRef) error {
	return temporalx.ApplicationError(a.Dispatch.Cancel(ctx, ref))
}
