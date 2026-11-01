package api

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/dbt"
	"github.com/unsuitable001/openetl/internal/spec"
	"github.com/unsuitable001/openetl/internal/store/db"
)

// applyDbtProject stores a dbt project's bundle as an artifact, and points
// the project's name at it.
func (s *Server) applyDbtProject(ctx context.Context, q *db.Queries, doc *spec.Document, wsID uuid.UUID) (*apiv1.AppliedResource, error) {
	bundle, _, err := dbt.Canonical(doc.Bundle)
	if err != nil {
		return nil, invalid("%v", err)
	}
	digest := connectorhost.Digest(bundle)
	res := &apiv1.AppliedResource{Kind: doc.Kind, Name: resourceName(doc.Workspace, "dbtProjects", doc.Name), Version: digest}
	cur, err := q.GetDbtProject(ctx, db.GetDbtProjectParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: doc.Name})
	created := errors.Is(err, pgx.ErrNoRows)
	switch {
	case err == nil && cur.Digest == digest:
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UNCHANGED
		return res, nil
	case err != nil && !created:
		return nil, err
	}
	if err := q.InsertArtifact(ctx, db.InsertArtifactParams{OrgID: s.Tenant.Org, Digest: digest, MediaType: dbt.MediaType, Content: bundle}); err != nil {
		return nil, err
	}
	if created {
		res.Action = apiv1.ApplyAction_APPLY_ACTION_CREATED
		err = q.InsertDbtProject(ctx, db.InsertDbtProjectParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, ID: uuid.New(), Name: doc.Name, Digest: digest})
	} else {
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UPDATED
		err = q.UpdateDbtProject(ctx, db.UpdateDbtProjectParams{OrgID: s.Tenant.Org, ID: cur.ID, Digest: digest})
	}
	return res, err
}

// checkPostSync fails unless every dbt project a pipeline's steps name is in
// its workspace.
func (s *Server) checkPostSync(ctx context.Context, q *db.Queries, p *apiv1.PipelineSpec, wsID uuid.UUID, workspace string) error {
	for _, step := range p.GetPostSync() {
		name := step.GetDbt().GetProject()
		_, err := q.GetDbtProject(ctx, db.GetDbtProjectParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: name})
		if errors.Is(err, pgx.ErrNoRows) {
			return invalid("dbt project %q doesn't exist in workspace %q", name, workspace)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
