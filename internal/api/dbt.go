package api

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/dbt"
	"github.com/unsuitable001/openetl/internal/spec"
	"github.com/unsuitable001/openetl/internal/store/db"
)

// applyDbtProject stores a dbt project's bundle as an artifact, and points
// the project's name at it.
func (s *Server) applyDbtProject(ctx context.Context, q *db.Queries, doc *spec.Document, wsID uuid.UUID, provenance string, ch *changes) (*apiv1.AppliedResource, error) {
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
		res.Action, cur.ID = apiv1.ApplyAction_APPLY_ACTION_CREATED, uuid.New()
		err = q.InsertDbtProject(ctx, db.InsertDbtProjectParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, ID: cur.ID, Name: doc.Name, Digest: digest})
	} else {
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UPDATED
		err = q.UpdateDbtProject(ctx, db.UpdateDbtProjectParams{OrgID: s.Tenant.Org, ID: cur.ID, Digest: digest})
	}
	ch.dbtProjects[cur.ID] = provenance
	return res, err
}

// dbtProjectUsers lists the pipelines whose current versions run each
// project.
func (s *Server) dbtProjectUsers(ctx context.Context, q *db.Queries, ws string, wsID uuid.UUID, names []string) (map[string][]string, error) {
	rows, err := q.ListPipelinesUsingDbtProjects(ctx, db.ListPipelinesUsingDbtProjectsParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Names: names})
	if err != nil {
		return nil, err
	}
	users := map[string][]string{}
	for _, r := range rows {
		users[r.Project] = append(users[r.Project], resourceName(ws, "pipelines", r.Name))
	}
	return users, nil
}

func toDbtProject(ws string, r db.DbtProject, usedBy []string) *apiv1.DbtProject {
	return &apiv1.DbtProject{
		Name: resourceName(ws, "dbtProjects", r.Name), Digest: r.Digest, Etag: etag(r.Revision),
		CreateTime: timestamppb.New(r.CreateTime), UpdateTime: timestamppb.New(r.UpdateTime), UsedBy: usedBy,
	}
}

func (s *Server) GetDbtProject(ctx context.Context, req *connect.Request[apiv1.GetDbtProjectRequest]) (*connect.Response[apiv1.GetDbtProjectResponse], error) {
	ws, id, err := parseName(req.Msg.GetName(), "dbtProjects")
	if err != nil {
		return nil, err
	}
	wsID, err := s.wsID(ctx, ws)
	if err != nil {
		return nil, err
	}
	r, err := s.Store.GetDbtProject(ctx, db.GetDbtProjectParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: id})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("%s doesn't exist", req.Msg.GetName())
	}
	if err != nil {
		return nil, err
	}
	users, err := s.dbtProjectUsers(ctx, s.Store.Queries, ws, wsID, []string{id})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.GetDbtProjectResponse{DbtProject: toDbtProject(ws, r, users[id])}), nil
}

func (s *Server) ListDbtProjects(ctx context.Context, req *connect.Request[apiv1.ListDbtProjectsRequest]) (*connect.Response[apiv1.ListDbtProjectsResponse], error) {
	ws, wsID, after, n, err := s.listArgs(ctx, req.Msg.GetParent(), req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, err
	}
	rows, err := s.Store.ListDbtProjects(ctx, db.ListDbtProjectsParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, After: after, MaxRows: n})
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListDbtProjectsResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		res.NextPageToken = nextToken(rows[len(rows)-1].Name)
	}
	names := make([]string, len(rows))
	for i, r := range rows {
		names[i] = r.Name
	}
	users, err := s.dbtProjectUsers(ctx, s.Store.Queries, ws, wsID, names)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		res.DbtProjects = append(res.DbtProjects, toDbtProject(ws, r, users[r.Name]))
	}
	return connect.NewResponse(res), nil
}

// DeleteDbtProject leaves its bundles: older pipeline versions pin them.
func (s *Server) DeleteDbtProject(ctx context.Context, req *connect.Request[apiv1.DeleteDbtProjectRequest]) (*connect.Response[apiv1.DeleteDbtProjectResponse], error) {
	ws, id, err := parseName(req.Msg.GetName(), "dbtProjects")
	if err != nil {
		return nil, err
	}
	wsID, err := s.wsID(ctx, ws)
	if err != nil {
		return nil, err
	}
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		// The lock waits for applies that pin the project, so their
		// pipelines are seen below.
		cur, err := q.LockDbtProject(ctx, db.LockDbtProjectParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: id})
		if errors.Is(err, pgx.ErrNoRows) {
			return notFound("%s doesn't exist", req.Msg.GetName())
		}
		if err != nil {
			return err
		}
		if err := checkEtag(req.Msg.GetEtag(), cur.Revision); err != nil {
			return err
		}
		users, err := s.dbtProjectUsers(ctx, q, ws, wsID, []string{id})
		if err != nil {
			return err
		}
		if u := users[id]; len(u) > 0 {
			return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s is used by %s", req.Msg.GetName(), strings.Join(u, ", ")))
		}
		return q.DeleteDbtProject(ctx, db.DeleteDbtProjectParams{OrgID: s.Tenant.Org, ID: cur.ID})
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.DeleteDbtProjectResponse{}), nil
}
