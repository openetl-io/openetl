package api

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/internal/spec"
	"github.com/unsuitable001/openetl/internal/store/db"
)

// applySparkCluster stores a Spark cluster's settings as a new version, as
// applyConnector does a source's.
func (s *Server) applySparkCluster(ctx context.Context, q *db.Queries, doc *spec.Document, wsID uuid.UUID, provenance string, ch *changes) (*apiv1.AppliedResource, error) {
	var secrets []string
	for _, v := range doc.SparkCluster.GetSubmit().GetConf().GetFields() {
		if name, ok := secretRef(v); ok && !slices.Contains(secrets, name) {
			secrets = append(secrets, name)
		}
	}
	slices.Sort(secrets)
	if err := s.checkSecretsExist(ctx, q, wsID, doc.Workspace, secrets); err != nil {
		return nil, err
	}
	version, err := digest(doc.SparkCluster)
	if err != nil {
		return nil, err
	}
	stored, err := protojson.Marshal(doc.SparkCluster)
	if err != nil {
		return nil, err
	}
	res := &apiv1.AppliedResource{Kind: doc.Kind, Name: resourceName(doc.Workspace, "sparkClusters", doc.Name), Version: version}
	cur, err := q.GetSparkCluster(ctx, db.GetSparkClusterParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: doc.Name})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		res.Action, cur.ID = apiv1.ApplyAction_APPLY_ACTION_CREATED, uuid.New()
		err = q.InsertSparkCluster(ctx, db.InsertSparkClusterParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, ID: cur.ID, Name: doc.Name, Spec: stored, Version: version, Secrets: secrets})
	case err != nil:
		return nil, err
	case cur.Version == version:
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UNCHANGED
		return res, nil
	default:
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UPDATED
		err = q.UpdateSparkCluster(ctx, db.UpdateSparkClusterParams{OrgID: s.Tenant.Org, ID: cur.ID, Spec: stored, Version: version, Secrets: secrets})
	}
	if err != nil {
		return nil, err
	}
	ch.sparkClusters[cur.ID] = provenance
	return res, q.InsertSparkClusterVersion(ctx, db.InsertSparkClusterVersionParams{
		OrgID: s.Tenant.Org, ClusterID: cur.ID, Version: version, Spec: stored, Secrets: secrets, Provenance: provenance,
	})
}

// pinSparkCluster resolves a Spark step's cluster, and refuses a step the
// cluster can't run.
func pinSparkCluster(ctx context.Context, q *db.Queries, org, wsID uuid.UUID, workspace string, step *apiv1.SparkStep) (string, error) {
	c, err := q.ShareSparkCluster(ctx, db.ShareSparkClusterParams{OrgID: org, WorkspaceID: wsID, Name: step.GetCluster()})
	if errors.Is(err, pgx.ErrNoRows) {
		return "", invalid("Spark cluster %q doesn't exist in workspace %q", step.GetCluster(), workspace)
	}
	if err != nil {
		return "", err
	}
	var cs apiv1.SparkClusterSpec
	if err := protojson.Unmarshal(c.Spec, &cs); err != nil {
		return "", err
	}
	if strings.HasPrefix(cs.GetSubmit().GetMaster(), "spark://") && step.GetCode().GetPython() != "" {
		return "", invalid("Spark cluster %q is a standalone master, which can't run a Python application in cluster deploy mode; use a jar, or a YARN, Kubernetes or EMR cluster", step.GetCluster())
	}
	return c.Version, nil
}

// sparkClusterUsers lists the pipelines whose current versions use each
// cluster.
func (s *Server) sparkClusterUsers(ctx context.Context, q *db.Queries, ws string, wsID uuid.UUID, names []string) (map[string][]string, error) {
	rows, err := q.ListPipelinesUsingSparkClusters(ctx, db.ListPipelinesUsingSparkClustersParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Names: names})
	if err != nil {
		return nil, err
	}
	users := map[string][]string{}
	for _, r := range rows {
		users[r.Cluster] = append(users[r.Cluster], resourceName(ws, "pipelines", r.Name))
	}
	return users, nil
}

func toSparkCluster(ws string, r db.SparkCluster, usedBy []string) (*apiv1.SparkCluster, error) {
	c := &apiv1.SparkCluster{
		Name: resourceName(ws, "sparkClusters", r.Name), Spec: &apiv1.SparkClusterSpec{}, Version: r.Version, Etag: etag(r.Revision),
		CreateTime: timestamppb.New(r.CreateTime), UpdateTime: timestamppb.New(r.UpdateTime), UsedBy: usedBy,
	}
	return c, protojson.Unmarshal(r.Spec, c.Spec)
}

func (s *Server) GetSparkCluster(ctx context.Context, req *connect.Request[apiv1.GetSparkClusterRequest]) (*connect.Response[apiv1.GetSparkClusterResponse], error) {
	ws, id, err := parseName(req.Msg.GetName(), "sparkClusters")
	if err != nil {
		return nil, err
	}
	wsID, err := s.wsID(ctx, ws)
	if err != nil {
		return nil, err
	}
	r, err := s.Store.GetSparkCluster(ctx, db.GetSparkClusterParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: id})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("%s doesn't exist", req.Msg.GetName())
	}
	if err != nil {
		return nil, err
	}
	users, err := s.sparkClusterUsers(ctx, s.Store.Queries, ws, wsID, []string{id})
	if err != nil {
		return nil, err
	}
	c, err := toSparkCluster(ws, r, users[id])
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.GetSparkClusterResponse{SparkCluster: c}), nil
}

func (s *Server) ListSparkClusters(ctx context.Context, req *connect.Request[apiv1.ListSparkClustersRequest]) (*connect.Response[apiv1.ListSparkClustersResponse], error) {
	ws, wsID, after, n, err := s.listArgs(ctx, req.Msg.GetParent(), req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, err
	}
	rows, err := s.Store.ListSparkClusters(ctx, db.ListSparkClustersParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, After: after, MaxRows: n})
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListSparkClustersResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		res.NextPageToken = nextToken(rows[len(rows)-1].Name)
	}
	names := make([]string, len(rows))
	for i, r := range rows {
		names[i] = r.Name
	}
	users, err := s.sparkClusterUsers(ctx, s.Store.Queries, ws, wsID, names)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		c, err := toSparkCluster(ws, r, users[r.Name])
		if err != nil {
			return nil, err
		}
		res.SparkClusters = append(res.SparkClusters, c)
	}
	return connect.NewResponse(res), nil
}

// DeleteSparkCluster deletes its versions too, so older pipeline versions that
// pin one can no longer run their Spark steps.
func (s *Server) DeleteSparkCluster(ctx context.Context, req *connect.Request[apiv1.DeleteSparkClusterRequest]) (*connect.Response[apiv1.DeleteSparkClusterResponse], error) {
	ws, id, err := parseName(req.Msg.GetName(), "sparkClusters")
	if err != nil {
		return nil, err
	}
	wsID, err := s.wsID(ctx, ws)
	if err != nil {
		return nil, err
	}
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		// The lock waits for applies that pin the cluster, so their
		// pipelines are seen below.
		cur, err := q.LockSparkCluster(ctx, db.LockSparkClusterParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: id})
		if errors.Is(err, pgx.ErrNoRows) {
			return notFound("%s doesn't exist", req.Msg.GetName())
		}
		if err != nil {
			return err
		}
		if err := checkEtag(req.Msg.GetEtag(), cur.Revision); err != nil {
			return err
		}
		users, err := s.sparkClusterUsers(ctx, q, ws, wsID, []string{id})
		if err != nil {
			return err
		}
		if u := users[id]; len(u) > 0 {
			return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s is used by %s", req.Msg.GetName(), strings.Join(u, ", ")))
		}
		return q.DeleteSparkCluster(ctx, db.DeleteSparkClusterParams{OrgID: s.Tenant.Org, ID: cur.ID})
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.DeleteSparkClusterResponse{}), nil
}

// pinnedSparkClusters is what a sync's runner needs of the clusters a
// pipeline version pins: their settings, with the secrets this server holds
// filled in.
func (s *Server) pinnedSparkClusters(ctx context.Context, wsID, pipeline uuid.UUID, version string) (map[string]*dataplanev1.PinnedSparkCluster, error) {
	rows, err := s.Store.GetPinnedSparkClusters(ctx, db.GetPinnedSparkClustersParams{OrgID: s.Tenant.Org, PipelineID: pipeline, Version: version})
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	out := make(map[string]*dataplanev1.PinnedSparkCluster, len(rows))
	for _, r := range rows {
		p := &dataplanev1.PinnedSparkCluster{Version: r.Version, Spec: &apiv1.SparkClusterSpec{}}
		if err := protojson.Unmarshal(r.Spec, p.Spec); err != nil {
			return nil, err
		}
		if sub := p.Spec.GetSubmit(); sub != nil {
			conf := &apiv1.ConnectorConfig{Config: sub.GetConf()}
			if p.Secrets, err = s.resolveSecrets(ctx, "Spark cluster "+r.Name, wsID, conf, r.Secrets); err != nil {
				return nil, err
			}
		}
		out[r.Name] = p
	}
	return out, nil
}
