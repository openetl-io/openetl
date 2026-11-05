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
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/spec"
	"github.com/unsuitable001/openetl/internal/store/db"
)

// flinkTarget is a Flink cluster's secret reference outside conf, under the
// name PinnedFlinkCluster's target_secrets give it.
func flinkTarget(c *apiv1.FlinkClusterSpec) (string, *structpb.Value) {
	if k := c.GetKubernetes(); k.GetKubeconfig() != nil {
		return "kubeconfig", k.GetKubeconfig()
	}
	if r := c.GetRest(); r.GetAuth() != nil {
		return "auth", r.GetAuth()
	}
	return "", nil
}

// applyFlinkCluster stores a Flink cluster's settings as a new version, as
// applySparkCluster does a Spark cluster's.
func (s *Server) applyFlinkCluster(ctx context.Context, q *db.Queries, doc *spec.Document, wsID uuid.UUID, provenance string, ch *changes) (*apiv1.AppliedResource, error) {
	secrets := []string{}
	var refs []*structpb.Value
	for _, v := range doc.FlinkCluster.GetConf().GetFields() {
		refs = append(refs, v)
	}
	if _, v := flinkTarget(doc.FlinkCluster); v != nil {
		refs = append(refs, v)
	}
	for _, v := range refs {
		if name, ok := secretRef(v); ok && !slices.Contains(secrets, name) {
			secrets = append(secrets, name)
		}
	}
	slices.Sort(secrets)
	if err := s.checkSecretsExist(ctx, q, wsID, doc.Workspace, secrets); err != nil {
		return nil, err
	}
	version, err := digest(doc.FlinkCluster)
	if err != nil {
		return nil, err
	}
	stored, err := protojson.Marshal(doc.FlinkCluster)
	if err != nil {
		return nil, err
	}
	res := &apiv1.AppliedResource{Kind: doc.Kind, Name: resourceName(doc.Workspace, "flinkClusters", doc.Name), Version: version}
	cur, err := q.GetFlinkCluster(ctx, db.GetFlinkClusterParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: doc.Name})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		res.Action, cur.ID = apiv1.ApplyAction_APPLY_ACTION_CREATED, uuid.New()
		err = q.InsertFlinkCluster(ctx, db.InsertFlinkClusterParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, ID: cur.ID, Name: doc.Name, Spec: stored, Version: version, Secrets: secrets})
	case err != nil:
		return nil, err
	case cur.Version == version:
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UNCHANGED
		return res, nil
	default:
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UPDATED
		err = q.UpdateFlinkCluster(ctx, db.UpdateFlinkClusterParams{OrgID: s.Tenant.Org, ID: cur.ID, Spec: stored, Version: version, Secrets: secrets})
	}
	if err != nil {
		return nil, err
	}
	ch.flinkClusters[cur.ID] = provenance
	return res, q.InsertFlinkClusterVersion(ctx, db.InsertFlinkClusterVersionParams{
		OrgID: s.Tenant.Org, ClusterID: cur.ID, Version: version, Spec: stored, Secrets: secrets, Provenance: provenance,
	})
}

// pinFlinkCluster resolves a Flink transform's cluster, and checks the
// pipeline's flinkAccess against it. It returns the cluster's version.
func pinFlinkCluster(ctx context.Context, q *db.Queries, org, wsID uuid.UUID, workspace string, p *apiv1.PipelineSpec) (string, error) {
	cluster := p.GetTransform().GetFlink().GetCluster()
	c, err := q.ShareFlinkCluster(ctx, db.ShareFlinkClusterParams{OrgID: org, WorkspaceID: wsID, Name: cluster})
	if errors.Is(err, pgx.ErrNoRows) {
		return "", invalid("Flink cluster %q doesn't exist in workspace %q", cluster, workspace)
	}
	if err != nil {
		return "", err
	}
	var cs apiv1.FlinkClusterSpec
	if err := protojson.Unmarshal(c.Spec, &cs); err != nil {
		return "", err
	}
	a := p.GetFlinkAccess()
	switch {
	case a.GetEmrRole() != "" && cs.GetEmr() == nil:
		return "", invalid("flinkAccess.emrRole is for EMR clusters, and Flink cluster %q isn't one", cluster)
	case a.GetKeys() != nil && cs.GetEmr() != nil:
		return "", invalid("flinkAccess.keys are for clusters other than EMR's, and Flink cluster %q is one; give it emrRole", cluster)
	case a.GetKeys() != nil && cs.GetRest() != nil:
		return "", invalid("flinkAccess.keys: Flink cluster %q is a session cluster, which reads its filesystems' settings when it starts, so a job can't bring its own keys; give the cluster access to the data plane's store", cluster)
	}
	return c.Version, nil
}

// flinkClusterUsers lists the pipelines whose current versions use each
// cluster.
func (s *Server) flinkClusterUsers(ctx context.Context, q *db.Queries, ws string, wsID uuid.UUID, names []string) (map[string][]string, error) {
	rows, err := q.ListPipelinesUsingFlinkClusters(ctx, db.ListPipelinesUsingFlinkClustersParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Names: names})
	if err != nil {
		return nil, err
	}
	users := map[string][]string{}
	for _, r := range rows {
		users[r.Cluster] = append(users[r.Cluster], resourceName(ws, "pipelines", r.Name))
	}
	return users, nil
}

func toFlinkCluster(ws string, r db.FlinkCluster, usedBy []string) (*apiv1.FlinkCluster, error) {
	c := &apiv1.FlinkCluster{
		Name: resourceName(ws, "flinkClusters", r.Name), Spec: &apiv1.FlinkClusterSpec{}, Version: r.Version, Etag: etag(r.Revision),
		CreateTime: timestamppb.New(r.CreateTime), UpdateTime: timestamppb.New(r.UpdateTime), UsedBy: usedBy,
	}
	return c, protojson.Unmarshal(r.Spec, c.Spec)
}

func (s *Server) GetFlinkCluster(ctx context.Context, req *connect.Request[apiv1.GetFlinkClusterRequest]) (*connect.Response[apiv1.GetFlinkClusterResponse], error) {
	ws, id, err := parseName(req.Msg.GetName(), "flinkClusters")
	if err != nil {
		return nil, err
	}
	wsID, err := s.wsID(ctx, ws)
	if err != nil {
		return nil, err
	}
	r, err := s.Store.GetFlinkCluster(ctx, db.GetFlinkClusterParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: id})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("%s doesn't exist", req.Msg.GetName())
	}
	if err != nil {
		return nil, err
	}
	users, err := s.flinkClusterUsers(ctx, s.Store.Queries, ws, wsID, []string{id})
	if err != nil {
		return nil, err
	}
	c, err := toFlinkCluster(ws, r, users[id])
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.GetFlinkClusterResponse{FlinkCluster: c}), nil
}

func (s *Server) ListFlinkClusters(ctx context.Context, req *connect.Request[apiv1.ListFlinkClustersRequest]) (*connect.Response[apiv1.ListFlinkClustersResponse], error) {
	ws, wsID, after, n, err := s.listArgs(ctx, req.Msg.GetParent(), req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, err
	}
	rows, err := s.Store.ListFlinkClusters(ctx, db.ListFlinkClustersParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, After: after, MaxRows: n})
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListFlinkClustersResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		res.NextPageToken = nextToken(rows[len(rows)-1].Name)
	}
	names := make([]string, len(rows))
	for i, r := range rows {
		names[i] = r.Name
	}
	users, err := s.flinkClusterUsers(ctx, s.Store.Queries, ws, wsID, names)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		c, err := toFlinkCluster(ws, r, users[r.Name])
		if err != nil {
			return nil, err
		}
		res.FlinkClusters = append(res.FlinkClusters, c)
	}
	return connect.NewResponse(res), nil
}

// DeleteFlinkCluster deletes its versions too, so older pipeline versions that
// pin one can no longer run their Flink transform.
func (s *Server) DeleteFlinkCluster(ctx context.Context, req *connect.Request[apiv1.DeleteFlinkClusterRequest]) (*connect.Response[apiv1.DeleteFlinkClusterResponse], error) {
	ws, id, err := parseName(req.Msg.GetName(), "flinkClusters")
	if err != nil {
		return nil, err
	}
	wsID, err := s.wsID(ctx, ws)
	if err != nil {
		return nil, err
	}
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		cur, err := q.LockFlinkCluster(ctx, db.LockFlinkClusterParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: id})
		if errors.Is(err, pgx.ErrNoRows) {
			return notFound("%s doesn't exist", req.Msg.GetName())
		}
		if err != nil {
			return err
		}
		if err := checkEtag(req.Msg.GetEtag(), cur.Revision); err != nil {
			return err
		}
		users, err := s.flinkClusterUsers(ctx, q, ws, wsID, []string{id})
		if err != nil {
			return err
		}
		if u := users[id]; len(u) > 0 {
			return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s is used by %s", req.Msg.GetName(), strings.Join(u, ", ")))
		}
		return q.DeleteFlinkCluster(ctx, db.DeleteFlinkClusterParams{OrgID: s.Tenant.Org, ID: cur.ID})
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.DeleteFlinkClusterResponse{}), nil
}

// pinnedFlinkClusters is what a runner needs of the Flink cluster a pipeline
// version pins: its settings, with the secrets this server holds filled in.
func (s *Server) pinnedFlinkClusters(ctx context.Context, wsID, pipeline uuid.UUID, version string) (map[string]*dataplanev1.PinnedFlinkCluster, error) {
	rows, err := s.Store.GetPinnedFlinkClusters(ctx, db.GetPinnedFlinkClustersParams{OrgID: s.Tenant.Org, PipelineID: pipeline, Version: version})
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	out := make(map[string]*dataplanev1.PinnedFlinkCluster, len(rows))
	for _, r := range rows {
		if out[r.Name], err = s.pinFlinkCluster(ctx, wsID, r.Name, r.Version, r.Spec, r.Secrets); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *Server) pinFlinkCluster(ctx context.Context, wsID uuid.UUID, name, version string, spec []byte, secrets []string) (*dataplanev1.PinnedFlinkCluster, error) {
	p := &dataplanev1.PinnedFlinkCluster{Version: version, Spec: &apiv1.FlinkClusterSpec{}}
	if err := protojson.Unmarshal(spec, p.Spec); err != nil {
		return nil, err
	}
	what := "Flink cluster " + name
	var err error
	if conf := p.Spec.GetConf(); conf != nil {
		if p.Secrets, err = s.resolveSecrets(ctx, what, wsID, &apiv1.ConnectorConfig{Config: conf}, secrets); err != nil {
			return nil, err
		}
	}
	if field, v := flinkTarget(p.Spec); v != nil {
		target := &apiv1.ConnectorConfig{Config: &structpb.Struct{Fields: map[string]*structpb.Value{field: v}}}
		if p.TargetSecrets, err = s.resolveSecrets(ctx, what, wsID, target, secrets); err != nil {
			return nil, err
		}
		resolved := target.GetConfig().GetFields()[field]
		if k := p.Spec.GetKubernetes(); k != nil {
			k.Kubeconfig = resolved
		} else {
			p.Spec.GetRest().Auth = resolved
		}
	}
	return p, nil
}

// teardownFlink starts a teardown of the deleted pipeline p's Flink jobs on
// the cluster its last version pinned, if any, without waiting for it: a
// job whose runner died before the deletion is left running, with its
// JobManager on Kubernetes, since no attempt follows to cancel it.
func (s *Server) teardownFlink(ctx context.Context, p db.Pipeline, pins []db.GetPinnedFlinkClustersRow) {
	ctx = context.WithoutCancel(ctx)
	for _, pin := range pins {
		in := contract.ConnectorOpInput{
			Op: contract.OpTeardownFlink, PipelineID: p.ID.String(),
			OrgID: s.Tenant.Org.String(), WorkspaceID: p.WorkspaceID.String(), DataPlane: p.DataPlaneID,
		}
		op := db.InsertConnectorOpParams{PipelineID: &p.ID, FlinkClusterID: &pin.ID, FlinkVersion: &pin.Version}
		err := s.insertOp(ctx, &in, op)
		if err == nil {
			err = s.Ops.Start(ctx, in)
		}
		if err != nil {
			s.log().Error("Flink teardown not started", "pipeline", p.ID, "cluster", pin.Name, "err", err)
		}
	}
}

func (s *Server) GetFlinkTeardown(ctx context.Context, req *connect.Request[dataplanev1.GetFlinkTeardownRequest]) (*connect.Response[dataplanev1.GetFlinkTeardownResponse], error) {
	audit(ctx).set("operation", req.Msg.GetOperationId())
	denied := connect.NewError(connect.CodePermissionDenied, fmt.Errorf("the API sent no Flink teardown %q to data plane %q", req.Msg.GetOperationId(), callerOf(ctx).dataPlane))
	id, err := uuid.Parse(req.Msg.GetOperationId())
	if err != nil {
		return nil, denied
	}
	r, err := s.Store.GetFlinkTeardown(ctx, db.GetFlinkTeardownParams{OrgID: s.Tenant.Org, ID: id, DataPlaneID: callerOf(ctx).dataPlane})
	if errors.Is(err, pgx.ErrNoRows) || err == nil && r.PipelineID == nil {
		return nil, denied
	}
	if err != nil {
		return nil, err
	}
	pin, err := s.pinFlinkCluster(ctx, r.WorkspaceID, r.Name, r.Version, r.Spec, r.Secrets)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&dataplanev1.GetFlinkTeardownResponse{OrgId: s.Tenant.Org.String(), PipelineId: r.PipelineID.String(), Cluster: pin}), nil
}
