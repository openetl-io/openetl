package api

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/internal/events"
	"github.com/unsuitable001/openetl/internal/orchestration/schedules"
	"github.com/unsuitable001/openetl/internal/plan"
	"github.com/unsuitable001/openetl/internal/store"
	"github.com/unsuitable001/openetl/internal/store/db"
)

func etag(revision int64) string { return strconv.FormatInt(revision, 10) }

func checkEtag(want string, revision int64) error {
	if want != "" && want != etag(revision) {
		return connect.NewError(connect.CodeAborted, errors.New("etag doesn't match; the resource changed"))
	}
	return nil
}

func (s *Server) wsID(ctx context.Context, name string) (uuid.UUID, error) {
	ws, err := s.Store.GetWorkspace(ctx, db.GetWorkspaceParams{OrgID: s.Tenant.Org, Name: name})
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, notFound("workspace %q doesn't exist", name)
	}
	return ws.ID, err
}

// ---- Pipelines

func (s *Server) pipeline(ctx context.Context, ws string, p db.Pipeline) (*apiv1.Pipeline, error) {
	v, err := s.Store.GetPipelineVersion(ctx, db.GetPipelineVersionParams{OrgID: s.Tenant.Org, PipelineID: p.ID, Version: p.Version})
	if err != nil {
		return nil, err
	}
	out := &apiv1.Pipeline{
		Name:       resourceName(ws, "pipelines", p.Name),
		Spec:       &apiv1.PipelineSpec{},
		Version:    p.Version,
		Etag:       etag(p.Revision),
		CreateTime: timestamppb.New(p.CreateTime),
		UpdateTime: timestamppb.New(p.UpdateTime),
		Paused:     p.Paused,
	}
	if err := protojson.Unmarshal(v.Spec, out.Spec); err != nil {
		return nil, err
	}
	out.Status, err = s.pipelineStatus(ctx, out.Name, p.ID)
	return out, err
}

func (s *Server) pipelineStatus(ctx context.Context, name string, id uuid.UUID) (*apiv1.PipelineStatus, error) {
	out := &apiv1.PipelineStatus{}
	open, err := s.Store.CountOpenIncidents(ctx, db.CountOpenIncidentsParams{OrgID: s.Tenant.Org, PipelineIds: []uuid.UUID{id}})
	if err != nil {
		return nil, err
	}
	for _, o := range open {
		out.OpenIncidents = o.Open
	}
	latest, err := s.Store.ListSyncs(ctx, db.ListSyncsParams{OrgID: s.Tenant.Org, PipelineID: id, MaxRows: 1})
	if err != nil || len(latest) == 0 {
		return out, err
	}
	syncs, err := s.syncsOut(ctx, name, []db.GetSyncRow{db.GetSyncRow(latest[0])})
	if err != nil {
		return nil, err
	}
	out.LastSync = syncs[0]
	end, err := s.Store.LastSyncSuccess(ctx, db.LastSyncSuccessParams{OrgID: s.Tenant.Org, PipelineID: id})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return nil, err
	case end != nil:
		out.LastSuccessTime = timestamppb.New(*end)
	}
	return out, nil
}

func (s *Server) getPipeline(ctx context.Context, name string) (string, db.Pipeline, error) {
	ws, id, err := parseName(name, "pipelines")
	if err != nil {
		return "", db.Pipeline{}, err
	}
	wsID, err := s.wsID(ctx, ws)
	if err != nil {
		return "", db.Pipeline{}, err
	}
	p, err := s.Store.GetPipeline(ctx, db.GetPipelineParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: id})
	if errors.Is(err, pgx.ErrNoRows) {
		return "", p, notFound("%s doesn't exist", name)
	}
	return ws, p, err
}

func (s *Server) GetPipeline(ctx context.Context, req *connect.Request[apiv1.GetPipelineRequest]) (*connect.Response[apiv1.GetPipelineResponse], error) {
	ws, p, err := s.getPipeline(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	out, err := s.pipeline(ctx, ws, p)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.GetPipelineResponse{Pipeline: out}), nil
}

func (s *Server) ListPipelines(ctx context.Context, req *connect.Request[apiv1.ListPipelinesRequest]) (*connect.Response[apiv1.ListPipelinesResponse], error) {
	ws, err := parseParent(req.Msg.GetParent())
	if err != nil {
		return nil, err
	}
	wsID, err := s.wsID(ctx, ws)
	if err != nil {
		return nil, err
	}
	after, n, err := page(req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, err
	}
	rows, err := s.Store.ListPipelines(ctx, db.ListPipelinesParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, After: after, MaxRows: n})
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListPipelinesResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		res.NextPageToken = nextToken(rows[len(rows)-1].Name)
	}
	for _, r := range rows {
		p, err := s.pipeline(ctx, ws, r)
		if err != nil {
			return nil, err
		}
		res.Pipelines = append(res.Pipelines, p)
	}
	return connect.NewResponse(res), nil
}

// DeletePipeline removes its schedule and stops its supervisor first, so a
// failure leaves a pipeline that applying again runs.
func (s *Server) DeletePipeline(ctx context.Context, req *connect.Request[apiv1.DeletePipelineRequest]) (*connect.Response[apiv1.DeletePipelineResponse], error) {
	ws, p, err := s.getPipeline(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	if err := checkEtag(req.Msg.GetEtag(), p.Revision); err != nil {
		return nil, err
	}
	if err := s.Schedules.Delete(ctx, p.ID.String()); err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	if err := s.Schedules.StopStream(ctx, p.ID.String()); err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	// They go with the pipeline.
	flinkPins, err := s.Store.GetPinnedFlinkClusters(ctx, db.GetPinnedFlinkClustersParams{OrgID: s.Tenant.Org, PipelineID: p.ID, Version: p.Version})
	if err != nil {
		return nil, err
	}
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		n, err := q.DeletePipeline(ctx, db.DeletePipelineParams{OrgID: s.Tenant.Org, ID: p.ID})
		if err != nil || n == 0 {
			return err
		}
		return s.recordPipeline(ctx, q, events.PipelineDeleted, ws, p)
	})
	if err != nil {
		return nil, err
	}
	s.teardown(ctx, p)
	s.teardownFlink(ctx, p, flinkPins)
	return connect.NewResponse(&apiv1.DeletePipelineResponse{}), nil
}

// recordPipeline records an event about a pipeline, in q's transaction.
func (s *Server) recordPipeline(ctx context.Context, q *db.Queries, typ, ws string, p db.Pipeline) error {
	_, err := events.Record(ctx, q, s.Tenant.Org, events.Event{
		Workspace: p.WorkspaceID, Type: typ, Pipeline: p.Name,
		Data: events.Pipeline{Pipeline: resourceName(ws, "pipelines", p.Name), Version: p.Version},
	})
	return err
}

func (s *Server) PausePipeline(ctx context.Context, req *connect.Request[apiv1.PausePipelineRequest]) (*connect.Response[apiv1.PausePipelineResponse], error) {
	p, err := s.setPaused(ctx, req.Msg.GetName(), true)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.PausePipelineResponse{Pipeline: p}), nil
}

func (s *Server) ResumePipeline(ctx context.Context, req *connect.Request[apiv1.ResumePipelineRequest]) (*connect.Response[apiv1.ResumePipelineResponse], error) {
	p, err := s.setPaused(ctx, req.Msg.GetName(), false)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.ResumePipelineResponse{Pipeline: p}), nil
}

// setPaused records the state first: syncs and supervisors check it before
// they run, so a schedule left behind by a failure can't run a paused
// pipeline. Calling it again repairs the schedule, or for a continuous
// pipeline, has its supervisor stop or start its job.
func (s *Server) setPaused(ctx context.Context, name string, paused bool) (*apiv1.Pipeline, error) {
	ws, p, err := s.getPipeline(ctx, name)
	if err != nil {
		return nil, err
	}
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		n, err := q.SetPipelinePaused(ctx, db.SetPipelinePausedParams{OrgID: s.Tenant.Org, ID: p.ID, Paused: paused})
		if err != nil || n == 0 {
			return err
		}
		typ := events.PipelineResumed
		if paused {
			typ = events.PipelinePaused
		}
		return s.recordPipeline(ctx, q, typ, ws, p)
	})
	if err != nil {
		return nil, err
	}
	p.Paused = paused
	out, err := s.pipeline(ctx, ws, p)
	if err != nil {
		return nil, err
	}
	if plan.Continuous(out.GetSpec()) {
		err = s.Schedules.Refresh(ctx, s.Tenant.Org.String(), p.ID.String())
	} else {
		err = s.Schedules.SetPaused(ctx, p.ID.String(), paused)
	}
	if err != nil {
		s.log().Error("schedule pause failed", "pipeline", p.ID, "paused", paused, "err", err)
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("the pipeline's state was stored, but what runs it couldn't be changed; try again: %w", err))
	}
	return out, nil
}

func (s *Server) SyncPipeline(ctx context.Context, req *connect.Request[apiv1.SyncPipelineRequest]) (*connect.Response[apiv1.SyncPipelineResponse], error) {
	_, p, err := s.getPipeline(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	if p.Paused {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s is paused; resume it first", req.Msg.GetName()))
	}
	v, err := s.Store.GetPipelineVersion(ctx, db.GetPipelineVersionParams{OrgID: s.Tenant.Org, PipelineID: p.ID, Version: p.Version})
	if err != nil {
		return nil, err
	}
	var spec apiv1.PipelineSpec
	if err := protojson.Unmarshal(v.Spec, &spec); err != nil {
		return nil, err
	}
	if plan.Continuous(&spec) {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s is continuous, so it has no syncs to start", req.Msg.GetName()))
	}
	err = s.Schedules.Trigger(ctx, p.ID.String())
	switch {
	case errors.Is(err, schedules.ErrSyncRunning):
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("a sync of %s is running", req.Msg.GetName()))
	case err != nil:
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewResponse(&apiv1.SyncPipelineResponse{}), nil
}

// ---- Sources and destinations

type connection struct {
	name             string
	id, workspace    uuid.UUID
	spec             []byte
	version          string
	revision         int64
	created, updated time.Time
	// The connector version's digest.
	connector string
	// The secrets its config names.
	secrets []string
}

func fromSource(r db.Source) connection {
	return connection{r.Name, r.ID, r.WorkspaceID, r.Spec, r.Version, r.Revision, r.CreateTime, r.UpdateTime, r.ConnectorDigest, r.Secrets}
}

func fromDestination(r db.Destination) connection {
	return connection{r.Name, r.ID, r.WorkspaceID, r.Spec, r.Version, r.Revision, r.CreateTime, r.UpdateTime, r.ConnectorDigest, r.Secrets}
}

func (c connection) config() (*apiv1.ConnectorConfig, error) {
	cfg := &apiv1.ConnectorConfig{}
	return cfg, protojson.Unmarshal(c.spec, cfg)
}

func (c connection) source(ws string, versions map[string]*apiv1.ConnectorVersion) (*apiv1.Source, error) {
	cfg, err := c.config()
	return &apiv1.Source{Name: resourceName(ws, "sources", c.name), Spec: cfg, Version: c.version, Etag: etag(c.revision),
		CreateTime: timestamppb.New(c.created), UpdateTime: timestamppb.New(c.updated), Connector: versions[c.connector]}, err
}

func (c connection) destination(ws string, versions map[string]*apiv1.ConnectorVersion) (*apiv1.Destination, error) {
	cfg, err := c.config()
	return &apiv1.Destination{Name: resourceName(ws, "destinations", c.name), Spec: cfg, Version: c.version, Etag: etag(c.revision),
		CreateTime: timestamppb.New(c.created), UpdateTime: timestamppb.New(c.updated), Connector: versions[c.connector]}, err
}

// connectorVersions returns the applied connector versions cs run, by
// digest.
func (s *Server) connectorVersions(ctx context.Context, cs ...connection) (map[string]*apiv1.ConnectorVersion, error) {
	var digests []string
	for _, c := range cs {
		if c.connector != "" {
			digests = append(digests, c.connector)
		}
	}
	out := map[string]*apiv1.ConnectorVersion{}
	if len(digests) == 0 {
		return out, nil
	}
	rows, err := s.Store.ConnectorVersionsByDigest(ctx, db.ConnectorVersionsByDigestParams{OrgID: s.Tenant.Org, Digests: digests})
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		out[r.Digest] = &apiv1.ConnectorVersion{
			Name: connectorName(r.Name, r.Version), Version: r.Version, Digest: r.Digest,
			Author: r.Author, Logo: r.Logo, ReleaseTag: r.ReleaseTag, ReleaseUrl: r.ReleaseUrl,
			Description: r.Description, License: r.License, Repository: r.Repository, Docs: r.Docs, Tags: r.Tags,
		}
	}
	return out, nil
}

func (s *Server) getSource(ctx context.Context, name string) (string, connection, error) {
	ws, id, err := parseName(name, "sources")
	if err != nil {
		return "", connection{}, err
	}
	wsID, err := s.wsID(ctx, ws)
	if err != nil {
		return "", connection{}, err
	}
	r, err := s.Store.GetSource(ctx, db.GetSourceParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: id})
	if errors.Is(err, pgx.ErrNoRows) {
		return "", connection{}, notFound("%s doesn't exist", name)
	}
	return ws, fromSource(r), err
}

func (s *Server) getDestination(ctx context.Context, name string) (string, connection, error) {
	ws, id, err := parseName(name, "destinations")
	if err != nil {
		return "", connection{}, err
	}
	wsID, err := s.wsID(ctx, ws)
	if err != nil {
		return "", connection{}, err
	}
	r, err := s.Store.GetDestination(ctx, db.GetDestinationParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: id})
	if errors.Is(err, pgx.ErrNoRows) {
		return "", connection{}, notFound("%s doesn't exist", name)
	}
	return ws, fromDestination(r), err
}

func (s *Server) GetSource(ctx context.Context, req *connect.Request[apiv1.GetSourceRequest]) (*connect.Response[apiv1.GetSourceResponse], error) {
	ws, c, err := s.getSource(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	versions, err := s.connectorVersions(ctx, c)
	if err != nil {
		return nil, err
	}
	out, err := c.source(ws, versions)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.GetSourceResponse{Source: out}), nil
}

func (s *Server) GetDestination(ctx context.Context, req *connect.Request[apiv1.GetDestinationRequest]) (*connect.Response[apiv1.GetDestinationResponse], error) {
	ws, c, err := s.getDestination(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	versions, err := s.connectorVersions(ctx, c)
	if err != nil {
		return nil, err
	}
	out, err := c.destination(ws, versions)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.GetDestinationResponse{Destination: out}), nil
}

func (s *Server) ListSources(ctx context.Context, req *connect.Request[apiv1.ListSourcesRequest]) (*connect.Response[apiv1.ListSourcesResponse], error) {
	ws, wsID, after, n, err := s.listArgs(ctx, req.Msg.GetParent(), req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, err
	}
	rows, err := s.Store.ListSources(ctx, db.ListSourcesParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, After: after, MaxRows: n})
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListSourcesResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		res.NextPageToken = nextToken(rows[len(rows)-1].Name)
	}
	cs := make([]connection, len(rows))
	for i, r := range rows {
		cs[i] = fromSource(r)
	}
	versions, err := s.connectorVersions(ctx, cs...)
	if err != nil {
		return nil, err
	}
	for _, c := range cs {
		out, err := c.source(ws, versions)
		if err != nil {
			return nil, err
		}
		res.Sources = append(res.Sources, out)
	}
	return connect.NewResponse(res), nil
}

func (s *Server) ListDestinations(ctx context.Context, req *connect.Request[apiv1.ListDestinationsRequest]) (*connect.Response[apiv1.ListDestinationsResponse], error) {
	ws, wsID, after, n, err := s.listArgs(ctx, req.Msg.GetParent(), req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, err
	}
	rows, err := s.Store.ListDestinations(ctx, db.ListDestinationsParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, After: after, MaxRows: n})
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListDestinationsResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		res.NextPageToken = nextToken(rows[len(rows)-1].Name)
	}
	cs := make([]connection, len(rows))
	for i, r := range rows {
		cs[i] = fromDestination(r)
	}
	versions, err := s.connectorVersions(ctx, cs...)
	if err != nil {
		return nil, err
	}
	for _, c := range cs {
		out, err := c.destination(ws, versions)
		if err != nil {
			return nil, err
		}
		res.Destinations = append(res.Destinations, out)
	}
	return connect.NewResponse(res), nil
}

func (s *Server) listArgs(ctx context.Context, parent string, size int32, token string) (string, uuid.UUID, string, int32, error) {
	ws, err := parseParent(parent)
	if err != nil {
		return "", uuid.Nil, "", 0, err
	}
	wsID, err := s.wsID(ctx, ws)
	if err != nil {
		return "", uuid.Nil, "", 0, err
	}
	after, n, err := page(size, token)
	return ws, wsID, after, n, err
}

func inUse(err error, name string) error {
	if store.IsForeignKeyViolation(err) {
		return connect.NewError(connect.CodeFailedPrecondition, errors.New(name+" is used by a pipeline"))
	}
	return err
}

func (s *Server) DeleteSource(ctx context.Context, req *connect.Request[apiv1.DeleteSourceRequest]) (*connect.Response[apiv1.DeleteSourceResponse], error) {
	_, c, err := s.getSource(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	if err := checkEtag(req.Msg.GetEtag(), c.revision); err != nil {
		return nil, err
	}
	if _, err := s.Store.DeleteSource(ctx, db.DeleteSourceParams{OrgID: s.Tenant.Org, ID: c.id}); err != nil {
		return nil, inUse(err, req.Msg.GetName())
	}
	return connect.NewResponse(&apiv1.DeleteSourceResponse{}), nil
}

func (s *Server) DeleteDestination(ctx context.Context, req *connect.Request[apiv1.DeleteDestinationRequest]) (*connect.Response[apiv1.DeleteDestinationResponse], error) {
	_, c, err := s.getDestination(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	if err := checkEtag(req.Msg.GetEtag(), c.revision); err != nil {
		return nil, err
	}
	if _, err := s.Store.DeleteDestination(ctx, db.DeleteDestinationParams{OrgID: s.Tenant.Org, ID: c.id}); err != nil {
		return nil, inUse(err, req.Msg.GetName())
	}
	return connect.NewResponse(&apiv1.DeleteDestinationResponse{}), nil
}

// ---- Data plane

func (s *Server) GetSyncSpec(ctx context.Context, req *connect.Request[dataplanev1.GetSyncSpecRequest]) (*connect.Response[dataplanev1.GetSyncSpecResponse], error) {
	audit(ctx).set("pipelineId", req.Msg.GetPipelineId())
	audit(ctx).set("version", req.Msg.GetVersion())
	id, err := uuid.Parse(req.Msg.GetPipelineId())
	if err != nil {
		return nil, invalid("pipeline_id: %v", err)
	}
	row, err := s.Store.GetSyncSpec(ctx, db.GetSyncSpecParams{OrgID: s.Tenant.Org, PipelineID: id, Version: req.Msg.GetVersion()})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("pipeline %s has no version %q, or its source or destination was deleted", id, req.Msg.GetVersion())
	}
	if err != nil {
		return nil, err
	}
	audit(ctx).touched(resourceName(row.Workspace, "pipelines", row.Pipeline))
	if err := onDataPlane(ctx, row.DataPlaneID, fmt.Sprintf("pipeline %s version %s", id, row.Version)); err != nil {
		return nil, err
	}
	res := &dataplanev1.GetSyncSpecResponse{
		OrgId:       s.Tenant.Org.String(),
		PipelineId:  id.String(),
		Version:     row.Version,
		DataPlane:   row.DataPlaneID,
		Pipeline:    &apiv1.PipelineSpec{},
		Source:      &apiv1.ConnectorConfig{},
		Destination: &apiv1.ConnectorConfig{},

		SourceConnector:      row.SourceConnector,
		DestinationConnector: row.DestinationConnector,
	}
	if err := protojson.Unmarshal(row.PipelineSpec, res.Pipeline); err != nil {
		return nil, err
	}
	if err := protojson.Unmarshal(row.SourceSpec, res.Source); err != nil {
		return nil, err
	}
	if err := protojson.Unmarshal(row.DestinationSpec, res.Destination); err != nil {
		return nil, err
	}
	if res.SourceSecrets, err = s.resolveSecrets(ctx, "the source", row.WorkspaceID, res.Source, row.SourceSecrets); err != nil {
		return nil, err
	}
	if res.DestinationSecrets, err = s.resolveSecrets(ctx, "the destination", row.WorkspaceID, res.Destination, row.DestinationSecrets); err != nil {
		return nil, err
	}
	if res.SparkClusters, err = s.pinnedSparkClusters(ctx, row.WorkspaceID, id, row.Version); err != nil {
		return nil, err
	}
	if keys := res.Pipeline.GetSparkAccess().GetKeys(); keys != nil {
		if res.SparkAccessSecrets, err = s.resolveSecrets(ctx, "the pipeline's sparkAccess", row.WorkspaceID, &apiv1.ConnectorConfig{Config: keys}, row.AccessSecrets); err != nil {
			return nil, err
		}
	}
	if res.FlinkClusters, err = s.pinnedFlinkClusters(ctx, row.WorkspaceID, id, row.Version); err != nil {
		return nil, err
	}
	if keys := res.Pipeline.GetFlinkAccess().GetKeys(); keys != nil {
		if res.FlinkAccessSecrets, err = s.resolveSecrets(ctx, "the pipeline's flinkAccess", row.WorkspaceID, &apiv1.ConnectorConfig{Config: keys}, row.AccessSecrets); err != nil {
			return nil, err
		}
	}
	return connect.NewResponse(res), nil
}
