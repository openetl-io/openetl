package api

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/store/db"
)

func pinnedConfig(ws, collection string, name *string, version string, spec []byte, connector *string) (*apiv1.PinnedConfig, error) {
	out := &apiv1.PinnedConfig{Version: version}
	if name != nil {
		out.Name = resourceName(ws, collection, *name)
	}
	if connector != nil {
		out.Connector = *connector
	}
	if spec == nil {
		return out, nil
	}
	out.Spec = &apiv1.ConnectorConfig{}
	return out, protojson.Unmarshal(spec, out.Spec)
}

func pipelineVersion(ws string, p db.Pipeline, r db.ListPipelineVersionsRow) (*apiv1.PipelineVersion, error) {
	out := &apiv1.PipelineVersion{
		Name:       resourceName(ws, "pipelines", p.Name) + "/versions/" + r.Version,
		Spec:       &apiv1.PipelineSpec{},
		DataPlane:  r.DataPlaneID,
		Provenance: r.Provenance,
		CreateTime: timestamppb.New(r.CreateTime),
		Current:    r.Version == p.Version,
	}
	if err := protojson.Unmarshal(r.Spec, out.Spec); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(r.Dbt, &out.Dbt); err != nil {
		return nil, err
	}
	var err error
	if out.Source, err = pinnedConfig(ws, "sources", r.SourceName, r.SourceVersion, r.SourceSpec, r.SourceConnector); err != nil {
		return nil, err
	}
	out.Destination, err = pinnedConfig(ws, "destinations", r.DestinationName, r.DestinationVersion, r.DestinationSpec, r.DestinationConnector)
	return out, err
}

func (s *Server) GetPipelineVersion(ctx context.Context, req *connect.Request[apiv1.GetPipelineVersionRequest]) (*connect.Response[apiv1.GetPipelineVersionResponse], error) {
	pipeline, version, ok := strings.Cut(req.Msg.GetName(), "/versions/")
	if !ok || version == "" || strings.Contains(version, "/") {
		return nil, invalid("name %q isn't workspaces/{workspace}/pipelines/{pipeline}/versions/{version}", req.Msg.GetName())
	}
	ws, p, err := s.getPipeline(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	r, err := s.Store.GetPipelineVersionPins(ctx, db.GetPipelineVersionPinsParams{OrgID: s.Tenant.Org, PipelineID: p.ID, Version: version})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("%s doesn't exist", req.Msg.GetName())
	}
	if err != nil {
		return nil, err
	}
	out, err := pipelineVersion(ws, p, db.ListPipelineVersionsRow(r))
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.GetPipelineVersionResponse{PipelineVersion: out}), nil
}

func (s *Server) ListPipelineVersions(ctx context.Context, req *connect.Request[apiv1.ListPipelineVersionsRequest]) (*connect.Response[apiv1.ListPipelineVersionsResponse], error) {
	ws, p, err := s.getPipeline(ctx, req.Msg.GetParent())
	if err != nil {
		return nil, err
	}
	after, n, err := page(req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, err
	}
	params := db.ListPipelineVersionsParams{OrgID: s.Tenant.Org, PipelineID: p.ID, MaxRows: n}
	if after != "" {
		at, version, _ := strings.Cut(after, " ")
		t, err := time.Parse(time.RFC3339Nano, at)
		if err != nil || version == "" {
			return nil, invalid("bad page_token")
		}
		params.BeforeTime, params.BeforeVersion = &t, version
	}
	rows, err := s.Store.ListPipelineVersions(ctx, params)
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListPipelineVersionsResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		last := rows[len(rows)-1]
		res.NextPageToken = nextToken(last.CreateTime.Format(time.RFC3339Nano) + " " + last.Version)
	}
	for _, r := range rows {
		v, err := pipelineVersion(ws, p, r)
		if err != nil {
			return nil, err
		}
		res.PipelineVersions = append(res.PipelineVersions, v)
	}
	return connect.NewResponse(res), nil
}
