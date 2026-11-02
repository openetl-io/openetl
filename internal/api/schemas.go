package api

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/internal/events"
	"github.com/unsuitable001/openetl/internal/store/db"
	"github.com/unsuitable001/openetl/internal/streamschema"
)

type streamSchema struct {
	stream  string
	table   string
	columns []byte
	digest  string
	raw     []byte
}

func describeSchema(stream string, raw []byte) (streamSchema, error) {
	d, err := streamschema.Describe(raw)
	if err != nil {
		return streamSchema{}, err
	}
	cols, err := json.Marshal(d.Columns)
	if err != nil {
		return streamSchema{}, err
	}
	return streamSchema{stream: stream, table: d.Table, columns: cols, digest: d.Digest, raw: raw}, nil
}

// ReportSchemas adds each stream's schema to its history, unless the
// history has it.
func (s *Server) ReportSchemas(ctx context.Context, req *connect.Request[dataplanev1.ReportSchemasRequest]) (*connect.Response[dataplanev1.ReportSchemasResponse], error) {
	id, err := uuid.Parse(req.Msg.GetPipelineId())
	if err != nil {
		return nil, invalid("pipeline_id: %v", err)
	}
	if err := s.pipelineOnDataPlane(ctx, id); err != nil {
		return nil, err
	}
	var schemas []streamSchema
	for name, raw := range req.Msg.GetSchemas() {
		d, err := describeSchema(name, raw)
		if err != nil {
			return nil, invalid("stream %s: schema: %v", name, err)
		}
		schemas = append(schemas, d)
	}
	slices.SortFunc(schemas, func(a, b streamSchema) int { return strings.Compare(a.stream, b.stream) })
	gen, epoch := int64(req.Msg.GetGeneration()), int64(req.Msg.GetEpoch()) //nolint:gosec // generations and epochs are small
	res := &dataplanev1.ReportSchemasResponse{}
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		res.Added = nil
		if _, err := q.LockPipelineSchemas(ctx, db.LockPipelineSchemasParams{OrgID: s.Tenant.Org, PipelineID: id}); err != nil {
			return err
		}
		for _, d := range schemas {
			added, err := q.AddStreamSchema(ctx, db.AddStreamSchemaParams{
				OrgID: s.Tenant.Org, PipelineID: id, Stream: d.stream, Digest: d.digest, TableName: d.table,
				Columns: d.columns, ArrowSchema: d.raw, Generation: gen, Epoch: epoch,
			})
			if err != nil {
				return err
			}
			if len(added) == 0 {
				continue
			}
			res.Added = append(res.Added, d.stream)
			if v := added[0]; v > 1 {
				if err := s.recordSchemaChange(ctx, q, id, d, v); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("pipeline %s doesn't exist", id)
	}
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(res), nil
}

// recordSchemaChange records a stream's new schema version, and how it
// differs from the one before.
func (s *Server) recordSchemaChange(ctx context.Context, q *db.Queries, pipeline uuid.UUID, d streamSchema, version int32) error {
	names, err := q.GetPipelineNames(ctx, db.GetPipelineNamesParams{OrgID: s.Tenant.Org, ID: pipeline})
	if err != nil {
		return err
	}
	prevRaw, err := q.GetStreamSchemaColumns(ctx, db.GetStreamSchemaColumnsParams{OrgID: s.Tenant.Org, PipelineID: pipeline, Stream: d.stream, Version: version - 1})
	if err != nil {
		return err
	}
	var prev, cur []streamschema.Column
	if err := json.Unmarshal(prevRaw, &prev); err != nil {
		return err
	}
	if err := json.Unmarshal(d.columns, &cur); err != nil {
		return err
	}
	name := resourceName(names.Workspace, "pipelines", names.Name)
	data := events.Schema{Pipeline: name, Stream: d.stream, Version: version, Table: d.table}
	for _, c := range cur {
		data.Columns = append(data.Columns, events.Column{Name: c.Name, Type: c.Type, Nullable: c.Nullable})
		i := slices.IndexFunc(prev, func(p streamschema.Column) bool { return p.Name == c.Name })
		switch {
		case i < 0:
			data.Added = append(data.Added, c.Name)
		case prev[i].Type != c.Type || prev[i].Nullable != c.Nullable:
			data.Changed = append(data.Changed, c.Name)
		}
	}
	for _, p := range prev {
		if !slices.ContainsFunc(cur, func(c streamschema.Column) bool { return c.Name == p.Name }) {
			data.Removed = append(data.Removed, p.Name)
		}
	}
	_, err = events.Record(ctx, q, s.Tenant.Org, events.Event{
		Workspace: names.WorkspaceID, Type: events.SchemaChanged, Pipeline: names.Name, Data: data,
		Key: events.SchemaChanged + "/" + pipeline.String() + "/" + d.stream + "/" + strconv.Itoa(int(version)),
	})
	return err
}

func (s *Server) ListStreamSchemas(ctx context.Context, req *connect.Request[apiv1.ListStreamSchemasRequest]) (*connect.Response[apiv1.ListStreamSchemasResponse], error) {
	_, p, err := s.getPipeline(ctx, req.Msg.GetParent())
	if err != nil {
		return nil, err
	}
	after, n, err := page(req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, err
	}
	params := db.ListStreamSchemasParams{OrgID: s.Tenant.Org, PipelineID: p.ID, Stream: req.Msg.GetStream(), MaxRows: n}
	if after != "" {
		stream, v, ok := strings.Cut(after, "\x00")
		version, err := strconv.ParseInt(v, 10, 32)
		if !ok || err != nil {
			return nil, invalid("bad page_token")
		}
		params.AfterStream, params.AfterVersion = stream, int32(version)
	}
	rows, err := s.Store.ListStreamSchemas(ctx, params)
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListStreamSchemasResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		last := rows[len(rows)-1]
		res.NextPageToken = nextToken(last.Stream + "\x00" + strconv.Itoa(int(last.Version)))
	}
	for _, r := range rows {
		var cols []streamschema.Column
		if err := json.Unmarshal(r.Columns, &cols); err != nil {
			return nil, err
		}
		out := &apiv1.StreamSchema{
			Stream: r.Stream, Version: r.Version, Table: r.Table, Digest: r.Digest,
			Generation: uint64(r.Generation), Epoch: uint64(r.Epoch), //nolint:gosec // never negative
			CreateTime: timestamppb.New(r.CreateTime),
		}
		for _, c := range cols {
			out.Columns = append(out.Columns, &apiv1.SchemaColumn{Name: c.Name, Type: c.Type, Nullable: c.Nullable, Source: c.Source})
		}
		res.StreamSchemas = append(res.StreamSchemas, out)
	}
	return connect.NewResponse(res), nil
}
