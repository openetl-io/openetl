package api

import (
	"cmp"
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

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/events"
	"github.com/openetl-io/openetl/internal/platform/ints"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/internal/streamschema"
)

type streamSchema struct {
	stream string
	// One of a transform's outputs.
	output  bool
	table   string
	columns []byte
	digest  string
	raw     []byte
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
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
	for _, set := range []struct {
		output  bool
		schemas map[string][]byte
	}{{false, req.Msg.GetSchemas()}, {true, req.Msg.GetOutputSchemas()}} {
		for name, raw := range set.schemas {
			d, err := describeSchema(name, raw)
			if err != nil {
				return nil, invalid("stream %s: schema: %v", name, err)
			}
			d.output = set.output
			schemas = append(schemas, d)
		}
	}
	slices.SortFunc(schemas, func(a, b streamSchema) int {
		if a.output != b.output {
			return cmp.Compare(boolInt(a.output), boolInt(b.output))
		}
		return strings.Compare(a.stream, b.stream)
	})
	var b bigints
	gen, epoch := b.of("generation", req.Msg.GetGeneration()), b.of("epoch", req.Msg.GetEpoch())
	if b.err != nil {
		return nil, b.err
	}
	res := &dataplanev1.ReportSchemasResponse{}
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		res.Added, res.AddedOutputs = nil, nil
		if _, err := q.LockPipelineSchemas(ctx, db.LockPipelineSchemasParams{OrgID: s.Tenant.Org, PipelineID: id}); err != nil {
			return err
		}
		for _, d := range schemas {
			added, err := q.AddStreamSchema(ctx, db.AddStreamSchemaParams{
				OrgID: s.Tenant.Org, PipelineID: id, Output: d.output, Stream: d.stream, Digest: d.digest, TableName: d.table,
				Columns: d.columns, ArrowSchema: d.raw, Generation: gen, Epoch: epoch,
			})
			if err != nil {
				return err
			}
			if len(added) == 0 {
				continue
			}
			if d.output {
				res.AddedOutputs = append(res.AddedOutputs, d.stream)
			} else {
				res.Added = append(res.Added, d.stream)
			}
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
	prevRaw, err := q.GetStreamSchemaColumns(ctx, db.GetStreamSchemaColumnsParams{OrgID: s.Tenant.Org, PipelineID: pipeline, Output: d.output, Stream: d.stream, Version: version - 1})
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
	data := events.Schema{Pipeline: name, Stream: d.stream, Output: d.output, Version: version, Table: d.table, Columns: []events.Column{}}
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
		Key: schemaEventKey(pipeline, d, version),
	})
	return err
}

// schemaEventKey records a version's event once. An output's versions are
// its own, apart from those of the stream that shares its name: whether
// it's an input or an output is a segment of its own, before the stream's
// name, which may hold slashes, and the version is the last.
func schemaEventKey(pipeline uuid.UUID, d streamSchema, version int32) string {
	kind := "input"
	if d.output {
		kind = "output"
	}
	return events.SchemaChanged + "/" + pipeline.String() + "/" + kind + "/" + d.stream + "/" + strconv.Itoa(int(version))
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
		parts := strings.Split(after, "\x00")
		if len(parts) == 2 {
			// A token from before outputs: an input's.
			parts = append([]string{"i"}, parts...)
		}
		version, err := strconv.ParseInt(parts[len(parts)-1], 10, 32)
		if len(parts) != 3 || (parts[0] != "i" && parts[0] != "o") || err != nil {
			return nil, invalid("bad page_token")
		}
		params.AfterInput, params.AfterStream, params.AfterVersion = parts[0] == "i", parts[1], int32(version)
	}
	rows, err := s.Store.ListStreamSchemas(ctx, params)
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListStreamSchemasResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		last := rows[len(rows)-1]
		kind := "i"
		if last.Output {
			kind = "o"
		}
		res.NextPageToken = nextToken(kind + "\x00" + last.Stream + "\x00" + strconv.Itoa(int(last.Version)))
	}
	for _, r := range rows {
		var cols []streamschema.Column
		if err := json.Unmarshal(r.Columns, &cols); err != nil {
			return nil, err
		}
		out := &apiv1.StreamSchema{
			Stream: r.Stream, Output: r.Output, Version: r.Version, Table: r.Table, Digest: r.Digest,
			Generation: ints.Uint64(r.Generation), Epoch: ints.Uint64(r.Epoch),
			CreateTime: timestamppb.New(r.CreateTime),
		}
		for _, c := range cols {
			out.Columns = append(out.Columns, &apiv1.SchemaColumn{Name: c.Name, Type: c.Type, Nullable: c.Nullable, Source: c.Source})
		}
		res.StreamSchemas = append(res.StreamSchemas, out)
	}
	return connect.NewResponse(res), nil
}
