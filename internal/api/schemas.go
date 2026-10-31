package api

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
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
			n, err := q.AddStreamSchema(ctx, db.AddStreamSchemaParams{
				OrgID: s.Tenant.Org, PipelineID: id, Stream: d.stream, Digest: d.digest, TableName: d.table,
				Columns: d.columns, ArrowSchema: d.raw, Generation: gen, Epoch: epoch,
			})
			if err != nil {
				return err
			}
			if n > 0 {
				res.Added = append(res.Added, d.stream)
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
