package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/internal/store/db"
)

// Schema metadata the engine writes.
const (
	tableKey       = "openetl.table"
	sourceNamesKey = "openetl.source_names"
)

type column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	// Where the destination name differs.
	Source string `json:"source,omitempty"`
}

type streamSchema struct {
	stream  string
	table   string
	columns []byte
	digest  string
	raw     []byte
}

func describeSchema(stream string, raw []byte) (streamSchema, error) {
	r, err := ipc.NewReader(bytes.NewReader(raw))
	if err != nil {
		return streamSchema{}, err
	}
	defer r.Release()
	s := r.Schema()
	md := s.Metadata()
	out := streamSchema{stream: stream, raw: raw}
	out.table, _ = md.GetValue(tableKey)
	var sources map[string]string
	if v, ok := md.GetValue(sourceNamesKey); ok {
		if err := json.Unmarshal([]byte(v), &sources); err != nil {
			return streamSchema{}, err
		}
	}
	cols := make([]column, 0, s.NumFields())
	for _, f := range s.Fields() {
		cols = append(cols, column{Name: f.Name, Type: typeName(f), Nullable: f.Nullable, Source: sources[f.Name]})
	}
	if out.columns, err = json.Marshal(cols); err != nil {
		return streamSchema{}, err
	}
	sum := sha256.Sum256(append([]byte(out.table+"\x00"), out.columns...))
	out.digest = hex.EncodeToString(sum[:])
	return out, nil
}

// typeName is the field's Arrow type, with its extension where the field
// metadata names one.
func typeName(f arrow.Field) string {
	if _, ok := f.Type.(arrow.ExtensionType); ok {
		return f.Type.String()
	}
	if ext, ok := f.Metadata.GetValue("ARROW:extension:name"); ok {
		return "extension<" + ext + ">"
	}
	return f.Type.String()
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
