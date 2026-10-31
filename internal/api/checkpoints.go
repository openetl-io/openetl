package api

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/internal/store"
	"github.com/unsuitable001/openetl/internal/store/db"
)

// A checkpoint's create_time partitions the mirror, so one far from now
// would make a partition nothing else uses.
const maxClockSkew = 24 * time.Hour

type mirroredStream struct {
	Name            string `json:"name"`
	Rows            uint64 `json:"rows"`
	QuarantinedRows uint64 `json:"quarantined_rows"`
	Forced          bool   `json:"forced"`
	Complete        bool   `json:"complete"`
	SchemaDigest    string `json:"schema_digest"`
}

// ReportCheckpoint adds a committed checkpoint to the pipeline's mirror,
// unless it's there.
func (s *Server) ReportCheckpoint(ctx context.Context, req *connect.Request[dataplanev1.ReportCheckpointRequest]) (*connect.Response[dataplanev1.ReportCheckpointResponse], error) {
	m := req.Msg
	id, err := uuid.Parse(m.GetPipelineId())
	if err != nil {
		return nil, invalid("pipeline_id: %v", err)
	}
	if m.GetRunId() == "" {
		return nil, invalid("run_id is empty")
	}
	if !m.GetCreateTime().IsValid() {
		return nil, invalid("create_time is missing or invalid")
	}
	created := m.GetCreateTime().AsTime()
	if d := time.Since(created); d > maxClockSkew || d < -maxClockSkew {
		return nil, invalid("create_time %s is more than %s from the control plane's clock", created.Format(time.RFC3339), maxClockSkew)
	}
	streams := make([]mirroredStream, 0, len(m.GetStreams()))
	for _, st := range m.GetStreams() {
		streams = append(streams, mirroredStream{
			Name: st.GetName(), Rows: st.GetRows(), QuarantinedRows: st.GetQuarantinedRows(),
			Forced: st.GetForced(), Complete: st.GetComplete(), SchemaDigest: st.GetSchemaDigest(),
		})
	}
	slices.SortFunc(streams, func(a, b mirroredStream) int { return strings.Compare(a.Name, b.Name) })
	raw, err := json.Marshal(streams)
	if err != nil {
		return nil, err
	}
	arg := db.InsertMirroredCheckpointParams{
		OrgID: s.Tenant.Org, PipelineID: id, RunID: m.GetRunId(), CommitTime: created, Streams: raw,
		Epoch: int64(m.GetEpoch()), Generation: int64(m.GetGeneration()), //nolint:gosec // generations and epochs are small
	}
	if p := m.GetPrevious(); p != nil {
		e, g := int64(p.GetEpoch()), int64(p.GetGeneration()) //nolint:gosec // as above
		arg.PreviousEpoch, arg.PreviousGeneration = &e, &g
	}
	added, err := s.Store.MirrorCheckpoint(ctx, arg)
	if store.IsForeignKeyViolation(err) {
		return nil, notFound("pipeline %s doesn't exist", id)
	}
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&dataplanev1.ReportCheckpointResponse{Added: added}), nil
}
