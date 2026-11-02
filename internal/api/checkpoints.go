package api

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
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
	CollapsedRows   uint64 `json:"collapsed_rows"`
	Forced          bool   `json:"forced"`
	Complete        bool   `json:"complete"`
	SchemaDigest    string `json:"schema_digest"`
}

// mirroredProgress is a read's progress as checkpoint_mirror.progress keeps
// it.
type mirroredProgress struct {
	Stream             string  `json:"stream"`
	LagBytes           *uint64 `json:"lag_bytes,omitempty"`
	LagRecords         *uint64 `json:"lag_records,omitempty"`
	LagMs              *int64  `json:"lag_ms,omitempty"`
	RetentionLeftBytes *uint64 `json:"retention_left_bytes,omitempty"`
	RetentionLeftMs    *int64  `json:"retention_left_ms,omitempty"`
}

func millis(d *durationpb.Duration) *int64 {
	if d == nil {
		return nil
	}
	ms := d.AsDuration().Milliseconds()
	return &ms
}

// ReportCheckpoint adds a committed checkpoint to the pipeline's mirror,
// unless it's there.
func (s *Server) ReportCheckpoint(ctx context.Context, req *connect.Request[dataplanev1.ReportCheckpointRequest]) (*connect.Response[dataplanev1.ReportCheckpointResponse], error) {
	m := req.Msg
	id, err := uuid.Parse(m.GetPipelineId())
	if err != nil {
		return nil, invalid("pipeline_id: %v", err)
	}
	if err := s.pipelineOnDataPlane(ctx, id); err != nil {
		return nil, err
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
			Name: st.GetName(), Rows: st.GetRows(), QuarantinedRows: st.GetQuarantinedRows(), CollapsedRows: st.GetCollapsedRows(),
			Forced: st.GetForced(), Complete: st.GetComplete(), SchemaDigest: st.GetSchemaDigest(),
		})
	}
	slices.SortFunc(streams, func(a, b mirroredStream) int { return strings.Compare(a.Name, b.Name) })
	raw, err := json.Marshal(streams)
	if err != nil {
		return nil, err
	}
	var progress []byte
	if len(m.GetProgress()) > 0 {
		ps := make([]mirroredProgress, len(m.GetProgress()))
		for i, p := range m.GetProgress() {
			ps[i] = mirroredProgress{
				Stream: p.GetStream(), LagBytes: p.LagBytes, LagRecords: p.LagRecords, LagMs: millis(p.GetLag()),
				RetentionLeftBytes: p.RetentionLeftBytes, RetentionLeftMs: millis(p.GetRetentionLeft()),
			}
		}
		if progress, err = json.Marshal(ps); err != nil {
			return nil, err
		}
	}
	arg := db.InsertMirroredCheckpointParams{
		OrgID: s.Tenant.Org, PipelineID: id, RunID: m.GetRunId(), CommitTime: created, Streams: raw, Progress: progress,
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

func (s *Server) ListCheckpoints(ctx context.Context, req *connect.Request[apiv1.ListCheckpointsRequest]) (*connect.Response[apiv1.ListCheckpointsResponse], error) {
	_, p, err := s.getPipeline(ctx, req.Msg.GetParent())
	if err != nil {
		return nil, err
	}
	after, n, err := page(req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, err
	}
	params := db.ListCheckpointsParams{OrgID: s.Tenant.Org, PipelineID: p.ID, MaxRows: n}
	if after != "" {
		e, g, ok := strings.Cut(after, ".")
		epoch, err1 := strconv.ParseInt(e, 10, 64)
		gen, err2 := strconv.ParseInt(g, 10, 64)
		if !ok || err1 != nil || err2 != nil {
			return nil, invalid("bad page_token")
		}
		params.BeforeEpoch, params.BeforeGeneration = &epoch, gen
	}
	rows, err := s.Store.ListCheckpoints(ctx, params)
	if err != nil {
		return nil, err
	}
	versions, err := s.schemaVersions(ctx, p.ID)
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListCheckpointsResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		last := rows[len(rows)-1]
		res.NextPageToken = nextToken(fmt.Sprintf("%d.%d", last.Epoch, last.Generation))
	}
	for _, r := range rows {
		var streams []mirroredStream
		if err := json.Unmarshal(r.Streams, &streams); err != nil {
			return nil, err
		}
		c := &apiv1.Checkpoint{
			Epoch: uint64(r.Epoch), Generation: uint64(r.Generation), //nolint:gosec // never negative
			CommitTime: timestamppb.New(r.CommitTime),
		}
		if r.PreviousEpoch != nil && r.PreviousGeneration != nil {
			c.PreviousEpoch, c.PreviousGeneration = uint64(*r.PreviousEpoch), uint64(*r.PreviousGeneration) //nolint:gosec // as above
		}
		if r.SyncID != nil {
			c.Sync = req.Msg.GetParent() + "/syncs/" + r.SyncID.String()
		}
		for _, st := range streams {
			c.Streams = append(c.Streams, &apiv1.CheckpointStream{
				Name: st.Name, Rows: st.Rows, QuarantinedRows: st.QuarantinedRows, CollapsedRows: st.CollapsedRows,
				Forced: st.Forced, Complete: st.Complete, SchemaVersion: versions[st.Name+"\x00"+st.SchemaDigest],
			})
		}
		res.Checkpoints = append(res.Checkpoints, c)
	}
	return connect.NewResponse(res), nil
}

// schemaVersions maps each stream's schema digests to their versions, keyed
// by stream and digest, NUL between.
func (s *Server) schemaVersions(ctx context.Context, pipeline uuid.UUID) (map[string]int32, error) {
	rows, err := s.Store.StreamSchemaVersions(ctx, db.StreamSchemaVersionsParams{OrgID: s.Tenant.Org, PipelineID: pipeline})
	if err != nil {
		return nil, err
	}
	out := make(map[string]int32, len(rows))
	for _, r := range rows {
		out[r.Stream+"\x00"+r.Digest] = r.Version
	}
	return out, nil
}
