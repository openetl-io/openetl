package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/attemptlog"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/store/db"
)

// LogReader is implemented by ops.Ops.
type LogReader interface {
	ReadLogs(ctx context.Context, in contract.ReadLogsInput) (contract.ReadLogsResult, error)
}

var syncStates = map[string]apiv1.SyncState{
	"running":   apiv1.SyncState_SYNC_STATE_RUNNING,
	"succeeded": apiv1.SyncState_SYNC_STATE_SUCCEEDED,
	"failed":    apiv1.SyncState_SYNC_STATE_FAILED,
	"stopped":   apiv1.SyncState_SYNC_STATE_STOPPED,
}

func syncOut(pipeline string, r db.GetSyncRow) *apiv1.Sync {
	out := &apiv1.Sync{
		Name:       pipeline + "/syncs/" + r.ID.String(),
		Version:    r.Version,
		State:      syncStates[r.Status],
		StartTime:  timestamppb.New(r.StartTime),
		DataPlane:  r.DataPlaneID,
		Continuous: r.Continuous,
	}
	if r.Error != nil {
		out.Error = *r.Error
	}
	if r.EndTime != nil {
		out.EndTime = timestamppb.New(*r.EndTime)
	}
	out.FailedAttempts = r.FailedAttempts
	if r.LastAttemptError != nil {
		out.LastAttemptError = *r.LastAttemptError
	}
	if r.LastAttemptTime != nil {
		out.LastAttemptTime = timestamppb.New(*r.LastAttemptTime)
	}
	return out
}

// getSync returns the sync named workspaces/{w}/pipelines/{p}/syncs/{s}, and
// its pipeline's name.
func (s *Server) getSync(ctx context.Context, name string) (string, db.GetSyncRow, error) {
	pipeline, id, ok := strings.Cut(name, "/syncs/")
	sync, err := uuid.Parse(id)
	if !ok || err != nil || sync.String() != id {
		return "", db.GetSyncRow{}, invalid("name %q isn't workspaces/{workspace}/pipelines/{pipeline}/syncs/{sync}", name)
	}
	_, p, err := s.getPipeline(ctx, pipeline)
	if err != nil {
		return "", db.GetSyncRow{}, err
	}
	r, err := s.Store.GetSync(ctx, db.GetSyncParams{OrgID: s.Tenant.Org, PipelineID: p.ID, ID: sync})
	if errors.Is(err, pgx.ErrNoRows) {
		return "", r, notFound("%s doesn't exist", name)
	}
	return pipeline, r, err
}

// syncsOut is the pipeline's syncs, with what their committed epochs
// carried.
func (s *Server) syncsOut(ctx context.Context, pipeline string, rows []db.GetSyncRow) ([]*apiv1.Sync, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	runs := make([]string, len(rows))
	since := rows[0].StartTime
	for i, r := range rows {
		runs[i] = r.RunID
		if r.StartTime.Before(since) {
			since = r.StartTime
		}
	}
	streams, err := s.Store.SyncStreams(ctx, db.SyncStreamsParams{
		OrgID: s.Tenant.Org, PipelineID: rows[0].PipelineID, RunIds: runs, Since: since.Add(-maxClockSkew),
	})
	if err != nil {
		return nil, err
	}
	byRun := map[string][]*apiv1.SyncStream{}
	lastCommit := map[string]time.Time{}
	for _, st := range streams {
		ss := &apiv1.SyncStream{
			Name: st.Name, Rows: uint64(st.Rows), QuarantinedRows: uint64(st.QuarantinedRows), CollapsedRows: uint64(st.CollapsedRows), //nolint:gosec // sums of counts
			HeldCells: uint64(st.HeldCells), //nolint:gosec // as above
		}
		if len(st.HeldColumns) > 0 {
			if err := json.Unmarshal(st.HeldColumns, &ss.HeldColumns); err != nil {
				return nil, fmt.Errorf("sync %s's held columns: %w", st.RunID, err)
			}
		}
		byRun[st.RunID] = append(byRun[st.RunID], ss)
		if st.LastCommitTime.After(lastCommit[st.RunID]) {
			lastCommit[st.RunID] = st.LastCommitTime
		}
	}
	progress, err := s.Store.SyncProgress(ctx, db.SyncProgressParams{
		OrgID: s.Tenant.Org, PipelineID: rows[0].PipelineID, RunIds: runs, Since: since.Add(-maxClockSkew),
	})
	if err != nil {
		return nil, err
	}
	progressByRun := map[string][]*apiv1.ReadProgress{}
	for _, p := range progress {
		var ps []mirroredProgress
		if err := json.Unmarshal(p.Progress, &ps); err != nil {
			return nil, fmt.Errorf("sync %s's progress: %w", p.RunID, err)
		}
		for _, m := range ps {
			progressByRun[p.RunID] = append(progressByRun[p.RunID], &apiv1.ReadProgress{
				Stream: m.Stream, LagBytes: m.LagBytes, LagRecords: m.LagRecords, Lag: fromMillis(m.LagMs),
				RetentionLeftBytes: m.RetentionLeftBytes, RetentionLeft: fromMillis(m.RetentionLeftMs),
				Time: timestamppb.New(p.CommitTime),
			})
		}
	}
	out := make([]*apiv1.Sync, len(rows))
	for i, r := range rows {
		out[i] = syncOut(pipeline, r)
		out[i].Streams = byRun[r.RunID]
		out[i].Progress = progressByRun[r.RunID]
		if t, ok := lastCommit[r.RunID]; ok {
			out[i].LastCommitTime = timestamppb.New(t)
		}
	}
	return out, nil
}

func fromMillis(ms *int64) *durationpb.Duration {
	if ms == nil {
		return nil
	}
	return durationpb.New(time.Duration(*ms) * time.Millisecond)
}

func (s *Server) GetSync(ctx context.Context, req *connect.Request[apiv1.GetSyncRequest]) (*connect.Response[apiv1.GetSyncResponse], error) {
	pipeline, r, err := s.getSync(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	out, err := s.syncsOut(ctx, pipeline, []db.GetSyncRow{r})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.GetSyncResponse{Sync: out[0]}), nil
}

func (s *Server) ListSyncs(ctx context.Context, req *connect.Request[apiv1.ListSyncsRequest]) (*connect.Response[apiv1.ListSyncsResponse], error) {
	_, p, err := s.getPipeline(ctx, req.Msg.GetParent())
	if err != nil {
		return nil, err
	}
	after, n, err := page(req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, err
	}
	params := db.ListSyncsParams{OrgID: s.Tenant.Org, PipelineID: p.ID, MaxRows: n}
	if after != "" {
		at, id, _ := strings.Cut(after, " ")
		t, err1 := time.Parse(time.RFC3339Nano, at)
		u, err2 := uuid.Parse(id)
		if err1 != nil || err2 != nil {
			return nil, invalid("bad page_token")
		}
		params.BeforeTime, params.BeforeID = &t, u
	}
	rows, err := s.Store.ListSyncs(ctx, params)
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListSyncsResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		last := rows[len(rows)-1]
		res.NextPageToken = nextToken(last.StartTime.Format(time.RFC3339Nano) + " " + last.ID.String())
	}
	page := make([]db.GetSyncRow, len(rows))
	for i, r := range rows {
		page[i] = db.GetSyncRow(r)
	}
	if res.Syncs, err = s.syncsOut(ctx, req.Msg.GetParent(), page); err != nil {
		return nil, err
	}
	return connect.NewResponse(res), nil
}

// ReadSyncLog asks a runner of the sync's data plane for a page of its log.
// Syncs from before logs, whose IDs aren't UUIDv7s, have none.
func (s *Server) ReadSyncLog(ctx context.Context, req *connect.Request[apiv1.ReadSyncLogRequest]) (*connect.Response[apiv1.ReadSyncLogResponse], error) {
	_, r, err := s.getSync(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	in := contract.ReadLogsInput{DataPlane: r.DataPlaneID, OrgID: s.Tenant.Org.String(), PipelineID: r.PipelineID.String(), SyncID: r.ID.String()}
	if t := req.Msg.GetPageToken(); t != "" {
		b, err := base64.RawURLEncoding.DecodeString(t)
		if err != nil || json.Unmarshal(b, &in.After) != nil {
			return nil, invalid("bad page_token")
		}
	}
	res := &apiv1.ReadSyncLogResponse{}
	if r.ID.Version() != 7 {
		return connect.NewResponse(res), nil
	}
	if s.Logs == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("this server can't read logs"))
	}
	out, err := s.Logs.ReadLogs(ctx, in)
	if err != nil {
		return nil, logsError(err, r.DataPlaneID)
	}
	for _, l := range out.Lines {
		res.Entries = append(res.Entries, logEntry(l))
	}
	// The sync's status was read first, so an ended sync's log is whole.
	if len(out.Lines) > 0 || r.Status == "running" {
		b, err := json.Marshal(out.Next)
		if err != nil {
			return nil, err
		}
		res.NextPageToken = base64.RawURLEncoding.EncodeToString(b)
	}
	return connect.NewResponse(res), nil
}

func logsError(err error, dp string) error {
	if app, ok := errors.AsType[*temporal.ApplicationError](err); ok {
		switch app.Type() {
		case errs.ConfigInvalid.String():
			return connect.NewError(connect.CodeInvalidArgument, errors.New(app.Message()))
		case contract.NoRunner:
			return connect.NewError(connect.CodeUnavailable, fmt.Errorf("no runner of data plane %q read the log within %s", dp, contract.LogsPickup))
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return connect.NewError(connect.CodeUnavailable, fmt.Errorf("reading the log failed: %w", err))
}

// logEntry parses a line from slog's JSON handler. A line that isn't JSON
// is its message.
func logEntry(l contract.LogLine) *apiv1.LogEntry {
	e := &apiv1.LogEntry{}
	e.PostSyncStep, e.Attempt = taskOf(l.Task)
	var fields map[string]any
	if err := json.Unmarshal([]byte(l.Text), &fields); err != nil {
		e.Message = l.Text
		return e
	}
	if v, ok := fields["time"].(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			e.Time = timestamppb.New(t)
			delete(fields, "time")
		}
	}
	for key, dst := range map[string]*string{"level": &e.Level, "msg": &e.Message} {
		if v, ok := fields[key].(string); ok {
			*dst = v
			delete(fields, key)
		}
	}
	if len(fields) > 0 {
		e.Attrs, _ = structpb.NewStruct(fields)
	}
	return e
}

func taskOf(task string) (step, attempt int32) {
	s, a, _ := attemptlog.ParseTask(task)
	return int32(s), int32(a) //nolint:gosec // three digits
}
