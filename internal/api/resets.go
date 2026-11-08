package api

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/events"
	"github.com/openetl-io/openetl/internal/heal"
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/store/db"
)

const (
	requestReset  = "reset"
	requestRewind = "rewind"
)

func (s *Server) ResetPipeline(ctx context.Context, req *connect.Request[apiv1.ResetPipelineRequest]) (*connect.Response[apiv1.ResetPipelineResponse], error) {
	ws, p, err := s.getPipeline(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	streams := slices.Compact(slices.Sorted(slices.Values(req.Msg.GetStreams())))
	if len(streams) > 0 {
		audit(ctx).set("streams", strings.Join(streams, ","))
	}
	out, err := s.pipeline(ctx, ws, p)
	if err != nil {
		return nil, err
	}
	spec := out.GetSpec()
	if spec.GetTransform().GetFlink() != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s's Flink job keeps state of its own, which a reset wouldn't; delete the pipeline and apply it again to read from the start", out.GetName()))
	}
	if named := spec.GetSource().GetStreams(); len(named) > 0 {
		for _, st := range streams {
			if !slices.ContainsFunc(named, func(ps *apiv1.PipelineStream) bool { return ps.GetName() == st }) {
				return nil, invalid("%s has no stream %q", out.GetName(), st)
			}
		}
	}
	if streams == nil {
		streams = []string{}
	}
	if err := s.request(ctx, ws, p, requestReset, streams, events.PipelineReset); err != nil {
		return nil, err
	}
	if err := s.refreshContinuous(ctx, p, spec); err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.ResetPipelineResponse{Pipeline: out}), nil
}

func (s *Server) RewindPipeline(ctx context.Context, req *connect.Request[apiv1.RewindPipelineRequest]) (*connect.Response[apiv1.RewindPipelineResponse], error) {
	ws, p, err := s.getPipeline(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	out, err := s.pipeline(ctx, ws, p)
	if err != nil {
		return nil, err
	}
	if out.GetSpec().GetTransform().GetFlink() != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s's Flink job restores from its own checkpoint, which goes with the latest one; delete the pipeline and apply it again to read from the start", out.GetName()))
	}
	if err := s.request(ctx, ws, p, requestRewind, []string{}, ""); err != nil {
		return nil, err
	}
	if err := s.refreshContinuous(ctx, p, out.GetSpec()); err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.RewindPipelineResponse{Pipeline: out}), nil
}

// request records a request of the pipeline's checkpoint log for its next
// attempt, and the event typ if there is one.
func (s *Server) request(ctx context.Context, ws string, p db.Pipeline, kind string, streams []string, typ string) error {
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		if _, err := q.InsertCheckpointRequest(ctx, db.InsertCheckpointRequestParams{
			OrgID: s.Tenant.Org, ID: id, PipelineID: p.ID, Kind: kind, Streams: streams, Principal: callerOf(ctx).principal,
		}); err != nil {
			return err
		}
		if typ == "" {
			return nil
		}
		_, err := events.Record(ctx, q, s.Tenant.Org, events.Event{
			Workspace: p.WorkspaceID, Type: typ, Pipeline: p.Name,
			Data: events.Pipeline{Pipeline: resourceName(ws, "pipelines", p.Name), Version: p.Version, Streams: streams},
		})
		return err
	})
	if err != nil {
		return err
	}
	audit(ctx).set(kind, id.String())
	return nil
}

// refreshContinuous has a continuous pipeline's supervisor look at it
// again, which restarts a running run at a checkpoint.
func (s *Server) refreshContinuous(ctx context.Context, p db.Pipeline, spec *apiv1.PipelineSpec) error {
	if !plan.Continuous(spec) {
		return nil
	}
	if err := s.Schedules.Refresh(ctx, s.Tenant.Org.String(), p.ID.String()); err != nil {
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf("the request was stored, but the pipeline's run couldn't be told; it takes it when it next starts: %w", err))
	}
	return nil
}

// pendingRequests are the resets an attempt takes, and the approval of a
// rewind, if one is pending.
func (s *Server) pendingRequests(ctx context.Context, pipeline uuid.UUID) ([]*dataplanev1.Reset, string, error) {
	rows, err := s.Store.PendingCheckpointRequests(ctx, db.PendingCheckpointRequestsParams{OrgID: s.Tenant.Org, PipelineID: pipeline})
	if err != nil {
		return nil, "", err
	}
	var resets []*dataplanev1.Reset
	rewind := ""
	for _, r := range rows {
		switch r.Kind {
		case requestReset:
			resets = append(resets, &dataplanev1.Reset{Id: r.ID.String(), Streams: r.Streams})
		case requestRewind:
			rewind = r.ID.String()
		}
	}
	return resets, rewind, nil
}

// takeResets marks the resets up to the newest a checkpoint carries taken.
func (s *Server) takeResets(ctx context.Context, pipeline uuid.UUID, newest string) error {
	if newest == "" {
		return nil
	}
	id, err := uuid.Parse(newest)
	if err != nil {
		return invalid("last_reset: %v", err)
	}
	return s.Store.TakeResets(ctx, db.TakeResetsParams{OrgID: s.Tenant.Org, PipelineID: pipeline, Newest: id})
}

func (s *Server) ReportRewind(ctx context.Context, req *connect.Request[dataplanev1.ReportRewindRequest]) (*connect.Response[dataplanev1.ReportRewindResponse], error) {
	m := req.Msg
	id, err := uuid.Parse(m.GetPipelineId())
	if err != nil {
		return nil, invalid("pipeline_id: %v", err)
	}
	if err := s.pipelineOnDataPlane(ctx, id); err != nil {
		return nil, err
	}
	if m.GetRunId() == "" || m.GetUnreadable() == nil {
		return nil, invalid("run_id and unreadable are needed")
	}
	sync, err := s.Store.GetSyncByRun(ctx, db.GetSyncByRunParams{OrgID: s.Tenant.Org, PipelineID: id, RunID: m.GetRunId()})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("pipeline %s has no sync of run %s", id, m.GetRunId())
	}
	if err != nil {
		return nil, err
	}
	u := m.GetUnreadable()
	restored := "none, so the read started again from the beginning"
	if r := m.GetRestored(); r != nil {
		restored = fmt.Sprintf("%d-g%d", r.GetEpoch(), r.GetGeneration())
	}
	by := "heal mode, as reading it again keeps every stream's guarantee"
	if m.GetApproval() != "" {
		by = "a person's approval"
	}
	o := heal.Observation{
		Pipeline: id, Version: sync.Version, Class: heal.CheckpointCorrupt, Code: "CHECKPOINT_CORRUPT", Severity: heal.Warning,
		Message: truncate(fmt.Sprintf("Checkpoint %d-g%d couldn't be read (%s). The pipeline restarted from the one before it, %s, by %s.",
			u.GetEpoch(), u.GetGeneration(), m.GetReason(), restored, by), maxError),
		Details: map[string]string{
			"unreadable": fmt.Sprintf("%d-g%d", u.GetEpoch(), u.GetGeneration()), "restored": restored,
			"approved": strconv.FormatBool(m.GetApproval() != ""),
		},
		Taken: []heal.Action{{
			Kind: "rewind", State: "applied", Time: time.Now().UTC(),
			Description: "Restarted from the checkpoint before the one that can't be read, which delivers its epoch again.",
		}},
		Sync: &sync.ID, Key: fmt.Sprintf("rewind/%d-g%d", u.GetEpoch(), u.GetGeneration()),
	}
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		if err := q.TakeRewinds(ctx, db.TakeRewindsParams{OrgID: s.Tenant.Org, PipelineID: id}); err != nil {
			return err
		}
		_, err := heal.Observe(ctx, q, s.Tenant.Org, o)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&dataplanev1.ReportRewindResponse{}), nil
}

func (s *Server) ReportRetention(ctx context.Context, req *connect.Request[dataplanev1.ReportRetentionRequest]) (*connect.Response[dataplanev1.ReportRetentionResponse], error) {
	m := req.Msg
	id, err := uuid.Parse(m.GetPipelineId())
	if err != nil {
		return nil, invalid("pipeline_id: %v", err)
	}
	if err := s.pipelineOnDataPlane(ctx, id); err != nil {
		return nil, err
	}
	op, err := uuid.Parse(m.GetOperationId())
	if err != nil {
		return nil, invalid("operation_id: %v", err)
	}
	row, err := s.Store.GetConnectorOp(ctx, db.GetConnectorOpParams{OrgID: s.Tenant.Org, ID: op, DataPlaneID: callerOf(ctx).dataPlane})
	if errors.Is(err, pgx.ErrNoRows) || err == nil && (row.PipelineID == nil || *row.PipelineID != id) {
		return nil, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("the API sent no retention operation %s on pipeline %s to this data plane", op, id))
	}
	if err != nil {
		return nil, err
	}
	p, err := s.Store.GetPipelineByID(ctx, db.GetPipelineByIDParams{OrgID: s.Tenant.Org, ID: id})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("pipeline %s doesn't exist", id)
	}
	if err != nil {
		return nil, err
	}
	at := retentionAt{version: p.Version, key: "probe/" + m.GetOperationId()}
	if c := m.GetCheckpoint(); c != nil {
		at.what = fmt.Sprintf(" from checkpoint %d-g%d, while it isn't running", c.GetEpoch(), c.GetGeneration())
	}
	if err := s.observeRetention(ctx, id, at, m.GetProgress()); err != nil {
		return nil, err
	}
	return connect.NewResponse(&dataplanev1.ReportRetentionResponse{}), nil
}
