package api

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/heal"
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/store/db"
)

// runtime sets what heal and people changed of how p runs: its paused
// streams and heal's overrides.
func (s *Server) runtime(ctx context.Context, p *apiv1.Pipeline, id uuid.UUID) error {
	paused, err := s.Store.ListPausedStreams(ctx, db.ListPausedStreamsParams{OrgID: s.Tenant.Org, Pipelines: []uuid.UUID{id}})
	if err != nil {
		return err
	}
	for _, r := range paused {
		ps := &apiv1.PausedStream{Stream: r.Stream, Reason: r.Reason, CreateTime: timestamppb.New(r.CreateTime)}
		if r.IncidentID != nil {
			ps.Incident = p.GetName() + "/incidents/" + r.IncidentID.String()
		}
		p.PausedStreams = append(p.PausedStreams, ps)
	}
	overrides, err := s.Store.ListPipelineOverrides(ctx, db.ListPipelineOverridesParams{OrgID: s.Tenant.Org, Pipelines: []uuid.UUID{id}})
	if err != nil {
		return err
	}
	for _, o := range overrides {
		p.Overrides = append(p.Overrides, heal.Override(p.GetName(), p.GetSpec(), o))
	}
	return nil
}

// runtimeForSync sets the streams an attempt skips, and heal's rate limit
// scale. They're read when the attempt starts, whichever version it runs.
func (s *Server) runtimeForSync(ctx context.Context, id uuid.UUID, res *dataplanev1.GetSyncSpecResponse) error {
	paused, err := s.Store.ListPausedStreams(ctx, db.ListPausedStreamsParams{OrgID: s.Tenant.Org, Pipelines: []uuid.UUID{id}})
	if err != nil {
		return err
	}
	for _, r := range paused {
		res.PausedStreams = append(res.PausedStreams, r.Stream)
	}
	o, err := s.Store.GetPipelineOverride(ctx, db.GetPipelineOverrideParams{OrgID: s.Tenant.Org, PipelineID: id, Field: heal.OverrideRateLimitScale})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	res.RateLimitScale, err = heal.ParseScale(o.Value)
	return err
}

// setStreamPaused pauses or resumes one stream. A continuous pipeline's run
// restarts at a checkpoint to take it.
func (s *Server) setStreamPaused(ctx context.Context, name, stream string, paused bool) (*apiv1.Pipeline, error) {
	ws, p, err := s.getPipeline(ctx, name)
	if err != nil {
		return nil, err
	}
	cur, err := s.pipeline(ctx, ws, p)
	if err != nil {
		return nil, err
	}
	changed := false
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		if !paused {
			changed, err = heal.ResumeStream(ctx, q, s.Tenant.Org, p.ID, stream)
			return err
		}
		if err := s.checkStreamPause(ctx, q, p.ID, cur, stream); err != nil {
			return err
		}
		changed, err = heal.PauseStream(ctx, q, s.Tenant.Org, p.ID, stream, "Paused by "+cmp.Or(callerOf(ctx).principal, "someone")+".", nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	out, err := s.pipeline(ctx, ws, p)
	if err != nil {
		return nil, err
	}
	if changed && plan.Continuous(out.GetSpec()) {
		if err := s.Schedules.Refresh(ctx, s.Tenant.Org.String(), p.ID.String()); err != nil {
			s.log().Error("restarting the run failed", "pipeline", p.ID, "stream", stream, "err", err)
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("the stream's state was stored, but the run couldn't be told to restart; try again: %w", err))
		}
	}
	return out, nil
}

// checkStreamPause refuses to pause a stream that isn't the pipeline's, or
// that can't stop on its own, which the newest checkpoint with the stream
// tells, or failing that the spec.
func (s *Server) checkStreamPause(ctx context.Context, q *db.Queries, id uuid.UUID, p *apiv1.Pipeline, stream string) error {
	spec := p.GetSpec()
	var streams []string
	var read apiv1.SyncMode
	for _, ps := range spec.GetSource().GetStreams() {
		streams = append(streams, ps.GetName())
		if ps.GetName() == stream {
			read = ps.GetSync()
		}
	}
	raw, err := q.LatestMirroredStreams(ctx, db.LatestMirroredStreamsParams{OrgID: s.Tenant.Org, PipelineID: id, Stream: stream})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return err
	default:
		var mirrored []mirroredStream
		if err := json.Unmarshal(raw, &mirrored); err != nil {
			return err
		}
		for _, m := range mirrored {
			if m.Name == stream && m.Read != apiv1.SyncMode_SYNC_MODE_UNSPECIFIED {
				read = m.Read
			}
			if len(spec.GetSource().GetStreams()) == 0 {
				streams = append(streams, m.Name)
			}
		}
	}
	if !slices.Contains(streams, stream) {
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("%s has no stream %q", p.GetName(), stream))
	}
	others := 0
	for _, name := range streams {
		if name != stream && !slices.ContainsFunc(p.GetPausedStreams(), func(ps *apiv1.PausedStream) bool { return ps.GetStream() == name }) {
			others++
		}
	}
	if why := heal.WhyNotPause(read, spec.GetTransform() != nil, others); why != "" {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("stream %q can't be paused on its own: %s", stream, why))
	}
	return nil
}

func (s *Server) RemovePipelineOverride(ctx context.Context, req *connect.Request[apiv1.RemovePipelineOverrideRequest]) (*connect.Response[apiv1.RemovePipelineOverrideResponse], error) {
	field := req.Msg.GetField()
	if !slices.Contains(heal.Overrides, field) {
		return nil, invalid("field %q isn't one heal overrides: %v", field, heal.Overrides)
	}
	ws, p, err := s.getPipeline(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	removed := false
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		n, err := q.DeletePipelineOverride(ctx, db.DeletePipelineOverrideParams{OrgID: s.Tenant.Org, PipelineID: p.ID, Field: field})
		if err != nil || n == 0 {
			return err
		}
		removed = true
		return heal.RecordOverride(ctx, q, s.Tenant.Org, p.WorkspaceID, ws, p.Name, field, "", nil)
	})
	if err != nil {
		return nil, err
	}
	out, err := s.pipeline(ctx, ws, p)
	if err != nil {
		return nil, err
	}
	if removed && field == heal.OverrideEvery && !plan.Continuous(out.GetSpec()) {
		if err := s.Schedules.Upsert(ctx, s.Tenant.Org.String(), p.ID.String(), out.GetSpec().GetSchedule(), p.Paused); err != nil {
			s.log().Error("schedule update failed", "pipeline", p.ID, "err", err)
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("the override was removed, but the schedule couldn't be changed; apply the pipeline again: %w", err))
		}
	}
	return connect.NewResponse(&apiv1.RemovePipelineOverrideResponse{Pipeline: out}), nil
}

// overridesAfter splits the pipeline's overrides into those its new version
// keeps and those it drops, by whether the version changes their fields.
func (s *Server) overridesAfter(ctx context.Context, q *db.Queries, p *planned) (kept, dropped []db.PipelineOverride, err error) {
	all, err := q.ListPipelineOverrides(ctx, db.ListPipelineOverridesParams{OrgID: s.Tenant.Org, Pipelines: []uuid.UUID{p.id}})
	if err != nil || len(all) == 0 || p.prev == "" || p.prev == p.version {
		return all, nil, err
	}
	row, err := q.GetSyncSpec(ctx, db.GetSyncSpecParams{OrgID: s.Tenant.Org, PipelineID: p.id, Version: p.prev})
	if err != nil {
		return nil, nil, err
	}
	before, err := pinsOf(row)
	if err != nil {
		return nil, nil, err
	}
	for _, o := range all {
		if heal.Kept(o.Field, before.Spec, p.v.spec, before.SourceConnector, p.v.src.ConnectorDigest) {
			kept = append(kept, o)
		} else {
			dropped = append(dropped, o)
		}
	}
	return kept, dropped, nil
}

// planRuntime adds to a plan the overrides the version keeps, notes for
// those it drops, and which of its streams are paused.
func (s *Server) planRuntime(ctx context.Context, q *db.Queries, p *planned, out *apiv1.PipelinePlan) error {
	kept, dropped, err := s.overridesAfter(ctx, q, p)
	if err != nil {
		return err
	}
	for _, o := range kept {
		out.Overrides = append(out.Overrides, heal.Override(p.name, p.v.spec, o))
	}
	for _, o := range dropped {
		out.Notes = append(out.Notes, fmt.Sprintf("heal's override of %s (%s) goes, since the version changes what it overrides", o.Field, o.Value))
	}
	paused, err := q.ListPausedStreams(ctx, db.ListPausedStreamsParams{OrgID: s.Tenant.Org, Pipelines: []uuid.UUID{p.id}})
	if err != nil {
		return err
	}
	for _, ps := range paused {
		listed := p.v.spec.GetSource().GetStreams()
		if len(listed) > 0 && !slices.ContainsFunc(listed, func(st *apiv1.PipelineStream) bool { return st.GetName() == ps.Stream }) {
			out.Notes = append(out.Notes, fmt.Sprintf("stream %s is paused, and the version doesn't read it, so its pause goes", ps.Stream))
			continue
		}
		found := false
		for _, st := range out.Streams {
			if st.GetName() == ps.Stream {
				st.Paused, found = true, true
			}
		}
		if !found {
			out.Notes = append(out.Notes, fmt.Sprintf("stream %s is paused, so syncs skip it (openetl resume --stream %s)", ps.Stream, ps.Stream))
		}
	}
	return nil
}

// keepRuntime drops what the Apply's new versions change: heal's overrides
// of the fields they change, and pauses of streams they no longer read.
func (s *Server) keepRuntime(ctx context.Context, q *db.Queries, ch *changes) error {
	for _, p := range ch.pipelines {
		if p.prev == "" || p.prev == p.version {
			continue
		}
		_, dropped, err := s.overridesAfter(ctx, q, p)
		if err != nil {
			return err
		}
		for _, o := range dropped {
			if _, err := q.DeletePipelineOverride(ctx, db.DeletePipelineOverrideParams{OrgID: s.Tenant.Org, PipelineID: p.id, Field: o.Field}); err != nil {
				return err
			}
			n, err := q.GetPipelineNames(ctx, db.GetPipelineNamesParams{OrgID: s.Tenant.Org, ID: p.id})
			if err != nil {
				return err
			}
			if err := heal.RecordOverride(ctx, q, s.Tenant.Org, n.WorkspaceID, n.Workspace, n.Name, o.Field, "", nil); err != nil {
				return err
			}
		}
		// A transform reads every stream.
		if listed := p.v.spec.GetSource().GetStreams(); len(listed) > 0 || p.v.spec.GetTransform() != nil {
			names := make([]string, 0, len(listed))
			for _, st := range listed {
				if p.v.spec.GetTransform() == nil {
					names = append(names, st.GetName())
				}
			}
			if err := q.ResumeStreamsNotIn(ctx, db.ResumeStreamsNotInParams{OrgID: s.Tenant.Org, PipelineID: p.id, Streams: names}); err != nil {
				return err
			}
		}
	}
	return nil
}

// effectiveSchedule is when the pipeline's syncs start: its spec's, or
// heal's override of how often.
func (s *Server) effectiveSchedule(ctx context.Context, pipelineID string, spec *apiv1.Schedule) (*apiv1.Schedule, error) {
	id, err := uuid.Parse(pipelineID)
	if err != nil {
		return nil, err
	}
	o, err := s.Store.GetPipelineOverride(ctx, db.GetPipelineOverrideParams{OrgID: s.Tenant.Org, PipelineID: id, Field: heal.OverrideEvery})
	if errors.Is(err, pgx.ErrNoRows) {
		return spec, nil
	}
	if err != nil {
		return nil, err
	}
	return heal.Schedule(spec, o.Value)
}
