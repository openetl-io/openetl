package api

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/internal/heal"
	"github.com/unsuitable001/openetl/internal/plan"
	"github.com/unsuitable001/openetl/internal/store/db"
)

// A held incident turns critical once its oldest hold is this old.
const heldCritical = 24 * time.Hour

func (s *Server) ListHolds(ctx context.Context, req *connect.Request[apiv1.ListHoldsRequest]) (*connect.Response[apiv1.ListHoldsResponse], error) {
	_, p, err := s.getPipeline(ctx, req.Msg.GetParent())
	if err != nil {
		return nil, err
	}
	rows, err := s.Store.ListHolds(ctx, db.ListHoldsParams{OrgID: s.Tenant.Org, PipelineID: p.ID})
	if err != nil {
		return nil, err
	}
	out := make([]*apiv1.Hold, len(rows))
	for i, r := range rows {
		out[i] = hold(r)
	}
	return connect.NewResponse(&apiv1.ListHoldsResponse{Holds: out}), nil
}

func hold(r db.ListHoldsRow) *apiv1.Hold {
	h := &apiv1.Hold{
		Stream: r.Stream, Column: r.ColumnName, Whole: r.Whole, Reason: r.Reason,
		SinceGeneration: uint64(r.SinceGeneration), SinceEpoch: uint64(r.SinceEpoch), //nolint:gosec // generations and epochs are positive
		Cells: uint64(r.Cells), Discarded: r.Discarded, Releasing: r.Releasing, //nolint:gosec // counts are positive
		Pending: r.ReleaseID != nil && !r.Discarded && !r.Releasing,
	}
	if r.StartTime != nil {
		h.StartTime = timestamppb.New(*r.StartTime)
	}
	return h
}

func (s *Server) ReleaseHold(ctx context.Context, req *connect.Request[apiv1.ReleaseHoldRequest]) (*connect.Response[apiv1.ReleaseHoldResponse], error) {
	h, err := s.release(ctx, req.Msg.GetParent(), req.Msg.GetStream(), req.Msg.GetColumn(), false)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.ReleaseHoldResponse{Hold: h}), nil
}

func (s *Server) DiscardHold(ctx context.Context, req *connect.Request[apiv1.DiscardHoldRequest]) (*connect.Response[apiv1.DiscardHoldResponse], error) {
	h, err := s.release(ctx, req.Msg.GetParent(), req.Msg.GetStream(), req.Msg.GetColumn(), true)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.DiscardHoldResponse{Hold: h}), nil
}

// release records a release, or a discard, of the stream's open holds on
// column: the column held whole, its held cells, or both. A continuous
// pipeline's run restarts to take it.
func (s *Server) release(ctx context.Context, parent, stream, column string, discard bool) (*apiv1.Hold, error) {
	audit(ctx).set("stream", stream)
	audit(ctx).set("column", column)
	ws, p, err := s.getPipeline(ctx, parent)
	if err != nil {
		return nil, err
	}
	if stream == "" || column == "" {
		return nil, invalid("stream and column are required")
	}
	what := "release"
	if discard {
		what = "discard"
	}
	var out *apiv1.Hold
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		rows, err := q.ListHolds(ctx, db.ListHoldsParams{OrgID: s.Tenant.Org, PipelineID: p.ID})
		if err != nil {
			return err
		}
		found := false
		for _, r := range rows {
			if r.Stream != stream || r.ColumnName != column {
				continue
			}
			found = true
			if r.Discarded || r.Releasing {
				continue
			}
			id, err := uuid.NewV7()
			if err != nil {
				return err
			}
			got, err := q.InsertHoldRelease(ctx, db.InsertHoldReleaseParams{
				OrgID: s.Tenant.Org, ID: id, PipelineID: p.ID, Stream: stream, ColumnName: column, Whole: r.Whole,
				SinceGeneration: r.SinceGeneration, SinceEpoch: r.SinceEpoch, Discard: discard, Principal: callerOf(ctx).principal,
			})
			if err != nil {
				return err
			}
			if got.Discard != discard {
				was := "release"
				if got.Discard {
					was = "discard"
				}
				return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("a %s of the hold on %s.%s waits for the pipeline's next run", was, stream, column))
			}
			r.ReleaseID, r.ReleaseDiscards = &got.ID, &got.Discard
			if out == nil {
				out = hold(r)
			}
		}
		switch {
		case !found:
			return notFound("stream %s of %s holds nothing in column %s", stream, parent, column)
		case out == nil:
			return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("the hold on %s.%s is already discarded or being released", stream, column))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	audit(ctx).set("action", what)
	pipeline, err := s.pipeline(ctx, ws, p)
	if err != nil {
		return nil, err
	}
	if plan.Continuous(pipeline.GetSpec()) {
		if err := s.Schedules.Refresh(ctx, s.Tenant.Org.String(), p.ID.String()); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// recordHolds replaces the pipeline's holds with a checkpoint's, if it's
// newer than the one they come from.
func (s *Server) recordHolds(ctx context.Context, pipeline uuid.UUID, m *dataplanev1.ReportCheckpointRequest) error {
	return s.Store.Tx(ctx, func(q *db.Queries) error {
		n, err := q.ClaimHoldCheckpoint(ctx, db.ClaimHoldCheckpointParams{
			OrgID: s.Tenant.Org, PipelineID: pipeline,
			Generation: int64(m.GetGeneration()), Epoch: int64(m.GetEpoch()), //nolint:gosec // generations and epochs are small
		})
		if err != nil || n == 0 {
			return err
		}
		if err := q.DeleteHolds(ctx, db.DeleteHoldsParams{OrgID: s.Tenant.Org, PipelineID: pipeline}); err != nil {
			return err
		}
		for _, st := range m.GetStreams() {
			for _, h := range st.GetHolds() {
				arg := db.InsertHoldParams{
					OrgID: s.Tenant.Org, PipelineID: pipeline, Stream: st.GetName(), ColumnName: h.GetColumn(), Whole: h.GetWhole(),
					Reason: h.GetReason(), SinceGeneration: int64(h.GetSince().GetGeneration()), SinceEpoch: int64(h.GetSince().GetEpoch()), //nolint:gosec // as above
					Cells: int64(h.GetCells()), Discarded: h.GetDiscarded(), Releasing: h.GetReleasing(), //nolint:gosec // counts fit
				}
				if t := h.GetStartTime(); t.IsValid() {
					at := t.AsTime()
					arg.StartTime = &at
				}
				if err := q.InsertHold(ctx, arg); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// checkHolds opens a held incident for each stream holding something it
// hadn't been, turns it critical once its oldest hold is a day old, and
// resolves it once the stream holds nothing. Discarded columns count as
// nothing: there's no decision left.
func (s *Server) checkHolds(ctx context.Context, pipeline uuid.UUID, sync db.Sync, m *dataplanev1.ReportCheckpointRequest) error {
	open, err := s.Store.OpenIncidents(ctx, db.OpenIncidentsParams{OrgID: s.Tenant.Org, PipelineID: pipeline, Class: string(heal.Held)})
	if err != nil {
		return err
	}
	opened := map[string]bool{}
	for _, o := range open {
		opened[o.Stream] = true
	}
	now := m.GetCreateTime().AsTime()
	return s.Store.Tx(ctx, func(q *db.Queries) error {
		for _, st := range m.GetStreams() {
			var held []string
			whole, cells := 0, uint64(0)
			oldest := now
			for _, h := range st.GetHolds() {
				if h.GetDiscarded() {
					continue
				}
				if t := h.GetStartTime(); t.IsValid() && t.AsTime().Before(oldest) {
					oldest = t.AsTime()
				}
				if h.GetWhole() {
					whole++
					held = append(held, fmt.Sprintf("column %s (%s)", h.GetColumn(), h.GetReason()))
				} else {
					cells += h.GetCells()
					held = append(held, fmt.Sprintf("%d cells of %s (%s)", h.GetCells(), h.GetColumn(), h.GetReason()))
				}
			}
			if len(held) == 0 {
				if opened[st.GetName()] {
					stream := st.GetName()
					if _, err := heal.Resolve(ctx, q, s.Tenant.Org, pipeline, []string{string(heal.Held)}, &stream, "the stream holds nothing"); err != nil {
						return err
					}
				}
				continue
			}
			var parts []string
			if whole > 0 {
				parts = append(parts, plural(whole, "column"))
			}
			if cells > 0 {
				parts = append(parts, plural(int(cells), "cell"))
			}
			o := heal.Observation{
				Pipeline: pipeline, Version: sync.Version, Stream: st.GetName(), Class: heal.Held, Severity: heal.Warning,
				Message: "The stream holds back " + strings.Join(held, ", ") + ".",
				Details: map[string]string{
					"held": strings.Join(parts, " and "), "columns": strconv.Itoa(whole), "cells": strconv.FormatUint(cells, 10),
					"oldest": oldest.UTC().Format(time.RFC3339),
				},
				Sync: &sync.ID, Key: sync.ID.String(),
			}
			if now.Sub(oldest) >= heldCritical {
				o.Severity = heal.Critical
			}
			if _, err := heal.Observe(ctx, q, s.Tenant.Org, o); err != nil {
				return err
			}
		}
		return nil
	})
}

func plural(n int, what string) string {
	if n == 1 {
		return "1 " + what
	}
	return strconv.Itoa(n) + " " + what + "s"
}

// pendingReleases are the releases a run of the pipeline takes.
func (s *Server) pendingReleases(ctx context.Context, pipeline uuid.UUID) ([]*dataplanev1.HoldRelease, error) {
	rows, err := s.Store.PendingHoldReleases(ctx, db.PendingHoldReleasesParams{OrgID: s.Tenant.Org, PipelineID: pipeline})
	if err != nil {
		return nil, err
	}
	out := make([]*dataplanev1.HoldRelease, 0, len(rows))
	for _, r := range rows {
		out = append(out, &dataplanev1.HoldRelease{
			Stream: r.Stream, Column: r.ColumnName, Discard: r.Discard,
			Since: &dataplanev1.CheckpointRef{Generation: uint64(r.SinceGeneration), Epoch: uint64(r.SinceEpoch)}, //nolint:gosec // positive
		})
	}
	return out, nil
}
