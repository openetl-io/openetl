package api

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/heal"
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/platform/ints"
	"github.com/openetl-io/openetl/internal/store/db"
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
		if out[i], err = hold(r); err != nil {
			return nil, err
		}
	}
	return connect.NewResponse(&apiv1.ListHoldsResponse{Holds: out}), nil
}

func hold(r db.ListHoldsRow) (*apiv1.Hold, error) {
	refused := refusedBy(r)
	h := &apiv1.Hold{
		Stream: r.Stream, Column: r.ColumnName, Whole: r.Whole, Reason: r.Reason,
		SinceGeneration: ints.Uint64(r.SinceGeneration), SinceEpoch: ints.Uint64(r.SinceEpoch),
		Cells: ints.Uint64(r.Cells), Discarded: r.Discarded, Releasing: r.Releasing,
		Pending: r.ReleaseID != nil && (!r.Discarded || !discards(r)) && !r.Releasing && !refused,
	}
	if refused && !r.Discarded {
		h.Refused = &apiv1.HoldRefusal{Reason: r.Refusal}
		if r.ReleaseEnds != nil && *r.ReleaseEnds {
			h.Refused.Reason += "; it was discarded while being patched in, and ended in a run in which the stream held, so the discard dropped nothing"
		}
	}
	if r.StartTime != nil {
		h.StartTime = timestamppb.New(*r.StartTime)
	}
	if len(r.Cause) > 0 {
		h.Cause = &apiv1.HoldCause{}
		if err := protojson.Unmarshal(r.Cause, h.Cause); err != nil {
			return nil, fmt.Errorf("hold on %s.%s: %w", r.Stream, r.ColumnName, err)
		}
	}
	explain(h)
	return h, nil
}

// refusedBy reports whether the hold's release is one it refused.
func refusedBy(r db.ListHoldsRow) bool {
	return r.ReleaseID != nil && r.RefusedRelease != nil && *r.ReleaseID == *r.RefusedRelease
}

// discards reports whether the hold's release is a discard, or a release a
// person discarded, which a run takes as one unless it has started patching
// it in.
func discards(r db.ListHoldsRow) bool {
	return r.ReleaseDiscards != nil && *r.ReleaseDiscards || r.ReleaseEnds != nil && *r.ReleaseEnds
}

// replacedBy reports whether a release of the hold replaces the one it
// has: the discard a discarded hold took.
func replacedBy(r db.ListHoldsRow) bool {
	return r.Discarded && r.ReleaseID != nil && discards(r)
}

// released reports whether a discard of the hold ends a release of it: one
// being patched in, or one that waits for the pipeline's next run, unless
// it ends a discarded hold.
func released(r db.ListHoldsRow) bool {
	return r.Releasing || !r.Discarded && r.ReleaseID != nil && r.ReleaseDiscards != nil && !*r.ReleaseDiscards
}

func (s *Server) ReleaseHold(ctx context.Context, req *connect.Request[apiv1.ReleaseHoldRequest]) (*connect.Response[apiv1.ReleaseHoldResponse], error) {
	h, _, err := s.release(ctx, req.Msg.GetParent(), req.Msg.GetStream(), req.Msg.GetColumn(), false, req.Msg.GetNewVersion())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.ReleaseHoldResponse{Hold: h}), nil
}

func (s *Server) DiscardHold(ctx context.Context, req *connect.Request[apiv1.DiscardHoldRequest]) (*connect.Response[apiv1.DiscardHoldResponse], error) {
	h, ends, err := s.release(ctx, req.Msg.GetParent(), req.Msg.GetStream(), req.Msg.GetColumn(), true, false)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.DiscardHoldResponse{Hold: h, EndsRelease: ends}), nil
}

// release records a release, or a discard, of the stream's open holds on
// column: the column held whole, its held cells, or both. With newVersion,
// the column held whole goes to a new version of it, and its cells with
// it. A release of a discarded column hold ends it: the column is delivered
// again, and the values it dropped stay dropped. A discard of a hold that
// has a release, waiting for the next run or being patched in, marks the
// release instead, and reports so (ends): a run takes it as a discard,
// unless a run has started patching it in, as only the pipeline's record
// says. Then the release goes on while the stream holds, a run in which the
// stream doesn't hold drops what it hasn't patched in, and the mark lapses
// once the release ends. A continuous pipeline's run restarts to take it.
func (s *Server) release(ctx context.Context, parent, stream, column string, discard, newVersion bool) (*apiv1.Hold, bool, error) {
	audit(ctx).set("stream", stream)
	audit(ctx).set("column", column)
	if newVersion {
		audit(ctx).set("new_version", "true")
	}
	ws, p, err := s.getPipeline(ctx, parent)
	if err != nil {
		return nil, false, err
	}
	if stream == "" || column == "" {
		return nil, false, invalid("stream and column are required")
	}
	what := "release"
	if discard {
		what = "discard"
	}
	var out *apiv1.Hold
	ends := false
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		// Before any of them, so the transaction locks them by ID, as a
		// checkpoint's report does.
		if err := q.LockHoldReleases(ctx, db.LockHoldReleasesParams{OrgID: s.Tenant.Org, PipelineID: p.ID, Stream: stream, ColumnName: column}); err != nil {
			return err
		}
		rows, err := q.ListHolds(ctx, db.ListHoldsParams{OrgID: s.Tenant.Org, PipelineID: p.ID})
		if err != nil {
			return err
		}
		found, whole, discarded := false, false, false
		// The column's held cells, which a discard marks a release with.
		var cells int64
		for i, r := range rows {
			if r.Stream != stream || r.ColumnName != column {
				continue
			}
			if !r.Whole {
				cells = r.Cells
			}
			whole = whole || r.Whole && !r.Discarded && !r.Releasing
			discarded = discarded || r.Whole && r.Discarded
			// A new release replaces the one the hold refused, or the discard
			// it took.
			if refusedBy(r) || !discard && replacedBy(r) {
				if err := q.DeleteHoldRelease(ctx, db.DeleteHoldReleaseParams{OrgID: s.Tenant.Org, ID: *r.ReleaseID}); err != nil {
					return err
				}
				rows[i].ReleaseID, rows[i].ReleaseDiscards, rows[i].ReleaseEnds = nil, nil, nil
			}
		}
		switch {
		case newVersion && !whole && discarded:
			return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("the hold on %s.%s is discarded, so it can't go to a new version; release it to deliver the column again", stream, column))
		case newVersion && !whole:
			return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s.%s isn't held whole, so it can't go to a new version", stream, column))
		}
		for _, r := range rows {
			if r.Stream != stream || r.ColumnName != column {
				continue
			}
			found = true
			if discard && released(r) {
				// A run may have started patching it in, which only the
				// pipeline's record says, so the release stays, marked.
				ends = true
				r.ReleaseEnds = &ends
				if out == nil {
					if out, err = hold(r); err != nil {
						return err
					}
				}
				continue
			}
			if r.Releasing && !discard || r.Discarded && discard {
				continue
			}
			id, err := uuid.NewV7()
			if err != nil {
				return err
			}
			// A release made again of a hold whose release a person
			// discarded clears the discard.
			got, err := q.InsertHoldRelease(ctx, db.InsertHoldReleaseParams{
				OrgID: s.Tenant.Org, ID: id, PipelineID: p.ID, Stream: stream, ColumnName: column, Whole: r.Whole,
				SinceGeneration: r.SinceGeneration, SinceEpoch: r.SinceEpoch, Discard: discard, NewVersion: newVersion, Principal: callerOf(ctx).principal,
			})
			if err != nil {
				return err
			}
			if got.Discard != discard || got.NewVersion != newVersion {
				was := "release"
				switch {
				case got.Discard:
					was = "discard"
				case got.NewVersion:
					was = "release to a new version"
				}
				return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("a %s of the hold on %s.%s waits for the pipeline's next run", was, stream, column))
			}
			r.ReleaseID, r.ReleaseDiscards = &got.ID, &got.Discard
			if out == nil {
				if out, err = hold(r); err != nil {
					return err
				}
			}
		}
		if ends {
			// A run knows the release it patches in by its ID, and the
			// column's holds can change while it does, so the column's
			// releases are marked.
			n, err := q.EndHoldReleases(ctx, db.EndHoldReleasesParams{OrgID: s.Tenant.Org, PipelineID: p.ID, Stream: stream, ColumnName: column, Cells: cells})
			if err != nil {
				return err
			}
			if n == 0 {
				return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("the hold on %s.%s is being released, and the server has no record of the release to discard", stream, column))
			}
		}
		switch {
		case !found:
			return notFound("stream %s of %s holds nothing in column %s", stream, parent, column)
		case out == nil && discard:
			return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("the hold on %s.%s is already discarded", stream, column))
		case out == nil:
			return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("the hold on %s.%s is being released; to drop what it hasn't patched in once the stream stops holding, discard it", stream, column))
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	audit(ctx).set("action", what)
	pipeline, err := s.pipeline(ctx, ws, p)
	if err != nil {
		return nil, false, err
	}
	if plan.Continuous(pipeline.GetSpec()) {
		if err := s.Schedules.Refresh(ctx, s.Tenant.Org.String(), p.ID.String()); err != nil {
			return nil, false, err
		}
	}
	return out, ends, nil
}

// holder is a stream the destination gets at a checkpoint, with its holds:
// one of the source's, or a SQL transform's output.
type holder struct {
	name  string
	holds []*dataplanev1.CheckpointHold
}

// holders are a checkpoint's streams, then its outputs that hold.
func holders(m *dataplanev1.ReportCheckpointRequest) []holder {
	out := make([]holder, 0, len(m.GetStreams())+len(m.GetOutputs()))
	for _, st := range m.GetStreams() {
		out = append(out, holder{st.GetName(), st.GetHolds()})
	}
	for _, o := range m.GetOutputs() {
		out = append(out, holder{o.GetName(), o.GetHolds()})
	}
	return out
}

// holdRows are a checkpoint's holds as the pipeline's rows.
func (s *Server) holdRows(pipeline uuid.UUID, m *dataplanev1.ReportCheckpointRequest, b *bigints) ([]db.InsertHoldParams, error) {
	var rows []db.InsertHoldParams
	for _, st := range holders(m) {
		for _, h := range st.holds {
			arg := db.InsertHoldParams{
				OrgID: s.Tenant.Org, PipelineID: pipeline, Stream: st.name, ColumnName: h.GetColumn(), Whole: h.GetWhole(),
				Reason: h.GetReason(), SinceGeneration: b.of("hold since.generation", h.GetSince().GetGeneration()), SinceEpoch: b.of("hold since.epoch", h.GetSince().GetEpoch()),
				Cells: b.of("hold cells", h.GetCells()), Discarded: h.GetDiscarded(), Releasing: h.GetReleasing(),
				Refusal: h.GetRefused().GetReason(),
			}
			if t := h.GetStartTime(); t.IsValid() {
				at := t.AsTime()
				arg.StartTime = &at
			}
			if c := h.GetCause(); c != nil {
				var err error
				if arg.Cause, err = protojson.Marshal(c); err != nil {
					return nil, err
				}
			}
			if id, err := uuid.Parse(h.GetRefusedRelease()); err == nil {
				arg.RefusedRelease = &id
			}
			rows = append(rows, arg)
		}
	}
	return rows, nil
}

// recordHolds replaces the pipeline's holds with rows, a checkpoint's, if
// it's newer than the one they come from, and unmarks the discarded
// releases that are over.
func (s *Server) recordHolds(ctx context.Context, pipeline uuid.UUID, generation, epoch int64, rows []db.InsertHoldParams) error {
	return s.Store.Tx(ctx, func(q *db.Queries) error {
		n, err := q.ClaimHoldCheckpoint(ctx, db.ClaimHoldCheckpointParams{
			OrgID: s.Tenant.Org, PipelineID: pipeline, Generation: generation, Epoch: epoch,
		})
		if err != nil || n == 0 {
			return err
		}
		if err := q.DeleteHolds(ctx, db.DeleteHoldsParams{OrgID: s.Tenant.Org, PipelineID: pipeline}); err != nil {
			return err
		}
		for _, arg := range rows {
			if err := q.InsertHold(ctx, arg); err != nil {
				return err
			}
		}
		return q.LapseHoldReleases(ctx, db.LapseHoldReleasesParams{OrgID: s.Tenant.Org, PipelineID: pipeline})
	})
}

// checkHolds opens a held incident for each stream holding something it
// hadn't been, turns it critical once its oldest hold is a day old, and
// resolves it once the stream holds nothing, or isn't in the checkpoint, as
// a stream the pipeline doesn't read now. Discarded columns count as
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
		// A SQL transform's output may share a stream's name; it holds and
		// the stream doesn't.
		outputs := map[string]bool{}
		for _, o := range m.GetOutputs() {
			outputs[o.GetName()] = true
		}
		for _, o := range open {
			if !slices.ContainsFunc(holders(m), func(h holder) bool { return h.name == o.Stream }) {
				stream := o.Stream
				if _, err := heal.Resolve(ctx, q, s.Tenant.Org, pipeline, []string{string(heal.Held)}, &stream, "the stream holds nothing"); err != nil {
					return err
				}
			}
		}
		for i, st := range holders(m) {
			if i < len(m.GetStreams()) && outputs[st.name] {
				continue
			}
			var held []string
			var holds []*apiv1.Hold
			whole, cells := 0, uint64(0)
			oldest := now
			for _, h := range st.holds {
				if h.GetDiscarded() {
					continue
				}
				if t := h.GetStartTime(); t.IsValid() && t.AsTime().Before(oldest) {
					oldest = t.AsTime()
				}
				if h.GetWhole() {
					whole++
				} else {
					cells += h.GetCells()
				}
				ah := &apiv1.Hold{
					Stream: st.name, Column: h.GetColumn(), Whole: h.GetWhole(), Reason: h.GetReason(), Cells: h.GetCells(),
					Releasing: h.GetReleasing(), Cause: h.GetCause(), Refused: h.GetRefused(),
				}
				explain(ah)
				holds = append(holds, ah)
				held = append(held, ah.GetReason())
			}
			if len(held) == 0 {
				if opened[st.name] {
					stream := st.name
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
				Pipeline: pipeline, Version: sync.Version, Stream: st.name, Class: heal.Held, Severity: heal.Warning,
				Message: "The stream holds back " + strings.Join(parts, " and ") + ": " + strings.Join(held, " "),
				Holds:   holds,
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
		out = append(out, holdRelease(r))
	}
	return out, nil
}

func holdRelease(r db.PendingHoldReleasesRow) *dataplanev1.HoldRelease {
	return &dataplanev1.HoldRelease{
		Id: r.ID.String(), Stream: r.Stream, Column: r.ColumnName, Discard: r.Discard, NewVersion: r.NewVersion,
		Ends: r.Ends, EndsCells: ints.Uint64(r.EndsCells),
		Since: &dataplanev1.CheckpointRef{Generation: ints.Uint64(r.SinceGeneration), Epoch: ints.Uint64(r.SinceEpoch)},
	}
}
