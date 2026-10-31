package api

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/spec"
	"github.com/unsuitable001/openetl/internal/store"
	"github.com/unsuitable001/openetl/internal/store/db"
)

var errValidateOnly = errors.New("validate only")

type schedule struct {
	pipelineID string
	spec       *apiv1.Schedule
	paused     bool
}

func (s *Server) Apply(ctx context.Context, req *connect.Request[apiv1.ApplyRequest]) (*connect.Response[apiv1.ApplyResponse], error) {
	defWS := store.DefaultName
	if req.Msg.GetWorkspace() != "" {
		ws, err := parseParent(req.Msg.GetWorkspace())
		if err != nil {
			return nil, err
		}
		defWS = ws
	}
	docs := make([]*spec.Document, len(req.Msg.GetDocuments()))
	origins := make([]string, len(docs))
	seen := map[string]int{}
	for i, d := range req.Msg.GetDocuments() {
		origins[i] = cmp.Or(d.GetOrigin(), fmt.Sprintf("document %d", i+1))
		doc, err := spec.Parse(d.GetContent())
		if err != nil {
			return nil, invalid("%s: %v", origins[i], err)
		}
		doc.Workspace = cmp.Or(doc.Workspace, defWS)
		key := doc.Kind + "/" + doc.Workspace + "/" + doc.Name
		if j, ok := seen[key]; ok {
			return nil, invalid("%s and %s both define %s %s", origins[j], origins[i], doc.Kind, doc.Name)
		}
		seen[key] = i
		docs[i] = doc
	}
	if len(docs) == 0 {
		return nil, invalid("no documents")
	}
	// Pipelines refer to sources and destinations, so those go first.
	order := make([]int, len(docs))
	for i := range order {
		order[i] = i
	}
	isPipeline := func(i int) int {
		if docs[i].Kind == spec.KindPipeline {
			return 1
		}
		return 0
	}
	slices.SortStableFunc(order, func(a, b int) int { return cmp.Compare(isPipeline(a), isPipeline(b)) })

	res := &apiv1.ApplyResponse{Resources: make([]*apiv1.AppliedResource, len(docs))}
	var scheds []schedule
	err := s.Store.Tx(ctx, func(q *db.Queries) error {
		for _, i := range order {
			applied, sched, err := s.applyOne(ctx, q, docs[i])
			if err != nil {
				return fmt.Errorf("%s: %w", origins[i], err)
			}
			res.Resources[i] = applied
			if sched != nil {
				scheds = append(scheds, *sched)
			}
		}
		if req.Msg.GetValidateOnly() {
			return errValidateOnly
		}
		return nil
	})
	if errors.Is(err, errValidateOnly) {
		return connect.NewResponse(res), nil
	}
	if err != nil {
		return nil, err
	}
	for _, sc := range scheds {
		if err := s.Schedules.Upsert(ctx, s.Tenant.Org.String(), sc.pipelineID, sc.spec, sc.paused); err != nil {
			s.log().Error("schedule update failed", "pipeline", sc.pipelineID, "err", err)
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("the definitions were stored, but a schedule couldn't be updated; apply again: %w", err))
		}
	}
	return connect.NewResponse(res), nil
}

func (s *Server) workspace(ctx context.Context, q *db.Queries, name string) (uuid.UUID, error) {
	ws, err := q.GetWorkspace(ctx, db.GetWorkspaceParams{OrgID: s.Tenant.Org, Name: name})
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, invalid("workspace %q doesn't exist", name)
	}
	return ws.ID, err
}

func (s *Server) applyOne(ctx context.Context, q *db.Queries, doc *spec.Document) (*apiv1.AppliedResource, *schedule, error) {
	wsID, err := s.workspace(ctx, q, doc.Workspace)
	if err != nil {
		return nil, nil, err
	}
	switch doc.Kind {
	case spec.KindSource, spec.KindDestination:
		res, err := s.applyConnector(ctx, q, doc, wsID)
		return res, nil, err
	default:
		return s.applyPipeline(ctx, q, doc, wsID)
	}
}

func digest(m proto.Message, extra ...string) (string, error) {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write(b)
	for _, e := range extra {
		h.Write([]byte{0})
		h.Write([]byte(e))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (s *Server) applyConnector(ctx context.Context, q *db.Queries, doc *spec.Document, wsID uuid.UUID) (*apiv1.AppliedResource, error) {
	cfg, err := protojson.Marshal(doc.Connector.GetConfig())
	if err != nil {
		return nil, err
	}
	collection := "sources"
	if doc.Kind == spec.KindSource {
		_, err = connectorhost.NewSource(doc.Connector.GetConnector(), cfg)
	} else {
		collection = "destinations"
		_, err = connectorhost.NewDestination(doc.Connector.GetConnector(), cfg)
	}
	if err != nil {
		return nil, invalid("%v", err)
	}
	version, err := digest(doc.Connector)
	if err != nil {
		return nil, err
	}
	stored, err := protojson.Marshal(doc.Connector)
	if err != nil {
		return nil, err
	}
	res := &apiv1.AppliedResource{Kind: doc.Kind, Name: resourceName(doc.Workspace, collection, doc.Name), Version: version}

	var cur struct {
		id      uuid.UUID
		version string
	}
	if doc.Kind == spec.KindSource {
		row, gerr := q.GetSource(ctx, db.GetSourceParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: doc.Name})
		cur.id, cur.version, err = row.ID, row.Version, gerr
	} else {
		row, gerr := q.GetDestination(ctx, db.GetDestinationParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: doc.Name})
		cur.id, cur.version, err = row.ID, row.Version, gerr
	}
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		res.Action = apiv1.ApplyAction_APPLY_ACTION_CREATED
		if doc.Kind == spec.KindSource {
			err = q.InsertSource(ctx, db.InsertSourceParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, ID: uuid.New(), Name: doc.Name, Spec: stored, Version: version})
		} else {
			err = q.InsertDestination(ctx, db.InsertDestinationParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, ID: uuid.New(), Name: doc.Name, Spec: stored, Version: version})
		}
	case err != nil:
	case cur.version == version:
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UNCHANGED
	default:
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UPDATED
		if doc.Kind == spec.KindSource {
			err = q.UpdateSource(ctx, db.UpdateSourceParams{OrgID: s.Tenant.Org, ID: cur.id, Spec: stored, Version: version})
		} else {
			err = q.UpdateDestination(ctx, db.UpdateDestinationParams{OrgID: s.Tenant.Org, ID: cur.id, Spec: stored, Version: version})
		}
	}
	return res, err
}

func (s *Server) applyPipeline(ctx context.Context, q *db.Queries, doc *spec.Document, wsID uuid.UUID) (*apiv1.AppliedResource, *schedule, error) {
	p := doc.Pipeline
	src, err := q.GetSource(ctx, db.GetSourceParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: p.GetSource().GetRef()})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, invalid("source %q doesn't exist in workspace %q", p.GetSource().GetRef(), doc.Workspace)
	}
	if err != nil {
		return nil, nil, err
	}
	dst, err := q.GetDestination(ctx, db.GetDestinationParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: p.GetDestination().GetRef()})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, invalid("destination %q doesn't exist in workspace %q", p.GetDestination().GetRef(), doc.Workspace)
	}
	if err != nil {
		return nil, nil, err
	}
	dp := cmp.Or(p.GetDataPlane(), s.DefaultDataPlane)
	if _, err := q.GetDataPlane(ctx, db.GetDataPlaneParams{OrgID: s.Tenant.Org, ID: dp}); errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, invalid("data plane %q doesn't exist", dp)
	} else if err != nil {
		return nil, nil, err
	}

	version, err := digest(p, src.ID.String(), dst.ID.String(), dp)
	if err != nil {
		return nil, nil, err
	}
	stored, err := protojson.Marshal(p)
	if err != nil {
		return nil, nil, err
	}
	res := &apiv1.AppliedResource{Kind: doc.Kind, Name: resourceName(doc.Workspace, "pipelines", doc.Name), Version: version}

	cur, err := q.GetPipeline(ctx, db.GetPipelineParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: doc.Name})
	id := cur.ID
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		res.Action = apiv1.ApplyAction_APPLY_ACTION_CREATED
		id = uuid.New()
		err = q.InsertPipeline(ctx, db.InsertPipelineParams{
			OrgID: s.Tenant.Org, WorkspaceID: wsID, ID: id, Name: doc.Name,
			SourceID: src.ID, DestinationID: dst.ID, DataPlaneID: dp, Version: version,
		})
	case err != nil:
	case cur.Version == version:
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UNCHANGED
	default:
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UPDATED
		err = q.UpdatePipeline(ctx, db.UpdatePipelineParams{
			OrgID: s.Tenant.Org, ID: id, SourceID: src.ID, DestinationID: dst.ID, DataPlaneID: dp, Version: version,
		})
	}
	if err != nil {
		return nil, nil, err
	}
	err = q.InsertPipelineVersion(ctx, db.InsertPipelineVersionParams{
		OrgID: s.Tenant.Org, PipelineID: id, Version: version, Spec: stored,
		SourceID: src.ID, DestinationID: dst.ID, DataPlaneID: dp,
	})
	if err != nil {
		return nil, nil, err
	}
	return res, &schedule{pipelineID: id.String(), spec: p.GetSchedule(), paused: cur.Paused}, nil
}
