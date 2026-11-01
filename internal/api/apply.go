package api

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"

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
		var key string
		if versioned(doc) {
			key = "connector/" + connectorKind(doc) + "/" + doc.Name + "@" + doc.Version
		} else {
			doc.Workspace = cmp.Or(doc.Workspace, defWS)
			key = doc.Kind + "/" + doc.Workspace + "/" + doc.Name
		}
		if j, ok := seen[key]; ok {
			return nil, invalid("%s and %s both define %s %s", origins[j], origins[i], doc.Kind, doc.Name)
		}
		seen[key] = i
		docs[i] = doc
	}
	if len(docs) == 0 {
		return nil, invalid("no documents")
	}
	for _, doc := range docs {
		audit(ctx).touched(documentName(doc))
	}
	for i, doc := range docs {
		perm, ws := "documents.apply", doc.Workspace
		if versioned(doc) {
			perm, ws = "connectors.apply", ""
		}
		if err := authorize(ctx, perm, ws); err != nil {
			return nil, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("%s: %w", origins[i], errors.Unwrap(err)))
		}
	}
	if err := s.pinImages(ctx, docs, origins); err != nil {
		return nil, err
	}
	// Sources and destinations refer to connector versions, and pipelines to
	// sources, destinations and dbt projects, so each goes before what refers
	// to it.
	order := make([]int, len(docs))
	for i := range order {
		order[i] = i
	}
	rank := func(i int) int {
		switch {
		case versioned(docs[i]):
			return 0
		case docs[i].Kind == spec.KindPipeline:
			return 2
		}
		return 1
	}
	slices.SortStableFunc(order, func(a, b int) int { return cmp.Compare(rank(a), rank(b)) })
	client := cmp.Or(req.Msg.GetClient(), "api")
	audit(ctx).set("client", client)

	res := &apiv1.ApplyResponse{Resources: make([]*apiv1.AppliedResource, len(docs))}
	var scheds []schedule
	err := s.Store.Tx(ctx, func(q *db.Queries) error {
		for _, i := range order {
			applied, sched, err := s.applyOne(ctx, q, docs[i], client+":"+origins[i])
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
		audit(ctx).set("validateOnly", true)
		auditApplied(ctx, res)
		return connect.NewResponse(res), nil
	}
	if err != nil {
		return nil, err
	}
	auditApplied(ctx, res)
	for _, sc := range scheds {
		if err := s.Schedules.Upsert(ctx, s.Tenant.Org.String(), sc.pipelineID, sc.spec, sc.paused); err != nil {
			s.log().Error("schedule update failed", "pipeline", sc.pipelineID, "err", err)
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("the definitions were stored, but a schedule couldn't be updated; apply again: %w", err))
		}
	}
	return connect.NewResponse(res), nil
}

// documentName is the resource a document defines.
func documentName(doc *spec.Document) string {
	if versioned(doc) {
		return connectorName(connectorKind(doc), doc.Name, doc.Version)
	}
	collection := map[string]string{
		spec.KindSource: "sources", spec.KindDestination: "destinations", spec.KindPipeline: "pipelines", spec.KindDbtProject: "dbtProjects",
	}[doc.Kind]
	return resourceName(doc.Workspace, collection, doc.Name)
}

// auditApplied records what each document did, and the version it has.
func auditApplied(ctx context.Context, res *apiv1.ApplyResponse) {
	applied := make([]any, len(res.GetResources()))
	for i, r := range res.GetResources() {
		applied[i] = map[string]any{
			"name": r.GetName(), "action": strings.ToLower(strings.TrimPrefix(r.GetAction().String(), "APPLY_ACTION_")), "version": r.GetVersion(),
		}
	}
	audit(ctx).set("applied", applied)
}

func (s *Server) workspace(ctx context.Context, q *db.Queries, name string) (uuid.UUID, error) {
	ws, err := q.GetWorkspace(ctx, db.GetWorkspaceParams{OrgID: s.Tenant.Org, Name: name})
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, invalid("workspace %q doesn't exist", name)
	}
	return ws.ID, err
}

func (s *Server) applyOne(ctx context.Context, q *db.Queries, doc *spec.Document, provenance string) (*apiv1.AppliedResource, *schedule, error) {
	if versioned(doc) {
		res, err := s.applyConnectorVersion(ctx, q, doc, provenance)
		return res, nil, err
	}
	wsID, err := s.workspace(ctx, q, doc.Workspace)
	if err != nil {
		return nil, nil, err
	}
	switch doc.Kind {
	case spec.KindSource, spec.KindDestination:
		res, err := s.applyConnector(ctx, q, doc, wsID)
		return res, nil, err
	case spec.KindDbtProject:
		res, err := s.applyDbtProject(ctx, q, doc, wsID)
		return res, nil, err
	default:
		return s.applyPipeline(ctx, q, doc, wsID)
	}
}

// digest hashes m and extra; empty extras leave the digest as it was
// without them.
func digest(m proto.Message, extra ...string) (string, error) {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write(b)
	for _, e := range extra {
		if e == "" {
			continue
		}
		h.Write([]byte{0})
		h.Write([]byte(e))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (s *Server) applyConnector(ctx context.Context, q *db.Queries, doc *spec.Document, wsID uuid.UUID) (*apiv1.AppliedResource, error) {
	kind, collection := "source", "sources"
	if doc.Kind == spec.KindDestination {
		kind, collection = "destination", "destinations"
	}
	name, ref := connectorhost.SplitRef(doc.Connector.GetConnector())
	v, err := s.resolveConnector(ctx, q, kind, name, ref)
	if err != nil {
		return nil, err
	}
	env, connector := s.Connectors, ""
	if v != nil {
		env, connector = env.With(v), v.Digest
	}
	props, known, err := connectorhost.SecretProperties(kind, name, env)
	if err != nil {
		return nil, invalid("%v", err)
	}
	secrets, masked, err := bindSecrets(doc.Connector.GetConfig(), props, known)
	if err != nil {
		return nil, err
	}
	if err := s.checkSecretsExist(ctx, q, wsID, doc.Workspace, secrets); err != nil {
		return nil, err
	}
	cfg, err := protojson.Marshal(masked)
	if err != nil {
		return nil, err
	}
	if kind == "source" {
		_, err = connectorhost.NewSource(name, cfg, env)
	} else {
		_, err = connectorhost.NewDestination(name, cfg, env)
	}
	if err != nil {
		return nil, invalid("%v", err)
	}
	version, err := digest(doc.Connector, connector)
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
			err = q.InsertSource(ctx, db.InsertSourceParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, ID: uuid.New(), Name: doc.Name, Spec: stored, Version: version, ConnectorDigest: connector, Secrets: secrets})
		} else {
			err = q.InsertDestination(ctx, db.InsertDestinationParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, ID: uuid.New(), Name: doc.Name, Spec: stored, Version: version, ConnectorDigest: connector, Secrets: secrets})
		}
	case err != nil:
	case cur.version == version:
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UNCHANGED
	default:
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UPDATED
		if doc.Kind == spec.KindSource {
			err = q.UpdateSource(ctx, db.UpdateSourceParams{OrgID: s.Tenant.Org, ID: cur.id, Spec: stored, Version: version, ConnectorDigest: connector, Secrets: secrets})
		} else {
			err = q.UpdateDestination(ctx, db.UpdateDestinationParams{OrgID: s.Tenant.Org, ID: cur.id, Spec: stored, Version: version, ConnectorDigest: connector, Secrets: secrets})
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
	if err := s.checkPostSync(ctx, q, p, wsID, doc.Workspace); err != nil {
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
