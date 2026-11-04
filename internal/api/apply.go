package api

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/events"
	"github.com/unsuitable001/openetl/internal/plan"
	"github.com/unsuitable001/openetl/internal/spec"
	"github.com/unsuitable001/openetl/internal/store"
	"github.com/unsuitable001/openetl/internal/store/db"
)

var errValidateOnly = errors.New("validate only")

type schedule struct {
	pipelineID string
	spec       *apiv1.Schedule
	paused     bool
	continuous bool
}

// runs is what runs a pipeline an apply changed: its schedule, or if it's
// continuous, its supervisor. Each is set again on every apply, which repairs
// one an earlier apply failed to set.
func (s *Server) runs(ctx context.Context, sc schedule) error {
	if sc.continuous {
		// A sync still running is fenced by the stream job.
		if err := s.Schedules.Delete(ctx, sc.pipelineID); err != nil {
			return err
		}
		return s.Schedules.Refresh(ctx, s.Tenant.Org.String(), sc.pipelineID)
	}
	if err := s.Schedules.StopStream(ctx, sc.pipelineID); err != nil {
		return err
	}
	return s.Schedules.Upsert(ctx, s.Tenant.Org.String(), sc.pipelineID, sc.spec, sc.paused)
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
	if err := s.pinBinaries(ctx, docs, origins); err != nil {
		return nil, err
	}
	// Sources and destinations refer to connector versions, and pipelines to
	// sources, destinations, dbt projects and Spark clusters, so each goes
	// before what refers to it.
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
	// Pipelines a source or destination change gave a version.
	var dependents []*planned
	err := s.Store.Tx(ctx, func(q *db.Queries) error {
		dependents = nil
		ch := newChanges()
		for _, i := range order {
			applied, sched, err := s.applyOne(ctx, q, docs[i], client+":"+origins[i], ch)
			if err != nil {
				return fmt.Errorf("%s: %w", origins[i], err)
			}
			res.Resources[i] = applied
			if sched != nil {
				scheds = append(scheds, *sched)
			}
		}
		var err error
		defined := len(ch.pipelines)
		if res.Dependents, err = s.repin(ctx, q, ch); err != nil {
			return err
		}
		dependents = ch.pipelines[defined:]
		for _, p := range ch.pipelines {
			pl, err := s.plan(ctx, q, p)
			if err != nil {
				return fmt.Errorf("%s: plan: %w", p.name, err)
			}
			res.Plans = append(res.Plans, pl)
		}
		if req.Msg.GetValidateOnly() {
			unknownVersions(res, ch)
			return errValidateOnly
		}
		if err := refuse(ch, res.Plans); err != nil {
			return err
		}
		return s.recordApplied(ctx, q, ch)
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
	for _, d := range dependents {
		if plan.Continuous(d.v.spec) {
			scheds = append(scheds, schedule{pipelineID: d.id.String(), continuous: true})
		}
	}
	for _, sc := range scheds {
		if err := s.runs(ctx, sc); err != nil {
			s.log().Error("schedule update failed", "pipeline", sc.pipelineID, "err", err)
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("the definitions were stored, but what runs a pipeline couldn't be updated; apply again: %w", err))
		}
	}
	return connect.NewResponse(res), nil
}

// unknownVersions clears the version of each pipeline whose source or
// destination the dry run created: a pipeline's version hashes their IDs,
// and the apply will give them others.
func unknownVersions(res *apiv1.ApplyResponse, ch *changes) {
	unknown := map[string]bool{}
	for i, p := range ch.pipelines {
		if ch.created[p.v.src.ID] || ch.created[p.v.dst.ID] {
			unknown[p.name] = true
			res.Plans[i].Version = ""
		}
	}
	for _, r := range res.GetResources() {
		if unknown[r.GetName()] {
			r.Version = ""
		}
	}
}

// documentName is the resource a document defines.
func documentName(doc *spec.Document) string {
	if versioned(doc) {
		return connectorName(connectorKind(doc), doc.Name, doc.Version)
	}
	collection := map[string]string{
		spec.KindSource: "sources", spec.KindDestination: "destinations", spec.KindPipeline: "pipelines", spec.KindDbtProject: "dbtProjects",
		spec.KindWebhook: "webhooks", spec.KindSparkCluster: "sparkClusters",
	}[doc.Kind]
	return resourceName(doc.Workspace, collection, doc.Name)
}

// auditApplied records what each document and dependent did, and the
// version it has.
func auditApplied(ctx context.Context, res *apiv1.ApplyResponse) {
	var applied []any
	for _, r := range slices.Concat(res.GetResources(), res.GetDependents()) {
		applied = append(applied, map[string]any{
			"name": r.GetName(), "action": strings.ToLower(strings.TrimPrefix(r.GetAction().String(), "APPLY_ACTION_")), "version": r.GetVersion(),
		})
	}
	for _, r := range res.GetDependents() {
		audit(ctx).touched(r.GetName())
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

func (s *Server) applyOne(ctx context.Context, q *db.Queries, doc *spec.Document, provenance string, ch *changes) (*apiv1.AppliedResource, *schedule, error) {
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
		res, err := s.applyConnector(ctx, q, doc, wsID, provenance, ch)
		return res, nil, err
	case spec.KindDbtProject:
		res, err := s.applyDbtProject(ctx, q, doc, wsID, provenance, ch)
		return res, nil, err
	case spec.KindWebhook:
		res, err := s.applyWebhook(ctx, q, doc, wsID)
		return res, nil, err
	case spec.KindSparkCluster:
		res, err := s.applySparkCluster(ctx, q, doc, wsID, provenance, ch)
		return res, nil, err
	default:
		return s.applyPipeline(ctx, q, doc, wsID, provenance, ch)
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

func (s *Server) applyConnector(ctx context.Context, q *db.Queries, doc *spec.Document, wsID uuid.UUID, provenance string, ch *changes) (*apiv1.AppliedResource, error) {
	kind, collection := "source", "sources"
	if doc.Kind == spec.KindDestination {
		kind, collection = "destination", "destinations"
	}
	name, ref := connectorhost.SplitRef(doc.Connector.GetConnector())
	v, err := s.resolveConnector(ctx, q, kind, name, ref)
	if err != nil {
		return nil, err
	}
	connector := ""
	if v != nil {
		connector = v.Digest
	}
	b, err := s.bind(kind, name, v, doc.Connector.GetConfig())
	if err != nil {
		return nil, err
	}
	secrets := b.secrets
	if err := s.checkSecretsExist(ctx, q, wsID, doc.Workspace, secrets); err != nil {
		return nil, err
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
		res.Action, cur.id = apiv1.ApplyAction_APPLY_ACTION_CREATED, uuid.New()
		ch.created[cur.id] = true
		if doc.Kind == spec.KindSource {
			err = q.InsertSource(ctx, db.InsertSourceParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, ID: cur.id, Name: doc.Name, Spec: stored, Version: version, ConnectorDigest: connector, Secrets: secrets})
		} else {
			err = q.InsertDestination(ctx, db.InsertDestinationParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, ID: cur.id, Name: doc.Name, Spec: stored, Version: version, ConnectorDigest: connector, Secrets: secrets})
		}
	case err != nil:
		return nil, err
	case cur.version == version:
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UNCHANGED
		return res, nil
	default:
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UPDATED
		if doc.Kind == spec.KindSource {
			err = q.UpdateSource(ctx, db.UpdateSourceParams{OrgID: s.Tenant.Org, ID: cur.id, Spec: stored, Version: version, ConnectorDigest: connector, Secrets: secrets})
		} else {
			err = q.UpdateDestination(ctx, db.UpdateDestinationParams{OrgID: s.Tenant.Org, ID: cur.id, Spec: stored, Version: version, ConnectorDigest: connector, Secrets: secrets})
		}
	}
	if err != nil {
		return nil, err
	}
	if doc.Kind == spec.KindSource {
		ch.sources[cur.id] = provenance
		err = q.InsertSourceVersion(ctx, db.InsertSourceVersionParams{
			OrgID: s.Tenant.Org, SourceID: cur.id, Version: version, Spec: stored, ConnectorDigest: connector, Secrets: secrets, Provenance: provenance,
		})
	} else {
		ch.destinations[cur.id] = provenance
		err = q.InsertDestinationVersion(ctx, db.InsertDestinationVersionParams{
			OrgID: s.Tenant.Org, DestinationID: cur.id, Version: version, Spec: stored, ConnectorDigest: connector, Secrets: secrets, Provenance: provenance,
		})
	}
	return res, err
}

// pinned is a pipeline version: its spec, and what the spec named when it
// was applied.
type pinned struct {
	spec      *apiv1.PipelineSpec
	src       db.Source
	dst       db.Destination
	dataPlane string
	// Each dbt project's bundle, by name.
	dbt map[string]string
	// Each Spark cluster's version, by name.
	spark map[string]string
}

// pin resolves what a pipeline's spec names in its workspace.
func (s *Server) pin(ctx context.Context, q *db.Queries, p *apiv1.PipelineSpec, wsID uuid.UUID, workspace string) (*pinned, error) {
	v := &pinned{spec: p, dataPlane: cmp.Or(p.GetDataPlane(), s.DefaultDataPlane), dbt: map[string]string{}, spark: map[string]string{}}
	var err error
	v.src, err = q.GetSource(ctx, db.GetSourceParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: p.GetSource().GetRef()})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, invalid("source %q doesn't exist in workspace %q", p.GetSource().GetRef(), workspace)
	}
	if err != nil {
		return nil, err
	}
	v.dst, err = q.GetDestination(ctx, db.GetDestinationParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: p.GetDestination().GetRef()})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, invalid("destination %q doesn't exist in workspace %q", p.GetDestination().GetRef(), workspace)
	}
	if err != nil {
		return nil, err
	}
	for _, step := range p.GetPostSync() {
		if sp := step.GetSpark(); sp != nil {
			if v.spark[sp.GetCluster()], err = pinSparkCluster(ctx, q, s.Tenant.Org, wsID, workspace, sp); err != nil {
				return nil, err
			}
			continue
		}
		name := step.GetDbt().GetProject()
		proj, err := q.ShareDbtProject(ctx, db.ShareDbtProjectParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: name})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, invalid("dbt project %q doesn't exist in workspace %q", name, workspace)
		}
		if err != nil {
			return nil, err
		}
		v.dbt[name] = proj.Digest
	}
	if _, err := q.GetDataPlane(ctx, db.GetDataPlaneParams{OrgID: s.Tenant.Org, ID: v.dataPlane}); errors.Is(err, pgx.ErrNoRows) {
		return nil, invalid("data plane %q doesn't exist", v.dataPlane)
	} else if err != nil {
		return nil, err
	}
	return v, nil
}

func (v *pinned) version() (string, error) {
	extra := []string{v.src.ID.String(), v.dst.ID.String(), v.dataPlane, v.src.Version, v.dst.Version}
	for _, name := range slices.Sorted(maps.Keys(v.dbt)) {
		extra = append(extra, name+"@"+v.dbt[name])
	}
	for _, name := range slices.Sorted(maps.Keys(v.spark)) {
		extra = append(extra, "spark:"+name+"@"+v.spark[name])
	}
	return digest(v.spec, extra...)
}

// store makes v the current version of the pipeline cur, or of a new one if
// cur is nil.
func (v *pinned) store(ctx context.Context, q *db.Queries, org, wsID uuid.UUID, name string, cur *db.Pipeline, provenance string) (apiv1.ApplyAction, string, uuid.UUID, error) {
	version, err := v.version()
	if err != nil {
		return 0, "", uuid.Nil, err
	}
	var action apiv1.ApplyAction
	var id uuid.UUID
	switch {
	case cur == nil:
		action, id = apiv1.ApplyAction_APPLY_ACTION_CREATED, uuid.New()
		err = q.InsertPipeline(ctx, db.InsertPipelineParams{
			OrgID: org, WorkspaceID: wsID, ID: id, Name: name,
			SourceID: v.src.ID, DestinationID: v.dst.ID, DataPlaneID: v.dataPlane, Version: version,
		})
	case cur.Version == version:
		return apiv1.ApplyAction_APPLY_ACTION_UNCHANGED, version, cur.ID, nil
	default:
		action, id = apiv1.ApplyAction_APPLY_ACTION_UPDATED, cur.ID
		err = q.UpdatePipeline(ctx, db.UpdatePipelineParams{
			OrgID: org, ID: id, SourceID: v.src.ID, DestinationID: v.dst.ID, DataPlaneID: v.dataPlane, Version: version,
		})
	}
	if err != nil {
		return 0, "", uuid.Nil, err
	}
	spec, err := protojson.Marshal(v.spec)
	if err != nil {
		return 0, "", uuid.Nil, err
	}
	dbt, err := json.Marshal(v.dbt)
	if err != nil {
		return 0, "", uuid.Nil, err
	}
	spark, err := json.Marshal(v.spark)
	if err != nil {
		return 0, "", uuid.Nil, err
	}
	err = q.InsertPipelineVersion(ctx, db.InsertPipelineVersionParams{
		OrgID: org, PipelineID: id, Version: version, Spec: spec,
		SourceID: v.src.ID, DestinationID: v.dst.ID, DataPlaneID: v.dataPlane,
		SourceVersion: v.src.Version, DestinationVersion: v.dst.Version, Dbt: dbt, SparkClusters: spark, Provenance: provenance,
	})
	return action, version, id, err
}

func (s *Server) applyPipeline(ctx context.Context, q *db.Queries, doc *spec.Document, wsID uuid.UUID, provenance string, ch *changes) (*apiv1.AppliedResource, *schedule, error) {
	v, err := s.pin(ctx, q, doc.Pipeline, wsID, doc.Workspace)
	if err != nil {
		return nil, nil, err
	}
	cur, err := q.GetPipeline(ctx, db.GetPipelineParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: doc.Name})
	curp := &cur
	if errors.Is(err, pgx.ErrNoRows) {
		curp = nil
	} else if err != nil {
		return nil, nil, err
	}
	res := &apiv1.AppliedResource{Kind: doc.Kind, Name: resourceName(doc.Workspace, "pipelines", doc.Name)}
	var id uuid.UUID
	res.Action, res.Version, id, err = v.store(ctx, q, s.Tenant.Org, wsID, doc.Name, curp, provenance)
	if err != nil {
		return nil, nil, err
	}
	ch.pipelines = append(ch.pipelines, &planned{id: id, name: res.Name, prev: cur.Version, version: res.Version, action: res.Action, v: v, provenance: provenance})
	return res, &schedule{pipelineID: id.String(), spec: doc.Pipeline.GetSchedule(), paused: cur.Paused, continuous: plan.Continuous(doc.Pipeline)}, nil
}

// recordApplied records an event for each pipeline the Apply created or
// gave a new version.
func (s *Server) recordApplied(ctx context.Context, q *db.Queries, ch *changes) error {
	for _, p := range ch.pipelines {
		typ := events.PipelineUpdated
		switch p.action {
		case apiv1.ApplyAction_APPLY_ACTION_CREATED:
			typ = events.PipelineCreated
		case apiv1.ApplyAction_APPLY_ACTION_UPDATED:
		default:
			continue
		}
		_, err := events.Record(ctx, q, s.Tenant.Org, events.Event{
			Workspace: p.v.src.WorkspaceID, Type: typ, Pipeline: p.name[strings.LastIndex(p.name, "/")+1:],
			Data: events.Pipeline{Pipeline: p.name, Version: p.version, PreviousVersion: p.prev, Provenance: p.provenance},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// changes is what an Apply has changed so far: sources, destinations, dbt
// projects and Spark clusters, with the provenance of the documents that
// changed them, and the pipelines it gave versions, those the documents
// define first.
type changes struct {
	sources       map[uuid.UUID]string
	destinations  map[uuid.UUID]string
	dbtProjects   map[uuid.UUID]string
	sparkClusters map[uuid.UUID]string
	// The sources and destinations it created.
	created   map[uuid.UUID]bool
	pipelines []*planned
}

func newChanges() *changes {
	return &changes{
		sources: map[uuid.UUID]string{}, destinations: map[uuid.UUID]string{}, dbtProjects: map[uuid.UUID]string{},
		sparkClusters: map[uuid.UUID]string{}, created: map[uuid.UUID]bool{},
	}
}

// repin gives a new version to each pipeline whose current version pins a
// changed source, destination, dbt project or Spark cluster, unless the
// Apply applied the pipeline itself.
func (s *Server) repin(ctx context.Context, q *db.Queries, ch *changes) ([]*apiv1.AppliedResource, error) {
	if len(ch.sources)+len(ch.destinations)+len(ch.dbtProjects)+len(ch.sparkClusters) == 0 {
		return nil, nil
	}
	rows, err := q.ListPipelinesUsing(ctx, db.ListPipelinesUsingParams{
		OrgID:         s.Tenant.Org,
		Sources:       slices.Collect(maps.Keys(ch.sources)),
		Destinations:  slices.Collect(maps.Keys(ch.destinations)),
		DbtProjects:   slices.Collect(maps.Keys(ch.dbtProjects)),
		SparkClusters: slices.Collect(maps.Keys(ch.sparkClusters)),
	})
	if err != nil {
		return nil, err
	}
	var out []*apiv1.AppliedResource
	for _, r := range rows {
		if slices.ContainsFunc(ch.pipelines, func(p *planned) bool { return p.id == r.ID }) {
			continue
		}
		name := resourceName(r.Workspace, "pipelines", r.Name)
		var p apiv1.PipelineSpec
		if err := protojson.Unmarshal(r.Spec, &p); err != nil {
			return nil, err
		}
		v, err := s.pin(ctx, q, &p, r.WorkspaceID, r.Workspace)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		provenance, err := ch.cause(ctx, q, s.Tenant.Org, r)
		if err != nil {
			return nil, err
		}
		action, version, _, err := v.store(ctx, q, s.Tenant.Org, r.WorkspaceID, r.Name, &db.Pipeline{ID: r.ID, Version: r.Version}, provenance)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		if action != apiv1.ApplyAction_APPLY_ACTION_UNCHANGED {
			out = append(out, &apiv1.AppliedResource{Kind: spec.KindPipeline, Name: name, Action: action, Version: version})
			ch.pipelines = append(ch.pipelines, &planned{id: r.ID, name: name, prev: r.Version, version: version, action: action, v: v, provenance: provenance})
		}
	}
	return out, nil
}

// cause is the provenance of a change that a dependent pipeline's new version
// takes in.
func (ch *changes) cause(ctx context.Context, q *db.Queries, org uuid.UUID, r db.ListPipelinesUsingRow) (string, error) {
	if p, ok := ch.sources[r.SourceID]; ok {
		return p, nil
	}
	if p, ok := ch.destinations[r.DestinationID]; ok {
		return p, nil
	}
	var dbt map[string]string
	if err := json.Unmarshal(r.Dbt, &dbt); err != nil {
		return "", err
	}
	for _, name := range slices.Sorted(maps.Keys(dbt)) {
		proj, err := q.GetDbtProject(ctx, db.GetDbtProjectParams{OrgID: org, WorkspaceID: r.WorkspaceID, Name: name})
		if err != nil {
			return "", err
		}
		if p, ok := ch.dbtProjects[proj.ID]; ok {
			return p, nil
		}
	}
	var spark map[string]string
	if err := json.Unmarshal(r.SparkClusters, &spark); err != nil {
		return "", err
	}
	for _, name := range slices.Sorted(maps.Keys(spark)) {
		c, err := q.GetSparkCluster(ctx, db.GetSparkClusterParams{OrgID: org, WorkspaceID: r.WorkspaceID, Name: name})
		if err != nil {
			return "", err
		}
		if p, ok := ch.sparkClusters[c.ID]; ok {
			return p, nil
		}
	}
	return "", nil
}
