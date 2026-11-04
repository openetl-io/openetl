package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/plan"
	"github.com/unsuitable001/openetl/internal/store/db"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// bound is a source's or destination's config bound to its connector, with
// a placeholder for each secret it names.
type bound struct {
	secrets     []string
	source      source.Source
	destination destination.Destination
}

// bind binds a config to a connector of a kind, at version v; nil for a
// built-in, or one in the connectors directory.
func (s *Server) bind(kind, name string, v *connectorhost.Version, config *structpb.Struct) (*bound, error) {
	env := s.Connectors
	if v != nil {
		env = env.With(v, kind)
	}
	props, known, err := connectorhost.SecretProperties(kind, name, env)
	if err != nil {
		return nil, invalid("%v", err)
	}
	secrets, masked, err := bindSecrets(config, props, known)
	if err != nil {
		return nil, err
	}
	cfg, err := protojson.Marshal(masked)
	if err != nil {
		return nil, err
	}
	b := &bound{secrets: secrets}
	if kind == "source" {
		b.source, err = connectorhost.NewSource(name, cfg, env)
	} else {
		b.destination, err = connectorhost.NewDestination(name, cfg, env)
	}
	if err != nil {
		return nil, invalid("%v", err)
	}
	return b, nil
}

// capabilities are what the destination's connector declares; false if the
// control plane can't tell, as for a connector only runners have.
func (s *Server) capabilities(ctx context.Context, q *db.Queries, dst db.Destination) (destination.Capabilities, bool, error) {
	var c apiv1.ConnectorConfig
	if err := protojson.Unmarshal(dst.Spec, &c); err != nil {
		return destination.Capabilities{}, false, err
	}
	name, _ := connectorhost.SplitRef(c.GetConnector())
	var v *connectorhost.Version
	if dst.ConnectorDigest != "" {
		var err error
		if v, err = s.resolveConnector(ctx, q, "destination", name, dst.ConnectorDigest); err != nil {
			return destination.Capabilities{}, false, err
		}
	}
	// A config that no longer binds is the runner's to report.
	b, err := s.bind("destination", name, v, c.GetConfig())
	if err != nil {
		return destination.Capabilities{}, false, nil
	}
	caps := b.destination.Capabilities()
	return caps, caps.Class != 0, nil
}

// planned is a pipeline an Apply gave a version.
type planned struct {
	id   uuid.UUID
	name string
	// The version that was current; empty for a new pipeline.
	prev    string
	version string
	action  apiv1.ApplyAction
	v       *pinned
	// Where the version came from.
	provenance string
}

func (v *pinned) pins() plan.Version {
	return plan.Version{
		Spec: v.spec, Source: v.src.Version, Destination: v.dst.Version,
		SourceConnector: v.src.ConnectorDigest, DestinationConnector: v.dst.ConnectorDigest, Dbt: v.dbt, SparkClusters: v.spark, FlinkClusters: v.flink,
	}
}

func pinsOf(r db.GetSyncSpecRow) (plan.Version, error) {
	v := plan.Version{
		Spec: &apiv1.PipelineSpec{}, Source: r.SourceVersion, Destination: r.DestinationVersion,
		SourceConnector: r.SourceConnector, DestinationConnector: r.DestinationConnector,
	}
	if err := protojson.Unmarshal(r.PipelineSpec, v.Spec); err != nil {
		return v, err
	}
	if err := json.Unmarshal(r.Dbt, &v.Dbt); err != nil {
		return v, err
	}
	if err := json.Unmarshal(r.SparkClusters, &v.SparkClusters); err != nil {
		return v, err
	}
	return v, json.Unmarshal(r.FlinkClusters, &v.FlinkClusters)
}

// plan is how the pipeline's new version would sync, from its source's
// latest catalog.
func (s *Server) plan(ctx context.Context, q *db.Queries, p *planned) (*apiv1.PipelinePlan, error) {
	out := &apiv1.PipelinePlan{Pipeline: p.name, Version: p.version}
	if p.prev != "" && p.prev != p.version {
		row, err := q.GetSyncSpec(ctx, db.GetSyncSpecParams{OrgID: s.Tenant.Org, PipelineID: p.id, Version: p.prev})
		if err != nil {
			return nil, err
		}
		before, err := pinsOf(row)
		if err != nil {
			return nil, err
		}
		if out.Changes, err = plan.Diff(before, p.v.pins()); err != nil {
			return nil, err
		}
	}
	unrunnable, notes, err := s.checkRunners(ctx, q, p.v)
	if err != nil {
		return nil, err
	}
	out.Problems, out.Notes = unrunnable, notes
	for _, c := range []struct {
		kind, ref, digest string
		spec              []byte
	}{{"source", p.v.spec.GetSource().GetRef(), p.v.src.ConnectorDigest, p.v.src.Spec}, {"destination", p.v.spec.GetDestination().GetRef(), p.v.dst.ConnectorDigest, p.v.dst.Spec}} {
		var cfg apiv1.ConnectorConfig
		if c.digest == "" || protojson.Unmarshal(c.spec, &cfg) != nil {
			continue
		}
		if name, ref := connectorhost.SplitRef(cfg.GetConnector()); ref == "" {
			out.Warnings = append(out.Warnings, &apiv1.PlanWarning{Message: fmt.Sprintf(
				"%s %q names connector %s without a version, so applying it again may pin a newer one; %s@MAJOR keeps to one major version", c.kind, c.ref, name, name)})
		}
	}
	src, dst := p.v.spec.GetSource().GetRef(), p.v.spec.GetDestination().GetRef()
	caps, known, err := s.capabilities(ctx, q, p.v.dst)
	if err != nil {
		return nil, err
	}
	if !known {
		out.Notes = append(out.Notes, fmt.Sprintf("the control plane doesn't know destination %q's capabilities, so only the runner checks delivery, before each sync", dst))
		return out, nil
	}
	var cat *apiv1.Catalog
	row, err := q.GetLatestSourceCatalog(ctx, db.GetLatestSourceCatalogParams{OrgID: s.Tenant.Org, SourceID: p.v.src.ID})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		out.Notes = append(out.Notes, fmt.Sprintf("source %q hasn't been discovered, so the plan checks only the delivery settings (openetl discover %s)", src, src))
	case err != nil:
		return nil, err
	default:
		cat = &apiv1.Catalog{}
		if err := protojson.Unmarshal(row.Catalog, cat); err != nil {
			return nil, err
		}
		if row.SourceVersion != p.v.src.Version {
			out.Notes = append(out.Notes, fmt.Sprintf("source %q was last discovered with another config, and the plan uses that catalog (openetl discover %s)", src, src))
		}
	}
	streams, problems, warnings := plan.Pipeline(p.v.spec, cat, caps)
	out.Streams, out.Warnings = streams, append(out.Warnings, warnings...)
	for _, e := range problems {
		out.Problems = append(out.Problems, e.Error())
	}
	return out, nil
}

func (s *Server) GetPipelinePlan(ctx context.Context, req *connect.Request[apiv1.GetPipelinePlanRequest]) (*connect.Response[apiv1.GetPipelinePlanResponse], error) {
	_, p, err := s.getPipeline(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	row, err := s.Store.GetSyncSpec(ctx, db.GetSyncSpecParams{OrgID: s.Tenant.Org, PipelineID: p.ID, Version: p.Version})
	if err != nil {
		return nil, err
	}
	v, err := pinsOf(row)
	if err != nil {
		return nil, err
	}
	pl := &planned{id: p.ID, name: req.Msg.GetName(), version: p.Version, v: &pinned{
		spec: v.Spec,
		src:  db.Source{ID: p.SourceID, Version: row.SourceVersion},
		dst:  db.Destination{Spec: row.DestinationSpec, ConnectorDigest: row.DestinationConnector},
	}}
	out, err := s.plan(ctx, s.Store.Queries, pl)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.GetPipelinePlanResponse{Plan: out}), nil
}

// refuse fails if the plan of a pipeline the Apply creates or changes has
// problems.
func refuse(ch *changes, plans []*apiv1.PipelinePlan) error {
	var lines []string
	for i, p := range ch.pipelines {
		if p.action == apiv1.ApplyAction_APPLY_ACTION_UNCHANGED {
			continue
		}
		for _, problem := range plans[i].GetProblems() {
			lines = append(lines, p.name+": "+problem)
		}
	}
	if len(lines) == 0 {
		return nil
	}
	return connect.NewError(connect.CodeFailedPrecondition, errors.New("the plan fails, so nothing was applied:\n"+strings.Join(lines, "\n")))
}
