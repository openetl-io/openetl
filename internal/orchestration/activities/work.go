package activities

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/connectorhost/external"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/dispatch"
	"github.com/openetl-io/openetl/internal/orchestration/sizing"
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/spec"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// EnqueueWork sends a workflow's work to its data plane, and places it if
// it's an attempt with room waiting. A pipeline's attempt is sized here, not in
// its workflow, so a size adds nothing to the workflow's history; so are its
// workers.
func (a *Activities) EnqueueWork(ctx context.Context, in contract.WorkInput) error {
	needs, err := a.prepare(ctx, &in)
	if err != nil {
		return err
	}
	return a.Dispatch.Enqueue(ctx, in, needs)
}

// prepare sizes a pipeline's attempt and works out its workers, and returns
// what a runner must have to run its connector versions.
func (a *Activities) prepare(ctx context.Context, in *contract.WorkInput) ([]dispatch.Need, error) {
	if in.PipelineID == "" {
		return nil, nil
	}
	org, pipeline, err := ids(in.OrgID, in.PipelineID)
	if err != nil {
		return nil, err
	}
	v, err := a.Store.GetSyncSpec(ctx, db.GetSyncSpecParams{OrgID: org, PipelineID: pipeline, Version: in.Version})
	if err != nil {
		return nil, err
	}
	var needs []dispatch.Need
	var hints []string
	var dst *spec.Role
	for _, r := range []struct{ role, digest string }{{"source", v.SourceConnector}, {"destination", v.DestinationConnector}} {
		if r.digest == "" {
			continue
		}
		role, err := a.role(ctx, org, r.digest, r.role)
		if err != nil {
			return nil, err
		}
		if role != nil {
			needs = append(needs, dispatch.NeedOf(role))
			hints = append(hints, role.Resources.Hint)
		}
		if r.role == "destination" {
			dst = role
		}
	}
	if in.Size != (resources.Size{}) {
		return needs, nil
	}
	var pipelineSpec apiv1.PipelineSpec
	if err := protojson.Unmarshal(v.PipelineSpec, &pipelineSpec); err != nil {
		return nil, err
	}
	sp := sizing.SpecOf(pipelineSpec.GetResources())
	if in.Workers, in.OnePod, err = a.workers(ctx, org, v, &pipelineSpec, dst, sp.Pool); err != nil {
		return nil, err
	}
	history, err := sizing.History(ctx, a.Store.Queries, org, pipeline)
	if err != nil {
		return nil, err
	}
	var runners []resources.Size
	if in.Workers > 0 {
		launchers, _, err := a.Dispatch.Launchers(ctx, org, v.DataPlaneID, sp.Pool)
		if err != nil {
			return nil, err
		}
		for _, l := range launchers {
			runners = append(runners, l.Largest())
		}
		in.Coordinator = sizing.Coordinator
	} else if runners, err = a.Dispatch.Capacities(ctx, org, v.DataPlaneID, sp.Pool); err != nil {
		return nil, err
	}
	sin := sizing.Input{Spec: sp, Hint: sizing.LargestHint(hints...), History: history, Runners: runners}
	continuous := in.Kind == contract.RunStreamJob
	if err := sizing.Rescaled(ctx, a.Store.Queries, org, pipeline, in.Version, continuous, pipelineSpec.GetResources().GetFinishWithin().AsDuration(), &sin); err != nil {
		return nil, err
	}
	c := sizing.Choose(sin)
	in.Size, in.SizeReason, in.Pool = c.Size, string(c.Reason), sp.Pool
	return needs, nil
}

// workers is how many workers an attempt runs on, and why it runs on one pod
// although its spec asks for more. What the control plane can't tell, such
// as a destination's capabilities that only its running connector gives, the
// attempt's coordinator checks, and it places the attempt again on one pod if
// it must.
func (a *Activities) workers(ctx context.Context, org uuid.UUID, v db.GetSyncSpecRow, p *apiv1.PipelineSpec, dst *spec.Role, pool string) (int, string, error) {
	asked := int(p.GetResources().GetWorkers())
	if asked <= 1 {
		return 0, "", nil
	}
	caps, engine, known, err := destinationOf(v, dst)
	if err != nil {
		return 0, "", err
	}
	workers, why := asked, ""
	if known {
		var cat *apiv1.Catalog
		row, err := a.Store.GetLatestSourceCatalog(ctx, db.GetLatestSourceCatalogParams{OrgID: org, SourceID: v.SourceID})
		switch {
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil:
			return 0, "", err
		default:
			cat = &apiv1.Catalog{}
			if err := protojson.Unmarshal(row.Catalog, cat); err != nil {
				return 0, "", err
			}
		}
		workers, why, _ = plan.Workers(p, cat, caps, engine)
	}
	if why != "" {
		return 0, why, nil
	}
	launchers, reporting, err := a.Dispatch.Launchers(ctx, org, v.DataPlaneID, pool)
	if err != nil {
		return 0, "", err
	}
	if len(launchers) == 0 && reporting {
		return 0, fmt.Sprintf("no pod launcher of pool %q is reporting, and an attempt runs on workers only in pods", pool), nil
	}
	return workers, "", nil
}

// destinationOf is what the control plane knows of a version's destination
// without running it: its capabilities, from its descriptor or as a
// built-in, and whether the engine writes it; known is false if only its
// running connector can tell.
func destinationOf(v db.GetSyncSpecRow, role *spec.Role) (caps destination.Capabilities, engine, known bool, err error) {
	if role != nil && role.Spec != nil {
		return external.Capabilities(role.Spec), false, true, nil
	}
	var c apiv1.ConnectorConfig
	if err := protojson.Unmarshal(v.DestinationSpec, &c); err != nil {
		return caps, false, false, err
	}
	name, _ := connectorhost.SplitRef(c.GetConnector())
	if v.DestinationConnector != "" || !connectorhost.Builtin("destination", name) {
		return caps, false, false, nil
	}
	cfg, err := protojson.Marshal(c.GetConfig())
	if err != nil {
		return caps, false, false, err
	}
	// A config that doesn't bind is the runner's to report.
	d, bindErr := connectorhost.NewDestination(name, cfg, connectorhost.Env{})
	if bindErr != nil {
		return caps, false, false, nil
	}
	return d.Capabilities(), connectorhost.EngineWritten(d), true, nil
}

func (a *Activities) role(ctx context.Context, org uuid.UUID, digest, role string) (*spec.Role, error) {
	artifact, err := a.Store.GetArtifact(ctx, db.GetArtifactParams{OrgID: org, Digest: digest})
	if err != nil {
		return nil, err
	}
	ver, err := connectorhost.ParseVersion(artifact.Content)
	if err != nil {
		return nil, err
	}
	r := ver.Roles[role]
	if r == nil {
		return nil, nil
	}
	return r.Role, nil
}

// GetWork reports whether work finished, and how.
func (a *Activities) GetWork(ctx context.Context, ref contract.WorkRef) (contract.WorkStatus, error) {
	return a.Dispatch.Status(ctx, ref)
}

// CancelWork stops work.
func (a *Activities) CancelWork(ctx context.Context, ref contract.WorkRef) error {
	return a.Dispatch.Cancel(ctx, ref)
}
