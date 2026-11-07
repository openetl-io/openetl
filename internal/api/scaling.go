package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/events"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/sizing"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/store/db"
)

// RescalePipeline sets the size of the pipeline's jobs until its next
// version, or gives the spec back its say. What's running stops at a
// checkpoint and starts again at the new size, and what hasn't started is
// sized again (ADR-0027 §7).
func (s *Server) RescalePipeline(ctx context.Context, req *connect.Request[apiv1.RescalePipelineRequest]) (*connect.Response[apiv1.RescalePipelineResponse], error) {
	ws, p, err := s.getPipeline(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	size, err := rescaleSize(req.Msg.GetSize())
	if err != nil {
		return nil, err
	}
	if err := s.checkRescale(ctx, p, size); err != nil {
		return nil, err
	}
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		if _, err := q.LockJobScale(ctx, db.LockJobScaleParams{OrgID: s.Tenant.Org, PipelineID: p.ID}); err != nil {
			return err
		}
		version := p.Version
		if size == (resources.Size{}) {
			version = ""
		}
		if err := q.SetScaleSize(ctx, db.SetScaleSizeParams{OrgID: s.Tenant.Org, PipelineID: p.ID, SizeCpu: size.CPU, SizeMemory: size.Memory, SizeVersion: version}); err != nil {
			return err
		}
		if err := rescaleAttempts(ctx, q, s.Tenant.Org, p.ID, nil); err != nil {
			return err
		}
		data := events.Rescale{Pipeline: resourceName(ws, "pipelines", p.Name), Version: p.Version, By: "api"}
		if size != (resources.Size{}) {
			data.Size = size.Name()
		}
		_, err := events.Record(ctx, q, s.Tenant.Org, events.Event{Workspace: p.WorkspaceID, Type: events.PipelineRescaled, Pipeline: p.Name, Data: data})
		return err
	})
	if err != nil {
		return nil, err
	}
	out, err := s.pipeline(ctx, ws, p)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.RescalePipelineResponse{Pipeline: out}), nil
}

// rescaleSize is the size a rescale asks for; zero for none.
func rescaleSize(s *apiv1.JobSize) (resources.Size, error) {
	switch {
	case s == nil:
		return resources.Size{}, nil
	case s.GetPreset() != "":
		p, ok := resources.Preset(s.GetPreset())
		if !ok || s.GetCpuMillis() != 0 || s.GetMemoryBytes() != 0 {
			return resources.Size{}, invalid("size: %q isn't xs, s, m, l or xl, or it has amounts too", s.GetPreset())
		}
		return p, nil
	case s.GetCpuMillis() <= 0 || s.GetMemoryBytes() <= 0:
		return resources.Size{}, invalid("size: give a preset, or a positive cpu_millis and memory_bytes")
	}
	return sizing.Of(s), nil
}

// checkRescale refuses a size no runner of the pipeline's pool can take, if
// any reported lately: it would wait for one forever.
func (s *Server) checkRescale(ctx context.Context, p db.Pipeline, size resources.Size) error {
	if size == (resources.Size{}) {
		return nil
	}
	v, err := s.Store.GetPipelineVersion(ctx, db.GetPipelineVersionParams{OrgID: s.Tenant.Org, PipelineID: p.ID, Version: p.Version})
	if err != nil {
		return err
	}
	var spec apiv1.PipelineSpec
	if err := protojson.Unmarshal(v.Spec, &spec); err != nil {
		return err
	}
	pool := sizing.SpecOf(spec.GetResources()).Pool
	runners, err := s.Dispatch.Capacities(ctx, s.Tenant.Org, v.DataPlaneID, pool)
	if err != nil || len(runners) == 0 || sizing.FitsAny(size, runners) {
		return err
	}
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s is larger than any runner of data plane %q and pool %q can take: %s", size.Name(), v.DataPlaneID, pool, largestCapacities(runners)))
}

// rescaleAttempts has the pipeline's attempts take a new size: those running
// stop at a checkpoint and end as drained, and those no runner has started
// end so at once, so their workflows send them again, sized again. id, if
// set, is the one running attempt to rescale.
func rescaleAttempts(ctx context.Context, q *db.Queries, org, pipeline uuid.UUID, id *string) error {
	if id == nil {
		outcome, err := json.Marshal(contract.WorkOutcome{Code: errs.Draining.String(), Message: "rescaled before it started"})
		if err != nil {
			return err
		}
		if _, err := q.RescaleWaiting(ctx, db.RescaleWaitingParams{OrgID: org, PipelineID: pipeline, Outcome: outcome}); err != nil {
			return err
		}
	}
	_, err := q.RescaleRunning(ctx, db.RescaleRunningParams{OrgID: org, PipelineID: pipeline, ID: id})
	return err
}

// scaling is what rescaled the pipeline's jobs besides its spec; nil if
// nothing has.
func (s *Server) scaling(ctx context.Context, id uuid.UUID, version string) (*apiv1.JobScaling, error) {
	j, err := s.Store.GetJobScale(ctx, db.GetJobScaleParams{OrgID: s.Tenant.Org, PipelineID: id})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := &apiv1.JobScaling{Note: j.Note}
	if j.Lanes > 0 {
		out.Lanes = uint32(j.Lanes)
	}
	if j.SizeVersion == version && j.SizeCpu > 0 {
		out.Size = sizing.Out(resources.Size{CPU: j.SizeCpu, Memory: j.SizeMemory})
	}
	if j.RescaleTime != nil {
		out.RescaleTime = timestamppb.New(*j.RescaleTime)
	}
	if out.Size == nil && out.Lanes == 0 && out.Note == "" {
		return nil, nil
	}
	return out, nil
}
