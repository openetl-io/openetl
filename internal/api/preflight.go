package api

import (
	"context"
	"fmt"

	"connectrpc.com/connect"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/store/db"
)

// PreflightPipeline runs the pipeline's preflight on a runner of its data
// plane: both connections' checks, then both connectors' preflights for
// what a sync of its current version would read and write. Then, with the
// source's catalog refreshed, it plans the pipeline, and the plan's
// problems and warnings are the last part's items.
func (s *Server) PreflightPipeline(ctx context.Context, req *connect.Request[apiv1.PreflightPipelineRequest]) (*connect.Response[apiv1.PreflightPipelineResponse], error) {
	ws, p, err := s.getPipeline(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	srcRow, err := s.Store.GetSourceByID(ctx, db.GetSourceByIDParams{OrgID: s.Tenant.Org, ID: p.SourceID})
	if err != nil {
		return nil, err
	}
	dstRow, err := s.Store.GetDestinationByID(ctx, db.GetDestinationByIDParams{OrgID: s.Tenant.Org, ID: p.DestinationID})
	if err != nil {
		return nil, err
	}
	src := fromSource(srcRow)
	resources := map[string]string{
		"source":      fmt.Sprintf("workspaces/%s/sources/%s", ws, src.name),
		"destination": fmt.Sprintf("workspaces/%s/destinations/%s", ws, dstRow.Name),
	}
	res, err := s.runOp(ctx, p.DataPlaneID, src, contract.ConnectorOpInput{Op: contract.OpPreflight, SourceID: src.id.String(), PipelineID: p.ID.String(), Version: p.Version})
	if err != nil {
		return nil, err
	}
	out := &apiv1.PreflightResult{Ok: true, Version: p.Version}
	srcChecked := false
	var plan *apiv1.PreflightPart
	for _, g := range res.Preflight {
		part := &apiv1.PreflightPart{
			Role: g.Role, Stage: g.Stage, Resource: resources[g.Role], Version: g.Version,
			Ok: g.Code == "" && g.Skipped == "", Code: g.Code, Message: g.Message, Skipped: g.Skipped, Items: apiItems(g.Items),
		}
		if g.Code != "" {
			out.Ok = false
		}
		if g.Role == "source" && g.Stage == "check" && part.Ok {
			srcChecked = true
		}
		if g.Role == "sync" {
			plan = part
			continue
		}
		out.Parts = append(out.Parts, part)
	}
	if plan == nil {
		plan = &apiv1.PreflightPart{Role: "sync", Stage: "plan", Ok: true}
	}
	// The plan reads the source's latest catalog, which needs the source.
	if !srcChecked {
		if plan.Ok {
			plan.Ok, plan.Skipped = false, "the source's check failed, so its catalog can't be refreshed"
		}
	} else if _, err := s.runOp(ctx, p.DataPlaneID, src, contract.ConnectorOpInput{Op: contract.OpDiscover, SourceID: src.id.String()}); err != nil {
		return nil, err
	} else if pl, err := s.GetPipelinePlan(ctx, connect.NewRequest(&apiv1.GetPipelinePlanRequest{Name: req.Msg.GetName()})); err != nil {
		return nil, err
	} else {
		planItems(plan, pl.Msg.GetPlan())
	}
	if !plan.Ok && plan.Skipped == "" {
		out.Ok = false
	}
	out.Parts = append(out.Parts, plan)
	return connect.NewResponse(&apiv1.PreflightPipelineResponse{Result: out}), nil
}

// planItems adds the plan's problems, as failed items, and its warnings to
// part.
func planItems(part *apiv1.PreflightPart, pl *apiv1.PipelinePlan) {
	for i, problem := range pl.GetProblems() {
		part.Items = append(part.Items, &apiv1.CheckItem{Name: fmt.Sprintf("plan.problem.%d", i+1), Title: "The plan", Status: "failed", Code: "CONFIG_INVALID", Message: problem})
		if part.Ok {
			part.Ok, part.Code, part.Message = false, "CONFIG_INVALID", problem
		}
	}
	for i, w := range pl.GetWarnings() {
		title := "The plan"
		if w.GetStream() != "" {
			title = "Stream " + w.GetStream()
		}
		part.Items = append(part.Items, &apiv1.CheckItem{Name: fmt.Sprintf("plan.warning.%d", i+1), Title: title, Status: "warning", Message: w.GetMessage()})
	}
	if len(part.Items) == 0 {
		part.Items = append(part.Items, &apiv1.CheckItem{Name: "plan", Title: "The plan", Status: "passed", Message: fmt.Sprintf("%d streams, no problems", len(pl.GetStreams()))})
	}
}

func apiItems(items []contract.CheckItem) []*apiv1.CheckItem {
	var out []*apiv1.CheckItem
	for _, it := range items {
		out = append(out, &apiv1.CheckItem{Name: it.Name, Title: it.Title, Status: it.Status, Message: it.Message, Code: it.Code, Fix: it.Fix, Field: it.Field})
	}
	return out
}
