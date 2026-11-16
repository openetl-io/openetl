package api

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/sdk/go/check"
)

// PreflightPipeline runs the pipeline's preflight on a runner of its data
// plane: both connections' checks, settling what a sync of its current
// version would read and write, and both connectors' preflights for that.
// If that settled, it refreshes the source's catalog and plans that
// version, and the plan's problems and warnings are the plan part's items.
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
	out, err := preflightResult(p.Version, res, resources, func() (*apiv1.PipelinePlan, error) {
		discover := contract.ConnectorOpInput{Op: contract.OpDiscover, SourceID: src.id.String()}
		if _, err := s.runOpAs(ctx, p.DataPlaneID, src, discover, catalogNotRefreshed); err != nil {
			return nil, err
		}
		// The version preflighted, which an apply since may have replaced.
		return s.planAt(ctx, req.Msg.GetName(), p.ID, p.Version)
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.PreflightPipelineResponse{Result: out}), nil
}

// catalogNotRefreshed leads the error of a preflight whose discover, which
// refreshes the source's catalog for the plan, couldn't run.
const catalogNotRefreshed = "the preflight couldn't run: the source's catalog couldn't be refreshed"

// preflightResult is the API's result of a preflight of the pipeline's
// version that ran res, its parts in the order they ran. plan refreshes the
// source's catalog and plans the version; it runs only if the plan part
// passed, or, from a runner that sends none, if the source's check did. The
// result is ok only if every part is: a skipped part never is.
func preflightResult(version string, res contract.ConnectorOpResult, resources map[string]string, plan func() (*apiv1.PipelinePlan, error)) (*apiv1.PreflightResult, error) {
	out := &apiv1.PreflightResult{Version: version}
	srcChecked := false
	var planPart *apiv1.PreflightPart
	for _, g := range res.Preflight {
		part := &apiv1.PreflightPart{
			Role: g.Role, Stage: g.Stage, Resource: resources[g.Role], Version: g.Version,
			Code: g.Code, Message: g.Message, Skipped: g.Skipped, Items: apiItems(g.Items),
		}
		settle(part)
		if g.Role == "source" && g.Stage == "check" && part.Ok {
			srcChecked = true
		}
		if g.Role == "sync" {
			planPart = part
		}
		out.Parts = append(out.Parts, part)
	}
	if planPart == nil {
		planPart = &apiv1.PreflightPart{Role: "sync", Stage: "plan", Ok: true}
		if !srcChecked {
			planPart.Ok, planPart.Skipped = false, "the source's check didn't pass, so its catalog can't be refreshed"
		}
		out.Parts = append(out.Parts, planPart)
	}
	if planPart.Ok {
		pl, err := plan()
		if err != nil {
			return nil, err
		}
		planItems(planPart, pl)
		settle(planPart)
	}
	out.Ok = !slices.ContainsFunc(out.Parts, func(p *apiv1.PreflightPart) bool { return !p.GetOk() })
	return out, nil
}

// settle sets part's ok, and its code and message from its first failed
// item if it has none.
func settle(part *apiv1.PreflightPart) {
	var ok bool
	ok, part.Code, part.Message = verdict(part.GetCode(), part.GetMessage(), part.GetItems())
	part.Ok = ok && part.GetSkipped() == ""
}

// verdict is whether a result with code and message, empty if it passed,
// and items passed, and its code and message: when code is empty, the first
// failed item's code, INTERNAL if it has none, and its text as a connector's
// error gives it. An item fails closed: any status but passed, warning or
// skipped is a failure.
func verdict(code, message string, items []*apiv1.CheckItem) (bool, string, string) {
	for _, it := range items {
		if !failing(it.GetStatus()) {
			continue
		}
		if code == "" {
			code, message = cmp.Or(it.GetCode(), errs.Internal.String()), itemError(it)
		}
		return false, code, message
	}
	return code == "", code, message
}

func failing(status string) bool {
	switch status {
	case check.Passed.String(), check.Warning.String(), check.Skipped.String():
		return false
	}
	return true
}

// itemError is "title: message: detail", the placeholders filled.
func itemError(it *apiv1.CheckItem) string {
	m := (&check.Failure{Message: it.GetMessage(), Params: it.GetParams(), Detail: it.GetDetail()}).Error()
	if t := check.Render(it.GetTitle(), it.GetParams()); t != "" {
		return t + ": " + m
	}
	return m
}

// planItems adds the plan's problems, as failed items, and its warnings to
// part. Their messages are the plan's text, which has no placeholders a
// param fills.
func planItems(part *apiv1.PreflightPart, pl *apiv1.PipelinePlan) {
	kind := check.Configuration.String()
	for i, problem := range pl.GetProblems() {
		part.Items = append(part.Items, &apiv1.CheckItem{
			Name: fmt.Sprintf("plan.problem.%d", i+1), Title: "The plan", Status: check.Failed.String(), Message: problem,
			Code: errs.ConfigInvalid.String(), Kind: kind, Reason: check.ConfigInvalid.Name,
		})
	}
	for i, w := range pl.GetWarnings() {
		it := &apiv1.CheckItem{Name: fmt.Sprintf("plan.warning.%d", i+1), Title: "The plan", Status: check.Warning.String(), Message: w.GetMessage(), Kind: kind}
		if s := w.GetStream(); s != "" {
			it.Title, it.Params = "Stream {stream}", map[string]string{"stream": s}
			if slices.Contains(check.Placeholders(it.GetMessage()), "stream") {
				// The param would fill the plan's own text.
				it.Title, it.Params = "Stream "+s, nil
			}
		}
		part.Items = append(part.Items, it)
	}
	if len(part.Items) == 0 && part.GetCode() == "" {
		part.Items = append(part.Items, &apiv1.CheckItem{
			Name: "plan", Title: "The plan", Status: check.Passed.String(), Kind: kind,
			Message: fmt.Sprintf("%d streams, no problems", len(pl.GetStreams())),
		})
	}
}

// apiItems are a result's items as the API gives them, templates unfilled.
func apiItems(items []contract.CheckItem) []*apiv1.CheckItem {
	var out []*apiv1.CheckItem
	for _, it := range items {
		a := &apiv1.CheckItem{
			Name: it.Name, Title: it.Title, Status: it.Status, Message: it.Message, Code: it.Code, Fix: it.Fix, Field: it.Field,
			Kind: it.Kind, Description: it.Description, Reason: it.Reason, Params: it.Params, Detail: it.Detail,
		}
		if it.Duration > 0 {
			a.Duration = durationpb.New(it.Duration)
		}
		out = append(out, a)
	}
	return out
}
