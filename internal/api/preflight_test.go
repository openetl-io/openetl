package api

import (
	"testing"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

func TestPlanItems(t *testing.T) {
	part := &apiv1.PreflightPart{Ok: true}
	planItems(part, &apiv1.PipelinePlan{Problems: []string{"orders: no cursor"}, Warnings: []*apiv1.PlanWarning{{Stream: "users", Message: "collapses rows"}}})
	if part.GetOk() || part.GetCode() != "CONFIG_INVALID" || len(part.GetItems()) != 2 {
		t.Fatalf("part %v", part)
	}
	if w := part.GetItems()[1]; w.GetStatus() != "warning" || w.GetTitle() != "Stream users" {
		t.Errorf("warning %v", w)
	}
	clean := &apiv1.PreflightPart{Ok: true}
	planItems(clean, &apiv1.PipelinePlan{Streams: []*apiv1.StreamPlan{{Name: "a"}}})
	if !clean.GetOk() || len(clean.GetItems()) != 1 || clean.GetItems()[0].GetStatus() != "passed" {
		t.Errorf("a clean plan %v", clean)
	}
}
