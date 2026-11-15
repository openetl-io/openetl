package plan

import (
	"fmt"
	"slices"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/sqlplan"
)

// Operators are a SQL transform's stateful operators against applied, what
// planning gave the pipeline's current version: each one a statement keeps,
// then each one applied keeps that no statement does now. applied is empty
// for a new pipeline, and nil where the current version's statements weren't
// planned, which leaves what each operator is to it unknown. read are the
// streams the pipeline has read: a new or changed operator over one needs a
// backfill to build its state from the stream's start, unless each sync
// reads the stream whole. The warnings say what applying does to state, and
// the notes which state it drops.
func Operators(spec *apiv1.PipelineSpec, cat *apiv1.Catalog, a *sqlplan.Answer, applied *apiv1.SqlPlan, read map[string]bool) ([]*apiv1.OperatorPlan, []*apiv1.PlanWarning, []string) {
	var out []*apiv1.OperatorPlan
	var warnings []*apiv1.PlanWarning
	var notes []string
	before := map[string]string{}
	for _, o := range applied.GetOutputs() {
		if o.GetOperator() != "" {
			before[o.GetName()] = o.GetOperator()
		}
	}
	taken := map[string]bool{}
	for _, st := range a.Statements {
		if st.Operator == "" {
			continue
		}
		op := &apiv1.OperatorPlan{Id: st.Operator, Output: st.Output, Line: st.At.Line, Column: st.At.Column, Class: st.Class}
		if len(st.Inputs) == 1 {
			op.Input = st.Inputs[0]
		}
		var ps *apiv1.PipelineStream
		full := false
		for _, s := range selected(spec, cat) {
			if s.cs.GetName() == op.Input {
				ps = s.ps
				full = ReadMode(spec, s.cs.GetSyncModes(), s.ps) == apiv1.SyncMode_SYNC_MODE_FULL_REFRESH
			}
		}
		keeps(op, st.State, ps)
		switch {
		case applied == nil:
		case slices.Contains(applied.GetOperators(), st.Operator):
			op.Change = apiv1.OperatorChange_OPERATOR_CHANGE_KEPT
			taken[st.Operator] = true
		case before[st.Output] != "":
			op.Change, op.Replaces = apiv1.OperatorChange_OPERATOR_CHANGE_CHANGED, before[st.Output]
			taken[op.Replaces] = true
		default:
			op.Change = apiv1.OperatorChange_OPERATOR_CHANGE_NEW
			op.Earlier = slices.Contains(applied.GetDropped(), st.Operator)
		}
		fresh := op.Change == apiv1.OperatorChange_OPERATOR_CHANGE_NEW || op.Change == apiv1.OperatorChange_OPERATOR_CHANGE_CHANGED
		if fresh && op.Input != "" && read[op.Input] && !full {
			op.Backfill = []string{op.Input}
		}
		if w := changeWarning(op); w != "" {
			warnings = append(warnings, &apiv1.PlanWarning{Stream: op.Output, Message: w})
		}
		out = append(out, op)
	}
	for _, id := range applied.GetOperators() {
		if taken[id] {
			continue
		}
		op := &apiv1.OperatorPlan{Id: id, Change: apiv1.OperatorChange_OPERATOR_CHANGE_DROPPED}
		for name, was := range before {
			if was == id {
				op.Output = name
			}
		}
		notes = append(notes, droppedNote(op))
		out = append(out, op)
	}
	return out, warnings, notes
}

// keeps fills in what op keeps, from planning's state and the spec's
// settings for the stream it reads, ps.
func keeps(op *apiv1.OperatorPlan, s *sqlplan.State, ps *apiv1.PipelineStream) {
	if s == nil {
		return
	}
	op.EventTime, op.Window, op.Keys, op.Aggregates = s.EventTime, s.Window, s.Keys, s.Aggregates
	size := time.Duration(s.SizeMicros) * time.Microsecond
	op.Size = durationpb.New(size)
	if s.SlideMicros > 0 {
		op.Slide = durationpb.New(time.Duration(s.SlideMicros) * time.Microsecond)
	}
	lateness := ps.GetWatermark().GetLateness().AsDuration()
	op.Lateness = durationpb.New(lateness)
	op.Retention = durationpb.New(size + lateness)
}

// changeWarning says what a new or changed operator's state starts as, and
// what builds it; empty for one that keeps its state, or a new one with
// nothing to build.
func changeWarning(op *apiv1.OperatorPlan) string {
	at := fmt.Sprintf("transform.sql:%d:%d: ", op.GetLine(), op.GetColumn())
	backfill := ""
	if len(op.GetBackfill()) > 0 {
		backfill = fmt.Sprintf(": from the next sync, its windows hold only rows read from then on, and a window open now loses the rows read before; "+
			"openetl reset --stream %s, after applying, reads %s again from the start to build it (a backfill)", op.GetInput(), op.GetInput())
	}
	switch op.GetChange() {
	case apiv1.OperatorChange_OPERATOR_CHANGE_CHANGED:
		return at + fmt.Sprintf("the group window writing %s computes or keeps something else now, so it's a new operator, %s, in place of %s: "+
			"its state starts empty, and %s's is dropped%s", op.GetOutput(), op.GetId(), op.GetReplaces(), op.GetReplaces(), backfill)
	case apiv1.OperatorChange_OPERATOR_CHANGE_NEW:
		if op.GetEarlier() {
			return at + fmt.Sprintf("the group window writing %s is operator %s, which an earlier version had and the current version dropped: "+
				"if no sync has run since, the next sync restores its state from then; otherwise its state starts empty%s", op.GetOutput(), op.GetId(), backfill)
		}
		if backfill == "" {
			return ""
		}
		return at + fmt.Sprintf("the group window writing %s is new, and its state starts empty%s", op.GetOutput(), backfill)
	}
	return ""
}

func droppedNote(op *apiv1.OperatorPlan) string {
	if op.GetOutput() == "" {
		return fmt.Sprintf("transform.sql: no statement keeps operator %s now, so the next sync drops its state", op.GetId())
	}
	return fmt.Sprintf("transform.sql: no statement keeps operator %s, which wrote %s, now, so the next sync drops its state", op.GetId(), op.GetOutput())
}
