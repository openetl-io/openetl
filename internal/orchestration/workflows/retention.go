package workflows

import (
	"time"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

// Retention deletes what the control plane no longer needs: the checkpoint
// mirror's days older than the checkpoint history, and the audit log's
// months older than the audit history, whose next month it creates ahead.
// Sync attempts prune the checkpoint log themselves.
func (w Workflows) Retention(ctx orchestration.Context, in contract.RetentionInput) (*contract.RetentionResult, error) {
	if in.CheckpointHistory <= 0 {
		return nil, orchestration.NewFailure(errs.New(errs.ConfigInvalid, "the checkpoint history is %v; it must be positive", in.CheckpointHistory))
	}
	step := orchestration.StepOptions{Timeout: 5 * time.Minute, Attempts: 5}
	drop := contract.DropMirrorPartitionsInput{Before: ctx.Now().Add(-in.CheckpointHistory)}
	var res contract.RetentionResult
	if err := ctx.Step(contract.DropMirrorPartitions, step, drop, &res.MirrorPartitions); err != nil {
		return nil, err
	}
	if !ctx.Version("audit-retention") {
		return &res, nil
	}
	audit := contract.AuditPartitionsInput{Now: ctx.Now()}
	if in.AuditHistory > 0 {
		audit.Before = audit.Now.Add(-in.AuditHistory)
	}
	if err := ctx.Step(contract.AuditPartitions, step, audit, &res.AuditPartitions); err != nil {
		return nil, err
	}
	return &res, nil
}
