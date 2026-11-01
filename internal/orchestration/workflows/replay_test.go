package workflows_test

import (
	"log/slog"
	"path/filepath"
	"testing"

	tlog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/orchestration/workflows"
)

// Recorded histories replay against the current code, so a change that would
// break running workflows fails here; such a change needs workflow.GetVersion.
// Record new ones with OPENETL_RECORD_HISTORIES=dir make e2e.
func TestReplayHistories(t *testing.T) {
	files, err := filepath.Glob("testdata/histories/*.json")
	if err != nil || len(files) == 0 {
		t.Fatalf("no histories: %v", err)
	}
	r := worker.NewWorkflowReplayer()
	r.RegisterWorkflowWithOptions(workflows.Workflows{}.Sync, workflow.RegisterOptions{Name: contract.SyncWorkflow})
	r.RegisterWorkflowWithOptions(workflows.Workflows{}.ConnectorOp, workflow.RegisterOptions{Name: contract.ConnectorOpWorkflow})
	r.RegisterWorkflowWithOptions(workflows.Workflows{}.Retention, workflow.RegisterOptions{Name: contract.RetentionWorkflow})
	r.RegisterWorkflowWithOptions(workflows.Workflows{}.ReadLogs, workflow.RegisterOptions{Name: contract.ReadLogsWorkflow})
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			if err := r.ReplayWorkflowHistoryFromJSONFile(tlog.NewStructuredLogger(slog.New(slog.DiscardHandler)), f); err != nil {
				t.Fatal(err)
			}
		})
	}
}
