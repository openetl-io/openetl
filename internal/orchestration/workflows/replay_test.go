package workflows_test

import (
	"log/slog"
	"path/filepath"
	"testing"

	tlog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"

	"github.com/openetl-io/openetl/internal/orchestration/temporalbackend"
	"github.com/openetl-io/openetl/internal/orchestration/workflows"
)

// Recorded histories replay against the current code, so a change that would
// break running workflows on Temporal fails here; such a change needs
// Context.Version. Record new ones with OPENETL_RECORD_HISTORIES=dir make e2e.
func TestReplayHistories(t *testing.T) {
	files, err := filepath.Glob("testdata/histories/*.json")
	if err != nil || len(files) == 0 {
		t.Fatalf("no histories: %v", err)
	}
	r := worker.NewWorkflowReplayer()
	for _, wf := range (workflows.Workflows{}).All() {
		temporalbackend.RegisterWorkflow(r, wf)
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			if err := r.ReplayWorkflowHistoryFromJSONFile(tlog.NewStructuredLogger(slog.New(slog.DiscardHandler)), f); err != nil {
				t.Fatal(err)
			}
		})
	}
}
