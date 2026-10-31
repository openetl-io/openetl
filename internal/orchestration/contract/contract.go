// Package contract names the workflows, activities and task queues, and
// defines their payloads, so workflows and data-plane activities can share
// them without importing each other.
package contract

const (
	OrchestratorQueue = "cp.orchestrator"

	SyncWorkflow = "SyncWorkflow"

	StartSync      = "StartSync"
	FinishSync     = "FinishSync"
	RunSyncAttempt = "RunSyncAttempt"
)

func BatchQueue(dataPlane string) string { return "dp." + dataPlane + ".batch" }

func SyncWorkflowID(pipelineID string) string { return "sync/" + pipelineID }

func ScheduleID(pipelineID string) string { return "schedule/" + pipelineID }

type SyncInput struct {
	OrgID      string
	PipelineID string
}

type StartSyncResult struct {
	// Empty if the pipeline is paused, and no sync was recorded.
	SyncID    string
	Version   string
	DataPlane string
	Paused    bool
}

type AttemptInput struct {
	OrgID      string
	PipelineID string
	Version    string
}

type AttemptResult struct {
	Generation uint64
	// applied, redone or lost.
	Recovery string
	// The epoch the attempt resumed from; 0 from the beginning.
	Restored uint64
	// Epochs this attempt committed.
	Committed int
	Last      uint64
}

type FinishSyncInput struct {
	OrgID  string
	SyncID string
	// Empty on success.
	Error  string
	Result *AttemptResult
}

type SyncResult struct {
	SyncID  string
	Attempt AttemptResult
}
