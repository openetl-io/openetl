// Package contract names the workflows, activities and task queues, and
// defines their payloads, so workflows and data-plane activities can share
// them without importing each other.
package contract

import (
	"time"

	"go.temporal.io/sdk/temporal"
)

const (
	OrchestratorQueue = "cp.orchestrator"

	SyncWorkflow        = "SyncWorkflow"
	ConnectorOpWorkflow = "ConnectorOpWorkflow"
	RetentionWorkflow   = "RetentionWorkflow"

	StartSync            = "StartSync"
	FinishSync           = "FinishSync"
	RunSyncAttempt       = "RunSyncAttempt"
	RunDbt               = "RunDbt"
	CheckConnector       = "CheckConnector"
	DiscoverSource       = "DiscoverSource"
	DropMirrorPartitions = "DropMirrorPartitions"
)

// DbtFailed is the error type of a dbt command that ran and failed. It isn't
// retried; the next sync runs the step again.
const DbtFailed = "DBT_FAILED"

// One retention schedule runs for the whole deployment.
const (
	RetentionWorkflowID = "gc"
	RetentionScheduleID = "schedule/gc"
)

// DefaultCheckpointHistory is how long the control plane keeps the
// checkpoint mirror.
const DefaultCheckpointHistory = 30 * 24 * time.Hour

// Connector operations.
const (
	OpCheck    = "check"
	OpDiscover = "discover"
)

// Search attributes on workflows, for finding them in Temporal. openetld
// adds them to the namespace when it starts.
var (
	OrgIDAttr       = temporal.NewSearchAttributeKeyKeyword("OrgID")
	WorkspaceIDAttr = temporal.NewSearchAttributeKeyKeyword("WorkspaceID")
	PipelineIDAttr  = temporal.NewSearchAttributeKeyKeyword("PipelineID")
	DataPlaneIDAttr = temporal.NewSearchAttributeKeyKeyword("DataPlaneID")
	// The connectors the workflow runs, such as postgres.
	ConnectorDefAttr = temporal.NewSearchAttributeKeyKeywordList("ConnectorDef")

	SearchAttributes = []temporal.SearchAttributeKey{OrgIDAttr, WorkspaceIDAttr, PipelineIDAttr, DataPlaneIDAttr, ConnectorDefAttr}
)

// OpPickup is how long a connector operation waits for a runner of its data
// plane to take it.
const OpPickup = time.Minute

func BatchQueue(dataPlane string) string { return "dp." + dataPlane + ".batch" }

func SyncWorkflowID(pipelineID string) string { return "sync/" + pipelineID }

func ScheduleID(pipelineID string) string { return "schedule/" + pipelineID }

func OpWorkflowID(id string) string { return "op/" + id }

type SyncInput struct {
	OrgID      string
	PipelineID string
}

type StartSyncResult struct {
	// Empty if the pipeline is paused, and no sync was recorded.
	SyncID      string
	Version     string
	DataPlane   string
	Paused      bool
	WorkspaceID string
	// Of the source and the destination.
	Connectors []string
	// The pipeline version's post-sync steps, with their projects resolved.
	PostSync []PostSyncStep
}

// PostSyncStep is one of a pipeline's post-sync steps. Dbt is its only
// kind.
type PostSyncStep struct {
	Dbt *DbtStep
}

type DbtStep struct {
	Project string
	// The project's bundle when the sync started: sha256:{hex}.
	Digest string
	// build, run or test.
	Command         string
	Select, Exclude string
	// Empty: the schema the pipeline writes to.
	Schema string
}

type DbtInput struct {
	OrgID      string
	PipelineID string
	// The pipeline version the sync ran, for its destination.
	Version string
	Step    DbtStep
}

type DbtResult struct {
	Project string
	Digest  string
	Command string
	// Nodes by status, as dbt reports them: success, error, skipped, pass,
	// fail and so on.
	Nodes   map[string]int
	Seconds float64
}

// PostSyncResult is a post-sync step's result.
type PostSyncResult struct {
	Dbt *DbtResult `json:",omitempty"`
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
	Error string
	// The attempt that committed the sync's data; nil if none did.
	Result *AttemptResult
	// The post-sync steps that succeeded.
	PostSync []PostSyncResult
}

type SyncResult struct {
	SyncID  string
	Attempt AttemptResult
}

type ConnectorOpInput struct {
	OrgID       string
	WorkspaceID string
	DataPlane   string
	// The connector's name, such as postgres.
	Connector string
	// OpCheck or OpDiscover, which only sources have.
	Op string
	// One of them.
	SourceID      string
	DestinationID string
}

type RetentionInput struct {
	// How long the checkpoint mirror keeps a day's checkpoints.
	CheckpointHistory time.Duration
}

type RetentionResult struct {
	// The checkpoint mirror's partitions dropped.
	MirrorPartitions []string
}

type DropMirrorPartitionsInput struct {
	// Days that end by then are dropped.
	Before time.Time
}

type ConnectorOpResult struct {
	// Of the source or destination the operation ran on.
	Version string
	// A failed check's taxonomy code and message; empty if it passed.
	Code    string
	Message string
	// The discovered catalog's digest (source_catalogs.digest).
	Catalog string
}
