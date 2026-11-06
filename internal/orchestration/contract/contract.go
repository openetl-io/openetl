// Package contract names the workflows, the steps they take and the work they
// send to data planes, and defines their payloads, so workflows and runners
// can share them without importing each other.
package contract

import (
	"encoding/json"
	"time"

	"github.com/unsuitable001/openetl/internal/platform/resources"
)

const (
	OrchestratorQueue = "cp.orchestrator"

	SyncWorkflow             = "SyncWorkflow"
	StreamSupervisorWorkflow = "StreamSupervisorWorkflow"
	ConnectorOpWorkflow      = "ConnectorOpWorkflow"
	RetentionWorkflow        = "RetentionWorkflow"
	ReadLogsWorkflow         = "ReadLogsWorkflow"

	// Steps on the control plane.
	StartSync            = "StartSync"
	FinishSync           = "FinishSync"
	GetStreamState       = "GetStreamState"
	StartRun             = "StartRun"
	EnqueueWork          = "EnqueueWork"
	GetWork              = "GetWork"
	CancelWork           = "CancelWork"
	DropMirrorPartitions = "DropMirrorPartitions"
	AuditPartitions      = "AuditPartitions"

	// Work on a runner.
	RunSyncAttempt = "RunSyncAttempt"
	RunStreamJob   = "RunStreamJob"
	RunDbt         = "RunDbt"
	RunSpark       = "RunSpark"
	CheckConnector = "CheckConnector"
	DiscoverSource = "DiscoverSource"
	TeardownSource = "TeardownSource"
	TeardownFlink  = "TeardownFlink"
	ReadLogs       = "ReadLogs"
)

// Kinds of light operation, which runners take a fixed number of at a time,
// each kind apart so busy runners still answer log reads.
const (
	LightOps  = "ops"
	LightLogs = "logs"
)

// Priorities of attempts waiting for room; lower goes first.
const (
	PriorityManual = iota
	PriorityScheduled
	PriorityBackfill
)

// WakeSignal tells a workflow that work it sent finished. It carries the
// work's ID.
const WakeSignal = "work-finished"

// WorkInput is work a workflow sends to a data plane.
type WorkInput struct {
	OrgID     string
	DataPlane string
	// Unique across retries; the same ID enqueues once.
	ID         string
	WorkflowID string
	// What runs it, such as RunSyncAttempt.
	Kind string
	// Empty for an attempt, which is placed on a runner; otherwise LightOps
	// or LightLogs, which any runner of the data plane takes.
	Light string
	// The kind's input.
	Input    any
	Priority int
	// What an attempt is placed for. Zero for a pipeline's sync attempt or stream
	// job: EnqueueWork works it out.
	Size resources.Size `json:",omitzero"`
	// What set the size.
	SizeReason string `json:",omitempty"`
	// The runners that may take an attempt. Empty: the default pool.
	Pool string `json:",omitempty"`
	// How long it may go unreported.
	Heartbeat time.Duration
	// How long a light operation may wait for a runner. An attempt waits for
	// room as long as it takes.
	Pickup time.Duration `json:",omitempty"`
	// How long it may run.
	Timeout time.Duration
	// An attempt's pipeline version, whose connectors a runner must be able to
	// run; empty for other work.
	PipelineID string `json:",omitempty"`
	Version    string `json:",omitempty"`
	// A sync attempt's or stream job's sync, and which of its attempts it is,
	// which keeps its usage. Empty for other work.
	SyncID  string `json:",omitempty"`
	Attempt int32  `json:",omitempty"`
}

// WorkRef names work a workflow sent.
type WorkRef struct {
	OrgID string
	ID    string
}

// WorkStatus is whether work has finished, and how.
type WorkStatus struct {
	Done    bool
	Outcome WorkOutcome `json:",omitzero"`
}

// WorkOutcome is how work ended. It failed if it has a code, and stopped when
// asked to if it's Cancelled.
type WorkOutcome struct {
	Result       json.RawMessage `json:",omitempty"`
	Code         string          `json:",omitempty"`
	Message      string          `json:",omitempty"`
	NonRetryable bool            `json:",omitempty"`
	// How long to wait before a retry, if the failure says.
	RetryAfter time.Duration `json:",omitempty"`
	Cancelled  bool          `json:",omitempty"`
}

// NoRunner is the code of a light operation no runner took in time.
const NoRunner = "NO_RUNNER"

// Failure is an error with a code of its own, outside the taxonomy, which
// isn't retried.
type Failure struct {
	Code    string
	Message string
}

func (f *Failure) Error() string { return f.Message }

// DbtFailed is the error type of a dbt command that ran and failed. It isn't
// retried; the next sync runs the step again.
const DbtFailed = "DBT_FAILED"

// SparkFailed is the error type of a Spark application that ran and failed.
// It isn't retried; the next sync runs the step again.
const SparkFailed = "SPARK_FAILED"

// FlinkFailed is the error type of a continuous pipeline's Flink job that
// failed past its restart strategy. It fails the run.
const FlinkFailed = "FLINK_FAILED"

// One retention schedule runs for the whole deployment.
const (
	RetentionWorkflowID = "gc"
	RetentionScheduleID = "schedule/gc"
)

// DefaultCheckpointHistory is how long the control plane keeps the
// checkpoint mirror.
const DefaultCheckpointHistory = 30 * 24 * time.Hour

// DefaultLogHistory is how long a data plane keeps syncs' logs.
const DefaultLogHistory = 30 * 24 * time.Hour

// Connector operations.
const (
	OpCheck    = "check"
	OpDiscover = "discover"
	// Removes what a source keeps upstream for a deleted pipeline.
	OpTeardown = "teardown"
	// Cancels a deleted pipeline's Flink jobs.
	OpTeardownFlink = "teardown-flink"
)

// OpPickup is how long a connector operation waits for a runner of its data
// plane to take it.
const OpPickup = time.Minute

// OpTimeout is how long a connector operation may take in all.
const OpTimeout = 15 * time.Minute

// LogsPickup is how long a log read waits for a runner of its data plane.
const LogsPickup = 15 * time.Second

func SyncWorkflowID(pipelineID string) string { return "sync/" + pipelineID }

func ScheduleID(pipelineID string) string { return "schedule/" + pipelineID }

func StreamWorkflowID(pipelineID string) string { return "stream/" + pipelineID }

// RefreshSignal asks a continuous pipeline's supervisor to look at the
// pipeline again: a new version, a pause or resume, a change of mode, or
// its deletion. It carries nothing.
const RefreshSignal = "refresh"

func OpWorkflowID(id string) string { return "op/" + id }

func LogsWorkflowID(id string) string { return "logs/" + id }

type SyncInput struct {
	OrgID      string
	PipelineID string
}

type StartSyncResult struct {
	// Empty if the pipeline is paused or continuous, and no sync was
	// recorded.
	SyncID      string
	Version     string
	DataPlane   string
	Paused      bool
	Continuous  bool
	WorkspaceID string
	// Of the source and the destination.
	Connectors []string
	// The pipeline version's post-sync steps, with the bundles it pins.
	PostSync []PostSyncStep
	// The API started it by hand.
	Manual bool `json:",omitempty"`
	// The pool the pipeline's work goes to; empty for the default.
	Pool string `json:",omitempty"`
}

// PostSyncStep is one of a pipeline's post-sync steps: Dbt or Spark.
type PostSyncStep struct {
	Dbt   *DbtStep   `json:",omitempty"`
	Spark *SparkStep `json:",omitempty"`
}

type DbtStep struct {
	Project string
	// The bundle the pipeline version pins: sha256:{hex}.
	Digest string
	// build, run or test.
	Command         string
	Select, Exclude string
	// Empty: the schema the pipeline writes to.
	Schema string
}

// SparkStep runs the user's own Spark application.
type SparkStep struct {
	Cluster string
	// The cluster's version the pipeline version pins.
	ClusterVersion string
	// One of Jar and Python: an s3:// or https:// URI.
	Jar, Python string
	SHA256      string
	MainClass   string
	Args        []string
	Conf        map[string]string
}

type SparkInput struct {
	OrgID      string
	PipelineID string
	Version    string
	Step       SparkStep
	SyncID     string
	// The sync's run, which its checkpoint records name.
	RunKey string
	// The step's place in the pipeline's post-sync steps, from 1.
	StepNumber int
	// The step's attempt, from 1.
	Attempt int32
}

type SparkResult struct {
	Cluster string
	// What the cluster calls the application: a driver's or YARN
	// application's ID, a driver pod's name, or an EMR step's ID.
	Submission string
	// As the cluster reports it, such as FINISHED or COMPLETED.
	State   string
	Seconds float64
	// The application's UI, where the cluster has one.
	URL string `json:",omitempty"`
}

type DbtInput struct {
	OrgID      string
	PipelineID string
	// The pipeline version the sync ran, for its destination.
	Version string
	Step    DbtStep
	SyncID  string
	// The step's place in the pipeline's post-sync steps, from 1.
	StepNumber int
	// The step's attempt, from 1.
	Attempt int32
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
	Dbt   *DbtResult   `json:",omitempty"`
	Spark *SparkResult `json:",omitempty"`
}

type AttemptInput struct {
	OrgID      string
	PipelineID string
	Version    string
	SyncID     string
	// The sync's run, which its checkpoint records name.
	RunKey string
	// The sync's attempt, from 1, counting attempts that drained.
	Attempt int32
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
	// The error's taxonomy code, if it has one, such as SCHEMA_DRIFT.
	Code string
	// A continuous pipeline's run that stopped when asked to, for a new
	// version, a pause or a change of mode.
	Stopped bool
	// The attempt that committed the sync's data; nil if none did.
	Result *AttemptResult
	// The post-sync steps that succeeded.
	PostSync []PostSyncResult
}

type SyncResult struct {
	SyncID  string
	Attempt AttemptResult
}

type StreamInput struct {
	OrgID      string
	PipelineID string
	// The run a previous workflow run continued as new in the middle of;
	// nil to look at the pipeline and start one.
	Run *StreamRun
}

// StreamState is what a continuous pipeline's supervisor acts on.
type StreamState struct {
	// False once the pipeline is deleted or scheduled, which ends the
	// supervisor.
	Continuous  bool
	Paused      bool
	Version     string
	DataPlane   string
	WorkspaceID string
	// Of the source and the destination.
	Connectors []string
	// The newest hold release or discard (ADR-0025), which a run takes when
	// it starts; empty for none.
	Releases string
	// The pool its jobs go to; empty for the default.
	Pool string `json:",omitempty"`
}

// StreamRun is one run of a continuous pipeline: a version running from when
// it starts until it is stopped or fails.
type StreamRun struct {
	State  StreamState
	SyncID string
	RunKey string
	// Attempts before the current one, which drained or failed.
	Earlier int32
	// The wait before the job's last retry; zero once a job has run long
	// enough to be healthy.
	Backoff time.Duration `json:",omitempty"`
	// The next job is a retry, and waits Backoff first.
	Retry bool `json:",omitempty"`
	// The job running when a workflow run continued as new, which the next
	// workflow run waits for, and when it started.
	Job      string    `json:",omitempty"`
	JobStart time.Time `json:",omitzero"`
}

type StartRunInput struct {
	OrgID      string
	PipelineID string
	Version    string
}

type StartRunResult struct {
	SyncID string
	// Joins the run's checkpoints in the control plane's mirror
	// (syncs.run_id).
	RunKey string
}

type StreamJobInput struct {
	OrgID      string
	PipelineID string
	Version    string
	SyncID     string
	RunKey     string
	// The run's attempt, from 1, counting attempts that drained or failed.
	Attempt int32
}

type ConnectorOpInput struct {
	// Names the workflow; the runner shows it to the API to read the
	// connector's config.
	OpID        string
	OrgID       string
	WorkspaceID string
	DataPlane   string
	// The connector's name, such as postgres.
	Connector string
	// OpCheck, or OpDiscover or OpTeardown, which only sources have, or
	// OpTeardownFlink, on no connector.
	Op string
	// The deleted pipeline, for OpTeardown and OpTeardownFlink.
	PipelineID string
	// One of them.
	SourceID      string
	DestinationID string
}

type RetentionInput struct {
	// How long the checkpoint mirror keeps a day's checkpoints.
	CheckpointHistory time.Duration
	// How long the audit log keeps an entry at least: a month goes once it
	// ended that long ago. Zero keeps every entry.
	AuditHistory time.Duration
}

type RetentionResult struct {
	// The checkpoint mirror's partitions dropped.
	MirrorPartitions []string
	// The audit log's.
	AuditPartitions []string
}

type AuditPartitionsInput struct {
	// The next month's partition is created ahead.
	Now time.Time
	// Months that end by then are dropped; none if it's zero.
	Before time.Time
}

type DropMirrorPartitionsInput struct {
	// Days that end by then are dropped.
	Before time.Time
}

type ReadLogsInput struct {
	// The data plane that ran the sync.
	DataPlane  string
	OrgID      string
	PipelineID string
	SyncID     string
	// Where the last read ended; empty for the start.
	After LogCursor
	// At most this many bytes of lines, but at least one line.
	MaxBytes int
}

// LogCursor is a read's place in a sync's log: a chunk, and how many of its
// lines were read.
type LogCursor struct {
	Key   string
	Lines int
}

type ReadLogsResult struct {
	Lines []LogLine
	Next  LogCursor
}

type LogLine struct {
	// The attempt's directory, such as attempt-001 or step-01-attempt-001.
	Task string
	// A JSON object.
	Text string
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
