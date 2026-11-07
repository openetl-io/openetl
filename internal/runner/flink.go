package runner

import (
	"context"
	"time"
)

// Flink runs the user's Flink job around a continuous pipeline's attempts,
// on one FlinkCluster. Each generation's job has its own ID,
// FlinkJobID's, and its checkpoints are below the pipeline's checkpoint
// directory.
type Flink interface {
	// CancelAll cancels every job of the pipeline, whoever deployed it, and
	// returns once each is terminal, so none can complete a checkpoint
	// after it.
	CancelAll(ctx context.Context) error
	// Completed is job's completed checkpoints that are still retained,
	// ascending by ID; none when the job never completed one.
	Completed(ctx context.Context, job string) ([]FlinkCheckpoint, error)
	// SavepointDir is where a stop named name writes its savepoint.
	SavepointDir(name string) string
	// Savepoint is the completed savepoint below dir; "" if there's none.
	Savepoint(ctx context.Context, dir string) (string, error)
	// Deploy starts a generation's job, and returns once the cluster has
	// accepted it.
	Deploy(ctx context.Context, d FlinkDeploy) (FlinkJob, error)
	// Prune deletes the checkpoint directories of the pipeline's jobs whose
	// IDs keep doesn't hold, and the savepoint directories, as SavepointDir
	// names them, that it doesn't hold either. It's best effort per
	// directory, and fails only if it can't list them.
	Prune(ctx context.Context, keep []string) error
}

// FlinkJob is one deployed job.
type FlinkJob interface {
	ID() string
	// Wait returns once the job is terminal: nil if it finished, after a
	// Stop; a *FlinkFailed with the job's exception if it failed; and an
	// errs.Transient error if the cluster can't be reached.
	Wait(ctx context.Context) error
	// Stop stops the job with a synchronous savepoint into dir, and returns
	// once the savepoint is complete and the job finished.
	Stop(ctx context.Context, dir string) error
	// Cancel cancels the job and returns once it's terminal.
	Cancel(ctx context.Context) error
	// UI links to the job in Flink's web UI; "" if the cluster has none.
	UI() string
}

type FlinkDeploy struct {
	Generation uint64
	// A checkpoint's or savepoint's path to restore from; empty for none.
	RestoreFrom string
	// What the job's arguments carry for the bridge: its address
	// (host:port), its certificate's SHA-256 fingerprint (lowercase hex),
	// and the attempt's token.
	Bridge      string
	Fingerprint string
	Token       string
	// Flink's checkpoint interval: the pipeline's commit interval.
	CheckpointInterval time.Duration
}

// FlinkFailed: the user's job failed, and isn't retried (FLINK_FAILED).
type FlinkFailed struct{ Message string }

func (f *FlinkFailed) Error() string { return "the Flink job failed: " + f.Message }

type FlinkCheckpoint struct {
	ID   int64
	Path string
}
