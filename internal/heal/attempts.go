package heal

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl/internal/store/db"
)

const (
	// How many of a continuous run's attempts fail in a row, without a
	// commit and within crashLoopWindow, before it's a crash loop.
	crashLoopFailures = 3
	crashLoopWindow   = 24 * time.Hour
	maxAttemptError   = 2048
)

// AttemptFailure is an attempt that failed, as its runner reports it or as
// the dispatcher finds it: its result, or its lease running out.
type AttemptFailure struct {
	Pipeline uuid.UUID
	RunID    string
	Attempt  int32
	Code     string
	Message  string
	Time     time.Time
}

// AttemptFailed records a failed attempt once, however often it's told, and
// opens the run's crash loop incident once enough have failed.
func AttemptFailed(ctx context.Context, q *db.Queries, org uuid.UUID, f AttemptFailure) error {
	msg := f.Message
	if len(msg) > maxAttemptError {
		msg = strings.ToValidUTF8(msg[:maxAttemptError], "")
	}
	arg := db.RecordAttemptFailureParams{OrgID: org, PipelineID: f.Pipeline, RunID: f.RunID, Attempt: f.Attempt, Error: &msg, Time: &f.Time}
	if f.Code != "" {
		arg.Code = &f.Code
	}
	row, err := q.RecordAttemptFailure(ctx, arg)
	if errors.Is(err, pgx.ErrNoRows) {
		// Told already, or its sync has ended.
		return nil
	}
	if err != nil || !row.Continuous || row.FailuresSinceCommit < crashLoopFailures {
		return err
	}
	recent, err := q.RecentFailedAttempts(ctx, db.RecentFailedAttemptsParams{OrgID: org, SyncID: row.ID, Attempt: f.Attempt, Since: time.Now().Add(-crashLoopWindow)})
	if err != nil || recent < crashLoopFailures {
		return err
	}
	_, err = Observe(ctx, q, org, Observation{
		Pipeline: f.Pipeline, Version: row.Version, Class: CrashLoop, Code: f.Code, Message: msg,
		Details: map[string]string{"failures": strconv.Itoa(int(row.FailuresSinceCommit)), "attempt": strconv.Itoa(int(f.Attempt))},
		Sync:    &row.ID, Key: row.ID.String(),
	})
	return err
}
