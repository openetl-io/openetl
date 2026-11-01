package activities

import (
	"cmp"
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/unsuitable001/openetl/internal/attemptlog"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/platform/temporalx"
)

// attemptLog also writes log to the attempt's log in store, if there is a
// store and a sync. Call done last, so it has every line.
func attemptLog(ctx context.Context, store checkpoint.Store, log *slog.Logger, org, pipeline, sync string, step, attempt int) (_ *slog.Logger, done func()) {
	if store == nil || sync == "" {
		return log, func() {}
	}
	w := attemptlog.NewWriter(store, attemptlog.SyncPrefix(org, pipeline, sync)+attemptlog.Task(step, attempt)+"/", log)
	return slog.New(slog.NewMultiHandler(log.Handler(), w.Handler())), func() { w.Close(context.WithoutCancel(ctx)) }
}

// pruneLogs deletes the pipeline's logs of syncs older than history, in the
// background. Wait for it with the returned func.
func pruneLogs(ctx context.Context, store checkpoint.Store, history time.Duration, org, pipeline string, log *slog.Logger) (wait func()) {
	if store == nil {
		return func() {}
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	done := make(chan struct{})
	go func() {
		defer close(done)
		n, err := attemptlog.Prune(ctx, store, org, pipeline, time.Now().Add(-cmp.Or(history, contract.DefaultLogHistory)))
		switch {
		case err != nil:
			log.Warn("pruning old syncs' logs failed; the next sync tries again", "deleted", n, "err", err)
		case n > 0:
			log.Info("pruned old syncs' logs", "deleted", n)
		}
	}()
	return func() {
		<-done
		cancel()
	}
}

// Logs reads syncs' logs for the API.
type Logs struct {
	Store checkpoint.Store
}

// The most a read returns, well below Temporal's payload limit.
const maxLogRead = 256 << 10

func (a *Logs) ReadLogs(ctx context.Context, in contract.ReadLogsInput) (contract.ReadLogsResult, error) {
	res, err := a.read(ctx, in)
	return res, temporalx.ApplicationError(err)
}

func (a *Logs) read(ctx context.Context, in contract.ReadLogsInput) (contract.ReadLogsResult, error) {
	var res contract.ReadLogsResult
	for _, id := range []string{in.OrgID, in.PipelineID, in.SyncID} {
		if u, err := uuid.Parse(id); err != nil || u.String() != id {
			return res, errs.New(errs.ConfigInvalid, "%q isn't an ID", id)
		}
	}
	prefix := attemptlog.SyncPrefix(in.OrgID, in.PipelineID, in.SyncID)
	if in.After.Key != "" && !strings.HasPrefix(in.After.Key, prefix) {
		return res, errs.New(errs.ConfigInvalid, "the cursor isn't in sync %s's log", in.SyncID)
	}
	limit := maxLogRead
	if in.MaxBytes > 0 {
		limit = min(in.MaxBytes, limit)
	}
	lines, next, err := attemptlog.Read(ctx, a.Store, prefix, attemptlog.Cursor(in.After), limit)
	if err != nil {
		return res, errs.Wrap(errs.Transient, err, "read the sync's log")
	}
	res.Next = contract.LogCursor(next)
	for _, l := range lines {
		res.Lines = append(res.Lines, contract.LogLine{Task: l.Task, Text: l.Text})
	}
	return res, nil
}
