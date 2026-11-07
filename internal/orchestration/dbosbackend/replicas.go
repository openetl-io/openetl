package dbosbackend

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openetl-io/openetl/internal/orchestration"
)

const (
	// How often an orchestrator says it's alive.
	reportEvery = 10 * time.Second
	// How long another waits before it takes over its workflows. A replica
	// that was only partitioned and comes back is fenced by DBOS, and what it
	// sent to runners by the checkpoint log.
	reportLapse = 30 * time.Second
	// An advisory lock on the system database.
	leaderLock = 7236402
	// Rows of executors long gone are of no use.
	forgetExecutor = time.Hour

	// How long a finished workflow's rows stay, and how often they're removed.
	retainFinished = 7 * 24 * time.Hour
	collectEvery   = time.Hour
	collectBatch   = 1000
)

func (b *Backend) report(ctx context.Context, version string) error {
	_, err := b.pool.Exec(ctx, `
		INSERT INTO openetl.executors (id, version, reported_at) VALUES ($1, $2, now())
		ON CONFLICT (id) DO UPDATE SET version = excluded.version, reported_at = excluded.reported_at`, b.o.ExecutorID, version)
	return err
}

// lapse is how long an orchestrator may go without reporting, and every is how
// often it reports, the leader looks for workflows to take over, and the others
// see whether to lead.
func (b *Backend) lapse() (lapse, every time.Duration) {
	lapse = cmp.Or(b.o.Lapse, reportLapse)
	return lapse, min(reportEvery, lapse/3)
}

// runReplica reports this orchestrator, and leads when it can, until ctx ends.
func (b *Backend) runReplica(ctx context.Context, version string) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.lead(ctx, version)
	}()
	_, every := b.lapse()
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			<-done
			return
		case <-tick.C:
		}
		if err := b.report(ctx, version); err != nil && ctx.Err() == nil {
			b.log.Warn("reporting this orchestrator failed", "err", err)
		}
	}
}

// lead holds the leader's lock when no other does, and does the leader's work.
// The lock goes with its connection, so a leader that's lost its database
// stops leading.
func (b *Backend) lead(ctx context.Context, version string) {
	_, every := b.lapse()
	for {
		if err := b.leadOnce(ctx, version); err != nil && ctx.Err() == nil {
			b.log.Warn("leading failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
	}
}

func (b *Backend) leadOnce(ctx context.Context, version string) error {
	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	leading := false
	defer func() { release(conn, leading) }()
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderLock).Scan(&leading); err != nil || !leading {
		return err
	}
	b.log.Info("leading the orchestrators", "executor", b.o.ExecutorID)
	_, every := b.lapse()
	tick := time.NewTicker(every)
	defer tick.Stop()
	var collected time.Time
	for {
		if err := conn.Ping(ctx); err != nil {
			return err
		}
		if err := b.recoverLapsed(ctx, version); err != nil && ctx.Err() == nil {
			b.log.Warn("recovering workflows failed", "err", err)
		}
		if time.Since(collected) >= collectEvery {
			collected = time.Now()
			if err := b.collect(ctx); err != nil && ctx.Err() == nil {
				b.log.Warn("removing finished workflows failed", "err", err)
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

// release gives a connection back, unless it holds the lock, which goes only
// with the connection.
func release(conn *pgxpool.Conn, locked bool) {
	if !locked {
		conn.Release()
		return
	}
	c := conn.Hijack()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.Close(ctx)
}

// recoverLapsed looks for workflows no live orchestrator will run: those of a
// replica whose reports have lapsed, or of another version with no one left
// to run it. Those of this version carry on from where they were, here or on
// another replica. Those of another can't, so they're cancelled and started
// again, as the workflows know how to.
func (b *Backend) recoverLapsed(ctx context.Context, version string) error {
	live, err := b.liveExecutors(ctx)
	if err != nil {
		return err
	}
	list, err := dbos.ListWorkflows(b.with(ctx), dbos.WithFilterStatus(dbos.WorkflowStatusPending, dbos.WorkflowStatusEnqueued),
		dbos.WithFilterLoadInput(false), dbos.WithFilterLoadOutput(false))
	if err != nil {
		return err
	}
	liveVersions := map[string]bool{version: true}
	for _, v := range live {
		liveVersions[v] = true
	}
	// What the API enqueues waits for the latest version, so one that's gone,
	// like a canary taken away, would stop every new workflow.
	latest, err := dbos.GetLatestApplicationVersion(b.with(ctx))
	if err == nil && !liveVersions[latest.Name] {
		b.log.Info("no orchestrator runs the latest version; this one takes over from it", "latest", latest.Name, "version", version)
		err = dbos.SetLatestApplicationVersion(b.with(ctx), version)
	}
	if err != nil && !errors.Is(err, dbos.ErrNoApplicationVersions) {
		return fmt.Errorf("the latest version: %w", err)
	}
	var resume, restart []string
	for _, w := range list {
		var running bool
		if w.Status == dbos.WorkflowStatusPending {
			v, ok := live[w.ExecutorID]
			running = (ok && v == w.ApplicationVersion) || (w.ExecutorID == b.o.ExecutorID && w.ApplicationVersion == version)
		} else {
			// With no version yet, it goes to the latest.
			running = w.ApplicationVersion == "" || liveVersions[w.ApplicationVersion]
		}
		switch {
		case running:
		case w.ApplicationVersion == version:
			resume = append(resume, w.ID)
		default:
			restart = append(restart, w.ID)
		}
	}
	if len(resume) > 0 {
		b.log.Info("recovering workflows of a lapsed orchestrator", "count", len(resume))
		if _, err := dbos.ResumeWorkflows[json.RawMessage](b.with(ctx), resume); err != nil {
			return fmt.Errorf("resuming: %w", err)
		}
	}
	if len(restart) > 0 {
		b.log.Info("starting workflows of another version again", "count", len(restart))
		if err := b.restart(ctx, restart); err != nil {
			return err
		}
	}
	_, err = b.pool.Exec(ctx, `DELETE FROM openetl.executors WHERE reported_at < now() - $1::interval`, forgetExecutor.String())
	return err
}

// liveExecutors maps the orchestrators that have reported lately to their
// versions.
func (b *Backend) liveExecutors(ctx context.Context) (map[string]string, error) {
	lapse, _ := b.lapse()
	rows, err := b.pool.Query(ctx, `SELECT id, version FROM openetl.executors WHERE reported_at >= now() - $1::interval`, lapse.String())
	if err != nil {
		return nil, err
	}
	live := map[string]string{}
	var id, version string
	_, err = pgx.ForEachRow(rows, []any{&id, &version}, func() error {
		live[id] = version
		return nil
	})
	return live, err
}

// restart gives rows of a version nothing runs a next generation, and cancels
// them. That way round, a crash in between leaves the row to be seen again. An
// operation isn't started again, as whoever waits for it hears it failed.
func (b *Backend) restart(ctx context.Context, rows []string) error {
	list, err := dbos.ListWorkflows(b.with(ctx), dbos.WithFilterWorkflowIDs(rows...), dbos.WithFilterLoadInput(true), dbos.WithFilterLoadOutput(false))
	if err != nil {
		return err
	}
	var failed error
	cancel := make([]string, 0, len(list))
	for _, w := range list {
		if queueFor(w.Name) != queueOps {
			if err := b.supersede(ctx, w); err != nil {
				failed = errors.Join(failed, fmt.Errorf("starting %s again: %w", w.ID, err))
				continue
			}
		}
		cancel = append(cancel, w.ID)
	}
	if len(cancel) > 0 {
		if err := dbos.CancelWorkflows(b.with(ctx), cancel); err != nil {
			failed = errors.Join(failed, fmt.Errorf("cancelling: %w", err))
		}
	}
	return failed
}

// supersede starts a row's next generation with the input it had, unless
// there is one.
func (b *Backend) supersede(ctx context.Context, w dbos.WorkflowStatus) error {
	env, err := decodeEnvelope(w.Input)
	if err != nil {
		return err
	}
	n := max(env.Generation, 1)
	env.Generation = n + 1
	err = pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
		if err := advance(ctx, tx, env.ID, n); err != nil {
			return err
		}
		return b.enqueue(ctx, tx, generationID(env.ID, n+1), w.Name, env, 0)
	})
	if errors.Is(err, dbos.ErrWorkflowIDInUse) {
		return nil
	}
	return err
}

// decodeEnvelope reads an input as DBOS lists it, which is still JSON.
func decodeEnvelope(input any) (envelope, error) {
	var env envelope
	s, ok := input.(string)
	if !ok {
		return env, fmt.Errorf("an input of type %T", input)
	}
	return env, json.Unmarshal([]byte(s), &env)
}

// Terminate ends a workflow with nothing it can do about it, unlike Cancel,
// which is what an operator or a test wants of one that's gone wrong.
func (b *Backend) Terminate(ctx context.Context, workflowID string) error {
	n, err := b.generation(ctx, workflowID)
	if err != nil {
		return err
	}
	return dbos.CancelWorkflow(b.with(ctx), generationID(workflowID, n))
}

// collect removes finished workflows past their keep.
func (b *Backend) collect(ctx context.Context) error {
	for {
		list, err := dbos.ListWorkflows(b.with(ctx),
			dbos.WithFilterStatus(dbos.WorkflowStatusSuccess, dbos.WorkflowStatusError, dbos.WorkflowStatusCancelled, dbos.WorkflowStatusMaxRecoveryAttemptsExceeded),
			dbos.WithFilterCreatedBefore(time.Now().Add(-retainFinished)),
			dbos.WithFilterLimit(collectBatch), dbos.WithFilterLoadInput(false), dbos.WithFilterLoadOutput(false))
		if err != nil {
			return err
		}
		ids := make([]string, 0, len(list))
		for _, w := range list {
			ids = append(ids, w.ID)
		}
		if len(ids) > 0 {
			if err := dbos.DeleteWorkflows(b.with(ctx), ids); err != nil {
				return err
			}
		}
		if len(ids) < collectBatch {
			break
		}
	}
	// An ID whose newest generation has gone can start from its first.
	_, err := b.pool.Exec(ctx, `DELETE FROM openetl.generations g WHERE NOT EXISTS (
		SELECT 1 FROM dbos.workflow_status w WHERE w.workflow_uuid = g.id || '@' || g.generation)`)
	return err
}

// startNext is what ending with Continue does. What's in the mailbox goes to
// the next generation, and what's sent after the ID has moved on goes there
// anyway, but for a message already on its way, so a workflow must still look
// at what a message would say.
func (b *Backend) startNext(g Generation, input json.RawMessage) (string, error) {
	next := generationID(g.ID, g.Number+1)
	_, err := dbos.RunAsStep(g.Ctx, func(ctx context.Context) (struct{}, error) {
		plain := plainContext{ctx}
		env := envelope{ID: g.ID, Generation: g.Number + 1, Attributes: g.Attributes, Trace: traceHeaders(plain), Input: input}
		err := pgx.BeginFunc(plain, b.pool, func(tx pgx.Tx) error {
			if err := advance(plain, tx, g.ID, g.Number); err != nil {
				return err
			}
			return b.enqueue(plain, tx, next, g.Workflow, env, 0)
		})
		// A crash after it committed brings this here again.
		if errors.Is(err, dbos.ErrWorkflowIDInUse) {
			return struct{}{}, nil
		}
		return struct{}{}, err
	}, dbos.WithStepName("continue"))
	if err != nil {
		return "", err
	}
	for {
		m, err := dbos.Recv[orchestration.Message](g.Ctx, mailbox, 0)
		if errors.Is(err, dbos.ErrTimeout) {
			return next, nil
		}
		if err != nil {
			return "", err
		}
		if err := dbos.Send(g.Ctx, next, m, mailbox); err != nil {
			return "", err
		}
	}
}
