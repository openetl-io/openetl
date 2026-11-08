package dbosbackend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5"
	"github.com/robfig/cron/v3"

	"github.com/openetl-io/openetl/internal/orchestration"
)

// DBOS's own schedules don't fit. An interval there starts counting when a
// replica installs it, so replicas fire apart and a restart starts it over;
// jitter stops at 10 s; nothing stops a run overlapping the last; and a new one
// takes up to its 30 s poll to start. So they're rows here, and every
// orchestrator fires what's due.

const (
	// How often an orchestrator looks for what's due, unless told to.
	scheduleTick = time.Second
	// How many it fires at a time.
	scheduleBatch = 100
)

// timetable says when a schedule is due.
type timetable struct {
	every  time.Duration
	cron   string
	jitter time.Duration
}

// next is when it's due after a time, so one start makes up for however many
// were missed. Intervals count from the Unix epoch, as Temporal's do, so
// every 1h is due on the hour.
func (t timetable) next(after time.Time) (time.Time, error) {
	if t.every > 0 {
		every := int64(t.every)
		return time.Unix(0, (after.UnixNano()/every+1)*every).UTC(), nil
	}
	s, err := cron.ParseStandard(t.cron)
	if err != nil {
		return time.Time{}, err
	}
	// A cron without a zone goes by the zone of the time it's given.
	return s.Next(after.UTC()), nil
}

// fireAt is when to start what's due at a time.
func (t timetable) fireAt(due time.Time) time.Time {
	if t.jitter <= 0 {
		return due
	}
	return due.Add(rand.N(t.jitter)) // #nosec G404 -- spreading starts out needs no secrecy
}

func (b *Backend) UpsertSchedule(ctx context.Context, s orchestration.Schedule) error {
	input, err := json.Marshal(s.Input)
	if err != nil {
		return err
	}
	t := timetable{every: s.Every, cron: s.Cron, jitter: s.Jitter}
	err = pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
			return err
		}
		next, err := t.next(now)
		if err != nil {
			return err
		}
		due, fire := next, t.fireAt(next)
		if s.StartNow && !s.Paused {
			due, fire = now, now
		}
		// A changed timetable counts from now, and otherwise the schedule
		// stays on its course.
		_, err = tx.Exec(ctx, `
			INSERT INTO openetl.schedules (id, every_ms, cron, jitter_ms, workflow_id, workflow, input, org, paused, due, fire_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			ON CONFLICT (id) DO UPDATE SET
				due = CASE WHEN (schedules.every_ms, schedules.cron) = (excluded.every_ms, excluded.cron) THEN schedules.due ELSE $12 END,
				fire_at = CASE WHEN (schedules.every_ms, schedules.cron) = (excluded.every_ms, excluded.cron) THEN schedules.fire_at ELSE $13 END,
				every_ms = excluded.every_ms, cron = excluded.cron, jitter_ms = excluded.jitter_ms,
				workflow_id = excluded.workflow_id, workflow = excluded.workflow, input = excluded.input, org = excluded.org`,
			s.ID, s.Every.Milliseconds(), s.Cron, s.Jitter.Milliseconds(), s.WorkflowID, s.Workflow, input, s.Org, s.Paused, due, fire, next, t.fireAt(next))
		return err
	})
	if err != nil {
		return fmt.Errorf("schedule %s: %w", s.ID, err)
	}
	select {
	case b.fire <- struct{}{}:
	default:
	}
	return nil
}

func (b *Backend) DeleteSchedule(ctx context.Context, id string) error {
	_, err := b.pool.Exec(ctx, `DELETE FROM openetl.schedules WHERE id = $1`, id)
	return err
}

// PauseSchedule skips what would have started while it was paused.
func (b *Backend) PauseSchedule(ctx context.Context, id string, paused bool) error {
	return pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
		var (
			everyMS, jitterMS int64
			cronSpec          string
			due, now          time.Time
		)
		err := tx.QueryRow(ctx, `SELECT every_ms, cron, jitter_ms, due, now() FROM openetl.schedules WHERE id = $1 FOR UPDATE`, id).Scan(&everyMS, &cronSpec, &jitterMS, &due, &now)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("no schedule %s", id)
		}
		if err != nil {
			return err
		}
		if paused || due.After(now) {
			_, err := tx.Exec(ctx, `UPDATE openetl.schedules SET paused = $2 WHERE id = $1`, id, paused)
			return err
		}
		t := timetable{every: time.Duration(everyMS) * time.Millisecond, cron: cronSpec, jitter: time.Duration(jitterMS) * time.Millisecond}
		next, err := t.next(now)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE openetl.schedules SET paused = false, due = $2, fire_at = $3 WHERE id = $1`, id, next, t.fireAt(next))
		return err
	})
}

// scheduleRow is a schedule to start a workflow of.
type scheduleRow struct {
	id, workflowID, workflow, org, lastWorkflow string
	input                                       json.RawMessage
}

func (r scheduleRow) envelope(ctx context.Context) envelope {
	return envelope{ID: r.workflowID, Attributes: orchestration.Attributes{Org: r.org}, Trace: traceHeaders(ctx), Input: r.input}
}

// startFrom starts the schedule's workflow, under an ID of its own, unless the
// last one it started is running. The row stays locked meanwhile.
func (b *Backend) startFrom(ctx context.Context, tx pgx.Tx, r scheduleRow, at time.Time) (bool, error) {
	if r.lastWorkflow != "" {
		// A deploy may have started it again as a later generation.
		n, err := b.generation(ctx, r.lastWorkflow)
		if err != nil {
			return false, err
		}
		running, err := b.isActive(ctx, generationID(r.lastWorkflow, n))
		if err != nil {
			return false, err
		}
		if running {
			return false, nil
		}
	}
	id := fmt.Sprintf("%s-%s", r.workflowID, at.UTC().Format("2006-01-02T15:04:05.000Z"))
	env := r.envelope(ctx)
	env.ID = id
	err := b.enqueue(ctx, tx, id, r.workflow, env, 0)
	if errors.Is(err, dbos.ErrWorkflowIDInUse) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_, err = tx.Exec(ctx, `UPDATE openetl.schedules SET last_workflow = $2 WHERE id = $1`, r.id, id)
	return err == nil, err
}

// TriggerSchedule starts its workflow now, or returns ErrRunning. It doesn't
// move when the schedule is next due.
func (b *Backend) TriggerSchedule(ctx context.Context, id string) error {
	err := pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
		r := scheduleRow{id: id}
		var now time.Time
		err := tx.QueryRow(ctx, `SELECT workflow_id, workflow, input, org, last_workflow, now() FROM openetl.schedules WHERE id = $1 FOR UPDATE`, id).
			Scan(&r.workflowID, &r.workflow, &r.input, &r.org, &r.lastWorkflow, &now)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("no schedule %s", id)
		}
		if err != nil {
			return err
		}
		started, err := b.startFrom(ctx, tx, r, now)
		if err != nil {
			return err
		}
		if !started {
			return orchestration.ErrRunning
		}
		return nil
	})
	return err
}

func (b *Backend) ListSchedules(ctx context.Context) ([]orchestration.ScheduleState, error) {
	rows, err := b.pool.Query(ctx, `SELECT id, org, paused FROM openetl.schedules`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (orchestration.ScheduleState, error) {
		var s orchestration.ScheduleState
		err := r.Scan(&s.ID, &s.Org, &s.Paused)
		return s, err
	})
}

// runSchedules fires what's due until ctx ends.
func (b *Backend) runSchedules(ctx context.Context) {
	tick := time.NewTicker(scheduleTick)
	defer tick.Stop()
	for {
		for {
			fired, err := b.fireDue(ctx)
			if err != nil {
				if ctx.Err() == nil {
					b.log.Warn("firing schedules failed", "err", err)
				}
				break
			}
			if fired < scheduleBatch {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-b.fire:
		}
	}
}

// fireDue fires a batch of what's due, and returns how many it looked at.
// Orchestrators skip what another has locked.
func (b *Backend) fireDue(ctx context.Context) (int, error) {
	var count int
	err := pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, every_ms, cron, jitter_ms, workflow_id, workflow, input, org, last_workflow, fire_at, now()
			FROM openetl.schedules WHERE NOT paused AND fire_at <= now()
			ORDER BY fire_at LIMIT $1 FOR UPDATE SKIP LOCKED`, scheduleBatch)
		if err != nil {
			return err
		}
		type dueRow struct {
			scheduleRow
			t         timetable
			fire, now time.Time
		}
		due, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (dueRow, error) {
			var (
				d                 dueRow
				everyMS, jitterMS int64
			)
			err := row.Scan(&d.id, &everyMS, &d.t.cron, &jitterMS, &d.workflowID, &d.workflow, &d.input, &d.org, &d.lastWorkflow, &d.fire, &d.now)
			d.t.every, d.t.jitter = time.Duration(everyMS)*time.Millisecond, time.Duration(jitterMS)*time.Millisecond
			return d, err
		})
		if err != nil {
			return err
		}
		count = len(due)
		for _, d := range due {
			// A run that's still going misses this one, as Temporal's SKIP does.
			if _, err := b.startFrom(ctx, tx, d.scheduleRow, d.fire); err != nil {
				return err
			}
			next, err := d.t.next(d.now)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE openetl.schedules SET due = $2, fire_at = $3 WHERE id = $1`, d.id, next, d.t.fireAt(next)); err != nil {
				return err
			}
		}
		return nil
	})
	return count, err
}
