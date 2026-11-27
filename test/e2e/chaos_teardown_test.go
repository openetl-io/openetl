//go:build e2e && chaos

package e2e

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

// teardownWait is how long a deleted pipeline's teardowns may take. They
// take seconds, and a minute more after a try that fails. Under the control
// plane's faults more tries fail, and an orchestrator killed, or cut off
// from Postgres, while it runs one leaves it claimed for 20 minutes.
func teardownWait() time.Duration {
	if *chaosKills > 0 || *chaosPostgres > 0 || *chaosTemporal > 0 {
		return 25 * time.Minute
	}
	return 3 * time.Minute
}

// What a query of teardowns says of each.
const teardownDesc = `format('%s %s of %s on data plane %s (tries: %s, last error: %s)',
	CASE WHEN source_id IS NOT NULL THEN 'source' WHEN destination_id IS NOT NULL THEN 'destination' ELSE 'Flink cluster' END,
	coalesce(nullif(connector, ''), name), pipeline, data_plane_id, tries, coalesce(last_error, 'none'))`

// deletePipeline deletes the run's pipeline, trying again for a minute while
// the API fails. It fails t, and says why, if it can't.
func (s *chaosSuite) deletePipeline(t *testing.T, name string) string {
	t.Helper()
	req := &apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(2 * time.Second) {
		var err error
		s.pg.do(func() {
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			_, err = pipelines().DeletePipeline(ctx, connect.NewRequest(req))
		})
		if err == nil || connect.CodeOf(err) == connect.CodeNotFound {
			return ""
		}
		if time.Now().After(deadline) {
			msg := fmt.Sprintf("deleting pipeline %s: %v", name, err)
			t.Error(msg)
			return msg
		}
	}
}

// awaitTeardowns waits for the deleted pipeline's teardowns to finish. A
// runner of the pipeline's data plane runs them, so the run's runners must
// outlive them, or what they remove, such as a CDC source's replication
// slot, stays behind. It fails t, and says what's left, if they take longer
// than teardownWait.
func (s *chaosSuite) awaitTeardowns(t *testing.T, name, pipeline string) string {
	t.Helper()
	wait := teardownWait()
	for deadline := time.Now().Add(wait); ; time.Sleep(time.Second) {
		var left []string
		var err error
		s.pg.do(func() {
			left, err = teardowns("SELECT "+teardownDesc+" FROM teardowns WHERE pipeline_id = $1::uuid ORDER BY create_time", pipeline)
		})
		if err == nil && len(left) == 0 {
			return ""
		}
		if time.Now().After(deadline) {
			if err != nil {
				left = append(left, "reading them: "+err.Error())
			}
			msg := fmt.Sprintf("pipeline %s's teardowns hadn't finished %v after it was deleted, so what they remove is left behind: %s", name, wait, strings.Join(left, "; "))
			t.Error(msg)
			return msg
		}
	}
}

// teardowns runs q, which returns teardownDesc's text of each teardown it
// selects.
func teardowns(q string, args ...any) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, env.dbURL)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	rows, err := conn.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// leftBehind runs once the suite's runners have stopped. Each teardown left
// on the suite's data planes, which no runner takes now, fails t and is
// removed, so the orchestrator doesn't try it again for days, with the
// suites after waiting behind it. Each replication slot left of a pipeline
// that's gone fails t and is dropped, so the CDC runs of the suites after
// still get one of Postgres's few.
func (s *chaosSuite) leftBehind(t *testing.T) {
	s.mu.Lock()
	planes := slices.Clone(s.planes)
	s.mu.Unlock()
	left, err := teardowns("DELETE FROM teardowns WHERE data_plane_id = ANY($1) RETURNING "+teardownDesc, planes)
	switch {
	case err != nil:
		t.Errorf("removing the teardowns left on the suite's data planes: %v", err)
	case len(left) > 0:
		t.Errorf("teardowns left on the suite's data planes, which no runner will take now, removed:\n  %s", strings.Join(left, "\n  "))
	}
	slots, err := dropLeakedSlots()
	if err != nil {
		t.Errorf("dropping the replication slots of deleted pipelines: %v", err)
	}
	if len(slots) > 0 {
		t.Errorf("replication slots of deleted pipelines, which their teardowns never dropped, dropped now: %s", strings.Join(slots, ", "))
	}
}

// dropLeakedSlots drops this database's replication slots named after a
// pipeline that's gone, as a Postgres source names its own, and returns
// their names.
func dropLeakedSlots() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, env.dbURL)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	rows, err := conn.Query(ctx, `SELECT slot_name::text FROM pg_replication_slots s
		WHERE database = current_database() AND slot_name ~ '^openetl_[0-9a-f]{32}$'
		AND NOT EXISTS (SELECT 1 FROM pipelines p WHERE replace(p.id::text, '-', '') = substr(s.slot_name, 9))
		ORDER BY slot_name`)
	if err != nil {
		return nil, err
	}
	slots, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	var errs []error
	for _, slot := range slots {
		if err := dropSlot(ctx, conn, slot); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", slot, err))
		}
	}
	return slots, errors.Join(errs...)
}

// dropSlot drops the replication slot, ending the connection that holds it
// if there is one.
func dropSlot(ctx context.Context, conn *pgx.Conn, slot string) error {
	for try := 0; ; try++ {
		if _, err := conn.Exec(ctx, "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active", slot); err != nil {
			return err
		}
		_, err := conn.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", slot)
		// In use: the connection it ended hasn't let go yet.
		if pe, ok := errors.AsType[*pgconn.PgError](err); !ok || pe.Code != "55006" || try >= 20 {
			return err
		}
		pause(ctx, 500*time.Millisecond)
	}
}

// behind says what may hold a pipeline's rows back: how it last failed, and
// where its newest attempt is.
func behind(t *testing.T, pipeline string) string {
	t.Helper()
	return lastFailure(t, pipeline) + "; " + newestAttempt(t, pipeline)
}

// newestAttempt says where the pipeline's newest attempt is in its dispatch:
// waiting for room; placed on a runner, or on a pod launcher and whether the
// launcher has claimed it, until its start deadline; running until its
// lease's; or finished. It also says when that runner or launcher last
// reported: an attempt placed on one that has died waits for its start
// deadline.
func newestAttempt(t *testing.T, pipeline string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, env.dbURL)
	if err != nil {
		return "reading its newest attempt: " + err.Error()
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var (
		attempt, placements int
		age                 float64
		state, plane, code  string
		runner, launcher    *string
		launched            bool
		// Seconds; nil where there's none.
		startIn, leaseIn, finished, runnerReport, launcherReport *float64
	)
	err = conn.QueryRow(ctx, `SELECT a.attempt, extract(epoch FROM now() - a.create_time)::float8,
			d.state, d.data_plane_id, d.runner, d.launcher, d.launched, d.placements,
			extract(epoch FROM d.start_deadline - now())::float8, extract(epoch FROM d.lease_deadline - now())::float8,
			extract(epoch FROM now() - d.finish_time)::float8, coalesce(d.outcome->>'Code', ''),
			(SELECT extract(epoch FROM now() - r.report_time)::float8 FROM runners r
				WHERE r.org_id = d.org_id AND r.data_plane_id = d.data_plane_id AND r.id = d.runner),
			(SELECT extract(epoch FROM now() - r.report_time)::float8 FROM runners r
				WHERE r.org_id = d.org_id AND r.data_plane_id = d.data_plane_id AND r.id = d.launcher)
		FROM attempts a JOIN pipelines p ON p.org_id = a.org_id AND p.id = a.pipeline_id
		JOIN dispatches d ON d.org_id = a.org_id AND d.id = a.id
		WHERE p.name = $1 ORDER BY a.create_time DESC LIMIT 1`, pipeline).Scan(
		&attempt, &age, &state, &plane, &runner, &launcher, &launched, &placements,
		&startIn, &leaseIn, &finished, &code, &runnerReport, &launcherReport)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return "no attempt of it yet"
	case err != nil:
		return "reading its newest attempt: " + err.Error()
	}
	dur := func(s float64) time.Duration {
		return time.Duration(s * float64(time.Second)).Round(100 * time.Millisecond)
	}
	deadline := func(what string, s *float64) string {
		switch {
		case s == nil:
			return "no " + what
		case *s < 0:
			return fmt.Sprintf("%s %v ago", what, dur(-*s))
		}
		return fmt.Sprintf("%s in %v", what, dur(*s))
	}
	reported := func(s *float64) string {
		if s == nil {
			return "has no report"
		}
		return fmt.Sprintf("last reported %v ago", dur(*s))
	}
	name := func(s *string) string {
		if s == nil {
			return "none"
		}
		return *s
	}
	head := fmt.Sprintf("newest attempt: attempt %d of its sync, made %v ago, ", attempt, dur(age))
	switch {
	case state == "pending":
		return head + fmt.Sprintf("waiting for room on data plane %s, after %d placements", plane, placements)
	case state == "placed" && launcher != nil:
		claimed := "not claimed by it"
		if launched {
			claimed = "claimed by it"
		}
		return head + fmt.Sprintf("placed on pod launcher %s of data plane %s (placement %d), %s, %s; the launcher %s",
			*launcher, plane, placements, claimed, deadline("start deadline", startIn), reported(launcherReport))
	case state == "placed":
		return head + fmt.Sprintf("placed on runner %s of data plane %s (placement %d), not started, %s; the runner %s",
			name(runner), plane, placements, deadline("start deadline", startIn), reported(runnerReport))
	case state == "running":
		var pod string
		if launcher != nil {
			pod = fmt.Sprintf(", in a pod of launcher %s, which %s", *launcher, reported(launcherReport))
		}
		return head + fmt.Sprintf("running on runner %s of data plane %s (placement %d)%s, %s; the runner %s",
			name(runner), plane, placements, pod, deadline("lease deadline", leaseIn), reported(runnerReport))
	case state == "finished" && finished != nil:
		if code != "" {
			code = ": " + code
		}
		return head + fmt.Sprintf("finished %v ago%s", dur(*finished), code)
	}
	return head + state
}

// lastFailure says how the pipeline last failed: its latest sync's error
// or, since a stream's attempts leave that empty, its latest failed
// attempt's.
func lastFailure(t *testing.T, pipeline string) string {
	t.Helper()
	msg, err := query[string](t, `SELECT coalesce(s.error, '') FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id
		WHERE p.name = $1 ORDER BY s.start_time DESC LIMIT 1`, pipeline)
	if err == nil && msg != "" {
		return "last sync: " + msg
	}
	msg, err = query[string](t, `SELECT concat_ws(': ', nullif(d.outcome->>'Code', ''), d.outcome->>'Message') FROM attempts a
		JOIN pipelines p ON p.org_id = a.org_id AND p.id = a.pipeline_id JOIN dispatches d ON d.org_id = a.org_id AND d.id = a.id
		WHERE p.name = $1 AND coalesce(d.outcome->>'Message', '') <> '' ORDER BY a.create_time DESC LIMIT 1`, pipeline)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return "no sync or attempt of it failed"
	case err != nil:
		return "reading its last failure: " + err.Error()
	}
	return "last failed attempt: " + msg
}
