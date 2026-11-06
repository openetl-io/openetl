-- A workflow's retry enqueues nothing new.
-- name: EnqueueDispatch :execrows
INSERT INTO dispatches (
    org_id, id, data_plane_id, workflow_id, kind, input, light, pool, size_cpu, size_memory, priority, needs, trace,
    heartbeat_ms, start_timeout_ms, timeout_ms, start_deadline
) VALUES (
    @org_id, @id, @data_plane_id, @workflow_id, @kind, @input, @light, @pool, @size_cpu, @size_memory, @priority, @needs, @trace,
    @heartbeat_ms, sqlc.arg(start_timeout_ms)::bigint, @timeout_ms,
    now() + (sqlc.arg(start_timeout_ms)::bigint * interval '1 millisecond')
) ON CONFLICT (org_id, id) DO NOTHING;

-- name: GetDispatchOutcome :one
SELECT state, outcome FROM dispatches WHERE org_id = @org_id AND id = @id;

-- name: LockPlacement :exec
-- Passes of a data plane run one at a time, until the transaction ends.
SELECT pg_advisory_xact_lock(hashtextextended(@data_plane::text, 0));

-- name: PlacementRunners :many
-- A data plane's runners reporting now, with the room their placements use.
SELECT r.*,
    COALESCE(u.cpu, 0)::bigint AS used_cpu,
    COALESCE(u.memory, 0)::bigint AS used_memory,
    COALESCE(u.attempts, 0)::bigint AS attempts
FROM runners r
LEFT JOIN LATERAL (
    SELECT sum(d.size_cpu) AS cpu, sum(d.size_memory) AS memory, count(*) AS attempts
    FROM dispatches d
    WHERE d.org_id = r.org_id AND d.data_plane_id = r.data_plane_id AND d.runner = r.id AND d.state IN ('placed', 'running')
) u ON true
WHERE r.org_id = @org_id AND r.data_plane_id = @data_plane_id
    AND r.report_time >= now() - (sqlc.arg(lapse_ms)::bigint * interval '1 millisecond')
ORDER BY r.id;

-- name: PendingAttempts :many
-- The attempts waiting for room, longest-waiting first within each priority,
-- with how long they've waited.
SELECT id, pool, size_cpu, size_memory, priority, needs,
    (extract(epoch FROM now() - enqueue_time) * 1000)::bigint AS waited_ms
FROM dispatches
WHERE org_id = @org_id AND data_plane_id = @data_plane_id AND state = 'pending' AND light = ''
ORDER BY priority, enqueue_time
LIMIT @max_rows;

-- name: PlaceDispatch :exec
UPDATE dispatches SET state = 'placed', runner = @runner, start_deadline = now() + (start_timeout_ms * interval '1 millisecond')
WHERE org_id = @org_id AND id = @id AND state = 'pending';

-- name: NotifyDispatch :exec
-- Wakes the API replicas whose runners may have work now.
SELECT pg_notify('openetl_dispatch', @data_plane::text);

-- name: ClaimPlaced :many
-- The attempts placed on this runner in time, which are its to run now.
UPDATE dispatches SET state = 'running',
    run_deadline = now() + (timeout_ms * interval '1 millisecond'),
    lease_deadline = now() + (LEAST(heartbeat_ms, timeout_ms) * interval '1 millisecond')
WHERE org_id = @org_id AND data_plane_id = @data_plane_id AND runner = @runner AND state = 'placed' AND start_deadline > now() AND NOT cancel
RETURNING *;

-- name: ClaimLight :many
-- Light operations of this kind, up to what the runner has room for. The CTE
-- makes the limit hold: as a subquery it can run more than once.
WITH picked AS MATERIALIZED (
    SELECT w.id FROM dispatches w
    WHERE w.org_id = @org_id AND w.data_plane_id = @data_plane_id AND w.light = @light AND w.state = 'pending' AND w.start_deadline > now() AND NOT w.cancel
    ORDER BY w.enqueue_time
    FOR UPDATE SKIP LOCKED
    LIMIT @max_rows
)
UPDATE dispatches d SET state = 'running', runner = @runner,
    run_deadline = now() + (d.timeout_ms * interval '1 millisecond'),
    lease_deadline = now() + (LEAST(d.heartbeat_ms, d.timeout_ms) * interval '1 millisecond')
FROM picked
WHERE d.org_id = @org_id AND d.id = picked.id
RETURNING d.*;

-- name: ExtendLeases :many
-- What a runner's report keeps alive, and which of it was asked to stop. What
-- the report lists and this doesn't return isn't the runner's any more.
UPDATE dispatches d SET
    lease_deadline = LEAST(now() + (d.heartbeat_ms * interval '1 millisecond'), d.run_deadline),
    epoch = GREATEST(d.epoch, (@epochs::bigint[])[array_position(@ids::text[], d.id)])
WHERE d.org_id = @org_id AND d.data_plane_id = @data_plane_id AND d.runner = @runner AND d.id = ANY(@ids::text[]) AND d.state = 'running'
RETURNING d.id, d.cancel;

-- name: FinishRunning :execrows
-- A runner's result, if the work is still its own.
UPDATE dispatches SET state = 'finished', outcome = @outcome, finish_time = now()
WHERE org_id = @org_id AND data_plane_id = @data_plane_id AND id = @id AND runner = @runner AND state = 'running';

-- name: IsFinishedByRunner :one
-- Whether the runner's result is in already, as when it sends it again.
SELECT EXISTS (
    SELECT 1 FROM dispatches
    WHERE org_id = @org_id AND data_plane_id = @data_plane_id AND id = @id AND runner = @runner AND state = 'finished'
);

-- name: ReplaceRunning :execrows
-- A runner with no room for what was placed on it: it's placed again.
UPDATE dispatches SET state = 'pending', runner = NULL, start_deadline = NULL, run_deadline = NULL, lease_deadline = NULL
WHERE org_id = @org_id AND data_plane_id = @data_plane_id AND id = @id AND runner = @runner AND state = 'running' AND light = '';

-- name: FinishDispatch :exec
-- The placer's own verdict on a pending attempt.
UPDATE dispatches SET state = 'finished', outcome = @outcome, finish_time = now()
WHERE org_id = @org_id AND id = @id AND state = 'pending';

-- name: CancelWaiting :execrows
-- A workflow's cancellation of work no runner has started.
UPDATE dispatches SET state = 'finished', outcome = @outcome, finish_time = now()
WHERE org_id = @org_id AND id = @id AND state IN ('pending', 'placed');

-- name: CancelRunning :exec
-- Its runner hears on its next report.
UPDATE dispatches SET cancel = true WHERE org_id = @org_id AND id = @id AND state = 'running';

-- name: ReturnUnstarted :execrows
-- Attempts whose runner didn't start them in time go back to wait for room,
-- keeping their place in the queue.
UPDATE dispatches SET state = 'pending', runner = NULL, start_deadline = NULL
WHERE org_id = @org_id AND state = 'placed' AND start_deadline <= now();

-- name: ExpireUnclaimed :execrows
-- Light operations no runner took in time.
UPDATE dispatches SET state = 'finished', outcome = @outcome, finish_time = now()
WHERE org_id = @org_id AND state = 'pending' AND light <> '' AND start_deadline <= now();

-- name: ExpireLeases :execrows
-- Running work whose runner stopped reporting it, or that ran out of time.
UPDATE dispatches SET state = 'finished', outcome = @outcome, finish_time = now()
WHERE org_id = @org_id AND state = 'running' AND lease_deadline <= now();

-- name: PendingDataPlanes :many
SELECT DISTINCT data_plane_id FROM dispatches WHERE org_id = @org_id AND state = 'pending' AND light = '';

-- name: UnwokenDispatches :many
SELECT id, workflow_id FROM dispatches
WHERE org_id = @org_id AND state = 'finished' AND NOT woken
ORDER BY finish_time
LIMIT @max_rows;

-- name: MarkWoken :exec
UPDATE dispatches SET woken = true WHERE org_id = @org_id AND id = @id;

-- name: DeleteFinishedDispatches :exec
-- Light operations go sooner: a log read's result is a page of a log.
DELETE FROM dispatches
WHERE org_id = @org_id AND state = 'finished' AND woken
    AND ((light = '' AND finish_time < @before) OR (light <> '' AND finish_time < @light_before));

-- name: PlacementCapacity :many
-- Runners reporting now by pool, for the placer's metrics.
SELECT r.data_plane_id, r.pool,
    sum(r.capacity_cpu)::bigint AS cpu, sum(r.capacity_memory)::bigint AS memory
FROM runners r
WHERE r.org_id = @org_id AND r.report_time >= now() - (sqlc.arg(lapse_ms)::bigint * interval '1 millisecond') AND NOT r.draining
GROUP BY r.data_plane_id, r.pool;

-- name: PlacementDemand :many
-- Attempts placed, running or waiting by pool and size, for the placer's
-- metrics.
SELECT data_plane_id, pool, size_cpu, size_memory, state,
    count(*)::bigint AS attempts,
    COALESCE(max(extract(epoch FROM now() - enqueue_time)), 0)::double precision AS oldest_seconds
FROM dispatches
WHERE org_id = @org_id AND light = '' AND state IN ('pending', 'placed', 'running')
GROUP BY data_plane_id, pool, size_cpu, size_memory, state;

-- name: TakeManualSync :one
-- Whether the API just started this pipeline's sync by hand. RETURNING sees
-- the row as updated, so the mark is read first.
WITH marked AS (
    SELECT p.id, p.manual_sync_time FROM pipelines p
    WHERE p.org_id = @org_id AND p.id = @id AND p.manual_sync_time IS NOT NULL
    FOR UPDATE
)
UPDATE pipelines u SET manual_sync_time = NULL
FROM marked
WHERE u.org_id = @org_id AND u.id = marked.id
RETURNING marked.manual_sync_time > now() - interval '5 minutes' AS manual;

-- name: MarkManualSync :exec
UPDATE pipelines SET manual_sync_time = now() WHERE org_id = @org_id AND id = @id;

-- name: ReturnPlaced :exec
-- A draining runner won't start what's placed on it, so it's placed again.
UPDATE dispatches SET state = 'pending', runner = NULL, start_deadline = NULL
WHERE org_id = @org_id AND data_plane_id = @data_plane_id AND runner = @runner AND state = 'placed';

-- name: GetDispatchWorkflow :one
SELECT workflow_id FROM dispatches WHERE org_id = @org_id AND id = @id;

-- name: RunnerDisconnected :exec
-- A runner whose long poll was dropped has probably died, so it stops counting
-- for placement until it reports again.
UPDATE runners SET report_time = LEAST(report_time, now() - (sqlc.arg(lapse_ms)::bigint * interval '1 millisecond'))
WHERE org_id = @org_id AND data_plane_id = @data_plane_id AND id = @id;
