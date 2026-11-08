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

-- A workflow's retry of the same work inserts nothing new.
-- name: InsertAttempt :exec
INSERT INTO attempts (org_id, id, pipeline_id, sync_id, attempt, size_cpu, size_memory, size_reason)
VALUES (@org_id, @id, @pipeline_id, @sync_id, @attempt, @size_cpu, @size_memory, @size_reason)
ON CONFLICT (org_id, id) DO NOTHING;

-- name: RecordAttemptUsage :exec
-- What runners' reports say of what they run. Reports may arrive out of order
-- across a result, so usage only grows.
UPDATE attempts a SET
    cpu_seconds = GREATEST(a.cpu_seconds, (@cpu_seconds::double precision[])[array_position(@ids::text[], a.id)]),
    peak_memory = GREATEST(a.peak_memory, (@peak_memory::bigint[])[array_position(@ids::text[], a.id)]),
    runner_cpu_seconds = GREATEST(a.runner_cpu_seconds, (@runner_cpu_seconds::double precision[])[array_position(@ids::text[], a.id)]),
    runner_memory = GREATEST(a.runner_memory, (@runner_memory::bigint[])[array_position(@ids::text[], a.id)]),
    seconds = GREATEST(a.seconds, (@seconds::double precision[])[array_position(@ids::text[], a.id)]),
    rows = GREATEST(a.rows, (@rows::bigint[])[array_position(@ids::text[], a.id)]),
    bytes = GREATEST(a.bytes, (@bytes::bigint[])[array_position(@ids::text[], a.id)]),
    lanes = GREATEST(a.lanes, (@lanes::integer[])[array_position(@ids::text[], a.id)]),
    lane_limit = GREATEST(a.lane_limit, (@lane_limit::integer[])[array_position(@ids::text[], a.id)]),
    busy_seconds = GREATEST(a.busy_seconds, (@busy_seconds::double precision[])[array_position(@ids::text[], a.id)]),
    backpressured_seconds = GREATEST(a.backpressured_seconds, (@backpressured_seconds::double precision[])[array_position(@ids::text[], a.id)]),
    idle_seconds = GREATEST(a.idle_seconds, (@idle_seconds::double precision[])[array_position(@ids::text[], a.id)]),
    lane_rows = GREATEST(a.lane_rows, (@lane_rows::bigint[])[array_position(@ids::text[], a.id)])
WHERE a.org_id = @org_id AND a.id = ANY(@ids::text[]);

-- name: SetAttemptCode :exec
UPDATE attempts SET code = @code WHERE org_id = @org_id AND id = @id;

-- name: RecentAttempts :many
-- The pipeline's latest attempts that say anything about its size: they ran,
-- or they failed. Newest first.
SELECT size_cpu, size_memory, cpu_seconds, peak_memory, runner_cpu_seconds, runner_memory, seconds, code
FROM attempts
WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND (peak_memory > 0 OR cpu_seconds > 0 OR code <> '')
ORDER BY create_time DESC
LIMIT @max_rows;

-- name: LastSyncAttempts :many
-- The attempts of the pipeline's latest scheduled sync that succeeded, in
-- order.
SELECT a.bytes, a.seconds, a.lanes, a.lane_limit FROM attempts a
WHERE a.org_id = @org_id AND a.sync_id = (
    SELECT s.id FROM syncs s
    WHERE s.org_id = @org_id AND s.pipeline_id = @pipeline_id AND s.status = 'succeeded' AND NOT s.continuous
    ORDER BY s.start_time DESC LIMIT 1
)
ORDER BY a.attempt, a.create_time;

-- name: SyncAttempts :many
-- The syncs' attempts, in order.
SELECT * FROM attempts
WHERE org_id = @org_id AND sync_id = ANY(@sync_ids::uuid[])
ORDER BY sync_id, attempt, create_time;

-- name: GetDispatchOutcome :one
SELECT state, outcome FROM dispatches WHERE org_id = @org_id AND id = @id;

-- name: LockPlacement :exec
-- Passes of a data plane run one at a time, until the transaction ends.
SELECT pg_advisory_xact_lock(hashtextextended(@data_plane::text, 0));

-- name: PlacementRunners :many
-- A data plane's runners and pod launchers reporting now, with the room their
-- placements use: a launcher's go on using it while their pods run them.
SELECT r.*,
    COALESCE(u.cpu, 0)::bigint AS used_cpu,
    COALESCE(u.memory, 0)::bigint AS used_memory,
    COALESCE(u.attempts, 0)::bigint AS attempts
FROM runners r
LEFT JOIN LATERAL (
    SELECT sum(d.size_cpu) AS cpu, sum(d.size_memory) AS memory, count(*) AS attempts
    FROM dispatches d
    WHERE d.org_id = r.org_id AND d.data_plane_id = r.data_plane_id AND (d.runner = r.id OR d.launcher = r.id) AND d.state IN ('placed', 'running')
) u ON true
WHERE r.org_id = @org_id AND r.data_plane_id = @data_plane_id AND r.attempt = ''
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
-- On a runner, or on a pod launcher with the time a pod takes to start.
UPDATE dispatches SET state = 'placed', runner = @runner, launcher = sqlc.narg('launcher'), placements = placements + 1,
    start_deadline = now() + (COALESCE(sqlc.narg('start_ms')::bigint, start_timeout_ms) * interval '1 millisecond')
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
    AND launcher IS NULL
RETURNING *;

-- name: ClaimLaunches :many
-- The attempts placed on this pod launcher that it hasn't made pods for.
UPDATE dispatches SET launched = true
WHERE org_id = @org_id AND data_plane_id = @data_plane_id AND launcher = @launcher AND runner = @launcher
    AND state = 'placed' AND NOT launched AND start_deadline > now() AND NOT cancel
RETURNING *;

-- name: ClaimLaunched :many
-- A pod's attempt, if it's still the pod's placement and in time: from now
-- it's the pod's to run.
UPDATE dispatches SET state = 'running', runner = @runner,
    run_deadline = now() + (timeout_ms * interval '1 millisecond'),
    lease_deadline = now() + (LEAST(heartbeat_ms, timeout_ms) * interval '1 millisecond')
WHERE org_id = @org_id AND data_plane_id = @data_plane_id AND id = @id AND placements = @placement
    AND launcher IS NOT NULL AND launched AND state = 'placed' AND start_deadline > now() AND NOT cancel
RETURNING *;

-- name: FinishLaunch :execrows
-- A launcher's word on an attempt whose pod couldn't be made or ended without
-- a result, if it's still that placement's.
UPDATE dispatches SET state = 'finished', outcome = @outcome, finish_time = now()
WHERE org_id = @org_id AND data_plane_id = @data_plane_id AND id = @id AND launcher = @launcher AND placements = @placement
    AND state IN ('placed', 'running');

-- name: ReturnLaunch :execrows
-- A launcher that couldn't make a pod for now: the attempt is placed again.
UPDATE dispatches SET state = 'pending', runner = NULL, launcher = NULL, launched = false, start_deadline = NULL
WHERE org_id = @org_id AND data_plane_id = @data_plane_id AND id = @id AND launcher = @launcher AND placements = @placement
    AND state = 'placed';

-- name: LiveLaunches :many
-- Which of a launcher's pods' placements are still waiting to start or
-- running. Across the org's data planes, so a launcher that sees another's
-- pods leaves them be.
SELECT id, placements FROM dispatches
WHERE org_id = @org_id AND id = ANY(@ids::text[]) AND launcher IS NOT NULL AND state IN ('placed', 'running');

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
RETURNING d.id, d.cancel, d.drain;

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
-- A runner with no room for what was placed on it: it's placed again, unless
-- it's been cancelled, which finishes it.
UPDATE dispatches SET
    state = CASE WHEN cancel THEN 'finished' ELSE 'pending' END,
    outcome = CASE WHEN cancel THEN sqlc.arg(cancelled)::jsonb ELSE outcome END,
    finish_time = CASE WHEN cancel THEN now() ELSE finish_time END,
    runner = NULL, launcher = NULL, launched = false, start_deadline = NULL, run_deadline = NULL, lease_deadline = NULL
WHERE org_id = @org_id AND data_plane_id = @data_plane_id AND id = @id AND runner = @runner AND state = 'running' AND light = '';

-- name: FinishDispatch :exec
-- The placer's own verdict on a pending attempt.
UPDATE dispatches SET state = 'finished', outcome = @outcome, finish_time = now()
WHERE org_id = @org_id AND id = @id AND state = 'pending';

-- name: CancelDispatch :exec
-- A workflow's cancellation. What no runner has started finishes now, and a
-- runner hears of what it's running on its next report. One statement, so work
-- going from one state to the other meanwhile is seen in the state it ends up in.
UPDATE dispatches SET
    state = CASE WHEN state = 'running' THEN state ELSE 'finished' END,
    outcome = CASE WHEN state = 'running' THEN outcome ELSE sqlc.arg(outcome)::jsonb END,
    finish_time = CASE WHEN state = 'running' THEN finish_time ELSE now() END,
    cancel = true
WHERE org_id = @org_id AND id = @id AND state IN ('pending', 'placed', 'running');

-- name: RescaleWaiting :execrows
-- A rescale's: the pipeline's attempts no runner has started finish as
-- drained, so their workflows send them again at once, sized again.
UPDATE dispatches d SET state = 'finished', outcome = @outcome, finish_time = now()
FROM attempts a
WHERE d.org_id = @org_id AND a.org_id = d.org_id AND a.id = d.id AND a.pipeline_id = @pipeline_id
  AND d.state IN ('pending', 'placed');

-- name: RescaleRunning :execrows
-- A rescale's: the pipeline's running attempts stop at a checkpoint and end
-- as drained, as their runners hear on their next reports.
UPDATE dispatches d SET drain = true
FROM attempts a
WHERE d.org_id = @org_id AND a.org_id = d.org_id AND a.id = d.id AND a.pipeline_id = @pipeline_id
  AND d.state = 'running' AND NOT d.cancel AND (sqlc.narg('id')::text IS NULL OR d.id = sqlc.narg('id'));

-- name: ReturnUnstarted :execrows
-- Attempts whose runner or pod didn't start them in time go back to wait for
-- room, keeping their place in the queue.
UPDATE dispatches SET state = 'pending', runner = NULL, launcher = NULL, launched = false, start_deadline = NULL
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
    AND NOT r.launcher AND r.attempt = ''
GROUP BY r.data_plane_id, r.pool;

-- name: PlacementDemand :many
-- Attempts placed, running or waiting by pool and size, and whether a
-- launcher's pod has them, for the placer's metrics.
SELECT data_plane_id, pool, size_cpu, size_memory, state, (launcher IS NOT NULL)::boolean AS pod,
    count(*)::bigint AS attempts,
    COALESCE(max(extract(epoch FROM now() - enqueue_time)), 0)::double precision AS oldest_seconds
FROM dispatches
WHERE org_id = @org_id AND light = '' AND state IN ('pending', 'placed', 'running')
GROUP BY data_plane_id, pool, size_cpu, size_memory, state, launcher IS NOT NULL;

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
-- A draining runner won't start what's placed on it, so it's placed again;
-- but not what a launcher made a pod for, which the pod may start.
UPDATE dispatches SET state = 'pending', runner = NULL, launcher = NULL, start_deadline = NULL
WHERE org_id = @org_id AND data_plane_id = @data_plane_id AND runner = @runner AND state = 'placed' AND NOT launched;

-- name: GetDispatchWorkflow :one
SELECT workflow_id FROM dispatches WHERE org_id = @org_id AND id = @id;

-- name: RunnerDisconnected :exec
-- A runner whose long poll was dropped has probably died, so it stops counting
-- for placement until it reports again.
UPDATE runners SET report_time = LEAST(report_time, now() - (sqlc.arg(lapse_ms)::bigint * interval '1 millisecond'))
WHERE org_id = @org_id AND data_plane_id = @data_plane_id AND id = @id;

-- name: LatestAttempt :one
SELECT id, attempt, create_time FROM attempts
WHERE org_id = @org_id AND sync_id = @sync_id
ORDER BY attempt DESC, create_time DESC
LIMIT 1;

-- name: UnfinishedWorkOfSyncs :many
-- What the syncs have running or waiting, their post-sync steps included.
SELECT id FROM dispatches
WHERE org_id = @org_id AND state <> 'finished' AND input->>'SyncID' = ANY(@sync_ids::text[]);
