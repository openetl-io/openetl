-- name: GetJobScale :one
SELECT * FROM job_scales WHERE org_id = @org_id AND pipeline_id = @pipeline_id;

-- The pipeline's row, made if it has none, locked until the transaction ends.
-- name: LockJobScale :one
INSERT INTO job_scales (org_id, pipeline_id) VALUES (@org_id, @pipeline_id)
ON CONFLICT (org_id, pipeline_id) DO UPDATE SET org_id = excluded.org_id
RETURNING *;

-- name: SetScaleSize :exec
-- RescalePipeline's size for a version; 0 gives the spec back its say.
UPDATE job_scales SET size_cpu = @size_cpu, size_memory = @size_memory, size_version = @size_version, rescale_time = now()
WHERE org_id = @org_id AND pipeline_id = @pipeline_id;

-- name: SetScaleLanes :exec
-- The job loop's rescale.
UPDATE job_scales SET lanes = @lanes, note = '', rescale_time = now()
WHERE org_id = @org_id AND pipeline_id = @pipeline_id;

-- name: SetScaleNote :exec
UPDATE job_scales SET note = @note WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND note <> @note;

-- name: SampleLanes :execrows
-- Each running stream job's lanes as its runner last reported them, unless
-- it was sampled less than @every_seconds ago.
INSERT INTO lane_samples (org_id, attempt_id, pipeline_id, lanes, lane_limit, busy_seconds, backpressured_seconds, idle_seconds, lane_rows)
SELECT a.org_id, a.id, a.pipeline_id, a.lanes, a.lane_limit, a.busy_seconds, a.backpressured_seconds, a.idle_seconds, a.lane_rows
FROM dispatches d
JOIN attempts a ON a.org_id = d.org_id AND a.id = d.id
WHERE d.org_id = @org_id AND d.state = 'running' AND d.kind = @kind AND a.lanes > 0
  AND NOT EXISTS (
    SELECT 1 FROM lane_samples s
    WHERE s.org_id = a.org_id AND s.attempt_id = a.id AND s.sample_time > now() - make_interval(secs => @every_seconds::float8)
  );

-- name: PruneLaneSamples :exec
DELETE FROM lane_samples WHERE org_id = @org_id AND sample_time < @before;

-- name: ScalingCandidates :many
-- Running stream jobs that report lanes and aren't stopping, with what
-- rescaled their pipelines.
SELECT a.id, a.pipeline_id, a.lanes AS attempt_lanes, s.version,
    COALESCE(j.size_cpu, 0)::bigint AS scale_cpu,
    COALESCE(j.size_version, '')::text AS scale_version,
    COALESCE(j.lanes, 0)::integer AS scale_lanes,
    COALESCE(j.note, '')::text AS note,
    j.rescale_time
FROM dispatches d
JOIN attempts a ON a.org_id = d.org_id AND a.id = d.id
JOIN syncs s ON s.org_id = a.org_id AND s.id = a.sync_id
LEFT JOIN job_scales j ON j.org_id = a.org_id AND j.pipeline_id = a.pipeline_id
WHERE d.org_id = @org_id AND d.state = 'running' AND d.kind = @kind AND NOT d.drain AND NOT d.cancel AND a.lanes > 0;

-- name: AttemptLaneSamples :many
-- Oldest first.
SELECT sample_time, lanes, lane_limit, busy_seconds, backpressured_seconds, idle_seconds, lane_rows
FROM lane_samples
WHERE org_id = @org_id AND attempt_id = @attempt_id
ORDER BY sample_time;

-- name: RecentProgress :many
-- What the pipeline's checkpoints since @since said of its read's lag, oldest
-- first.
SELECT commit_time, progress FROM checkpoint_mirror
WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND commit_time >= @since AND progress IS NOT NULL
ORDER BY commit_time;
