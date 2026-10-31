-- name: InsertMirroredCheckpoint :execrows
INSERT INTO checkpoint_mirror (org_id, pipeline_id, epoch, generation, previous_epoch, previous_generation, run_id, commit_time, streams)
VALUES (@org_id, @pipeline_id, @epoch, @generation, @previous_epoch, @previous_generation, @run_id, @commit_time, @streams)
ON CONFLICT DO NOTHING;

-- name: ListCheckpoints :many
SELECT * FROM checkpoint_mirror WHERE org_id = @org_id AND pipeline_id = @pipeline_id
ORDER BY epoch DESC, generation DESC LIMIT @max_rows;
