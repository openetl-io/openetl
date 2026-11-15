package runner

import (
	"log/slog"
	"slices"

	"github.com/openetl-io/openetl/internal/checkpoint"
)

// restoring is rec as an attempt restores it, on one process or on
// workers: without what resets newer than it drop, and without the state of
// operators the version changed or dropped. reset is the newest reset it
// then carries, and streams the streams that reset reads again.
func (j *Job) restoring(rec *checkpoint.Record, log *slog.Logger) (restored *checkpoint.Record, reset string, streams []string) {
	restored, reset, streams = j.reset(rec)
	if len(streams) > 0 {
		log.Info("reading streams again from the start", "streams", streams, "reset", reset)
	}
	restored, dropped := j.withoutDropped(restored)
	if len(dropped) > 0 {
		log.Info("dropping the state of operators the pipeline's version changed or dropped", "operators", dropped)
	}
	return restored, reset, streams
}

// withoutDropped returns rec without the state of the operators the
// pipeline's version changed or dropped, as a copy, and those operators.
// That state can't serve the
// operators that took their place. The engine's records then stop naming
// it, and retention deletes its runs once no kept record does. The state of
// another operator the job doesn't keep stays, and the engine refuses it:
// what that operator keeps changed under the version, as when a column it
// groups by changes type, and only a reset of its streams drops it.
func (j *Job) withoutDropped(rec *checkpoint.Record) (*checkpoint.Record, []string) {
	if rec == nil || j.SQL == nil || len(j.SQL.Dropped) == 0 {
		return rec, nil
	}
	var dropped []string
	kept := slices.DeleteFunc(slices.Clone(rec.Snapshots), func(s checkpoint.Snapshot) bool {
		drop := slices.Contains(j.SQL.Dropped, s.Operator) && !slices.Contains(j.SQL.Operators, s.Operator)
		if drop && !slices.Contains(dropped, s.Operator) {
			dropped = append(dropped, s.Operator)
		}
		return drop
	})
	if len(dropped) == 0 {
		return rec, nil
	}
	cp := *rec
	cp.Snapshots = kept
	return &cp, dropped
}
