package runner

import (
	"fmt"
	"math"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

// recordsSteps says the attempt's group windows record the steps of their
// watermarks in each epoch: a worker's, whose destination's writer lanes
// commit on their own, so that a redo of one writer lane's part of an
// epoch can replay them.
func (j *Job) recordsSteps() bool {
	return j.writers > 0 && j.keepsState() && j.Destination.Capabilities().LaneCommits
}

// windowInput is the stream a stateful operator's statement reads; empty
// for none.
func (j *Job) windowInput(op string) string {
	if j.SQL == nil {
		return ""
	}
	for _, e := range j.SQL.Expected {
		if e.Operator == op {
			return e.Input
		}
	}
	return ""
}

// recordSteps keeps in rec the steps of the watermarks of the group windows
// over stream name that parts carry, which started the epoch at floor.
// Steps it can't keep are left out: a redo of the epoch then takes their
// rows as having met the floor.
func (c *coordinator) recordSteps(rec *checkpoint.Record, name string, floor int64, parts []checkpoint.OperatorPart) {
	by := map[string][]checkpoint.LaneSteps{}
	for _, p := range parts {
		if len(p.Steps) > 0 && c.job.windowInput(p.Operator) == name {
			by[p.Operator] = append(by[p.Operator], p.Steps...)
		}
	}
	if len(by) == 0 {
		return
	}
	var ids []string
	for _, s := range rec.Splits(name) {
		ids = append(ids, s.ID)
	}
	st := rec.Streams[name]
	for op, lanes := range by {
		b, err := checkpoint.EncodeSteps(lanes, ids, floor)
		if err != nil {
			c.log.Warn("a group window's watermark steps are left out of the record", "stream", name, "operator", op, "epoch", rec.Epoch, "err", err)
			continue
		}
		if b != nil {
			st.Steps = setIn(st.Steps, op, b)
		}
	}
	rec.Streams[name] = st
}

// redoSteps are the steps of rec's group windows' watermarks that a redo of
// committable i replays, by operator: those of the keyed lanes whose key
// groups the committable holds.
func (j *Job) redoSteps(rec *checkpoint.Record, i int) (map[string][]checkpoint.LaneSteps, error) {
	kg := rec.Coverage[i].KeyGroups
	if kg == nil {
		return nil, nil
	}
	var out map[string][]checkpoint.LaneSteps
	for name, st := range rec.Streams {
		floor := int64(math.MinInt64)
		if st.Floor != nil {
			floor = *st.Floor
		}
		var ids []string
		for _, s := range rec.Splits(name) {
			ids = append(ids, s.ID)
		}
		for op, b := range st.Steps {
			lanes, err := checkpoint.DecodeSteps(b, ids, floor)
			if err != nil {
				return nil, errs.Wrap(errs.CheckpointCorrupt, err, fmt.Sprintf("runner: epoch %d's record of operator %s's steps over stream %q", rec.Epoch, op, name))
			}
			for _, l := range lanes {
				if l.KeyGroups.Last < kg.First || l.KeyGroups.First > kg.Last {
					continue
				}
				if out == nil {
					out = map[string][]checkpoint.LaneSteps{}
				}
				out[op] = append(out[op], l)
			}
		}
	}
	return out, nil
}
