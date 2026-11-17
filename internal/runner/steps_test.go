package runner

import (
	"log/slog"
	"reflect"
	"testing"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// laneSteps are a keyed lane's steps of key groups first to last: of split
// "p0" at place 4, and "p1" at 2 and 8.
func laneSteps(first, last int, wm int64) checkpoint.LaneSteps {
	return checkpoint.LaneSteps{KeyGroups: checkpoint.KeyGroupRange{First: first, Last: last}, Splits: []checkpoint.SplitSteps{
		{Split: "p0", Steps: []checkpoint.Step{{Place: 4, Watermark: wm}}},
		{Split: "p1", Steps: []checkpoint.Step{{Place: 2, Watermark: wm}, {Place: 8, Watermark: wm + 10}}},
	}}
}

// The coordinator keeps each window's steps over a stream in the record's
// stream, every engine's together, and leaves out another stream's and an
// operator it doesn't know. A redo of a committable gives the engine the
// steps of the keyed lanes in its key groups.
func TestARecordKeepsTheStepsARedoReplays(t *testing.T) {
	floor, closed := int64(100), int64(300)
	j := &Job{SQL: &SQLTransform{Expected: []ExpectedOutput{
		{Name: "tens", Input: "s", Operator: "w1"},
		{Name: "other", Input: "t", Operator: "w2"},
	}}}
	c := &coordinator{job: j, log: slog.New(slog.DiscardHandler)}
	rec := &checkpoint.Record{Epoch: 5, Streams: map[string]checkpoint.StreamState{"s": {Floor: &floor, Closed: &closed}, "t": {}}}
	rec.SetSplits("s", []checkpoint.SplitState{{ID: "p0", State: []byte("1")}, {ID: "p1", State: []byte("1")}})
	parts := []checkpoint.OperatorPart{
		{Operator: "w1", Steps: []checkpoint.LaneSteps{laneSteps(0, 63, 150)}},
		{Operator: "w1", Steps: []checkpoint.LaneSteps{laneSteps(128, 255, 170)}},
		{Operator: "w1"},
		{Operator: "w2", Steps: []checkpoint.LaneSteps{laneSteps(0, 63, 150)}},
		{Operator: "gone", Steps: []checkpoint.LaneSteps{laneSteps(0, 63, 150)}},
	}
	c.recordSteps(rec, "s", floor, parts)
	if steps := rec.Streams["s"].Steps; len(steps) != 1 || len(steps["w1"]) == 0 {
		t.Fatalf("stream s keeps steps %v", steps)
	}
	for _, kg := range []struct {
		first, last int
		want        []checkpoint.LaneSteps
	}{
		{0, 127, []checkpoint.LaneSteps{laneSteps(0, 63, 150)}},
		{128, 255, []checkpoint.LaneSteps{laneSteps(128, 255, 170)}},
		{64, 127, nil},
	} {
		rec.Coverage = []checkpoint.Coverage{{KeyGroups: &checkpoint.KeyGroupRange{First: kg.first, Last: kg.last}}}
		got, err := j.redoSteps(rec, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got["w1"], kg.want) || len(got) > 1 {
			t.Errorf("key groups %d to %d: steps %+v, want %+v", kg.first, kg.last, got, kg.want)
		}
	}
	// A committable without key groups holds no window's rows.
	rec.Coverage = []checkpoint.Coverage{{}}
	if got, err := j.redoSteps(rec, 0); got != nil || err != nil {
		t.Errorf("without key groups: %v, %v", got, err)
	}
	// Steps the record can't decode are a corrupt checkpoint.
	rec.Coverage = []checkpoint.Coverage{{KeyGroups: &checkpoint.KeyGroupRange{First: 0, Last: 255}}}
	st := rec.Streams["s"]
	st.Steps["w1"] = st.Steps["w1"][:3]
	if _, err := j.redoSteps(rec, 0); errs.CodeOf(err) != errs.CheckpointCorrupt {
		t.Errorf("cut short: %v", err)
	}
}

// Only a worker, whose destination's lanes commit on their own, records
// the steps, and only of a SQL transform that keeps state.
func TestOnlyAWorkerOfLaneCommitsRecordsSteps(t *testing.T) {
	sql := &SQLTransform{Operators: []string{"w1"}}
	for _, c := range []struct {
		writers int
		sql     *SQLTransform
		lanes   bool
		want    bool
	}{
		{writers: 2, sql: sql, lanes: true, want: true},
		{writers: 0, sql: sql, lanes: true},
		{writers: 2, sql: &SQLTransform{}, lanes: true},
		{writers: 2, sql: sql},
	} {
		j := &Job{SQL: c.sql, writers: c.writers, Destination: capsDest{caps: destination.Capabilities{LaneCommits: c.lanes}}}
		if got := j.recordsSteps(); got != c.want {
			t.Errorf("%d writers, %d operators, lane commits %v: records steps %v", c.writers, len(c.sql.Operators), c.lanes, got)
		}
	}
}
