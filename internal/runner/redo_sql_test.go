package runner

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// sqlLanesJob reads items, and writes by_id, keyed by id and exchanged by
// it before the writer, and names, which has no key, from it, into a
// destination whose writer lanes commit on their own.
func sqlLanesJob() *Job {
	return &Job{
		Streams: []Stream{{Name: "items", Read: source.Incremental, Mode: destination.Append, PrimaryKey: []string{"id"}}},
		Outputs: []Stream{
			{Name: "by_id", Mode: destination.Append, PrimaryKey: []string{"id"}, From: "items", Transformed: true},
			{Name: "names", Mode: destination.Append, From: "items", Transformed: true},
		},
		SQL:         &SQLTransform{SQL: "INSERT INTO by_id SELECT id, name FROM items; INSERT INTO names SELECT name FROM items"},
		Destination: laneCommits{lanesDest{8}},
		Lanes:       2,
	}
}

// A worker's committable names each of its splits once for each output
// whose rows of it its writer lane holds: an exchanged output's rows of the
// writer lane's key groups, from every split the worker read, and all of
// another output's rows from the splits its own lanes read.
func TestAWorkersShareNamesEachOutputsRows(t *testing.T) {
	j := sqlLanesJob()
	p := &workerPart{job: j, global: []int{3, 4}}
	plan := Plan{Lanes: 2, WriterLanes: 2, Exchanges: j.exchanges(5)}
	if !reflect.DeepEqual(plan.Exchanges, map[string][]string{"by_id": {"id"}}) {
		t.Fatalf("exchanges %v", plan.Exchanges)
	}
	e := &workerEpoch{descs: make([]destination.Staged, 2), ready: &Ready{}, splitLanes: map[checkpoint.SplitRef]splitRead{
		{Stream: "items", ID: "a"}: {lane: 0, from: []byte("1"), rows: 5},
		{Stream: "items", ID: "b"}: {lane: 1, rows: 7},
	}}
	s := p.share(1, 2, e, plan)
	got := map[uint32][]string{}
	for _, c := range s.GetCommittables() {
		got[c.GetWriterLane()] = refNames(c.GetSplits())
	}
	want := map[uint32][]string{
		3: {"items/a:0:5>by_id", "items/a:0:5>names", "items/b:0:7>by_id"},
		4: {"items/a:128:5>by_id", "items/b:128:7>by_id", "items/b:0:7>names"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("committables %v, want %v", got, want)
	}
	for _, c := range s.GetCommittables() {
		for _, r := range c.GetSplits() {
			if r.GetOutput() == "by_id" && !slices.Equal(r.GetKey(), []string{"id"}) {
				t.Errorf("writer lane %d's by_id names key %v", c.GetWriterLane(), r.GetKey())
			}
		}
	}
}

// sqlRecord is an epoch of items, splits a and b each read past where its
// part began, whose committable 0 holds by_id's key groups 0 to 127 of both
// and all of names' rows of a.
func sqlRecord() *checkpoint.Record {
	kg := &checkpoint.KeyGroupRange{First: 0, Last: 127}
	return &checkpoint.Record{
		Epoch: 4, Generation: 1,
		Streams: map[string]checkpoint.StreamState{"items": {Splits: []checkpoint.SplitState{{ID: "a", State: []byte("9")}, {ID: "b", State: []byte("19")}}}},
		Coverage: []checkpoint.Coverage{{Splits: []checkpoint.CoveredSplit{
			{Stream: "items", ID: "a", Output: "by_id", KeyGroups: kg, Key: []string{"id"}, From: []byte("4"), Rows: 5},
			{Stream: "items", ID: "a", Output: "names", From: []byte("4"), Rows: 5},
			{Stream: "items", ID: "b", Output: "by_id", KeyGroups: kg, Key: []string{"id"}, From: []byte("12"), Rows: 7},
		}}},
		Committables: [][]byte{nil},
	}
}

// A redo reads each split once, on a lane of the outputs whose rows of it
// the committable holds, and keeps an exchanged output's key groups: a
// split that reached the writer lane only through the exchange gives it
// none of names' rows.
func TestARedoReadsEachOutputsPart(t *testing.T) {
	part, err := sqlLanesJob().redoReads(sqlRecord(), 0)
	if err != nil {
		t.Fatal(err)
	}
	var reads []string
	for _, r := range part.reads {
		reads = append(reads, fmt.Sprintf("%s/%s@%s→%s on %d", r.stream, r.id, r.from, r.to.State, r.lane))
	}
	if want := []string{"items/a@4→9 on 0", "items/b@12→19 on 1"}; !slices.Equal(reads, want) {
		t.Errorf("reads %v, want %v", reads, want)
	}
	keeps := []map[string]bool{{"by_id": true, "names": true}, {"by_id": true}}
	if !reflect.DeepEqual(part.keeps, keeps) {
		t.Errorf("lanes keep %v, want %v", part.keeps, keeps)
	}
	if !reflect.DeepEqual(part.ranges, map[string]checkpoint.KeyGroupRange{"by_id": {First: 0, Last: 127}}) {
		t.Errorf("ranges %v", part.ranges)
	}

	// Without a part to read again, a split isn't read, and a lane keeps
	// nothing.
	rec := sqlRecord()
	rec.Coverage[0].Splits = rec.Coverage[0].Splits[2:]
	rec.Coverage[0].Splits[0].From = []byte("19")
	part, err = sqlLanesJob().redoReads(rec, 0)
	if err != nil || len(part.reads) != 0 || !reflect.DeepEqual(part.keeps, []map[string]bool{{}}) {
		t.Errorf("reads %v, keeps %v: %v", part.reads, part.keeps, err)
	}
}

// What the pipeline's SQL writes now must be what wrote the epoch: an
// output keyed otherwise, written from another stream, gone, or a SQL
// transform gone fails the redo with CONFIG_INVALID.
func TestARedoOfAnotherSQLIsRefused(t *testing.T) {
	for name, c := range map[string]struct {
		change func(*Job)
		says   string
	}{
		"rekeyed":    {change: func(j *Job) { j.Outputs[0].PrimaryKey = []string{"name"} }, says: "output \"by_id\" between writer lanes by its primary key (id), and the pipeline now keys it by (name)"},
		"other from": {change: func(j *Job) { j.Outputs[1].From = "other" }, says: "output \"names\" written from stream \"items\""},
		"gone":       {change: func(j *Job) { j.Outputs = j.Outputs[:1] }, says: "output \"names\" written from stream \"items\""},
		"no SQL":     {change: func(j *Job) { j.SQL, j.Outputs = nil, nil }, says: "the pipeline has no SQL transform now"},
	} {
		t.Run(name, func(t *testing.T) {
			j := sqlLanesJob()
			c.change(j)
			_, err := j.redoReads(sqlRecord(), 0)
			if errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(fmt.Sprint(err), c.says) {
				t.Errorf("%v (%s), want CONFIG_INVALID saying %q", err, errs.CodeOf(err), c.says)
			}
		})
	}
}

func idBatch(t *testing.T, ids ...int64) arrow.RecordBatch {
	t.Helper()
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(ids, nil)
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{col}, int64(len(ids)))
}

// A redo's lanes meet on its one writer: each lane's rows of the streams it
// keeps, the others' dropped, and each stream's barrier once every lane
// has passed it, after all their rows.
func TestARedosLanesMeetOnItsWriter(t *testing.T) {
	dropped := &releases{RecordBatch: idBatch(t, 9)}
	in := []Frame{
		{Kind: FrameBatch, Lane: 0, Stream: "by_id", Batch: idBatch(t, 1)},
		{Kind: FrameBatch, Lane: 1, Stream: "names", Batch: dropped},
		{Kind: FrameBarrier, Lane: 0, Stream: "by_id", Epoch: 4},
		{Kind: FrameBatch, Lane: 0, Stream: "names", Batch: idBatch(t, 2)},
		{Kind: FrameBarrier, Lane: 0, Stream: "names", Epoch: 4},
		{Kind: FrameBatch, Lane: 1, Stream: "by_id", Batch: idBatch(t, 3)},
		{Kind: FrameBarrier, Lane: 1, Stream: "names", Epoch: 4},
		{Kind: FrameBarrier, Lane: 1, Stream: "by_id", Epoch: 4},
		{Kind: FrameReady, Epoch: 4, Ready: &Ready{}},
	}
	from, to := make(chan Frame, len(in)), make(chan Frame, len(in))
	for _, f := range in {
		from <- f
	}
	close(from)
	if err := joinLanes(t.Context(), from, to, []map[string]bool{{"by_id": true, "names": true}, {"by_id": true}}); err != nil {
		t.Fatal(err)
	}
	var got []string
	for f := range to {
		s := fmt.Sprintf("%d %s on %d", f.Kind, f.Stream, f.Lane)
		if f.Batch != nil {
			s += fmt.Sprint(" ", f.Batch.Column(0).(*array.Int64).Int64Values())
			f.Batch.Release()
		}
		got = append(got, s)
	}
	b, r, ready := FrameBatch, FrameBarrier, FrameReady
	want := []string{
		fmt.Sprintf("%d by_id on 0 [1]", b), fmt.Sprintf("%d names on 0 [2]", b), fmt.Sprintf("%d by_id on 0 [3]", b),
		fmt.Sprintf("%d names on 0", r), fmt.Sprintf("%d by_id on 0", r), fmt.Sprintf("%d  on 0", ready),
	}
	if !slices.Equal(got, want) {
		t.Errorf("the writer got\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if dropped.n != 1 {
		t.Errorf("a dropped batch was released %d times", dropped.n)
	}
}
