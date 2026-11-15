package runner

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"

	attemptv1 "github.com/openetl-io/openetl/gen/openetl/attempt/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/runner/attempt"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// meshingFlow is a dataflow whose engines exchange rows between workers,
// for laying attempts out; it runs nothing.
type meshingFlow struct{ Passthrough }

func (meshingFlow) Meshes() bool { return true }

type lanesDest struct{ lanes int }

func (d lanesDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.StageAndApply, WriterLanes: d.lanes,
		WriteModes: []destination.WriteMode{destination.Append, destination.Merge}}
}

func (lanesDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return nil, errors.New("not opened")
}

func meshJob(flow Dataflow) *Job {
	return &Job{
		Streams: []Stream{
			{Name: "items", Read: source.Incremental, Mode: destination.Merge, PrimaryKey: []string{"id"}},
			{Name: "changes", Read: source.CDC, Mode: destination.Merge, PrimaryKey: []string{"id"}},
			{Name: "logs", Read: source.Incremental, Mode: destination.Append},
		},
		Destination: lanesDest{8}, Dataflow: flow, Lanes: 4,
	}
}

func splitsOf(n int) []checkpoint.SplitState {
	out := make([]checkpoint.SplitState, n)
	for i := range out {
		out[i] = checkpoint.SplitState{ID: strconv.Itoa(i)}
	}
	return out
}

// readers are the workers that read each stream's splits.
func readers(a *attemptv1.Assignment) map[string]map[uint32]bool {
	out := map[string]map[uint32]bool{}
	for _, s := range a.GetSplits() {
		name := s.GetSplit().GetStream()
		if out[name] == nil {
			out[name] = map[uint32]bool{}
		}
		out[name][a.GetLanes()[s.GetLane()].GetWorker()] = true
	}
	return out
}

// Where the workers' engines exchange rows, a keyed stream whose splits
// don't partition it by key spreads over them, and its rows meet on the
// writer lane owning their key group in whichever worker: every lane and
// writer lane has its key groups in the attempt's layout. Streams read with
// cdc still share one worker. A dataflow that can't exchange between
// workers keeps such a stream on one.
func TestLayoutExchangesBetweenWorkers(t *testing.T) {
	splits := map[string][]checkpoint.SplitState{"items": splitsOf(6), "changes": {{ID: checkpoint.WholeStream}}, "logs": splitsOf(3)}
	j := meshJob(meshingFlow{})
	if !j.meshed(3) || j.meshed(1) {
		t.Fatal("an attempt on 3 workers with a keyed stream exchanges between them, and on 1 doesn't")
	}
	a, err := j.layout(3, splits, map[string]bool{}, false)
	if err != nil {
		t.Fatal(err)
	}
	r := readers(a)
	if len(r["items"]) != 3 || len(r["changes"]) != 1 {
		t.Errorf("items read by workers %v, changes by %v", r["items"], r["changes"])
	}
	lanes, writers := map[uint32]int{}, map[uint32]int{}
	for _, l := range a.GetLanes() {
		lanes[l.GetWorker()]++
	}
	for _, l := range a.GetWriterLanes() {
		writers[l.GetWorker()]++
	}
	var ls, ws []int
	for w := range uint32(3) {
		ls, ws = append(ls, lanes[w]), append(ws, writers[w])
	}
	want, err := plan.NewWorkerLayout(256, ls, ws)
	if err != nil {
		t.Fatal(err)
	}
	for i, l := range a.GetLanes() {
		if kg := l.GetKeyGroups(); kg == nil || int(kg.GetFirst()) != want.Lanes[i].First || int(kg.GetLast()) != want.Lanes[i].Last {
			t.Errorf("lane %d owns %v, want %v", i, kg, want.Lanes[i])
		}
	}
	for w, l := range a.GetWriterLanes() {
		if kg := l.GetKeyGroups(); kg == nil || int(kg.GetFirst()) != want.Writers[w].First || int(kg.GetLast()) != want.Writers[w].Last {
			t.Errorf("writer lane %d owns %v, want %v", w, kg, want.Writers[w])
		}
	}

	a, err = meshJob(Passthrough{}).layout(3, splits, map[string]bool{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if r := readers(a); len(r["items"]) != 1 || a.GetLanes()[0].GetKeyGroups() != nil {
		t.Errorf("without the exchange between workers, items read by %v, and lane 0 owns %v", r["items"], a.GetLanes()[0].GetKeyGroups())
	}
}

// A worker has no more lanes than its writer lanes have key groups.
func TestLanesStayWithinTheirKeyGroups(t *testing.T) {
	j := meshJob(meshingFlow{})
	j.KeyGroups, j.Lanes, j.Destination = 4, 4, lanesDest{4}
	a, err := j.layout(4, map[string][]checkpoint.SplitState{"items": splitsOf(16)}, map[string]bool{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.GetLanes()) != 4 || len(a.GetWriterLanes()) != 4 {
		t.Errorf("%d lanes and %d writer lanes over 4 key groups", len(a.GetLanes()), len(a.GetWriterLanes()))
	}
}

// A writer lane's committable holds its key groups of every split of an
// exchanged stream that any worker read in the epoch, as any lane sends
// their rows to any writer lane; other streams' splits stay where they were.
func TestCoverageCrossesWorkers(t *testing.T) {
	r0, r1 := attempt.KeyGroups(0, 127), attempt.KeyGroups(128, 255)
	f := &fleet{writers: 2, ranges: []plan.KeyGroupRange{{First: 0, Last: 127}, {First: 128, Last: 255}}, crossing: map[string][]string{"items": {"id"}}}
	refs := [][]*attemptv1.SplitRef{
		{{Stream: "items", Split: "a", KeyGroups: r0, Key: []string{"id"}, Rows: 5}, {Stream: "logs", Split: "x", Rows: 1}},
		{{Stream: "items", Split: "b", KeyGroups: r1, Key: []string{"id"}, Rows: 7}},
	}
	f.cross(refs)
	got := func(w int) []string {
		var out []string
		for _, r := range refs[w] {
			out = append(out, r.GetStream()+"/"+r.GetSplit()+":"+strconv.Itoa(int(r.GetKeyGroups().GetFirst()))+":"+strconv.FormatUint(r.GetRows(), 10))
		}
		return out
	}
	if w0, w1 := got(0), got(1); !slices.Equal(w0, []string{"items/a:0:5", "logs/x:0:1", "items/b:0:7"}) || !slices.Equal(w1, []string{"items/b:128:7", "items/a:128:5"}) {
		t.Errorf("writer lane 0 holds %v, writer lane 1 %v", w0, w1)
	}
	if !slices.Equal(refs[1][1].GetKey(), []string{"id"}) {
		t.Errorf("a split from another worker has key %v", refs[1][1].GetKey())
	}
}

// A worker's engine takes its part of the attempt's layout, and the
// others' lanes, writer lanes and addresses.
func TestAWorkersMeshIsItsAssignments(t *testing.T) {
	a := &attemptv1.Assignment{
		Workers:     []string{"10.0.0.1:7720", "10.0.0.2:7720"},
		Lanes:       []*attemptv1.Lane{{Worker: 0}, {Worker: 1}, {Worker: 1}, {Worker: 1}},
		WriterLanes: []*attemptv1.Lane{{Worker: 0}, {Worker: 1}, {Worker: 1}},
	}
	p := &workerPart{t: attempt.Target{Worker: 1, Exchange: "10.0.0.2:7720", Listen: ":7720"}}
	m := p.mesh(a, &attemptv1.Exchange{Certificate: []byte("c"), Key: []byte("k"), Secret: []byte("s")})
	want := []MeshEngine{{Address: "10.0.0.1:7720", Lanes: 1, WriterLanes: 1}, {Address: "10.0.0.2:7720", Lanes: 3, WriterLanes: 2}}
	if m.Engine != 1 || m.Listen != ":7720" || !slices.Equal(m.Engines, want) || string(m.Secret) != "s" {
		t.Fatalf("%+v", m)
	}
	l, err := Plan{Lanes: 3, WriterLanes: 2, Mesh: m}.layout()
	if err != nil {
		t.Fatal(err)
	}
	all, _ := plan.NewWorkerLayout(256, []int{1, 3}, []int{1, 2})
	if !slices.Equal(l.Writers, all.Writers[1:]) || !slices.Equal(l.Lanes, all.Lanes[1:]) || !slices.Equal(l.WriterOf, []int{0, 0, 1}) {
		t.Errorf("worker 1's layout %+v of %+v", l, all)
	}
	if err := (Passthrough{}).Run(t.Context(), Plan{Mesh: m}, nil, make(chan Frame)); err == nil {
		t.Error("a dataflow without the exchange between engines ran a plan with one")
	}
}
