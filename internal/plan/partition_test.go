package plan

import (
	"fmt"
	"slices"
	"testing"

	"google.golang.org/protobuf/proto"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

func splits(cols ...string) Partitioning    { return Partitioning{Kind: BySplits, Columns: cols} }
func keyGroups(cols ...string) Partitioning { return Partitioning{Kind: ByKeyGroup, Columns: cols} }
func needs(cols ...string) Need             { return Need{Columns: cols} }

// A partitioning by columns K meets a need for columns J when K is a subset
// of J.
func TestTheSubsetRule(t *testing.T) {
	for _, tc := range []struct {
		p    Partitioning
		n    Need
		want bool
	}{
		{Partitioning{}, needs(), true},
		{Partitioning{}, needs("id"), false},
		{splits("id"), needs("id"), true},
		{splits("id"), needs("id", "at"), true},
		{splits("id"), needs("at"), false},
		{keyGroups("k"), needs("window_start", "k"), true},
		{keyGroups("k", "x"), needs("k"), false},
		{keyGroups("a", "b"), needs("b", "c", "a"), true},
		{Partitioning{Kind: OneLane}, needs("id"), true},
		{Partitioning{Kind: ByKeyGroup}, needs("id"), false},
		// State by key group needs lanes that own key groups.
		{splits("k"), Need{Columns: []string{"k"}, KeyGroups: true}, false},
		{keyGroups("k"), Need{Columns: []string{"k", "x"}, KeyGroups: true}, true},
		{keyGroups("k"), Need{KeyGroups: true}, false},
		{Partitioning{Kind: OneLane}, Need{KeyGroups: true}, true},
	} {
		if got := tc.p.Meets(tc.n); got != tc.want {
			t.Errorf("%v meets %v: %v, want %v", tc.p, tc.n.Columns, got, tc.want)
		}
	}
}

// A group-window statement over its group keys k, whose output, keyed by
// (window_start, k), passes k through, as StatementOperator gives it.
func aggregate(groupKeys ...string) Operator {
	passes := map[string]string{}
	for _, k := range groupKeys {
		passes[k] = k
	}
	return StatementOperator(sqlplan.Statement{At: sqlplan.Position{Line: 3, Column: 1}, Need: groupKeys, Passes: passes, Operator: "window-1"})
}

func rowWise(passes map[string]string) Operator {
	return Operator{Kind: OperatorStatement, At: sqlplan.Position{Line: 1, Column: 1}, Passes: passes}
}

func TestPlace(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source Partitioning
		ops    []Operator
		// Where exchanges sit, and each operator's input after them.
		exchanges []int
		in        []Partitioning
	}{
		{name: "splits by key", source: splits("id"), ops: []Operator{WriterOperator([]string{"id"}, false)},
			in: []Partitioning{splits("id")}},
		{name: "unpartitioned and keyed", source: Partitioning{}, ops: []Operator{WriterOperator([]string{"id"}, false)},
			exchanges: []int{0}, in: []Partitioning{keyGroups("id")}},
		{name: "keyless", source: Partitioning{}, ops: []Operator{WriterOperator(nil, false)},
			in: []Partitioning{{}}},
		{name: "one writer lane", source: Partitioning{}, ops: []Operator{WriterOperator([]string{"id"}, true)},
			in: []Partitioning{{Kind: OneLane}}},
		{name: "a rename keeps it", source: splits("id"),
			ops: []Operator{rowWise(map[string]string{"id": "uid", "email": "email"}), WriterOperator([]string{"uid"}, false)},
			in:  []Partitioning{splits("id"), splits("uid")}},
		{name: "a computed key ends it", source: splits("id"),
			ops:       []Operator{rowWise(map[string]string{"email": "email"}), WriterOperator([]string{"k"}, false)},
			exchanges: []int{1}, in: []Partitioning{splits("id"), keyGroups("k")}},
		{name: "an aggregate reaches its writer with one exchange", source: Partitioning{},
			ops:       []Operator{aggregate("k"), WriterOperator([]string{"window_start", "k"}, false)},
			exchanges: []int{0}, in: []Partitioning{keyGroups("k"), keyGroups("k")}},
		{name: "an aggregate keeps state by key group, so splits by its keys don't meet it", source: splits("k"),
			ops:       []Operator{aggregate("k"), WriterOperator([]string{"window_start", "k"}, false)},
			exchanges: []int{0}, in: []Partitioning{keyGroups("k"), keyGroups("k")}},
		{name: "an aggregate without keys runs on one lane", source: Partitioning{},
			ops:       []Operator{aggregate(), WriterOperator([]string{"window_start"}, false)},
			exchanges: []int{0}, in: []Partitioning{{Kind: OneLane}, {Kind: OneLane}}},
		{name: "an aggregate over a stream split by more than its keys", source: splits("k", "x"),
			ops:       []Operator{aggregate("k"), WriterOperator([]string{"window_start", "k"}, false)},
			exchanges: []int{0}, in: []Partitioning{keyGroups("k"), keyGroups("k")}},
		{name: "a statement that re-keys exchanges again", source: Partitioning{},
			ops: []Operator{aggregate("k"), rowWise(map[string]string{"window_start": "window_start"}),
				WriterOperator([]string{"window_start"}, false)},
			exchanges: []int{0, 2}, in: []Partitioning{keyGroups("k"), keyGroups("k"), keyGroups("window_start")}},
		{name: "an aggregate into one writer lane", source: Partitioning{},
			ops:       []Operator{aggregate("k"), WriterOperator([]string{"window_start", "k"}, true)},
			exchanges: []int{0}, in: []Partitioning{keyGroups("k"), {Kind: OneLane}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := Place(tc.source, tc.ops...)
			var in []Partitioning
			for _, s := range p.Steps {
				in = append(in, s.In)
			}
			if !slices.Equal(p.Exchanges(), tc.exchanges) || fmt.Sprint(in) != fmt.Sprint(tc.in) {
				t.Errorf("exchanges before %v, inputs %v; want %v, %v", p.Exchanges(), in, tc.exchanges, tc.in)
			}
		})
	}
}

// A source's partitioning: by its key where its splits partition it by
// key, but never a changelog's.
func TestSourcePartitioning(t *testing.T) {
	key := []string{"id"}
	if p := SourcePartitioning(key, true, false); fmt.Sprint(p) != fmt.Sprint(splits("id")) {
		t.Errorf("split by key: %v", p)
	}
	for _, p := range []Partitioning{SourcePartitioning(key, false, false), SourcePartitioning(key, true, true), SourcePartitioning(nil, true, false)} {
		if p.Kind != Unpartitioned {
			t.Errorf("%v, want unpartitioned", p)
		}
	}
}

// Until an exchange crosses pods, a stream whose path has one is read on
// one worker: OneWorker gives a keyed stream that reason exactly when the
// path has an exchange.
func TestOneWorkerWherePathsExchange(t *testing.T) {
	for _, splitsByKey := range []bool{false, true} {
		for _, read := range []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_INCREMENTAL, apiv1.SyncMode_SYNC_MODE_CDC} {
			cdc := read == apiv1.SyncMode_SYNC_MODE_CDC
			path := Place(SourcePartitioning([]string{"id"}, splitsByKey, cdc), WriterOperator([]string{"id"}, false))
			why := OneWorker(destination.Merge, true, splitsByKey, read)
			if (why != "") != (len(path.Exchanges()) > 0) {
				t.Errorf("splits by key %v, %v: one worker %q, exchanges %v", splitsByKey, read, why, path.Exchanges())
			}
		}
	}
}

func TestPathAPI(t *testing.T) {
	st := sqlplan.Statement{At: sqlplan.Position{Line: 2, Column: 1}, Passes: map[string]string{"id": "uid"}}
	src, ops := StatementPath(Partitioning{}, st, []string{"uid"}, false).API()
	want := []*apiv1.PlanOperator{
		{Kind: apiv1.PlanOperatorKind_PLAN_OPERATOR_KIND_STATEMENT, Line: 2, Column: 1,
			Partitioning: &apiv1.Partitioning{Kind: apiv1.PartitioningKind_PARTITIONING_KIND_NONE}},
		{Kind: apiv1.PlanOperatorKind_PLAN_OPERATOR_KIND_WRITER, Needs: []string{"uid"}, Exchange: true,
			Partitioning: &apiv1.Partitioning{Kind: apiv1.PartitioningKind_PARTITIONING_KIND_KEY_GROUP, Columns: []string{"uid"}}},
	}
	if src.GetKind() != apiv1.PartitioningKind_PARTITIONING_KIND_NONE || len(ops) != len(want) {
		t.Fatalf("%v, %v", src, ops)
	}
	for i := range want {
		if !proto.Equal(ops[i], want[i]) {
			t.Errorf("operator %d: %v, want %v", i, ops[i], want[i])
		}
	}
}

// Lanes cut their writer lanes' ranges, and the same layouts as the
// engine's layouts_match_the_runners.
func TestLayout(t *testing.T) {
	for _, groups := range []int{1, 2, 4, 8, 16, 256} {
		for writers := 1; writers <= 9; writers++ {
			for lanes := writers; lanes <= min(groups, 40); lanes++ {
				l, err := NewLayout(groups, lanes, writers)
				if err != nil {
					t.Fatal(err)
				}
				next := 0
				for i, r := range l.Lanes {
					w := l.Writers[l.WriterOf[i]]
					if r.First != next || r.First > r.Last || r.First < w.First || r.Last > w.Last {
						t.Fatalf("%d groups, %d lanes, %d writers: lane %d owns %v of writer lane %v", groups, lanes, writers, i, r, w)
					}
					next = r.Last + 1
				}
				for w, r := range l.Writers {
					if !slices.Contains(l.WriterOf, w) {
						t.Fatalf("writer lane %d has no lane", w)
					}
					for g := r.First; g <= r.Last; g++ {
						if g*writers/groups != w {
							t.Fatalf("writer lane %d owns group %d, which an exchange sends to %d", w, g, g*writers/groups)
						}
					}
				}
				if next != groups {
					t.Fatalf("%d groups, %d lanes, %d writers: lanes end at %d", groups, lanes, writers, next)
				}
			}
		}
	}
	for _, bad := range [][3]int{{4, 5, 1}, {256, 1, 2}, {256, 1, 0}} {
		if _, err := NewLayout(bad[0], bad[1], bad[2]); err == nil {
			t.Errorf("%v laid out", bad)
		}
	}
	type r = KeyGroupRange
	for _, tc := range []struct {
		groups, lanes, writers int
		ranges                 []KeyGroupRange
		writerOf               []int
	}{
		{256, 1, 1, []r{{0, 255}}, []int{0}},
		{256, 4, 2, []r{{0, 63}, {64, 127}, {128, 191}, {192, 255}}, []int{0, 0, 1, 1}},
		{256, 3, 2, []r{{0, 63}, {64, 127}, {128, 255}}, []int{0, 0, 1}},
		{256, 5, 3, []r{{0, 42}, {43, 85}, {86, 128}, {129, 170}, {171, 255}}, []int{0, 0, 1, 1, 2}},
		{8, 7, 5, []r{{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 6}, {7, 7}}, []int{0, 0, 1, 1, 2, 3, 4}},
		{4, 4, 3, []r{{0, 0}, {1, 1}, {2, 2}, {3, 3}}, []int{0, 0, 1, 2}},
	} {
		l, err := NewLayout(tc.groups, tc.lanes, tc.writers)
		if err != nil || !slices.Equal(l.Lanes, tc.ranges) || !slices.Equal(l.WriterOf, tc.writerOf) {
			t.Errorf("%d groups, %d lanes, %d writers: %v %v %v", tc.groups, tc.lanes, tc.writers, l.Lanes, l.WriterOf, err)
		}
	}
}

// pathText is a stream plan's path, for comparing.
func pathText(p *apiv1.StreamPlan) string {
	s := p.GetPartitioning().GetKind().String() + fmt.Sprint(p.GetPartitioning().GetColumns())
	for _, op := range p.GetOperators() {
		s += " > " + op.GetKind().String()
		if op.GetExchange() {
			s += " after an exchange"
		}
		s += " " + op.GetPartitioning().GetKind().String() + fmt.Sprint(op.GetPartitioning().GetColumns())
	}
	return s
}

// The plan gives each stream its path: what its source gives, and where an
// exchange sits on the way to the writer.
func TestPipelinePaths(t *testing.T) {
	lanes := warehouse
	lanes.WriterLanes = 8
	keyed := func(name string, splitsByKey bool, mode apiv1.SyncMode) *apiv1.CatalogStream {
		return &apiv1.CatalogStream{Name: name, PrimaryKey: []string{"id"}, CursorField: "id", Replay: apiv1.Replay_REPLAY_EXACT, SyncModes: []apiv1.SyncMode{mode}, SplitsByKey: splitsByKey,
			Columns: []*apiv1.CatalogColumn{{Name: "id", Type: "int64"}, {Name: "k", Type: "utf8"}}}
	}
	cat := &apiv1.Catalog{Streams: []*apiv1.CatalogStream{
		keyed("ranged", true, apiv1.SyncMode_SYNC_MODE_INCREMENTAL),
		keyed("hashed", false, apiv1.SyncMode_SYNC_MODE_INCREMENTAL),
		keyed("changes", true, apiv1.SyncMode_SYNC_MODE_CDC),
		{Name: "logs", Replay: apiv1.Replay_REPLAY_EXACT, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_FULL_REFRESH}},
	}}
	const writer = "PLAN_OPERATOR_KIND_WRITER"
	paths := func(plans []*apiv1.StreamPlan) map[string]string {
		out := map[string]string{}
		for _, p := range plans {
			out[p.GetName()] = pathText(p)
		}
		return out
	}
	plans, problems, _ := Pipeline(&apiv1.PipelineSpec{}, cat, lanes)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	want := map[string]string{
		"ranged":  "PARTITIONING_KIND_SPLITS[id] > " + writer + " PARTITIONING_KIND_SPLITS[id]",
		"hashed":  "PARTITIONING_KIND_NONE[] > " + writer + " after an exchange PARTITIONING_KIND_KEY_GROUP[id]",
		"changes": "PARTITIONING_KIND_NONE[] > " + writer + " after an exchange PARTITIONING_KIND_KEY_GROUP[id]",
		"logs":    "PARTITIONING_KIND_NONE[] > " + writer + " PARTITIONING_KIND_NONE[]",
	}
	if got := paths(plans); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("paths\n%v\nwant\n%v", got, want)
	}
	// Into one writer lane, every lane merges.
	plans, _, _ = Pipeline(&apiv1.PipelineSpec{}, cat, warehouse)
	if got := pathText(plans[1]); got != "PARTITIONING_KIND_NONE[] > "+writer+" PARTITIONING_KIND_ONE_LANE[]" {
		t.Errorf("hashed into one writer lane: %s", got)
	}

	// A statement's output: renamed, its stream's partitioning carries on;
	// an aggregate exchanged on its group key reaches its writer with no
	// second exchange.
	a := &sqlplan.Answer{Statements: []sqlplan.Statement{
		{At: sqlplan.Position{Line: 1, Column: 1}, Output: "renamed", Class: "row_wise", Inputs: []string{"ranged"}, Key: []string{"rid"},
			Passes: map[string]string{"id": "rid", "k": "k"}},
		{At: sqlplan.Position{Line: 2, Column: 1}, Output: "hourly", Class: "group_window", Inputs: []string{"ranged"}, Key: []string{"window_start", "k"},
			Need: []string{"k"}, Passes: map[string]string{"k": "k"}},
	}}
	spec := sqlSpec("…", apiv1.Passthrough_PASSTHROUGH_NONE, nil)
	plans, problems, _ = PipelineSQL(spec, cat, lanes, a)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	const statement = "PLAN_OPERATOR_KIND_STATEMENT"
	want = map[string]string{
		"renamed": "PARTITIONING_KIND_SPLITS[id] > " + statement + " PARTITIONING_KIND_SPLITS[id] > " + writer + " PARTITIONING_KIND_SPLITS[rid]",
		"hourly": "PARTITIONING_KIND_SPLITS[id] > " + statement + " after an exchange PARTITIONING_KIND_KEY_GROUP[k] > " +
			writer + " PARTITIONING_KIND_KEY_GROUP[k]",
	}
	if got := paths(plans); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("paths\n%v\nwant\n%v", got, want)
	}
	tp := Transform(spec, cat, a)
	if ex := [][]string{tp.GetStatements()[0].GetExchange(), tp.GetStatements()[1].GetExchange()}; ex[0] != nil || !slices.Equal(ex[1], []string{"k"}) {
		t.Errorf("statements' exchanges %v", ex)
	}
}
