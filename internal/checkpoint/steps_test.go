package checkpoint

import (
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
)

// splitIDs are n splits' IDs, as a Kafka topic's partitions.
func splitIDs(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("orders-%d", i)
	}
	return out
}

// randomSteps are steps of lanes keyed lanes over splits, from seed: each
// lane's rows of some splits met some of the epoch's watermarks, which
// rise from floor.
func randomSteps(seed uint64, lanes, splits int, floor int64) []LaneSteps {
	rng := rand.New(rand.NewPCG(seed, 1))
	marks := make([]int64, 1+rng.IntN(40))
	at := floor
	for i := range marks {
		at += 1 + rng.Int64N(3_000_000)
		marks[i] = at
	}
	ids := splitIDs(splits)
	per := 256 / lanes
	var out []LaneSteps
	for l := range lanes {
		ls := LaneSteps{KeyGroups: KeyGroupRange{First: l * per, Last: (l+1)*per - 1}}
		for _, id := range ids {
			if rng.IntN(4) == 0 {
				continue
			}
			var s SplitSteps
			s.Split = id
			place, mark := uint64(rng.IntN(5)), rng.IntN(len(marks))
			for mark < len(marks) {
				s.Steps = append(s.Steps, Step{Place: place, Watermark: marks[mark]})
				place += 1 + uint64(rng.IntN(2000))
				mark += 1 + rng.IntN(4)
			}
			ls.Splits = append(ls.Splits, s)
		}
		out = append(out, ls)
	}
	return out
}

// A record keeps steps as they were: each lane's, by its key groups, and
// each split's, of any floor, the end of input among the watermarks.
func TestStepsRoundTrip(t *testing.T) {
	cases := 0
	for seed := range uint64(300) {
		floor := []int64{math.MinInt64, 0, 1_763_000_000_000_000}[seed%3]
		lanes := 1 + int(seed%8)
		want := randomSteps(seed, lanes, 1+int(seed%13), floor)
		if seed%5 == 0 {
			last := &want[len(want)-1]
			if len(last.Splits) > 0 {
				s := &last.Splits[0]
				s.Steps = append(s.Steps, Step{Place: s.Steps[len(s.Steps)-1].Place + 1, Watermark: math.MaxInt64})
			}
		}
		ids := splitIDs(1 + int(seed%13))
		b, err := EncodeSteps(want, ids, floor)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		got, err := DecodeSteps(b, ids, floor)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		// Lanes without steps are left out.
		want = slices.DeleteFunc(want, func(l LaneSteps) bool { return len(l.Splits) == 0 })
		if len(want) == 0 {
			want = nil
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("seed %d: got %+v, want %+v", seed, got, want)
		}
		cases++
	}
	if cases != 300 {
		t.Fatalf("%d cases", cases)
	}
}

// A split the record doesn't name is left out, as a redo doesn't read it;
// no steps are no bytes.
func TestStepsLeaveOutWhatARedoDoesntRead(t *testing.T) {
	lanes := []LaneSteps{{KeyGroups: KeyGroupRange{First: 0, Last: 9}, Splits: []SplitSteps{
		{Split: "gone", Steps: []Step{{Place: 3, Watermark: 50}}},
		{Split: "p1", Steps: []Step{{Place: 0, Watermark: 40}, {Place: 9, Watermark: 70}}},
	}}}
	b, err := EncodeSteps(lanes, []string{"p0", "p1"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeSteps(b, []string{"p0", "p1"}, 10)
	want := []LaneSteps{{KeyGroups: KeyGroupRange{First: 0, Last: 9}, Splits: lanes[0].Splits[1:]}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, %v; want %+v", got, err, want)
	}
	for _, ls := range [][]LaneSteps{nil, {{KeyGroups: KeyGroupRange{First: 0, Last: 9}}}, {{KeyGroups: KeyGroupRange{First: 0, Last: 9}, Splits: []SplitSteps{{Split: "gone", Steps: []Step{{Place: 1, Watermark: 2}}}}}}} {
		if b, err := EncodeSteps(ls, []string{"p0"}, 0); b != nil || err != nil {
			t.Errorf("%+v encoded as %v, %v", ls, b, err)
		}
	}
}

// Steps that don't ascend, or lanes that overlap, aren't encoded; bytes
// cut short, or that name what the record hasn't, aren't decoded.
func TestStepsRefuseWhatTheyCantBe(t *testing.T) {
	kg := KeyGroupRange{First: 0, Last: 9}
	for name, ls := range map[string][]LaneSteps{
		"places":     {{KeyGroups: kg, Splits: []SplitSteps{{Split: "p0", Steps: []Step{{Place: 4, Watermark: 1}, {Place: 4, Watermark: 2}}}}}},
		"watermarks": {{KeyGroups: kg, Splits: []SplitSteps{{Split: "p0", Steps: []Step{{Place: 4, Watermark: 2}, {Place: 5, Watermark: 2}}}}}},
		"overlap": {
			{KeyGroups: kg, Splits: []SplitSteps{{Split: "p0", Steps: []Step{{Place: 4, Watermark: 2}}}}},
			{KeyGroups: KeyGroupRange{First: 9, Last: 12}, Splits: []SplitSteps{{Split: "p0", Steps: []Step{{Place: 4, Watermark: 2}}}}},
		},
	} {
		if _, err := EncodeSteps(ls, []string{"p0"}, 0); !errors.Is(err, errSteps) {
			t.Errorf("%s: %v", name, err)
		}
	}
	b, err := EncodeSteps(randomSteps(7, 4, 6, 0), splitIDs(6), 0)
	if err != nil || len(b) < 8 {
		t.Fatal(len(b), err)
	}
	for n := range len(b) {
		if _, err := DecodeSteps(b[:n], splitIDs(6), 0); n > 0 && !errors.Is(err, errSteps) {
			t.Fatalf("%d of %d bytes: %v", n, len(b), err)
		}
	}
	if _, err := DecodeSteps(append(slices.Clone(b), 0), splitIDs(6), 0); !errors.Is(err, errSteps) {
		t.Errorf("a byte after them: %v", err)
	}
	if _, err := DecodeSteps(b, splitIDs(2), 0); !errors.Is(err, errSteps) {
		t.Errorf("fewer splits: %v", err)
	}
}

// shape is an epoch's steps for TestStepsBytes: splits read into lanes
// keyed lanes, of which each lane's rows of each split met steps
// watermarks, the window's moving by step microseconds each time and the
// split's rows by rows.
type shape struct {
	name                 string
	splits, lanes, steps int
	step                 int64
	rows                 uint64
	// At most, in bytes.
	most int
}

// The bytes a record keeps of an epoch's steps, for a few shapes, as the
// record's JSON has them. Quiet epochs keep none. A tumbling minute crosses
// at most one window's end in a ten-second epoch, so each keyed lane's rows
// of a split take a step at most once; sessions, or any window with
// lateness, take one each time the watermark moved between the lane's rows
// of a split, here twenty times an epoch, or eight with 64 splits. A keyed
// lane takes 1,024 steps an epoch at most, which bounds a busy epoch's: 16
// for each of 64 splits.
func TestStepsBytes(t *testing.T) {
	const floor = 1_763_000_000_000_000
	for _, s := range []shape{
		{name: "quiet", splits: 64, lanes: 32, steps: 0},
		{name: "tumble, 1 split", splits: 1, lanes: 1, steps: 1, step: 60_000_000, rows: 30_000, most: 24},
		{name: "tumble, 8 splits", splits: 8, lanes: 8, steps: 1, step: 60_000_000, rows: 4_000, most: 400},
		{name: "tumble, 64 splits", splits: 64, lanes: 32, steps: 1, step: 60_000_000, rows: 500, most: 12_000},
		{name: "session, 1 split", splits: 1, lanes: 1, steps: 20, step: 500_000, rows: 1_500, most: 200},
		{name: "session, 8 splits", splits: 8, lanes: 8, steps: 20, step: 500_000, rows: 200, most: 6_000},
		{name: "session, 64 splits", splits: 64, lanes: 32, steps: 8, step: 500_000, rows: 60, most: 60_000},
		{name: "most steps, 64 splits", splits: 64, lanes: 32, steps: 16, step: 100_000, rows: 25, most: 100_000},
	} {
		ids := splitIDs(s.splits)
		var lanes []LaneSteps
		per := 256 / s.lanes
		for l := range s.lanes {
			ls := LaneSteps{KeyGroups: KeyGroupRange{First: l * per, Last: (l+1)*per - 1}}
			for k, id := range ids {
				var sp SplitSteps
				sp.Split = id
				// Lanes' rows of a split meet the watermark's steps at
				// places of their own.
				place := uint64(l*7+k*3) % max(s.rows, 1)
				for i := range s.steps {
					sp.Steps = append(sp.Steps, Step{Place: place, Watermark: floor + int64(i+1)*s.step})
					place += s.rows + uint64(l+k)%5
				}
				if len(sp.Steps) > 0 {
					ls.Splits = append(ls.Splits, sp)
				}
			}
			lanes = append(lanes, ls)
		}
		b, err := EncodeSteps(lanes, ids, floor)
		if err != nil {
			t.Fatal(err)
		}
		inJSON := len(base64.StdEncoding.EncodeToString(b))
		t.Logf("%-22s %2d splits, %2d keyed lanes, %2d steps each: %6d bytes, %6d in the record's JSON", s.name, s.splits, s.lanes, s.steps, len(b), inJSON)
		if s.steps == 0 && b != nil {
			t.Errorf("%s: %d bytes", s.name, len(b))
		}
		if inJSON > s.most {
			t.Errorf("%s: %d bytes in the record's JSON, over %d", s.name, inJSON, s.most)
		}
		if got, err := DecodeSteps(b, ids, floor); err != nil || (s.steps > 0 && !reflect.DeepEqual(got, lanes)) {
			t.Errorf("%s: decoded as other steps: %v", s.name, err)
		}
	}
}
