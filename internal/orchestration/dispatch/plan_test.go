package dispatch

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/openetl-io/openetl/internal/platform/resources"
)

func runner(id string, capacity, used resources.Size) Runner {
	return Runner{RunnerFacts: RunnerFacts{ID: id, Release: "dev", Protocol: "v1", Platform: "linux/amd64", Launchers: []string{LauncherSubprocess}}, Pool: "default", Capacity: capacity, Used: used}
}

func request(id string, waited time.Duration) Request {
	return Request{ID: id, Org: "o", Pool: "default", Size: resources.S, Priority: 1, Waited: waited}
}

func placed(ds []Decision) map[string]string {
	out := map[string]string{}
	for _, d := range ds {
		out[d.ID] = d.Runner
	}
	return out
}

func TestBestFitLeavesLeastRoom(t *testing.T) {
	ds := Plan([]Runner{
		runner("big", resources.XL, resources.Size{}),
		runner("small", resources.M, resources.S),
		runner("full", resources.S, resources.S),
	}, []Request{request("a", 0)}, nil, DefaultReserveAfter)
	if got := placed(ds); got["a"] != "small" {
		t.Fatalf("placed on %q, want small: the runner it leaves least room on", got["a"])
	}
}

func TestBestFitTiesGoToFewestAttempts(t *testing.T) {
	a, b := runner("a", resources.M, resources.Size{}), runner("b", resources.M, resources.Size{})
	a.Attempts = 2
	ds := Plan([]Runner{a, b}, []Request{request("r", 0)}, nil, DefaultReserveAfter)
	if got := placed(ds); got["r"] != "b" {
		t.Fatalf("placed on %q, want b, with fewer attempts", got["r"])
	}
}

func TestPlacementsInAPassUseRoom(t *testing.T) {
	ds := Plan([]Runner{runner("a", resources.M, resources.Size{})},
		[]Request{request("first", time.Minute), request("second", time.Second), request("third", 0)}, nil, DefaultReserveAfter)
	got := placed(ds)
	if got["first"] != "a" || got["second"] != "a" || len(got) != 2 {
		t.Fatalf("got %v, want the two oldest on a", got)
	}
}

func TestDrainingAndOtherPoolsTakeNothing(t *testing.T) {
	draining := runner("draining", resources.XL, resources.Size{})
	draining.Draining = true
	other := runner("other", resources.XL, resources.Size{})
	other.Pool = "big"
	if ds := Plan([]Runner{draining, other}, []Request{request("r", 0)}, nil, DefaultReserveAfter); len(ds) != 0 {
		t.Fatalf("got %+v, want nothing placed", ds)
	}
}

func TestPriorityGoesFirst(t *testing.T) {
	manual, scheduled, backfill := request("manual", 0), request("scheduled", time.Minute), request("backfill", 90*time.Second)
	manual.Priority, backfill.Priority = 0, 2
	ds := Plan([]Runner{runner("a", resources.S, resources.Size{})}, []Request{backfill, scheduled, manual}, nil, DefaultReserveAfter)
	if got := placed(ds); got["manual"] != "a" || len(got) != 1 {
		t.Fatalf("got %v, want only the manual sync", got)
	}
}

func TestFairAcrossOrgsByReservedCPU(t *testing.T) {
	a1, a2, b1 := request("a1", time.Minute), request("a2", 50*time.Second), request("b1", time.Second)
	a1.Org, a2.Org, b1.Org = "a", "a", "b"
	// Org a has more reserved, so b's newer request goes first, and the
	// oldest of a's next.
	ds := Plan([]Runner{runner("r", resources.M, resources.Size{})}, []Request{a1, a2, b1}, map[string]int64{"a": 4000, "b": 1000}, DefaultReserveAfter)
	if len(ds) != 2 || ds[0].ID != "b1" || ds[1].ID != "a1" {
		t.Fatalf("got %+v, want b1 then a1", ds)
	}
}

func TestWeightsScaleAnOrgsShare(t *testing.T) {
	a, b := request("a", time.Minute), request("b", time.Second)
	a.Org, b.Org = "a", "b"
	a.Weight = 10
	// a has twice b's CPU, but ten times the weight.
	ds := Plan([]Runner{runner("r", resources.S, resources.Size{})}, []Request{a, b}, map[string]int64{"a": 2000, "b": 1000}, DefaultReserveAfter)
	if got := placed(ds); got["a"] != "r" || len(got) != 1 {
		t.Fatalf("got %v, want a", got)
	}
}

func TestSmallerRequestsPassALargerOneThatDoesntFit(t *testing.T) {
	large, small := request("large", time.Minute), request("small", 0)
	large.Size = resources.L
	ds := Plan([]Runner{runner("r", resources.L, resources.M)}, []Request{large, small}, nil, DefaultReserveAfter)
	if got := placed(ds); got["small"] != "r" || len(got) != 1 {
		t.Fatalf("got %v, want the small one placed past the large", got)
	}
}

// Past the wait, a large request holds the runner with the most room, so
// small ones can't keep it from ever fitting.
func TestStarvingRequestReservesTheRunnerWithTheMostRoom(t *testing.T) {
	large, small := request("large", 3*time.Minute), request("small", 0)
	large.Size = resources.L
	fuller, emptier := runner("fuller", resources.L, resources.L), runner("emptier", resources.L, resources.M)
	ds := Plan([]Runner{fuller, emptier}, []Request{large, small}, nil, DefaultReserveAfter)
	if len(ds) != 0 {
		t.Fatalf("got %+v, want nothing: fuller is full and emptier is reserved", ds)
	}
	// Once the reserved runner has room, the large one goes there first.
	emptier.Used = resources.Size{}
	ds = Plan([]Runner{fuller, emptier}, []Request{small, large}, nil, DefaultReserveAfter)
	if got := placed(ds); got["large"] != "emptier" {
		t.Fatalf("got %v", got)
	}
}

// A larger runner appearing takes the reservation over, so the smaller one
// that had it is free again.
func TestALargerRunnerTakesTheReservationOver(t *testing.T) {
	large, small := request("large", 3*time.Minute), request("small", 0)
	large.Size = resources.L
	ds := Plan([]Runner{runner("small-runner", resources.L, resources.M), runner("new", resources.XL, resources.Size{})}, []Request{large, small}, nil, DefaultReserveAfter)
	got := placed(ds)
	if got["large"] != "new" || got["small"] != "small-runner" {
		t.Fatalf("got %v", got)
	}
}

func TestASizeNoRunnerCanTakeFailsAfterTheWait(t *testing.T) {
	r := request("r", 3*time.Minute)
	r.Size = resources.XL
	ds := Plan([]Runner{runner("a", resources.M, resources.Size{}), runner("b", resources.L, resources.Size{})}, []Request{r}, nil, DefaultReserveAfter)
	if len(ds) != 1 || ds[0].Runner != "" || !strings.Contains(ds[0].Failure, "8 vCPU, 16 GiB") || !strings.Contains(ds[0].Failure, "the largest has 4 vCPU, 8 GiB") {
		t.Fatalf("got %+v", ds)
	}
	// It waits before that, as a larger runner may be starting.
	r.Waited = time.Minute
	if ds := Plan([]Runner{runner("b", resources.L, resources.Size{})}, []Request{r}, nil, DefaultReserveAfter); len(ds) != 0 {
		t.Fatalf("got %+v, want it still waiting", ds)
	}
}

func TestNoRunnerReportingKeepsARequestWaiting(t *testing.T) {
	if ds := Plan(nil, []Request{request("r", time.Hour)}, nil, DefaultReserveAfter); len(ds) != 0 {
		t.Fatalf("got %+v", ds)
	}
}

func TestConnectorsARunnerCantRunSteerPlacement(t *testing.T) {
	r := request("r", 0)
	r.Needs = []Need{{Protocol: "v1", Binaries: []string{"linux/amd64"}}}
	wrong := runner("wrong", resources.M, resources.S)
	wrong.Platform = "darwin/arm64"
	if got := placed(Plan([]Runner{wrong, runner("right", resources.XL, resources.Size{})}, []Request{r}, nil, DefaultReserveAfter)); got["r"] != "right" {
		t.Fatalf("got %v", got)
	}
	r.Waited = 3 * time.Minute
	ds := Plan([]Runner{wrong}, []Request{r}, nil, DefaultReserveAfter)
	if len(ds) != 1 || !strings.Contains(ds[0].Failure, "no binary for darwin/arm64") {
		t.Fatalf("got %+v", ds)
	}
}

func TestPlanDoesntChangeItsInput(t *testing.T) {
	runners := []Runner{runner("a", resources.M, resources.Size{})}
	requests := []Request{request("b", 0), request("a", time.Minute)}
	org := map[string]int64{"o": 1}
	want := []Request{request("b", 0), request("a", time.Minute)}
	Plan(runners, requests, org, DefaultReserveAfter)
	if runners[0].Used != (resources.Size{}) || !reflect.DeepEqual(requests, want) || org["o"] != 1 {
		t.Fatalf("runners %+v, requests %+v, org %+v", runners, requests, org)
	}
}

func TestWhySaysWhatTheRunnerLacks(t *testing.T) {
	rn := RunnerFacts{ID: "r", Release: "0.1.0", Protocol: "v1", Platform: "linux/amd64", Features: []string{"a"}, Launchers: []string{LauncherSubprocess}}
	for _, c := range []struct {
		need Need
		want string
	}{
		{Need{Manifest: true}, ""},
		{Need{Platform: "0.2.0", Manifest: true}, "needs openetl 0.2.0"},
		{Need{Protocol: "v2"}, "speaks protocol v2"},
		{Need{Protocol: "v1", Features: []string{"b"}, Binaries: []string{"linux/amd64"}}, "needs the feature b"},
		{Need{Protocol: "v1", Binaries: []string{"linux/amd64"}}, ""},
		{Need{Protocol: "v1", Binaries: []string{"linux/arm64"}}, "no binary for linux/amd64"},
		{Need{Protocol: "v1", Image: true, ImagePlatforms: []string{"linux/amd64"}}, "has only an image"},
	} {
		got := c.need.Why(rn)
		if (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
			t.Errorf("%+v: %q, want %q", c.need, got, c.want)
		}
	}
}
