package dispatch

import (
	"errors"
	"reflect"
	"testing"

	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/store/db"
)

func demandRow(dataPlane string, size resources.Size, state string, pod bool, attempts int64, oldest float64) db.PlacementDemandRow {
	return db.PlacementDemandRow{DataPlaneID: dataPlane, Pool: "default", SizeCpu: size.CPU, SizeMemory: size.Memory, State: state, Pod: pod, Attempts: attempts, OldestSeconds: oldest}
}

// Placed and running attempts add up; waiting ones go where the placer would
// send them, so runners' demand leaves out what pods take.
func TestDemandByWhereItRuns(t *testing.T) {
	asked := map[string]int{}
	planes := map[string][]Runner{
		"a": {runner("shared", resources.L, resources.Size{}), launcher("pods", Pods{Min: resources.M})},
		"b": {runner("shared", resources.L, resources.Size{})},
	}
	got, err := sumDemand([]db.PlacementDemandRow{
		demandRow("a", resources.S, "placed", false, 1, 5),
		demandRow("a", resources.S, "running", false, 2, 50),
		demandRow("a", resources.L, "running", true, 1, 60),
		demandRow("a", resources.S, "pending", false, 3, 7),
		demandRow("a", resources.L, "pending", false, 2, 9),
		demandRow("b", resources.L, "pending", false, 1, 4),
	}, func(dataPlane string) ([]Runner, error) {
		asked[dataPlane]++
		return planes[dataPlane], nil
	})
	if err != nil {
		t.Fatal(err)
	}
	key := func(dataPlane string, size resources.Size, pending, pod bool) demandKey {
		return demandKey{dataPlane: dataPlane, pool: "default", size: size, pending: pending, pod: pod}
	}
	want := []demand{
		{key("a", resources.S, false, false), 3, 0},
		{key("a", resources.L, false, true), 1, 0},
		{key("a", resources.S, true, false), 3, 7},
		{key("a", resources.L, true, true), 2, 9},
		{key("b", resources.L, true, false), 1, 4},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
	if asked["a"] != 1 || asked["b"] != 1 {
		t.Errorf("runners read %v, want once a data plane", asked)
	}
	if got[1].runsOn() != "pod" || got[0].runsOn() != "runner" {
		t.Errorf("runs on %q and %q", got[1].runsOn(), got[0].runsOn())
	}

	failed := errors.New("no")
	if _, err := sumDemand([]db.PlacementDemandRow{demandRow("a", resources.S, "pending", false, 1, 1)}, func(string) ([]Runner, error) { return nil, failed }); !errors.Is(err, failed) {
		t.Fatalf("err %v", err)
	}
}
