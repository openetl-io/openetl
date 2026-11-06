package sizing

import (
	"testing"

	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/platform/resources"
)

func TestChoose(t *testing.T) {
	oom := errs.ResourceExhausted.String()
	mem := func(gib float64) int64 { return int64(gib * float64(resources.GiB)) }
	custom := resources.Size{CPU: 6000, Memory: 12 * resources.GiB}
	for _, tc := range []struct {
		name string
		in   Input
		want Choice
	}{
		{"nothing says more", Input{}, Choice{resources.S, FromDefault}},
		{"the largest hint", Input{Hint: resources.L}, Choice{resources.L, FromHint}},
		{"the spec", Input{Spec: Spec{Size: resources.M}, Hint: resources.L}, Choice{resources.M, FromSpec}},
		{"amounts", Input{Spec: Spec{Size: custom}}, Choice{custom, FromSpec}},
		{
			"above the recent peak, with a quarter's headroom",
			Input{History: []Attempt{{Size: resources.S, Memory: mem(1.7)}}},
			Choice{resources.M, FromHistory},
		},
		{
			"history can shrink a job below its hint",
			Input{Hint: resources.L, History: []Attempt{{Size: resources.L, Memory: mem(0.5)}}},
			Choice{resources.XS, FromHistory},
		},
		{
			"the 95th percentile, not the latest",
			Input{History: []Attempt{{Memory: mem(0.1)}, {Memory: mem(3)}, {Memory: mem(0.1)}}},
			Choice{resources.M, FromHistory},
		},
		{
			"CPU counts on an attempt of a minute or more",
			Input{History: []Attempt{{Memory: mem(0.1), CPU: 2.5 * 600, Seconds: 600}}},
			Choice{resources.L, FromHistory},
		},
		{
			"CPU of a short attempt says nothing",
			Input{History: []Attempt{{Memory: mem(0.1), CPU: 50, Seconds: 10}}},
			Choice{resources.XS, FromHistory},
		},
		{
			"failing before measuring anything leaves the hint",
			Input{Hint: resources.M, History: []Attempt{{Size: resources.M, Code: "TRANSIENT"}}},
			Choice{resources.M, FromHint},
		},
		{
			"out of memory: one size up",
			Input{History: []Attempt{{Size: resources.S, Memory: mem(1), Code: oom}}},
			Choice{resources.M, FromOOM},
		},
		{
			"out of memory holds over later attempts that peak lower",
			Input{History: []Attempt{{Size: resources.M, Memory: mem(0.2)}, {Size: resources.M, Memory: mem(0.2)}, {Size: resources.S, Memory: mem(0.2), Code: oom}}},
			Choice{resources.M, FromOOM},
		},
		{
			"out of memory twice: two sizes up",
			Input{History: []Attempt{{Size: resources.M, Code: oom}, {Size: resources.S, Code: oom}}},
			Choice{resources.L, FromOOM},
		},
		{
			"a set size stays when it runs out of memory, without max",
			Input{Spec: Spec{Size: resources.M}, History: []Attempt{{Size: resources.M, Code: oom}}},
			Choice{resources.M, FromSpec},
		},
		{
			"a set size grows up to max",
			Input{Spec: Spec{Size: resources.M, Max: resources.XL}, History: []Attempt{{Size: resources.M, Code: oom}}},
			Choice{resources.L, FromOOM},
		},
		{
			"max stops it",
			Input{Spec: Spec{Max: resources.M}, History: []Attempt{{Size: resources.M, Code: oom}}},
			Choice{resources.M, FromSpec},
		},
		{
			"auto stops at xl without max",
			Input{History: []Attempt{{Size: resources.XL, Code: oom}}},
			Choice{resources.XL, FromSpec},
		},
		{
			"max above xl lets it double",
			Input{Spec: Spec{Max: resources.Size{CPU: 32000, Memory: 64 * resources.GiB}}, History: []Attempt{{Size: resources.XL, Code: oom}}},
			Choice{resources.Size{CPU: 16000, Memory: 32 * resources.GiB}, FromOOM},
		},
		{
			"min holds it up",
			Input{Spec: Spec{Min: resources.M}, History: []Attempt{{Memory: mem(0.2)}}},
			Choice{resources.M, FromSpec},
		},
		{
			"a min above xl holds without a max",
			Input{Spec: Spec{Min: resources.Size{CPU: 16000, Memory: 32 * resources.GiB}}},
			Choice{resources.Size{CPU: 16000, Memory: 32 * resources.GiB}, FromSpec},
		},
		{
			"the most a runner can take",
			Input{Hint: resources.XL, Runners: []resources.Size{{CPU: 3500, Memory: 7 * resources.GiB}, {CPU: 7500, Memory: 15 * resources.GiB}}},
			Choice{resources.L, FromLargest},
		},
		{
			"a set size that no runner can take stays, and fails",
			Input{Spec: Spec{Size: resources.XL}, Runners: []resources.Size{{CPU: 1000, Memory: 2 * resources.GiB}}},
			Choice{resources.XL, FromSpec},
		},
		{
			"a min no runner can take stays, and fails",
			Input{Spec: Spec{Min: resources.L}, Runners: []resources.Size{{CPU: 1000, Memory: 2 * resources.GiB}}},
			Choice{resources.L, FromSpec},
		},
		{
			"a set size's growth stops at the most a runner can take",
			Input{Spec: Spec{Size: resources.S, Max: resources.XL}, History: []Attempt{{Size: resources.M, Code: oom}}, Runners: []resources.Size{{CPU: 2000, Memory: 4 * resources.GiB}}},
			Choice{resources.M, FromLargest},
		},
		{
			"runners too small for any preset change nothing",
			Input{Runners: []resources.Size{{CPU: 100, Memory: 100}}},
			Choice{resources.S, FromDefault},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Choose(tc.in); got != tc.want {
				t.Fatalf("Choose = %v (%s), want %v (%s)", got.Size, got.Reason, tc.want.Size, tc.want.Reason)
			}
		})
	}
}

func TestReasonsHaveAPINames(t *testing.T) {
	for _, r := range []Reason{FromSpec, FromHint, FromDefault, FromHistory, FromOOM, FromLargest} {
		if r.API() == 0 {
			t.Errorf("%s has no API name", r)
		}
	}
}

func TestLargestHint(t *testing.T) {
	if got := LargestHint("", "s", "bad", "m", "xs"); got != resources.M {
		t.Fatalf("LargestHint = %v, want m", got)
	}
	if got := LargestHint("", ""); got != (resources.Size{}) {
		t.Fatalf("LargestHint = %v, want none", got)
	}
}
