// Package dispatch sends workflows' work to data planes through the API
// (ADR-0028 §1) and places attempts on runners (ADR-0027 §6): dispatch rows
// in Postgres, leases on its clock, long polls, and waking the workflows
// waiting for results.
package dispatch

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/unsuitable001/openetl/internal/platform/resources"
	"github.com/unsuitable001/openetl/internal/spec"
)

// LauncherSubprocess runs a role's binary as a subprocess.
const LauncherSubprocess = "subprocess"

// RunnerFacts is what a runner reports of what it can run.
type RunnerFacts struct {
	ID, Release, Protocol, Platform string
	Features, Launchers             []string
}

// Need is what one of a job's connector roles needs of a runner.
type Need struct {
	// The openetl release it needs, if it says.
	Platform string `json:",omitempty"`
	// It's a manifest, which any runner runs.
	Manifest bool     `json:",omitempty"`
	Protocol string   `json:",omitempty"`
	Features []string `json:",omitempty"`
	// The platforms it has binaries for.
	Binaries []string `json:",omitempty"`
	// It has an image, for these platforms.
	Image          bool     `json:",omitempty"`
	ImagePlatforms []string `json:",omitempty"`
}

// NeedOf is what r needs of a runner.
func NeedOf(r *spec.Role) Need {
	n := Need{Platform: r.Requires.Platform, Manifest: r.Manifest != nil}
	if r.Spec != nil {
		n.Protocol, n.Features = r.Spec.GetProtocolVersion(), r.Spec.GetRequiredFeatures()
	}
	for p := range r.Binaries {
		n.Binaries = append(n.Binaries, p)
	}
	slices.Sort(n.Binaries)
	if r.Image != nil {
		n.Image, n.ImagePlatforms = true, r.Image.Platforms
	}
	return n
}

// Why is why rn can't run it, or "".
func (n Need) Why(rn RunnerFacts) string {
	if p := n.Platform; p != "" && rn.Release != "dev" && (!spec.ValidVersion(rn.Release) || spec.CompareVersions(rn.Release, p) < 0) {
		return fmt.Sprintf("it needs openetl %s or later, and runner %s is %s", p, rn.ID, rn.Release)
	}
	if n.Manifest {
		return ""
	}
	if n.Protocol != rn.Protocol {
		return fmt.Sprintf("it speaks protocol %s, and runner %s speaks %s", n.Protocol, rn.ID, rn.Protocol)
	}
	for _, f := range n.Features {
		if !slices.Contains(rn.Features, f) {
			return fmt.Sprintf("it needs the feature %s, which runner %s hasn't", f, rn.ID)
		}
	}
	if slices.Contains(n.Binaries, rn.Platform) && slices.Contains(rn.Launchers, LauncherSubprocess) {
		return ""
	}
	containers := slices.ContainsFunc(rn.Launchers, func(l string) bool { return l != LauncherSubprocess })
	if n.Image && containers && slices.Contains(n.ImagePlatforms, rn.Platform) {
		return ""
	}
	switch {
	case n.Image && len(n.Binaries) == 0 && !containers:
		return fmt.Sprintf("it has only an image, and runner %s has no container launcher", rn.ID)
	case n.Image && containers:
		return fmt.Sprintf("neither its binaries nor its image are for %s, runner %s's platform", rn.Platform, rn.ID)
	}
	return fmt.Sprintf("it has no binary for %s, runner %s's platform", rn.Platform, rn.ID)
}

// Runner is a runner reporting now, as the placer sees it.
type Runner struct {
	RunnerFacts
	Pool     string
	Capacity resources.Size
	// What's placed on it, and what it runs that isn't any more.
	Used resources.Size
	// What's placed on it.
	Attempts int
	Draining bool
}

// Request is an attempt waiting for room.
type Request struct {
	ID string
	// Who it's fair among; today every request of a pass has the same org,
	// since orgs don't share data planes.
	Org  string
	Pool string
	Size resources.Size
	// Lower goes first.
	Priority int
	Waited   time.Duration
	// The org's share of the data plane; zero counts as 1.
	Weight float64
	Needs  []Need
}

// Decision is where a request goes: on Runner, or it fails with Failure.
type Decision struct {
	ID      string
	Runner  string
	Failure string
}

// DefaultReserveAfter is how long a request waits before it reserves a runner
// and can fail for good.
const DefaultReserveAfter = 2 * time.Minute

// Plan decides what the room on runners goes to. Org holds the CPU each org
// has reserved now. A request that fits a runner goes on the one it leaves
// least room on, among those of the highest priority, then the org with the
// least reserved CPU per weight, then the longest-waiting.
//
// A request that has waited reserveAfter goes first if it fits. If it doesn't,
// it reserves the runner with the most room, so nothing else goes there until
// it fits, or it fails if no runner reporting now could ever take it.
func Plan(runners []Runner, requests []Request, org map[string]int64, reserveAfter time.Duration) []Decision {
	p := &planner{runners: slices.Clone(runners), org: map[string]int64{}, held: map[string]bool{}}
	for k, v := range org {
		p.org[k] = v
	}
	queue := slices.Clone(requests)
	slices.SortStableFunc(queue, func(a, b Request) int {
		return cmp.Or(cmp.Compare(a.Priority, b.Priority), cmp.Compare(b.Waited, a.Waited), cmp.Compare(a.ID, b.ID))
	})
	var rest []Request
	for _, r := range queue {
		if r.Waited < reserveAfter {
			rest = append(rest, r)
			continue
		}
		p.starving(r)
	}
	for {
		i := p.next(rest)
		if i < 0 {
			return p.out
		}
		p.place(rest[i], p.bestFit(rest[i]))
		rest = slices.Delete(rest, i, i+1)
	}
}

type planner struct {
	runners []Runner
	org     map[string]int64
	held    map[string]bool
	out     []Decision
}

func (p *planner) room(i int) resources.Size {
	return p.runners[i].Capacity.Sub(p.runners[i].Used)
}

// eligible is a runner of the request's pool, not stopping, that can run it.
func eligible(rn *Runner, r Request) bool {
	if rn.Pool != r.Pool || rn.Draining {
		return false
	}
	for _, n := range r.Needs {
		if n.Why(rn.RunnerFacts) != "" {
			return false
		}
	}
	return true
}

// bestFit is the eligible runner r fits and leaves least room on, or -1.
// Runners held for another request don't count.
func (p *planner) bestFit(r Request) int {
	best := -1
	for i := range p.runners {
		rn := &p.runners[i]
		if p.held[rn.ID] || !eligible(rn, r) || !r.Size.Fits(p.room(i)) {
			continue
		}
		if best < 0 {
			best = i
			continue
		}
		a, b := p.room(i).Sub(r.Size).Weight(), p.room(best).Sub(r.Size).Weight()
		if a < b || a == b && (rn.Attempts < p.runners[best].Attempts || rn.Attempts == p.runners[best].Attempts && rn.ID < p.runners[best].ID) {
			best = i
		}
	}
	return best
}

func (p *planner) place(r Request, i int) {
	p.runners[i].Used = p.runners[i].Used.Add(r.Size)
	p.runners[i].Attempts++
	p.org[r.Org] += r.Size.CPU
	p.out = append(p.out, Decision{ID: r.ID, Runner: p.runners[i].ID})
}

// next is the request that gets room next, or -1 if none fits.
func (p *planner) next(queue []Request) int {
	best := -1
	for i, r := range queue {
		if best >= 0 && r.Priority > queue[best].Priority {
			break
		}
		if p.bestFit(r) < 0 {
			continue
		}
		// The queue is by priority and age, so an earlier one wins a tie.
		if best < 0 || p.share(r) < p.share(queue[best]) {
			best = i
		}
	}
	return best
}

func (p *planner) share(r Request) float64 {
	weight := r.Weight
	if weight <= 0 {
		weight = 1
	}
	return float64(p.org[r.Org]) / weight
}

// starving handles a request that has waited too long.
func (p *planner) starving(r Request) {
	if i := p.bestFit(r); i >= 0 {
		p.place(r, i)
		return
	}
	var candidates, pool []int
	for i := range p.runners {
		rn := &p.runners[i]
		if rn.Pool != r.Pool || rn.Draining {
			continue
		}
		pool = append(pool, i)
		if eligible(rn, r) && r.Size.Fits(rn.Capacity) {
			candidates = append(candidates, i)
		}
	}
	if len(pool) == 0 {
		// None reporting now; one may come.
		return
	}
	if len(candidates) == 0 {
		p.out = append(p.out, Decision{ID: r.ID, Failure: p.neverFits(r, pool)})
		return
	}
	reserve := -1
	for _, i := range candidates {
		if p.held[p.runners[i].ID] {
			continue
		}
		if reserve < 0 || p.room(i).Weight() > p.room(reserve).Weight() {
			reserve = i
		}
	}
	if reserve >= 0 {
		p.held[p.runners[reserve].ID] = true
	}
}

func (p *planner) neverFits(r Request, pool []int) string {
	var why []string
	var largest resources.Size
	for _, i := range pool {
		rn := &p.runners[i]
		if rn.Capacity.Weight() > largest.Weight() {
			largest = rn.Capacity
		}
		for _, n := range r.Needs {
			if w := n.Why(rn.RunnerFacts); w != "" {
				why = append(why, w)
			}
		}
	}
	if len(why) > 0 {
		slices.Sort(why)
		return fmt.Sprintf("no runner of pool %q can run its connectors: %s", r.Pool, strings.Join(slices.Compact(why), "; "))
	}
	return fmt.Sprintf("its size, %s, doesn't fit any runner of pool %q; the largest has %s", r.Size, r.Pool, largest)
}
