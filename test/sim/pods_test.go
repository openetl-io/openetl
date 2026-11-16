package sim

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/openetl-io/openetl/internal/orchestration/dispatch"
	"github.com/openetl-io/openetl/internal/orchestration/sizing"
	"github.com/openetl-io/openetl/internal/platform/resources"
)

// The pods kind models how an attempt on workers is placed and run as pods.
// The placer is dispatch.Plan itself: it gives an attempt's coordinator and
// every worker room on one pod launcher, or none of them, and fails an
// attempt no launcher could ever take. The rest is a model of the
// dispatcher's rows, the launchers and the pods: a launcher makes a
// placement's pods, workers' first, deletes those it made if one can't be
// made, and on each report reaps the pods of placements that ended. A
// placement holds its room in the launcher's caps from when it's placed
// until it has ended and a report lists no pod of it that hasn't. A
// coordinator's pod claims its placement before its start deadline, and
// checks first that the job can run on workers: if not, the same attempt is
// placed again on one pod. It says where its workers join, fails the attempt
// if one doesn't join in time or is lost, and runs it once all have. A
// worker's pod asks for its placement's target until it has one, and joins
// it. Pods start late or never, die, and fail to be made; a coordinator
// finds it has no room and gives its attempt back to be placed again. Each
// pipeline's syncs ask for workers or not, as rescaling it does, and the
// control plane knows whether its job can run on them, or leaves that to
// the coordinator.
//
// The model's contract, checked on every seed:
//   - caps: the attempts placed on a launcher hold no more pods or CPU,
//     counting each worker and the coordinator, than it takes, and nor do
//     its pods that haven't ended with those it's yet to make;
//   - gang: a launch makes all of a placement's pods, or leaves none;
//   - join: a worker is told to join only its own placement's
//     coordinator, and joins no other;
//   - liveness: every sync ends, and succeeds unless no launcher could ever
//     take its attempt, which then fails for that;
//   - reaped: once every sync has ended, no pod is left.

const (
	pTicks = 3000
	// Ticks: a placement's coordinator has to claim it; workers have to
	// join; a worker or coordinator may go without a heartbeat; a worker's
	// pod waits for its target; between a launcher's reports; before a
	// request may reserve a launcher or fail.
	pStartTimeout = 8
	pJoinTimeout  = 6
	pBeatTimeout  = 3
	pWait         = 30
	pReport       = 5
	pReserveAfter = 5
	pMaxAttempts  = 25
	// Ticks after the last sync ends for the launchers to reap its pods.
	pReapTicks = 20
)

// podsBroken each break the placement or the pods' protocol in one way, and
// name the invariant that must catch it.
var podsBroken = []struct{ name, invariant, what string }{
	{"gang-as-one-pod", "caps", "the placer counts an attempt on workers as its coordinator's pod alone"},
	{"launch-keeps-made", "gang", "a launch that fails part-way leaves the pods it made"},
	{"one-pod-as-failure", "liveness", "a coordinator that finds its job can't run on workers fails the attempt, rather than having it placed again on one pod"},
	{"stale-target", "join", "an attempt placed again from running keeps where its workers joined its last coordinator"},
	{"join-unbounded", "liveness", "a coordinator waits for its workers to join for as long as it takes"},
	{"workers-unreaped", "reaped", "a launcher reaps an ended placement's coordinator's pod, not its workers'"},
	{"room-at-end", "caps", "the placer gives a placement's room back when it ends, not once its launcher reports its pods gone or ended"},
}

type pState uint8

const (
	pPending pState = iota
	pPlaced
	pRunning
	pFinished
)

// pPipe is a pipeline: its job's size, whether the control plane knows if
// its job can run on workers, whether it can, and the workers each of its
// syncs asks for, 0 or more than 1.
type pPipe struct {
	name        string
	size        resources.Size
	known, fits bool
	syncs       []int
	sync        int
	d           *pDispatch
	// Each sync's end, "ok" or why it failed, and the workers its last
	// attempt was placed on.
	results []string
	workers []int
}

// pDispatch is an attempt's dispatch row.
type pDispatch struct {
	id            string
	pipe          *pPipe
	sync, attempt int
	// Its workers as placed, 0 on one pod, and why it runs on one pod
	// although its spec asks for workers.
	workers  int
	onePod   string
	state    pState
	launcher int
	// Placements so far; a pod runs the one it was made for.
	placement       int
	launched        bool
	launchFails     int
	deadline, since int
	target          *pTarget
	runner          *pPod
	outcome         string
}

// pTarget is where the workers of a placement join its coordinator.
type pTarget struct {
	placement int
	co        *pPod
}

type pcPhase uint8

const (
	pcClaim pcPhase = iota
	pcJoin
	pcRun
)

// pPod is a pod a launcher made for a placement: its coordinator's, worker
// 0, or a worker's, from 1.
type pPod struct {
	d                 *pDispatch
	placement, worker int
	launcher          int
	// The tick it starts at; -1: never.
	startAt, since         int
	running, ended, failed bool
	// It told the dispatcher how the attempt ended, and the launcher told
	// it how the pod did.
	result, told bool
	// A coordinator's.
	phase  pcPhase
	opened int
	joined map[int]bool
	beats  map[int]int
	epochs int
	// A worker's: the coordinator it joined.
	co *pPod
	// Its attempt's size, or a coordinator's of workers.
	size resources.Size
}

func (p *pPod) name() string {
	if p.worker == 0 {
		return fmt.Sprintf("%s.p%d", p.d.id, p.placement)
	}
	return fmt.Sprintf("%s.p%d-w%d", p.d.id, p.placement, p.worker)
}

type pLauncher struct {
	id    string
	caps  dispatch.Pods
	pods  []*pPod
	holds []*pHold
}

// pHold is the room a placement holds of its launcher's caps, as the
// dispatcher's launches rows do.
type pHold struct {
	d         *pDispatch
	placement int
	pods      int
	total     resources.Size
}

type pModel struct {
	rng       *rand.Rand
	broken    string
	tick      int
	launchers []*pLauncher
	pipes     []*pPipe
	all       []*pDispatch
	// Faults left: pod kills, pods that never start or start late, pods that
	// can't be made, and coordinators that find no room.
	kills, stuck, slow, launchFaults, noRoom int
	problems, trace                          []string
	stats                                    map[string]int
}

func (m *pModel) tracef(format string, args ...any) {
	m.trace = append(m.trace, fmt.Sprintf("t%d "+format, append([]any{m.tick}, args...)...))
}

func (m *pModel) problem(invariant, format string, args ...any) {
	p := fmt.Sprintf("["+invariant+"] "+format, args...)
	if !slices.Contains(m.problems, p) {
		m.problems = append(m.problems, p)
		m.tracef("PROBLEM %s", p)
	}
}

func newPodsModel(seed uint64, broken string) *pModel {
	rng := rand.New(rand.NewPCG(seed, uint64(len("pods"))))
	pick := func(xs ...int) int { return xs[rng.IntN(len(xs))] }
	m := &pModel{rng: rng, broken: broken, stats: map[string]int{},
		kills: rng.IntN(4), stuck: rng.IntN(3), slow: rng.IntN(3), launchFaults: rng.IntN(4), noRoom: rng.IntN(3)}
	for i := range 1 + rng.IntN(2) {
		caps := dispatch.Pods{MaxPods: pick(0, 4, 6, 8, 12), MaxCPU: int64(pick(0, 6000, 12000, 24000))}
		if rng.IntN(4) == 0 {
			caps.Max = resources.M
		}
		m.launchers = append(m.launchers, &pLauncher{id: fmt.Sprintf("launcher-%d", i), caps: caps})
	}
	sizes := []resources.Size{resources.XS, resources.S, resources.M}
	for i := range 1 + rng.IntN(3) {
		p := &pPipe{name: fmt.Sprintf("pipe%d", i), size: sizes[rng.IntN(len(sizes))], known: rng.IntN(2) == 0, fits: rng.IntN(4) != 0}
		for range 2 + rng.IntN(2) {
			p.syncs = append(p.syncs, pick(0, 2, 3, 4))
		}
		m.pipes = append(m.pipes, p)
	}
	// What each broken variant needs to show.
	first := m.pipes[0]
	switch broken {
	case "gang-as-one-pod":
		m.launchers[0].caps = dispatch.Pods{MaxPods: 3}
		m.launchers = m.launchers[:1]
		first.known, first.fits, first.syncs[0] = true, true, 3
	case "launch-keeps-made":
		m.launchFaults = 3
		first.fits, first.syncs[0] = true, 3
	case "one-pod-as-failure":
		first.known, first.fits, first.syncs[0] = false, false, 2
	case "stale-target":
		m.noRoom = 3
		first.known, first.fits, first.syncs = true, true, []int{3, 3, 3}
	case "join-unbounded":
		m.stuck = 3
		first.fits, first.syncs[0] = true, 3
	case "workers-unreaped":
		first.fits, first.syncs[0] = true, 2
	}
	return m
}

func (m *pModel) String() string {
	var b strings.Builder
	for _, l := range m.launchers {
		fmt.Fprintf(&b, "%s{pods=%d cpu=%d max=%v} ", l.id, l.caps.MaxPods, l.caps.MaxCPU, l.caps.Max)
	}
	for _, p := range m.pipes {
		fmt.Fprintf(&b, "%s{%s known=%v fits=%v syncs=%v} ", p.name, p.size.Name(), p.known, p.fits, p.syncs)
	}
	fmt.Fprintf(&b, "kills=%d stuck=%d slow=%d launchFaults=%d noRoom=%d", m.kills, m.stuck, m.slow, m.launchFaults, m.noRoom)
	if m.broken != "" {
		fmt.Fprintf(&b, " broken=%s", m.broken)
	}
	return b.String()
}

// enqueue sends a sync's attempt, as EnqueueWork does: its workers are the
// spec's, unless the control plane knows the job can't run on them.
func (m *pModel) enqueue(p *pPipe, attempt int) {
	d := &pDispatch{id: fmt.Sprintf("%s/s%d/a%d", p.name, p.sync, attempt), pipe: p, sync: p.sync, attempt: attempt,
		workers: p.syncs[p.sync], launcher: -1, since: m.tick}
	if d.workers > 0 && p.known && !p.fits {
		d.workers, d.onePod = 0, "the job can't run on workers"
		m.stats["one pod, at enqueue"]++
	}
	p.d = d
	m.all = append(m.all, d)
	m.tracef("%s enqueued on %d workers", d.id, d.workers)
}

// orchestrate plays the sync workflows: the next attempt of a sync whose
// attempt failed, or the next sync.
func (m *pModel) orchestrate() {
	for _, p := range m.pipes {
		if p.sync >= len(p.syncs) || p.d.state != pFinished {
			continue
		}
		d := p.d
		switch {
		case d.outcome == "ok", strings.HasPrefix(d.outcome, "never fits"):
			p.results = append(p.results, d.outcome)
		case d.attempt < pMaxAttempts:
			m.enqueue(p, d.attempt+1)
			continue
		default:
			p.results = append(p.results, "gave up after "+d.outcome)
		}
		p.workers = append(p.workers, d.workers)
		if p.sync++; p.sync < len(p.syncs) {
			m.enqueue(p, 1)
		}
	}
}

func (m *pModel) finish(d *pDispatch, outcome string) {
	d.state, d.outcome, d.target = pFinished, outcome, nil
	m.tracef("%s finished: %s", d.id, outcome)
}

// held is what an attempt's placement holds of its launcher as the placer
// counts it.
func (m *pModel) held(d *pDispatch) (pods int, total resources.Size) {
	if m.broken == "gang-as-one-pod" {
		return 1, d.pipe.size
	}
	return needs(d)
}

// needs is what an attempt's placement's pods need between them: each
// worker's and its coordinator's, or its one pod's.
func needs(d *pDispatch) (pods int, total resources.Size) {
	if d.workers == 0 {
		return 1, d.pipe.size
	}
	return d.workers + 1, d.pipe.size.Times(d.workers).Add(sizing.Coordinator)
}

// liveAt says a placement of an attempt still waits to start or runs.
func liveAt(d *pDispatch, placement int) bool {
	return d.placement == placement && (d.state == pPlaced || d.state == pRunning)
}

// place is a placement pass: the launchers and the attempts waiting, as the
// dispatcher reads them, through the placer.
func (m *pModel) place() {
	runners := make([]dispatch.Runner, len(m.launchers))
	for i, l := range m.launchers {
		caps := l.caps
		runners[i] = dispatch.Runner{RunnerFacts: dispatch.RunnerFacts{ID: l.id}, Pool: "default", Pods: &caps}
	}
	var requests []dispatch.Request
	byID := map[string]*pDispatch{}
	for _, d := range m.all {
		switch d.state {
		case pPlaced, pRunning:
			pods, total := m.held(d)
			runners[d.launcher].Used = runners[d.launcher].Used.Add(total)
			runners[d.launcher].Attempts += pods
		case pPending:
			r := dispatch.Request{ID: d.id, Org: "org", Pool: "default", Size: d.pipe.size, Waited: time.Duration(m.tick-d.since) * time.Second}
			if d.workers > 0 && m.broken != "gang-as-one-pod" {
				r.Workers, r.Coordinator = d.workers, sizing.Coordinator
			}
			requests = append(requests, r)
			byID[d.id] = d
		}
	}
	if len(requests) == 0 {
		return
	}
	// What placements that ended hold until their pods are reported gone
	// or ended.
	for i, l := range m.launchers {
		for _, h := range l.holds {
			if !liveAt(h.d, h.placement) && m.broken != "room-at-end" {
				runners[i].Unreaped += h.pods
				runners[i].Used = runners[i].Used.Add(h.total)
			}
		}
	}
	for _, dec := range dispatch.Plan(runners, requests, map[string]int64{}, pReserveAfter*time.Second) {
		d := byID[dec.ID]
		if dec.Runner == "" {
			m.stats["never fits"]++
			m.finish(d, "never fits: "+dec.Failure)
			continue
		}
		d.launcher = slices.IndexFunc(m.launchers, func(l *pLauncher) bool { return l.id == dec.Runner })
		d.state, d.launched, d.launchFails, d.deadline = pPlaced, false, 0, m.tick+pStartTimeout
		d.placement++
		pods, total := m.held(d)
		l := m.launchers[d.launcher]
		l.holds = append(l.holds, &pHold{d: d, placement: d.placement, pods: pods, total: total})
		if d.workers > 0 {
			m.stats["gangs placed"]++
		}
		m.tracef("%s placement %d on %s, %d workers", d.id, d.placement, dec.Runner, d.workers)
	}
}

// returnUnstarted places again what wasn't claimed in time.
func (m *pModel) returnUnstarted() {
	for _, d := range m.all {
		if d.state == pPlaced && m.tick >= d.deadline {
			d.state, d.launcher, d.launched = pPending, -1, false
			m.stats["placements not started in time"]++
			m.tracef("%s placement %d didn't start in time", d.id, d.placement)
		}
	}
}

func (m *pModel) podsOf(l *pLauncher, d *pDispatch, placement int) int {
	n := 0
	for _, p := range l.pods {
		if p.d == d && p.placement == placement {
			n++
		}
	}
	return n
}

// launch makes the pods of what's placed on a launcher and not launched:
// the workers' first, then the coordinator's. A pod that can't be made
// deletes those made; the third time, the attempt is placed again.
func (m *pModel) launch() {
	for _, d := range m.all {
		if d.state != pPlaced || d.launched {
			continue
		}
		l := m.launchers[d.launcher]
		var made []*pPod
		failed := false
		for _, w := range append(seq(1, d.workers), 0) {
			if slices.ContainsFunc(l.pods, func(p *pPod) bool { return p.d == d && p.placement == d.placement && p.worker == w }) {
				continue
			}
			if m.launchFaults > 0 && m.rng.IntN(100) < 20 {
				m.launchFaults--
				failed = true
				break
			}
			p := &pPod{d: d, placement: d.placement, worker: w, launcher: d.launcher, startAt: m.tick + m.rng.IntN(3), size: d.pipe.size}
			if w == 0 && d.workers > 0 {
				p.size = sizing.Coordinator
			}
			switch {
			case m.stuck > 0 && m.rng.IntN(100) < 10:
				m.stuck--
				p.startAt = -1
				m.stats["pods that never start"]++
			case m.slow > 0 && m.rng.IntN(100) < 10:
				m.slow--
				p.startAt = m.tick + pJoinTimeout + pBeatTimeout + 2
				m.stats["pods that start late"]++
			}
			l.pods = append(l.pods, p)
			made = append(made, p)
		}
		if failed {
			if m.broken != "launch-keeps-made" {
				l.pods = slices.DeleteFunc(l.pods, func(p *pPod) bool { return slices.Contains(made, p) })
			}
			m.stats["launches failed"]++
			if n := m.podsOf(l, d, d.placement); n != 0 {
				m.problem("gang", "a failed launch of %s placement %d left %d of its %d pods", d.id, d.placement, n, d.workers+1)
			}
			if d.launchFails++; d.launchFails >= 3 {
				d.state, d.launcher = pPending, -1
				m.tracef("%s placement %d couldn't be launched", d.id, d.placement)
			}
			continue
		}
		d.launched = true
		if n := m.podsOf(l, d, d.placement); n != d.workers+1 {
			m.problem("gang", "%s placement %d launched with %d of its %d pods", d.id, d.placement, n, d.workers+1)
		}
	}
}

func seq(from, to int) []int {
	var out []int
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

// live says a pod's placement still waits to start or runs.
func live(p *pPod) bool { return liveAt(p.d, p.placement) }

// tend is a launcher's report: it lists its pods, which frees the room of
// placements that ended with none of their pods left that hasn't, then
// deletes the pods of placements that ended, and says how a placement's
// pod that ended without its result did.
func (m *pModel) tend() {
	type placement struct {
		d *pDispatch
		n int
	}
	for li, l := range m.launchers {
		holding := map[placement]bool{}
		for _, p := range l.pods {
			if !p.ended {
				holding[placement{p.d, p.placement}] = true
			}
		}
		l.holds = slices.DeleteFunc(l.holds, func(h *pHold) bool {
			return !liveAt(h.d, h.placement) && !holding[placement{h.d, h.placement}]
		})
		for _, p := range slices.Clone(l.pods) {
			if !live(p) {
				if m.broken == "workers-unreaped" && p.worker > 0 {
					continue
				}
				l.pods = slices.DeleteFunc(l.pods, func(x *pPod) bool { return x == p })
				if !p.ended {
					m.end(p, "deleted")
				}
				m.stats["pods reaped"]++
				continue
			}
			if p.ended && !p.told {
				p.told = true
				if d := p.d; (p.failed || p.worker == 0 && !p.result) && d.launcher == li {
					m.stats["launcher reports"]++
					m.finish(d, fmt.Sprintf("transient: pod %s ended without the attempt's result", p.name()))
				}
			}
		}
	}
}

// end ends a pod's process.
func (m *pModel) end(p *pPod, why string) {
	p.ended, p.running = true, false
	m.tracef("%s ended: %s", p.name(), why)
}

// step runs a pod for a tick.
func (m *pModel) step(p *pPod) {
	switch {
	case p.ended:
	case !p.running:
		if p.startAt >= 0 && m.tick >= p.startAt {
			p.running, p.since = true, m.tick
		}
	case p.worker == 0:
		m.coordinate(p)
	default:
		m.work(p)
	}
}

func (m *pModel) coordinate(p *pPod) {
	d := p.d
	switch p.phase {
	case pcClaim:
		if d.placement != p.placement || d.state != pPlaced || !d.launched || m.tick >= d.deadline {
			m.end(p, "its placement moved on")
			return
		}
		d.state, d.runner = pRunning, p
		switch {
		case d.workers == 0:
			p.phase, p.epochs = pcRun, 2+m.rng.IntN(3)
		case !d.pipe.fits && m.broken == "one-pod-as-failure":
			m.report(p, "transient: the job can't run on workers")
		case !d.pipe.fits:
			d.state, d.workers, d.onePod, d.target, d.launcher, d.launched, d.runner = pPending, 0, "the job can't run on workers", nil, -1, false, nil
			p.result = true
			m.end(p, "one pod")
			m.stats["one pod, from the coordinator"]++
			m.tracef("%s placement %d goes again on one pod", d.id, p.placement)
		default:
			d.target = &pTarget{placement: p.placement, co: p}
			p.phase, p.opened, p.joined, p.beats = pcJoin, m.tick, map[int]bool{}, map[int]int{}
		}
	case pcJoin:
		if !m.watch(p) {
			return
		}
		if len(p.joined) == d.workers {
			p.phase, p.epochs = pcRun, 2+m.rng.IntN(4)
			return
		}
		if m.broken != "join-unbounded" && m.tick-p.opened > pJoinTimeout {
			m.stats["workers that didn't join"]++
			m.report(p, "transient: a worker didn't join")
		}
	case pcRun:
		if !m.watch(p) {
			return
		}
		if d.workers > 0 && m.noRoom > 0 && m.rng.IntN(100) < 10 {
			m.noRoom--
			d.state, d.launcher, d.launched, d.runner = pPending, -1, false, nil
			if m.broken != "stale-target" {
				d.target = nil
			}
			p.result = true
			m.end(p, "no room")
			m.stats["attempts given back from running"]++
			m.tracef("%s placement %d is given back", d.id, p.placement)
			return
		}
		if p.epochs--; p.epochs <= 0 {
			if d.workers > 0 {
				m.stats["attempts on workers that succeeded"]++
			}
			m.report(p, "ok")
		}
	}
}

// watch fails the attempt once a joined worker has gone quiet; it reports
// whether the coordinator goes on.
func (m *pModel) watch(p *pPod) bool {
	for _, w := range seq(1, p.d.workers) {
		if p.joined[w] && m.tick-p.beats[w] > pBeatTimeout {
			m.stats["workers lost"]++
			m.report(p, fmt.Sprintf("transient: lost worker %d", w))
			return false
		}
	}
	return true
}

// report is a coordinator's result, which counts if the attempt is still
// its own; it ends the pod.
func (m *pModel) report(p *pPod, outcome string) {
	if d := p.d; d.state == pRunning && d.runner == p {
		m.finish(d, outcome)
	}
	p.result = true
	m.end(p, outcome)
}

func (m *pModel) work(p *pPod) {
	d := p.d
	if p.co != nil {
		if p.co.ended {
			m.end(p, "its coordinator ended")
			return
		}
		p.co.beats[p.worker] = m.tick
		return
	}
	// The API's answer to its poll: its attempt while that placement's,
	// with where it joins once its coordinator has said.
	if d.placement != p.placement || (d.state != pPlaced && d.state != pRunning) || d.workers < p.worker {
		m.end(p, "its placement moved on")
		return
	}
	t := d.target
	if t == nil {
		if m.tick-p.since > pWait {
			m.end(p, "never told where to join")
		}
		return
	}
	// Told another placement's coordinator, the worker dials it: one that
	// has gone, or whatever took its address since.
	if t.placement != p.placement || t.co.d != d {
		m.problem("join", "%s is told to join %s", p.name(), t.co.name())
	}
	if t.co.ended || !t.co.running || t.co.phase != pcJoin {
		m.end(p, "its coordinator isn't taking workers")
		return
	}
	if t.co.placement != p.placement || t.co.d != d {
		m.problem("join", "%s joins %s", p.name(), t.co.name())
	}
	t.co.joined[p.worker], t.co.beats[p.worker] = true, m.tick
	p.co = t.co
	m.stats["workers joined"]++
}

// fault kills a running pod now and then.
func (m *pModel) fault() {
	if m.kills == 0 || m.rng.IntN(100) >= 4 {
		return
	}
	var running []*pPod
	for _, l := range m.launchers {
		for _, p := range l.pods {
			if p.running {
				running = append(running, p)
			}
		}
	}
	if len(running) == 0 {
		return
	}
	p := running[m.rng.IntN(len(running))]
	m.kills--
	p.failed = true
	m.end(p, "killed")
	m.stats["pods killed"]++
	m.tracef("%s killed", p.name())
}

// checkCaps checks what the attempts placed on each launcher hold, each
// worker and coordinator counted.
func (m *pModel) checkCaps() {
	for i, l := range m.launchers {
		pods, cpu := 0, int64(0)
		for _, d := range m.all {
			if (d.state == pPlaced || d.state == pRunning) && d.launcher == i {
				pods += d.workers + 1
				cpu += d.pipe.size.CPU * int64(max(d.workers, 1))
				if d.workers > 0 {
					cpu += sizing.Coordinator.CPU
				}
			}
		}
		if l.caps.MaxPods > 0 && pods > l.caps.MaxPods {
			m.problem("caps", "%s holds %d pods, and takes %d", l.id, pods, l.caps.MaxPods)
		}
		if l.caps.MaxCPU > 0 && cpu > l.caps.MaxCPU {
			m.problem("caps", "%s holds %d mCPU, and takes %d", l.id, cpu, l.caps.MaxCPU)
		}
		// What it has: pods that haven't ended, whatever their placement,
		// and those it's yet to make.
		has, using := 0, resources.Size{}
		for _, p := range l.pods {
			if !p.ended {
				has, using = has+1, using.Add(p.size)
			}
		}
		for _, d := range m.all {
			if d.state == pPlaced && d.launcher == i && !d.launched {
				pods, total := needs(d)
				has, using = has+pods, using.Add(total)
			}
		}
		if l.caps.MaxPods > 0 && has > l.caps.MaxPods {
			m.problem("caps", "%s has %d pods that haven't ended or are to be made, and takes %d", l.id, has, l.caps.MaxPods)
		}
		if l.caps.MaxCPU > 0 && using.CPU > l.caps.MaxCPU {
			m.problem("caps", "%s's pods that haven't ended or are to be made have %d mCPU, and it takes %d", l.id, using.CPU, l.caps.MaxCPU)
		}
	}
}

func (m *pModel) syncsDone() bool {
	return !slices.ContainsFunc(m.pipes, func(p *pPipe) bool { return p.sync < len(p.syncs) })
}

func (m *pModel) podsLeft() []string {
	var out []string
	for _, l := range m.launchers {
		for _, p := range l.pods {
			out = append(out, p.name())
		}
	}
	return out
}

func (m *pModel) run() {
	for _, p := range m.pipes {
		m.enqueue(p, 1)
	}
	doneAt := -1
	for m.tick = 0; m.tick < pTicks; m.tick++ {
		m.orchestrate()
		if m.syncsDone() {
			if doneAt < 0 {
				doneAt = m.tick
			}
			if left := m.podsLeft(); len(left) == 0 {
				return
			} else if m.tick-doneAt > pReapTicks {
				m.problem("reaped", "every sync ended %d ticks ago, and pods are left: %v", m.tick-doneAt, left)
				return
			}
		}
		m.place()
		m.returnUnstarted()
		m.launch()
		if m.tick%pReport == 0 {
			m.tend()
		}
		var pods []*pPod
		for _, l := range m.launchers {
			pods = append(pods, l.pods...)
		}
		m.rng.Shuffle(len(pods), func(i, j int) { pods[i], pods[j] = pods[j], pods[i] })
		for _, p := range pods {
			m.step(p)
		}
		m.fault()
		m.checkCaps()
	}
	var running []string
	for _, p := range m.pipes {
		if p.sync < len(p.syncs) {
			running = append(running, fmt.Sprintf("%s sync %d (%s state %d)", p.name, p.sync, p.d.id, p.d.state))
		}
	}
	m.problem("liveness", "stuck after %d ticks: %v; pods %v", pTicks, running, m.podsLeft())
}

// check checks how each sync ended.
func (m *pModel) check() {
	for _, p := range m.pipes {
		for i, r := range p.results {
			switch {
			case r == "ok":
			case strings.HasPrefix(r, "never fits"):
				workers := p.workers[i]
				for _, l := range m.launchers {
					if workers > 0 && l.caps.AllowsWorkers(workers, p.size, sizing.Coordinator) || workers == 0 && l.caps.Allows(p.size) {
						m.problem("liveness", "%s sync %d failed though %s takes it: %s", p.name, i, l.id, r)
					}
				}
			default:
				m.problem("liveness", "%s sync %d failed: %s", p.name, i, r)
			}
		}
	}
}

func runPods(seed uint64, broken string) seedResult {
	if !slices.ContainsFunc(podsBroken, func(v struct{ name, invariant, what string }) bool { return v.name == broken }) {
		broken = ""
	}
	m := newPodsModel(seed, broken)
	res := seedResult{config: "pods " + m.String(), stats: m.stats}
	m.run()
	m.check()
	res.trace = strings.Join(lastN(m.trace, 400), "\n")
	res.steps = m.trace
	res.problems = m.problems
	if len(m.problems) > 0 {
		ps := m.problems
		if len(ps) > 8 {
			ps = append(slices.Clone(ps[:8]), fmt.Sprintf("… and %d more", len(ps)-8))
		}
		res.failure = strings.Join(ps, "; ")
	}
	return res
}

// TestBrokenPods shows each invariant of the pods kind catches placement or
// pods broken the way it guards against.
func TestBrokenPods(t *testing.T) {
	for _, v := range podsBroken {
		t.Run(v.name, func(t *testing.T) {
			seeds := uint64(max(*seedsFlag, 3000))
			for seed := uint64(1); seed <= seeds; seed++ {
				res := runPods(seed, v.name)
				for _, p := range res.problems {
					if strings.HasPrefix(p, "["+v.invariant+"]") {
						t.Logf("%s: seed %d, %s: %s", v.what, seed, res.config, p)
						return
					}
				}
			}
			t.Errorf("%s: no seed of %d broke the %s invariant", v.what, seeds, v.invariant)
		})
	}
}
