//go:build e2e && chaos

package e2e

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/openetl-io/openetl/test/e2e/ledger"
)

// Crash points, at the commit protocol's steps. The runner writing the
// pipeline's checkpoints dies there.
const (
	// An epoch is staged; its record isn't written.
	crashStaged = "staged"
	// The record is written; HEAD doesn't point to it.
	crashRecorded = "recorded"
	// HEAD points to the record; the destination's Commit hasn't started.
	crashHead = "head"
	// Up to 300 ms after HEAD's swap: during the destination's Commit, or
	// just after it.
	crashCommit = "commit"
)

var crashPoints = []string{crashStaged, crashRecorded, crashHead, crashCommit}

// Crash points of an attempt on workers, where the worker that commits dies
// rather than the coordinator, which writes the checkpoints.
const (
	// HEAD points to the record; the worker hasn't been asked to commit.
	crashWorkerHead = "worker head"
	// Up to 300 ms after HEAD's swap: while the worker commits, or just
	// after.
	crashWorkerCommit = "worker commit"
)

// Misses in a row after which a crash point isn't armed again: what it
// kills can't be found, as when the suite can't tell which pod runs the
// pipeline.
const crashMisses = 5

// What an object-store proxy does with a request.
type action struct {
	kind int
	// A kill's crash point. It's counted when it fires, and again once it
	// has killed, as "killed at crash POINT", or found nothing to kill: see
	// faultBook.missed.
	point string
	// When the crash point fired.
	at time.Time
	// A kill is of the worker that commits the attempt's epochs, which
	// sees the response go to its coordinator, rather than of what wrote.
	worker bool
	delay  time.Duration
	// actKillLater's delay after the response.
	later  time.Duration
	status int
	code   string
}

const (
	// Forward the request.
	_ = iota
	// Kill the runner without forwarding.
	actKillBefore
	// Forward, then kill the runner before it sees the response.
	actKillAfter
	// Forward and respond, then kill the runner a moment later.
	actKillLater
	// Forward, then drop the connection: the write's outcome is unknown to
	// the runner.
	actDrop
	// Respond with an error, without forwarding.
	actStatus
)

// faultBook holds the faults each pipeline's run has armed, and what the
// proxies saw of it: which slot writes its checkpoints, its records, and
// how long it took a later attempt to write one after each kill.
type faultBook struct {
	mu    sync.Mutex
	rng   *rand.Rand
	pipes map[string]*pipeFaults
}

type pipeFaults struct {
	// Crash points armed, in the order they fire.
	armed []string
	// The chance that a request under the pipeline gets an injected
	// object-store fault.
	noise float64
	// Slots that wrote a record, and not yet the HEAD swap after it.
	recorded  map[int]bool
	host      int
	lastWrite time.Time
	records   ledger.Records
	counts    map[string]int
	// Each crash point's misses since it last killed.
	misses map[string]int
	// A strict run's: its crash points fire only while it's ready.
	strict bool
	// The run's faults have stopped: see quiet.
	quiet bool
}

func newFaultBook(seed uint64) *faultBook {
	return &faultBook{rng: rand.New(rand.NewPCG(seed, 0xfa17)), pipes: map[string]*pipeFaults{}}
}

func (b *faultBook) add(pipeline string, noise float64, strict bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pipes[pipeline] = &pipeFaults{noise: noise, recorded: map[int]bool{}, host: -1, counts: map[string]int{}, misses: map[string]int{}, strict: strict}
}

// remove stops faulting the pipeline, and returns what it injected and the
// resume times it measured.
func (b *faultBook) remove(pipeline string) (map[string]int, []time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	f := b.pipes[pipeline]
	delete(b.pipes, pipeline)
	if f == nil {
		return nil, nil
	}
	return f.counts, f.records.Resumes()
}

func (b *faultBook) arm(pipeline, point string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if f := b.pipes[pipeline]; f != nil {
		f.armed = append(f.armed, point)
	}
}

// quiet disarms the pipeline's crash point and stops its noise.
func (b *faultBook) quiet(pipeline string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if f := b.pipes[pipeline]; f != nil {
		for _, p := range f.armed {
			f.counts["missed crash "+p]++
		}
		f.armed, f.noise, f.quiet = nil, 0, true
	}
}

// missed notes that the crash point act fired for didn't land, as why
// says: what it was to kill had gone, most likely to another run's kill of
// the runner they share. Its landing is retracted, and until the run is
// quiet, or the point has missed crashMisses times in a row, it's armed
// again, first, for the pipeline's next request it fits. Either way the
// miss is counted: as re-armed, or as missed, which fails a strict run.
func (b *faultBook) missed(pipeline string, act action, why string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	f := b.pipes[pipeline]
	if f == nil {
		return
	}
	f.records.Unland(act.at)
	f.misses[act.point]++
	switch n := f.misses[act.point]; {
	case n >= crashMisses:
		f.counts[fmt.Sprintf("missed crash %s: %s, %d times in a row", act.point, why, n)]++
	case f.quiet:
		f.counts["missed crash "+act.point+": "+why]++
	default:
		f.counts["re-armed crash "+act.point+": "+why]++
		f.armed = append([]string{act.point}, f.armed...)
	}
}

// killed notes that the crash point act fired for killed.
func (b *faultBook) killed(pipeline string, act action) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if f := b.pipes[pipeline]; f != nil {
		f.counts["killed at crash "+act.point]++
		delete(f.misses, act.point)
	}
}

// counts is what the pipeline's run has injected so far.
func (b *faultBook) counts(pipeline string) map[string]int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if f := b.pipes[pipeline]; f != nil {
		return maps.Clone(f.counts)
	}
	return nil
}

func (b *faultBook) count(pipeline, what string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if f := b.pipes[pipeline]; f != nil {
		f.counts[what]++
	}
}

// decide is what the proxy does with a request under the pipeline, as it
// arrives: of kind, and for a record or HEAD, written by the attempt of
// generation gen, 0 if unknown.
func (b *faultBook) decide(slot int, pipeline string, kind int, gen int64) action {
	b.mu.Lock()
	defer b.mu.Unlock()
	f := b.pipes[pipeline]
	if f == nil {
		return action{}
	}
	switch {
	case kind != reqOther && gen > 0:
		f.records.Seen(gen)
	case kind == reqHead:
		f.counts["HEAD without a generation"]++
	}
	armed := ""
	if len(f.armed) > 0 && (!f.strict || f.records.Ready()) {
		armed = f.armed[0]
	}
	// Every crash point kills: it lands, and the attempt's death is noted,
	// as it fires, before the kill. A kill that finds nothing retracts it.
	fire := func(a action) action {
		f.counts["crash "+armed]++
		f.armed, a.point, a.at = f.armed[1:], armed, time.Now()
		f.records.LandedKill(a.at)
		return a
	}
	switch {
	case armed == crashStaged && kind == reqRecord:
		return fire(action{kind: actKillBefore})
	case armed == crashRecorded && kind == reqRecord:
		return fire(action{kind: actKillAfter})
	case armed == crashHead && kind == reqHead && f.recorded[slot]:
		return fire(action{kind: actKillAfter})
	case armed == crashCommit && kind == reqHead && f.recorded[slot]:
		return fire(action{kind: actKillLater, later: upTo(b.rng, 300*time.Millisecond)})
	case armed == crashWorkerHead && kind == reqHead && f.recorded[slot]:
		return fire(action{kind: actKillAfter, worker: true})
	case armed == crashWorkerCommit && kind == reqHead && f.recorded[slot]:
		return fire(action{kind: actKillLater, worker: true, later: upTo(b.rng, 300*time.Millisecond)})
	}
	if f.noise == 0 || b.rng.Float64() >= f.noise {
		return action{}
	}
	switch n := b.rng.IntN(4); {
	case n == 0:
		f.counts["s3 503"]++
		return action{kind: actStatus, status: 503, code: "SlowDown"}
	case n == 1 && kind != reqOther:
		f.counts["s3 unknown outcome"]++
		return action{kind: actDrop}
	case n == 2 && kind == reqHead:
		f.counts["s3 412"]++
		return action{kind: actStatus, status: 412, code: "PreconditionFailed"}
	default:
		f.counts["s3 latency"]++
		return action{delay: 200*time.Millisecond + upTo(b.rng, 2*time.Second)}
	}
}

// wrote notes a successful write under the pipeline by the slot's runner:
// for a HEAD, one naming a record of generation committed, 0 if none.
func (b *faultBook) wrote(slot int, pipeline string, kind int, committed int64) {
	if kind == reqOther {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	f := b.pipes[pipeline]
	if f == nil {
		return
	}
	f.host, f.lastWrite = slot, time.Now()
	switch kind {
	case reqRecord:
		f.recorded[slot] = true
	case reqHead:
		f.recorded[slot] = false
		f.records.Committed(committed, time.Now())
	}
}

// ready reports whether one of a strict run's faults may land on the
// pipeline: see ledger.Records.Ready.
func (b *faultBook) ready(pipeline string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	f := b.pipes[pipeline]
	return f != nil && f.records.Ready()
}

// resuming reports whether a kill of the pipeline's attempt waits for a
// later attempt's record.
func (b *faultBook) resuming(pipeline string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	f := b.pipes[pipeline]
	return f != nil && f.records.Resuming()
}

// land notes that one of the pipeline's run's own faults lands now, and
// with kill, that it kills the pipeline's attempt, before it does. With
// ready, it lands only while the pipeline is ready (see
// ledger.Records.Ready). It reports whether it landed.
func (b *faultBook) land(pipeline string, kill, ready bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	f := b.pipes[pipeline]
	switch {
	case f == nil:
		return true
	case ready && !f.records.Ready():
		return false
	case kill:
		f.records.LandedKill(time.Now())
	default:
		f.records.Landed()
	}
	return true
}

// strict is what a strict run of the pipeline fails of so far: see
// ledger.Records.Strict.
func (b *faultBook) strict(pipeline string, faults, kills int) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if f := b.pipes[pipeline]; f != nil {
		return f.records.Strict(faults, kills)
	}
	return []string{"the pipeline isn't in the fault book"}
}

// died notes that the slot's runner is being killed, which starts the
// resume clock of each pipeline it was writing, again if its runner died
// before, so a resume is timed from its runner's last death. It's how a
// run sees another run's kill of the runner they share; its own kills are
// noted as they land.
func (b *faultBook) died(slot int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	for _, f := range b.pipes {
		if f.host == slot && now.Sub(f.lastWrite) < 10*time.Second {
			f.records.Killed(now)
		}
	}
}

// upTo is a random duration in [0, d).
func upTo(rng *rand.Rand, d time.Duration) time.Duration { return time.Duration(rng.Int64N(int64(d))) }
