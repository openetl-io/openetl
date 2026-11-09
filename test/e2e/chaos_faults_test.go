//go:build e2e && chaos

package e2e

import (
	"math/rand/v2"
	"sync"
	"time"
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

// What an object-store proxy does with a request.
type action struct {
	kind  int
	delay time.Duration
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
// proxies saw of it: which slot writes its checkpoints, and how long it
// took to write one again after that slot's runner died.
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
	// When its host's runner was killed, until a record shows the pipeline
	// resumed.
	killed  time.Time
	counts  map[string]int
	resumes []time.Duration
}

func newFaultBook(seed uint64) *faultBook {
	return &faultBook{rng: rand.New(rand.NewPCG(seed, 0xfa17)), pipes: map[string]*pipeFaults{}}
}

func (b *faultBook) add(pipeline string, noise float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pipes[pipeline] = &pipeFaults{noise: noise, recorded: map[int]bool{}, host: -1, counts: map[string]int{}}
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
	return f.counts, f.resumes
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
		f.armed, f.noise = nil, 0
	}
}

func (b *faultBook) count(pipeline, what string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if f := b.pipes[pipeline]; f != nil {
		f.counts[what]++
	}
}

func (b *faultBook) decide(slot int, pipeline string, kind int) action {
	b.mu.Lock()
	defer b.mu.Unlock()
	f := b.pipes[pipeline]
	if f == nil {
		return action{}
	}
	armed := ""
	if len(f.armed) > 0 {
		armed = f.armed[0]
	}
	// A runner killed before its write is the pipeline's, though it may
	// not have written for it yet; died times the others.
	fire := func(a action) action {
		f.counts["crash "+armed]++
		f.armed = f.armed[1:]
		if a.kind == actKillBefore {
			f.killed = time.Now()
		}
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

// wrote notes a successful write under the pipeline by the slot's runner.
func (b *faultBook) wrote(slot int, pipeline string, kind int) {
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
		if !f.killed.IsZero() {
			f.resumes = append(f.resumes, time.Since(f.killed))
			f.killed = time.Time{}
		}
	case reqHead:
		f.recorded[slot] = false
	}
}

// died notes that the slot's runner is being killed, which starts the
// resume clock of each pipeline it was writing, again if its runner died
// before, so a resume is timed from its runner's last death.
func (b *faultBook) died(slot int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	for _, f := range b.pipes {
		if f.host == slot && now.Sub(f.lastWrite) < 10*time.Second {
			f.killed = now
		}
	}
}

// podDied starts the pipeline's resume clock: a pod runs only its own attempt.
func (b *faultBook) podDied(pipeline string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if f := b.pipes[pipeline]; f != nil {
		f.killed = time.Now()
	}
}

// upTo is a random duration in [0, d).
func upTo(rng *rand.Rand, d time.Duration) time.Duration { return time.Duration(rng.Int64N(int64(d))) }
