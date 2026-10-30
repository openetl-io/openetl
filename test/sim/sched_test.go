package sim

import (
	"cmp"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing/synctest"
	"time"
)

// Every call a fake makes to the outside world is a yield point. The
// scheduler lets one waiting call proceed at a time, chosen by the seed, and
// decides whether it fails.

type decision uint8

const (
	proceed        decision = iota
	failBefore              // not applied; the caller gets a transient error
	unknownOutcome          // applied, but the caller gets a timeout
	crashBefore             // the attempt dies before the call takes effect
	crashAfter              // the call takes effect, then the attempt dies
)

func (d decision) String() string {
	return [...]string{"proceed", "fail-before", "unknown-outcome", "crash-before", "crash-after"}[d]
}

var (
	errTransient = errors.New("sim: transient failure")
	errTimeout   = errors.New("sim: timeout, outcome unknown")
	errCrashed   = errors.New("sim: attempt crashed")
)

// outcome applies a call according to d and returns what the caller sees.
func outcome(d decision, apply func() error) error {
	switch d {
	case proceed:
		return apply()
	case failBefore:
		return errTransient
	case unknownOutcome:
		_ = apply()
		return errTimeout
	case crashAfter:
		_ = apply()
		return errCrashed
	default:
		return errCrashed
	}
}

type waiter struct {
	attempt int
	op      string
	seq     uint64
	reply   chan decision
}

const (
	maxSteps    = 200_000
	maxAttempts = 60
	timeLimit   = 48 * time.Hour // virtual time
	faultPct    = 4
)

type scheduler struct {
	rng *rand.Rand

	mu      sync.Mutex
	waiting []*waiter
	seq     uint64
	crashed map[int]bool
	dead    bool // the seed failed; every call crashes

	faults  int // faults left to inject
	zombies int // replacements left to start while the current attempt runs
	trace   []string
}

func newScheduler(rng *rand.Rand, faults, zombies int) *scheduler {
	return &scheduler{rng: rng, crashed: make(map[int]bool), faults: faults, zombies: zombies}
}

func (s *scheduler) tracef(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trace = append(s.trace, fmt.Sprintf("%s "+format, append([]any{time.Now().Format("15:04:05.000")}, args...)...))
}

// point blocks until the scheduler lets the caller proceed.
func (s *scheduler) point(attempt int, op string) decision {
	s.mu.Lock()
	if s.crashed[attempt] || s.dead {
		s.mu.Unlock()
		return crashBefore
	}
	w := &waiter{attempt: attempt, op: op, seq: s.seq, reply: make(chan decision, 1)}
	s.seq++
	s.waiting = append(s.waiting, w)
	s.mu.Unlock()
	return <-w.reply
}

// release removes w from the waiting set and sends it d. A crash releases
// every other waiter of the same attempt too.
func (s *scheduler) release(w *waiter, d decision) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d == crashBefore || d == crashAfter {
		s.crashed[w.attempt] = true
	}
	kept := s.waiting[:0]
	for _, x := range s.waiting {
		switch {
		case x == w:
			x.reply <- d
		case s.crashed[x.attempt]:
			x.reply <- crashBefore
		default:
			kept = append(kept, x)
		}
	}
	s.waiting = kept
}

func (s *scheduler) decide(w *waiter) decision {
	if s.faults > 0 && s.rng.IntN(100) < faultPct {
		s.faults--
		d := []decision{failBefore, unknownOutcome, crashBefore, crashAfter}[s.rng.IntN(4)]
		s.tracef("a%d %s: %s", w.attempt, w.op, d)
		return d
	}
	return proceed
}

type finish struct {
	attempt int
	err     error
}

// run plays the role of Temporal: it starts attempts, retries the newest one
// when it fails, and sometimes starts a replacement while the current attempt
// is still running, which leaves the old one behind as a zombie. It returns
// once the newest attempt has succeeded and every attempt has stopped.
func (s *scheduler) run(start func(attempt int) error) error {
	done := make(chan finish, maxAttempts+1)
	alive, newest, newestOK := 0, 0, false
	launch := func() {
		newest++
		alive++
		k := newest
		s.tracef("a%d started", k)
		go func() { done <- finish{k, start(k)} }()
	}
	var failure error
	t0 := time.Now()
	launch()
	for step := 0; ; step++ {
		synctest.Wait()
		for drained := false; !drained; {
			select {
			case f := <-done:
				alive--
				s.tracef("a%d finished: %v", f.attempt, f.err)
				if f.attempt != newest || failure != nil {
					continue
				}
				if f.err == nil {
					newestOK = true
					continue
				}
				if newest >= maxAttempts {
					failure = fmt.Errorf("gave up after %d attempts; last error: %w", newest, f.err)
					continue
				}
				launch()
			default:
				drained = true
			}
		}
		if failure != nil {
			// Crash every attempt and wait for its goroutines to exit, so the
			// bubble doesn't end deadlocked.
			if alive == 0 {
				return failure
			}
			if !s.crashAll() {
				time.Sleep(time.Minute)
			}
			continue
		}
		if newestOK && alive == 0 {
			return nil
		}
		if step > maxSteps || time.Since(t0) > timeLimit {
			failure = fmt.Errorf("stuck: %d steps, %v of virtual time, %d attempts alive", step, time.Since(t0), alive)
			continue
		}

		s.mu.Lock()
		ws := slices.Clone(s.waiting)
		s.mu.Unlock()
		if len(ws) == 0 {
			time.Sleep(time.Second) // everyone is waiting for a timer
			continue
		}
		switch r := s.rng.IntN(1000); {
		case r < 30: // let timers fire: commit intervals, forced barriers
			time.Sleep(time.Duration(1+s.rng.IntN(30)) * time.Second)
			continue
		case r < 45 && s.zombies > 0 && alive == 1 && !newestOK:
			s.zombies--
			s.tracef("replacing a%d while it runs", newest)
			launch()
			continue
		}
		slices.SortFunc(ws, func(a, b *waiter) int {
			return cmp.Or(cmp.Compare(a.attempt, b.attempt), strings.Compare(a.op, b.op), cmp.Compare(a.seq, b.seq))
		})
		w := ws[s.rng.IntN(len(ws))]
		s.release(w, s.decide(w))
	}
}

// crashAll makes every current and future call crash, and releases every
// waiter. It reports whether anyone was waiting.
func (s *scheduler) crashAll() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dead = true
	for _, w := range s.waiting {
		w.reply <- crashBefore
	}
	n := len(s.waiting)
	s.waiting = nil
	return n > 0
}
