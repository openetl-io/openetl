package hostservices

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	minRate     = rate.Limit(0.1)
	adjustEvery = time.Second
)

// limiter is a rate limit that adapts to the API: it halves when a response
// is throttled and creeps back up while none are, at most once a second
// either way. A key with no declared rate starts unlimited, and its first
// throttle halves the rate it was sending at.
type limiter struct {
	mu      sync.Mutex
	ceiling rate.Limit
	lim     *rate.Limiter
	// No request starts before this.
	paused   time.Time
	adjusted time.Time
	// Requests sent in the one-second window starting at second, and in the
	// one before it.
	second     time.Time
	sent, prev int
}

func newLimiter(ceiling rate.Limit) *limiter {
	return &limiter{ceiling: ceiling, lim: rate.NewLimiter(ceiling, 1)}
}

// reserve takes the next slot and returns how long to wait for it, and a
// function that gives the slot back.
func (l *limiter) reserve(now time.Time) (time.Duration, func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	at := now
	if l.paused.After(at) {
		at = l.paused
	}
	r := l.lim.ReserveN(at, 1)
	switch d := now.Sub(l.second); {
	case d >= 2*adjustEvery:
		l.prev, l.sent, l.second = 0, 0, now
	case d >= adjustEvery:
		l.prev, l.sent, l.second = l.sent, 0, now
	}
	l.sent++
	return r.DelayFrom(now), func() { r.CancelAt(now) }
}

// throttled pauses the key for after, and lowers its rate.
func (l *limiter) throttled(now time.Time, after time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if p := now.Add(after); p.After(l.paused) {
		l.paused = p
	}
	if now.Sub(l.adjusted) < adjustEvery {
		return
	}
	cur := l.lim.Limit()
	if cur == rate.Inf {
		cur = rate.Limit(max(l.sent, l.prev))
	}
	l.lim.SetLimitAt(now, max(minRate, cur/2))
	l.adjusted = now
}

// ok raises the key's rate towards its ceiling.
func (l *limiter) ok(now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cur := l.lim.Limit()
	if cur >= l.ceiling || now.Sub(l.adjusted) < adjustEvery {
		return
	}
	l.lim.SetLimitAt(now, min(l.ceiling, cur+max(1, cur/20)))
	l.adjusted = now
}

func (l *limiter) limit() rate.Limit { return l.lim.Limit() }
