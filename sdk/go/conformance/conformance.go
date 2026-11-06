// Package conformance is the connector conformance kit: checks that drive a
// connector over the connector protocol, the way the platform does, and
// assert what the protocol promises. It knows nothing of the connector's
// language, so it certifies any connector it can start.
//
// A connector's tests put data upstream and run a SourceSuite, or give a
// DestinationSuite a way to read the destination back, and call Test:
//
//	conformance.DestinationSuite{
//		Launch: conformance.Exec(conformance.Build(t, "./cmd/destination-acme")),
//		Config: cfg,
//		Read:   readTable,
//	}.Test(t)
//
// Run runs the same checks outside go test.
package conformance

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// T is what a check reports to: a *testing.T, or Run's reporter.
type T interface {
	Helper()
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
	Logf(format string, args ...any)
	Skipf(format string, args ...any)
	Failed() bool
	Context() context.Context
	Cleanup(func())
}

// Check is one named check of a suite.
type Check struct {
	Name string
	Run  func(T)
}

// Result is how a check went, for Run.
type Result struct {
	Check   string
	Failed  bool
	Skipped bool
	// What the check logged and reported, in order. Flags start with
	// "flag:".
	Log []string
}

// flagf reports something that doesn't fail a check but that the
// connector's author should know, such as a stream that can't resume
// mid-read.
func flagf(t T, format string, args ...any) {
	t.Helper()
	t.Logf("flag: "+format, args...)
}

func test(t *testing.T, checks []Check) {
	for _, c := range checks {
		t.Run(c.Name, func(t *testing.T) { c.Run(t) })
	}
}

func run(ctx context.Context, checks []Check, report func(Result)) {
	for _, c := range checks {
		r := &reporter{ctx: ctx}
		done := make(chan struct{})
		go func() {
			defer close(done)
			defer r.cleanup()
			c.Run(r)
		}()
		<-done
		report(Result{Check: c.Name, Failed: r.failed, Skipped: r.skipped, Log: r.log})
	}
}

// reporter is a T outside go test. Fatalf and Skipf end the check's
// goroutine, as testing's do.
type reporter struct {
	ctx      context.Context
	mu       sync.Mutex
	failed   bool
	skipped  bool
	log      []string
	cleanups []func()
}

func (r *reporter) Helper() {}

func (r *reporter) Errorf(format string, args ...any) {
	r.add(format, args...)
	r.mu.Lock()
	r.failed = true
	r.mu.Unlock()
}

func (r *reporter) Fatalf(format string, args ...any) {
	r.Errorf(format, args...)
	runtime.Goexit()
}

func (r *reporter) Logf(format string, args ...any) { r.add(format, args...) }

func (r *reporter) Skipf(format string, args ...any) {
	r.add(format, args...)
	r.mu.Lock()
	r.skipped = true
	r.mu.Unlock()
	runtime.Goexit()
}

func (r *reporter) Failed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failed
}

func (r *reporter) Context() context.Context { return r.ctx }

func (r *reporter) Cleanup(f func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanups = append(r.cleanups, f)
}

func (r *reporter) add(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.log = append(r.log, strings.TrimSpace(fmt.Sprintf(format, args...)))
}

func (r *reporter) cleanup() {
	r.mu.Lock()
	fs := r.cleanups
	r.cleanups = nil
	r.mu.Unlock()
	for i := len(fs) - 1; i >= 0; i-- {
		fs[i]()
	}
}
