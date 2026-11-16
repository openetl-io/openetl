// Package check runs a connector's checks: a check of one connection, or a
// preflight of a pipeline. A connector declares a list of named checks, and
// Run runs them in order, each to an item that passed, warned, failed or was
// skipped.
//
// A Check's Title and Description, a Failure's Message and Fix, and an
// item's Message are templates: {name}, with name matching [a-z][a-z0-9_]*,
// stands for the param of that name, and a placeholder without a param stays
// as written. Params are shown to people and kept with results, so they
// never hold secrets.
//
// A Check's Run returns nil to pass, Skip's error to skip, Warn's to warn,
// and any other error to fail; Classify gives an error that isn't a *Failure
// a standard reason. A nil *Failure is a nil error only as the error itself:
// inside another error, it's skipped over.
//
// Of, RunList, FromError, FromProto and Report.Proto are for hosts and the
// SDK's serving code, not for a connector's checks.
package check

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
)

// Kind is what a check verifies. Its numbers are the protocol's CheckKind's.
type Kind int

const (
	Configuration Kind = iota + 1
	Connection
	Authentication
	Permission
	Resource
	Compatibility
	Quota
	// A Kind outside Configuration to Custom, zero included, counts as Custom.
	Custom
)

var kindNames = [...]string{Configuration: "configuration", Connection: "connection", Authentication: "authentication",
	Permission: "permission", Resource: "resource", Compatibility: "compatibility", Quota: "quota", Custom: "custom"}

func (k Kind) String() string { return kindNames[k.orCustom()] }

func (k Kind) orCustom() Kind {
	if k < Configuration || k > Custom {
		return Custom
	}
	return k
}

// Status is how an item went. Its numbers are the protocol's CheckStatus's.
type Status int

const (
	Passed Status = iota + 1
	// A failure that doesn't fail the report: an Advisory check's, or Warn's.
	Warning
	Failed
	// Not checked: a check it needs didn't pass, Run returned Skip's error,
	// or the run was cancelled first.
	Skipped
)

var statusNames = [...]string{Passed: "passed", Warning: "warning", Failed: "failed", Skipped: "skipped"}

func (s Status) String() string {
	if s < Passed || s > Skipped {
		return "unspecified"
	}
	return statusNames[s]
}

// failing reports whether s fails a report: Failed, or none of the four.
func failing(s Status) bool { return s != Passed && s != Warning && s != Skipped }

// Check is one named check of a connector's list. Declaring it does no I/O;
// only Run does.
type Check struct {
	// Unique in the list and stable between releases: lowercase letters and
	// digits in words joined by ".", "_" or "-", such as "stream.orders.read".
	// The function Name makes one from parts that may not fit, such as a
	// stream's name.
	Name string
	// Zero: Custom.
	Kind Kind
	// A template.
	Title string
	// A template. Optional.
	Description string
	// The config field the check concerns, as a dotted path such as
	// "consumer.name". Optional.
	Field string
	// Each name matches [a-z][a-z0-9_]*. Never secrets.
	Params Params
	// Names of earlier checks that must have passed or warned; otherwise
	// this check is Skipped.
	Needs []string
	// A failure is a Warning. An error no standard reason fits reads "The
	// check raised a warning.", with its text as the detail; for a sentence
	// of its own, give it a Reason, as with Reason.Wrap.
	Advisory bool
	// Zero: only ctx's deadline.
	Timeout time.Duration
	// Nil passes, Skip's error skips, Warn's warns, any other fails.
	Run func(ctx context.Context, p *Probe) error
}

// Probe is a running check's handle. Its methods are safe for concurrent
// use, and the zero Probe is usable, as in a test of one Run. Once its check
// has ended, Param and Found do nothing and Cleanup runs fn at once, so a
// check the run stopped waiting for can't change its item.
type Probe struct {
	mu       sync.Mutex
	params   Params
	found    string
	cleanups []func(context.Context)
	// Set once the check has ended: what a late cleanup runs under.
	ended context.Context
}

// Param sets a placeholder's value, as fmt.Sprint prints it, over the
// check's own. Never a secret.
func (p *Probe) Param(name string, value any) {
	v := fmt.Sprint(value)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ended != nil {
		return
	}
	if p.params == nil {
		p.params = Params{}
	}
	p.params[name] = v
}

// Found is a passing check's message, a template: what it found, such as
// "PostgreSQL {version}".
func (p *Probe) Found(message string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ended == nil {
		p.found = message
	}
}

// Cleanup runs fn after every check has ended, last first. Its context
// outlives cancellation, and ends 10 seconds after cleanups start. Run waits
// for fn only until shortly after Run's context is done, so a slow cleanup
// doesn't hold up the report past its deadline; fn then finishes in the
// background. A panic in fn is recovered.
func (p *Probe) Cleanup(fn func(ctx context.Context)) {
	p.mu.Lock()
	ended := p.ended
	if ended == nil {
		p.cleanups = append(p.cleanups, fn)
	}
	p.mu.Unlock()
	if ended != nil {
		ctx, cancel := context.WithTimeout(ended, cleanupLimit)
		defer cancel()
		cleanup(ctx, fn)
	}
}

// end stops p taking more, and gives what it has.
func (p *Probe) end(ctx context.Context) (Params, string, []func(context.Context)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ended = context.WithoutCancel(ctx)
	params, found, cleanups := p.params, p.found, p.cleanups
	p.params, p.cleanups = nil, nil
	if params == nil {
		params = Params{}
	}
	return params, found, cleanups
}

type skipError struct{ reason string }

func (e *skipError) Error() string { return "skipped: " + e.reason }

// Skip, returned from Run, marks the check Skipped, with the reason as the
// item's message. The reason is a template like any message: a {name} in
// the formatted text is filled from the params, so a value that may hold
// braces belongs in a param, not in args.
func Skip(format string, args ...any) error {
	return &skipError{reason: fmt.Sprintf(format, args...)}
}

type warnError struct{ err error }

func (e *warnError) Error() string { return e.err.Error() }
func (e *warnError) Unwrap() error { return e.err }

// Warn, returned from Run, makes err a warning. Nil for a nil err. An err no
// standard reason fits reads "The check raised a warning.", with its text as
// the detail; for a sentence of its own, give it a Reason, as with
// Reason.Wrap.
func Warn(err error) error {
	if isNil(err) {
		return nil
	}
	return &warnError{err: err}
}

// isNil reports whether err is nil, or is itself a nil *Failure, as
// Reason.Wrap and Classify return for a nil error. Only err itself counts: a
// nil *Failure inside a chain doesn't hide what else the chain holds.
func isNil(err error) bool {
	if err == nil {
		return true
	}
	f, ok := any(err).(*Failure)
	return ok && f == nil
}

var (
	namePattern  = regexp.MustCompile(`^[a-z0-9]+([._-][a-z0-9]+)*$`)
	paramPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	// What a placeholder with a mistyped name looks like.
	nearPlaceholder = regexp.MustCompile(`\{[A-Za-z0-9_.-]+\}`)
	notInName       = regexp.MustCompile(`[^a-z0-9]+`)
)

// Name is a check's name made of parts that may not fit a name, such as a
// stream's: each part lowercased, each run of anything but a-z and 0-9 in it
// replaced by "-", "-" trimmed from its ends, and "x" for a part left empty;
// then the parts joined by ".". So Name("stream", "My Orders", "read") is
// "stream.my-orders.read". The result is always a valid name, but different
// parts can give the same one, such as "My Orders" and "my_orders", which
// Validate then refuses as repeated: a list made from such parts needs
// something more in each name that tells them apart.
func Name(parts ...string) string {
	if len(parts) == 0 {
		return "x"
	}
	fit := make([]string, len(parts))
	for i, p := range parts {
		p = strings.Trim(notInName.ReplaceAllString(strings.ToLower(p), "-"), "-")
		if p == "" {
			p = "x"
		}
		fit[i] = p
	}
	return strings.Join(fit, ".")
}

// Validate reports the first thing wrong with checks: a name that's
// malformed or repeated, a check without a title or a Run, a need that isn't
// an earlier check, a param name that a placeholder can't have, or a title
// or description with what looks like a placeholder but isn't one, such as
// {Host} or {db-host}.
func Validate(checks []Check) error {
	seen := make(map[string]bool, len(checks))
	for i, c := range checks {
		switch {
		case !namePattern.MatchString(c.Name):
			return fmt.Errorf("check %d: the name %q isn't lowercase letters and digits in words joined by \".\", \"_\" or \"-\"", i+1, c.Name)
		case seen[c.Name]:
			return fmt.Errorf("check %d: the name %q is repeated", i+1, c.Name)
		case c.Title == "":
			return fmt.Errorf("check %q has no title", c.Name)
		case c.Run == nil:
			return fmt.Errorf("check %q has no Run", c.Name)
		}
		for _, n := range c.Needs {
			if !seen[n] {
				return fmt.Errorf("check %q needs %q, which isn't an earlier check", c.Name, n)
			}
		}
		for _, n := range slices.Sorted(maps.Keys(c.Params)) {
			if !paramPattern.MatchString(n) {
				return fmt.Errorf("check %q: the param name %q isn't lowercase letters, digits and \"_\", starting with a letter, so no placeholder can use it", c.Name, n)
			}
		}
		for _, t := range []string{c.Title, c.Description} {
			for _, m := range nearPlaceholder.FindAllString(t, -1) {
				if !paramPattern.MatchString(m[1 : len(m)-1]) {
					return fmt.Errorf("check %q: %s isn't a placeholder, whose name is lowercase letters, digits and \"_\", starting with a letter", c.Name, m)
				}
			}
		}
		seen[c.Name] = true
	}
	return nil
}

const (
	cleanupLimit = 10 * time.Second
	// How long a check, or the cleanups, may take to return after their
	// deadline before the run stops waiting for them.
	grace = 100 * time.Millisecond
)

// Run runs the checks in order, then their cleanups, and gives an item for
// each check.
//
// A check that returns nil only after its deadline, its Timeout's or ctx's,
// fails as timed_out, and one that hasn't returned shortly after its
// deadline is left running while the run moves on, failed as timed_out too.
// Once ctx is cancelled, the check running and those left are Skipped; once
// ctx's deadline passes, those left are. A run ctx stops either way isn't
// OK: Err is the first failed item's error, or else one that wraps ctx's.
//
// Run waits for the cleanups until they've all returned, or until shortly
// after whichever comes first: ctx is done, or the cleanups' own 10 seconds
// end. Cleanups still running then finish in the background, under their
// own context. So with a deadline on ctx, Run returns shortly after it,
// whatever the cleanups do.
//
// A panic in a check's Run or in a cleanup is recovered, the first as the
// check's failure. A list Validate refuses runs nothing: the report has no
// items, and its error, with the code INTERNAL, says what's wrong.
func Run(ctx context.Context, checks []Check) Report {
	if err := Validate(checks); err != nil {
		return Report{err: &Failure{Reason: Unknown.Name, Code: Unknown.Code, Message: "the connector declares its checks wrongly", Detail: clean(err.Error())}}
	}
	var cleanups []func(context.Context)
	defer func() { runCleanups(ctx, cleanups) }()
	r := Report{Items: make([]Item, 0, len(checks))}
	ended := make(map[string]Status, len(checks))
	stopped := false
	for _, c := range checks {
		it := Item{Name: c.Name, Kind: c.Kind.orCustom(), Title: c.Title, Description: c.Description, Field: c.Field, Params: maps.Clone(c.Params)}
		switch need, unmet := unmetNeed(c.Needs, ended); {
		case ctx.Err() != nil:
			it.Status, it.Message, stopped = Skipped, "the checks stopped before it ran: "+ctx.Err().Error(), true
		case unmet:
			it.Status, it.Message = Skipped, fmt.Sprintf("needs %s, which %s", need, was(ended[need]))
		default:
			cs, s := runOne(ctx, c, &it)
			cleanups, stopped = append(cleanups, cs...), stopped || s
		}
		if it.Params == nil {
			it.Params = Params{}
		}
		ended[c.Name] = it.Status
		r.Items = append(r.Items, it.valid())
	}
	if err := ctx.Err(); err != nil && stopped {
		r.err = stoppedBy(err)
	}
	return r
}

// stoppedBy is the error of a run that ctx's err stopped.
func stoppedBy(err error) *Failure {
	f := &Failure{Reason: Unknown.Name, Code: Unknown.Code, Message: "the checks were cancelled", Detail: err.Error(), cause: err}
	if errors.Is(err, context.DeadlineExceeded) {
		f.Reason, f.Code, f.Message = TimedOut.Name, TimedOut.Code, "the checks ran out of time"
	}
	return f
}

func unmetNeed(needs []string, ended map[string]Status) (string, bool) {
	for _, n := range needs {
		if s := ended[n]; s != Passed && s != Warning {
			return n, true
		}
	}
	return "", false
}

func was(s Status) string {
	if s == Skipped {
		return "was skipped"
	}
	return s.String()
}

// outcome is how a check's Run went.
type outcome struct {
	pass    bool
	skip    *skipError
	warn    bool
	failure *Failure
	// The check's context was done when it returned.
	late bool
	// The run's context's error when it returned.
	stop error
}

// runOne runs c into it. It gives the cleanups c registered, and whether the
// run's context stopped c.
func runOne(ctx context.Context, c Check, it *Item) ([]func(context.Context), bool) {
	cctx, cancel := ctx, context.CancelFunc(func() {})
	if c.Timeout > 0 {
		cctx, cancel = context.WithTimeout(ctx, c.Timeout)
	}
	defer cancel()
	p := &Probe{params: maps.Clone(c.Params)}
	done := make(chan outcome, 1)
	start := time.Now()
	go func() { done <- call(ctx, cctx, c, p) }()
	var o outcome
	returned := true
	select {
	case o = <-done:
	case <-cctx.Done():
		t := time.NewTimer(grace)
		select {
		case o = <-done:
		case <-t.C:
			returned, o.stop = false, ctx.Err()
		}
		t.Stop()
	}
	it.Duration = time.Since(start)
	params, found, cleanups := p.end(ctx)
	it.Params = params
	switch {
	case o.stop != nil && !errors.Is(o.stop, context.DeadlineExceeded):
		it.Status, it.Message = Skipped, "the checks were cancelled while it ran"
		return cleanups, true
	case !returned || o.pass && o.late:
		// The check, not the network, is what was slow: TimedOut's fix,
		// about firewalls, doesn't apply.
		f := TimedOut.Fail()
		f.Message, f.Fix = "The check didn't finish in time.", ""
		o.failure = f
	case o.pass:
		it.Status, it.Message = Passed, found
		return cleanups, false
	case o.skip != nil:
		it.Status, it.Message = Skipped, o.skip.reason
		return cleanups, o.stop != nil
	}
	f := o.failure
	for k, v := range f.Params {
		if _, ok := params[k]; !ok {
			params[k] = v
		}
	}
	f.Params = params
	it.Status = Failed
	if o.warn || c.Advisory {
		it.Status = Warning
		if f.Reason == Unknown.Name && f.Message == Unknown.Message {
			f.Message = unknownWarning
		}
	}
	it.Message, it.Failure = f.Message, f
	return cleanups, o.stop != nil
}

// call runs c, recovering a panic in Run or in classifying what it returned.
func call(ctx, cctx context.Context, c Check, p *Probe) (o outcome) {
	defer func() {
		if r := recover(); r != nil {
			o = outcome{failure: Unknown.Wrap(fmt.Errorf("the check panicked: %v", r))}
		}
		o.late, o.stop = cctx.Err() != nil, ctx.Err()
	}()
	err := c.Run(cctx, p)
	if isNil(err) {
		return outcome{pass: true}
	}
	if s, ok := errors.AsType[*skipError](err); ok {
		return outcome{skip: s}
	}
	_, warn := errors.AsType[*warnError](err)
	return outcome{warn: warn, failure: Classify(err)}
}

// runCleanups runs fns, last first, and waits for them until they return,
// or until grace after ctx or their own context is done, whichever is
// first. Those still running then finish in the background, their context
// unchanged.
func runCleanups(ctx context.Context, fns []func(context.Context)) {
	if len(fns) == 0 {
		return
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupLimit)
	done := make(chan struct{})
	go func() {
		defer cancel()
		defer close(done)
		for _, fn := range slices.Backward(fns) {
			cleanup(cctx, fn)
		}
	}()
	select {
	case <-done:
		return
	case <-ctx.Done():
	case <-cctx.Done():
	}
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
	}
}

func cleanup(ctx context.Context, fn func(context.Context)) {
	defer func() { _ = recover() }()
	fn(ctx)
}
