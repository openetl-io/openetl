package check

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

type codedErr struct {
	code  protocolv1.ErrorCode
	after time.Duration
}

func (e *codedErr) Error() string                   { return "coded" }
func (e *codedErr) ErrorCode() protocolv1.ErrorCode { return e.code }
func (e *codedErr) RetryAfter() time.Duration       { return e.after }

func pass(context.Context, *Probe) error { return nil }

func fails(err error) func(context.Context, *Probe) error {
	return func(context.Context, *Probe) error { return err }
}

func statuses(r Report) string {
	var s []string
	for _, it := range r.Items {
		s = append(s, it.Name+"="+it.Status.String())
	}
	return strings.Join(s, " ")
}

// one runs c alone, named and titled if it isn't, and gives its item.
func one(t *testing.T, c Check) Item {
	t.Helper()
	if c.Name == "" {
		c.Name = "c"
	}
	if c.Title == "" {
		c.Title = "T"
	}
	r := Run(t.Context(), []Check{c})
	if len(r.Items) != 1 {
		t.Fatalf("%d items: %v", len(r.Items), r.Err())
	}
	return r.Items[0]
}

func codeOf(err error) protocolv1.ErrorCode {
	if c, ok := errors.AsType[coded](err); ok {
		return c.ErrorCode()
	}
	return protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED
}

// The numbers are the protocol's, which hosts convert by.
func TestNumbers(t *testing.T) {
	for s, p := range map[Status]protocolv1.CheckStatus{
		Passed: protocolv1.CheckStatus_CHECK_STATUS_PASSED, Warning: protocolv1.CheckStatus_CHECK_STATUS_WARNING,
		Failed: protocolv1.CheckStatus_CHECK_STATUS_FAILED, Skipped: protocolv1.CheckStatus_CHECK_STATUS_SKIPPED,
	} {
		if int(s) != int(p) {
			t.Errorf("%s is %d, the protocol's %s %d", s, s, p, p)
		}
	}
	for k := Configuration; k <= Custom; k++ {
		if name := strings.ToLower(strings.TrimPrefix(protocolv1.CheckKind(k).String(), "CHECK_KIND_")); name != k.String() {
			t.Errorf("kind %d is %s, the protocol's %s", k, k, name)
		}
	}
	if Kind(0).String() != "custom" || Kind(42).String() != "custom" || Status(0).String() != "unspecified" {
		t.Error("an unknown kind is custom, and an unknown status unspecified")
	}
}

func TestRun(t *testing.T) {
	var order []string
	r := Run(t.Context(), []Check{
		{Name: "connect", Kind: Connection, Title: "Reach {address}", Field: "host", Params: Params{"address": "db:5432", "who": "check"},
			Run: func(_ context.Context, p *Probe) error {
				p.Cleanup(func(context.Context) { order = append(order, "first") })
				p.Cleanup(func(context.Context) { order = append(order, "second") })
				p.Param("version", 17)
				p.Found("PostgreSQL {version}")
				return nil
			}},
		{Name: "auth", Title: "Sign in", Needs: []string{"connect"}, Params: Params{"who": "check"},
			Run: func(_ context.Context, p *Probe) error {
				p.Param("who", "probe")
				p.Param("port", 5432)
				return PrivilegeMissing.Fail("who", "failure", "principal", "etl")
			}},
		{Name: "tables", Title: "Read tables", Needs: []string{"auth"}, Run: pass},
		{Name: "columns", Title: "Read columns", Needs: []string{"tables"}, Run: pass},
		{Name: "tls", Title: "TLS", Advisory: true, Run: fails(errors.New("old cipher"))},
		{Name: "after-warn", Title: "After", Needs: []string{"tls"}, Run: pass},
		{Name: "warn", Title: "Warn", Run: fails(Warn(RateLimited.Fail()))},
		{Name: "cdc", Title: "CDC", Run: fails(Skip("no %s stream", "cdc"))},
		{Name: "panic", Title: "Panic", Run: func(context.Context, *Probe) error { panic("boom") }},
		{Name: "nil-classify", Title: "Nil", Run: fails(Classify(nil))},
		{Name: "nil-wrap", Title: "Nil", Run: fails(NotFound.Wrap(nil, "resource", "x"))},
		{Name: "slow", Title: "Slow", Timeout: time.Millisecond, Run: func(ctx context.Context, _ *Probe) error {
			<-ctx.Done()
			return ctx.Err()
		}},
	})
	want := "connect=passed auth=failed tables=skipped columns=skipped tls=warning after-warn=passed warn=warning cdc=skipped panic=failed nil-classify=passed nil-wrap=passed slow=failed"
	if got := statuses(r); got != want {
		t.Fatalf("statuses:\n got %s\nwant %s", got, want)
	}
	if !slices.Equal(order, []string{"second", "first"}) {
		t.Errorf("cleanups ran %v", order)
	}
	by := map[string]Item{}
	for _, it := range r.Items {
		by[it.Name] = it
	}
	if c := by["connect"]; c.Message != "PostgreSQL {version}" || c.Rendered().Message != "PostgreSQL 17" || c.Field != "host" || c.Failure != nil {
		t.Errorf("connect: %+v", c)
	}
	if got := by["tables"].Message; got != "needs auth, which failed" {
		t.Errorf("tables: %q", got)
	}
	if got := by["columns"].Message; got != "needs tables, which was skipped" {
		t.Errorf("columns: %q", got)
	}
	if got := by["cdc"].Message; got != "no cdc stream" {
		t.Errorf("cdc: %q", got)
	}
	if f := by["tls"].Failure; f == nil || f.Reason != "unknown" || f.Detail != "old cipher" {
		t.Errorf("tls: %+v", f)
	}
	if f := by["panic"].Failure; f == nil || f.Code != protocolv1.ErrorCode_ERROR_CODE_INTERNAL || !strings.Contains(f.Detail, "boom") {
		t.Errorf("panic: %+v", f)
	}
	if f := by["slow"].Failure; f == nil || f.Reason != "timed_out" {
		t.Errorf("slow: %+v", f)
	}
	if by["connect"].Kind != Connection || by["tables"].Kind != Custom {
		t.Errorf("kinds %v, %v", by["connect"].Kind, by["tables"].Kind)
	}
	if a := by["auth"]; a.Message != PrivilegeMissing.Message || a.Failure.Message != a.Message {
		t.Errorf("a failed item's message is its failure's: %q, %q", a.Message, a.Failure.Message)
	}
	if p := by["auth"].Params; p["who"] != "probe" || p["port"] != "5432" || p["principal"] != "etl" {
		t.Errorf("auth's params %v: the probe's win over the check's, and the failure's only add", p)
	}

	err := r.Err()
	if err == nil || err.Error() != "Sign in: etl has no {privilege} on {object}." {
		t.Fatalf("Err: %v", err)
	}
	if codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		t.Errorf("Err's code: %v", err)
	}
	if r.OK() {
		t.Error("OK with a failed item")
	}
}

func TestRunAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cleaned := false
	r := Run(ctx, []Check{
		{Name: "a", Title: "A", Run: func(_ context.Context, p *Probe) error {
			p.Cleanup(func(ctx context.Context) { cleaned = ctx.Err() == nil })
			return nil
		}},
		{Name: "b", Title: "B", Run: func(context.Context, *Probe) error {
			cancel()
			return nil
		}},
		{Name: "c", Title: "C", Run: pass},
	})
	if got := statuses(r); got != "a=passed b=skipped c=skipped" {
		t.Errorf("statuses %s", got)
	}
	if !cleaned {
		t.Error("the cleanup didn't run with a live context")
	}
}

// A nil *Failure is a nil error only as the error itself.
func TestNilFailureOnlyAtTheTop(t *testing.T) {
	denied := errors.New("permission denied for table orders")
	for name, err := range map[string]error{
		"joined after a nil Wrap":     errors.Join(NotFound.Wrap(nil, "resource", "x"), PermissionDenied.Wrap(denied)),
		"joined after a nil Classify": errors.Join(Classify(nil), denied),
		"wrapping a nil failure":      fmt.Errorf("%w: %w", Classify(nil), denied),
	} {
		if it := one(t, Check{Run: fails(err)}); it.Status != Failed || it.Failure == nil || !strings.Contains(it.Failure.Detail, denied.Error()) {
			t.Errorf("%s: %s, %+v", name, it.Status, it.Failure)
		}
		if it := one(t, Check{Run: fails(Warn(err))}); it.Status != Warning {
			t.Errorf("%s, warned: %s", name, it.Status)
		}
		if Classify(err) == nil || Warn(err) == nil {
			t.Errorf("%s: Classify or Warn made it nil", name)
		}
	}
	if f := Classify(errors.Join(NotFound.Wrap(nil), errSlot.With("slot", "s"))); f.Reason != "slot_missing" || f.Params["slot"] != "s" {
		t.Errorf("the failure past a nil one: %s %v", f.Reason, f.Params)
	}
	if f := Classify(errors.Join(Classify(nil), &codedErr{code: protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, after: time.Minute})); f.Reason != "credentials_rejected" || f.After != time.Minute {
		t.Errorf("the code and wait past a nil failure: %s, %s", f.Reason, f.After)
	}
}

// A run its context ended is not OK: what it didn't finish is Skipped, and
// Err carries the context's error.
func TestCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	r := Run(ctx, []Check{{Name: "a", Title: "A", Run: fails(errors.New("x"))}})
	if statuses(r) != "a=skipped" || r.OK() || !errors.Is(r.Err(), context.Canceled) {
		t.Errorf("cancelled before it started: %s, %v", statuses(r), r.Err())
	}

	ctx, cancel = context.WithCancel(t.Context())
	r = Run(ctx, []Check{
		{Name: "a", Title: "A", Run: func(ctx context.Context, _ *Probe) error {
			cancel()
			<-ctx.Done()
			return ctx.Err()
		}},
		{Name: "b", Title: "B", Run: pass},
	})
	if statuses(r) != "a=skipped b=skipped" || r.Items[0].Failure != nil || r.OK() || !errors.Is(r.Err(), context.Canceled) {
		t.Errorf("cancelled while a ran: %s, %+v, %v", statuses(r), r.Items[0].Failure, r.Err())
	}
}

// The context's deadline is the running check's: it fails as timed_out,
// returning nil after it or not.
func TestContextDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	r := Run(ctx, []Check{
		{Name: "a", Title: "A", Run: func(ctx context.Context, _ *Probe) error {
			<-ctx.Done()
			return nil
		}},
		{Name: "b", Title: "B", Run: pass},
	})
	if statuses(r) != "a=failed b=skipped" || r.Items[0].Failure.Reason != "timed_out" || r.OK() {
		t.Errorf("%s, %+v, OK %v", statuses(r), r.Items[0].Failure, r.OK())
	}
}

// A run whose deadline passes isn't OK, though its last check skips itself
// once the deadline has passed.
func TestContextDeadlineThenSkip(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	r := Run(ctx, []Check{{Name: "a", Title: "A", Run: func(ctx context.Context, _ *Probe) error {
		<-ctx.Done()
		return Skip("no time left")
	}}})
	if statuses(r) != "a=skipped" || r.OK() || !errors.Is(r.Err(), context.DeadlineExceeded) {
		t.Errorf("%s, OK %v, %v", statuses(r), r.OK(), r.Err())
	}
}

func TestStatusOutsideTheFourFails(t *testing.T) {
	for _, s := range []Status{0, 7} {
		r := Report{Items: []Item{{Name: "a", Title: "A", Status: Passed}, {Name: "b", Title: "B", Status: s}}}
		if r.OK() || r.Err() == nil || codeOf(r.Err()) != protocolv1.ErrorCode_ERROR_CODE_INTERNAL {
			t.Errorf("status %d: OK %v, Err %v", s, r.OK(), r.Err())
		}
	}
	if r := (Report{Items: []Item{{Status: Passed}, {Status: Warning}, {Status: Skipped}}}); !r.OK() || r.Err() != nil {
		t.Errorf("passed, warning and skipped: %v", r.Err())
	}
}

// A check that doesn't return shortly after its deadline is left behind as
// timed_out, and its Probe can't change its item.
func TestHungCheckIsAbandoned(t *testing.T) {
	release, cleaned := make(chan struct{}), make(chan struct{})
	start := time.Now()
	r := Run(t.Context(), []Check{
		{Name: "hung", Title: "Hung", Timeout: 10 * time.Millisecond, Run: func(_ context.Context, p *Probe) error {
			<-release // ignores its context, as a driver's cgo call can
			for i := range 100 {
				p.Param("late", i)
			}
			p.Found("late")
			p.Cleanup(func(context.Context) { close(cleaned) })
			return nil
		}},
		{Name: "next", Title: "Next", Run: pass},
	})
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("the run waited %s for a check with a 10ms timeout", d)
	}
	if statuses(r) != "hung=failed next=passed" || r.Items[0].Failure.Reason != "timed_out" || r.Items[0].Failure.Fix != "" {
		t.Fatalf("%s, %+v", statuses(r), r.Items[0].Failure)
	}
	close(release)
	select {
	case <-cleaned:
	case <-time.After(5 * time.Second):
		t.Error("a cleanup registered after the run left the check didn't run")
	}
	if it := r.Items[0]; it.Params["late"] != "" || it.Message == "late" {
		t.Errorf("the abandoned check changed its item: %+v", it)
	}
}

// A warning no reason fits says it warned, not that the check failed, on
// both sides of the wire; one with a reason or a message of its own, and a
// failure, read as before. A warning never sets the answer's error.
func TestUnknownWarning(t *testing.T) {
	cause := errors.New("a directory is on one disk")
	r := Run(t.Context(), []Check{
		{Name: "warn", Title: "W", Run: fails(Warn(cause))},
		{Name: "advisory", Title: "A", Advisory: true, Run: fails(cause)},
		{Name: "reason", Title: "R", Run: fails(Warn(RateLimited.Wrap(cause)))},
		{Name: "message", Title: "M", Advisory: true, Run: fails(&Failure{Message: "One disk.", Detail: cause.Error()})},
		{Name: "own-reason", Title: "O", Run: fails(Warn(&Failure{Reason: "one_disk", Detail: cause.Error()}))},
	})
	want := map[string][2]string{
		"warn": {"unknown", unknownWarning}, "advisory": {"unknown", unknownWarning},
		"reason": {"rate_limited", RateLimited.Message}, "message": {"unknown", "One disk."}, "own-reason": {"one_disk", Unknown.Message},
	}
	items, rerr := r.Proto()
	if !r.OK() || r.Err() != nil || rerr != nil {
		t.Fatalf("warnings set the answer's error: %v, %v", r.Err(), rerr)
	}
	if m := items[0].GetError().GetMessage(); m != unknownWarning {
		t.Errorf("the warning's error on the wire: %q", m)
	}
	back := FromProto(&protocolv1.CheckResponse{Items: items})
	for i, it := range r.Items {
		w := want[it.Name]
		for _, got := range []Item{it, back.Items[i]} {
			if f := got.Failure; got.Status != Warning || got.Message != w[1] || f == nil || f.Message != w[1] || f.Reason != w[0] || f.Detail != cause.Error() ||
				f.Reason == "unknown" && f.Code != protocolv1.ErrorCode_ERROR_CODE_INTERNAL {
				t.Errorf("%s: %s %q, %+v", it.Name, got.Status, got.Message, f)
			}
		}
	}
	if it := one(t, Check{Run: fails(cause)}); it.Status != Failed || it.Message != Unknown.Message || it.Failure.Message != Unknown.Message {
		t.Errorf("a failure: %s %q", it.Status, it.Message)
	}
}

// A network timeout the check returns itself keeps TimedOut's fix, which a
// check that was slow on its own doesn't get.
func TestNetworkTimeoutKeepsItsFix(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	it := one(t, Check{Run: func(ctx context.Context, _ *Probe) error {
		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", ln.Addr().String())
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
		_, err = conn.Read(make([]byte, 1)) // the listener never writes
		return err
	}})
	if it.Status != Failed || it.Failure.Reason != "timed_out" || it.Failure.Fix != TimedOut.Fix {
		t.Errorf("%s, %+v", it.Status, it.Failure)
	}
}

func TestPassAfterItsTimeoutIsNotAPass(t *testing.T) {
	it := one(t, Check{Timeout: 5 * time.Millisecond, Run: func(ctx context.Context, _ *Probe) error {
		<-ctx.Done()
		return nil
	}})
	if it.Status != Failed || it.Failure.Reason != "timed_out" || it.Failure.Code != transient || it.Failure.Fix != "" {
		t.Errorf("%s, %+v", it.Status, it.Failure)
	}
	if it := one(t, Check{Advisory: true, Timeout: 5 * time.Millisecond, Run: func(ctx context.Context, _ *Probe) error {
		<-ctx.Done()
		return nil
	}}); it.Status != Warning {
		t.Errorf("an advisory check that timed out: %s", it.Status)
	}
}

// A panic in one cleanup leaves the others to run.
func TestCleanupPanics(t *testing.T) {
	var ran []string
	r := Run(t.Context(), []Check{
		{Name: "a", Title: "A", Run: func(_ context.Context, p *Probe) error {
			p.Cleanup(func(context.Context) { ran = append(ran, "a") })
			return nil
		}},
		{Name: "b", Title: "B", Run: func(_ context.Context, p *Probe) error {
			p.Cleanup(func(context.Context) { panic("cleanup") })
			return nil
		}},
		{Name: "c", Title: "C", Run: func(_ context.Context, p *Probe) error {
			p.Cleanup(func(context.Context) { ran = append(ran, "c") })
			return nil
		}},
	})
	if !slices.Equal(ran, []string{"c", "a"}) || !r.OK() {
		t.Errorf("cleanups ran %v; %v", ran, r.Err())
	}
}

func TestZeroProbe(_ *testing.T) {
	var p Probe
	p.Param("x", 1)
	p.Found("found {x}")
	p.Cleanup(func(context.Context) {})
}

// The check's and the probe's params win; a failure's add only names they
// don't have, so a title says what the config says.
func TestParams(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	_ = ln.Close()
	configured := "localhost:" + port
	it := one(t, Check{Title: "Reach {address}", Params: Params{"address": configured}, Run: func(ctx context.Context, p *Probe) error {
		p.Param("tried", "once")
		var d net.Dialer
		_, err := d.DialContext(ctx, "tcp4", configured)
		return err
	}})
	if it.Failure == nil || it.Failure.Reason != "connection_refused" {
		t.Fatalf("%+v", it.Failure)
	}
	if got := it.Rendered(); got.Title != "Reach "+configured || got.Failure.Message != configured+" refused the connection." || got.Params["tried"] != "once" {
		t.Errorf("title %q, message %q, params %v", got.Title, got.Failure.Message, got.Params)
	}
}

func TestValidate(t *testing.T) {
	ok := Check{Name: "a", Title: "A", Run: pass}
	for name, checks := range map[string][]Check{
		"no name":            {{Title: "A", Run: pass}},
		"malformed name":     {{Name: "Connect DB", Title: "A", Run: pass}},
		"repeated name":      {ok, ok},
		"no title":           {{Name: "a", Run: pass}},
		"no run":             {{Name: "a", Title: "A"}},
		"needs a later":      {{Name: "a", Title: "A", Needs: []string{"b"}, Run: pass}, {Name: "b", Title: "B", Run: pass}},
		"capital param":      {{Name: "a", Title: "Reach {host}", Params: Params{"Host": "db"}, Run: pass}},
		"hyphenated param":   {{Name: "a", Title: "A", Params: Params{"db-host": "db"}, Run: pass}},
		"capital title":      {{Name: "a", Title: "Reach {Host}", Params: Params{"host": "db"}, Run: pass}},
		"hyphen description": {{Name: "a", Title: "A", Description: "Uses {db-user}.", Run: pass}},
	} {
		if Validate(checks) == nil {
			t.Errorf("%s: valid", name)
		}
		r := Run(t.Context(), checks)
		err := r.Err()
		if len(r.Items) != 0 || err == nil || !strings.HasPrefix(err.Error(), "the connector declares its checks wrongly: ") ||
			codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_INTERNAL {
			t.Errorf("%s: Run gave %q and %v", name, statuses(r), err)
		}
	}
	if err := Validate([]Check{ok, {Name: "schema.usage_v2-x", Title: `Reach {host} with {"tls": true} {}`, Params: Params{"host": "db"}, Needs: []string{"a"}, Run: pass}}); err != nil {
		t.Error(err)
	}
}

// One package-level failure, as a connector declares its own, used by many
// runs at once.
var errSlot = Reason{Name: "slot_missing", Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, Message: "No slot {slot}."}.Fail()

func TestSharedFailure(t *testing.T) {
	checks := []Check{
		{Name: "a", Title: "A", Run: func(context.Context, *Probe) error { return errSlot.With("slot", "a", "table", "payroll") }},
		{Name: "b", Title: "B", Run: func(context.Context, *Probe) error { return errSlot.With("slot", "b") }},
		{Name: "c", Title: "C", Run: func(context.Context, *Probe) error { return errSlot }},
	}
	r := Run(t.Context(), checks)
	if v, ok := r.Items[1].Params["table"]; ok {
		t.Errorf("check b has check a's param table=%q", v)
	}
	if len(errSlot.Params) != 0 {
		t.Errorf("the shared failure has params %v", errSlot.Params)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { Run(context.Background(), checks) })
	}
	wg.Wait()
}

// slowCleanup is a check that passes, leaving a cleanup that ignores its
// context for d and then sends that context's error.
func slowCleanup(d time.Duration, ended chan<- error) Check {
	return Check{Name: "slow-cleanup", Title: "T", Run: func(_ context.Context, p *Probe) error {
		p.Cleanup(func(ctx context.Context) {
			time.Sleep(d)
			ended <- ctx.Err()
		})
		return nil
	}}
}

// Run doesn't wait for slow cleanups past its context, and they still run
// to the end under their own context.
func TestCleanupsAfterTheDeadline(t *testing.T) {
	for name, tc := range map[string]struct {
		ctx  func() (context.Context, context.CancelFunc)
		then Check
	}{
		"a deadline that passes while they run": {
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(t.Context(), 150*time.Millisecond)
			},
			then: Check{Name: "fast", Title: "T", Run: pass},
		},
		"a context cancelled before they start": {
			ctx: func() (context.Context, context.CancelFunc) { return context.WithCancel(t.Context()) },
		},
	} {
		ctx, cancel := tc.ctx()
		then := tc.then
		if then.Run == nil {
			then = Check{Name: "cancel", Title: "T", Run: func(context.Context, *Probe) error {
				cancel()
				return nil
			}}
		}
		ended := make(chan error, 1)
		start := time.Now()
		Run(ctx, []Check{slowCleanup(time.Second, ended), then})
		if d := time.Since(start); d > 700*time.Millisecond {
			t.Errorf("%s: Run waited %s for a cleanup", name, d)
		}
		select {
		case err := <-ended:
			t.Errorf("%s: Run waited for the cleanup to end (%v)", name, err)
		default:
		}
		select {
		case err := <-ended:
			if err != nil {
				t.Errorf("%s: the cleanup's own context ended early: %v", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("%s: the cleanup didn't finish in the background", name)
		}
		cancel()
	}
}

// With time to spare, Run waits for its cleanups.
func TestCleanupsWithTimeToSpare(t *testing.T) {
	long, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for name, ctx := range map[string]context.Context{"no deadline": t.Context(), "a far deadline": long} {
		ended := make(chan error, 1)
		Run(ctx, []Check{slowCleanup(200*time.Millisecond, ended)})
		select {
		case err := <-ended:
			if err != nil {
				t.Errorf("%s: the cleanup's context: %v", name, err)
			}
		default:
			t.Errorf("%s: Run returned before its cleanup", name)
		}
	}
}

func TestName(t *testing.T) {
	for _, tc := range []struct {
		parts []string
		want  string
	}{
		{[]string{"stream", "My Orders", "read"}, "stream.my-orders.read"},
		{[]string{"stream", "orders", "read"}, "stream.orders.read"},
		{[]string{"stream", "public.orders", "read"}, "stream.public-orders.read"},
		{[]string{"table", "Ünïcode Tabelle"}, "table.n-code-tabelle"},
		{[]string{"--a__b--", "!!!", ""}, "a-b.x.x"},
		{[]string{"2024 Q1"}, "2024-q1"},
		{nil, "x"},
	} {
		if got := Name(tc.parts...); got != tc.want {
			t.Errorf("Name(%q) = %q, want %q", tc.parts, got, tc.want)
		}
	}
	if Name("s", "My Orders") != Name("s", "my_orders") {
		t.Error("the documented collision doesn't happen")
	}
	awkward := []string{"", " ", ".", "..", "-", "_", "a.b", ".a.", "Ä", "日本語", "\xff\xfe", "x\x00y", "🙂 emoji", "0", "007", "a--b", "A_B-C.D", "\t\n", "K"}
	rng := rand.New(rand.NewPCG(1, 2))
	for range 2000 {
		b := make([]byte, rng.IntN(12))
		for i := range b {
			b[i] = byte(rng.IntN(256))
		}
		awkward = append(awkward, string(b))
	}
	for i, a := range awkward {
		for _, parts := range [][]string{{a}, {"stream", a, "read"}, {a, awkward[(i+1)%len(awkward)]}} {
			name := Name(parts...)
			if !namePattern.MatchString(name) {
				t.Fatalf("Name(%q) = %q, which isn't a name", parts, name)
			}
			if err := Validate([]Check{{Name: name, Title: "T", Run: pass}}); err != nil {
				t.Fatalf("Name(%q): %v", parts, err)
			}
		}
	}
}
