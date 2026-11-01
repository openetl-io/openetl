package hostservices

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/unsuitable001/openetl/internal/netguard"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/host"
)

var start = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

type clock struct {
	mu    sync.Mutex
	t     time.Time
	slept []time.Duration
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) sleep(_ context.Context, d time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
	c.slept = append(c.slept, d)
	return nil
}

func loopback() netguard.Policy {
	return netguard.Policy{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}}
}

func newServices(t *testing.T, o Options) (*Services, *clock) {
	t.Helper()
	s := New(o)
	c := &clock{t: start}
	s.now, s.sleep = c.now, c.sleep
	t.Cleanup(s.Close)
	return s, c
}

// server answers with each handler in turn, then with the last one.
func server(t *testing.T, hs ...http.HandlerFunc) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(n.Add(1)) - 1
		hs[min(i, len(hs)-1)](w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

func status(code int, headers ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		for i := 0; i+1 < len(headers); i += 2 {
			w.Header().Set(headers[i], headers[i+1])
		}
		w.WriteHeader(code)
		_, _ = io.WriteString(w, http.StatusText(code))
	}
}

func do(t *testing.T, s *Services, r *host.Request) (string, error) {
	t.Helper()
	resp, err := s.HTTP(t.Context(), r)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

func TestRetriesTransientStatuses(t *testing.T) {
	srv, hits := server(t, status(503), status(502), status(200))
	s, c := newServices(t, Options{Policy: loopback()})
	b, err := do(t, s, &host.Request{URL: srv.URL})
	if err != nil || b != "OK" || hits.Load() != 3 {
		t.Fatalf("%q, %v after %d tries", b, err, hits.Load())
	}
	for i, d := range c.slept {
		if hi := baseBackoff << i; d < hi/2 || d > hi {
			t.Errorf("backoff %d: %v, want [%v, %v]", i, d, hi/2, hi)
		}
	}
}

func TestRetryAfter(t *testing.T) {
	date := start.Add(-time.Hour)
	srv, _ := server(t,
		status(429, "Retry-After", "7"),
		// An HTTP date counts from the server's Date, not the host's clock.
		status(503, "Retry-After", date.Add(30*time.Second).Format(http.TimeFormat), "Date", date.Format(http.TimeFormat)),
		status(200))
	s, c := newServices(t, Options{Policy: loopback()})
	if _, err := do(t, s, &host.Request{URL: srv.URL}); err != nil {
		t.Fatal(err)
	}
	if len(c.slept) != 2 || c.slept[0] != 7*time.Second || c.slept[1] != 30*time.Second {
		t.Errorf("slept %v", c.slept)
	}
}

func TestRetryAfterPastTheBudget(t *testing.T) {
	srv, hits := server(t, status(429, "Retry-After", "600"), status(200))
	s, c := newServices(t, Options{Policy: loopback()})
	_, err := do(t, s, &host.Request{URL: srv.URL})
	if errs.CodeOf(err) != errs.RateLimited || errs.RetryAfterOf(err) != 10*time.Minute {
		t.Fatalf("%v (retry after %v)", err, errs.RetryAfterOf(err))
	}
	if hits.Load() != 1 || len(c.slept) != 0 {
		t.Errorf("%d tries, slept %v", hits.Load(), c.slept)
	}
}

func TestBudgetRunsOut(t *testing.T) {
	srv, hits := server(t, status(500))
	s, c := newServices(t, Options{Policy: loopback(), Budget: 10 * time.Second})
	_, err := do(t, s, &host.Request{URL: srv.URL})
	se, ok := errors.AsType[*host.StatusError](err)
	if !ok || se.Status != 500 || errs.CodeOf(err) != errs.Transient {
		t.Fatal(err)
	}
	if hits.Load() < 3 || c.now().Sub(start) > 10*time.Second {
		t.Errorf("%d tries over %v", hits.Load(), c.now().Sub(start))
	}
}

func TestBudgetBoundsTheWaitForAResponse(t *testing.T) {
	block := make(chan struct{})
	srv, _ := server(t, func(http.ResponseWriter, *http.Request) { <-block })
	defer close(block)
	s := New(Options{Policy: loopback(), Budget: 200 * time.Millisecond})
	defer s.Close()
	began := time.Now()
	_, err := do(t, s, &host.Request{URL: srv.URL})
	if errs.CodeOf(err) != errs.Transient || time.Since(began) > 5*time.Second {
		t.Fatalf("%v after %v", err, time.Since(began))
	}
}

func TestNonIdempotentRequests(t *testing.T) {
	for _, tc := range []struct {
		name       string
		first      int
		idempotent bool
		tries      int32
	}{
		{"503", 503, false, 1},
		{"503 with an idempotency key", 503, true, 2},
		{"429", 429, false, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, hits := server(t, status(tc.first), status(200))
			s, _ := newServices(t, Options{Policy: loopback()})
			_, _ = do(t, s, &host.Request{Method: http.MethodPost, URL: srv.URL, Body: []byte("{}"), Idempotent: tc.idempotent})
			if hits.Load() != tc.tries {
				t.Errorf("%d tries, want %d", hits.Load(), tc.tries)
			}
		})
	}
}

// A POST that was never sent is retried: the server comes up during the
// first backoff.
func TestDialFailuresAreRetried(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	s, c := newServices(t, Options{Policy: loopback(), Budget: 5 * time.Second})
	var srv *httptest.Server
	s.sleep = func(ctx context.Context, d time.Duration) error {
		if srv == nil {
			l, err := net.Listen("tcp", addr)
			if err != nil {
				t.Skipf("port %s was taken meanwhile: %v", addr, err)
			}
			srv = &httptest.Server{Listener: l, Config: &http.Server{Handler: status(200), ReadHeaderTimeout: time.Second}}
			srv.Start()
			t.Cleanup(srv.Close)
		}
		return c.sleep(ctx, d)
	}
	b, err := do(t, s, &host.Request{Method: http.MethodPost, URL: "http://" + addr})
	if err != nil || b != "OK" || len(c.slept) != 1 {
		t.Errorf("%q, %v, slept %v", b, err, c.slept)
	}
}

func TestFailedStatuses(t *testing.T) {
	reset := strconv.FormatInt(start.Add(90*time.Second).Unix(), 10)
	srv, hits := server(t,
		status(403, "X-RateLimit-Remaining", "0", "X-RateLimit-Reset", reset, "Date", start.Format(http.TimeFormat)),
		status(404))
	s, c := newServices(t, Options{Policy: loopback()})
	_, err := do(t, s, &host.Request{URL: srv.URL})
	se, ok := errors.AsType[*host.StatusError](err)
	if !ok || se.Status != 404 || string(se.Body) != "Not Found" || errs.CodeOf(err) != errs.ContractChanged {
		t.Fatal(err)
	}
	if hits.Load() != 2 || len(c.slept) != 1 || c.slept[0] != 90*time.Second {
		t.Errorf("%d tries, slept %v", hits.Load(), c.slept)
	}
}

func TestClassify(t *testing.T) {
	h := func(kv ...string) http.Header {
		out := http.Header{}
		for i := 0; i+1 < len(kv); i += 2 {
			out.Set(kv[i], kv[i+1])
		}
		return out
	}
	for _, tc := range []struct {
		status int
		header http.Header
		want   errs.Code
	}{
		{400, nil, errs.ContractChanged},
		{401, nil, errs.AuthInvalid},
		{403, nil, errs.PermissionDenied},
		{403, h("Retry-After", "60"), errs.RateLimited},
		{403, h("X-RateLimit-Remaining", "0"), errs.RateLimited},
		{404, nil, errs.ContractChanged},
		{408, nil, errs.Transient},
		{410, nil, errs.ContractChanged},
		{422, nil, errs.ContractChanged},
		{429, nil, errs.RateLimited},
		{500, nil, errs.Transient},
		{501, nil, errs.ContractChanged},
		{503, nil, errs.Transient},
		{504, nil, errs.Transient},
	} {
		if got := classify(tc.status, tc.header); got != tc.want {
			t.Errorf("%d %v: %v, want %v", tc.status, tc.header, got, tc.want)
		}
	}
}

func TestAuth(t *testing.T) {
	secrets := StaticSecrets{"token": sdk.NewSecret("s3cret")}
	for _, tc := range []struct {
		auth  host.Auth
		check func(*http.Request) string
		want  string
	}{
		{host.Bearer{TokenSecret: "token"}, header("Authorization"), "Bearer s3cret"},
		{host.Basic{Username: "u", PasswordSecret: "token"}, header("Authorization"), "Basic dTpzM2NyZXQ="},
		{host.APIKey{KeySecret: "token", Header: "x-api-key", Prefix: "Token "}, header("X-Api-Key"), "Token s3cret"},
		{host.APIKey{KeySecret: "token", QueryParam: "key"}, rawQuery, "page=2&key=s3cret"},
	} {
		got := make(chan string, 1)
		srv, _ := server(t, func(_ http.ResponseWriter, r *http.Request) { got <- tc.check(r) })
		s, _ := newServices(t, Options{Policy: loopback(), Secrets: secrets})
		if _, err := do(t, s, &host.Request{URL: srv.URL + "/?page=2", Auth: tc.auth}); err != nil {
			t.Fatalf("%T: %v", tc.auth, err)
		}
		if g := <-got; g != tc.want {
			t.Errorf("%T: %q, want %q", tc.auth, g, tc.want)
		}
	}

	s, _ := newServices(t, Options{Policy: loopback(), Secrets: secrets})
	_, err := s.HTTP(t.Context(), &host.Request{URL: "https://example.com", Auth: host.Bearer{TokenSecret: "other"}})
	if errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("unknown secret: %v", err)
	}
	_, err = s.HTTP(t.Context(), &host.Request{URL: "https://example.com", Header: http.Header{"Authorization": {"x"}}, Auth: host.Bearer{TokenSecret: "token"}})
	if errs.CodeOf(err) != errs.Internal {
		t.Errorf("two Authorization headers: %v", err)
	}
}

// Credentials go to the request's origin only, not to where it redirects.
func TestCredentialsStayWithTheOrigin(t *testing.T) {
	seen := make(chan string, 4)
	creds := func(_ http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Get("Authorization") + "|" + r.Header.Get("X-Api-Key") + "|" + r.URL.Query().Get("key")
	}
	other, _ := server(t, creds)
	origin, _ := server(t, func(w http.ResponseWriter, r *http.Request) {
		creds(w, r)
		switch r.URL.Path {
		case "/same":
			http.Redirect(w, r, "/done", http.StatusFound)
		case "/away":
			http.Redirect(w, r, other.URL+"/elsewhere", http.StatusFound)
		}
	})
	s, _ := newServices(t, Options{Policy: loopback(), Secrets: StaticSecrets{"k": sdk.NewSecret("v")}})
	for _, a := range []host.Auth{host.Bearer{TokenSecret: "k"}, host.APIKey{KeySecret: "k", Header: "X-Api-Key"}, host.APIKey{KeySecret: "k", QueryParam: "key"}} {
		if _, err := do(t, s, &host.Request{URL: origin.URL + "/away", Auth: a}); err != nil {
			t.Fatal(err)
		}
		if <-seen == "||" {
			t.Errorf("%T: the origin got no credential", a)
		}
		if g := <-seen; g != "||" {
			t.Errorf("%T: the redirect target got %q", a, g)
		}
		if _, err := do(t, s, &host.Request{URL: origin.URL + "/same", Auth: a}); err != nil {
			t.Fatal(err)
		}
		if first, second := <-seen, <-seen; first == "||" || second == "||" {
			t.Errorf("%T: a redirect within the origin lost the credential", a)
		}
	}
}

func TestBlockedAddressesAreConfigErrors(t *testing.T) {
	srv, hits := server(t, status(200))
	s, c := newServices(t, Options{})
	_, err := do(t, s, &host.Request{URL: srv.URL})
	if _, ok := errors.AsType[*netguard.BlockedError](err); !ok || errs.CodeOf(err) != errs.ConfigInvalid {
		t.Fatal(err)
	}
	if hits.Load() != 0 || len(c.slept) != 0 {
		t.Errorf("%d tries, slept %v", hits.Load(), c.slept)
	}
}

func TestBodyLimit(t *testing.T) {
	srv, _ := server(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(make([]byte, 100)) })
	s, _ := newServices(t, Options{Policy: loopback(), MaxBodyBytes: 10})
	_, err := do(t, s, &host.Request{URL: srv.URL})
	if errs.CodeOf(err) != errs.ResourceExhausted {
		t.Fatal(err)
	}
}

func TestRateLimitWaitPastTheBudget(t *testing.T) {
	srv, hits := server(t, status(200))
	s, c := newServices(t, Options{Policy: loopback(), Budget: 10 * time.Second, RateLimits: map[string]float64{"api": 0.01}})
	r := &host.Request{URL: srv.URL, RateLimitKey: "api"}
	if _, err := do(t, s, r); err != nil {
		t.Fatal(err)
	}
	_, err := do(t, s, r)
	if errs.CodeOf(err) != errs.RateLimited || errs.RetryAfterOf(err) != 100*time.Second {
		t.Fatalf("%v (retry after %v)", err, errs.RetryAfterOf(err))
	}
	if hits.Load() != 1 || len(c.slept) != 0 {
		t.Errorf("%d tries, slept %v", hits.Load(), c.slept)
	}
}

func TestLimiterAdapts(t *testing.T) {
	now := start
	l := newLimiter(10)
	if d, _ := l.reserve(now); d != 0 {
		t.Fatalf("first request waits %v", d)
	}
	if d, _ := l.reserve(now); d != 100*time.Millisecond {
		t.Fatalf("second request waits %v", d)
	}
	l.throttled(now, 3*time.Second)
	if l.limit() != 5 {
		t.Fatalf("limit %v after a throttle", l.limit())
	}
	l.throttled(now.Add(500*time.Millisecond), 0)
	if l.limit() != 5 {
		t.Errorf("limit %v after a second throttle within a second", l.limit())
	}
	if d, _ := l.reserve(now); d < 3*time.Second {
		t.Errorf("a request during the pause waits %v", d)
	}
	l.ok(now.Add(500 * time.Millisecond))
	l.ok(now.Add(time.Second))
	if l.limit() != 6 {
		t.Errorf("limit %v after a second without throttles", l.limit())
	}
	for i := range 20 {
		l.ok(now.Add(time.Duration(2+i) * time.Second))
	}
	if l.limit() != 10 {
		t.Errorf("limit %v, want the ceiling", l.limit())
	}

	// Unlimited: the first throttle halves the rate it was sending at.
	u := newLimiter(rate.Inf)
	for range 8 {
		u.reserve(now)
	}
	u.throttled(now, 0)
	if u.limit() != 4 {
		t.Errorf("limit %v, want 4", u.limit())
	}
}

func header(name string) func(*http.Request) string {
	return func(r *http.Request) string { return r.Header.Get(name) }
}

func rawQuery(r *http.Request) string { return r.URL.RawQuery }
