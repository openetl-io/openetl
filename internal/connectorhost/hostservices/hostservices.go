// Package hostservices implements host services for the connectors that
// run inside openetld.
package hostservices

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/unsuitable001/openetl/internal/netguard"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/sdk/go/host"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

const (
	defaultBudget       = 5 * time.Minute
	defaultMaxBodyBytes = 256 << 20
	// How much of a failed response's body a StatusError keeps.
	maxErrorBody = 64 << 10
	baseBackoff  = 500 * time.Millisecond
	maxBackoff   = 30 * time.Second
)

type Options struct {
	Policy  netguard.Policy
	Secrets Secrets
	// Requests per second, by rate limit key. Every key adapts when the API
	// throttles; one without a rate here starts unlimited.
	RateLimits map[string]float64
	// Zero: 5 minutes.
	Budget time.Duration
	// Zero: 256 MiB.
	MaxBodyBytes int64
}

// Services is one connector instance's host services. It's safe for
// concurrent use.
type Services struct {
	opts   Options
	client *http.Client

	mu       sync.Mutex
	limiters map[string]*limiter

	now   func() time.Time
	sleep func(context.Context, time.Duration) error
}

var _ host.Host = (*Services)(nil)

func New(o Options) *Services {
	o.Budget = cmp.Or(o.Budget, defaultBudget)
	o.MaxBodyBytes = cmp.Or(o.MaxBodyBytes, defaultMaxBodyBytes)
	return &Services{
		opts:     o,
		client:   o.Policy.ClientVia(func(rt http.RoundTripper) http.RoundTripper { return &egress{base: rt} }),
		limiters: map[string]*limiter{},
		now:      time.Now,
		sleep:    sleep,
	}
}

// Close closes idle connections.
func (s *Services) Close() { s.client.CloseIdleConnections() }

func (s *Services) HTTP(ctx context.Context, r *host.Request) (*host.Response, error) {
	u, err := url.Parse(r.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errs.New(errs.ConfigInvalid, "HTTP: the URL must be absolute, with scheme http or https")
	}
	method := cmp.Or(r.Method, http.MethodGet)
	cred, err := s.credential(ctx, r.Auth)
	if err != nil {
		return nil, err
	}
	if cred != nil && cred.header() != "" && r.Header.Get(cred.header()) != "" {
		return nil, errs.New(errs.Internal, "HTTP: the request sets %s, which its auth sets", cred.header())
	}
	lim := s.limiter(cmp.Or(r.RateLimitKey, strings.ToLower(u.Host)))
	retryAny := r.Idempotent || idempotent(method)
	deadline := s.now().Add(cmp.Or(r.Budget, s.opts.Budget))
	ctx = withAuth(ctx, u, cred)

	for try := 0; ; try++ {
		now := s.now()
		if wait, giveBack := lim.reserve(now); wait > 0 {
			if now.Add(wait).After(deadline) {
				giveBack()
				return nil, &errs.Error{Code: errs.RateLimited, Msg: "HTTP: waiting for the rate limit would exceed the budget", RetryAfter: wait}
			}
			if err := s.sleep(ctx, wait); err != nil {
				return nil, err
			}
		}
		resp, cancel, err := s.send(ctx, method, u, r, deadline)
		now = s.now()
		var wait time.Duration
		var final error
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			final = classifyErr(err)
			if errs.CodeOf(final) != errs.Transient || (!retryAny && !notSent(err)) {
				return nil, final
			}
			wait = backoff(try)
		case resp.StatusCode < 400:
			lim.ok(now)
			return &host.Response{
				Status: resp.StatusCode,
				Header: resp.Header,
				Body:   &body{rc: netguard.LimitBody(resp.Body, s.opts.MaxBodyBytes), cancel: cancel},
			}, nil
		default:
			se := statusError(resp, now)
			cancel()
			final = se
			code := errs.Code(se.Code)
			if code == errs.RateLimited {
				lim.throttled(now, se.After)
			} else {
				lim.ok(now)
			}
			if code != errs.RateLimited && (code != errs.Transient || !retryAny) {
				return nil, se
			}
			wait = cmp.Or(se.After, backoff(try))
		}
		if now.Add(wait).After(deadline) {
			return nil, final
		}
		if err := s.sleep(ctx, wait); err != nil {
			return nil, err
		}
	}
}

// send makes one try, and returns a function that releases it once the
// body is read. Waiting for the response's head counts against the budget;
// reading its body doesn't.
func (s *Services) send(ctx context.Context, method string, u *url.URL, r *host.Request, deadline time.Time) (*http.Response, context.CancelFunc, error) {
	tctx, cancel := context.WithCancel(ctx)
	var b io.Reader
	if r.Body != nil {
		b = bytes.NewReader(r.Body)
	}
	req, err := http.NewRequestWithContext(tctx, method, u.String(), b)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	if r.Header != nil {
		req.Header = r.Header.Clone()
	}
	timer := time.AfterFunc(deadline.Sub(s.now()), cancel)
	resp, err := s.client.Do(req)
	if !timer.Stop() && ctx.Err() == nil {
		if err == nil {
			_ = resp.Body.Close()
		}
		err = errors.New("the budget ran out waiting for a response")
	}
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return resp, cancel, nil
}

func statusError(resp *http.Response, now time.Time) *host.StatusError {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	_ = resp.Body.Close()
	code := classify(resp.StatusCode, resp.Header)
	return &host.StatusError{
		Status: resp.StatusCode,
		Header: resp.Header,
		Body:   b,
		Code:   protocolv1.ErrorCode(code),
		After:  after(code, resp.Header, now),
	}
}

func (s *Services) limiter(key string) *limiter {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.limiters[key]
	if !ok {
		ceiling := rate.Inf
		if r := s.opts.RateLimits[key]; r > 0 {
			ceiling = rate.Limit(r)
		}
		l = newLimiter(ceiling)
		s.limiters[key] = l
	}
	return l
}

type body struct {
	rc     io.ReadCloser
	cancel context.CancelFunc
}

func (b *body) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	switch {
	case err == nil, err == io.EOF:
		return n, err
	case errors.Is(err, netguard.ErrTooLarge):
		return n, errs.Wrap(errs.ResourceExhausted, err, "HTTP")
	default:
		return n, errs.Wrap(errs.Transient, err, "HTTP: reading the body")
	}
}

func (b *body) Close() error {
	err := b.rc.Close()
	b.cancel()
	return err
}

func idempotent(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace, http.MethodPut, http.MethodDelete:
		return true
	}
	return false
}

// backoff is exponential, with jitter over its upper half.
func backoff(try int) time.Duration {
	d := min(maxBackoff, baseBackoff<<min(try, 16))
	return d/2 + rand.N(d/2+1) //nolint:gosec // jitter needn't be unpredictable
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
