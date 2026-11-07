package hostservices

import (
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/openetl-io/openetl/internal/netguard"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

// classify maps a failed status to the taxonomy. A manifest's error mapping
// can override it.
func classify(status int, h http.Header) errs.Code {
	switch {
	case status == http.StatusUnauthorized:
		return errs.AuthInvalid
	case status == http.StatusForbidden:
		// GitHub, among others, throttles with a 403.
		if h.Get("Retry-After") != "" || h.Get("X-RateLimit-Remaining") == "0" {
			return errs.RateLimited
		}
		return errs.PermissionDenied
	case status == http.StatusTooManyRequests:
		return errs.RateLimited
	case status == http.StatusRequestTimeout, status == http.StatusTooEarly:
		return errs.Transient
	case status == http.StatusNotImplemented, status == http.StatusHTTPVersionNotSupported:
		return errs.ContractChanged
	case status >= 500:
		return errs.Transient
	default:
		return errs.ContractChanged
	}
}

// after returns how long a response says to wait: Retry-After, or for a
// throttled response the rate limit's reset.
func after(code errs.Code, h http.Header, now time.Time) time.Duration {
	if d, ok := retryAfter(h, now); ok {
		return d
	}
	if code != errs.RateLimited {
		return 0
	}
	for _, k := range []string{"X-RateLimit-Reset", "RateLimit-Reset"} {
		n, err := strconv.ParseInt(strings.TrimSpace(h.Get(k)), 10, 64)
		if err != nil || n < 0 {
			continue
		}
		// Some APIs send seconds from now, others a Unix time.
		if n < 1e9 {
			return time.Duration(n) * time.Second
		}
		return max(0, time.Unix(n, 0).Sub(serverNow(h, now)))
	}
	return 0
}

func retryAfter(h http.Header, now time.Time) (time.Duration, bool) {
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0, false
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
		return time.Duration(n) * time.Second, true
	}
	t, err := http.ParseTime(v)
	if err != nil {
		return 0, false
	}
	return max(0, t.Sub(serverNow(h, now))), true
}

// serverNow reads the response's Date, so times the server sends aren't off
// by the clocks' skew.
func serverNow(h http.Header, now time.Time) time.Time {
	if t, err := http.ParseTime(h.Get("Date")); err == nil {
		return t
	}
	return now
}

// classifyErr classifies a request that got no response.
func classifyErr(err error) error {
	var dns *net.DNSError
	switch {
	case errors.As(err, new(*errs.Error)):
		return err
	case errors.As(err, new(*netguard.BlockedError)):
		return errs.Wrap(errs.ConfigInvalid, err, "HTTP")
	case errors.As(err, new(*netguard.RedirectError)):
		return errs.Wrap(errs.ContractChanged, err, "HTTP")
	case errors.As(err, &dns) && dns.IsNotFound:
		return errs.Wrap(errs.ConfigInvalid, err, "HTTP")
	case errors.As(err, new(*tls.CertificateVerificationError)):
		return errs.Wrap(errs.ConfigInvalid, err, "HTTP")
	default:
		return errs.Wrap(errs.Transient, err, "HTTP")
	}
}

// notSent reports whether a request failed before any of it was sent, so it
// can be retried whatever its method.
func notSent(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}
