// Package httpcheck checks an HTTP API through the host's HTTP service.
//
//	me := host.Request{URL: cfg.BaseURL + "/v1/me", Auth: host.Bearer{TokenSecret: cfg.Token}}
//	checks := []check.Check{
//		httpcheck.Reach(h, me),        // api.reach
//		httpcheck.Authenticate(h, me), // api.auth, needs api.reach
//		httpcheck.Access("orders.read", "Read orders", h, host.Request{URL: cfg.BaseURL + "/v1/orders?limit=1", Auth: me.Auth}),
//	}
//
// A request whose Budget is zero gets 10 seconds, not the host's default,
// which allows for minutes of retries. A request's URL goes into params and
// messages without its user info, query and fragment, which can hold
// credentials.
//
// The checks hold no state of their own: one may run in any number of runs
// at once, as far as its Doer may.
package httpcheck

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/host"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// Doer is host.Host's HTTP, which the checks send their requests with.
type Doer interface {
	HTTP(ctx context.Context, req *host.Request) (*host.Response, error)
}

const (
	// A check's request's budget when it gives none.
	budget = 10 * time.Second
	// What JSON reads of a body, in bytes.
	maxBody = 1 << 20
	// What a failure's Detail keeps of a 4xx response's body, in bytes.
	maxBodyInDetail = 300
)

// redacted is u without its user info, query and fragment.
func redacted(u string) string {
	if p, err := url.Parse(u); err == nil {
		p.User, p.RawQuery, p.ForceQuery, p.Fragment, p.RawFragment = nil, "", false, "", ""
		return p.String()
	}
	scheme, rest := "", u
	if i := strings.Index(u, "://"); i >= 0 {
		scheme, rest = u[:i+3], u[i+3:]
	}
	if i := strings.IndexAny(rest, "?#"); i >= 0 {
		rest = rest[:i]
	}
	if i := strings.LastIndexByte(rest, '@'); i >= 0 {
		rest = rest[i+1:]
	}
	return scheme + rest
}

var network = map[string]check.Reason{
	check.ConnectionRefused.Name: check.ConnectionRefused, check.TimedOut.Name: check.TimedOut,
	check.TLSFailed.Name: check.TLSFailed, check.Unreachable.Name: check.Unreachable,
}

var urlInText = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*://[^\s"'<>]+`)

// redactText is s with every URL in it redacted.
func redactText(s string) string { return urlInText.ReplaceAllStringFunc(s, redacted) }

// address is u's host:port, with the scheme's port when u gives none.
func address(u string) string {
	p, err := url.Parse(u)
	if err != nil || p.Host == "" {
		return ""
	}
	if p.Port() != "" {
		return p.Host
	}
	port := "443"
	if p.Scheme == "http" {
		port = "80"
	}
	return net.JoinHostPort(p.Hostname(), port)
}

// Classify makes err, from Doer.HTTP for a request to rawURL, a failure, as
// the kit's checks do, for a check of the connector's own; nil for a nil
// err. It sets the param url, rawURL redacted, and those of the
// failure's templates: resource, the url, for check.NotFound; address, the
// url's host:port, for the network's reasons; and status for a
// *host.StatusError.
//
// A *host.StatusError is classified by its Code where the host, or the
// connector's manifest, gave one other than the status alone would get, and
// otherwise by the status: 400 check.ConfigInvalid, 401
// check.CredentialsRejected, 403 check.PermissionDenied, 404 and 410
// check.NotFound, 429 check.RateLimited, and 500 and above, but 501 and 505,
// check.Unavailable. Its Detail has the start of a 4xx response's body.
//
// Any other error is a request that got no response. One without the
// network's shape, as a host in another process gives, is check.Unreachable
// for the code TRANSIENT, check.ConfigInvalid for CONFIG_INVALID (a name that
// doesn't resolve, an address the host's egress rules refuse, or a
// certificate it doesn't trust), and else as check.Classify has it.
func Classify(err error, rawURL string) *check.Failure {
	if err == nil {
		return nil
	}
	u := redacted(rawURL)
	var f *check.Failure
	if se, ok := errors.AsType[*host.StatusError](err); ok {
		f = byStatus(err, se)
	} else {
		f = noResponse(err)
	}
	f.Detail = redactText(f.Detail)
	f = f.With("url", u)
	names := check.Placeholders(f.Message + " " + f.Fix)
	if slices.Contains(names, "resource") && f.Params["resource"] == "" {
		f = f.With("resource", u)
	}
	// The address the URL names, rather than the one dialled.
	if a := address(rawURL); a != "" && slices.Contains(names, "address") {
		f = f.With("address", a)
	}
	return f
}

// statusDefault reports whether code is what the host gives status alone.
func statusDefault(status int, code protocolv1.ErrorCode) bool {
	var d protocolv1.ErrorCode
	switch {
	case status == http.StatusUnauthorized:
		d = protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID
	case status == http.StatusForbidden:
		d = protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED
	case status == http.StatusTooManyRequests:
		d = protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED
	case status == http.StatusRequestTimeout, status == http.StatusTooEarly:
		d = protocolv1.ErrorCode_ERROR_CODE_TRANSIENT
	case status == http.StatusNotImplemented, status == http.StatusHTTPVersionNotSupported:
		d = protocolv1.ErrorCode_ERROR_CODE_CONTRACT_CHANGED
	case status >= 500:
		d = protocolv1.ErrorCode_ERROR_CODE_TRANSIENT
	default:
		d = protocolv1.ErrorCode_ERROR_CODE_CONTRACT_CHANGED
	}
	return code == d
}

// byCode is the reason for a host's code, if one fits it.
func byCode(code protocolv1.ErrorCode) (check.Reason, bool) {
	switch code {
	case protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID:
		return check.CredentialsRejected, true
	case protocolv1.ErrorCode_ERROR_CODE_AUTH_EXPIRED:
		return check.CredentialsExpired, true
	case protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED:
		return check.PermissionDenied, true
	case protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED:
		return check.RateLimited, true
	case protocolv1.ErrorCode_ERROR_CODE_RESOURCE_EXHAUSTED, protocolv1.ErrorCode_ERROR_CODE_DESTINATION_QUOTA:
		return check.QuotaExceeded, true
	case protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID:
		return check.ConfigInvalid, true
	case protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, protocolv1.ErrorCode_ERROR_CODE_DESTINATION_UNAVAILABLE:
		return check.Unavailable, true
	}
	return check.Reason{}, false
}

func byStatus(err error, se *host.StatusError) *check.Failure {
	var f *check.Failure
	if r, ok := byCode(se.Code); ok && !statusDefault(se.Status, se.Code) {
		f = r.Wrap(err)
		f.Code = se.Code
	} else {
		switch s := se.Status; {
		case s == http.StatusBadRequest:
			f = check.ConfigInvalid.Wrap(err)
		case s == http.StatusUnauthorized:
			f = check.CredentialsRejected.Wrap(err)
		case s == http.StatusForbidden:
			f = check.PermissionDenied.Wrap(err)
		case s == http.StatusNotFound, s == http.StatusGone:
			f = check.NotFound.Wrap(err)
		case s == http.StatusTooManyRequests:
			f = check.RateLimited.Wrap(err)
		case s >= 500 && s != http.StatusNotImplemented && s != http.StatusHTTPVersionNotSupported:
			f = check.Unavailable.Wrap(err)
		default:
			f = check.Unknown.Wrap(err)
			f.Message = "{url} answered with HTTP {status}."
			if se.Code != protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED {
				f.Code = se.Code
			}
		}
	}
	if se.Status < 500 {
		if b := bodyStart(se.Body); b != "" {
			f.Detail += ": " + b
		}
	}
	return f.With("status", strconv.Itoa(se.Status))
}

// bodyStart is the start of b as one line of valid UTF-8.
func bodyStart(b []byte) string {
	b = b[:min(len(b), maxBodyInDetail)]
	return strings.Join(strings.Fields(strings.ToValidUTF8(string(b), "")), " ")
}

// noResponse classifies the error of a request that got no response.
func noResponse(err error) *check.Failure {
	f := check.Classify(err)
	switch f.Reason {
	case check.ConnectionRefused.Name, check.TimedOut.Name, check.TLSFailed.Name, check.Unreachable.Name:
		// The standard message, which Classify fills with the URL's address,
		// where check.Classify had none to give.
		f.Message = network[f.Reason].Message
	case check.Unavailable.Name:
		// The host gives TRANSIENT, without a status, to a request that got
		// no response.
		g := check.Unreachable.Wrap(err)
		g.Code, g.Detail = f.Code, f.Detail
		g.Message = "Got no response from {url}."
		g.Fix = "Check the URL's host and port in the config, that the service is up, and that the runner's network reaches it."
		f = g
	case check.ConfigInvalid.Name:
		if f.Message == check.ConfigInvalid.Message {
			f.Message = "The request to {url} wasn't sent."
			f.Fix = "Check that the URL's host name resolves, that the runner's egress rules allow it, and that the runner trusts its certificate. The detail says which."
		}
	case check.Unknown.Name:
		f.Message = "The request to {url} failed."
		f.Fix = "Check the URL in the config. The detail has the cause."
	}
	return f
}

func send(ctx context.Context, h Doer, req host.Request) (*host.Response, error) {
	if req.Method == "" {
		req.Method = http.MethodGet
	}
	if req.Budget == 0 {
		req.Budget = budget
	}
	return h.HTTP(ctx, &req)
}

func fail(err error, u string) error {
	if err == nil {
		return nil
	}
	return Classify(err, u)
}

func newCheck(name string, kind check.Kind, title string, req host.Request, run func(context.Context, *check.Probe) error) check.Check {
	return check.Check{Name: name, Kind: kind, Title: title, Params: check.Params{"url": redacted(req.URL)}, Run: run}
}

// Reach is "api.reach", a Connection check that req gets an answer: any
// status below 500 passes. It sets the param url, and fails as Classify has
// it.
func Reach(h Doer, req host.Request) check.Check {
	return newCheck("api.reach", check.Connection, "Reach {url}", req, func(ctx context.Context, _ *check.Probe) error {
		resp, err := send(ctx, h, req)
		if se, ok := errors.AsType[*host.StatusError](err); ok && se.Status < 500 {
			return nil
		}
		if err != nil {
			return fail(err, req.URL)
		}
		return resp.Body.Close()
	})
}

// Authenticate is "api.auth", an Authentication check that req succeeds
// with its credentials. It needs "api.reach", so it goes after Reach in the
// same list, or check.Validate refuses the list. It sets the param url, and
// fails as Classify has it.
func Authenticate(h Doer, req host.Request) check.Check {
	c := Access("api.auth", "Sign in to {url}", h, req)
	c.Kind, c.Needs = check.Authentication, []string{"api.reach"}
	return c
}

// Access is a Permission check that req succeeds, its status below 400. It
// sets the param url, and fails as Classify has it: 403
// check.PermissionDenied and 404 check.NotFound, unless the host's code says
// otherwise. It needs nothing, so it can run without Reach; when it makes
// sense only once the API answers, set its Needs to "api.reach", so that
// an API that's down doesn't cost each such check its whole budget.
func Access(name, title string, h Doer, req host.Request) check.Check {
	return newCheck(name, check.Permission, title, req, func(ctx context.Context, _ *check.Probe) error {
		resp, err := send(ctx, h, req)
		if err != nil {
			return fail(err, req.URL)
		}
		return resp.Body.Close()
	})
}

// JSON is Access that also decodes the body, of at most 1 MiB, into a T and
// passes it to verify, whose error is the check's. A body that isn't JSON
// fails as check.ConfigInvalid, JSON that doesn't decode into a T as
// check.Unknown with the code CONTRACT_CHANGED, and a larger body as
// check.Unknown, each with a message that says which. Like Access, it needs
// nothing; set its Needs to "api.reach" when it makes sense only once the
// API answers.
func JSON[T any](name, title string, h Doer, req host.Request, verify func(ctx context.Context, p *check.Probe, body *T) error) check.Check {
	return newCheck(name, check.Permission, title, req, func(ctx context.Context, p *check.Probe) error {
		resp, err := send(ctx, h, req)
		if err != nil {
			return fail(err, req.URL)
		}
		defer func() { _ = resp.Body.Close() }()
		b, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
		if err != nil {
			return fail(err, req.URL)
		}
		if len(b) > maxBody {
			f := check.Unknown.Fail("limit", strconv.Itoa(maxBody))
			f.Message = "The response from {url} is larger than {limit} bytes, which is all the check reads."
			return f
		}
		var body T
		if err := json.Unmarshal(b, &body); err != nil {
			return undecoded(err, b)
		}
		return verify(ctx, p, &body)
	})
}

func undecoded(err error, body []byte) *check.Failure {
	if _, ok := errors.AsType[*json.UnmarshalTypeError](err); ok {
		f := check.Unknown.Wrap(err)
		f.Code = protocolv1.ErrorCode_ERROR_CODE_CONTRACT_CHANGED
		f.Message = "The JSON from {url} isn't what the connector expects."
		return f
	}
	f := check.ConfigInvalid.Wrap(err)
	f.Message = "{url} didn't answer with JSON."
	f.Fix = "Check the URL in the config: a page in place of the API's JSON often means a wrong base URL, or a proxy or sign-in page in the way."
	if s := bodyStart(body); s != "" {
		f.Detail += "; the response starts: " + s
	}
	return f
}
