package httpcheck

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/host"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// fake answers each path with a body for 200, or the error.
type fake map[string]any

func (f fake) HTTP(_ context.Context, req *host.Request) (*host.Response, error) {
	path := req.URL[strings.Index(req.URL, "/v1"):]
	path, _, _ = strings.Cut(path, "?")
	switch a := f[path].(type) {
	case string:
		return &host.Response{Status: 200, Body: io.NopCloser(strings.NewReader(a))}, nil
	case error:
		return nil, a
	}
	return nil, &host.StatusError{Status: 404, Code: protocolv1.ErrorCode_ERROR_CODE_CONTRACT_CHANGED}
}

// doer answers every request with f's answer.
type doer func(*host.Request) (*host.Response, error)

func (d doer) HTTP(_ context.Context, r *host.Request) (*host.Response, error) { return d(r) }

// rpcErr is what host.Client gives for a request that got no response: a
// code and the host's message, with nothing to unwrap.
type rpcErr struct {
	code protocolv1.ErrorCode
	msg  string
}

func (e *rpcErr) Error() string                   { return e.msg }
func (e *rpcErr) ErrorCode() protocolv1.ErrorCode { return e.code }

func run(t *testing.T, checks ...check.Check) check.Report {
	t.Helper()
	return check.Run(t.Context(), checks)
}

// rendered is the report's items rendered, each checked for placeholders
// left unfilled.
func rendered(t *testing.T, r check.Report) []check.Item {
	t.Helper()
	var items []check.Item
	for _, it := range r.Items {
		it = it.Rendered()
		text := it.Title + it.Message
		if it.Failure != nil {
			text += it.Failure.Message + it.Failure.Fix
		}
		if strings.Contains(text, "{") {
			t.Errorf("%s has a placeholder unfilled: %q", it.Name, text)
		}
		items = append(items, it)
	}
	return items
}

func TestChecks(t *testing.T) {
	api := fake{
		"/v1/me":     `{"plan": "free"}`,
		"/v1/orders": &host.StatusError{Status: 403, Code: protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, Body: []byte(`{"message": "missing scope read:orders"}`)},
	}
	req := func(path string) host.Request {
		return host.Request{URL: "https://user:pw@api.example.com" + path + "?api_key=s3cret#x"}
	}
	r := run(t,
		Reach(api, req("/v1/me")),
		Authenticate(api, req("/v1/me")),
		Access("orders.read", "Read orders", api, req("/v1/orders")),
		Access("refunds.read", "Read refunds", api, req("/v1/refunds")),
		JSON("plan", "Have a paid plan", api, req("/v1/me"), func(_ context.Context, p *check.Probe, b *struct{ Plan string }) error {
			p.Param("plan", b.Plan)
			if b.Plan == "free" {
				return check.Warn(check.QuotaExceeded.Fail())
			}
			return nil
		}),
	)
	var got []string
	for _, it := range r.Items {
		got = append(got, it.Name+"="+it.Status.String())
	}
	if strings.Join(got, " ") != "api.reach=passed api.auth=passed orders.read=failed refunds.read=failed plan=warning" {
		t.Fatalf("%v", got)
	}
	items := rendered(t, r)
	for _, it := range items {
		for k, v := range it.Params {
			if strings.Contains(v, "s3cret") || strings.Contains(v, "pw@") {
				t.Errorf("%s's param %s leaks a credential: %s", it.Name, k, v)
			}
		}
	}
	if u := items[0].Params["url"]; u != "https://api.example.com/v1/me" {
		t.Errorf("url %q", u)
	}
	orders := items[2].Failure
	if orders.Reason != "permission_denied" || orders.Code != protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED || items[2].Params["status"] != "403" ||
		orders.Detail != `HTTP 403 Forbidden: {"message": "missing scope read:orders"}` {
		t.Errorf("orders: %+v", orders)
	}
	refunds := items[3].Failure
	if refunds.Reason != "not_found" || refunds.Code != protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID ||
		refunds.Message != "https://api.example.com/v1/refunds doesn't exist." {
		t.Errorf("a 404 the host calls CONTRACT_CHANGED: %+v", refunds)
	}
	if items[4].Params["plan"] != "free" {
		t.Errorf("plan: %v", items[4].Params)
	}
}

func TestClassifyStatus(t *testing.T) {
	const (
		unspecified = protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED
		contract    = protocolv1.ErrorCode_ERROR_CODE_CONTRACT_CHANGED
	)
	for _, tc := range []struct {
		status int
		code   protocolv1.ErrorCode
		reason string
		want   protocolv1.ErrorCode
		msg    string
	}{
		{400, contract, "config_invalid", protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, "The config doesn't work with the upstream."},
		{401, protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, "credentials_rejected", protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, ""},
		{401, protocolv1.ErrorCode_ERROR_CODE_AUTH_EXPIRED, "credentials_expired", protocolv1.ErrorCode_ERROR_CODE_AUTH_EXPIRED, ""},
		{403, protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, "permission_denied", protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, ""},
		{403, protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED, "rate_limited", protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED, ""},
		{404, contract, "not_found", protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, "https://api.example.com/v1/x doesn't exist."},
		{410, contract, "not_found", protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, "https://api.example.com/v1/x doesn't exist."},
		{429, protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED, "rate_limited", protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED, ""},
		{405, contract, "unknown", contract, "https://api.example.com/v1/x answered with HTTP 405."},
		{422, contract, "unknown", contract, "https://api.example.com/v1/x answered with HTTP 422."},
		{501, contract, "unknown", contract, "https://api.example.com/v1/x answered with HTTP 501."},
		{500, unspecified, "unavailable", protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, ""},
		{503, protocolv1.ErrorCode_ERROR_CODE_DESTINATION_UNAVAILABLE, "unavailable", protocolv1.ErrorCode_ERROR_CODE_DESTINATION_UNAVAILABLE, ""},
		// Codes a manifest maps the status to, which win over the status.
		{403, protocolv1.ErrorCode_ERROR_CODE_RESOURCE_EXHAUSTED, "quota_exceeded", protocolv1.ErrorCode_ERROR_CODE_RESOURCE_EXHAUSTED, ""},
		{400, protocolv1.ErrorCode_ERROR_CODE_AUTH_EXPIRED, "credentials_expired", protocolv1.ErrorCode_ERROR_CODE_AUTH_EXPIRED, ""},
		{404, protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, "permission_denied", protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, ""},
		{500, protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, "config_invalid", protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, ""},
	} {
		se := &host.StatusError{Status: tc.status, Code: tc.code, After: time.Minute}
		f := Classify(se, "https://api.example.com/v1/x?key=s3cret")
		msg := check.Render(f.Message, f.Params)
		if f.Reason != tc.reason || f.Code != tc.want || f.After != time.Minute || f.Params["status"] != fmt.Sprint(tc.status) ||
			tc.msg != "" && msg != tc.msg || strings.Contains(msg, "{") {
			t.Errorf("HTTP %d (%s): %s %s %s %q", tc.status, tc.code, f.Reason, f.Code, f.After, msg)
		}
	}
	if Classify(nil, "https://api.example.com") != nil {
		t.Error("a nil error failed")
	}
}

func TestClassifyKeepsTheBodysStart(t *testing.T) {
	// A long body whose cut falls inside a rune, with an invalid byte and
	// lines in it.
	body := "{\"error\":\n  \"missing scope \xff read:orders\"}" + strings.Repeat("é", 400)
	f := Classify(&host.StatusError{Status: 403, Code: protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, Body: []byte(body)}, "https://api.example.com/v1/orders")
	if !strings.HasPrefix(f.Detail, `HTTP 403 Forbidden: {"error": "missing scope read:orders"}éé`) || !utf8.ValidString(f.Detail) ||
		len(f.Detail) > len("HTTP 403 Forbidden: ")+maxBodyInDetail {
		t.Errorf("%d bytes: %q", len(f.Detail), f.Detail)
	}
	f = Classify(&host.StatusError{Status: 502, Body: []byte("<html>Bad gateway</html>")}, "https://api.example.com/v1/orders")
	if f.Detail != "HTTP 502 Bad Gateway" {
		t.Errorf("a 5xx's body: %q", f.Detail)
	}
}

func TestNoResponse(t *testing.T) {
	const raw = "https://user:pw@api.example.com/v1/me?token=s3cret"
	refused := &url.Error{Op: "Get", URL: raw, Err: &net.OpError{
		Op: "dial", Net: "tcp", Addr: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 443},
		Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED},
	}}
	for _, tc := range []struct {
		name   string
		err    error
		reason string
		code   protocolv1.ErrorCode
		msg    string
	}{
		{
			"a host in another process, without a response",
			&rpcErr{protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, `TRANSIENT: HTTP: Get "https://user:***@api.example.com/v1/me?token=s3cret": dial tcp 10.0.0.1:443: connect: connection refused`},
			"unreachable", protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, "Got no response from https://api.example.com/v1/me.",
		},
		{
			"a host in another process, with a name that doesn't resolve",
			&rpcErr{protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, `CONFIG_INVALID: HTTP: Get "https://api.example.com/v1/me?token=s3cret": dial tcp: lookup api.example.com: no such host`},
			"config_invalid", protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, "The request to https://api.example.com/v1/me wasn't sent.",
		},
		{
			"a host in this process",
			fmt.Errorf("HTTP: %w", refused),
			"connection_refused", protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, "api.example.com:443 refused the connection.",
		},
		{
			"a request that ran out of time",
			fmt.Errorf("HTTP: %w", context.DeadlineExceeded),
			"timed_out", protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, "api.example.com:443 didn't answer in time.",
		},
		{
			"an error without a code",
			errors.New("HTTP: the host sent no response head"),
			"unknown", protocolv1.ErrorCode_ERROR_CODE_INTERNAL, "The request to https://api.example.com/v1/me failed.",
		},
	} {
		d := doer(func(*host.Request) (*host.Response, error) { return nil, tc.err })
		it := rendered(t, run(t, Reach(d, host.Request{URL: raw})))[0]
		f := it.Failure
		if f.Reason != tc.reason || f.Code != tc.code || f.Message != tc.msg || f.Fix == "" {
			t.Errorf("%s: %s %s %q %q", tc.name, f.Reason, f.Code, f.Message, f.Fix)
		}
		if strings.Contains(f.Detail, "s3cret") || strings.Contains(f.Detail, "user:") || f.Detail == "" {
			t.Errorf("%s: the detail %q", tc.name, f.Detail)
		}
	}
}

func TestBudget(t *testing.T) {
	var got []time.Duration
	d := doer(func(r *host.Request) (*host.Response, error) {
		got = append(got, r.Budget)
		return &host.Response{Status: 200, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})
	run(t, Reach(d, host.Request{URL: "https://api.example.com/v1/me"}),
		Access("me", "Me", d, host.Request{URL: "https://api.example.com/v1/me", Budget: time.Second}))
	if len(got) != 2 || got[0] != 10*time.Second || got[1] != time.Second {
		t.Errorf("budgets %v", got)
	}
}

func TestJSONBodies(t *testing.T) {
	for _, tc := range []struct {
		body, reason, msg string
		code              protocolv1.ErrorCode
	}{
		{"<html>Sign in</html>", "config_invalid", "https://api.example.com/v1/me didn't answer with JSON.", protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID},
		{`{"plan": 7}`, "unknown", "The JSON from https://api.example.com/v1/me isn't what the connector expects.", protocolv1.ErrorCode_ERROR_CODE_CONTRACT_CHANGED},
		{`{"plan": "` + strings.Repeat("x", maxBody) + `"}`, "unknown", "The response from https://api.example.com/v1/me is larger than 1048576 bytes, which is all the check reads.", protocolv1.ErrorCode_ERROR_CODE_INTERNAL},
	} {
		d := doer(func(*host.Request) (*host.Response, error) {
			return &host.Response{Status: 200, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
		})
		it := rendered(t, run(t, JSON("me", "Me", d, host.Request{URL: "https://api.example.com/v1/me"}, func(context.Context, *check.Probe, *struct{ Plan string }) error {
			return nil
		})))[0]
		if f := it.Failure; it.Status != check.Failed || f.Reason != tc.reason || f.Message != tc.msg || f.Code != tc.code {
			t.Errorf("%.20s: %s %+v", tc.body, it.Status, f)
		}
	}
	d := doer(func(*host.Request) (*host.Response, error) {
		return &host.Response{Status: 200, Body: io.NopCloser(strings.NewReader("<html>Sign in</html>"))}, nil
	})
	f := run(t, JSON("me", "Me", d, host.Request{URL: "https://api.example.com/v1/me"}, func(context.Context, *check.Probe, *map[string]any) error { return nil })).Items[0].Failure
	if !strings.HasSuffix(f.Detail, "the response starts: <html>Sign in</html>") {
		t.Errorf("the detail %q", f.Detail)
	}
}

func TestReachFailsOnlyWithoutAnAnswer(t *testing.T) {
	api := fake{"/v1/down": &host.StatusError{Status: 502}}
	down := Reach(api, host.Request{URL: "https://api.example.com/v1/down"})
	down.Name = "api.down"
	r := run(t, Reach(api, host.Request{URL: "https://api.example.com/v1/nope"}), down)
	if r.Items[0].Status != check.Passed || r.Items[1].Status != check.Failed || r.Items[1].Failure.Reason != "unavailable" {
		t.Errorf("%+v", r.Items)
	}
}

func TestAuthenticateNeedsReach(t *testing.T) {
	c := Authenticate(fake{}, host.Request{URL: "https://api.example.com/v1/me"})
	if err := check.Validate([]check.Check{c}); err == nil || !strings.Contains(err.Error(), `needs "api.reach"`) {
		t.Errorf("Authenticate alone: %v", err)
	}
}
