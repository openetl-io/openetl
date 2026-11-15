package check

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

func closedPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// get is the error of a GET of target, which mustn't answer.
func get(t *testing.T, target string) error {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatalf("%s answered", target)
	}
	return err
}

func parseURL(raw string) error {
	_, err := url.Parse(raw)
	return err
}

func TestClassify(t *testing.T) {
	addr := closedPort(t)
	_, refused := net.DialTimeout("tcp", addr, time.Second)
	if refused == nil {
		t.Fatal("the closed port answered")
	}
	_, noPort := net.Dial("tcp", "db")
	badURL := parseURL("postgres://etl@db:port/x")
	httpRefused := get(t, "http://"+addr+"/v1/me")
	scheme := get(t, "ftp://"+addr)
	own := NotFound.Fail("resource", "t")
	unavailable := protocolv1.ErrorCode_ERROR_CODE_DESTINATION_UNAVAILABLE
	for _, tc := range []struct {
		name   string
		err    error
		reason string
		code   protocolv1.ErrorCode
	}{
		{"refused", refused, "connection_refused", transient},
		{"refused, coded", fmt.Errorf("%w: %w", &codedErr{code: unavailable}, refused), "connection_refused", unavailable},
		{"refused over http", httpRefused, "connection_refused", transient},
		{"dns", &net.DNSError{Name: "nope.invalid", IsNotFound: true}, "dns_failed", transient},
		{"deadline", fmt.Errorf("ping: %w", context.DeadlineExceeded), "timed_out", transient},
		{"auth", &codedErr{code: protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID}, "credentials_rejected", protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID},
		{"drift", &codedErr{code: protocolv1.ErrorCode_ERROR_CODE_SCHEMA_DRIFT}, "unknown", protocolv1.ErrorCode_ERROR_CODE_SCHEMA_DRIFT},
		{"plain", errors.New("x"), "unknown", protocolv1.ErrorCode_ERROR_CODE_INTERNAL},
		{"a failure", fmt.Errorf("wrapped: %w", own), "not_found", configInvalid},
		// Mistakes in the config, not the network's.
		{"no port", noPort, "config_invalid", configInvalid},
		{"a bad IP", &net.ParseError{Type: "IP address", Text: "10.0.0"}, "config_invalid", configInvalid},
		{"a URL that doesn't parse", badURL, "config_invalid", configInvalid},
		// A *url.Error is a net.Error whatever it wraps.
		{"an unsupported scheme", scheme, "unknown", protocolv1.ErrorCode_ERROR_CODE_INTERNAL},
	} {
		if tc.err == nil {
			t.Fatalf("%s: no error", tc.name)
		}
		f := Classify(tc.err)
		if f.Reason != tc.reason || f.Code != tc.code {
			t.Errorf("%s: %s %s, want %s %s (%v)", tc.name, f.Reason, f.Code, tc.reason, tc.code, tc.err)
		}
		if strings.Contains(f.Error(), "{") {
			t.Errorf("%s: %q has an unfilled placeholder", tc.name, f.Error())
		}
	}
	if f := Classify(refused); f.Params["address"] != addr {
		t.Errorf("refused: params %v, want address %s", f.Params, addr)
	}
	if f := Classify(&net.DNSError{Name: "nope.invalid"}); f.Params["host"] != "nope.invalid" {
		t.Errorf("dns: params %v", f.Params)
	}
	if f := Classify(&codedErr{code: protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED, after: time.Minute}); f.After != time.Minute {
		t.Errorf("retry after %s", f.After)
	}
	if f := NotFound.Wrap(&codedErr{code: transient}); f.Code != configInvalid {
		t.Errorf("Wrap took the cause's code %s", f.Code)
	}
}

// A URL's userinfo and query don't reach Detail.
func TestURLSecrets(t *testing.T) {
	sent := get(t, "http://etl:hunter2@"+closedPort(t)+"/v1/me?api_key=sk_live_SECRET#frag")
	parsed := parseURL("postgres://etl:p@ss@db:port/x?password=hunter2")
	for name, err := range map[string]error{"sent": sent, "unparsed": parsed, "wrapped": fmt.Errorf("reach the API: %w", sent)} {
		f := Classify(err)
		for _, secret := range []string{"hunter2", "sk_live_SECRET", "p@ss", "etl:", "frag"} {
			if strings.Contains(f.Detail, secret) || strings.Contains(f.Error(), secret) {
				t.Errorf("%s: %q is in the detail %q", name, secret, f.Detail)
			}
		}
		if !strings.Contains(f.Detail, "/") {
			t.Errorf("%s: the detail %q lost the URL", name, f.Detail)
		}
	}
	if got := redactURL("https://u:p@api.example.com:8443/v1/x?k=v#f"); got != "https://api.example.com:8443/v1/x" {
		t.Errorf("redactURL: %q", got)
	}
}

// Error doesn't read "sentence.: detail", and wrapping text around a
// *Failure stays in its Detail.
func TestErrorText(t *testing.T) {
	r := Run(t.Context(), []Check{{Name: "a", Title: "Use schema", Run: fails(PermissionDenied.Wrap(errors.New("pq: permission denied for schema public")))}})
	if got := r.Err().Error(); got != "Use schema: The upstream denied access: pq: permission denied for schema public" {
		t.Errorf("Err: %q", got)
	}
	for _, tc := range []struct {
		err          error
		detail, text string
	}{
		{fmt.Errorf("schema analytics: %w", PermissionDenied.Fail()), "schema analytics", "The upstream denied access: schema analytics"},
		{fmt.Errorf("schema analytics: %w", PermissionDenied.Wrap(errors.New("pq: denied"))), "schema analytics: pq: denied", "The upstream denied access: schema analytics: pq: denied"},
		{Warn(PermissionDenied.Wrap(errors.New("pq: denied"))), "pq: denied", "The upstream denied access: pq: denied"},
		{PermissionDenied.Fail(), "", "The upstream denied access."},
	} {
		if f := Classify(tc.err); f.Detail != tc.detail || f.Error() != tc.text {
			t.Errorf("Classify(%q): detail %q, text %q; want %q, %q", tc.err, f.Detail, f.Error(), tc.detail, tc.text)
		}
	}
}

// With and Classify give copies: changing one leaves the original alone.
func TestCopies(t *testing.T) {
	own := NotFound.Fail("resource", "t")
	_ = own.With("extra", "1")
	c := Classify(fmt.Errorf("x: %w", own))
	c.Params["resource"] = "u"
	_ = c.With("more", "2")
	if len(own.Params) != 1 || own.Params["resource"] != "t" {
		t.Errorf("the original's params are %v", own.Params)
	}
	if Classify(own) == own {
		t.Error("Classify gave the failure itself")
	}
}

// A name without a value gets !MISSING, rather than a panic that would
// replace the check's failure.
func TestOddPairs(t *testing.T) {
	it := one(t, Check{Run: fails(PrivilegeMissing.Wrap(errors.New("ERROR: permission denied for table orders (SQLSTATE 42501)"), "principal"))})
	if f := it.Failure; f.Reason != "privilege_missing" || it.Params["principal"] != "!MISSING" {
		t.Errorf("%s, %v", f.Reason, it.Params)
	}
	if p := Unknown.Fail("a", "1", "b").With("c").Params; p["a"] != "1" || p["b"] != "!MISSING" || p["c"] != "!MISSING" {
		t.Errorf("params %v", p)
	}
}

// A hand-built Failure reads as Unknown's where it leaves something out.
func TestHandBuiltFailure(t *testing.T) {
	bare := &Failure{}
	if bare.Error() != Unknown.Message || bare.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_INTERNAL {
		t.Errorf("%q, %s", bare.Error(), bare.ErrorCode())
	}
	if f := Classify(bare); f.Reason != "unknown" || f.Code != protocolv1.ErrorCode_ERROR_CODE_INTERNAL || f.Message != Unknown.Message {
		t.Errorf("Classify: %+v", f)
	}
	it := one(t, Check{Run: fails(&Failure{Detail: "x"})})
	if f := it.Failure; f.Reason != "unknown" || f.Code != protocolv1.ErrorCode_ERROR_CODE_INTERNAL || it.Message != Unknown.Message {
		t.Errorf("Run: %+v", f)
	}
	if f := (Reason{Name: "own"}).Fail(); f.Code != protocolv1.ErrorCode_ERROR_CODE_INTERNAL || f.Message != Unknown.Message {
		t.Errorf("a reason without a code or a message: %+v", f)
	}
}

func TestNilFailures(t *testing.T) {
	var none *Failure
	if Classify(nil) != nil || Classify(none) != nil || NotFound.Wrap(nil) != nil || NotFound.Wrap(none) != nil || Warn(none) != nil {
		t.Error("a nil error made a failure")
	}
	if none.With("k", "v") != nil || none.Error() != "" || none.Unwrap() != nil || none.RetryAfter() != 0 || none.ErrorCode() != protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED {
		t.Error("a nil failure's methods")
	}
	if r := FromError(none); !r.OK() || r.Err() != nil {
		t.Errorf("FromError of a nil failure: %v", r.Err())
	}
}

func TestRender(t *testing.T) {
	p := Params{"host": "db", "n_1": "7", "a": "{host}"}
	for template, want := range map[string]string{
		"Reach {host}:{n_1} on {host}": "Reach db:7 on db",
		"{missing} stays":              "{missing} stays",
		`{"error": "x"} and {Host} {}`: `{"error": "x"} and {Host} {}`,
		"{a} isn't rendered again":     "{host} isn't rendered again",
		"":                             "",
	} {
		if got := Render(template, p); got != want {
			t.Errorf("Render(%q) = %q, want %q", template, got, want)
		}
	}
	if got := Placeholders("{b} {a} {b} {A}"); len(got) != 2 || got[0] != "b" || got[1] != "a" {
		t.Errorf("Placeholders: %v", got)
	}
}
