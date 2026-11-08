package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
)

// Only sign-in's calls take no credential, and only the UI makes them.
func TestPublicRules(t *testing.T) {
	for proc, r := range rules {
		if r.public() != browserOnly[proc] {
			t.Errorf("%s: public %v, browser only %v", proc, r.public(), browserOnly[proc])
		}
	}
	if _, ok := permissions[noCredential]; ok {
		t.Errorf("%q is a permission", noCredential)
	}
}

func TestGuardCookies(t *testing.T) {
	s := &Server{PublicURL: "https://etl.example.com"}
	var got *http.Request
	h, err := s.guardCookies(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got = r }))
	if err != nil {
		t.Fatal(err)
	}
	const list = "/openetl.api.v1.PipelineService/ListPipelines"
	signIn := apiv1connect.AccessServiceCreateSessionProcedure
	cases := []struct {
		name, method, path string
		header             map[string]string
		cookie, reached    bool
	}{
		{"no cookie", "POST", list, nil, false, true},
		{"same origin", "POST", list, map[string]string{"Connect-Protocol-Version": "1", "Sec-Fetch-Site": "same-origin"}, true, true},
		{"the public URL behind a proxy", "POST", list, map[string]string{"Connect-Protocol-Version": "1", "Origin": "https://etl.example.com"}, true, true},
		{"cross site", "POST", list, map[string]string{"Connect-Protocol-Version": "1", "Sec-Fetch-Site": "cross-site"}, false, true},
		{"same site, other origin", "POST", list, map[string]string{"Connect-Protocol-Version": "1", "Sec-Fetch-Site": "same-site"}, false, true},
		{"another origin", "POST", list, map[string]string{"Connect-Protocol-Version": "1", "Origin": "https://evil.example"}, false, true},
		{"not Connect", "POST", list, map[string]string{"Sec-Fetch-Site": "same-origin"}, false, true},
		{"REST", "GET", "/v1/workspaces/default/pipelines", map[string]string{"Connect-Protocol-Version": "1", "Sec-Fetch-Site": "same-origin"}, false, true},
		{"sign-in from the UI", "POST", signIn, map[string]string{"Connect-Protocol-Version": "1", "Sec-Fetch-Site": "same-origin"}, true, true},
		{"sign-in cross site", "POST", signIn, map[string]string{"Connect-Protocol-Version": "1", "Sec-Fetch-Site": "cross-site"}, false, false},
		{"sign-in not over Connect", "POST", signIn, map[string]string{"Sec-Fetch-Site": "same-origin"}, false, false},
	}
	for _, c := range cases {
		got = nil
		r := httptest.NewRequest(c.method, "http://localhost:8080"+c.path, nil)
		r.AddCookie(&http.Cookie{Name: "__Host-openetl_session", Value: "oetl_s_x"})
		for k, v := range c.header {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		switch {
		case c.reached != (got != nil):
			t.Errorf("%s: reached the API %v, status %d", c.name, got != nil, w.Code)
		case got != nil && c.cookie != (sessionCookie(got.Header, "__Host-openetl_session") != ""):
			t.Errorf("%s: cookie kept %v", c.name, !c.cookie)
		case !c.reached && w.Code != http.StatusForbidden:
			t.Errorf("%s: status %d", c.name, w.Code)
		}
	}
}

func TestCookie(t *testing.T) {
	for url, want := range map[string]string{
		"https://etl.example.com": "__Host-openetl_session=v; Path=/; HttpOnly; Secure; SameSite=Strict",
		"http://localhost:8080":   "openetl_session=v; Path=/; HttpOnly; SameSite=Strict",
		"":                        "openetl_session=v; Path=/; HttpOnly; SameSite=Strict",
	} {
		if got := (&Server{PublicURL: url}).cookie("v").String(); got != want {
			t.Errorf("%q: %s, want %s", url, got, want)
		}
	}
}

func TestSignInRate(t *testing.T) {
	var l signIns
	for i := range 3 {
		if err := l.allow("10.0.0.1:5000", 3); err != nil {
			t.Fatalf("sign-in %d: %v", i, err)
		}
	}
	if err := l.allow("10.0.0.1:5001", 3); connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("the fourth from one address: %v", err)
	}
	if err := l.allow("10.0.0.2:5000", 3); err != nil {
		t.Fatalf("another address: %v", err)
	}
}

type noDB struct{}

var errNoDB = errors.New("no database in this test")

func (noDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errNoDB
}
func (noDB) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, errNoDB }
func (noDB) QueryRow(context.Context, string, ...any) pgx.Row        { return noRow{} }

type noRow struct{}

func (noRow) Scan(...any) error { return errNoDB }

// With sign-in off, what a browser sends for a web page must not act as the
// owner, through the handler the server really serves.
func TestAuthlessRefusesWebPages(t *testing.T) {
	const apply = "/openetl.api.v1.ApplyService/Apply"
	connectJSON := map[string]string{"Content-Type": "application/json", "Connect-Protocol-Version": "1"}
	withHeaders := func(a map[string]string, b map[string]string) map[string]string {
		out := map[string]string{}
		for _, m := range []map[string]string{a, b} {
			for k, v := range m {
				out[k] = v
			}
		}
		return out
	}
	crossSite := map[string]string{"Origin": "https://evil.example", "Sec-Fetch-Site": "cross-site"}
	rebound := map[string]string{"Origin": "http://rebind.evil.example:8080", "Sec-Fetch-Site": "same-origin"}
	cases := []struct {
		name, method, url, body string
		header                  map[string]string
	}{
		{"cross-site REST POST with no content type", "POST", "http://localhost:8080/v1/workspaces/default:apply", `{"documents":[]}`, crossSite},
		{"cross-site Connect POST", "POST", "http://localhost:8080" + apply, `{}`, withHeaders(connectJSON, crossSite)},
		{"rebound hostname, Connect POST", "POST", "http://rebind.evil.example:8080" + apply, `{}`, withHeaders(connectJSON, rebound)},
		{"rebound hostname, REST read", "GET", "http://rebind.evil.example:8080/v1/workspaces/default/pipelines", "", rebound},
	}
	s := &Server{Store: &store.Store{Queries: db.New(noDB{})}, Authless: true, PublicURL: "http://localhost:8080"}
	h, err := s.Handler()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		r := httptest.NewRequest(c.method, c.url, strings.NewReader(c.body))
		for k, v := range c.header {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusForbidden {
			t.Errorf("%s: status %d, want 403; body %.200s", c.name, w.Code, w.Body.String())
		}
	}
}

func TestGuardAuthless(t *testing.T) {
	const bearer = "Bearer oetl_k_x"
	browser := func(site string) map[string]string { return map[string]string{"Sec-Fetch-Site": site} }
	cases := []struct {
		name                string
		authless, remote    bool
		method, url, public string
		header              map[string]string
		reached             bool
	}{
		{"the CLI or curl", true, false, "POST", "http://localhost:8080/x", "", nil, true},
		{"a read from curl", true, false, "GET", "http://localhost:8080/x", "", nil, true},
		{"the console", true, false, "POST", "http://localhost:8080/x", "", browser("same-origin"), true},
		{"the console's dev server, proxied", true, false, "POST", "http://localhost:5173/x", "", browser("same-origin"), true},
		{"a typed URL", true, false, "GET", "http://localhost:8080/x", "", browser("none"), true},
		{"IPv4 loopback", true, false, "POST", "http://127.0.0.1:8080/x", "", browser("same-origin"), true},
		{"IPv6 loopback", true, false, "POST", "http://[::1]:8080/x", "", browser("same-origin"), true},
		{"the public URL's host", true, false, "POST", "http://etl.internal:8080/x", "http://etl.internal:8080", browser("same-origin"), true},
		{"the public URL from another port", true, false, "POST", "http://localhost:8080/x", "http://localhost:5173", map[string]string{"Origin": "http://localhost:5173"}, true},
		{"another site", true, false, "POST", "http://localhost:8080/x", "", browser("cross-site"), false},
		{"a sibling site", true, false, "POST", "http://localhost:8080/x", "", browser("same-site"), false},
		{"another origin, no fetch metadata", true, false, "POST", "http://localhost:8080/x", "", map[string]string{"Origin": "https://evil.example"}, false},
		{"another site, reading", true, false, "GET", "http://localhost:8080/x", "", browser("cross-site"), true},
		{"a rebound hostname", true, false, "POST", "http://rebind.evil.example:8080/x", "", browser("same-origin"), false},
		{"a rebound hostname, reading", true, false, "GET", "http://rebind.evil.example:8080/x", "", browser("same-origin"), false},
		{"the wildcard address", true, false, "POST", "http://0.0.0.0:8080/x", "", browser("same-origin"), false},
		{"a key from any host", true, false, "POST", "http://rebind.evil.example:8080/x", "", map[string]string{"Authorization": bearer}, true},
		{"a basic credential isn't a key", true, false, "POST", "http://rebind.evil.example:8080/x", "", map[string]string{"Authorization": "Basic eA=="}, false},
		{"remote, any host", true, true, "POST", "http://etl.internal:8080/x", "", browser("same-origin"), true},
		{"remote, another site", true, true, "POST", "http://etl.internal:8080/x", "", browser("cross-site"), false},
		{"with sign-in, the normal checks decide", false, false, "POST", "http://rebind.evil.example:8080/x", "", browser("cross-site"), true},
	}
	for _, c := range cases {
		reached := false
		s := &Server{Authless: c.authless, AuthlessRemote: c.remote, PublicURL: c.public}
		h, err := s.guardAuthless(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached = true }))
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest(c.method, c.url, nil)
		for k, v := range c.header {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if reached != c.reached {
			t.Errorf("%s: reached %v, want %v; status %d, body %s", c.name, reached, c.reached, w.Code, w.Body.String())
		}
		if !c.reached && w.Code != http.StatusForbidden {
			t.Errorf("%s: status %d, want 403", c.name, w.Code)
		}
	}
}
