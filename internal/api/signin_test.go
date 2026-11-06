package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"

	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
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
