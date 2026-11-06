package webui

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestHandler(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "assets"), 0o750); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"index.html": "<html>ui</html>", "assets/app-1a2b.js": "js", "logo.svg": "<svg/>"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	api := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "api") })
	h, err := Handler(dir, api)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		method, path, body, cache string
	}{
		{"GET", "/", "<html>ui</html>", "no-cache"},
		{"GET", "/workspaces/default/pipelines/shop", "<html>ui</html>", "no-cache"},
		{"GET", "/assets/app-1a2b.js", "js", "public, max-age=31536000, immutable"},
		{"GET", "/logo.svg", "<svg/>", ""},
		{"GET", "/v1/workspaces/default/pipelines", "api", ""},
		{"POST", "/openetl.api.v1.PipelineService/ListPipelines", "api", ""},
		{"GET", "/healthz", "api", ""},
		{"GET", "/openapi.yaml", "api", ""},
		{"GET", "/../../etc/passwd", "<html>ui</html>", "no-cache"},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(c.method, c.path, nil))
		if got := rec.Body.String(); got != c.body || rec.Header().Get("Cache-Control") != c.cache {
			t.Errorf("%s %s: %q, cache %q; want %q, %q", c.method, c.path, got, rec.Header().Get("Cache-Control"), c.body, c.cache)
		}
	}
	if _, err := Handler(t.TempDir(), api); err == nil {
		t.Error("a directory without index.html was accepted")
	}
}

func TestPolicy(t *testing.T) {
	dir := t.TempDir()
	hash := "'sha256-47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU='"
	for name, body := range map[string]string{
		"index.html": `<meta property="csp-nonce" nonce="OPENETL_CSP_NONCE"><script nonce="OPENETL_CSP_NONCE"></script>`,
		"csp.json":   `{"styleHashes": ["` + hash + `", "'unsafe-inline'", "'sha256-x'; script-src *"]}`,
		"logo.svg":   "<svg/>",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	h, err := Handler(dir, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	get := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		return rec
	}
	nonce := regexp.MustCompile(`'nonce-([A-Za-z0-9+/=]+)'`)
	seen := map[string]bool{}
	for range 2 {
		rec := get("/workspaces/default/pipelines")
		csp := rec.Header().Get("Content-Security-Policy")
		m := nonce.FindStringSubmatch(csp)
		if m == nil {
			t.Fatalf("no nonce in %q", csp)
		}
		if seen[m[1]] {
			t.Errorf("nonce %s served twice", m[1])
		}
		seen[m[1]] = true
		want := `<meta property="csp-nonce" nonce="` + m[1] + `"><script nonce="` + m[1] + `"></script>`
		if rec.Body.String() != want {
			t.Errorf("body %q; want %q", rec.Body.String(), want)
		}
		for _, part := range []string{"default-src 'none'", "script-src 'self';", "style-src 'self' 'nonce-" + m[1] + "' " + hash + ";", "frame-ancestors 'none'"} {
			if !strings.Contains(csp, part) {
				t.Errorf("CSP %q lacks %q", csp, part)
			}
		}
		if strings.Contains(csp, "unsafe-inline") || strings.Contains(csp, "script-src *") {
			t.Errorf("CSP %q took csp.json's entries that aren't hashes", csp)
		}
	}
	rec := get("/logo.svg")
	if csp := rec.Header().Get("Content-Security-Policy"); !strings.HasPrefix(csp, "default-src 'none'") || strings.Contains(csp, "nonce") {
		t.Errorf("a file's CSP is %q", csp)
	}
	for _, rec := range []*httptest.ResponseRecorder{rec, get("/")} {
		for k, v := range map[string]string{
			"X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY", "Referrer-Policy": "same-origin",
			"Cross-Origin-Opener-Policy": "same-origin",
		} {
			if got := rec.Header().Get(k); got != v {
				t.Errorf("%s: %q; want %q", k, got, v)
			}
		}
		if p := rec.Header().Get("Permissions-Policy"); !strings.Contains(p, "camera=()") || !strings.Contains(p, "microphone=()") {
			t.Errorf("Permissions-Policy %q", p)
		}
	}
}
