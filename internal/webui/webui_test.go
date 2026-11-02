package webui

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
