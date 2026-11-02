// Package webui serves the built UI next to the API, on one origin.
package webui

import (
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path"
	"strings"
)

// Handler serves the UI built into dir, and passes the API's paths to api.
// A GET for anything else that isn't a file gets index.html, so the UI's
// routes load it.
func Handler(dir string, api http.Handler) (http.Handler, error) {
	root := os.DirFS(dir)
	if _, err := fs.Stat(root, "index.html"); err != nil {
		return nil, fmt.Errorf("webui: %s has no index.html; build it with make web: %w", dir, err)
	}
	files := http.FileServerFS(root)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isAPI(r.URL.Path) || (r.Method != http.MethodGet && r.Method != http.MethodHead) {
			api.ServeHTTP(w, r)
			return
		}
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "same-origin")
		name := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if info, err := fs.Stat(root, name); err == nil && !info.IsDir() && name != "index.html" {
			if strings.HasPrefix(name, "assets/") {
				// Vite puts a content hash in these names.
				h.Set("Cache-Control", "public, max-age=31536000, immutable")
			}
			files.ServeHTTP(w, r)
			return
		}
		// Read each time, so a rebuild needs no restart.
		index, err := fs.ReadFile(root, "index.html")
		if err != nil {
			http.Error(w, "the UI isn't built", http.StatusServiceUnavailable)
			return
		}
		h.Set("Cache-Control", "no-cache")
		h.Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(index)
	}), nil
}

func isAPI(p string) bool {
	return strings.HasPrefix(p, "/v1/") || strings.HasPrefix(p, "/openetl.") || p == "/healthz" || p == "/openapi.yaml"
}
