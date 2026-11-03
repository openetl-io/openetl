// Package webui serves the built UI next to the API, on one origin.
package webui

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path"
	"regexp"
	"strings"
)

// noncePlaceholder is where the build (web/vite.config.ts) leaves room for
// each index.html response's CSP nonce.
var noncePlaceholder = []byte("OPENETL_CSP_NONCE")

// The build's csp.json: hashes of stylesheets inserted without a nonce.
const cspFile = "csp.json"

var styleHash = regexp.MustCompile(`^'sha(256|384|512)-[A-Za-z0-9+/]+={0,2}'$`)

const permissionsPolicy = "accelerometer=(), bluetooth=(), camera=(), display-capture=(), geolocation=(), gyroscope=(), " +
	"hid=(), magnetometer=(), microphone=(), midi=(), payment=(), serial=(), usb=()"

// policy is the UI's Content-Security-Policy (ADR-0020). Connectors' logos
// are linked from their projects' sites, hence img-src https:.
func policy(nonce string, styleHashes []string) string {
	style := []string{"'self'"}
	if nonce != "" {
		style = append(style, "'nonce-"+nonce+"'")
	}
	style = append(style, styleHashes...)
	return strings.Join([]string{
		"default-src 'none'",
		"script-src 'self'",
		"style-src " + strings.Join(style, " "),
		"img-src 'self' data: https:",
		"font-src 'self'",
		"connect-src 'self'",
		"manifest-src 'self'",
		"base-uri 'none'",
		"form-action 'self'",
		"frame-ancestors 'none'",
	}, "; ")
}

// styleHashes reads csp.json, if the build wrote one, keeping only entries
// that are hash sources.
func styleHashes(root fs.FS) []string {
	b, err := fs.ReadFile(root, cspFile)
	if err != nil {
		return nil
	}
	var f struct {
		StyleHashes []string `json:"styleHashes"`
	}
	if json.Unmarshal(b, &f) != nil {
		return nil
	}
	var out []string
	for _, h := range f.StyleHashes {
		if styleHash.MatchString(h) {
			out = append(out, h)
		}
	}
	return out
}

func newNonce() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

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
		h.Set("Permissions-Policy", permissionsPolicy)
		h.Set("Cross-Origin-Opener-Policy", "same-origin")
		name := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if info, err := fs.Stat(root, name); err == nil && !info.IsDir() && name != "index.html" {
			h.Set("Content-Security-Policy", policy("", nil))
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
		nonce := newNonce()
		h.Set("Content-Security-Policy", policy(nonce, styleHashes(root)))
		h.Set("Cache-Control", "no-cache")
		h.Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(bytes.ReplaceAll(index, noncePlaceholder, []byte(nonce)))
	}), nil
}

func isAPI(p string) bool {
	return strings.HasPrefix(p, "/v1/") || strings.HasPrefix(p, "/openetl.") || p == "/healthz" || p == "/openapi.yaml"
}
