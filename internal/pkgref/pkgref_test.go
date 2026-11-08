package pkgref

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/openetl-io/openetl/internal/github"
)

const descriptor = `apiVersion: openetl.io/v1alpha2
kind: Connector
metadata:
  name: %s
  version: 1.0.0
  repository: https://github.com/%s
spec:
  roles:
    source:
      requires: {protocol: v1}
      runtime:
        binaries:
          linux/amd64: {url: "https://example.com/x", digest: "sha256:0000000000000000000000000000000000000000000000000000000000000000"}
      egress: {tier: host}
      config: {type: object}
`

func TestResolveGitHub(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		owner, rest, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/repos/"), "/")
		switch {
		case strings.HasPrefix(r.URL.Path, "/assets/"):
			name := strings.TrimPrefix(r.URL.Path, "/assets/")
			owner, _, _ := strings.Cut(name, "/")
			_, _ = fmt.Fprintf(w, descriptor, name, owner+"/conn")
		case rest == "conn/releases/tags/v1.0.0" || rest == "conn/releases/tags/pg/v1.0.0":
			_, _ = fmt.Fprintf(w, `{"tag_name": "v1.0.0", "html_url": "https://github.com/%[1]s/conn/releases/tag/v1.0.0", "assets": [
				{"id": 1, "name": "pg.connector.yaml", "url": "http://%[2]s/assets/%[1]s/pg"},
				{"id": 2, "name": "kafka.connector.yaml", "url": "http://%[2]s/assets/%[1]s/kafka"}]}`, owner, r.Host)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	res := Resolver{GitHub: func(context.Context, string) (*github.Client, error) {
		return &github.Client{HTTP: srv.Client(), API: srv.URL}, nil
	}}
	got, err := res.Resolve(t.Context(), "github.com/acme/conn/pg@1.0.0")
	if err != nil || !strings.Contains(string(got.Content), "name: acme/pg") || got.Provenance != "https://github.com/acme/conn/releases/tag/v1.0.0" {
		t.Errorf("a package of a release: %+v, %v", got, err)
	}
	if _, err := res.Resolve(t.Context(), "github.com/acme/conn@1.0.0"); err == nil || !strings.Contains(err.Error(), "has 2 descriptors") {
		t.Errorf("a release of two packages, none named: %v", err)
	}
	// The openetl owner's packages come from the official repository only.
	if _, err := res.Resolve(t.Context(), "github.com/openetl/conn/pg@1.0.0"); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Errorf("an impostor: %v", err)
	}
	res.Official = []string{"https://github.com/openetl/conn"}
	if _, err := res.Resolve(t.Context(), "github.com/openetl/conn/pg@1.0.0"); err != nil {
		t.Errorf("an official package: %v", err)
	}
	if _, err := res.Resolve(t.Context(), "https://github.com/acme/conn"); err == nil {
		t.Error("a URL that isn't a reference")
	}
}

func TestIs(t *testing.T) {
	for s, want := range map[string]bool{
		"oci://ghcr.io/a/b:1.0.0": true, "github.com/a/b@1.0.0": true, "./github.com/x.yaml": false, "dist/pg.connector.yaml": false,
	} {
		if Is(s) != want {
			t.Errorf("Is(%s) = %v", s, !want)
		}
	}
}
