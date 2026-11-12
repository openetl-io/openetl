package pkgref

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/registry"

	"github.com/openetl-io/openetl/internal/github"
	"github.com/openetl-io/openetl/internal/oci"
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

func TestNewest(t *testing.T) {
	for _, c := range []struct {
		name     string
		versions []string
		want     string
	}{
		{"a stable version beats a higher prerelease", []string{"1.0.0", "2.0.0-rc.1", "0.9.0"}, "1.0.0"},
		{"semantic-version order, not the strings'", []string{"1.2.0", "1.10.0", "1.9.3"}, "1.10.0"},
		{"prereleases only", []string{"1.0.0-alpha", "1.0.0-beta.10", "1.0.0-beta.2"}, "1.0.0-beta.10"},
		{"one version", []string{"0.1.0-alpha"}, "0.1.0-alpha"},
		{"tags that aren't versions are left out", []string{"latest", "0.3.0", "edge"}, "0.3.0"},
	} {
		if got, err := Newest(c.versions); err != nil || got != c.want {
			t.Errorf("%s: Newest(%v) = %q, %v; want %q", c.name, c.versions, got, err, c.want)
		}
	}
	for _, none := range [][]string{nil, {"latest", "edge"}} {
		if got, err := Newest(none); !errors.Is(err, ErrNoVersion) {
			t.Errorf("Newest(%v) = %q, %v", none, got, err)
		}
	}
}

func TestUnversioned(t *testing.T) {
	for ref, want := range map[string]string{
		"oci://ghcr.io/acme/connectors/pg": "oci://ghcr.io/acme/connectors/pg:1.2.0",
		"github.com/acme/conn":             "github.com/acme/conn@1.2.0",
		"github.com/acme/conn/pg":          "github.com/acme/conn/pg@1.2.0",
	} {
		if !Unversioned(ref) || WithVersion(ref, "1.2.0") != want {
			t.Errorf("%s: unversioned %v, at 1.2.0 %s", ref, Unversioned(ref), WithVersion(ref, "1.2.0"))
		}
	}
	for _, ref := range []string{"oci://ghcr.io/acme/connectors/pg:1.0.0", "oci://ghcr.io/acme/pg@sha256:" + strings.Repeat("a", 64), "github.com/acme/conn/pg@1.0.0", "dist/pg.connector.yaml"} {
		if Unversioned(ref) {
			t.Errorf("%s is unversioned", ref)
		}
	}
	if l := Location("github.com/acme/conn/pg"); l != "https://github.com/acme/conn" {
		t.Errorf("location %s", l)
	}
	if l := Location("oci://ghcr.io/acme/connectors/pg"); l != "oci://ghcr.io/acme/connectors/pg" {
		t.Errorf("location %s", l)
	}
}

// A reference without a version takes the newest version in the registry,
// when a connected source covers it; one with a version takes that version.
func TestResolveUnversionedOCI(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	defer srv.Close()
	ns := oci.Scheme + strings.TrimPrefix(srv.URL, "http://") + "/acme/connectors"
	c := &oci.Client{HTTP: srv.Client()}
	repo, err := oci.Parse(ns + "/pg")
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"0.9.0", "1.1.0", "1.0.0", "2.0.0-rc.1"} {
		desc := strings.Replace(fmt.Sprintf(descriptor, "acme/pg", "acme/conn"), "version: 1.0.0", "version: "+v, 1)
		if _, err := c.Push(t.Context(), repo, oci.Version{Name: "acme/pg", Version: v, Descriptor: []byte(desc)}); err != nil {
			t.Fatal(err)
		}
	}
	var asked []string
	res := Resolver{OCI: c, Source: func(_ context.Context, location string) (string, error) {
		asked = append(asked, location)
		if Covers(ns, location) {
			return "team", nil
		}
		return "", nil
	}}
	got, err := res.Resolve(t.Context(), ns+"/pg")
	if err != nil || !strings.Contains(string(got.Content), "version: 1.1.0") || !strings.HasPrefix(got.Provenance, ns+"/pg@sha256:") {
		t.Fatalf("unversioned: %+v, %v", got, err)
	}
	if len(asked) != 1 || asked[0] != ns+"/pg" {
		t.Errorf("asked for the sources covering %v", asked)
	}
	asked = nil
	got, err = res.Resolve(t.Context(), ns+"/pg:2.0.0-rc.1")
	if err != nil || !strings.Contains(string(got.Content), "version: 2.0.0-rc.1") || len(asked) != 0 {
		t.Errorf("an explicit version: %+v, %v, asked %v", got, err, asked)
	}
	if _, err := res.Resolve(t.Context(), ns+"/kafka"); err == nil || !strings.Contains(err.Error(), "connector source team offers none of it") {
		t.Errorf("a package without versions: %v", err)
	}
	other := oci.Scheme + strings.TrimPrefix(srv.URL, "http://") + "/elsewhere/pg"
	if _, err := res.Resolve(t.Context(), other); err == nil || !strings.Contains(err.Error(), "no connected source covers it") {
		t.Errorf("a package no source covers: %v", err)
	}
	res.Source = nil
	if _, err := res.Resolve(t.Context(), ns+"/pg"); err == nil || !strings.Contains(err.Error(), "names no version: add :VERSION") {
		t.Errorf("without sources: %v", err)
	}
}

// A GitHub repository's versions of a package are its releases tagged with
// the package's tags that have its descriptor, drafts left out.
func TestVersionsGitHub(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/conn/releases" {
			http.NotFound(w, r)
			return
		}
		_, _ = fmt.Fprint(w, `[
			{"tag_name": "pg/v1.0.0", "assets": [{"name": "pg.connector.yaml"}]},
			{"tag_name": "pg/v1.1.0-rc.1", "assets": [{"name": "pg.connector.yaml"}]},
			{"tag_name": "pg/v2.0.0", "draft": true, "assets": [{"name": "pg.connector.yaml"}]},
			{"tag_name": "pg/v3.0.0", "assets": [{"name": "notes.txt"}]},
			{"tag_name": "kafka/v5.0.0", "assets": [{"name": "kafka.connector.yaml"}]},
			{"tag_name": "v9.0.0", "assets": [{"name": "all.connector.yaml"}]}]`)
	}))
	defer srv.Close()
	res := Resolver{GitHub: func(context.Context, string) (*github.Client, error) {
		return &github.Client{HTTP: srv.Client(), API: srv.URL}, nil
	}}
	for ref, want := range map[string]string{
		"github.com/acme/conn/pg": "1.0.0 1.1.0-rc.1",
		"github.com/acme/conn":    "9.0.0",
	} {
		got, err := res.Versions(t.Context(), ref)
		if err != nil || strings.Join(got, " ") != want {
			t.Errorf("%s: %v, %v; want %s", ref, got, err, want)
		}
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
