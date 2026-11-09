package oci

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/opencontainers/go-digest"
)

func testRegistry(t *testing.T) (*Client, Ref) {
	t.Helper()
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	ns, err := Parse(Scheme + strings.TrimPrefix(srv.URL, "http://") + "/acme/connectors")
	if err != nil {
		t.Fatal(err)
	}
	return &Client{HTTP: srv.Client()}, ns
}

func TestParse(t *testing.T) {
	for in, want := range map[string]Ref{
		"oci://ghcr.io/openetl-io/openetl-connectors":                {Registry: "ghcr.io", Repository: "openetl-io/openetl-connectors"},
		"oci://ghcr.io/openetl-io/openetl-connectors/postgres:0.2.0": {Registry: "ghcr.io", Repository: "openetl-io/openetl-connectors/postgres", Tag: "0.2.0"},
		"oci://localhost:5000/a/b":                                   {Registry: "localhost:5000", Repository: "a/b"},
		"oci://localhost:5000/a/b@sha256:" + strings.Repeat("a", 64): {Registry: "localhost:5000", Repository: "a/b", Digest: "sha256:" + strings.Repeat("a", 64)},
	} {
		got, err := Parse(in)
		if err != nil || got != want {
			t.Errorf("Parse(%s) = %+v, %v; want %+v", in, got, err, want)
		}
		if got.String() != in {
			t.Errorf("%+v.String() = %s, not %s", got, got.String(), in)
		}
	}
	for _, bad := range []string{"https://ghcr.io/a", "oci://ghcr.io", "oci://ghcr.io/A/b", "oci://ghcr.io/a@sha256:x", "oci://ghcr.io/a@md5:" + strings.Repeat("a", 32), "oci://ghcr.io/a:b+c"} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("Parse(%s) took it", bad)
		}
	}
	ns, _ := Parse("oci://ghcr.io/acme/connectors")
	for repo, want := range map[string]bool{
		"oci://ghcr.io/acme/connectors":          true,
		"oci://ghcr.io/acme/connectors/postgres": true,
		"oci://ghcr.io/acme/connectorsx":         false,
		"oci://ghcr.io/acme":                     false,
		"oci://docker.io/acme/connectors/x":      false,
	} {
		r, _ := Parse(repo)
		if ns.Covers(r) != want {
			t.Errorf("%s covers %s: %v", ns, repo, !want)
		}
	}
}

func TestNewer(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want bool
	}{
		{"1.0.0", "", true},
		{"1.0.0-rc.1", "", true},
		{"1.0.0", "0.9.0", true},
		{"0.9.0", "1.0.0", false},
		{"1.0.0", "1.0.0", true},
		{"2.0.0-rc.1", "1.0.0", false},
		{"1.0.0", "2.0.0-rc.1", true},
		{"2.0.0-rc.2", "2.0.0-rc.1", true},
	} {
		if got := Newer(c.a, c.b); got != c.want {
			t.Errorf("Newer(%s, %s) = %v", c.a, c.b, got)
		}
	}
}

func TestPushPullAndIndex(t *testing.T) {
	c, ns := testRegistry(t)
	testPushPullAndIndex(t, c, ns)
}

// testPushPullAndIndex pushes, pulls and indexes packages in ns, which is
// empty.
func testPushPullAndIndex(t *testing.T, c *Client, ns Ref) {
	ctx := context.Background()
	repo := ns.Child("postgres")
	bin := []byte("#!/bin/sh\necho connector\n")
	logo := []byte(`<svg xmlns="http://www.w3.org/2000/svg"/>`)
	desc := []byte(`{"apiVersion":"openetl.io/v1alpha2","kind":"Connector","metadata":{"name":"acme/postgres","version":"1.0.0"}}`)
	v := Version{
		Name: "acme/postgres", Version: "1.0.0", Descriptor: desc,
		Binaries:    []Blob{{MediaType: MediaTypeBinary, Data: bin, Annotations: map[string]string{AnnotationRole: "source", AnnotationPlatform: "linux/amd64"}}},
		Logo:        &Blob{MediaType: "image/svg+xml", Data: logo, Annotations: map[string]string{AnnotationLogo: ""}},
		Annotations: map[string]string{AnnotationName: "acme/postgres"},
	}
	m, err := c.Push(ctx, repo, v)
	if err != nil {
		t.Fatal(err)
	}
	again, err := c.Push(ctx, repo, v)
	if err != nil || again.Digest != m.Digest {
		t.Errorf("pushing the same version again: %v, %v", again.Digest, err)
	}
	other := v
	other.Descriptor = append(bytes.Clone(desc), ' ')
	if _, err := c.Push(ctx, repo, other); !errors.Is(err, ErrVersionExists) {
		t.Errorf("pushing the version with another descriptor: %v", err)
	}
	if _, err := c.Push(ctx, repo, Version{Version: "1.0.0+build", Descriptor: desc}); err == nil {
		t.Error("a version with build metadata was pushed")
	}

	p, err := c.Pull(ctx, repo.At("1.0.0"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(p.Descriptor, desc) || p.Manifest.Descriptor.Digest != m.Digest {
		t.Errorf("pulled %s, %s", p.Descriptor, p.Manifest.Descriptor.Digest)
	}
	if _, err := c.Pull(ctx, repo.At(m.Digest.String())); err != nil {
		t.Errorf("pull by digest: %v", err)
	}

	var got bytes.Buffer
	if err := c.FetchBlob(ctx, repo.At(digest.FromBytes(bin).String()), &got, 1<<20); err != nil || !bytes.Equal(got.Bytes(), bin) {
		t.Errorf("fetched %q, %v", got.Bytes(), err)
	}
	if err := c.FetchBlob(ctx, repo.At(digest.FromBytes(bin).String()), io.Discard, 3); err == nil {
		t.Error("a blob over the limit was fetched")
	}
	if err := c.FetchBlob(ctx, repo.At(digest.FromBytes([]byte("missing")).String()), io.Discard, 1<<20); !NotFound(err) {
		t.Errorf("a missing blob: %v", err)
	}

	card := Card{Name: "acme/postgres", Title: "PostgreSQL", Version: "1.0.0", Roles: []string{"destination", "source"}, Tags: []string{"database"}, Repository: repo.String()}
	moved, err := c.UpdateIndex(ctx, ns, card, &Blob{MediaType: "image/svg+xml", Data: logo})
	if err != nil || !moved {
		t.Fatalf("UpdateIndex: %v, %v", moved, err)
	}
	cards, err := c.ListIndex(ctx, ns, nil)
	if err != nil || len(cards) != 1 {
		t.Fatalf("ListIndex: %v, %v", cards, err)
	}
	got1 := cards[0]
	if got1.Name != "acme/postgres" || got1.Title != "PostgreSQL" || got1.Version != "1.0.0" || strings.Join(got1.Roles, ",") != "destination,source" || got1.Logo == nil || got1.Repository != repo.String() {
		t.Errorf("card %+v", got1)
	}
	if b, err := c.ReadBlob(ctx, Index(ns), *got1.Logo, MaxLogo); err != nil || !bytes.Equal(b, logo) {
		t.Errorf("logo %q, %v", b, err)
	}

	older := card
	older.Version = "0.9.0"
	if moved, err := c.UpdateIndex(ctx, ns, older, nil); err != nil || moved {
		t.Errorf("an older version moved the index: %v, %v", moved, err)
	}
	pre := card
	pre.Version = "2.0.0-rc.1"
	if moved, err := c.UpdateIndex(ctx, ns, pre, nil); err != nil || moved {
		t.Errorf("a prerelease moved the index past a release: %v, %v", moved, err)
	}
	taken := card
	taken.Name = "other/postgres"
	if _, err := c.UpdateIndex(ctx, ns, taken, nil); !errors.Is(err, ErrIndexTaken) {
		t.Errorf("another owner's package: %v", err)
	}
	newer := card
	newer.Version = "1.1.0"
	if moved, err := c.UpdateIndex(ctx, ns, newer, nil); err != nil || !moved {
		t.Errorf("a newer version: %v, %v", moved, err)
	}
	cards, err = c.ListIndex(ctx, ns, map[string]Card{"postgres": got1})
	if err != nil || len(cards) != 1 || cards[0].Version != "1.1.0" || cards[0].Logo != nil {
		t.Errorf("after 1.1.0: %+v, %v", cards, err)
	}
	same, err := c.ListIndex(ctx, ns, map[string]Card{"postgres": {Name: "cached", Digest: cards[0].Digest}})
	if err != nil || len(same) != 1 || same[0].Name != "cached" {
		t.Errorf("a known card was fetched again: %+v, %v", same, err)
	}

	// A package pushed without an index entry shows up where the registry
	// lists repositories.
	kafka := v
	kafka.Name, kafka.Annotations = "acme/kafka", map[string]string{AnnotationName: "acme/kafka", "org.opencontainers.image.title": "Kafka"}
	if _, err := c.Push(ctx, ns.Child("kafka"), kafka); err != nil {
		t.Fatal(err)
	}
	un := c.ListUnindexed(ctx, ns, map[string]bool{"postgres": true}, nil)
	if len(un) != 1 || un[0].Name != "acme/kafka" || un[0].Title != "Kafka" || un[0].Version != "1.0.0" || un[0].Repository != ns.Child("kafka").String() {
		t.Errorf("unindexed: %+v", un)
	}
	if cards, err := c.ListIndex(ctx, ns.Child("nothing"), nil); err != nil || len(cards) != 0 {
		t.Errorf("a namespace without an index: %v, %v", cards, err)
	}
}

// A registry that lists everything it hosts, sorted, is read from ns to just
// past it, and one that ignores where to start is still read only that far.
func TestListUnindexedReadsOnlyTheNamespace(t *testing.T) {
	everything := []string{"aaa/first", "acme/connectors-other/x", "acme/connectors/index", "acme/connectors/kafka", "acme/connectors/nested/x", "acme/zzz", "zzz/last"}
	for _, honoursLast := range []bool{true, false} {
		inner := registry.New(registry.Logger(log.New(io.Discard, "", 0)))
		var requests []string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v2/_catalog" {
				inner.ServeHTTP(w, r)
				return
			}
			last := r.URL.Query().Get("last")
			requests = append(requests, last)
			names := everything
			if honoursLast || strings.HasPrefix(last, "page:") {
				start := strings.TrimPrefix(last, "page:")
				names = names[sort.SearchStrings(names, start):]
				if start != "" && len(names) > 0 && names[0] == start {
					names = names[1:]
				}
			}
			page := names[:min(2, len(names))]
			if len(names) > len(page) {
				w.Header().Set("Link", fmt.Sprintf("</v2/_catalog?last=page:%s>; rel=\"next\"", page[len(page)-1]))
			}
			_ = json.NewEncoder(w).Encode(map[string][]string{"repositories": page})
		}))
		defer srv.Close()
		ns, _ := Parse(Scheme + strings.TrimPrefix(srv.URL, "http://") + "/acme/connectors")
		c := &Client{HTTP: srv.Client()}
		v := Version{Name: "acme/kafka", Version: "1.0.0", Descriptor: []byte("{}"), Annotations: map[string]string{AnnotationName: "acme/kafka"}}
		if _, err := c.Push(t.Context(), ns.Child("kafka"), v); err != nil {
			t.Fatal(err)
		}
		un := c.ListUnindexed(t.Context(), ns, nil, nil)
		if len(un) != 1 || un[0].Name != "acme/kafka" {
			t.Errorf("honoursLast=%v: unindexed: %+v", honoursLast, un)
		}
		if requests[0] != "acme/connectors/" {
			t.Errorf("honoursLast=%v: the first listing started at %q", honoursLast, requests[0])
		}
		for _, last := range requests {
			if strings.HasPrefix(last, "page:acme/zzz") || strings.HasPrefix(last, "page:zzz") {
				t.Errorf("honoursLast=%v: read on past the namespace: %q", honoursLast, requests)
			}
		}
	}
}

func TestCredentials(t *testing.T) {
	inner := registry.New(registry.Logger(log.New(io.Discard, "", 0)))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if u, p, ok := r.BasicAuth(); !ok || u != "bot" || p != "token" {
			w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		inner.ServeHTTP(w, r)
	}))
	defer srv.Close()
	ns, _ := Parse(Scheme + strings.TrimPrefix(srv.URL, "http://") + "/acme/connectors")
	var asked []string
	c := &Client{HTTP: srv.Client(), Credentials: func(_ context.Context, repo Ref) (Credential, error) {
		asked = append(asked, repo.String())
		return Credential{Username: "bot", Password: "token"}, nil
	}}
	if _, err := c.Push(context.Background(), ns.Child("postgres"), Version{Version: "1.0.0", Descriptor: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	if len(asked) == 0 || asked[0] != ns.Child("postgres").String() {
		t.Errorf("credentials asked for %v", asked)
	}
	anon := &Client{HTTP: srv.Client()}
	if _, err := anon.Pull(context.Background(), ns.Child("postgres").At("1.0.0")); err == nil {
		t.Error("pulled without credentials")
	}
}

func TestLogoType(t *testing.T) {
	for _, c := range []struct {
		name, content, want string
	}{
		{"logo.svg", `<?xml version="1.0"?><svg xmlns="http://www.w3.org/2000/svg"/>`, "image/svg+xml"},
		{"logo.png", "\x89PNG\r\n\x1a\nrest", "image/png"},
		{"logo.svg", "<html><script>", ""},
		{"logo.gif", "GIF89a", ""},
	} {
		if got := LogoType(c.name, []byte(c.content)); got != c.want {
			t.Errorf("LogoType(%s) = %q", c.name, got)
		}
	}
}

func TestFetchLogo(t *testing.T) {
	const svg = `<svg xmlns="http://www.w3.org/2000/svg"></svg>`
	const png = "\x89PNG\r\n\x1a\nrest"
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/mark":
			_, _ = io.WriteString(w, svg)
		case "/mark.png":
			_, _ = io.WriteString(w, png)
		case "/page":
			_, _ = io.WriteString(w, "<html><script>alert(1)</script></html>")
		case "/large":
			_, _ = io.WriteString(w, svg+strings.Repeat(" ", MaxLogo))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c := &Client{HTTP: srv.Client()}
	for path, want := range map[string]struct{ content, mediaType string }{
		"/mark":     {svg, "image/svg+xml"},
		"/mark.png": {png, "image/png"},
	} {
		b, mediaType, err := c.FetchLogo(t.Context(), srv.URL+path)
		if err != nil || string(b) != want.content || mediaType != want.mediaType {
			t.Errorf("%s: %q, %q, %v", path, b, mediaType, err)
		}
	}
	for _, url := range []string{srv.URL + "/page", srv.URL + "/large", srv.URL + "/missing", "http://example.com/mark.svg"} {
		if _, _, err := c.FetchLogo(t.Context(), url); err == nil {
			t.Errorf("%s was accepted", url)
		}
	}
}
