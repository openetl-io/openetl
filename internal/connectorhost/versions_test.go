package connectorhost

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/unsuitable001/openetl/internal/netguard"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/spec"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

func testDescriptor(bins map[string]spec.Binary) *spec.Descriptor {
	return &spec.Descriptor{Binaries: bins, Spec: &protocolv1.SpecResponse{
		ProtocolVersion: "v1",
		Kind:            protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE,
		Egress:          &protocolv1.Egress{Tier: protocolv1.EgressTier_EGRESS_TIER_RAW},
		ConfigSchema:    []byte(`{"type": "object", "required": ["dsn"], "properties": {"dsn": {"type": "string", "x-secret": true}}}`),
		Source:          &protocolv1.SourceCapabilities{},
	}}
}

func TestParseVersion(t *testing.T) {
	m, err := ParseVersion([]byte(acme))
	if err != nil {
		t.Fatal(err)
	}
	if m.Kind != "source" || m.Name != "acme" || m.Version != "0.1.0" || m.Manifest == nil || m.Digest != Digest([]byte(acme)) {
		t.Fatalf("manifest version = %+v", m)
	}
	pinned := map[string]spec.Binary{"linux/amd64": {URL: "https://example.com/source-pg", Digest: "sha256:" + strings.Repeat("0", 64)}}
	info := spec.Info{Author: "Example", Logo: "https://example.com/pg.svg", ReleaseTag: "source-pg/v1.0.0"}
	b, err := spec.EncodeDescriptor("pg", "1.0.0", info, testDescriptor(pinned))
	if err != nil {
		t.Fatal(err)
	}
	d, err := ParseVersion(b)
	if err != nil {
		t.Fatal(err)
	}
	if d.Kind != "source" || d.Name != "pg" || d.Descriptor == nil || d.Info != info {
		t.Fatalf("descriptor version = %+v", d)
	}
	unpinned, _ := spec.EncodeDescriptor("pg", "1.0.0", spec.Info{}, testDescriptor(map[string]spec.Binary{"linux/amd64": {URL: "https://example.com/source-pg"}}))
	if _, err := ParseVersion(unpinned); err == nil || !strings.Contains(err.Error(), "no digest") {
		t.Errorf("a descriptor with a binary unpinned: %v", err)
	}

	env := Env{}.With(m).With(d)
	if _, err := NewSource("pg", []byte(`{"dsn": "postgres://x"}`), env); err != nil {
		t.Errorf("a valid config: %v", err)
	}
	if _, err := NewSource("pg", []byte(`{}`), env); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("a config without its dsn: %v", err)
	}
	if _, err := NewSource("acme", []byte(`{"base_url": "https://x", "api_key": "k"}`), env); err != nil {
		t.Errorf("the manifest: %v", err)
	}
	if _, err := NewDestination("pg", nil, env); err == nil || !strings.Contains(err.Error(), "unknown destination") {
		t.Errorf("a source descriptor as a destination: %v", err)
	}
	if _, err := NewSource("other", nil, env); err == nil || !strings.Contains(err.Error(), "acme, faker, pg") {
		t.Errorf("unknown source: %v", err)
	}

	builtin, _ := spec.EncodeDescriptor("faker", "1.0.0", spec.Info{}, testDescriptor(pinned))
	if _, err := ParseVersion(builtin); err == nil {
		t.Error("a version of a built-in connector parsed")
	}
	if _, err := ParseVersion([]byte("apiVersion: openetl.io/v1alpha1\nkind: Source\nmetadata: {name: s}\nspec: {connector: faker}\n")); err == nil {
		t.Error("a Source parsed as a connector version")
	}
}

func TestFetcher(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	body := []byte("#!/bin/sh\n")
	var downloads atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloads.Add(1)
		if r.URL.Path != "/source-pg" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	client := (&netguard.Policy{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}}).Client()

	d := testDescriptor(map[string]spec.Binary{Platform(): {URL: srv.URL + "/source-pg"}, "plan9/386": {URL: srv.URL + "/missing"}})
	if err := PinBinaries(ctx, client, d); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("pinning a binary that isn't there: %v", err)
	}
	delete(d.Binaries, "plan9/386")
	if err := PinBinaries(ctx, client, d); err != nil {
		t.Fatal(err)
	}
	if got := d.Binaries[Platform()].Digest; got != Digest(body) {
		t.Fatalf("pinned digest %s, want %s", got, Digest(body))
	}
	content, err := spec.EncodeDescriptor("pg", "1.0.0", spec.Info{}, d)
	if err != nil {
		t.Fatal(err)
	}
	digest := Digest(content)
	fetches := 0
	f := &Fetcher{
		Artifact: func(_ context.Context, d string) ([]byte, error) {
			fetches++
			if d != digest {
				return nil, errors.New("no such artifact")
			}
			return content, nil
		},
		Client: client,
		Cache:  filepath.Join(tmp, "cache"),
	}
	downloads.Store(0)
	v, err := f.Fetch(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(v.Executable) != "source-pg" || !strings.HasPrefix(v.Executable, f.Cache) {
		t.Errorf("executable %s", v.Executable)
	}
	if fi, err := os.Stat(v.Executable); err != nil || fi.Mode().Perm()&0o100 == 0 {
		t.Errorf("the executable: %v, %v", fi, err)
	}
	if _, err := f.Fetch(ctx, digest); err != nil || fetches != 1 || downloads.Load() != 1 {
		t.Errorf("a second fetch: %v, %d fetches from the API, %d downloads", err, fetches, downloads.Load())
	}

	// A binary that isn't what its digest says fails, and isn't cached.
	body = []byte("something else")
	wrong := testDescriptor(map[string]spec.Binary{Platform(): {URL: srv.URL + "/source-pg", Digest: "sha256:" + strings.Repeat("1", 64)}})
	otherContent, _ := spec.EncodeDescriptor("pg", "1.0.1", spec.Info{}, wrong)
	f.Artifact = func(context.Context, string) ([]byte, error) { return otherContent, nil }
	if _, err := f.Fetch(ctx, Digest(otherContent)); errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "has digest") {
		t.Errorf("a binary with another digest: %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.Cache, "executables", "sha256-"+strings.Repeat("1", 64))); !os.IsNotExist(err) {
		t.Errorf("a binary with another digest was cached: %v", err)
	}
	elsewhere := testDescriptor(map[string]spec.Binary{"plan9/386": {URL: srv.URL + "/source-pg", Digest: Digest(body)}})
	elsewhereContent, _ := spec.EncodeDescriptor("pg", "1.0.2", spec.Info{}, elsewhere)
	f.Artifact = func(context.Context, string) ([]byte, error) { return elsewhereContent, nil }
	if _, err := f.Fetch(ctx, Digest(elsewhereContent)); errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "no binary for") {
		t.Errorf("no binary for this platform: %v", err)
	}
	f.Artifact = func(context.Context, string) ([]byte, error) {
		fetches++
		return content, nil
	}

	// A cached artifact that doesn't match its digest is fetched again.
	cached := filepath.Join(f.Cache, "artifacts", "sha256-"+strings.TrimPrefix(digest, "sha256:"))
	if err := os.WriteFile(cached, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Fetch(ctx, digest); err != nil || fetches != 2 {
		t.Errorf("after the cache was damaged: %v, %d fetches", err, fetches)
	}

	f.Artifact = func(context.Context, string) ([]byte, error) { return []byte("other"), nil }
	other := Digest([]byte("something else"))
	if _, err := f.Fetch(ctx, other); err == nil || !strings.Contains(err.Error(), "another digest") {
		t.Errorf("an artifact with another digest: %v", err)
	}
	if _, err := f.Fetch(ctx, "sha256:../../x"); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("a digest that isn't one: %v", err)
	}
}
