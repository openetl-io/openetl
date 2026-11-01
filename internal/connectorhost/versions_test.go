package connectorhost

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ggcr "github.com/google/go-containerregistry/pkg/registry"

	"github.com/unsuitable001/openetl/internal/netguard"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/registry"
	"github.com/unsuitable001/openetl/internal/spec"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

func testDescriptor(image string) *spec.Descriptor {
	return &spec.Descriptor{Image: image, Spec: &protocolv1.SpecResponse{
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
	b, err := spec.EncodeDescriptor("pg", "1.0.0", testDescriptor("ghcr.io/x/source-pg@sha256:"+strings.Repeat("0", 64)))
	if err != nil {
		t.Fatal(err)
	}
	d, err := ParseVersion(b)
	if err != nil {
		t.Fatal(err)
	}
	if d.Kind != "source" || d.Name != "pg" || d.Descriptor == nil {
		t.Fatalf("descriptor version = %+v", d)
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

	builtin, _ := spec.EncodeDescriptor("faker", "1.0.0", testDescriptor("ghcr.io/x/y@sha256:"+strings.Repeat("0", 64)))
	if _, err := ParseVersion(builtin); err == nil {
		t.Error("a version of a built-in connector parsed")
	}
	if _, err := ParseVersion([]byte("apiVersion: openetl.dev/v1alpha1\nkind: Source\nmetadata: {name: s}\nspec: {connector: faker}\n")); err == nil {
		t.Error("a Source parsed as a connector version")
	}
}

func TestFetcher(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(ggcr.New(ggcr.Logger(log.New(io.Discard, "", 0))))
	defer srv.Close()
	reg := &registry.Client{Policy: netguard.Policy{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}}}
	tmp := t.TempDir()
	exe := filepath.Join(tmp, "source-pg")
	if err := os.WriteFile(exe, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	image, err := reg.Push(ctx, strings.TrimPrefix(srv.URL, "http://")+"/source-pg:1.0.0", []registry.Executable{{Platform: registry.Platform(), Path: exe}})
	if err != nil {
		t.Fatal(err)
	}
	content, err := spec.EncodeDescriptor("pg", "1.0.0", testDescriptor(image))
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
		Registry: reg,
		Cache:    filepath.Join(tmp, "cache"),
	}
	v, err := f.Fetch(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(v.Executable) != "source-pg" || !strings.HasPrefix(v.Executable, f.Cache) {
		t.Errorf("executable %s", v.Executable)
	}
	if _, err := f.Fetch(ctx, digest); err != nil || fetches != 1 {
		t.Errorf("a second fetch: %v, %d fetches from the API", err, fetches)
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
