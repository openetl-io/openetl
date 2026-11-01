package connectorhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"

	"github.com/unsuitable001/openetl/internal/connectorhost/external"
	"github.com/unsuitable001/openetl/internal/declarative"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/registry"
	"github.com/unsuitable001/openetl/internal/spec"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

// Media types of connector versions' artifacts.
const (
	MediaTypeDescriptor = "application/vnd.openetl.connector.v1+json"
	MediaTypeManifest   = "application/vnd.openetl.manifest.v1+json"
)

// Version is a connector version: an external connector's descriptor, or a
// declarative source's manifest.
type Version struct {
	// "source" or "destination".
	Kind    string
	Name    string
	Version string
	// Of the artifact: sha256:{hex}.
	Digest string
	// One of these.
	Manifest   *declarative.Connector
	Descriptor *spec.Descriptor
	// The descriptor's executable, once a runner pulls it.
	Executable string
}

// External is an external connector with a descriptor.
type External struct {
	Spec *protocolv1.SpecResponse
	// Empty where configs are only validated, as on the control plane.
	Executable string
}

// Digest is an artifact's digest.
func Digest(content []byte) string {
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// ParseVersion parses a connector version's artifact: a Connector or a
// DeclarativeSource document. It does no I/O, and doesn't pin the image.
func ParseVersion(content []byte) (*Version, error) {
	doc, err := spec.Parse(content)
	if err != nil {
		return nil, err
	}
	v := &Version{Name: doc.Name, Version: doc.Version, Digest: Digest(content)}
	switch doc.Kind {
	case spec.KindDeclarativeSource:
		if v.Manifest, err = declarative.Load(doc.Manifest); err != nil {
			return nil, err
		}
		v.Kind = "source"
	case spec.KindConnector:
		s := doc.Descriptor.Spec
		if _, err := external.CompileSchema(s.GetConfigSchema()); err != nil {
			return nil, fmt.Errorf("config: %w", err)
		}
		v.Descriptor = doc.Descriptor
		v.Kind = "source"
		if s.GetKind() == protocolv1.ConnectorKind_CONNECTOR_KIND_DESTINATION {
			v.Kind = "destination"
		}
	default:
		return nil, fmt.Errorf("a %s isn't a connector version", doc.Kind)
	}
	if Builtin(v.Kind, v.Name) {
		return nil, fmt.Errorf("%s is a built-in %s connector", v.Name, v.Kind)
	}
	return v, nil
}

// With returns env with v added, for NewSource or NewDestination.
func (e Env) With(v *Version) Env {
	if v.Manifest != nil {
		e.Manifests = maps.Clone(e.Manifests)
		if e.Manifests == nil {
			e.Manifests = map[string]*declarative.Connector{}
		}
		e.Manifests[v.Name] = v.Manifest
		return e
	}
	e.Externals = maps.Clone(e.Externals)
	if e.Externals == nil {
		e.Externals = map[string]External{}
	}
	e.Externals[v.Kind+"/"+v.Name] = External{Spec: v.Descriptor.Spec, Executable: v.Executable}
	return e
}

// Fetcher gets connector versions by digest on a runner, through a cache:
// artifacts in {Cache}/artifacts, and descriptors' executables in
// {Cache}/executables.
type Fetcher struct {
	// An artifact's content, from the control plane.
	Artifact func(ctx context.Context, digest string) ([]byte, error)
	Registry *registry.Client
	Cache    string
}

// Fetch returns the connector version at digest, with its executable pulled
// for this platform.
func (f *Fetcher) Fetch(ctx context.Context, digest string) (*Version, error) {
	content, err := f.artifact(ctx, digest)
	if err != nil {
		return nil, err
	}
	v, err := ParseVersion(content)
	if err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "connector version "+digest)
	}
	if v.Descriptor != nil {
		v.Executable, err = f.Registry.Pull(ctx, v.Descriptor.Image, registry.Platform(), filepath.Join(f.Cache, "executables"))
		var np *registry.NoPlatformError
		switch {
		case errors.As(err, &np):
			return nil, errs.Wrap(errs.ConfigInvalid, err, fmt.Sprintf("%s connector %s %s", v.Kind, v.Name, v.Version))
		case err != nil:
			return nil, errs.Wrap(errs.Transient, err, fmt.Sprintf("%s connector %s %s", v.Kind, v.Name, v.Version))
		}
	}
	return v, nil
}

func (f *Fetcher) artifact(ctx context.Context, digest string) ([]byte, error) {
	hexDigest, ok := strings.CutPrefix(digest, "sha256:")
	if _, err := hex.DecodeString(hexDigest); !ok || err != nil || len(hexDigest) != sha256.Size*2 {
		return nil, errs.New(errs.ConfigInvalid, "artifact digest %q isn't sha256:{hex}", digest)
	}
	dir := filepath.Join(f.Cache, "artifacts")
	path := filepath.Join(dir, "sha256-"+hexDigest)
	if b, err := os.ReadFile(path); err == nil && Digest(b) == digest { //nolint:gosec // under the cache
		return b, nil
	}
	b, err := f.Artifact(ctx, digest)
	if err != nil {
		return nil, err
	}
	if Digest(b) != digest {
		return nil, errs.New(errs.Internal, "artifact %s has another digest, %s", digest, Digest(b))
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, errs.Wrap(errs.Internal, err, "connector cache")
	}
	tmp, err := os.CreateTemp(dir, ".artifact-")
	if err != nil {
		return nil, errs.Wrap(errs.Internal, err, "connector cache")
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return nil, errs.Wrap(errs.Internal, err, "connector cache")
	}
	if err := tmp.Close(); err != nil {
		return nil, errs.Wrap(errs.Internal, err, "connector cache")
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return nil, errs.Wrap(errs.Internal, err, "connector cache")
	}
	return b, nil
}

// Package pushes an external connector's executables to image, a tag, and
// returns its descriptor, as YAML, with the image pinned. One executable
// must be for this platform: it runs once, for its spec.
func Package(ctx context.Context, c *registry.Client, image, name, version string, exes []registry.Executable) ([]byte, error) {
	host := ""
	for _, e := range exes {
		if e.Platform == registry.Platform() {
			host = e.Path
		}
	}
	if host == "" {
		return nil, fmt.Errorf("no executable is for %s, so the spec can't be read", registry.Platform())
	}
	s, err := external.ReadSpec(ctx, host, external.Options{Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		return nil, fmt.Errorf("read %s's spec: %w", host, err)
	}
	pinned, err := c.Push(ctx, image, exes)
	if err != nil {
		return nil, err
	}
	doc, err := spec.MarshalDescriptor(name, version, &spec.Descriptor{Image: pinned, Spec: s})
	if err != nil {
		return nil, err
	}
	if _, err := ParseVersion(doc); err != nil {
		return nil, fmt.Errorf("the descriptor doesn't parse: %w", err)
	}
	return doc, nil
}
