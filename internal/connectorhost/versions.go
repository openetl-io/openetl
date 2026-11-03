package connectorhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/unsuitable001/openetl/internal/connectorhost/external"
	"github.com/unsuitable001/openetl/internal/declarative"
	"github.com/unsuitable001/openetl/internal/netguard"
	"github.com/unsuitable001/openetl/internal/platform/errs"
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
	Info    spec.Info
	// Of the artifact: sha256:{hex}.
	Digest string
	// One of these.
	Manifest   *declarative.Connector
	Descriptor *spec.Descriptor
	// The descriptor's executable, once a runner downloads it.
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

// ParseVersion parses a connector version's artifact: a Connector, whose
// binaries must be pinned, or a DeclarativeSource document. It does no I/O.
func ParseVersion(content []byte) (*Version, error) {
	doc, err := spec.Parse(content)
	if err != nil {
		return nil, err
	}
	v := &Version{Name: doc.Name, Version: doc.Version, Info: doc.Info, Digest: Digest(content)}
	switch doc.Kind {
	case spec.KindDeclarativeSource:
		if v.Manifest, err = declarative.Load(doc.Manifest); err != nil {
			return nil, err
		}
		v.Kind = "source"
	case spec.KindConnector:
		s := doc.Descriptor.Spec
		if p := doc.Descriptor.Unpinned(); len(p) > 0 {
			return nil, fmt.Errorf("binaries for %s have no digest", strings.Join(p, ", "))
		}
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

// MaxExecutable is the largest binary a runner or the API downloads.
const MaxExecutable = 1 << 30

// Platform is this machine's, as os/arch.
func Platform() string { return runtime.GOOS + "/" + runtime.GOARCH }

// Fetcher gets artifacts and connector versions by digest on a runner,
// through a cache: artifacts in {Cache}/artifacts, and descriptors'
// executables in {Cache}/executables.
type Fetcher struct {
	// An artifact's content, from the control plane.
	Artifact func(ctx context.Context, digest string) ([]byte, error)
	// Downloads binaries; a netguard client.
	Client *http.Client
	Cache  string
}

// Fetch returns the connector version at digest, with its executable
// downloaded for this platform.
func (f *Fetcher) Fetch(ctx context.Context, digest string) (*Version, error) {
	content, err := f.Get(ctx, digest)
	if err != nil {
		return nil, err
	}
	v, err := ParseVersion(content)
	if err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "connector version "+digest)
	}
	if v.Descriptor == nil {
		return v, nil
	}
	what := fmt.Sprintf("%s connector %s %s", v.Kind, v.Name, v.Version)
	b, ok := v.Descriptor.Binaries[Platform()]
	if !ok {
		return nil, errs.New(errs.ConfigInvalid, "%s has no binary for %s", what, Platform())
	}
	v.Executable, err = f.executable(ctx, b, v.Kind+"-"+v.Name)
	var mismatch *DigestError
	switch {
	case errors.As(err, &mismatch):
		return nil, errs.Wrap(errs.ConfigInvalid, err, what)
	case err != nil:
		return nil, errs.Wrap(errs.Transient, err, what)
	}
	return v, nil
}

// executable downloads b into {Cache}/executables/sha256-{hex}/name, once.
func (f *Fetcher) executable(ctx context.Context, b spec.Binary, name string) (string, error) {
	dir := filepath.Join(f.Cache, "executables")
	final := filepath.Join(dir, strings.Replace(b.Digest, ":", "-", 1))
	exe := filepath.Join(final, name)
	if fi, err := os.Stat(exe); err == nil && fi.Mode().IsRegular() {
		return exe, nil
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	tmp, err := os.MkdirTemp(dir, ".download-")
	if err != nil {
		return "", err
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	out, err := os.OpenFile(filepath.Join(tmp, name), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o755) //nolint:gosec // an executable, under the cache
	if err != nil {
		return "", err
	}
	got, err := download(ctx, f.Client, b.URL, out)
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return "", err
	}
	if got != b.Digest {
		return "", &DigestError{URL: b.URL, Want: b.Digest, Got: got}
	}
	// Another download of the same binary may have won the rename.
	if err := os.Rename(tmp, final); err != nil {
		if _, serr := os.Stat(exe); serr != nil {
			return "", err
		}
	}
	return exe, nil
}

// DigestError is a binary whose content doesn't match its digest.
type DigestError struct{ URL, Want, Got string }

func (e *DigestError) Error() string {
	return fmt.Sprintf("%s has digest %s, not %s", e.URL, e.Got, e.Want)
}

// download writes url's body to w, and returns its digest.
func download(ctx context.Context, c *http.Client, url string, w io.Writer) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download %s: %s", url, resp.Status)
	}
	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(w, h), netguard.LimitBody(resp.Body, MaxExecutable)); err != nil {
		return "", fmt.Errorf("download %s: %w", url, err)
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

// PinBinaries downloads each of d's binaries that has no digest, and fills
// in its digest.
func PinBinaries(ctx context.Context, c *http.Client, d *spec.Descriptor) error {
	for _, p := range d.Unpinned() {
		b := d.Binaries[p]
		var err error
		if b.Digest, err = download(ctx, c, b.URL, io.Discard); err != nil {
			return fmt.Errorf("binaries.%s: %w", p, err)
		}
		d.Binaries[p] = b
	}
	return nil
}

// Get returns the artifact at digest, from the cache if it's there.
func (f *Fetcher) Get(ctx context.Context, digest string) ([]byte, error) {
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

// Executable is one platform's executable, at a local path, and the URL it
// will be downloaded from.
type Executable struct {
	// os/arch.
	Platform string
	Path     string
	URL      string
}

// Package returns an external connector's descriptor, as YAML, with each
// executable's digest. One executable must be for this platform: it runs
// once, for its spec.
func Package(ctx context.Context, name, version string, info spec.Info, exes []Executable) ([]byte, error) {
	host := ""
	d := &spec.Descriptor{Binaries: map[string]spec.Binary{}}
	for _, e := range exes {
		if e.Platform == Platform() {
			host = e.Path
		}
		f, err := os.Open(e.Path)
		if err != nil {
			return nil, err
		}
		h := sha256.New()
		_, err = io.Copy(h, f)
		_ = f.Close()
		if err != nil {
			return nil, err
		}
		d.Binaries[e.Platform] = spec.Binary{URL: e.URL, Digest: "sha256:" + hex.EncodeToString(h.Sum(nil))}
	}
	if host == "" {
		return nil, fmt.Errorf("no executable is for %s, so the spec can't be read", Platform())
	}
	var err error
	if d.Spec, err = external.ReadSpec(ctx, host, external.Options{Logger: slog.New(slog.DiscardHandler)}); err != nil {
		return nil, fmt.Errorf("read %s's spec: %w", host, err)
	}
	doc, err := spec.MarshalDescriptor(name, version, info, d)
	if err != nil {
		return nil, err
	}
	if _, err := ParseVersion(doc); err != nil {
		return nil, fmt.Errorf("the descriptor doesn't parse: %w", err)
	}
	return doc, nil
}
