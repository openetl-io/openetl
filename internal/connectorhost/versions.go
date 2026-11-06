package connectorhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
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

// Version is a connector package's version, from one artifact: a
// descriptor, or a declarative source's manifest, which is a package with a
// source role.
type Version struct {
	// owner/name.
	Name    string
	Version string
	Info    spec.Info
	// Of the artifact: sha256:{hex}.
	Digest string
	Roles  map[string]*Role
	// A v1alpha1 descriptor or a DeclarativeSource document, either of which
	// may add its one role to a version that lacks it.
	OneRole bool
}

// Role is one of a version's roles.
type Role struct {
	*spec.Role
	// A manifest's, loaded.
	Manifest *declarative.Connector
	// A binary's, once a runner downloads it.
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
	v := &Version{Name: doc.Name, Version: doc.Version, Info: doc.Info, Digest: Digest(content), Roles: map[string]*Role{}}
	switch doc.Kind {
	case spec.KindDeclarativeSource:
		m, err := declarative.Load(doc.Manifest)
		if err != nil {
			return nil, err
		}
		var whole struct {
			Spec json.RawMessage `json:"spec"`
		}
		if err := json.Unmarshal(doc.Manifest, &whole); err != nil {
			return nil, err
		}
		v.Roles[spec.RoleSource] = &Role{Role: &spec.Role{Manifest: whole.Spec, Resources: spec.Resources{Hint: m.Hint()}}, Manifest: m}
		v.OneRole = true
	case spec.KindConnector:
		if p := doc.Descriptor.Unpinned(); len(p) > 0 {
			return nil, fmt.Errorf("binaries for %s have no digest", strings.Join(p, ", "))
		}
		for name, r := range doc.Descriptor.Roles {
			role := &Role{Role: r}
			if r.Manifest != nil {
				if role.Manifest, err = loadInline(doc, r.Manifest); err != nil {
					return nil, fmt.Errorf("roles.%s.runtime.manifest: %w", name, err)
				}
			} else if _, err := external.CompileSchema(r.Spec.GetConfigSchema()); err != nil {
				return nil, fmt.Errorf("roles.%s.config: %w", name, err)
			} else if _, err := external.SecretProperties(r.Spec.GetConfigSchema()); err != nil {
				return nil, fmt.Errorf("roles.%s.config: %w", name, err)
			}
			v.Roles[name] = role
		}
		v.OneRole = doc.APIVersion != spec.APIVersionConnector
	default:
		return nil, fmt.Errorf("a %s isn't a connector version", doc.Kind)
	}
	for role := range v.Roles {
		if Builtin(role, v.Name) {
			return nil, fmt.Errorf("%s is a built-in %s connector", v.Name, role)
		}
	}
	return v, nil
}

// loadInline loads a descriptor's inline manifest, which is a
// DeclarativeSource's spec, under the descriptor's name and version.
func loadInline(doc *spec.Document, manifest []byte) (*declarative.Connector, error) {
	var m map[string]any
	if err := json.Unmarshal(manifest, &m); err != nil {
		return nil, err
	}
	full, err := json.Marshal(map[string]any{
		"apiVersion": spec.APIVersion, "kind": spec.KindDeclarativeSource,
		"metadata": map[string]any{"name": doc.Name, "version": doc.Version}, "spec": m,
	})
	if err != nil {
		return nil, err
	}
	return declarative.Load(full)
}

// With returns env with v's role added, for NewSource or NewDestination.
func (e Env) With(v *Version, role string) Env {
	r := v.Roles[role]
	switch {
	case r == nil:
		return e
	case r.Manifest != nil:
		e.Manifests = maps.Clone(e.Manifests)
		if e.Manifests == nil {
			e.Manifests = map[string]*declarative.Connector{}
		}
		e.Manifests[v.Name] = r.Manifest
	default:
		e.Externals = maps.Clone(e.Externals)
		if e.Externals == nil {
			e.Externals = map[string]External{}
		}
		e.Externals[role+"/"+v.Name] = External{Spec: r.Spec, Executable: r.Executable}
	}
	return e
}

// Release is this build's release, such as 0.1.0, or "dev".
func Release() string { return external.Release() }

// Runnable reports why this runner can't run a role, or "" if it can: one
// without a manifest needs the protocol, every required feature, a release
// at least the one it requires, and a binary for this platform.
func Runnable(r *spec.Role) string {
	if p := r.Requires.Platform; p != "" && Release() != "dev" && spec.CompareVersions(Release(), p) < 0 {
		return fmt.Sprintf("it needs openetl %s or later, and this runner is %s", p, Release())
	}
	if r.Manifest != nil {
		return ""
	}
	if p := r.Spec.GetProtocolVersion(); p != external.Protocol {
		return fmt.Sprintf("it speaks protocol %s, and this runner speaks %s", p, external.Protocol)
	}
	for _, f := range r.Spec.GetRequiredFeatures() {
		if !slices.Contains(external.Features, f) {
			return fmt.Sprintf("it needs the feature %s, which this runner doesn't have", f)
		}
	}
	if _, ok := r.Binaries[Platform()]; ok {
		return ""
	}
	if r.Image != nil {
		return fmt.Sprintf("it has no binary for %s, only an image, and this runner has no container launcher", Platform())
	}
	return fmt.Sprintf("it has no binary for %s", Platform())
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

// Fetch returns the connector version at digest, with role's executable
// downloaded for this platform. A role this runner can't run fails with
// CONFIG_INVALID.
func (f *Fetcher) Fetch(ctx context.Context, digest, role string) (*Version, error) {
	content, err := f.Get(ctx, digest)
	if err != nil {
		return nil, err
	}
	v, err := ParseVersion(content)
	if err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "connector version "+digest)
	}
	what := fmt.Sprintf("%s %s's %s", v.Name, v.Version, role)
	r := v.Roles[role]
	if r == nil {
		return nil, errs.New(errs.ConfigInvalid, "%s %s has no %s role", v.Name, v.Version, role)
	}
	if why := Runnable(r.Role); why != "" {
		return nil, errs.New(errs.ConfigInvalid, "%s can't run here: %s", what, why)
	}
	if r.Manifest != nil {
		return v, nil
	}
	_, short := spec.SplitName(v.Name)
	r.Executable, err = f.Executable(ctx, r.Binaries[Platform()], role+"-"+short)
	var mismatch *DigestError
	switch {
	case errors.As(err, &mismatch):
		return nil, errs.Wrap(errs.ConfigInvalid, err, what)
	case err != nil:
		return nil, errs.Wrap(errs.Transient, err, what)
	}
	return v, nil
}

// Executable downloads b into {Cache}/executables/sha256-{hex}/name, once.
func (f *Fetcher) Executable(ctx context.Context, b spec.Binary, name string) (string, error) {
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
	// Another download of the same binary may have won the rename, or put it
	// there under another name, as a package whose roles share it does.
	if err := os.Rename(tmp, final); err != nil {
		if _, serr := os.Stat(exe); serr == nil {
			return exe, nil
		}
		if rerr := os.Rename(filepath.Join(tmp, name), exe); rerr != nil {
			return "", fmt.Errorf("%w; and into the existing directory: %w", err, rerr)
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
		role, platform, _ := strings.Cut(p, "/")
		b := d.Roles[role].Binaries[platform]
		var err error
		if b.Digest, err = download(ctx, c, b.URL, io.Discard); err != nil {
			return fmt.Errorf("roles.%s.runtime.binaries.%s: %w", role, platform, err)
		}
		d.Roles[role].Binaries[platform] = b
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

// PackageRole is what a role is packaged from. Its Spec is read from
// Executables' for this platform when it has one, and is Spec otherwise,
// such as an image's, read through a container runtime.
type PackageRole struct {
	Executables []Executable
	Spec        *protocolv1.SpecResponse
	Image       *spec.Image
	// A DeclarativeSource's spec, as canonical JSON.
	Manifest  []byte
	Resources spec.Resources
	State     *spec.State
	// The oldest release it runs on.
	Platform string
}

// Package returns a package's descriptor, as YAML, with each executable's
// digest.
func Package(ctx context.Context, name, version string, info spec.Info, roles map[string]PackageRole) ([]byte, error) {
	d := &spec.Descriptor{Roles: map[string]*spec.Role{}}
	for role, pr := range roles {
		r := &spec.Role{Image: pr.Image, Manifest: pr.Manifest, Resources: pr.Resources, State: pr.State, Spec: pr.Spec}
		r.Requires.Platform = pr.Platform
		host := ""
		for _, e := range pr.Executables {
			if r.Binaries == nil {
				r.Binaries = map[string]spec.Binary{}
			}
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
			r.Binaries[e.Platform] = spec.Binary{URL: e.URL, Digest: "sha256:" + hex.EncodeToString(h.Sum(nil))}
		}
		if host != "" {
			var err error
			if r.Spec, err = external.ReadSpec(ctx, host, spec.KindOf(role), external.Options{Logger: slog.New(slog.DiscardHandler)}); err != nil {
				return nil, fmt.Errorf("read %s's spec: %w", host, err)
			}
		}
		switch {
		case r.Manifest == nil && r.Spec == nil:
			return nil, fmt.Errorf("the %s role has no executable for %s, so its spec can't be read", role, Platform())
		case r.Spec != nil && spec.RoleOf(r.Spec) != role:
			return nil, fmt.Errorf("the %s role's executable is a %s", role, spec.RoleOf(r.Spec))
		case r.Spec != nil:
			r.Requires.Protocol, r.Requires.Features = r.Spec.GetProtocolVersion(), r.Spec.GetRequiredFeatures()
		}
		d.Roles[role] = r
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
