// Package registry pulls and pushes external connectors' images. A
// connector's image is an OCI image index with an image per platform, each
// holding the connector's executable as its entrypoint; the subprocess
// launcher runs that file. Registries are reached through netguard.
package registry

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"

	"github.com/unsuitable001/openetl/internal/netguard"
)

// MaxExecutable is the largest executable Pull extracts.
const MaxExecutable = 1 << 30

// Client talks to OCI registries. The zero Client reaches public registries
// anonymously.
type Client struct {
	Policy netguard.Policy
	// Nil: anonymous.
	Keychain authn.Keychain
}

func (c *Client) options(ctx context.Context) []remote.Option {
	opts := []remote.Option{remote.WithContext(ctx), remote.WithTransport(c.Policy.Transport()), remote.WithUserAgent("openetl")}
	if c.Keychain != nil {
		return append(opts, remote.WithAuthFromKeychain(c.Keychain))
	}
	return append(opts, remote.WithAuth(authn.Anonymous))
}

// Platform is the runner's, as os/arch.
func Platform() string { return runtime.GOOS + "/" + runtime.GOARCH }

// Image is an image pinned by digest.
type Image struct {
	// repository@sha256:…
	Ref string
	// os/arch, sorted.
	Platforms []string
}

// Resolve pins ref, which names a tag or a digest explicitly, to its digest.
func (c *Client) Resolve(ctx context.Context, ref string) (Image, error) {
	r, err := name.ParseReference(ref, name.StrictValidation)
	if err != nil {
		return Image{}, fmt.Errorf("image %q: %w", ref, err)
	}
	desc, err := remote.Get(r, c.options(ctx)...)
	if err != nil {
		return Image{}, fmt.Errorf("image %s: %w", ref, err)
	}
	img := Image{Ref: r.Context().Digest(desc.Digest.String()).String()}
	switch {
	case desc.MediaType.IsIndex():
		idx, err := desc.ImageIndex()
		if err != nil {
			return Image{}, fmt.Errorf("image %s: %w", ref, err)
		}
		m, err := idx.IndexManifest()
		if err != nil {
			return Image{}, fmt.Errorf("image %s: %w", ref, err)
		}
		for _, d := range m.Manifests {
			if d.MediaType.IsImage() && d.Platform != nil && d.Platform.OS != "unknown" {
				img.Platforms = append(img.Platforms, d.Platform.OS+"/"+d.Platform.Architecture)
			}
		}
	case desc.MediaType.IsImage():
		i, err := desc.Image()
		if err != nil {
			return Image{}, fmt.Errorf("image %s: %w", ref, err)
		}
		cfg, err := i.ConfigFile()
		if err != nil {
			return Image{}, fmt.Errorf("image %s: %w", ref, err)
		}
		img.Platforms = []string{cfg.OS + "/" + cfg.Architecture}
	default:
		return Image{}, fmt.Errorf("image %s is a %s, not an image or an image index", ref, desc.MediaType)
	}
	slices.Sort(img.Platforms)
	img.Platforms = slices.Compact(img.Platforms)
	if len(img.Platforms) == 0 {
		return Image{}, fmt.Errorf("image %s has no platform images", ref)
	}
	return img, nil
}

// Pull extracts platform's executable from image, which must be pinned by
// digest, into dir/sha256-{platform image digest}/, unless an earlier Pull
// has, and returns its path.
func (c *Client) Pull(ctx context.Context, image, platform, dir string) (string, error) {
	ref, err := name.NewDigest(image, name.StrictValidation)
	if err != nil {
		return "", fmt.Errorf("image %q must be pinned by digest: %w", image, err)
	}
	img, err := c.platformImage(ctx, ref, platform)
	if err != nil {
		return "", err
	}
	d, err := img.Digest()
	if err != nil {
		return "", err
	}
	cfg, err := img.ConfigFile()
	if err != nil {
		return "", fmt.Errorf("image %s: %w", image, err)
	}
	entry := cfg.Config.Entrypoint
	if len(entry) == 0 {
		entry = cfg.Config.Cmd
	}
	if len(entry) == 0 || !path.IsAbs(entry[0]) || path.Clean(entry[0]) != entry[0] {
		return "", fmt.Errorf("image %s's %s image needs an absolute entrypoint, its executable", image, platform)
	}
	final := filepath.Join(dir, d.Algorithm+"-"+d.Hex)
	exe := filepath.Join(final, path.Base(entry[0]))
	if fi, err := os.Stat(exe); err == nil && fi.Mode().IsRegular() {
		return exe, nil
	}

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	tmp, err := os.MkdirTemp(dir, ".pull-")
	if err != nil {
		return "", err
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	if err := extract(img, entry[0], filepath.Join(tmp, path.Base(entry[0]))); err != nil {
		return "", fmt.Errorf("image %s (%s): %w", image, platform, err)
	}
	// Another pull of the same image may have won the rename.
	if err := os.Rename(tmp, final); err != nil {
		if _, serr := os.Stat(exe); serr != nil {
			return "", err
		}
	}
	return exe, nil
}

func (c *Client) platformImage(ctx context.Context, ref name.Digest, platform string) (v1.Image, error) {
	desc, err := remote.Get(ref, c.options(ctx)...)
	if err != nil {
		return nil, fmt.Errorf("image %s: %w", ref, err)
	}
	if desc.MediaType.IsImage() {
		img, err := desc.Image()
		if err != nil {
			return nil, err
		}
		cfg, err := img.ConfigFile()
		if err != nil {
			return nil, err
		}
		if got := cfg.OS + "/" + cfg.Architecture; got != platform {
			return nil, fmt.Errorf("image %s is for %s, not %s", ref, got, platform)
		}
		return img, nil
	}
	idx, err := desc.ImageIndex()
	if err != nil {
		return nil, fmt.Errorf("image %s: %w", ref, err)
	}
	m, err := idx.IndexManifest()
	if err != nil {
		return nil, fmt.Errorf("image %s: %w", ref, err)
	}
	for _, d := range m.Manifests {
		if d.MediaType.IsImage() && d.Platform != nil && d.Platform.OS+"/"+d.Platform.Architecture == platform {
			return remote.Image(ref.Context().Digest(d.Digest.String()), c.options(ctx)...)
		}
	}
	return nil, &NoPlatformError{Image: ref.String(), Platform: platform}
}

// NoPlatformError is an image without an image for the runner's platform.
type NoPlatformError struct{ Image, Platform string }

func (e *NoPlatformError) Error() string {
	return fmt.Sprintf("image %s has no image for %s", e.Image, e.Platform)
}

// extract copies the regular file at entry in img's filesystem to dst.
func extract(img v1.Image, entry, dst string) error {
	rc := mutate.Extract(img)
	defer func() { _ = rc.Close() }()
	tr := tar.NewReader(rc)
	want := strings.TrimPrefix(entry, "/")
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("its entrypoint %s isn't in it", entry)
		}
		if err != nil {
			return err
		}
		if path.Clean(strings.TrimPrefix(h.Name, "./")) != want {
			continue
		}
		if h.Typeflag != tar.TypeReg {
			return fmt.Errorf("its entrypoint %s isn't a regular file", entry)
		}
		if h.Size > MaxExecutable {
			return fmt.Errorf("its entrypoint %s is larger than %d bytes", entry, MaxExecutable)
		}
		f, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o755) //nolint:gosec // an executable
		if err != nil {
			return err
		}
		if _, err := io.Copy(f, io.LimitReader(tr, MaxExecutable)); err != nil {
			_ = f.Close()
			return err
		}
		return f.Close()
	}
}

// Executable is one platform's connector executable.
type Executable struct {
	// os/arch
	Platform string
	Path     string
}

// Push pushes an image index to ref, a tag, with an image per executable
// that holds only the executable, at /{its base name}, as its entrypoint.
// The same executables give the same digest. It returns the pinned image.
func (c *Client) Push(ctx context.Context, ref string, exes []Executable) (string, error) {
	tag, err := name.NewTag(ref, name.StrictValidation)
	if err != nil {
		return "", fmt.Errorf("image %q: %w", ref, err)
	}
	if len(exes) == 0 {
		return "", errors.New("no executables")
	}
	idx := mutate.IndexMediaType(empty.Index, types.OCIImageIndex)
	seen := map[string]bool{}
	for _, e := range exes {
		goos, arch, ok := strings.Cut(e.Platform, "/")
		if !ok || goos == "" || arch == "" || strings.Contains(arch, "/") || seen[e.Platform] {
			return "", fmt.Errorf("platform %q must be os/arch, once each", e.Platform)
		}
		seen[e.Platform] = true
		img, err := executableImage(e.Path, goos, arch)
		if err != nil {
			return "", err
		}
		idx = mutate.AppendManifests(idx, mutate.IndexAddendum{
			Add:        img,
			Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: goos, Architecture: arch}},
		})
	}
	if err := remote.WriteIndex(tag, idx, c.options(ctx)...); err != nil {
		return "", fmt.Errorf("push %s: %w", ref, err)
	}
	d, err := idx.Digest()
	if err != nil {
		return "", err
	}
	return tag.Context().Digest(d.String()).String(), nil
}

func executableImage(exe, goos, arch string) (v1.Image, error) {
	b, err := os.ReadFile(exe) //nolint:gosec // the caller names it
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	base := filepath.Base(exe)
	if err := tw.WriteHeader(&tar.Header{Name: base, Mode: 0o755, Size: int64(len(b)), Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}); err != nil {
		return nil, err
	}
	if _, err := tw.Write(b); err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(buf.Bytes())), nil
	}, tarball.WithMediaType(types.OCILayer))
	if err != nil {
		return nil, err
	}
	img := mutate.ConfigMediaType(mutate.MediaType(empty.Image, types.OCIManifestSchema1), types.OCIConfigJSON)
	if img, err = mutate.AppendLayers(img, layer); err != nil {
		return nil, err
	}
	cfg, err := img.ConfigFile()
	if err != nil {
		return nil, err
	}
	cfg = cfg.DeepCopy()
	cfg.OS, cfg.Architecture = goos, arch
	cfg.Config.Entrypoint = []string{"/" + base}
	return mutate.ConfigFile(img, cfg)
}
