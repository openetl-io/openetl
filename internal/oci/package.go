package oci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/registry/remote"

	"github.com/openetl-io/openetl/internal/spec"
)

// Types of a package version's artifact, its config and its layers, and of
// an index card.
const (
	ArtifactType        = "application/vnd.openetl.connector.v1"
	MediaTypeDescriptor = "application/vnd.openetl.connector.descriptor.v1alpha2+json"
	MediaTypeBinary     = "application/vnd.openetl.connector.binary.v1"
	IndexArtifactType   = "application/vnd.openetl.connector.index.v1"
)

// Annotations of ours, beside org.opencontainers.image's.
const (
	AnnotationName       = "io.openetl.connector.name"
	AnnotationRoles      = "io.openetl.connector.roles"
	AnnotationTags       = "io.openetl.connector.tags"
	AnnotationRepository = "io.openetl.connector.repository"
	AnnotationRole       = "io.openetl.connector.role"
	AnnotationPlatform   = "io.openetl.connector.platform"
	AnnotationLogo       = "io.openetl.connector.logo"
)

// IndexName is a namespace's index repository, below it.
const IndexName = "index"

// MaxLogo is a logo's largest size.
const MaxLogo = 256 << 10

// LogoTypes are the media types a logo may have.
var LogoTypes = []string{"image/svg+xml", "image/png"}

// Blob is content to push: a file's, or Data.
type Blob struct {
	MediaType   string
	Path        string
	Data        []byte
	Annotations map[string]string
}

// Card is a package's entry in its namespace's index: what a catalog lists of
// it, as its publisher says.
type Card struct {
	// owner/name.
	Name          string
	Title         string
	Version       string
	Author        string
	Description   string
	License       string
	Source        string
	Documentation string
	Roles         []string
	Tags          []string
	// The package's repository, as an oci:// URL.
	Repository string
	Logo       *ocispec.Descriptor
	// The card's manifest's, sha256:{hex}.
	Digest string
}

// Annotations are the card's, as a manifest carries them.
func (c Card) Annotations() map[string]string {
	a := map[string]string{
		AnnotationName:                  c.Name,
		ocispec.AnnotationTitle:         c.Title,
		ocispec.AnnotationVersion:       c.Version,
		ocispec.AnnotationVendor:        c.Author,
		ocispec.AnnotationDescription:   c.Description,
		ocispec.AnnotationLicenses:      c.License,
		ocispec.AnnotationSource:        c.Source,
		ocispec.AnnotationDocumentation: c.Documentation,
		AnnotationRoles:                 strings.Join(c.Roles, ","),
		AnnotationTags:                  strings.Join(c.Tags, ","),
		AnnotationRepository:            c.Repository,
	}
	for k, v := range a {
		if v == "" {
			delete(a, k)
		}
	}
	return a
}

// CardOf reads a card from a manifest's annotations, and its logo layer.
func CardOf(m *Manifest) Card {
	a := m.Annotations
	c := Card{
		Name: a[AnnotationName], Title: a[ocispec.AnnotationTitle], Version: a[ocispec.AnnotationVersion],
		Author: a[ocispec.AnnotationVendor], Description: a[ocispec.AnnotationDescription], License: a[ocispec.AnnotationLicenses],
		Source: a[ocispec.AnnotationSource], Documentation: a[ocispec.AnnotationDocumentation],
		Roles: split(a[AnnotationRoles]), Tags: split(a[AnnotationTags]), Repository: a[AnnotationRepository],
		Digest: m.Descriptor.Digest.String(),
	}
	for _, l := range m.Layers {
		if _, ok := l.Annotations[AnnotationLogo]; ok && slices.Contains(LogoTypes, l.MediaType) {
			c.Logo = &l
			break
		}
	}
	return c
}

func split(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

// Version is a package version to push, as one manifest.
type Version struct {
	// owner/name.
	Name    string
	Version string
	// The descriptor as canonical JSON, naming its binaries and logo by the
	// oci:// URLs they're pushed to.
	Descriptor []byte
	Binaries   []Blob
	Logo       *Blob
	// The manifest's.
	Annotations map[string]string
}

// ErrVersionExists is a version already pushed with another descriptor.
var ErrVersionExists = errors.New("the version was already pushed with another descriptor; versions don't change, so give this one a new version")

// Push pushes v to repo, a package's repository, and tags it with its
// version. Pushing a version again with the same descriptor does nothing.
func (c *Client) Push(ctx context.Context, repo Ref, v Version) (ocispec.Descriptor, error) {
	if strings.Contains(v.Version, "+") {
		return ocispec.Descriptor{}, fmt.Errorf("version %s has build metadata, which a tag can't carry", v.Version)
	}
	r, err := c.Repository(repo)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	cfg := ocispec.Descriptor{MediaType: MediaTypeDescriptor, Digest: digest.FromBytes(v.Descriptor), Size: int64(len(v.Descriptor))}
	if m, err := c.FetchManifest(ctx, repo.At(v.Version)); err == nil {
		if m.Config.Digest != cfg.Digest {
			return ocispec.Descriptor{}, fmt.Errorf("%s: %w", repo.At(v.Version), ErrVersionExists)
		}
		return m.Descriptor, nil
	} else if !NotFound(err) {
		return ocispec.Descriptor{}, err
	}
	var layers []ocispec.Descriptor
	blobs := slices.Clone(v.Binaries)
	if v.Logo != nil {
		blobs = append(blobs, *v.Logo)
	}
	for _, b := range blobs {
		d, err := pushBlob(ctx, r, b)
		if err != nil {
			return ocispec.Descriptor{}, err
		}
		layers = append(layers, d)
	}
	if err := pushBytes(ctx, r, cfg, v.Descriptor); err != nil {
		return ocispec.Descriptor{}, err
	}
	m, err := oras.PackManifest(ctx, r, oras.PackManifestVersion1_1, ArtifactType, oras.PackManifestOptions{
		ConfigDescriptor: &cfg, Layers: layers, ManifestAnnotations: v.Annotations,
	})
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("%s: %w", repo, err)
	}
	if err := r.Tag(ctx, m, v.Version); err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("%s: %w", repo.At(v.Version), err)
	}
	return m, nil
}

// pushBlob pushes b unless the repository has it.
func pushBlob(ctx context.Context, r *remote.Repository, b Blob) (ocispec.Descriptor, error) {
	data := b.Data
	if b.Path != "" {
		var err error
		if data, err = os.ReadFile(b.Path); err != nil {
			return ocispec.Descriptor{}, err
		}
	}
	d := ocispec.Descriptor{MediaType: b.MediaType, Digest: digest.FromBytes(data), Size: int64(len(data)), Annotations: b.Annotations}
	return d, pushBytes(ctx, r, d, data)
}

func pushBytes(ctx context.Context, r *remote.Repository, d ocispec.Descriptor, data []byte) error {
	ok, err := r.Exists(ctx, d)
	if err != nil {
		return fmt.Errorf("%s@%s: %w", r.Reference.Repository, d.Digest, err)
	}
	if ok {
		return nil
	}
	if err := r.Push(ctx, d, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("push %s@%s: %w", r.Reference.Repository, d.Digest, err)
	}
	return nil
}

// Pulled is a package version, from its manifest.
type Pulled struct {
	Manifest *Manifest
	// The config: the descriptor, as canonical JSON.
	Descriptor []byte
}

// Pull reads the package version r names, by tag or digest.
func (c *Client) Pull(ctx context.Context, r Ref) (*Pulled, error) {
	m, err := c.FetchManifest(ctx, r)
	if err != nil {
		return nil, err
	}
	if m.ArtifactType != ArtifactType || m.Config.MediaType != MediaTypeDescriptor {
		return nil, fmt.Errorf("%s is a %q with a %q config, not a connector package", r, m.ArtifactType, m.Config.MediaType)
	}
	d, err := c.ReadBlob(ctx, r, m.Config, MaxManifest)
	if err != nil {
		return nil, err
	}
	return &Pulled{Manifest: m, Descriptor: d}, nil
}

// Index is ns's index repository.
func Index(ns Ref) Ref { return ns.Child(IndexName) }

// ErrIndexTaken is an index tag that holds another owner's package.
var ErrIndexTaken = errors.New("the namespace's index lists another package by that name")

// UpdateIndex moves the index tag of card's package to card, unless the tag
// holds a higher version. A prerelease moves it only past another, or when
// there's none. It reports whether it moved the tag. The card it replaces is
// deleted where the registry allows it.
func (c *Client) UpdateIndex(ctx context.Context, ns Ref, card Card, logo *Blob) (bool, error) {
	_, short := spec.SplitName(card.Name)
	idx := Index(ns)
	r, err := c.Repository(idx)
	if err != nil {
		return false, err
	}
	var old *Manifest
	switch m, err := c.FetchManifest(ctx, idx.At(short)); {
	case err == nil:
		old = m
		cur := CardOf(m)
		if cur.Name != card.Name {
			return false, fmt.Errorf("%s holds %s: %w", idx.At(short), cur.Name, ErrIndexTaken)
		}
		if !Newer(card.Version, cur.Version) {
			return false, nil
		}
	case !NotFound(err):
		return false, err
	}
	var layers []ocispec.Descriptor
	if logo != nil {
		l := *logo
		l.Annotations = map[string]string{AnnotationLogo: "", ocispec.AnnotationTitle: "logo" + logoExt(l.MediaType)}
		d, err := pushBlob(ctx, r, l)
		if err != nil {
			return false, err
		}
		layers = append(layers, d)
	}
	ann := card.Annotations()
	ann[ocispec.AnnotationCreated] = time.Now().UTC().Format(time.RFC3339)
	m, err := oras.PackManifest(ctx, r, oras.PackManifestVersion1_1, IndexArtifactType, oras.PackManifestOptions{Layers: layers, ManifestAnnotations: ann})
	if err != nil {
		return false, fmt.Errorf("%s: %w", idx, err)
	}
	if err := r.Tag(ctx, m, short); err != nil {
		return false, fmt.Errorf("%s: %w", idx.At(short), err)
	}
	if old != nil && old.Descriptor.Digest != m.Digest {
		// Untagged, it's garbage; registries that refuse deletes keep it.
		_ = r.Delete(ctx, old.Descriptor)
	}
	return true, nil
}

// Newer reports whether version a should replace b in an index: b is empty,
// or a is a higher or equal version, a release beating a prerelease.
func Newer(a, b string) bool {
	switch {
	case b == "":
		return true
	case spec.Prerelease(a) != spec.Prerelease(b):
		return spec.Prerelease(b)
	}
	return spec.CompareVersions(a, b) >= 0
}

func logoExt(mediaType string) string {
	if mediaType == "image/png" {
		return ".png"
	}
	return ".svg"
}

// ListIndex lists ns's index: each package's card. known holds cards from an
// earlier listing, by package short name; a card whose tag still has its
// digest isn't fetched again. A namespace without an index has no cards.
func (c *Client) ListIndex(ctx context.Context, ns Ref, known map[string]Card) ([]Card, error) {
	idx := Index(ns)
	tags, err := c.Tags(ctx, idx)
	if NotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Card
	for _, tag := range tags {
		ref := idx.At(tag)
		d, err := c.Resolve(ctx, ref)
		if err != nil {
			return nil, err
		}
		if k, ok := known[tag]; ok && k.Digest == d {
			out = append(out, k)
			continue
		}
		m, err := c.FetchManifest(ctx, idx.At(d))
		if err != nil {
			return nil, err
		}
		if m.ArtifactType != IndexArtifactType {
			continue
		}
		card := CardOf(m)
		if _, short := spec.SplitName(card.Name); card.Name == "" || short != tag || card.Version == "" {
			continue
		}
		if card.Repository == "" {
			card.Repository = ns.Child(tag).String()
		}
		out = append(out, card)
	}
	return out, nil
}

// unindexedListLimit bounds reading a registry's repository list, so a
// registry that serves it oddly can't use up the time for listing the index.
const unindexedListLimit = 30 * time.Second

var errPastNamespace = errors.New("past the namespace")

// ListUnindexed lists the packages below ns that its index doesn't, where
// the registry lists its repositories: each at its highest version, with a
// card from its manifest's annotations. indexed holds the index's packages'
// short names. It returns nothing, and no error, where the registry doesn't
// list repositories, which the OCI distribution spec doesn't ask of one.
func (c *Client) ListUnindexed(ctx context.Context, ns Ref, indexed map[string]bool, known map[string]Card) []Card {
	reg, err := c.Registry(ns)
	if err != nil {
		return nil
	}
	listing, cancel := context.WithTimeout(ctx, unindexedListLimit)
	defer cancel()
	// A registry can list everything it hosts, as GHCR does for a token, so
	// this starts at ns and stops past it: the names are sorted. A name before
	// ns means the registry ignored where to start.
	prefix := ns.Repository + "/"
	var repos []string
	err = reg.Repositories(listing, prefix, func(page []string) error {
		for _, r := range page {
			rest, ok := strings.CutPrefix(r, prefix)
			switch {
			case !ok && r > prefix:
				return errPastNamespace
			case ok && !strings.Contains(rest, "/") && rest != IndexName && !indexed[rest]:
				repos = append(repos, rest)
			}
		}
		return nil
	})
	if err != nil && !errors.Is(err, errPastNamespace) {
		return nil
	}
	var out []Card
	for _, name := range repos {
		repo := ns.Child(name)
		tags, err := c.Tags(ctx, repo)
		if err != nil {
			continue
		}
		best := ""
		for _, t := range tags {
			if spec.ValidVersion(t) && !strings.Contains(t, "+") && Newer(t, best) {
				best = t
			}
		}
		if best == "" {
			continue
		}
		d, err := c.Resolve(ctx, repo.At(best))
		if err != nil {
			continue
		}
		if k, ok := known[name]; ok && k.Digest == d {
			out = append(out, k)
			continue
		}
		m, err := c.FetchManifest(ctx, repo.At(d))
		if err != nil || m.ArtifactType != ArtifactType {
			continue
		}
		card := CardOf(m)
		card.Version, card.Repository = best, repo.String()
		if _, short := spec.SplitName(card.Name); card.Name == "" || short != name {
			continue
		}
		out = append(out, card)
	}
	return out
}

// BlobURL is the oci:// URL of content in repo, by its digest.
func BlobURL(repo Ref, content []byte) string {
	sum := sha256.Sum256(content)
	return repo.At("sha256:" + hex.EncodeToString(sum[:])).String()
}

// LogoType is the media type of a logo's content, from its file name and its
// bytes; "" if it's neither SVG nor PNG.
func LogoType(name string, content []byte) string {
	switch {
	case bytes.HasPrefix(content, []byte("\x89PNG\r\n\x1a\n")):
		return "image/png"
	case strings.EqualFold(path.Ext(name), ".svg") && looksLikeSVG(content):
		return "image/svg+xml"
	}
	return ""
}

// SniffLogoType is the media type of a logo fetched from a URL, whose path
// needn't end in its type's extension; "" if it's neither SVG nor PNG.
func SniffLogoType(content []byte) string { return LogoType("logo.svg", content) }

// FetchLogo downloads a logo from an https URL, and returns its content and
// media type: an SVG or PNG of at most MaxLogo bytes.
func (c *Client) FetchLogo(ctx context.Context, url string) ([]byte, string, error) {
	if !strings.HasPrefix(url, "https://") {
		return nil, "", fmt.Errorf("logo %q isn't an https URL", url)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", fmt.Errorf("logo %s: %w", url, err)
	}
	res, err := c.httpClient().Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("logo %s: %w", url, err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("logo %s: %s", url, res.Status)
	}
	content, err := ReadAllLimit(res.Body, MaxLogo)
	if err != nil {
		return nil, "", fmt.Errorf("logo %s: %w", url, err)
	}
	t := SniffLogoType(content)
	if t == "" {
		return nil, "", fmt.Errorf("logo %s is neither an SVG nor a PNG image", url)
	}
	return content, t, nil
}

func looksLikeSVG(b []byte) bool {
	head := b[:min(len(b), 1024)]
	return bytes.Contains(bytes.ToLower(head), []byte("<svg"))
}

// ReadAllLimit reads r, failing past limit bytes.
func ReadAllLimit(r io.Reader, limit int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, fmt.Errorf("it's more than %d bytes", limit)
	}
	return b, nil
}
