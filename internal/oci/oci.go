// Package oci reads and writes connector packages in OCI registries, through
// oras-go, and names their blobs by oci:// URLs.
package oci

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strings"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"
	"oras.land/oras-go/v2/registry/remote/errcode"

	"github.com/openetl-io/openetl/internal/netguard"
)

// Scheme prefixes a URL that names a repository, or a blob or manifest in
// one.
const Scheme = "oci://"

// MaxManifest is the largest manifest or descriptor read.
const MaxManifest = 4 << 20

// Ref is a repository in a registry, with a tag or a digest, or neither.
type Ref struct {
	// host[:port]
	Registry   string
	Repository string
	Tag        string
	// sha256:{hex}
	Digest string
}

// Parse reads oci://REGISTRY/REPOSITORY[:TAG][@DIGEST].
func Parse(s string) (Ref, error) {
	rest, ok := strings.CutPrefix(s, Scheme)
	if !ok {
		return Ref{}, fmt.Errorf("%q isn't an oci:// URL", s)
	}
	var r Ref
	if name, d, ok := strings.Cut(rest, "@"); ok {
		rest, r.Digest = name, d
		if _, err := digest.Parse(d); err != nil || !strings.HasPrefix(d, "sha256:") {
			return Ref{}, fmt.Errorf("%q: %q isn't a sha256 digest", s, d)
		}
	}
	if i := strings.LastIndex(rest, ":"); i > strings.LastIndex(rest, "/") {
		rest, r.Tag = rest[:i], rest[i+1:]
	}
	host, repo, ok := strings.Cut(rest, "/")
	if !ok || repo == "" {
		return Ref{}, fmt.Errorf("%q names no repository", s)
	}
	r.Registry, r.Repository = host, repo
	ref := registry.Reference{Registry: host, Repository: repo, Reference: r.Tag}
	if err := ref.ValidateRegistry(); err != nil {
		return Ref{}, fmt.Errorf("%q: %w", s, err)
	}
	if err := ref.ValidateRepository(); err != nil {
		return Ref{}, fmt.Errorf("%q: %w", s, err)
	}
	if r.Tag != "" {
		if err := ref.ValidateReferenceAsTag(); err != nil {
			return Ref{}, fmt.Errorf("%q: %w", s, err)
		}
	}
	return r, nil
}

// String is the oci:// URL.
func (r Ref) String() string {
	s := Scheme + r.Name()
	if r.Tag != "" {
		s += ":" + r.Tag
	}
	if r.Digest != "" {
		s += "@" + r.Digest
	}
	return s
}

// Name is REGISTRY/REPOSITORY.
func (r Ref) Name() string { return r.Registry + "/" + r.Repository }

// Repo is the repository alone, without a tag or digest.
func (r Ref) Repo() Ref { return Ref{Registry: r.Registry, Repository: r.Repository} }

// Child is the repository name below r's.
func (r Ref) Child(name string) Ref {
	return Ref{Registry: r.Registry, Repository: r.Repository + "/" + name}
}

// At is r's repository at a tag or a digest.
func (r Ref) At(reference string) Ref {
	out := r.Repo()
	if strings.HasPrefix(reference, "sha256:") {
		out.Digest = reference
	} else {
		out.Tag = reference
	}
	return out
}

// Reference is the tag, or the digest when there is one.
func (r Ref) Reference() string {
	if r.Digest != "" {
		return r.Digest
	}
	return r.Tag
}

// Covers reports whether r, a namespace or repository, holds repository.
func (r Ref) Covers(repository Ref) bool {
	return r.Registry == repository.Registry && (r.Repository == repository.Repository || strings.HasPrefix(repository.Repository, r.Repository+"/"))
}

// Credential is a registry's user and password or token. The zero value is
// anonymous.
type Credential struct {
	Username, Password string
	// An identity token, as docker login stores for some registries.
	RefreshToken string
}

// Credentials returns the credential for a repository; the zero Credential
// for none.
type Credentials func(ctx context.Context, repository Ref) (Credential, error)

// Client reaches registries.
type Client struct {
	// Nil: a netguard client that reaches public addresses only.
	HTTP        *http.Client
	Credentials Credentials
	// Hosts reached over plain HTTP besides loopback ones, which always are.
	PlainHTTP []string
}

// Repository is r's repository, with the client's credentials for it.
func (c *Client) Repository(r Ref) (*remote.Repository, error) {
	host, repo := r.Registry, r.Repository
	if host == "docker.io" {
		host = "registry-1.docker.io"
		if !strings.Contains(repo, "/") {
			repo = "library/" + repo
		}
	}
	hc := c.HTTP
	if hc == nil {
		hc = (&netguard.Policy{}).Client()
	}
	ac := &auth.Client{
		Client: hc,
		Cache:  auth.NewCache(),
		// The HTTP client's dialer already guards every address.
		BlockedRealmPrefixes: []netip.Prefix{},
	}
	if c.Credentials != nil {
		named := r.Repo()
		ac.Credential = func(ctx context.Context, _ string) (auth.Credential, error) {
			cred, err := c.Credentials(ctx, named)
			if err != nil {
				return auth.EmptyCredential, err
			}
			if cred == (Credential{}) {
				return auth.EmptyCredential, nil
			}
			return auth.Credential{Username: cred.Username, Password: cred.Password, RefreshToken: cred.RefreshToken}, nil
		}
	}
	rr, err := remote.NewRepository(host + "/" + repo)
	if err != nil {
		return nil, err
	}
	rr.Client, rr.PlainHTTP = ac, c.plain(host)
	rr.TagListMaxPages = 100
	return rr, nil
}

// Registry is r's registry, for listing its repositories.
func (c *Client) Registry(r Ref) (*remote.Registry, error) {
	repo, err := c.Repository(r)
	if err != nil {
		return nil, err
	}
	reg, err := remote.NewRegistry(repo.Reference.Registry)
	if err != nil {
		return nil, err
	}
	reg.Client, reg.PlainHTTP = repo.Client, repo.PlainHTTP
	return reg, nil
}

func (c *Client) plain(host string) bool {
	if slices.Contains(c.PlainHTTP, host) {
		return true
	}
	h, _, err := net.SplitHostPort(host)
	if err != nil {
		h = host
	}
	if h == "localhost" {
		return true
	}
	a, err := netip.ParseAddr(strings.Trim(h, "[]"))
	return err == nil && a.IsLoopback()
}

// FetchBlob writes r.Digest's blob to w, and fails unless it's at most limit
// bytes with that digest.
func (c *Client) FetchBlob(ctx context.Context, r Ref, w io.Writer, limit int64) error {
	if r.Digest == "" {
		return fmt.Errorf("%s names no blob's digest", r)
	}
	repo, err := c.Repository(r)
	if err != nil {
		return err
	}
	desc, rc, err := repo.Blobs().FetchReference(ctx, r.Digest)
	if err != nil {
		return fmt.Errorf("%s: %w", r, err)
	}
	defer func() { _ = rc.Close() }()
	if desc.Size > limit {
		return fmt.Errorf("%s is %d bytes, more than %d", r, desc.Size, limit)
	}
	vr := content.NewVerifyReader(rc, desc)
	if _, err := io.Copy(w, vr); err != nil {
		return fmt.Errorf("%s: %w", r, err)
	}
	if err := vr.Verify(); err != nil {
		return fmt.Errorf("%s: %w", r, err)
	}
	return nil
}

// ReadBlob reads a blob that d describes from r's repository, at most limit
// bytes.
func (c *Client) ReadBlob(ctx context.Context, r Ref, d ocispec.Descriptor, limit int64) ([]byte, error) {
	if d.Size < 0 || d.Size > limit {
		return nil, fmt.Errorf("%s@%s is %d bytes, more than %d", r.Name(), d.Digest, d.Size, limit)
	}
	repo, err := c.Repository(r)
	if err != nil {
		return nil, err
	}
	b, err := content.FetchAll(ctx, repo, d)
	if err != nil {
		return nil, fmt.Errorf("%s@%s: %w", r.Name(), d.Digest, err)
	}
	return b, nil
}

// Manifest is a manifest, as fetched.
type Manifest struct {
	ocispec.Manifest
	Descriptor ocispec.Descriptor
	Raw        []byte
}

// FetchManifest fetches the image manifest r's tag or digest names.
func (c *Client) FetchManifest(ctx context.Context, r Ref) (*Manifest, error) {
	repo, err := c.Repository(r)
	if err != nil {
		return nil, err
	}
	repo.ManifestMediaTypes = []string{ocispec.MediaTypeImageManifest}
	desc, rc, err := repo.FetchReference(ctx, r.Reference())
	if err != nil {
		return nil, fmt.Errorf("%s: %w", r, err)
	}
	defer func() { _ = rc.Close() }()
	if desc.Size > MaxManifest {
		return nil, fmt.Errorf("%s's manifest is %d bytes, more than %d", r, desc.Size, MaxManifest)
	}
	raw, err := content.ReadAll(rc, desc)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", r, err)
	}
	m := &Manifest{Descriptor: desc, Raw: raw}
	if err := json.Unmarshal(raw, &m.Manifest); err != nil {
		return nil, fmt.Errorf("%s: %w", r, err)
	}
	if m.MediaType != "" && m.MediaType != ocispec.MediaTypeImageManifest {
		return nil, fmt.Errorf("%s is a %s, not an image manifest", r, m.MediaType)
	}
	return m, nil
}

// Resolve returns the digest r's tag names, with one request.
func (c *Client) Resolve(ctx context.Context, r Ref) (string, error) {
	repo, err := c.Repository(r)
	if err != nil {
		return "", err
	}
	desc, err := repo.Resolve(ctx, r.Reference())
	if err != nil {
		return "", fmt.Errorf("%s: %w", r, err)
	}
	return desc.Digest.String(), nil
}

// Tags lists r's repository's tags.
func (c *Client) Tags(ctx context.Context, r Ref) ([]string, error) {
	repo, err := c.Repository(r)
	if err != nil {
		return nil, err
	}
	var out []string
	err = repo.Tags(ctx, "", func(tags []string) error {
		out = append(out, tags...)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", r, err)
	}
	return out, nil
}

// NotFound reports whether err is a registry's 404, for a repository, tag,
// manifest or blob.
func NotFound(err error) bool {
	if errors.Is(err, errdef.ErrNotFound) {
		return true
	}
	var resp *errcode.ErrorResponse
	return errors.As(err, &resp) && resp.StatusCode == http.StatusNotFound
}

// Image manifests' media types, for images' tags.
var imageTypes = []string{
	ocispec.MediaTypeImageIndex,
	"application/vnd.docker.distribution.manifest.list.v2+json",
	ocispec.MediaTypeImageManifest,
	"application/vnd.docker.distribution.manifest.v2+json",
}

// ResolveImage returns the digest of the image r's tag names, and the
// platforms it holds.
func (c *Client) ResolveImage(ctx context.Context, r Ref) (string, []string, error) {
	repo, err := c.Repository(r)
	if err != nil {
		return "", nil, err
	}
	repo.ManifestMediaTypes = imageTypes
	desc, rc, err := repo.FetchReference(ctx, r.Reference())
	if err != nil {
		return "", nil, fmt.Errorf("image %s: %w", r.Name(), err)
	}
	defer func() { _ = rc.Close() }()
	if desc.Size > MaxManifest {
		return "", nil, fmt.Errorf("image %s's manifest is %d bytes", r.Name(), desc.Size)
	}
	body, err := content.ReadAll(rc, desc)
	if err != nil {
		return "", nil, fmt.Errorf("image %s: %w", r.Name(), err)
	}
	var m struct {
		MediaType string `json:"mediaType"`
		Manifests []struct {
			Platform *ocispec.Platform `json:"platform"`
		} `json:"manifests"`
		Config ocispec.Descriptor `json:"config"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		return "", nil, fmt.Errorf("image %s: %w", r.Name(), err)
	}
	mediaType := desc.MediaType
	if mediaType == "" {
		mediaType = m.MediaType
	}
	var platforms []string
	switch mediaType {
	case imageTypes[0], imageTypes[1]:
		for _, d := range m.Manifests {
			if p := d.Platform; p != nil && p.OS != "unknown" && p.OS != "" {
				platforms = append(platforms, p.OS+"/"+p.Architecture)
			}
		}
	case imageTypes[2], imageTypes[3]:
		cfg, err := c.ReadBlob(ctx, r, m.Config, MaxManifest)
		if err != nil {
			return "", nil, fmt.Errorf("image %s's config: %w", r.Name(), err)
		}
		var p ocispec.Platform
		if err := json.Unmarshal(cfg, &p); err != nil || p.OS == "" {
			return "", nil, fmt.Errorf("image %s's config names no platform", r.Name())
		}
		platforms = []string{p.OS + "/" + p.Architecture}
	default:
		return "", nil, fmt.Errorf("image %s is a %q, not an image", r.Name(), mediaType)
	}
	slices.Sort(platforms)
	return desc.Digest.String(), slices.Compact(platforms), nil
}

// DockerCredentials reads credentials from Docker's config and its
// credential helpers, as docker login and oras login write them.
func DockerCredentials() (Credentials, error) {
	store, err := credentials.NewStoreFromDocker(credentials.StoreOptions{})
	if err != nil {
		return nil, err
	}
	get := credentials.Credential(store)
	return func(ctx context.Context, r Ref) (Credential, error) {
		c, err := get(ctx, r.Registry)
		if err != nil {
			return Credential{}, err
		}
		return Credential{Username: c.Username, Password: c.Password, RefreshToken: c.RefreshToken}, nil
	}, nil
}
