// Package pkgcatalog lists connected sources of connectors, OCI registry
// namespaces and GitHub repositories, into the catalog. It also resolves the
// references Apply takes, fetches logos and descriptors, and finds the
// credential for what's fetched under a source's URL.
package pkgcatalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/github"
	"github.com/openetl-io/openetl/internal/netguard"
	"github.com/openetl-io/openetl/internal/oci"
	"github.com/openetl-io/openetl/internal/secrets"
	"github.com/openetl-io/openetl/internal/spec"
	"github.com/openetl-io/openetl/internal/store/db"
)

// OfficialURL is where the official connectors are published.
const OfficialURL = "oci://ghcr.io/openetl-io/connectors"

// OfficialRepository is the official connectors' GitHub repository.
const OfficialRepository = "https://github.com/openetl-io/openetl-connectors"

// OfficialName is the official source's name.
const OfficialName = "official"

// MediaTypeDescriptor is a descriptor artifact's media type, as Apply stores
// it.
const MediaTypeDescriptor = connectorhost.MediaTypeDescriptor

// Catalog is an org's catalog and its sources.
type Catalog struct {
	Queries *db.Queries
	Org     uuid.UUID
	// Opens sources' secrets; nil: a source with a secret fails to list.
	Secrets *secrets.Keyring
	// Reaches registries, GitHub and logos; nil: a netguard client that
	// reaches public addresses only.
	HTTP *http.Client
	// Empty: GitHub's.
	GitHubAPI string
	// Registry hosts reached over plain HTTP, besides loopback ones.
	PlainHTTP []string
	// Where official packages may come from: the official source's URL and
	// repository. Empty: OfficialURL and OfficialRepository.
	Official []string
	Logger   *slog.Logger
}

func (c *Catalog) http() *http.Client {
	if c.HTTP == nil {
		return (&netguard.Policy{}).Client()
	}
	return c.HTTP
}

func (c *Catalog) log() *slog.Logger {
	if c.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return c.Logger
}

// Credential is a connected source's credential.
type Credential struct {
	// The source's name; empty when none covers the URL, or it has no
	// secret.
	Source   string
	Username string
	Password string
}

// Covers reports whether a source's URL covers url: url is it, or below it.
func Covers(source, url string) bool {
	return url == source || strings.HasPrefix(url, source+"/") || strings.HasPrefix(url, source+":") || strings.HasPrefix(url, source+"@")
}

// Credential returns the credential of the source with the longest URL that
// covers url.
func (c *Catalog) Credential(ctx context.Context, url string) (Credential, error) {
	sources, err := c.Queries.ListConnectorSources(ctx, c.Org)
	if err != nil {
		return Credential{}, err
	}
	var best *db.ListConnectorSourcesRow
	for i, s := range sources {
		if Covers(s.Url, url) && (best == nil || len(s.Url) > len(best.Url)) {
			best = &sources[i]
		}
	}
	if best == nil || best.Secret == "" {
		return Credential{}, nil
	}
	password, err := c.secret(ctx, best.Secret)
	if err != nil {
		return Credential{}, fmt.Errorf("connector source %s's secret: %w", best.Name, err)
	}
	return Credential{Source: best.Name, Username: best.Username, Password: password}, nil
}

// secret opens a secret by its resource name.
func (c *Catalog) secret(ctx context.Context, name string) (string, error) {
	ws, secret, ok := strings.Cut(strings.TrimPrefix(name, "workspaces/"), "/secrets/")
	if !ok {
		return "", fmt.Errorf("%q isn't a secret's name", name)
	}
	w, err := c.Queries.GetWorkspace(ctx, db.GetWorkspaceParams{OrgID: c.Org, Name: ws})
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("%s doesn't exist", name)
	}
	if err != nil {
		return "", err
	}
	r, err := c.Queries.GetSecret(ctx, db.GetSecretParams{OrgID: c.Org, WorkspaceID: w.ID, Name: secret})
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("%s doesn't exist", name)
	}
	if err != nil {
		return "", err
	}
	switch {
	case r.Ref != nil:
		return "", fmt.Errorf("%s is a reference that runners read, and the control plane needs its value", name)
	case c.Secrets == nil:
		return "", errors.New("this server has no secrets key (--secrets-key)")
	}
	v, err := c.Secrets.Open(ctx, c.Org, secrets.AAD(c.Org, w.ID, r.ID), r.Ciphertext)
	return string(v), err
}

// OCI is a registry client with the connected sources' credentials.
func (c *Catalog) OCI() *oci.Client {
	return &oci.Client{HTTP: c.http(), PlainHTTP: c.PlainHTTP, Credentials: func(ctx context.Context, r oci.Ref) (oci.Credential, error) {
		cred, err := c.Credential(ctx, r.String())
		return oci.Credential{Username: cred.Username, Password: cred.Password}, err
	}}
}

// GitHub is a GitHub client with the token of the source that covers
// repository, if any.
func (c *Catalog) GitHub(ctx context.Context, repository string) (*github.Client, error) {
	cred, err := c.Credential(ctx, repository)
	if err != nil {
		return nil, err
	}
	return &github.Client{HTTP: c.http(), API: c.GitHubAPI, Token: cred.Password}, nil
}

// official reports whether url is one official packages may come from.
func (c *Catalog) official(url string) bool {
	official := c.Official
	if len(official) == 0 {
		official = []string{OfficialURL, OfficialRepository}
	}
	for _, o := range official {
		if Covers(o, url) {
			return true
		}
	}
	return false
}

// Resolved is a connector version a reference names.
type Resolved struct {
	// The descriptor, YAML or JSON.
	Content []byte
	// The manifest's oci:// URL by digest, or the release's page.
	Provenance string
}

// Resolve fetches the connector version a reference names:
// oci://REGISTRY/REPOSITORY:VERSION or @sha256:…, or
// github.com/OWNER/REPO[/PACKAGE]@VERSION. A package under the reserved
// openetl owner must come from where the official packages are published.
func (c *Catalog) Resolve(ctx context.Context, reference string) (*Resolved, error) {
	var out *Resolved
	var where string
	switch {
	case strings.HasPrefix(reference, oci.Scheme):
		r, err := oci.Parse(reference)
		if err != nil {
			return nil, err
		}
		if r.Reference() == "" {
			return nil, fmt.Errorf("%s names no version: add :VERSION or @sha256:…", reference)
		}
		p, err := c.OCI().Pull(ctx, r)
		if err != nil {
			return nil, err
		}
		where = r.Repo().String()
		out = &Resolved{Content: p.Descriptor, Provenance: r.Repo().At(p.Manifest.Descriptor.Digest.String()).String()}
	case strings.HasPrefix(reference, "github.com/"):
		r, ok := github.ParseRef(reference)
		if !ok {
			return nil, fmt.Errorf("%q isn't github.com/OWNER/REPO[/PACKAGE]@VERSION", reference)
		}
		gh, err := c.GitHub(ctx, r.Repository())
		if err != nil {
			return nil, err
		}
		rel, err := gh.ReleaseByTag(ctx, r.Owner, r.Repo, r.Tag())
		if err != nil {
			return nil, err
		}
		a, err := descriptorAsset(rel, r.Package)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", reference, err)
		}
		var buf bytes.Buffer
		if err := gh.Download(ctx, a, &buf, oci.MaxManifest); err != nil {
			return nil, err
		}
		where = r.Repository()
		out = &Resolved{Content: buf.Bytes(), Provenance: rel.HTMLURL}
	default:
		return nil, fmt.Errorf("%q isn't an oci:// or github.com/ reference", reference)
	}
	doc, err := spec.Parse(out.Content)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", reference, err)
	}
	if doc.Kind != spec.KindConnector {
		return nil, fmt.Errorf("%s is a %s, not a Connector", reference, doc.Kind)
	}
	if owner, _ := spec.SplitName(doc.Name); owner == spec.OfficialOwner && !c.official(where) {
		return nil, fmt.Errorf("%s names %s, and the %s owner is reserved for the official connectors, which aren't published there", reference, doc.Name, spec.OfficialOwner)
	}
	return out, nil
}

// descriptorAsset is a release's descriptor: PACKAGE.connector.yaml, or its
// one *.connector.yaml when pkg is empty.
func descriptorAsset(rel *github.Release, pkg string) (github.Asset, error) {
	if pkg != "" {
		if a, ok := rel.Asset(pkg + ".connector.yaml"); ok {
			return a, nil
		}
		return github.Asset{}, fmt.Errorf("release %s has no %s.connector.yaml", rel.TagName, pkg)
	}
	var found []github.Asset
	for _, a := range rel.Assets {
		if strings.HasSuffix(a.Name, ".connector.yaml") {
			found = append(found, a)
		}
	}
	switch len(found) {
	case 1:
		return found[0], nil
	case 0:
		return github.Asset{}, fmt.Errorf("release %s has no *.connector.yaml", rel.TagName)
	}
	return github.Asset{}, fmt.Errorf("release %s has %d descriptors; name the package, github.com/OWNER/REPO/PACKAGE@VERSION", rel.TagName, len(found))
}

// Logo is a logo's content.
type Logo struct {
	MediaType string
	Content   []byte
}

// Digest is the logo's artifact digest.
func (l *Logo) Digest() string { return connectorhost.Digest(l.Content) }

// FetchLogo fetches a logo by its https or oci:// URL: SVG or PNG, at most
// oci.MaxLogo bytes.
func (c *Catalog) FetchLogo(ctx context.Context, url string) (*Logo, error) {
	var buf bytes.Buffer
	switch {
	case strings.HasPrefix(url, oci.Scheme):
		r, err := oci.Parse(url)
		if err != nil {
			return nil, err
		}
		if err := c.OCI().FetchBlob(ctx, r, &buf, oci.MaxLogo); err != nil {
			return nil, err
		}
	case strings.HasPrefix(url, "https://"):
		if err := c.download(ctx, url, &buf, oci.MaxLogo); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("logo %q isn't an https or oci:// URL", url)
	}
	t := oci.LogoType(url, buf.Bytes())
	if t == "" && bytes.Contains(bytes.ToLower(buf.Bytes()[:min(buf.Len(), 1024)]), []byte("<svg")) {
		t = "image/svg+xml"
	}
	if t == "" {
		return nil, fmt.Errorf("logo %s is neither an SVG nor a PNG image", url)
	}
	return &Logo{MediaType: t, Content: buf.Bytes()}, nil
}

// download fetches an https URL, as a release asset through GitHub's API
// when a source's token covers its repository.
func (c *Catalog) download(ctx context.Context, url string, w io.Writer, limit int64) error {
	if owner, repo, tag, name, ok := github.ParseAssetURL(url); ok {
		cred, err := c.Credential(ctx, url)
		if err != nil {
			return err
		}
		if cred.Password != "" {
			gh := &github.Client{HTTP: c.http(), API: c.GitHubAPI, Token: cred.Password}
			rel, err := gh.ReleaseByTag(ctx, owner, repo, tag)
			if err != nil {
				return err
			}
			a, ok := rel.Asset(name)
			if !ok {
				return fmt.Errorf("release %s has no asset %s", tag, name)
			}
			return gh.Download(ctx, a, w, limit)
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := c.http().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", url, resp.Status)
	}
	b, err := oci.ReadAllLimit(resp.Body, limit)
	if err != nil {
		return fmt.Errorf("%s: %w", url, err)
	}
	_, err = w.Write(b)
	return err
}

// StoreLogo keeps a logo as an artifact, and returns its digest.
func StoreLogo(ctx context.Context, q *db.Queries, org uuid.UUID, l *Logo) (string, error) {
	d := l.Digest()
	return d, q.InsertArtifact(ctx, db.InsertArtifactParams{OrgID: org, Digest: d, MediaType: l.MediaType, Content: l.Content})
}

// LogoPath is where the control plane serves a logo artifact.
func LogoPath(digest string) string { return "/logos/" + digest }

// ConnectOfficial connects the official source, once per org: an org that
// deleted it doesn't get it back. An empty url connects none.
func ConnectOfficial(ctx context.Context, q *db.Queries, org uuid.UUID, url string) error {
	if url == "" {
		return nil
	}
	return q.ConnectOfficialSource(ctx, db.ConnectOfficialSourceParams{OrgID: org, Name: OfficialName, Url: url})
}
