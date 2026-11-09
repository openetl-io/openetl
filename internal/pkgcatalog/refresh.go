package pkgcatalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/github"
	"github.com/openetl-io/openetl/internal/oci"
	"github.com/openetl-io/openetl/internal/spec"
	"github.com/openetl-io/openetl/internal/store/db"
)

// maxReleasePages bounds a GitHub repository's listing: 1,000 releases.
const maxReleasePages = 10

// Listed is what a source's listing did.
type Listed struct {
	Source   string
	Packages int
	// Empty when it listed.
	Error string
}

// Refresh lists every connected source into the catalog. A source that
// fails keeps its last listing, and records why.
func (c *Catalog) Refresh(ctx context.Context) ([]Listed, error) {
	sources, err := c.Queries.ListConnectorSources(ctx, c.Org)
	if err != nil {
		return nil, err
	}
	out := make([]Listed, 0, len(sources))
	for _, s := range sources {
		src := db.ConnectorSource{OrgID: s.OrgID, Name: s.Name, Url: s.Url, Username: s.Username, Secret: s.Secret, Official: s.Official}
		n, err := c.list(ctx, src)
		l := Listed{Source: s.Name, Packages: n}
		if err != nil {
			l.Error = err.Error()
			c.log().Warn("connector source failed to list", "source", s.Name, "url", s.Url, "err", err)
			if ferr := c.recordFailure(ctx, s.Name, s.Url, l.Error); ferr != nil {
				return out, ferr
			}
		} else if err := c.Queries.SourceListed(ctx, db.SourceListedParams{OrgID: c.Org, Name: s.Name, Url: s.Url}); err != nil {
			return out, err
		}
		out = append(out, l)
	}
	return out, nil
}

// recordFailure saves why a source failed to list. A listing that ran out of
// time ended ctx too, so this has a context of its own. One its caller gave
// up on says nothing about the source, and isn't saved.
func (c *Catalog) recordFailure(ctx context.Context, source, url, reason string) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil
	}
	save, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return c.Queries.SourceFailed(save, db.SourceFailedParams{OrgID: c.Org, Name: source, Url: url, Error: truncate(reason, 2000)})
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// list lists one source into the catalog, and returns how many packages it
// has.
func (c *Catalog) list(ctx context.Context, src db.ConnectorSource) (int, error) {
	rows, err := c.Queries.ListSourceCatalog(ctx, db.ListSourceCatalogParams{OrgID: c.Org, Source: src.Name})
	if err != nil {
		return 0, err
	}
	known := map[string]db.ConnectorCatalog{}
	for _, r := range rows {
		known[r.Key] = r
	}
	var pkgs []listedPackage
	if strings.HasPrefix(src.Url, oci.Scheme) {
		pkgs, err = c.listOCI(ctx, src, known)
	} else {
		pkgs, err = c.listGitHub(ctx, src, known)
	}
	if err != nil {
		return 0, err
	}
	keep := make([]string, 0, len(pkgs))
	for _, p := range pkgs {
		if slices.Contains(keep, p.row.Package) {
			continue
		}
		keep = append(keep, p.row.Package)
		r := p.row
		err := c.Queries.UpsertCatalogPackage(ctx, db.UpsertCatalogPackageParams{
			OrgID: c.Org, Source: src.Name, Package: r.Package, Key: r.Key, Title: r.Title, Version: r.Version, Author: r.Author,
			Description: r.Description, License: r.License, Repository: r.Repository, Docs: r.Docs, Roles: nonNil(r.Roles), Tags: nonNil(r.Tags),
			Reference: r.Reference, CardDigest: r.CardDigest, LogoDigest: r.LogoDigest,
		})
		if err != nil {
			return 0, err
		}
		if p.descriptor != nil {
			if err := c.storeDescriptor(ctx, src.Name, r.Package, r.CardDigest, p.descriptor); err != nil {
				return 0, err
			}
		}
	}
	return len(keep), c.Queries.DeleteCatalogPackages(ctx, db.DeleteCatalogPackagesParams{OrgID: c.Org, Source: src.Name, Keep: keep})
}

type listedPackage struct {
	row db.ConnectorCatalog
	// The descriptor, when the listing read it.
	descriptor *spec.Document
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func (c *Catalog) listOCI(ctx context.Context, src db.ConnectorSource, known map[string]db.ConnectorCatalog) ([]listedPackage, error) {
	ns, err := oci.Parse(src.Url)
	if err != nil {
		return nil, err
	}
	reg := c.OCI()
	cards := map[string]oci.Card{}
	for k, r := range known {
		cards[k] = oci.Card{Name: r.Package, Title: r.Title, Version: r.Version, Author: r.Author, Description: r.Description,
			License: r.License, Source: r.Repository, Documentation: r.Docs, Roles: r.Roles, Tags: r.Tags,
			Repository: strings.TrimSuffix(r.Reference, ":"+r.Version), Digest: r.CardDigest}
	}
	indexed, err := reg.ListIndex(ctx, ns, cards)
	if err != nil {
		return nil, err
	}
	in := map[string]bool{}
	for _, card := range indexed {
		_, short := spec.SplitName(card.Name)
		in[short] = true
	}
	all := append(indexed, reg.ListUnindexed(ctx, ns, in, cards)...)
	var out []listedPackage
	for _, card := range all {
		_, short := spec.SplitName(card.Name)
		repo, err := oci.Parse(card.Repository)
		if err != nil || !ns.Covers(repo) || !spec.ValidVersion(card.Version) {
			continue
		}
		r := db.ConnectorCatalog{
			Package: spec.FullName(card.Name), Key: short, Title: cmpOr(card.Title, short), Version: card.Version, Author: card.Author,
			Description: card.Description, License: card.License, Repository: card.Source, Docs: card.Documentation,
			Roles: card.Roles, Tags: card.Tags, Reference: repo.Repo().String() + ":" + card.Version, CardDigest: card.Digest,
		}
		if k, ok := known[short]; ok && k.CardDigest == card.Digest {
			r.LogoDigest = k.LogoDigest
		} else if card.Logo != nil {
			// An index card's logo is in the index; an unindexed package's,
			// in its own repository.
			from := oci.Index(ns)
			if !in[short] {
				from = repo
			}
			if d, err := c.ociLogo(ctx, from, *card.Logo); err != nil {
				c.log().Warn("a catalog logo failed", "package", r.Package, "err", err)
			} else {
				r.LogoDigest = &d
			}
		}
		out = append(out, listedPackage{row: r})
	}
	return out, nil
}

// ociLogo stores a logo blob, unless it's stored already.
func (c *Catalog) ociLogo(ctx context.Context, repo oci.Ref, d ocispec.Descriptor) (string, error) {
	digest := d.Digest.String()
	if _, err := c.Queries.GetLogo(ctx, db.GetLogoParams{OrgID: c.Org, Digest: digest}); err == nil {
		return digest, nil
	}
	b, err := c.OCI().ReadBlob(ctx, repo, d, oci.MaxLogo)
	if err != nil {
		return "", err
	}
	name := "logo.png"
	if d.MediaType == "image/svg+xml" {
		name = "logo.svg"
	}
	if t := oci.LogoType(name, b); t == "" || t != d.MediaType {
		return "", fmt.Errorf("%s isn't the %s it says", digest, d.MediaType)
	}
	return StoreLogo(ctx, c.Queries, c.Org, &Logo{MediaType: d.MediaType, Content: b})
}

func (c *Catalog) listGitHub(ctx context.Context, src db.ConnectorSource, known map[string]db.ConnectorCatalog) ([]listedPackage, error) {
	owner, repo, ok := github.ParseRepo(src.Url)
	if !ok {
		return nil, fmt.Errorf("%s isn't https://github.com/OWNER/REPO", src.Url)
	}
	gh, err := c.GitHub(ctx, src.Url)
	if err != nil {
		return nil, err
	}
	releases, err := gh.Releases(ctx, owner, repo, maxReleasePages)
	if err != nil {
		return nil, err
	}
	type best struct {
		version, pkg string
		rel          github.Release
		asset        github.Asset
	}
	found := map[string]best{}
	for _, rel := range releases {
		if rel.Draft {
			continue
		}
		for _, a := range rel.Assets {
			short, ok := strings.CutSuffix(a.Name, ".connector.yaml")
			if !ok || !spec.ValidName(short) {
				continue
			}
			pkg, version := short, ""
			if v, ok := strings.CutPrefix(rel.TagName, short+"/v"); ok {
				version = v
			} else if v, ok := strings.CutPrefix(rel.TagName, "v"); ok {
				version, pkg = v, ""
			}
			if !spec.ValidVersion(version) {
				continue
			}
			if b, ok := found[short]; !ok || version != b.version && oci.Newer(version, b.version) {
				found[short] = best{version: version, pkg: pkg, rel: rel, asset: a}
			}
		}
	}
	var out []listedPackage
	for _, short := range slices.Sorted(maps.Keys(found)) {
		b := found[short]
		digest := fmt.Sprintf("github:%d:%s", b.asset.ID, b.asset.UpdatedAt)
		if k, ok := known[short]; ok && k.CardDigest == digest {
			out = append(out, listedPackage{row: k})
			continue
		}
		var buf bytes.Buffer
		if err := gh.Download(ctx, b.asset, &buf, oci.MaxManifest); err != nil {
			return nil, err
		}
		doc, err := spec.Parse(buf.Bytes())
		if err != nil || doc.Kind != spec.KindConnector {
			c.log().Warn("a release's descriptor doesn't parse", "source", src.Name, "asset", b.asset.Name, "tag", b.rel.TagName, "err", err)
			continue
		}
		if _, s := spec.SplitName(doc.Name); s != short || doc.Version != b.version {
			c.log().Warn("a release's descriptor names another package or version", "source", src.Name, "asset", b.asset.Name, "name", doc.Name, "version", doc.Version)
			continue
		}
		ref := github.Ref{Owner: owner, Repo: repo, Package: b.pkg, Version: b.version}
		i := doc.Info
		r := db.ConnectorCatalog{
			Package: doc.Name, Key: short, Title: cmpOr(i.Title, short), Version: doc.Version, Author: i.Author,
			Description: i.Description, License: i.License, Repository: i.Repository, Docs: i.Docs,
			Roles: doc.Descriptor.Kinds(), Tags: i.Tags, Reference: ref.String(), CardDigest: digest,
		}
		if i.Logo != "" {
			if l, err := c.FetchLogo(ctx, i.Logo); err != nil {
				c.log().Warn("a catalog logo failed", "package", r.Package, "err", err)
			} else if d, err := StoreLogo(ctx, c.Queries, c.Org, l); err == nil {
				r.LogoDigest = &d
			}
		}
		out = append(out, listedPackage{row: r, descriptor: doc})
	}
	return out, nil
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// storeDescriptor keeps a package's descriptor as an artifact, as Apply
// stores it, for the card it was listed with.
func (c *Catalog) storeDescriptor(ctx context.Context, source, pkg, card string, doc *spec.Document) error {
	content, err := spec.EncodeVersion(doc)
	if err != nil {
		return err
	}
	d := connectorhost.Digest(content)
	if err := c.Queries.InsertArtifact(ctx, db.InsertArtifactParams{OrgID: c.Org, Digest: d, MediaType: MediaTypeDescriptor, Content: content}); err != nil {
		return err
	}
	return c.Queries.SetCatalogDescriptor(ctx, db.SetCatalogDescriptorParams{OrgID: c.Org, Source: source, Package: pkg, CardDigest: card, DescriptorDigest: &d})
}

// Descriptor returns a catalog package's highest version's descriptor,
// fetching it the first time.
func (c *Catalog) Descriptor(ctx context.Context, row db.GetCatalogPackageRow) (*spec.Document, error) {
	if row.DescriptorDigest != nil {
		a, err := c.Queries.GetArtifact(ctx, db.GetArtifactParams{OrgID: c.Org, Digest: *row.DescriptorDigest})
		if err == nil {
			return spec.Parse(a.Content)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
	}
	res, err := c.Resolve(ctx, row.Reference)
	if err != nil {
		return nil, err
	}
	doc, err := spec.Parse(res.Content)
	if err != nil {
		return nil, err
	}
	if doc.Name != row.Package || doc.Version != row.Version {
		return nil, fmt.Errorf("%s is %s %s, not %s %s as listed", row.Reference, doc.Name, doc.Version, row.Package, row.Version)
	}
	return doc, c.storeDescriptor(ctx, row.Source, row.Package, row.CardDigest, doc)
}
