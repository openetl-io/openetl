// Package pkgref fetches the connector version a reference names:
// oci://REGISTRY/REPOSITORY:VERSION or @sha256:…, or
// github.com/OWNER/REPO[/PACKAGE]@VERSION. The API resolves them for Apply,
// and the CLI for the commands that run a connector.
package pkgref

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/openetl-io/openetl/internal/github"
	"github.com/openetl-io/openetl/internal/oci"
	"github.com/openetl-io/openetl/internal/spec"
)

// OfficialURL is where the official connectors are published.
const OfficialURL = "oci://ghcr.io/openetl-io/openetl-connectors"

// OfficialRepository is the official connectors' GitHub repository.
const OfficialRepository = "https://github.com/openetl-io/openetl-connectors"

// Is reports whether s is a reference rather than a path.
func Is(s string) bool {
	return strings.HasPrefix(s, oci.Scheme) || strings.HasPrefix(s, "github.com/")
}

// Covers reports whether a source's URL covers url: url is it, or below it.
func Covers(source, url string) bool {
	return url == source || strings.HasPrefix(url, source+"/") || strings.HasPrefix(url, source+":") || strings.HasPrefix(url, source+"@")
}

// Resolved is a connector version a reference names.
type Resolved struct {
	// The descriptor, YAML or JSON.
	Content []byte
	// The manifest's oci:// URL by digest, or the release's page.
	Provenance string
}

// Resolver fetches what references name.
type Resolver struct {
	OCI *oci.Client
	// A client for repository's releases, an https://github.com/OWNER/REPO.
	GitHub func(ctx context.Context, repository string) (*github.Client, error)
	// Where official packages may come from. Empty: OfficialURL and
	// OfficialRepository.
	Official []string
}

// Resolve fetches the connector version reference names. A package under
// the reserved openetl owner must come from where the official packages are
// published.
func (r Resolver) Resolve(ctx context.Context, reference string) (*Resolved, error) {
	var out *Resolved
	var where string
	switch {
	case strings.HasPrefix(reference, oci.Scheme):
		ref, err := oci.Parse(reference)
		if err != nil {
			return nil, err
		}
		if ref.Reference() == "" {
			return nil, fmt.Errorf("%s names no version: add :VERSION or @sha256:…", reference)
		}
		p, err := r.OCI.Pull(ctx, ref)
		if err != nil {
			return nil, err
		}
		where = ref.Repo().String()
		out = &Resolved{Content: p.Descriptor, Provenance: ref.Repo().At(p.Manifest.Descriptor.Digest.String()).String()}
	case strings.HasPrefix(reference, "github.com/"):
		ref, ok := github.ParseRef(reference)
		if !ok {
			return nil, fmt.Errorf("%q isn't github.com/OWNER/REPO[/PACKAGE]@VERSION", reference)
		}
		gh, err := r.GitHub(ctx, ref.Repository())
		if err != nil {
			return nil, err
		}
		rel, err := gh.ReleaseByTag(ctx, ref.Owner, ref.Repo, ref.Tag())
		if err != nil {
			return nil, err
		}
		a, err := descriptorAsset(rel, ref.Package)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", reference, err)
		}
		var buf bytes.Buffer
		if err := gh.Download(ctx, a, &buf, oci.MaxManifest); err != nil {
			return nil, err
		}
		where = ref.Repository()
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
	if owner, _ := spec.SplitName(doc.Name); owner == spec.OfficialOwner && !r.official(where) {
		return nil, fmt.Errorf("%s names %s, and the %s owner is reserved for the official connectors, which aren't published there", reference, doc.Name, spec.OfficialOwner)
	}
	return out, nil
}

// official reports whether url is one official packages may come from.
func (r Resolver) official(url string) bool {
	official := r.Official
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
