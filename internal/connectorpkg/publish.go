package connectorpkg

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/oci"
	"github.com/openetl-io/openetl/internal/spec"
)

// Published is a package version pushed to a registry.
type Published struct {
	// owner/name.
	Name    string
	Version string
	// The version's manifest, by digest.
	Ref oci.Ref
	// Whether the namespace's index now lists this version.
	Indexed bool
}

// Publish pushes the package version whose descriptor Package wrote to
// descriptor, with the executables beside it and its logo, which is downloaded
// if it's an https URL, to ns, an
// oci://REGISTRY/NAMESPACE: as NAMESPACE/NAME, tagged with its version, and
// in the namespace's index when it's the package's highest version.
func Publish(ctx context.Context, c *oci.Client, ns oci.Ref, descriptor string) (*Published, error) {
	b, err := os.ReadFile(filepath.Clean(descriptor))
	if err != nil {
		return nil, err
	}
	doc, err := spec.Parse(b)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", descriptor, err)
	}
	if doc.Kind != spec.KindConnector || doc.APIVersion != spec.APIVersionConnector {
		return nil, fmt.Errorf("%s isn't a v1alpha2 Connector descriptor", descriptor)
	}
	_, short := spec.SplitName(doc.Name)
	repo := ns.Child(short)
	dir := filepath.Dir(descriptor)
	v := oci.Version{Name: doc.Name, Version: doc.Version}
	for _, role := range doc.Descriptor.Kinds() {
		r := doc.Descriptor.Roles[role]
		for _, p := range slices.Sorted(maps.Keys(r.Binaries)) {
			bin := r.Binaries[p]
			path := filepath.Join(dir, Asset(short, role, p))
			content, err := readBelow(dir, filepath.Base(path))
			if err != nil {
				return nil, fmt.Errorf("the %s role's executable for %s: %w; publish takes what openetl connector package wrote", role, p, err)
			}
			if d := connectorhost.Digest(content); d != bin.Digest {
				return nil, fmt.Errorf("%s has digest %s, and the descriptor says %s", path, d, bin.Digest)
			}
			v.Binaries = append(v.Binaries, oci.Blob{MediaType: oci.MediaTypeBinary, Data: content, Annotations: map[string]string{
				ocispec.AnnotationTitle: filepath.Base(path), oci.AnnotationRole: role, oci.AnnotationPlatform: p,
			}})
			bin.URL = oci.BlobURL(repo, content)
			r.Binaries[p] = bin
		}
	}
	var logo *oci.Blob
	if doc.Info.Logo != "" && !strings.HasPrefix(doc.Info.Logo, oci.Scheme) {
		if logo, err = packageLogo(ctx, c, dir, short, doc.Info.Logo); err != nil {
			return nil, fmt.Errorf("%s: %w", descriptor, err)
		}
		doc.Info.Logo = oci.BlobURL(repo, logo.Data)
	}
	if v.Descriptor, err = spec.EncodeDescriptor(doc.Name, doc.Version, doc.Info, doc.Descriptor); err != nil {
		return nil, err
	}
	card := Card(doc, repo)
	v.Logo, v.Annotations = logo, card.Annotations()
	delete(v.Annotations, oci.AnnotationRepository)
	m, err := c.Push(ctx, repo, v)
	if err != nil {
		return nil, err
	}
	out := &Published{Name: doc.Name, Version: doc.Version, Ref: repo.At(m.Digest.String())}
	if out.Indexed, err = c.UpdateIndex(ctx, ns, card, logo); err != nil {
		return out, fmt.Errorf("%s %s is pushed, and the index isn't updated: %w", doc.Name, doc.Version, err)
	}
	return out, nil
}

// packageLogo is a version's logo as a blob of its package: the file Package
// wrote beside the descriptor, or the image at its https URL, so that catalogs
// and private installs show it without reaching that site.
func packageLogo(ctx context.Context, c *oci.Client, dir, short, url string) (*oci.Blob, error) {
	blob := func(mediaType, name string, content []byte) *oci.Blob {
		return &oci.Blob{MediaType: mediaType, Data: content, Annotations: map[string]string{oci.AnnotationLogo: "", ocispec.AnnotationTitle: name}}
	}
	for _, t := range oci.LogoTypes {
		name := LogoAsset(short, t)
		if content, err := readBelow(dir, name); err == nil && strings.HasSuffix(url, "/"+name) {
			return blob(t, name, content), nil
		}
	}
	content, t, err := c.FetchLogo(ctx, url)
	if err != nil {
		return nil, err
	}
	return blob(t, LogoAsset(short, t), content), nil
}

// Card is a version's card in its namespace's index.
func Card(doc *spec.Document, repo oci.Ref) oci.Card {
	_, short := spec.SplitName(doc.Name)
	i := doc.Info
	return oci.Card{
		Name: doc.Name, Title: cmpOr(i.Title, short), Version: doc.Version, Author: i.Author, Description: i.Description,
		License: i.License, Source: i.Repository, Documentation: i.Docs, Roles: doc.Descriptor.Kinds(), Tags: i.Tags,
		Repository: repo.String(),
	}
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
