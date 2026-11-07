package connectorpkg

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/openetl-io/openetl/internal/oci"
	"github.com/openetl-io/openetl/internal/spec"
)

// Finding is how one check of an artifact went.
type Finding struct {
	Check string
	// What's wrong; empty when the check passed.
	Problems []string
}

// Artifact checks names, in the order CheckArtifact reports them.
const (
	CheckArtifactType = "artifact type"
	CheckConfig       = "config"
	CheckDescriptor   = "descriptor"
	CheckName         = "name and version"
	CheckBinaries     = "binaries"
	CheckLogo         = "logo"
	CheckAnnotations  = "annotations"
	CheckIndex        = "index"
)

// CheckArtifact checks the package version r names, by tag or digest, as
// Publish pushes one: its artifact and config types, the descriptor as its
// config, a layer for each binary and the logo, the annotations, and the
// namespace's index. It fails only when the manifest can't be read.
func CheckArtifact(ctx context.Context, c *oci.Client, r oci.Ref) ([]Finding, error) {
	if r.Reference() == "" {
		return nil, fmt.Errorf("%s names no version: add :VERSION or @sha256:…", r)
	}
	m, err := c.FetchManifest(ctx, r)
	if err != nil {
		return nil, err
	}
	a := &artifactCheck{ctx: ctx, c: c, r: r, repo: r.Repo(), m: m}
	a.check(CheckArtifactType, func(p *problems) {
		if m.ArtifactType != oci.ArtifactType {
			p.add("the manifest's artifact type is %q, not %q", m.ArtifactType, oci.ArtifactType)
		}
	})
	a.check(CheckConfig, func(p *problems) {
		if m.Config.MediaType != oci.MediaTypeDescriptor {
			p.add("the config's media type is %q, not %q", m.Config.MediaType, oci.MediaTypeDescriptor)
		}
	})
	a.check(CheckDescriptor, a.descriptor)
	if a.doc == nil {
		return a.out, nil
	}
	a.check(CheckName, a.nameAndVersion)
	a.check(CheckBinaries, a.binaries)
	a.check(CheckLogo, a.logo)
	a.check(CheckAnnotations, a.annotations)
	a.check(CheckIndex, a.index)
	return a.out, nil
}

type problems []string

func (p *problems) add(format string, args ...any) { *p = append(*p, fmt.Sprintf(format, args...)) }

type artifactCheck struct {
	ctx  context.Context
	c    *oci.Client
	r    oci.Ref
	repo oci.Ref
	m    *oci.Manifest
	doc  *spec.Document
	out  []Finding
}

func (a *artifactCheck) check(name string, f func(*problems)) {
	var p problems
	f(&p)
	a.out = append(a.out, Finding{Check: name, Problems: p})
}

// descriptor: the config is a v1alpha2 Connector descriptor, as canonical
// JSON.
func (a *artifactCheck) descriptor(p *problems) {
	b, err := a.c.ReadBlob(a.ctx, a.repo, a.m.Config, oci.MaxManifest)
	if err != nil {
		p.add("the config: %v", err)
		return
	}
	doc, err := spec.Parse(b)
	switch {
	case err != nil:
		p.add("the config isn't a descriptor: %v", err)
		return
	case doc.Kind != spec.KindConnector || doc.APIVersion != spec.APIVersionConnector:
		p.add("the config is a %s %s, not a %s Connector", doc.APIVersion, doc.Kind, spec.APIVersionConnector)
		return
	}
	a.doc = doc
	if enc, err := spec.EncodeDescriptor(doc.Name, doc.Version, doc.Info, doc.Descriptor); err != nil {
		p.add("the descriptor doesn't encode: %v", err)
	} else if !bytes.Equal(enc, b) {
		p.add("the config isn't the descriptor's canonical JSON, which a version's digest is taken of")
	}
}

// nameAndVersion: the repository is the package's, and the version's tag
// names this manifest.
func (a *artifactCheck) nameAndVersion(p *problems) {
	_, short := spec.SplitName(a.doc.Name)
	if last := path.Base(a.repo.Repository); last != short {
		p.add("the package is %s, and its repository ends in %s, not %s", a.doc.Name, last, short)
	}
	if a.r.Tag != "" && a.r.Tag != a.doc.Version {
		p.add("the tag is %s, and the descriptor's version %s", a.r.Tag, a.doc.Version)
	}
	switch d, err := a.c.Resolve(a.ctx, a.repo.At(a.doc.Version)); {
	case err != nil:
		p.add("tag %s: %v", a.doc.Version, err)
	case d != a.m.Descriptor.Digest.String():
		p.add("tag %s names %s, not this manifest, %s", a.doc.Version, d, a.m.Descriptor.Digest)
	}
}

// binaries: each of the descriptor's binaries is a layer of this
// repository, by its digest, annotated with its role and platform; and every
// binary layer is one of them.
func (a *artifactCheck) binaries(p *problems) {
	named := map[string]bool{}
	for _, role := range a.doc.Descriptor.Kinds() {
		r := a.doc.Descriptor.Roles[role]
		for _, platform := range slices.Sorted(maps.Keys(r.Binaries)) {
			b := r.Binaries[platform]
			what := fmt.Sprintf("the %s role's binary for %s", role, platform)
			named[b.Digest] = true
			u, err := oci.Parse(b.URL)
			switch {
			case err != nil:
				p.add("%s is at %s, not an oci:// URL", what, b.URL)
			case u.Repo() != a.repo || u.Digest != b.Digest:
				p.add("%s is at %s, not in %s by its digest %s", what, b.URL, a.repo, b.Digest)
			}
			l, ok := a.layer(b.Digest)
			if !ok {
				p.add("%s, %s, has no layer", what, b.Digest)
				continue
			}
			if l.MediaType != oci.MediaTypeBinary {
				p.add("%s's layer is a %q, not %q", what, l.MediaType, oci.MediaTypeBinary)
			}
			if l.Annotations[oci.AnnotationRole] != role || l.Annotations[oci.AnnotationPlatform] != platform {
				p.add("%s's layer is annotated as the %q role's for %q", what, l.Annotations[oci.AnnotationRole], l.Annotations[oci.AnnotationPlatform])
			}
			a.exists(p, what, l)
		}
	}
	for _, l := range a.m.Layers {
		if l.MediaType == oci.MediaTypeBinary && !named[l.Digest.String()] {
			p.add("binary layer %s isn't a binary the descriptor names", l.Digest)
		}
	}
}

// logo: a logo the descriptor names by an oci:// URL is a layer of this
// repository, an SVG or PNG; and a logo layer is the one it names.
func (a *artifactCheck) logo(p *problems) {
	logo := a.doc.Info.Logo
	want := ""
	if strings.HasPrefix(logo, oci.Scheme) {
		u, err := oci.Parse(logo)
		if err != nil || u.Repo() != a.repo || u.Digest == "" {
			p.add("the logo is at %s, not in %s by its digest", logo, a.repo)
		} else {
			want = u.Digest
			l, ok := a.layer(u.Digest)
			switch {
			case !ok:
				p.add("the logo, %s, has no layer", u.Digest)
			case !slices.Contains(oci.LogoTypes, l.MediaType):
				p.add("the logo's layer is a %q, not one of %s", l.MediaType, strings.Join(oci.LogoTypes, ", "))
			default:
				a.exists(p, "the logo", l)
			}
		}
	}
	for _, l := range a.m.Layers {
		if _, ok := l.Annotations[oci.AnnotationLogo]; ok && l.Digest.String() != want {
			p.add("logo layer %s isn't the logo the descriptor names", l.Digest)
		}
	}
}

// annotations: the manifest's are the metadata's, as Publish writes them.
func (a *artifactCheck) annotations(p *problems) {
	want := Card(a.doc, a.repo).Annotations()
	delete(want, oci.AnnotationRepository)
	ours := oci.Card{Name: "x", Title: "x", Version: "x", Author: "x", Description: "x", License: "x", Source: "x", Documentation: "x",
		Roles: []string{"x"}, Tags: []string{"x"}}.Annotations()
	delete(ours, oci.AnnotationRepository)
	for _, k := range slices.Sorted(maps.Keys(ours)) {
		got, has := a.m.Annotations[k]
		w, wanted := want[k]
		switch {
		case wanted && !has:
			p.add("annotation %s is missing; the metadata says %q", k, w)
		case wanted && got != w:
			p.add("annotation %s is %q; the metadata says %q", k, got, w)
		case !wanted && has:
			p.add("annotation %s is %q; the metadata has none", k, got)
		}
	}
}

// index: the namespace's index lists the package, at this version or a
// higher one.
func (a *artifactCheck) index(p *problems) {
	dir := path.Dir(a.repo.Repository)
	if dir == "." {
		p.add("the repository %s isn't below a namespace, which an index is in", a.repo)
		return
	}
	ns := oci.Ref{Registry: a.repo.Registry, Repository: dir}
	_, short := spec.SplitName(a.doc.Name)
	tag := oci.Index(ns).At(short)
	m, err := a.c.FetchManifest(a.ctx, tag)
	if err != nil {
		p.add("the index has no entry for %s: %v", short, err)
		return
	}
	if m.ArtifactType != oci.IndexArtifactType {
		p.add("%s is a %q, not an index card", tag, m.ArtifactType)
		return
	}
	card := oci.CardOf(m)
	switch {
	case card.Name != a.doc.Name:
		p.add("%s lists %s, not %s", tag, card.Name, a.doc.Name)
	case card.Version != a.doc.Version && oci.Newer(a.doc.Version, card.Version):
		p.add("%s lists version %s, below this one, %s", tag, card.Version, a.doc.Version)
	}
}

func (a *artifactCheck) layer(digest string) (ocispec.Descriptor, bool) {
	for _, l := range a.m.Layers {
		if l.Digest.String() == digest {
			return l, true
		}
	}
	return ocispec.Descriptor{}, false
}

// exists checks that the registry has a layer's blob, without fetching it.
func (a *artifactCheck) exists(p *problems, what string, l ocispec.Descriptor) {
	rr, err := a.c.Repository(a.repo)
	if err == nil {
		var ok bool
		if ok, err = rr.Blobs().Exists(a.ctx, l); err == nil && !ok {
			p.add("%s's blob, %s, isn't in the registry", what, l.Digest)
		}
	}
	if err != nil {
		p.add("%s's blob, %s: %v", what, l.Digest, err)
	}
}
