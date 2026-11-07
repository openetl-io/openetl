package connectorpkg

import (
	"io"
	"log"
	"maps"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ggcr "github.com/google/go-containerregistry/pkg/registry"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"

	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/oci"
)

func TestCheckArtifact(t *testing.T) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	file := "name: acme/files\ntitle: Files\nrepository: https://github.com/acme/files\nlogo: ./logo.svg\nroles:\n  destination:\n    build:\n" +
		"      command: cd " + repoRoot + " && go build -o " + dir + "/bin/files-$(go env GOOS)-$(go env GOARCH) ./internal/connectorhost/external/testconnector\n" +
		"      output: bin/files-{os}-{arch}\n"
	if err := os.WriteFile(filepath.Join(dir, FileName), []byte(file), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "logo.svg"), []byte(testLogo), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	res, err := Package(t.Context(), f, Options{Version: "2.0.0", Out: filepath.Join(dir, "dist"), Platforms: []string{connectorhost.Platform()}})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(ggcr.New(ggcr.Logger(log.New(io.Discard, "", 0))))
	defer srv.Close()
	c := &oci.Client{HTTP: srv.Client()}
	ns, err := oci.Parse(oci.Scheme + strings.TrimPrefix(srv.URL, "http://") + "/acme/connectors")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Publish(t.Context(), c, ns, res.Descriptor); err != nil {
		t.Fatal(err)
	}
	repo := ns.Child("files")

	problems := func(r oci.Ref) map[string][]string {
		t.Helper()
		fs, err := CheckArtifact(t.Context(), c, r)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string][]string{}
		for _, f := range fs {
			if len(f.Problems) > 0 {
				out[f.Check] = f.Problems
			}
		}
		return out
	}
	want := func(r oci.Ref, check, substr string) {
		t.Helper()
		ps := problems(r)
		if !strings.Contains(strings.Join(ps[check], "\n"), substr) {
			t.Errorf("%s: the %s check found %q, not %q; all: %v", r, check, ps[check], substr, ps)
		}
	}

	if ps := problems(repo.At("2.0.0")); len(ps) != 0 {
		t.Errorf("a published version: %v", ps)
	}
	d, err := c.Resolve(t.Context(), repo.At("2.0.0"))
	if err != nil {
		t.Fatal(err)
	}
	if ps := problems(repo.At(d)); len(ps) != 0 {
		t.Errorf("a published version by digest: %v", ps)
	}

	m, err := c.FetchManifest(t.Context(), repo.At("2.0.0"))
	if err != nil {
		t.Fatal(err)
	}
	rr, err := c.Repository(repo)
	if err != nil {
		t.Fatal(err)
	}
	// repack tags a changed copy of the published manifest.
	repack := func(tag string, change func(cfg *ocispec.Descriptor, layers *[]ocispec.Descriptor, ann map[string]string) string) oci.Ref {
		t.Helper()
		cfg, layers, ann := m.Config, append([]ocispec.Descriptor{}, m.Layers...), maps.Clone(m.Annotations)
		artifactType := change(&cfg, &layers, ann)
		desc, err := oras.PackManifest(t.Context(), rr, oras.PackManifestVersion1_1, artifactType, oras.PackManifestOptions{
			ConfigDescriptor: &cfg, Layers: layers, ManifestAnnotations: ann,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := rr.Tag(t.Context(), desc, tag); err != nil {
			t.Fatal(err)
		}
		return repo.At(tag)
	}
	want(repack("wrong-type", func(*ocispec.Descriptor, *[]ocispec.Descriptor, map[string]string) string {
		return "application/vnd.acme"
	}),
		CheckArtifactType, `not "application/vnd.openetl.connector.v1"`)
	want(repack("wrong-config", func(cfg *ocispec.Descriptor, _ *[]ocispec.Descriptor, _ map[string]string) string {
		cfg.MediaType = "application/json"
		return oci.ArtifactType
	}), CheckConfig, `"application/json"`)
	want(repack("no-binary", func(_ *ocispec.Descriptor, layers *[]ocispec.Descriptor, _ map[string]string) string {
		*layers = slicesDelete(*layers, oci.MediaTypeBinary)
		return oci.ArtifactType
	}), CheckBinaries, "has no layer")
	want(repack("other-title", func(_ *ocispec.Descriptor, _ *[]ocispec.Descriptor, ann map[string]string) string {
		ann[ocispec.AnnotationTitle] = "Not files"
		return oci.ArtifactType
	}), CheckAnnotations, `annotation org.opencontainers.image.title is "Not files"; the metadata says "Files"`)
	want(repo.At("other-title"), CheckName, "the tag is other-title")

	// The index lists another owner's package, then a lower version.
	ir, err := c.Repository(oci.Index(ns))
	if err != nil {
		t.Fatal(err)
	}
	card := func(name, version string) {
		t.Helper()
		desc, err := oras.PackManifest(t.Context(), ir, oras.PackManifestVersion1_1, oci.IndexArtifactType, oras.PackManifestOptions{
			ManifestAnnotations: oci.Card{Name: name, Version: version}.Annotations(),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := ir.Tag(t.Context(), desc, "files"); err != nil {
			t.Fatal(err)
		}
	}
	card("other/files", "2.0.0")
	want(repo.At("2.0.0"), CheckIndex, "lists other/files, not acme/files")
	card("acme/files", "1.0.0")
	want(repo.At("2.0.0"), CheckIndex, "lists version 1.0.0, below this one")
	card("acme/files", "3.0.0")
	if ps := problems(repo.At("2.0.0")); len(ps) != 0 {
		t.Errorf("an older version, the index at a newer one: %v", ps)
	}
}

func slicesDelete(ls []ocispec.Descriptor, mediaType string) []ocispec.Descriptor {
	var out []ocispec.Descriptor
	for _, l := range ls {
		if l.MediaType != mediaType {
			out = append(out, l)
		}
	}
	return out
}
