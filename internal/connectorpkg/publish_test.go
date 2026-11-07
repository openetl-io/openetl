package connectorpkg

import (
	"bytes"
	"io"
	"log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ggcr "github.com/google/go-containerregistry/pkg/registry"

	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/oci"
	"github.com/openetl-io/openetl/internal/spec"
)

const testLogo = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1 1"><rect width="1" height="1"/></svg>`

func TestPublish(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	file := "name: acme/files\ntitle: Files\nrepository: https://github.com/acme/files\nlogo: ./logo.svg\nroles:\n  destination:\n    build:\n" +
		"      command: cd " + repo + " && go build -o " + dir + "/bin/files-$(go env GOOS)-$(go env GOARCH) ./internal/connectorhost/external/testconnector\n" +
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
	out := filepath.Join(dir, "dist")
	res, err := Package(t.Context(), f, Options{Version: "2.0.0", Out: out, Platforms: []string{connectorhost.Platform()}})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(res.Descriptor)
	checkDescriptorSchema(t, b)
	if !bytes.Contains(b, []byte("logo: https://github.com/acme/files/releases/download/v2.0.0/files.logo.svg")) || !bytes.Contains(b, []byte("title: Files")) {
		t.Errorf("descriptor:\n%s", b)
	}
	if lb, err := os.ReadFile(res.Logo); err != nil || string(lb) != testLogo {
		t.Errorf("logo %s: %q, %v", res.Logo, lb, err)
	}

	srv := httptest.NewServer(ggcr.New(ggcr.Logger(log.New(io.Discard, "", 0))))
	defer srv.Close()
	c := &oci.Client{HTTP: srv.Client()}
	ns, err := oci.Parse(oci.Scheme + strings.TrimPrefix(srv.URL, "http://") + "/acme/connectors")
	if err != nil {
		t.Fatal(err)
	}
	p, err := Publish(t.Context(), c, ns, res.Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if p.Name != "acme/files" || p.Version != "2.0.0" || !p.Indexed || p.Ref.Repository != "acme/connectors/files" {
		t.Errorf("published %+v", p)
	}
	pulled, err := c.Pull(t.Context(), ns.Child("files").At("2.0.0"))
	if err != nil {
		t.Fatal(err)
	}
	doc, err := spec.Parse(pulled.Descriptor)
	if err != nil {
		t.Fatalf("%v\n%s", err, pulled.Descriptor)
	}
	// The API stores a version as its canonical JSON, which is the config.
	if enc, err := spec.EncodeVersion(doc); err != nil || !bytes.Equal(enc, pulled.Descriptor) {
		t.Errorf("the config isn't the descriptor's canonical JSON:\n%s\n%s", enc, pulled.Descriptor)
	}
	bin := doc.Descriptor.Roles["destination"].Binaries[connectorhost.Platform()]
	if !strings.HasPrefix(bin.URL, ns.Child("files").String()+"@sha256:") || !strings.HasSuffix(bin.URL, bin.Digest) {
		t.Errorf("binary %+v", bin)
	}
	blob, err := oci.Parse(bin.URL)
	if err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	if err := c.FetchBlob(t.Context(), blob, &got, connectorhost.MaxExecutable); err != nil || connectorhost.Digest(got.Bytes()) != bin.Digest {
		t.Errorf("the binary's blob: %v", err)
	}
	if !strings.HasPrefix(doc.Info.Logo, ns.Child("files").String()+"@sha256:") || doc.Info.Title != "Files" {
		t.Errorf("info %+v", doc.Info)
	}
	cards, err := c.ListIndex(t.Context(), ns, nil)
	if err != nil || len(cards) != 1 {
		t.Fatalf("index %+v, %v", cards, err)
	}
	if card := cards[0]; card.Name != "acme/files" || card.Title != "Files" || card.Version != "2.0.0" || card.Logo == nil ||
		strings.Join(card.Roles, ",") != "destination" || card.Repository != ns.Child("files").String() || card.Source != "https://github.com/acme/files" {
		t.Errorf("card %+v", card)
	}
	if m := pulled.Manifest; m.Annotations[oci.AnnotationName] != "acme/files" || m.Annotations["org.opencontainers.image.version"] != "2.0.0" || len(m.Layers) != 2 {
		t.Errorf("manifest %+v", m.Manifest)
	}

	// Again is a no-op; the same version with other content is refused.
	if _, err := Publish(t.Context(), c, ns, res.Descriptor); err != nil {
		t.Errorf("publishing again: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "logo.svg"), []byte(strings.Replace(testLogo, `"1"`, `"2"`, 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if res, err = Package(t.Context(), f, Options{Version: "2.0.0", Out: out, Platforms: []string{connectorhost.Platform()}}); err != nil {
		t.Fatal(err)
	}
	if _, err := Publish(t.Context(), c, ns, res.Descriptor); err == nil || !strings.Contains(err.Error(), "new version") {
		t.Errorf("publishing other content as the same version: %v", err)
	}
}

func TestLogoFile(t *testing.T) {
	dir := t.TempDir()
	base := "name: acme/x\nrepository: https://example.com/x\nroles:\n  source:\n    manifest: m.yaml\n"
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("m.yaml", "")
	for name, c := range map[string]struct{ logo, content string }{
		"missing":     {"./none.svg", ""},
		"not an svg":  {"./logo.svg", "<html><script>alert(1)</script></html>"},
		"too large":   {"./logo.svg", testLogo + strings.Repeat(" ", oci.MaxLogo)},
		"a png named": {"./logo.png", testLogo},
		"outside":     {"../logo.svg", ""},
	} {
		if c.content != "" {
			write(filepath.Base(c.logo), c.content)
		}
		write(FileName, base+"logo: "+c.logo+"\n")
		if _, err := Load(dir); err == nil {
			t.Errorf("%s: loaded", name)
		}
	}
	write("logo.svg", testLogo)
	write(FileName, base+"logo: ./logo.svg\n")
	f, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !f.LogoFile() || f.Info().Logo != "" {
		t.Errorf("file %+v", f)
	}
	if _, err := Parse([]byte(base + "logo: /etc/logo.svg\n")); err == nil {
		t.Error("an absolute logo path parsed")
	}
}
