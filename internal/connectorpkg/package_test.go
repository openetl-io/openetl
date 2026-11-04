package connectorpkg

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.yaml.in/yaml/v3"

	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/spec"
)

// checkDescriptorSchema checks a descriptor package wrote against its
// published schema.
func checkDescriptorSchema(t *testing.T, descriptor []byte) {
	t.Helper()
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(spec.DescriptorSchema()))
	if err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource("connector.schema.json", doc); err != nil {
		t.Fatal(err)
	}
	sch, err := c.Compile("connector.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var v any
	if err := yaml.Unmarshal(descriptor, &v); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(v)
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	if err := sch.Validate(inst); err != nil {
		t.Errorf("the descriptor isn't what the schema says: %v\n%s", err, descriptor)
	}
}

func TestParseFile(t *testing.T) {
	f, err := Load("testdata/files")
	if err != nil {
		t.Fatal(err)
	}
	if f.FullName() != "openetl/files" || f.ShortName() != "files" || len(f.Platforms) != 4 || !f.Builds() {
		t.Errorf("file %+v", f)
	}
	if err := f.Require([]string{"author", "logo", "license", "description", "docs", "tags"}); err != nil {
		t.Error(err)
	}
	m, err := Load("testdata/acme")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Require([]string{"author", "logo"}); err == nil || !strings.Contains(err.Error(), "author or logo") {
		t.Errorf("requiring what it hasn't: %v", err)
	}
	if err := m.Require([]string{"version"}); err == nil {
		t.Error("required an unknown field")
	}
	if err := m.CheckManifests(); err != nil {
		t.Error(err)
	}

	valid := "name: acme/x\nrepository: https://example.com/x\nroles:\n  source:\n    build: {go: ./cmd}\n"
	if _, err := Parse([]byte(valid)); err != nil {
		t.Fatal(err)
	}
	for name, doc := range map[string]string{
		"no repository":        "name: x\nroles: {source: {build: {go: .}}}\n",
		"no roles":             "name: x\nrepository: https://example.com/x\nroles: {}\n",
		"another role":         strings.Replace(valid, "source:", "sink:", 1),
		"nothing to run":       strings.Replace(valid, "build: {go: ./cmd}", "state: {version: 1, reads: 1}", 1),
		"go and command":       strings.Replace(valid, "{go: ./cmd}", "{go: ./cmd, command: make, output: 'x-{os}-{arch}'}", 1),
		"command, no output":   strings.Replace(valid, "{go: ./cmd}", "{command: make}", 1),
		"output, no os":        strings.Replace(valid, "{go: ./cmd}", "{command: make, output: dist/x}", 1),
		"name":                 strings.Replace(valid, "acme/x", "Acme/X", 1),
		"version in the file":  valid + "version: 1.0.0\n",
		"image tag":            strings.Replace(valid, "build: {go: ./cmd}", "image: ghcr.io/acme/x:1.0", 1),
		"manifest destination": "name: x\nrepository: https://example.com/x\nroles: {destination: {manifest: m.yaml}}\n",
		"manifest and build":   strings.Replace(valid, "build: {go: ./cmd}", "build: {go: ./cmd}\n    manifest: m.yaml", 1),
		"command, no image":    strings.Replace(valid, "build: {go: ./cmd}", "build: {go: ./cmd}\n    command: [x]", 1),
		"reads after version":  strings.Replace(valid, "build: {go: ./cmd}", "build: {go: ./cmd}\n    state: {version: 1, reads: 2}", 1),
		"logo over http":       valid + "logo: http://example.com/x.svg\n",
		"platform":             valid + "platforms: [linux-amd64]\n",
	} {
		if _, err := Parse([]byte(doc)); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
}

func TestPackage(t *testing.T) {
	f, err := Load("testdata/files")
	if err != nil {
		t.Fatal(err)
	}
	reg := fakeRegistry(t)
	f.Roles["destination"].Image = strings.TrimPrefix(reg.URL, "http://") + "/example/files"
	f.Roles["destination"].Command = []string{"/connector"}
	host := connectorhost.Platform()
	out := t.TempDir()
	res, err := Package(t.Context(), f, Options{
		Version: "0.3.0", Tag: "files/v0.3.0", Out: out, Platforms: []string{host, "linux/riscv64"},
		Registry: &Registry{Client: reg.Client(), Insecure: map[string]bool{strings.TrimPrefix(reg.URL, "http://"): true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Executables) != 2 || filepath.Base(res.Executables[1]) != "files-destination-linux-riscv64" {
		t.Errorf("executables %v", res.Executables)
	}
	b, err := os.ReadFile(res.Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(res.Descriptor) != "files.connector.yaml" {
		t.Errorf("descriptor %s", res.Descriptor)
	}
	checkDescriptorSchema(t, b)
	v, err := connectorhost.ParseVersion(b)
	if err != nil {
		t.Fatalf("%v\n%s", err, b)
	}
	d := v.Roles["destination"]
	exe, err := os.ReadFile(res.Executables[0])
	if err != nil {
		t.Fatal(err)
	}
	goos, arch, _ := strings.Cut(host, "/")
	if bin := d.Binaries[host]; bin.Digest != connectorhost.Digest(exe) ||
		bin.URL != "https://github.com/example/openetl-files/releases/download/files/v0.3.0/files-destination-"+goos+"-"+arch {
		t.Errorf("binary %+v", bin)
	}
	if d.Image == nil || d.Image.Digest != reg.digest || strings.Join(d.Image.Platforms, ",") != "linux/amd64,linux/arm64" || d.Image.Command[0] != "/connector" {
		t.Errorf("image %+v", d.Image)
	}
	if d.Resources.CPU != "500m" || d.State.Version != 2 || d.Spec.GetKind().String() != "CONNECTOR_KIND_DESTINATION" || d.Spec.GetProtocolVersion() != "v1" {
		t.Errorf("role %+v", d.Role)
	}
	if v.Name != "openetl/files" || v.Info.ReleaseTag != "files/v0.3.0" || v.Info.ReleaseURL != "https://github.com/example/openetl-files/releases/tag/files/v0.3.0" ||
		v.Info.License != "Apache-2.0" || len(v.Info.Tags) != 2 {
		t.Errorf("version %s, info %+v", v.Name, v.Info)
	}

	// Elsewhere than GitHub, runners need a URL template.
	m, err := Load("testdata/acme")
	if err != nil {
		t.Fatal(err)
	}
	res, err = Package(t.Context(), m, Options{Version: "1.0.0", Out: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	b, _ = os.ReadFile(res.Descriptor)
	checkDescriptorSchema(t, b)
	mv, err := connectorhost.ParseVersion(b)
	if err != nil {
		t.Fatalf("%v\n%s", err, b)
	}
	if s := mv.Roles["source"]; s.Manifest == nil || s.Requires.Platform != "0.1.0" || mv.Name != "example/acme" || mv.Info.ReleaseTag != "v1.0.0" || mv.Info.ReleaseURL != "" {
		t.Errorf("manifest package %+v, %+v", mv, s)
	}
	m.Roles["source"] = &Role{Build: &Build{Go: "."}}
	if _, err := Package(t.Context(), m, Options{Version: "1.0.0", Out: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "--url-template") {
		t.Errorf("a build elsewhere than GitHub, with no template: %v", err)
	}

	only, _ := Load("testdata/files")
	only.Roles["destination"] = &Role{Image: f.Roles["destination"].Image}
	_, err = Package(t.Context(), only, Options{Version: "0.3.0", Out: t.TempDir(), Registry: &Registry{Client: reg.Client(), Insecure: map[string]bool{strings.TrimPrefix(reg.URL, "http://"): true}}})
	if err == nil || !strings.Contains(err.Error(), "Docker or Podman") {
		t.Errorf("an image-only role with no way to read its spec: %v", err)
	}
	if _, err := Package(t.Context(), f, Options{Version: "0.3.0", Out: t.TempDir(), Platforms: []string{"plan9/386"}}); err == nil {
		t.Error("packaged without this platform's executable")
	}
}

func TestPackageCommand(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	file := "name: acme/files\nrepository: https://github.com/acme/files\nroles:\n  destination:\n    build:\n" +
		"      command: cd " + repo + " && go build -o " + dir + "/bin/files-$(go env GOOS)-$(go env GOARCH) ./internal/connectorhost/external/testconnector\n" +
		"      output: bin/files-{os}-{arch}\n"
	if err := os.WriteFile(filepath.Join(dir, FileName), []byte(file), 0o600); err != nil {
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
	b, _ := os.ReadFile(res.Descriptor)
	if !bytes.Contains(b, []byte("name: acme/files")) || !bytes.Contains(b, []byte("/releases/download/v2.0.0/files-destination-")) {
		t.Errorf("descriptor:\n%s", b)
	}
}

func TestSchemaIsJSONSchema(t *testing.T) {
	var v map[string]any
	if err := json.Unmarshal(Schema(), &v); err != nil || v["$schema"] == nil {
		t.Fatalf("schema: %v", err)
	}
	if _, err := fileSchema(); err != nil {
		t.Fatal(err)
	}
}

type registry struct {
	*httptest.Server
	digest string
}

// fakeRegistry serves example/files:0.3.0 as an index of two images, behind
// an anonymous token.
func fakeRegistry(t *testing.T) *registry {
	index := []byte(`{"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json", "manifests": [
		{"digest": "sha256:aa", "platform": {"os": "linux", "architecture": "arm64"}},
		{"digest": "sha256:bb", "platform": {"os": "linux", "architecture": "amd64"}},
		{"digest": "sha256:cc", "platform": {"os": "unknown", "architecture": "unknown"}}]}`)
	r := &registry{digest: connectorhost.Digest(index)}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.URL.Path == "/token":
			_, _ = w.Write([]byte(`{"token": "t"}`))
		case req.Header.Get("Authorization") != "Bearer t":
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+r.URL+`/token",service="fake"`)
			w.WriteHeader(http.StatusUnauthorized)
		case req.URL.Path == "/v2/example/files/manifests/0.3.0":
			w.Header().Set("Content-Type", "application/vnd.oci.image.index.v1+json")
			_, _ = w.Write(index)
		default:
			http.NotFound(w, req)
		}
	}))
	t.Cleanup(r.Close)
	return r
}
