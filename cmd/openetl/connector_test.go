package main

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/spec"
)

func TestPackage(t *testing.T) {
	exe := filepath.Join(t.TempDir(), "destination-files")
	if out, err := exec.Command("go", "build", "-o", exe, "../../internal/connectorhost/external/testconnector").CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	content, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	here := connectorhost.Platform()
	var out bytes.Buffer
	err = run(t.Context(), []string{"connector", "package", "--name", "files", "--version", "0.3.0",
		"--exe", here + "=" + exe, "--exe", "plan9/386=" + exe,
		"--url-template", "https://example.com/releases/download/v0.3.0/destination-files_{os}_{arch}",
		"--author", "Example", "--logo", "https://example.com/files.svg", "--release-tag", "v0.3.0", "--release-url", "https://example.com/releases/v0.3.0"}, &out)
	if err != nil {
		t.Fatal(err)
	}
	v, err := connectorhost.ParseVersion(out.Bytes())
	if err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	goos, arch, _ := strings.Cut(here, "/")
	bin := v.Descriptor.Binaries[here]
	if v.Kind != "destination" || v.Name != "files" || v.Version != "0.3.0" || len(v.Descriptor.Binaries) != 2 ||
		bin.URL != "https://example.com/releases/download/v0.3.0/destination-files_"+goos+"_"+arch || bin.Digest != connectorhost.Digest(content) {
		t.Fatalf("descriptor %+v\n%s", v, out.String())
	}
	if v.Info != (spec.Info{Author: "Example", Logo: "https://example.com/files.svg", ReleaseTag: "v0.3.0", ReleaseURL: "https://example.com/releases/v0.3.0"}) {
		t.Errorf("info %+v", v.Info)
	}
	if !strings.Contains(out.String(), "tier: raw") || !strings.Contains(out.String(), "x-secret: true") {
		t.Errorf("the descriptor leaves out the spec:\n%s", out.String())
	}

	out.Reset()
	err = run(t.Context(), []string{"connector", "package", "--name", "files", "--version", "0.3.0",
		"--exe", here + "=" + exe, "--url", here + "=https://example.com/files?platform=" + here}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := connectorhost.ParseVersion(out.Bytes()); err != nil || v.Descriptor.Binaries[here].URL != "https://example.com/files?platform="+here {
		t.Errorf("--url: %+v, %v", v, err)
	}

	for name, args := range map[string][]string{
		"no executable for this platform": {"--exe", "plan9/386=" + exe, "--url", "plan9/386=https://example.com/x"},
		"no url for an executable":        {"--exe", here + "=" + exe, "--url", "plan9/386=https://example.com/x"},
		"both kinds of url":               {"--exe", here + "=" + exe, "--url", here + "=https://example.com/x", "--url-template", "https://example.com/{os}"},
		"a logo over http":                {"--exe", here + "=" + exe, "--url", here + "=https://example.com/x", "--logo", "http://example.com/x.svg"},
	} {
		err := run(t.Context(), append([]string{"connector", "package", "--name", "files", "--version", "0.3.0"}, args...), io.Discard)
		if err == nil {
			t.Errorf("%s: packaged", name)
		}
	}
}

func TestConnectorApplyTakesOnlyVersions(t *testing.T) {
	err := onlyConnectorVersions([]byte("apiVersion: openetl.io/v1alpha1\nkind: Pipeline\n"), "p.yaml#1")
	if err == nil || !strings.Contains(err.Error(), "openetl apply") {
		t.Errorf("a pipeline: %v", err)
	}
	if err := onlyConnectorVersions([]byte("kind: DeclarativeSource\n"), "m.yaml#1"); err != nil {
		t.Error(err)
	}
}
