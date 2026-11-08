package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/connectorpkg"
)

func TestConnectorSchemas(t *testing.T) {
	for _, kind := range []string{"file", "descriptor", "manifest"} {
		var buf bytes.Buffer
		if err := run(t.Context(), []string{"connector", "schema", kind}, &buf); err != nil {
			t.Fatal(err)
		}
		var v map[string]any
		if err := json.Unmarshal(buf.Bytes(), &v); err != nil || v["$id"] == nil {
			t.Errorf("%s: %v", kind, err)
		}
	}
	if err := run(t.Context(), []string{"connector", "schema", "pipeline"}, io.Discard); err == nil {
		t.Error("a schema there isn't")
	}
}

func TestParseInterspersed(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	v := fs.String("v", "", "")
	got, err := parseInterspersed(fs, []string{"a", "-v", "1", "b", "--", "-c", "d"})
	if err != nil || *v != "1" || strings.Join(got, " ") != "a b -c d" {
		t.Errorf("got %q, %q, %v", got, *v, err)
	}
}

func TestPackage(t *testing.T) {
	files, acme := "../../internal/connectorpkg/testdata/files", "../../internal/connectorpkg/testdata/acme"
	out := t.TempDir()
	here := connectorhost.Platform()
	var buf bytes.Buffer
	err := run(t.Context(), []string{"connector", "package", "--tag", "files/v0.3.0", "--out", out, "--platforms", here, files, acme}, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != "openetl/files 0.3.0: "+filepath.Join(out, "files.connector.yaml")+"\n" {
		t.Errorf("printed %q", got)
	}
	b, err := os.ReadFile(filepath.Join(out, "files.connector.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	v, err := connectorhost.ParseVersion(b)
	if err != nil {
		t.Fatalf("%v\n%s", err, b)
	}
	exe, err := os.ReadFile(filepath.Join(out, connectorpkg.Asset("files", "destination", here)))
	if err != nil {
		t.Fatal(err)
	}
	if bin := v.Roles["destination"].Binaries[here]; v.Version != "0.3.0" || bin.Digest != connectorhost.Digest(exe) || !strings.Contains(bin.URL, "/releases/download/files/v0.3.0/") {
		t.Errorf("version %s, binary %+v", v.Version, bin)
	}

	buf.Reset()
	if err := run(t.Context(), []string{"connector", "package", acme, "--version", "1.0.0", "--out", out}, &buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "example/acme 1.0.0") {
		t.Errorf("printed %q", buf.String())
	}
	b, _ = os.ReadFile(filepath.Join(out, "acme.connector.yaml"))
	if !strings.Contains(string(b), "tag: v1.0.0") {
		t.Errorf("a single package's tag:\n%s", b)
	}

	for name, args := range map[string][]string{
		"no version":          {files},
		"tag and version":     {"--tag", "v1.0.0", "--version", "1.0.0", files},
		"no package named so": {"--tag", "other/v1.0.0", files, acme},
		"a tag for two":       {"--tag", "v1.0.0", files, acme},
		"not a version":       {"--version", "1.0", files},
		"no file":             {"--version", "1.0.0", t.TempDir()},
		"no executable here":  {"--version", "1.0.0", "--platforms", "plan9/386", files},
	} {
		if err := run(t.Context(), append([]string{"connector", "package", "--out", t.TempDir()}, args...), io.Discard); err == nil {
			t.Errorf("%s: packaged", name)
		}
	}
}

func TestValidate(t *testing.T) {
	files, acme := "../../internal/connectorpkg/testdata/files", "../../internal/connectorpkg/testdata/acme"
	var buf bytes.Buffer
	if err := run(t.Context(), []string{"connector", "validate", "--require", "author,logo", files}, &buf); err != nil {
		t.Fatal(err)
	}
	if buf.String() != files+": openetl/files\n" {
		t.Errorf("printed %q", buf.String())
	}
	err := run(t.Context(), []string{"connector", "validate", "--require", "author,logo", files, acme}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "example/acme has no author or logo") {
		t.Errorf("a file without what's required: %v", err)
	}
	broken := t.TempDir()
	if err := os.WriteFile(filepath.Join(broken, connectorpkg.FileName), []byte("name: x\nroles: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(t.Context(), []string{"connector", "validate", broken}, io.Discard); err == nil {
		t.Error("validated a file without a repository or roles")
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
