package main

import (
	"bytes"
	"io"
	"log"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	ggcr "github.com/google/go-containerregistry/pkg/registry"

	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/registry"
)

func TestPackage(t *testing.T) {
	srv := httptest.NewServer(ggcr.New(ggcr.Logger(log.New(io.Discard, "", 0))))
	defer srv.Close()
	exe := filepath.Join(t.TempDir(), "destination-files")
	if out, err := exec.Command("go", "build", "-o", exe, "../../internal/connectorhost/external/testconnector").CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	image := strings.TrimPrefix(srv.URL, "http://") + "/connectors/destination-files:0.3.0"
	var out bytes.Buffer
	err := run(t.Context(), []string{"connector", "package", "--image", image, "--name", "files", "--version", "0.3.0",
		"--exe", registry.Platform() + "=" + exe, "--exe", "plan9/386=" + exe}, &out)
	if err != nil {
		t.Fatal(err)
	}
	v, err := connectorhost.ParseVersion(out.Bytes())
	if err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if v.Kind != "destination" || v.Name != "files" || v.Version != "0.3.0" || !strings.Contains(v.Descriptor.Image, "/connectors/destination-files@sha256:") {
		t.Fatalf("descriptor %+v\n%s", v, out.String())
	}
	if !strings.Contains(out.String(), "tier: raw") || !strings.Contains(out.String(), "x-secret: true") {
		t.Errorf("the descriptor leaves out the spec:\n%s", out.String())
	}

	err = run(t.Context(), []string{"connector", "package", "--image", image, "--name", "files", "--version", "0.3.0", "--exe", "plan9/386=" + exe}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), registry.Platform()) {
		t.Errorf("without an executable for this platform: %v", err)
	}
}

func TestConnectorApplyTakesOnlyVersions(t *testing.T) {
	err := onlyConnectorVersions([]byte("apiVersion: openetl.dev/v1alpha1\nkind: Pipeline\n"), "p.yaml#1")
	if err == nil || !strings.Contains(err.Error(), "openetl apply") {
		t.Errorf("a pipeline: %v", err)
	}
	if err := onlyConnectorVersions([]byte("kind: DeclarativeSource\n"), "m.yaml#1"); err != nil {
		t.Error(err)
	}
}
