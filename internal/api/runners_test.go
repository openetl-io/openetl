package api

import (
	"strings"
	"testing"

	"github.com/unsuitable001/openetl/internal/spec"
	"github.com/unsuitable001/openetl/internal/store/db"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

func TestCanRun(t *testing.T) {
	runner := db.Runner{ID: "r1", Release: "0.2.0", Protocol: "v1", Features: []string{"a"}, Launchers: []string{"subprocess"}, Platform: "linux/amd64"}
	docker := runner
	docker.Launchers = []string{"subprocess", "docker"}
	bin := func() *spec.Role {
		return &spec.Role{Binaries: map[string]spec.Binary{"linux/amd64": {}}, Spec: &protocolv1.SpecResponse{ProtocolVersion: "v1"}}
	}
	image := &spec.Image{Ref: "ghcr.io/a/b", Platforms: []string{"linux/amd64"}}
	for name, tc := range map[string]struct {
		role   func() *spec.Role
		runner db.Runner
		want   string
	}{
		"binary":          {bin, runner, ""},
		"manifest":        {func() *spec.Role { return &spec.Role{Manifest: []byte("{}")} }, runner, ""},
		"other platform":  {func() *spec.Role { r := bin(); r.Binaries = map[string]spec.Binary{"darwin/arm64": {}}; return r }, runner, "no binary for linux/amd64"},
		"image only":      {func() *spec.Role { r := bin(); r.Binaries, r.Image = nil, image; return r }, runner, "only an image"},
		"image, docker":   {func() *spec.Role { r := bin(); r.Binaries, r.Image = nil, image; return r }, docker, ""},
		"feature":         {func() *spec.Role { r := bin(); r.Spec.RequiredFeatures = []string{"a"}; return r }, runner, ""},
		"missing feature": {func() *spec.Role { r := bin(); r.Spec.RequiredFeatures = []string{"b"}; return r }, runner, "feature b"},
		"protocol":        {func() *spec.Role { r := bin(); r.Spec.ProtocolVersion = "v2"; return r }, runner, "protocol v2"},
		"newer platform":  {func() *spec.Role { r := bin(); r.Requires.Platform = "0.3.0"; return r }, runner, "openetl 0.3.0"},
		"older platform":  {func() *spec.Role { r := bin(); r.Requires.Platform = "0.1.0"; return r }, runner, ""},
		"dev runs any":    {func() *spec.Role { r := bin(); r.Requires.Platform = "9.0.0"; return r }, db.Runner{Release: "dev", Protocol: "v1", Launchers: []string{"subprocess"}, Platform: "linux/amd64"}, ""},
	} {
		got := canRun(tc.role(), tc.runner)
		if (tc.want == "") != (got == "") || !strings.Contains(got, tc.want) {
			t.Errorf("%s: %q, want %q", name, got, tc.want)
		}
	}
}
