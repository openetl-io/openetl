package api

import (
	"cmp"
	"strings"
	"testing"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/spec"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// The plan says how many workers a job runs on, why one that asks for them
// runs on one pod, and what it couldn't check.
func TestPlanWorkers(t *testing.T) {
	sp := &apiv1.PipelineSpec{Resources: &apiv1.JobResources{Workers: 3}}
	lanes := destination.Capabilities{Class: destination.StageAndApply, WriterLanes: 4, WriteModes: []destination.WriteMode{destination.Append}}
	cat := &apiv1.Catalog{Streams: []*apiv1.CatalogStream{{Name: "orders", PrimaryKey: []string{"id"}}, {Name: "logs"}}}
	for name, tc := range map[string]struct {
		cat     *apiv1.Catalog
		caps    destination.Capabilities
		known   bool
		workers int
		has     string
	}{
		"unknown capabilities": {cat, lanes, false, 3, "doesn't know the destination's capabilities"},
		"one writer lane":      {cat, destination.Capabilities{Class: destination.StageAndApply}, true, 0, "resources.workers asks for 3 workers, and the job runs on one pod: the destination declares one writer lane"},
		"spread":               {cat, lanes, true, 3, "orders: each attempt reads it on one of its 3 workers"},
		"no catalog":           {nil, lanes, true, 3, "the source hasn't been discovered"},
	} {
		out := &apiv1.PipelinePlan{}
		got := planWorkers(sp, tc.cat, tc.caps, tc.known, false, out)
		var said []string
		for _, w := range out.GetWarnings() {
			said = append(said, cmp.Or(w.GetStream()+": ", "")+w.GetMessage())
		}
		said = append(said, out.GetNotes()...)
		if got != tc.workers || !strings.Contains(strings.Join(said, "\n"), tc.has) {
			t.Errorf("%s: %d workers, and said %q; want %d, and %q", name, got, said, tc.workers, tc.has)
		}
	}
	if got := planWorkers(&apiv1.PipelineSpec{}, cat, lanes, true, false, &apiv1.PipelinePlan{}); got != 0 {
		t.Errorf("%d workers for a job that asks for none", got)
	}
}

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
