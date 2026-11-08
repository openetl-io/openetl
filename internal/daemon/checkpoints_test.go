package daemon

import (
	"strings"
	"testing"

	"github.com/openetl-io/openetl/internal/platform/config"
)

func TestCheckCheckpoints(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  config.Config
		want string
	}{
		{name: "a runner with no store", cfg: config.Config{}, want: "needs a checkpoint store"},
		{name: "kubernetes and a directory", cfg: config.Config{Checkpoints: ".openetl/checkpoints", Launcher: "kubernetes"}, want: "kubernetes launcher"},
		{name: "kubernetes and a bucket", cfg: config.Config{Checkpoints: "s3://b?region=r", Launcher: "kubernetes"}},
		{name: "process launcher and a directory", cfg: config.Config{Checkpoints: "/x", Launcher: "process"}},
		{name: "a runner and a directory", cfg: config.Config{Checkpoints: "/x"}},
		{name: "a runner and a bucket", cfg: config.Config{Checkpoints: "gs://b"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := checkCheckpoints(tt.cfg)
			if tt.want == "" && err != nil {
				t.Fatal(err)
			}
			if tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)) {
				t.Fatalf("error = %v, want one saying %q", err, tt.want)
			}
		})
	}
}
