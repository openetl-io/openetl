package daemon

import (
	"flag"
	"io"
	"strings"
	"testing"

	"github.com/openetl-io/openetl/internal/platform/config"
)

// The process launcher's coordinators take its flags, so a fixed
// --attempt-listen port would be every one's.
func TestProcessLauncherRefusesAFixedAttemptPort(t *testing.T) {
	for listen, refused := range map[string]bool{"": false, "127.0.0.1:0": false, ":7720": true} {
		fs := flag.NewFlagSet("openetld", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		cfg, err := config.Parse(fs, []string{"--launcher", "process", "--attempt-listen", listen}, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = podLauncher(t.Context(), cfg)
		if refused != (err != nil) || (refused && !strings.Contains(err.Error(), "--attempt-listen")) {
			t.Errorf("--attempt-listen %q: %v, want refused %v", listen, err, refused)
		}
	}
}
