//go:build e2e

package e2e

import (
	"strings"
	"syscall"
	"testing"
	"time"
)

// The retry re-reads 150 rows per stream that are already applied, and
// dedupe at apply keeps each once.
func TestSupersetKilledMidSync(t *testing.T) {
	r := startRunner(t)
	name := unique("superset-killed")
	faker := `{"seed": 3, "count": 2000, "batchSize": 50, "delay": "50ms", "overlap": 150}`
	apply(t, pipelineDocs(name, faker, `{"require": "exactly_once", "commit": {"interval": "1s"}}`)...)
	pid := r.engine(t)
	eventually(t, 30*time.Second, "a committed epoch", func() bool { return committedEpochs(t, schema(name)) > 0 })
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	status, stats := waitSync(t, name, 90*time.Second)
	if status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if !strings.Contains(stats, `"Generation": 2`) {
		t.Errorf("stats %s: want the second attempt to have finished", stats)
	}
	checkRows(t, schema(name), 2000, "users", "purchases")
}

func TestPlanChecksFailTheSync(t *testing.T) {
	startRunner(t)
	name := unique("plan-checks")
	apply(t, pipelineDocs(name, `{"count": 10, "overlap": 5}`, `{"require": "exactly_once", "dedupe": "none"}`)...)
	status, msg := waitSync(t, name, 60*time.Second)
	if status != "failed" {
		t.Fatalf("sync %s: %s", status, msg)
	}
	for _, stream := range []string{"users", "purchases"} {
		if want := `stream "` + stream + `" gets at_least_once (superset replay with dedupe: none), below exactly_once`; !strings.Contains(msg, want) {
			t.Errorf("error %q doesn't say %q", msg, want)
		}
	}
}
