package daemon

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestRunnerID(t *testing.T) {
	host := func() (string, error) { return "h", nil }
	stable := fmt.Sprintf("h:%d", os.Getpid())

	// Runners in containers can share a hostname and a PID.
	a, err := runnerID("", host)
	if err != nil {
		t.Fatal(err)
	}
	b, err := runnerID("", host)
	if err != nil {
		t.Fatal(err)
	}
	if a == b || !strings.HasPrefix(a, stable+":") {
		t.Errorf("runners got %q and %q, want them different, starting %s:", a, b, stable)
	}

	// Attempts are placed on a pod launcher by ID, and an in-place restart
	// has to find them.
	for _, launcher := range []string{"kubernetes", "process"} {
		if id, err := runnerID(launcher, host); err != nil || id != stable {
			t.Errorf("a %s launcher got %q, %v; want %q", launcher, id, err, stable)
		}
	}

	failed := errors.New("no hostname")
	if id, err := runnerID("", func() (string, error) { return "", failed }); !errors.Is(err, failed) {
		t.Errorf("without a hostname: %q, %v", id, err)
	}
}
