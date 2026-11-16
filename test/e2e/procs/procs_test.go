package procs

import (
	"os"
	"os/exec"
	"slices"
	"testing"
)

// A pod's children are its pipeline's; a runner's only when it runs that
// pipeline alone. A runner other pipelines may share, or one dedicated to
// another, gives no owner, so a kill there finds nothing.
func TestOnlyAPodOrADedicatedRunnerOwnsAPipelinesConnectors(t *testing.T) {
	for _, c := range []struct {
		name     string
		host     Host
		pipeline string
		want     int
	}{
		{"a pod", Host{PID: 10, Pod: true}, "p", 10},
		{"a dedicated runner", Host{PID: 11, Only: "p"}, "p", 11},
		{"a shared runner", Host{PID: 12}, "p", 0},
		{"another pipeline's runner", Host{PID: 13, Only: "q"}, "p", 0},
		{"no process", Host{Pod: true}, "p", 0},
		{"no pipeline", Host{PID: 14}, "", 0},
	} {
		if got := Owner(c.host, c.pipeline); got != c.want {
			t.Errorf("%s: owner %d, want %d", c.name, got, c.want)
		}
	}
}

// Children finds a parent's child processes by the start of their names,
// and only its own.
func TestChildrenAreTheParentsByName(t *testing.T) {
	cmd := exec.CommandContext(t.Context(), "/bin/sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	me := os.Getpid()
	if got := Children(t.Context(), me, "sle"); !slices.Contains(got, cmd.Process.Pid) {
		t.Errorf("children %v, want %d among them", got, cmd.Process.Pid)
	}
	if got := Children(t.Context(), me, "source-"); slices.Contains(got, cmd.Process.Pid) {
		t.Errorf("children named source- %v have sleep's %d", got, cmd.Process.Pid)
	}
	if got := Children(t.Context(), cmd.Process.Pid, "sle"); len(got) != 0 {
		t.Errorf("sleep's children %v", got)
	}
	if got := Children(t.Context(), 0, "sle"); got != nil {
		t.Errorf("no parent's children %v", got)
	}
}
