package resources

import (
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestLimitsFollowTheSize(t *testing.T) {
	for _, tc := range []struct {
		size    Size
		threads int
		env     []string
	}{
		{XS, 1, []string{"GOMAXPROCS=1", "GOMEMLIMIT=536870912"}},
		{S, 1, []string{"GOMAXPROCS=1", "GOMEMLIMIT=1073741824"}},
		{Size{CPU: 1500, Memory: GiB}, 2, []string{"GOMAXPROCS=2", "GOMEMLIMIT=536870912"}},
		{XL, 8, []string{"GOMAXPROCS=8", "GOMEMLIMIT=8589934592"}},
	} {
		if got := tc.size.Threads(); got != tc.threads {
			t.Errorf("%s: %d threads, want %d", tc.size, got, tc.threads)
		}
		if got := tc.size.ConnectorEnv(); !slices.Equal(got, tc.env) {
			t.Errorf("%s: env %v, want %v", tc.size, got, tc.env)
		}
	}
	if got := (Size{}).ConnectorEnv(); got != nil {
		t.Errorf("no size: env %v", got)
	}
	if got, want := M.EngineMemory(), 3*GiB; got != want {
		t.Errorf("engine memory %d, want %d", got, want)
	}
	if got := M.MemoryCeiling(); got <= M.Memory || got > M.Memory*5/4 {
		t.Errorf("ceiling %d of a size of %d", got, M.Memory)
	}
}

func TestKillFirst(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("needs /proc")
	}
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	KillFirst(cmd.Process.Pid)
	b, err := os.ReadFile("/proc/" + strconv.Itoa(cmd.Process.Pid) + "/oom_score_adj")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(b)); got != "1000" {
		t.Errorf("oom_score_adj = %s", got)
	}
}
