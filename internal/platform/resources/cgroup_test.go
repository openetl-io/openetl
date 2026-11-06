package resources

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// balloonEnv has this test binary hold that many MiB until it's killed.
const balloonEnv = "OPENETL_TEST_BALLOON_MIB"

func TestMain(m *testing.M) {
	if v := os.Getenv(balloonEnv); v != "" {
		n, _ := strconv.Atoi(v)
		held := make([]byte, n<<20)
		for i := 0; i < len(held); i += 4096 {
			held[i] = 1
		}
		time.Sleep(time.Minute)
		_ = held[0]
		return
	}
	os.Exit(m.Run())
}

func balloon(mib int) *exec.Cmd {
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), balloonEnv+"="+strconv.Itoa(mib))
	return cmd
}

func cgroupFiles(t *testing.T, dir string, files ...string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dir, f), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

func TestCgroupsRefuseWhatIsntDelegated(t *testing.T) {
	root := t.TempDir()
	if _, err := openCgroups(root, os.Getpid()); err == nil {
		t.Error("an ordinary directory was accepted")
	}
	cgroupFiles(t, root, "cgroup.controllers")
	if err := os.WriteFile(filepath.Join(root, "cgroup.controllers"), []byte("cpu pids\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := openCgroups(root, os.Getpid()); err == nil || !strings.Contains(err.Error(), "memory") {
		t.Errorf("no memory controller: %v", err)
	}
}

// What a real cgroup does with these files is in TestCgroupForReal.
func TestCgroupFiles(t *testing.T) {
	root := t.TempDir()
	cgroupFiles(t, root, "cgroup.subtree_control", "cgroup.procs")
	if err := os.WriteFile(filepath.Join(root, "cgroup.controllers"), []byte("cpuset cpu io memory pids\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pid := os.Getpid()
	if err := os.WriteFile(filepath.Join(root, "cgroup.procs"), []byte(strconv.Itoa(pid)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cgroupFiles(t, filepath.Join(root, "runner"), "cgroup.procs")
	cgroupFiles(t, filepath.Join(root, "job-left-behind"))
	c, err := openCgroups(root, pid)
	if err != nil {
		t.Fatal(err)
	}
	if got := read(t, filepath.Join(root, "runner", "cgroup.procs")); got != strconv.Itoa(pid) {
		t.Errorf("the runner's cgroup holds %q", got)
	}
	if got := read(t, filepath.Join(root, "cgroup.subtree_control")); got != "+cpu +memory" {
		t.Errorf("subtree_control = %q", got)
	}
	if _, err := os.Stat(filepath.Join(root, "job-left-behind")); !os.IsNotExist(err) {
		t.Errorf("an earlier runner's cgroup is still there: %v", err)
	}

	job := filepath.Join(root, "job-a%2Fb")
	cgroupFiles(t, job, "cpu.max", "memory.max", "memory.oom.group", "memory.events")
	g, err := c.Job("a/b", Size{CPU: 500, Memory: 100 * MiB})
	if err != nil {
		t.Fatal(err)
	}
	for file, want := range map[string]string{"cpu.max": "50000 100000", "memory.max": strconv.FormatInt(110*MiB, 10), "memory.oom.group": "1"} {
		if got := read(t, filepath.Join(job, file)); got != want {
			t.Errorf("%s = %q, want %q", file, got, want)
		}
	}
	if g.OutOfMemory() {
		t.Error("out of memory with no events")
	}
	if err := os.WriteFile(filepath.Join(job, "memory.events"), []byte("low 0\noom 2\noom_kill 0\noom_group_kill 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !g.OutOfMemory() {
		t.Error("not out of memory after a kill")
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(job); !os.IsNotExist(err) {
		t.Errorf("the job's cgroup is still there: %v", err)
	}
}

// With OPENETL_TEST_CGROUP_ROOT, in a privileged container with a private
// cgroup namespace or on a delegated directory, so it can move this process.
func TestCgroupForReal(t *testing.T) {
	root := os.Getenv("OPENETL_TEST_CGROUP_ROOT")
	if root == "" {
		t.Skip("set OPENETL_TEST_CGROUP_ROOT to a delegated cgroup v2 directory")
	}
	c, err := OpenCgroups(root)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(read(t, filepath.Join(root, "cgroup.subtree_control")), "memory") {
		t.Error("memory isn't enabled for its children")
	}

	g, err := c.Job("within", Size{CPU: 500, Memory: 256 * MiB})
	if err != nil {
		t.Fatal(err)
	}
	m := NewMeter()
	m.Contain(g)
	within := balloon(32)
	m.Confine(within)
	if err := within.Start(); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "job-within")
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(read(t, filepath.Join(dir, "cgroup.procs")), strconv.Itoa(within.Process.Pid)) {
		if time.Now().After(deadline) {
			t.Fatalf("the process isn't in its cgroup, which holds %q", read(t, filepath.Join(dir, "cgroup.procs")))
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := read(t, filepath.Join(dir, "cpu.max")); got != "50000 100000" {
		t.Errorf("cpu.max = %q", got)
	}
	// Removing it kills what's in it.
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := within.Wait(); err == nil {
		t.Error("the process outlived its cgroup")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("its cgroup is still there: %v", err)
	}

	g, err = c.Job("over", Size{CPU: 500, Memory: 64 * MiB})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	m = NewMeter()
	m.Contain(g)
	over := balloon(256)
	m.Confine(over)
	if err := over.Start(); err != nil {
		t.Fatal(err)
	}
	if err := over.Wait(); err == nil {
		t.Error("a process over its cgroup's memory wasn't killed")
	}
	if !g.OutOfMemory() {
		t.Errorf("no kill in its events: %s", read(t, filepath.Join(root, "job-over", "memory.events")))
	}
}
