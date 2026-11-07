package resources

import (
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Cgroups makes a cgroup v2 for each job under a delegated directory,
// which belongs to one runner.
type Cgroups struct {
	root string
}

const (
	jobPrefix  = "job-"
	cpuPeriod  = 100_000 // µs, the kernel's default
	runnerName = "runner"
)

// OpenCgroups readies root, which systemd's Delegate=yes or a writable cgroup
// namespace gives a runner. It enables CPU and memory for root's children,
// which it can't while a process sits in root itself, so it moves this one down
// a level. It removes what an earlier runner left.
func OpenCgroups(root string) (*Cgroups, error) {
	if runtime.GOOS != "linux" {
		return nil, errors.New("cgroups need Linux")
	}
	return openCgroups(root, os.Getpid())
}

func openCgroups(root string, pid int) (*Cgroups, error) {
	have := fields(filepath.Join(root, "cgroup.controllers"))
	if !slices.Contains(have, "cpu") || !slices.Contains(have, "memory") {
		return nil, fmt.Errorf("%s isn't a cgroup v2 directory with the cpu and memory controllers, which it needs delegated to this runner", root)
	}
	if slices.Contains(fields(filepath.Join(root, "cgroup.procs")), strconv.Itoa(pid)) {
		self := filepath.Join(root, runnerName)
		if err := os.MkdirAll(self, 0o755); err != nil { //nolint:gosec // a cgroup's files have their own modes
			return nil, err
		}
		if err := write(self, "cgroup.procs", strconv.Itoa(pid)); err != nil {
			return nil, fmt.Errorf("move this runner out of %s: %w", root, err)
		}
	}
	if err := write(root, "cgroup.subtree_control", "+cpu +memory"); err != nil {
		return nil, fmt.Errorf("enable cpu and memory in %s, which takes having no other process in it: %w", root, err)
	}
	c := &Cgroups{root: root}
	old, err := filepath.Glob(filepath.Join(root, jobPrefix+"*"))
	if err != nil {
		return nil, err
	}
	for _, dir := range old {
		if err := remove(dir); err != nil {
			return nil, fmt.Errorf("remove what an earlier runner left: %w", err)
		}
	}
	return c, nil
}

// Job makes the cgroup of an attempt of this size. Going over its ceiling kills
// all of its processes, which then count as out of memory.
func (c *Cgroups) Job(id string, size Size) (*Cgroup, error) {
	path := filepath.Join(c.root, jobPrefix+url.PathEscape(id))
	if err := os.MkdirAll(path, 0o755); err != nil { //nolint:gosec // a cgroup's files have their own modes
		return nil, err
	}
	g := &Cgroup{path: path}
	err := errors.Join(
		write(path, "cpu.max", fmt.Sprintf("%d %d", size.CPU*cpuPeriod/1000, cpuPeriod)),
		write(path, "memory.max", strconv.FormatInt(size.MemoryCeiling(), 10)),
		write(path, "memory.oom.group", "1"),
	)
	// Without swap accounting there's no such file, and nothing to limit.
	if err2 := write(path, "memory.swap.max", "0"); !errors.Is(err2, fs.ErrNotExist) {
		err = errors.Join(err, err2)
	}
	if err == nil {
		g.dir, err = os.Open(path) //nolint:gosec // the operator's directory
	}
	if err != nil {
		_ = g.Close()
		return nil, err
	}
	return g, nil
}

// Cgroup is one attempt's.
type Cgroup struct {
	path string
	// Held open to start processes in it.
	dir *os.File
}

// OutOfMemory reports whether the kernel killed anything in it for its limit.
func (g *Cgroup) OutOfMemory() bool {
	b, err := os.ReadFile(filepath.Join(g.path, "memory.events")) //nolint:gosec // a cgroup's file
	if err != nil {
		return false
	}
	for line := range strings.Lines(string(b)) {
		if k, v, ok := strings.Cut(strings.TrimSpace(line), " "); ok && (k == "oom_kill" || k == "oom_group_kill") && v != "0" {
			return true
		}
	}
	return false
}

// Close kills what's left in it and removes it.
func (g *Cgroup) Close() error {
	if g.dir != nil {
		_ = g.dir.Close()
	}
	return remove(g.path)
}

// remove kills a job's cgroup's processes and removes it, which takes the
// kernel a moment after they die.
func remove(path string) error {
	_ = write(path, "cgroup.kill", "1")
	var err error
	for range 40 {
		if err = os.RemoveAll(path); err == nil { //nolint:gosec // the operator's directory
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return err
}

// write sets one of a cgroup's files. A cgroup makes its own, and asking to
// create one it doesn't have fails differently from a missing file.
func write(dir, file, value string) error {
	f, err := os.OpenFile(filepath.Join(dir, file), os.O_WRONLY, 0) //nolint:gosec // a cgroup's file
	if err != nil {
		return err
	}
	_, err = f.WriteString(value)
	return errors.Join(err, f.Close())
}
