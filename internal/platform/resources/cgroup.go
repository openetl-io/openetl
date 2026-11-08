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
// which belongs to one runner. It holds the directory open.
type Cgroups struct {
	root *os.Root
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

func openCgroups(dir string, pid int) (_ *Cgroups, err error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("%s isn't a cgroup v2 directory delegated to this runner: %w", dir, err)
	}
	defer func() {
		if err != nil {
			_ = root.Close()
		}
	}()
	have := fields(root.FS(), "cgroup.controllers")
	if !slices.Contains(have, "cpu") || !slices.Contains(have, "memory") {
		return nil, fmt.Errorf("%s isn't a cgroup v2 directory with the cpu and memory controllers, which it needs delegated to this runner", dir)
	}
	if slices.Contains(fields(root.FS(), "cgroup.procs"), strconv.Itoa(pid)) {
		if err := root.MkdirAll(runnerName, 0o750); err != nil {
			return nil, err
		}
		if err := write(root, filepath.Join(runnerName, "cgroup.procs"), strconv.Itoa(pid)); err != nil {
			return nil, fmt.Errorf("move this runner out of %s: %w", dir, err)
		}
	}
	if err := write(root, "cgroup.subtree_control", "+cpu +memory"); err != nil {
		return nil, fmt.Errorf("enable cpu and memory in %s, which takes having no other process in it: %w", dir, err)
	}
	old, err := fs.Glob(root.FS(), jobPrefix+"*")
	if err != nil {
		return nil, err
	}
	for _, name := range old {
		if err := remove(root, name); err != nil {
			return nil, fmt.Errorf("remove what an earlier runner left: %w", err)
		}
	}
	return &Cgroups{root: root}, nil
}

// Job makes the cgroup of an attempt of this size. Going over its ceiling kills
// all of its processes, which then count as out of memory.
func (c *Cgroups) Job(id string, size Size) (*Cgroup, error) {
	name := jobPrefix + url.PathEscape(id)
	if err := c.root.MkdirAll(name, 0o750); err != nil {
		return nil, err
	}
	g := &Cgroup{root: c.root, name: name}
	err := errors.Join(
		write(c.root, filepath.Join(name, "cpu.max"), fmt.Sprintf("%d %d", size.CPU*cpuPeriod/1000, cpuPeriod)),
		write(c.root, filepath.Join(name, "memory.max"), strconv.FormatInt(size.MemoryCeiling(), 10)),
		write(c.root, filepath.Join(name, "memory.oom.group"), "1"),
	)
	// Without swap accounting there's no such file, and nothing to limit.
	if err2 := write(c.root, filepath.Join(name, "memory.swap.max"), "0"); !errors.Is(err2, fs.ErrNotExist) {
		err = errors.Join(err, err2)
	}
	if err == nil {
		g.dir, err = c.root.Open(name)
	}
	if err != nil {
		_ = g.Close()
		return nil, err
	}
	return g, nil
}

// Cgroup is one attempt's: name in root.
type Cgroup struct {
	root *os.Root
	name string
	// Held open to start processes in it.
	dir *os.File
}

// OutOfMemory reports whether the kernel killed anything in it for its limit.
func (g *Cgroup) OutOfMemory() bool {
	b, err := g.root.ReadFile(filepath.Join(g.name, "memory.events"))
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
	return remove(g.root, g.name)
}

// remove kills a job's cgroup's processes and removes it, which takes the
// kernel a moment after they die.
func remove(root *os.Root, name string) error {
	_ = write(root, filepath.Join(name, "cgroup.kill"), "1")
	var err error
	for range 40 {
		if err = root.RemoveAll(name); err == nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return err
}

// write sets one of a cgroup's files. A cgroup makes its own, and asking to
// create one it doesn't have fails differently from a missing file.
func write(root *os.Root, file, value string) error {
	f, err := root.OpenFile(file, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	_, err = f.WriteString(value)
	return errors.Join(err, f.Close())
}
