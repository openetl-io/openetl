package resources

import (
	"errors"
	"io/fs"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// Detect is what this process may use: its cgroup's limits where it has them,
// and otherwise the host's CPU and memory.
func Detect() (Size, error) {
	return detect("/sys/fs/cgroup", hostMemory)
}

func detect(root string, host func() (int64, error)) (Size, error) {
	cgroup := os.DirFS(root)
	s := Size{CPU: int64(runtime.NumCPU()) * 1000}
	if cpu := cgroupCPU(cgroup); cpu > 0 && cpu < s.CPU {
		s.CPU = cpu
	}
	mem, err := host()
	if err != nil {
		return s, err
	}
	s.Memory = mem
	if m := cgroupMemory(cgroup); m > 0 && m < s.Memory {
		s.Memory = m
	}
	if s.Memory <= 0 {
		return s, errors.New("couldn't read how much memory there is; set --capacity")
	}
	return s, nil
}

// cgroupCPU is the cgroup's CPU quota in millicores; 0 if it has none.
func cgroupCPU(cgroup fs.FS) int64 {
	// v2: "max 100000" or "200000 100000".
	if f := fields(cgroup, "cpu.max"); len(f) == 2 {
		return millis(f[0], f[1])
	}
	quota, period := fields(cgroup, "cpu/cpu.cfs_quota_us"), fields(cgroup, "cpu/cpu.cfs_period_us")
	if len(quota) == 1 && len(period) == 1 {
		return millis(quota[0], period[0])
	}
	return 0
}

func millis(quota, period string) int64 {
	q, err := strconv.ParseInt(quota, 10, 64)
	p, perr := strconv.ParseInt(period, 10, 64)
	if err != nil || perr != nil || q <= 0 || p <= 0 {
		return 0
	}
	return q * 1000 / p
}

// cgroupMemory is the cgroup's memory limit in bytes; 0 if it has none.
func cgroupMemory(cgroup fs.FS) int64 {
	for _, name := range []string{"memory.max", "memory/memory.limit_in_bytes"} {
		if f := fields(cgroup, name); len(f) == 1 {
			if n, err := strconv.ParseInt(f[0], 10, 64); err == nil && n > 0 {
				return n
			}
		}
	}
	return 0
}

// fields is the fields of name in fsys; nil if it can't be read.
func fields(fsys fs.FS, name string) []string {
	b, err := fs.ReadFile(fsys, name)
	if err != nil {
		return nil
	}
	return strings.Fields(string(b))
}
