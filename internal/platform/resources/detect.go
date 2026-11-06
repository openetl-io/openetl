package resources

import (
	"errors"
	"os"
	"path/filepath"
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
	s := Size{CPU: int64(runtime.NumCPU()) * 1000}
	if cpu := cgroupCPU(root); cpu > 0 && cpu < s.CPU {
		s.CPU = cpu
	}
	mem, err := host()
	if err != nil {
		return s, err
	}
	s.Memory = mem
	if m := cgroupMemory(root); m > 0 && m < s.Memory {
		s.Memory = m
	}
	if s.Memory <= 0 {
		return s, errors.New("couldn't read how much memory there is; set --capacity")
	}
	return s, nil
}

// cgroupCPU is the cgroup's CPU quota in millicores; 0 if it has none.
func cgroupCPU(root string) int64 {
	// v2: "max 100000" or "200000 100000".
	if f := fields(filepath.Join(root, "cpu.max")); len(f) == 2 {
		return millis(f[0], f[1])
	}
	quota, period := fields(filepath.Join(root, "cpu", "cpu.cfs_quota_us")), fields(filepath.Join(root, "cpu", "cpu.cfs_period_us"))
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
func cgroupMemory(root string) int64 {
	for _, path := range []string{filepath.Join(root, "memory.max"), filepath.Join(root, "memory", "memory.limit_in_bytes")} {
		if f := fields(path); len(f) == 1 {
			if n, err := strconv.ParseInt(f[0], 10, 64); err == nil && n > 0 {
				return n
			}
		}
	}
	return 0
}

func fields(path string) []string {
	b, err := os.ReadFile(path) //nolint:gosec // cgroup files
	if err != nil {
		return nil
	}
	return strings.Fields(string(b))
}
