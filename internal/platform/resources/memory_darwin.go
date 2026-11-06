package resources

import "golang.org/x/sys/unix"

func hostMemory() (int64, error) {
	n, err := unix.SysctlUint64("hw.memsize")
	return int64(n), err //nolint:gosec // no machine has 2^63 bytes
}
