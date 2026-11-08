package resources

import (
	"fmt"

	"golang.org/x/sys/unix"

	"github.com/openetl-io/openetl/internal/platform/ints"
)

func hostMemory() (int64, error) {
	n, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0, err
	}
	m, ok := ints.Int64(n)
	if !ok {
		return 0, fmt.Errorf("hw.memsize is %d bytes, more than can be counted", n)
	}
	return m, nil
}
