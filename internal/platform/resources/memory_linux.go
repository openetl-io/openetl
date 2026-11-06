package resources

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
)

func hostMemory() (int64, error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	for s := bufio.NewScanner(f); s.Scan(); {
		if rest, ok := strings.CutPrefix(s.Text(), "MemTotal:"); ok {
			var kb int64
			if _, err := fmt.Sscanf(strings.TrimSpace(rest), "%d kB", &kb); err != nil {
				return 0, err
			}
			return kb * 1024, nil
		}
	}
	return 0, errors.New("/proc/meminfo has no MemTotal")
}
