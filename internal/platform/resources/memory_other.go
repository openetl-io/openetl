//go:build !linux && !darwin

package resources

import "errors"

func hostMemory() (int64, error) {
	return 0, errors.New("couldn't read how much memory there is; set --capacity")
}
