//go:build !linux

package resources

import "os/exec"

func (g *Cgroup) confine(*exec.Cmd) {}
