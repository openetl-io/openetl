package resources

import (
	"os/exec"
	"syscall"
)

// confine has cmd start in the cgroup, so nothing it does is charged elsewhere.
// It takes Linux 5.7.
func (g *Cgroup) confine(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.UseCgroupFD = true
	cmd.SysProcAttr.CgroupFD = int(g.dir.Fd())
}
