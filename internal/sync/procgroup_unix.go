//go:build unix

package sync

import (
	"os/exec"
	"syscall"
)

// setProcGroup places cmd in its own process group and makes context
// cancellation kill the WHOLE group. `git push` delegates the actual
// transfer to a git-remote-https child: killing only the parent (Go's
// default) would let that child finish publishing refs after the run was
// canceled — defeating the mid-push detach watchdog — and its open pipe
// ends would block cmd.Wait until the transfer ended on its own.
func setProcGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil // Cancel only fires after Start; guard regardless
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
