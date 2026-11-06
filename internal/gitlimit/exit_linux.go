//go:build linux

package gitlimit

import "golang.org/x/sys/unix"

// awaitExit blocks until the child with the given PID has exited, without
// reaping it, and reports true. The zombie keeps the PID from reuse until
// the caller reaps it. A false return means the child is already gone.
func awaitExit(pid int) bool {
	var info unix.Siginfo
	for {
		switch err := unix.Waitid(unix.P_PID, pid, &info, unix.WEXITED|unix.WNOWAIT, nil); err {
		case nil:
			return true
		case unix.EINTR:
		default:
			return false
		}
	}
}
