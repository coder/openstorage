//go:build darwin

package gitlimit

import (
	"os/exec"
	"sync"
	"syscall"
	"unsafe"
)

// pPID is waitid's idtype for one process ID (sys/wait.h).
const pPID = 1

// waitidNoWait calls waitid(P_PID, pid, ..., WEXITED|WNOWAIT) through the
// raw system call: neither Go's syscall package nor golang.org/x/sys/unix
// wraps waitid on macOS, and Go's own os package does not use it there.
func waitidNoWait(pid int) syscall.Errno {
	var info [128]byte // siginfo_t is 104 bytes
	for {
		_, _, errno := syscall.Syscall6(syscall.SYS_WAITID, pPID, uintptr(pid), uintptr(unsafe.Pointer(&info[0])), syscall.WEXITED|syscall.WNOWAIT, 0, 0)
		if errno != syscall.EINTR {
			return errno
		}
	}
}

var hold struct {
	once sync.Once
	ok   bool
}

// leaderHoldSupported reports whether waitid(WNOWAIT) works here. It checks
// once on a child that exits at once: the call must return and leave the
// child waitable. A waitid that ignored WNOWAIT would reap the child behind
// the registry's back, so the check fails closed and the registry reaps first.
func leaderHoldSupported() bool {
	hold.once.Do(func() {
		cmd := exec.Command("/usr/bin/true")
		if cmd.Start() != nil {
			return
		}
		if errno := waitidNoWait(cmd.Process.Pid); errno != 0 {
			_ = cmd.Wait()
			return
		}
		hold.ok = cmd.Wait() == nil
	})
	return hold.ok
}

// awaitExit blocks until the child with the given PID has exited, without
// reaping it, and reports true. The zombie keeps the PID from reuse until
// the caller reaps it. A false return means this platform cannot do it, or
// the child is already gone.
func awaitExit(pid int) bool {
	return leaderHoldSupported() && waitidNoWait(pid) == 0
}
