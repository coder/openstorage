//go:build unix

package gitread

import (
	"syscall"
	"time"
)

// cpuTime is the CPU time this process and the children it has reaped have
// used. It tells what a call costs where wall time moves with the load of the
// host.
func cpuTime() time.Duration {
	var self, kids syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &self) != nil || syscall.Getrusage(syscall.RUSAGE_CHILDREN, &kids) != nil {
		return 0
	}
	return time.Duration(self.Utime.Nano() + self.Stime.Nano() + kids.Utime.Nano() + kids.Stime.Nano())
}
