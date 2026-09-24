//go:build linux

package gitread

import (
	"context"
	"errors"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

// heapLimitedGit returns `git args...`. When maxHeapBytes > 0 it runs git
// behind /bin/sh, which sets RLIMIT_DATA (`ulimit -d`, KiB) and then execs
// git, so the limit is in force before git allocates anything and the
// process id stays git's (context kill and Wait still reach git). If the
// limit cannot be set, the shell exits 125 and git never runs.
func heapLimitedGit(ctx context.Context, maxHeapBytes int64, args ...string) *exec.Cmd {
	if maxHeapBytes == 0 {
		return exec.CommandContext(ctx, "git", args...)
	}
	kib := max(maxHeapBytes/1024, 1)
	wrapper := append([]string{"-c", `ulimit -d "$1" || exit 125; shift; exec git "$@"`, "sh", strconv.FormatInt(kib, 10)}, args...)
	return exec.CommandContext(ctx, "/bin/sh", wrapper...)
}

// heapLimitFailure reports whether a git run under a heap cap died of it:
// git's own "Out of memory" fatal, or, when the cap is hit before or
// outside git's checked allocators (dynamic loader, libc), a crash by
// SIGSEGV, SIGBUS, or SIGABRT. SIGKILL (context cancellation or deadline)
// is not a heap failure.
func heapLimitFailure(waitErr error, stderr string) bool {
	if strings.Contains(stderr, "Out of memory") || strings.Contains(stderr, "Cannot allocate memory") {
		return true
	}
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) {
		return false
	}
	ws, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !ws.Signaled() {
		return false
	}
	switch ws.Signal() {
	case syscall.SIGSEGV, syscall.SIGBUS, syscall.SIGABRT:
		return true
	}
	return false
}
