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

// HeapLimitSupported reports whether Run can cap a git child's heap on this
// platform (RLIMIT_DATA via `ulimit -d`).
func HeapLimitSupported() bool { return true }

// heapLimitWrapper is the /bin/sh script heapLimitedGit runs: $1 is the
// cap in KiB, the remaining arguments are git's. It only ever LOWERS the
// limit: when the inherited soft limit is already at or below the cap, it
// is kept (raising toward the cap past an inherited hard limit fails with
// EPERM). Lowering soft and hard together is always permitted because the
// hard limit is at least the soft limit.
const heapLimitWrapper = `cur=$(ulimit -S -d) || exit 125
if [ "$cur" = unlimited ] || [ "$cur" -gt "$1" ]; then ulimit -d "$1" || exit 125; fi
shift; exec git "$@"`

// heapLimitedGit returns `git args...`. When maxHeapBytes > 0 it runs git
// behind /bin/sh, which sets RLIMIT_DATA (`ulimit -d`, KiB) and then execs
// git, so the limit is in force before git allocates anything and the
// process id stays git's (context kill and Wait still reach git). An
// inherited stricter limit is kept. If the limit cannot be read or set,
// the shell exits 125 and git never runs.
//
// RLIMIT_DATA counts the heap and other private writable mappings, which
// includes every thread's stack and malloc arena; read-only pack mappings
// (shared page cache) are not counted.
func heapLimitedGit(ctx context.Context, maxHeapBytes int64, args ...string) *exec.Cmd {
	if maxHeapBytes == 0 {
		return exec.CommandContext(ctx, "git", args...)
	}
	kib := max(maxHeapBytes/1024, 1)
	wrapper := append([]string{"-c", heapLimitWrapper, "sh", strconv.FormatInt(kib, 10)}, args...)
	return exec.CommandContext(ctx, "/bin/sh", wrapper...)
}

// heapLimitFailure reports whether a git run under a heap cap died of it:
// git's fatal allocation diagnostics, or, when the cap is hit before or
// outside git's checked allocators (dynamic loader, libc), a crash by
// SIGSEGV, SIGBUS, or SIGABRT. SIGKILL (context cancellation or deadline)
// is not a heap failure.
//
// Only a fatal line that STARTS with a known diagnostic counts: git
// messages can quote request input (a path named "out of memory"), and
// such text never forms the start of the line.
func heapLimitFailure(waitErr error, stderr string) bool {
	for line := range strings.SplitSeq(stderr, "\n") {
		for _, prefix := range heapFailurePrefixes {
			if strings.HasPrefix(line, prefix) {
				return true
			}
		}
		// die_errno("mmap failed%s") ends with strerror(ENOMEM); the %s
		// is git's own hint text.
		if strings.HasPrefix(line, "fatal: mmap failed") && strings.HasSuffix(line, ": Cannot allocate memory") {
			return true
		}
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

// heapFailurePrefixes are git's fatal allocation diagnostics (git 2.54
// and 2.55): every x*alloc/xstrdup/xmmap "Out of memory..." die, and the
// zlib wrappers' "inflateInit: out of memory (no message)" family.
var heapFailurePrefixes = []string{
	"fatal: Out of memory",
	"fatal: inflateInit: out of memory",
	"fatal: inflate: out of memory",
	"fatal: deflateInit: out of memory",
	"fatal: deflate: out of memory",
}
