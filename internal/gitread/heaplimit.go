package gitread

import (
	"context"
	"os/exec"

	"github.com/coder/openstorage/internal/gitlimit"
)

// HeapLimitSupported reports whether Run can cap a git child's heap on this
// platform (Linux: RLIMIT_DATA). Outside Linux, Run runs git without a heap
// cap, under the admission, time, and output limits only (ADR-049); it
// never refuses a read because the cap is unavailable.
func HeapLimitSupported() bool { return gitlimit.HeapLimitSupported() }

// heapLimitedGit returns `git args...` under a heap cap (gitlimit.Command).
func heapLimitedGit(ctx context.Context, maxHeapBytes int64, args ...string) *exec.Cmd {
	return gitlimit.Command(ctx, maxHeapBytes, args...)
}

// heapLimitFailure reports whether a git run under a heap cap died of it
// (gitlimit.HeapFailure).
func heapLimitFailure(waitErr error, stderr string) bool {
	return gitlimit.HeapFailure(waitErr, stderr)
}
