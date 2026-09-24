//go:build !linux

package gitread

import (
	"context"
	"os/exec"
)

// HeapLimitSupported reports whether Run can cap a git child's heap on this
// platform. Outside Linux there is no qualified mechanism (macOS does not
// enforce RLIMIT_DATA). Run then runs git without a heap cap, under the
// admission, time, and output limits only (ADR-048); it never refuses a
// read because the cap is unavailable.
func HeapLimitSupported() bool { return false }

// heapLimitedGit is only reached with maxHeapBytes == 0 here: Run drops the
// cap first where HeapLimitSupported is false.
func heapLimitedGit(ctx context.Context, maxHeapBytes int64, args ...string) *exec.Cmd {
	assertf(maxHeapBytes == 0, "heapLimitedGit: heap cap %d requested on an unsupported platform", maxHeapBytes)
	return exec.CommandContext(ctx, "git", args...)
}

// heapLimitFailure: no cap is enforced here, so no failure is a cap hit.
func heapLimitFailure(error, string) bool { return false }
