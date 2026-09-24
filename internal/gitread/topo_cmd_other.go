//go:build !linux

package gitread

import (
	"context"
	"os/exec"
)

// HeapLimitSupported reports whether TopoRanks can enforce maxHeapBytes on
// this platform. Outside Linux there is no qualified mechanism (macOS does
// not enforce RLIMIT_DATA), so capped TopoRanks calls fail with
// ErrHeapLimitUnsupported instead of running uncapped.
func HeapLimitSupported() bool { return false }

// heapLimitedGit is only reached with maxHeapBytes == 0 here (TopoRanks
// refuses capped calls first).
func heapLimitedGit(ctx context.Context, maxHeapBytes int64, args ...string) *exec.Cmd {
	if maxHeapBytes != 0 {
		panic("gitread assertion failed: heapLimitedGit: heap cap requested on an unsupported platform")
	}
	return exec.CommandContext(ctx, "git", args...)
}

// heapLimitFailure: no cap is enforced here, so no failure is a cap hit.
func heapLimitFailure(error, string) bool { return false }
