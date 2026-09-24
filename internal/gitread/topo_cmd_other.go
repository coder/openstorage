//go:build !linux

package gitread

import (
	"context"
	"os/exec"
)

// heapLimitedGit returns `git args...`. The heap cap is enforced on Linux
// only (topo_cmd_linux.go); elsewhere maxHeapBytes is ignored.
func heapLimitedGit(ctx context.Context, _ int64, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, "git", args...)
}

// heapLimitFailure: no cap is enforced here, so no failure is a cap hit.
func heapLimitFailure(error, string) bool { return false }
