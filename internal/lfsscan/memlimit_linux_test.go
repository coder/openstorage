package lfsscan

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestLimitGitHeap (Codex security round 20): the walk subprocess heap cap
// must actually land as RLIMIT_DATA on the child — the kernel then fails
// the allocations a giant compressible tree needs before its children ever
// stream to the metered stages. Asserted through /proc/<pid>/limits, the
// kernel's own view of the applied limit.
func TestLimitGitHeap(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()
	if err := limitGitHeap(cmd.Process.Pid); err != nil {
		t.Fatalf("limitGitHeap: %v", err)
	}
	limits, err := os.ReadFile(fmt.Sprintf("/proc/%d/limits", cmd.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("%d", int64(lfsScanGitHeapLimit))
	for _, line := range strings.Split(string(limits), "\n") {
		if !strings.HasPrefix(line, "Max data size") {
			continue
		}
		if !strings.Contains(line, want) {
			t.Fatalf("data rlimit not applied: %q, want %s", line, want)
		}
		return
	}
	t.Fatalf("no 'Max data size' row in /proc limits:\n%s", limits)
}
