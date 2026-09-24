//go:build linux

package gitread

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestHeapLimitWrapperKeepsStricterInheritedLimit runs the exact wrapper
// script under an outer shell that sets an inherited RLIMIT_DATA (soft and
// hard), with a fake `git` that reports the limit it got. The wrapper must
// only ever lower the limit: an inherited stricter hard limit is kept, not
// "raised" (which fails with EPERM and would turn every request into 500).
func TestHeapLimitWrapperKeepsStricterInheritedLimit(t *testing.T) {
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte("#!/bin/sh\necho \"$(ulimit -S -d) $(ulimit -H -d)\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, tc := range []struct {
		name, inherited, want string
	}{
		{"no inherited limit", "", "131072 131072"},
		{"stricter inherited limit kept", "65536", "65536 65536"},
		{"looser inherited limit lowered", "262144", "131072 131072"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outer := `exec /bin/sh -c "$0" sh 131072 rev-list`
			if tc.inherited != "" {
				outer = "ulimit -S -d " + tc.inherited + " && ulimit -H -d " + tc.inherited + " && " + outer
			}
			cmd := exec.Command("/bin/sh", "-c", outer, heapLimitWrapper)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("wrapper failed: %v\n%s", err, out)
			}
			if got := strings.TrimSpace(string(out)); got != tc.want {
				t.Fatalf("git saw RLIMIT_DATA soft/hard %q KiB, want %q", got, tc.want)
			}
		})
	}
}

// Only git's own allocation diagnostics count as a heap failure: git
// errors that quote request input ("no such path out of memory") must keep
// their ordinary meaning.
func TestHeapLimitFailureMatchesOnlyGitDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		stderr string
		want   bool
	}{
		{"fatal: Out of memory, malloc failed (tried to allocate 4096 bytes)", true},
		{"warning: x\nfatal: inflateInit: out of memory (no message)", true},
		{"fatal: mmap failed: Cannot allocate memory", true},
		{"fatal: no such path out of memory in 1111111111111111111111111111111111111111", false},
		{"fatal: no such path Out of memory, malloc failed in HEAD", false},
		{"fatal: path 'fatal: Out of memory' does not exist", false},
		{"fatal: bad revision 'Cannot allocate memory'", false},
		{"", false},
	} {
		if got := heapLimitFailure(errors.New("exit status 128"), tc.stderr); got != tc.want {
			t.Errorf("heapLimitFailure(%q) = %v, want %v", tc.stderr, got, tc.want)
		}
	}
}
