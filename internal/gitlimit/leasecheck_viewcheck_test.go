//go:build openstorage_viewcheck

package gitlimit

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func mustPanic(t *testing.T, want string, f func()) {
	t.Helper()
	defer func() {
		t.Helper()
		got := recover()
		if got == nil {
			t.Fatalf("no panic, want one containing %q", want)
		}
		if msg, _ := got.(string); !strings.Contains(msg, want) {
			t.Fatalf("panic %v, want it to contain %q", got, want)
		}
	}()
	f()
}

func gitIn(dir string, args ...string) *exec.Cmd {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
	return cmd
}

// A binary built with the tag fails a git start that runs in a repository
// without the lease, so a missed call site shows up in the tests that
// exercise it, not as a deleted live ref lock.
func TestViewCheckRefusesAnUnleasedStartInARepository(t *testing.T) {
	repo := bareRepo(t)

	mustPanic(t, "without a lease", func() { _ = Start(gitIn(repo, "for-each-ref")) })
	mustPanic(t, "without a lease", func() { _ = Run(gitIn("", "upload-pack", "--stateless-rpc", repo)) })

	if err := Run(gitIn("", "version")); err != nil {
		t.Fatalf("git version, which reads no repository: %v", err)
	}
}

func TestViewCheckRefusesALeaseOfAnotherRepository(t *testing.T) {
	repo, other := bareRepo(t), bareRepo(t)

	mustPanic(t, "under the lease of", func() { _ = StartIn(t.Context(), repo, gitIn(other, "for-each-ref")) })

	if err := RunIn(t.Context(), repo, gitIn(repo, "for-each-ref")); err != nil {
		t.Fatalf("RunIn in its own repository: %v", err)
	}
}
