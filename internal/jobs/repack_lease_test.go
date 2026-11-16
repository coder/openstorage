package jobs

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/gitlimit"
)

// fakeGitPath puts a `git` first on PATH that exits at once and leaves a
// sleeper behind. The sleeper is a child of the git, so it holds whatever
// descriptors the git got, and it outlives the call that started the git: the
// lease is visible after the call returned, with no help from the parent's
// own copy. It returns the directory that holds one file per git run, named
// by its process id, with the sleeper's id inside.
func fakeGitPath(t *testing.T) (pids string) {
	t.Helper()
	dir := t.TempDir()
	pids = filepath.Join(dir, "pids")
	if err := os.Mkdir(pids, 0o700); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\n( exec sleep 60 </dev/null >/dev/null 2>&1 ) &\necho $! > \"$FAKEGIT_PIDS/$$\"\nsleep 0.2\nexit 0\n"
	if err := os.WriteFile(filepath.Join(dir, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FAKEGIT_PIDS", pids)
	t.Cleanup(func() {
		for _, pid := range sleepers(t, pids) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})
	return pids
}

func sleepers(t *testing.T, pids string) []int {
	t.Helper()
	entries, err := os.ReadDir(pids)
	if err != nil {
		t.Fatal(err)
	}
	var out []int
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(pids, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
		if err != nil {
			t.Fatalf("sleeper pid %q: %v", b, err)
		}
		out = append(out, pid)
	}
	return out
}

// leaseFollowsTheChild checks what runGitStdin and streamShowIndex owe the
// lease. The git child holds it after the call returned, because its
// sleeper does. Once the sleeper is gone the parent holds nothing, so an
// exclusive try wins: a lease that the parent never closed would stay busy.
func leaseFollowsTheChild(t *testing.T, repo, pids string) {
	t.Helper()
	got := sleepers(t, pids)
	if len(got) != 1 {
		t.Fatalf("the fake git ran %d times, want 1", len(got))
	}
	l, err := gitlimit.TryExclusive(repo)
	if !errors.Is(err, gitlimit.ErrLeaseBusy) {
		if err == nil {
			_ = l.Close()
		}
		t.Fatalf("TryExclusive while the git's child lives = %v, want ErrLeaseBusy: the git did not get the lease", err)
	}
	if err := syscall.Kill(got[0], syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		l, err := gitlimit.TryExclusive(repo)
		if err == nil {
			_ = l.Close()
			return
		}
		if !errors.Is(err, gitlimit.ErrLeaseBusy) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("the lease is still busy after the git's child exited: the parent kept its copy")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunGitStdinHoldsTheLeaseInTheChildAndReleasesItsOwn(t *testing.T) {
	repo := t.TempDir()
	pids := fakeGitPath(t)
	if _, err := runGitStdin(t.Context(), repo, strings.NewReader(""), "pack-objects", "-q"); err != nil {
		t.Fatal(err)
	}
	leaseFollowsTheChild(t, repo, pids)
}

func TestStreamShowIndexHoldsTheLeaseInTheChildAndReleasesItsOwn(t *testing.T) {
	repo := t.TempDir()
	pids := fakeGitPath(t)
	if err := streamShowIndex(t.Context(), repo, strings.NewReader(""), func(string) {}); err != nil {
		t.Fatal(err)
	}
	leaseFollowsTheChild(t, repo, pids)
}
