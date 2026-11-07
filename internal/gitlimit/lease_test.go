package gitlimit

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// bareRepo makes an empty bare repository in a temp dir.
func bareRepo(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "r.git")
	cmd := exec.Command("git", "init", "-q", "--bare", dir)
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, out)
	}
	return dir
}

// eventually polls until cond holds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func tryExclusive(t *testing.T, repo string) error {
	t.Helper()
	l, err := TryExclusive(repo)
	if err == nil {
		_ = l.Close()
	}
	return err
}

// A git that serve starts holds the lease with its whole process tree, after
// serve closed its own copy, and the lease ends with the tree. The tree is
// the real shape of #464: update-ref takes a ref lock, runs its
// reference-transaction hook in the "prepared" state, and the hook sleeps.
// The test runs on every git that CI runs: git must not close descriptor 3
// before it runs a hook.
func TestLeaseLivesWithTheGitProcessTree(t *testing.T) {
	repo := bareRepo(t)
	ready := filepath.Join(t.TempDir(), "ready")
	hook := "#!/bin/sh\ncat >/dev/null\nif [ \"$1\" = prepared ]; then : >\"$READY\"; exec sleep 60; fi\n"
	if err := os.WriteFile(filepath.Join(repo, "hooks", "reference-transaction"), []byte(hook), 0o755); err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "READY="+ready)
	commit := exec.Command("git", "-C", repo, "commit-tree", "4b825dc642cb6eb9a060e54bf8d69288fbee4904", "-m", "x")
	commit.Env = append(env, "GIT_AUTHOR_NAME=a", "GIT_AUTHOR_EMAIL=a@example.com", "GIT_COMMITTER_NAME=a", "GIT_COMMITTER_EMAIL=a@example.com")
	sha, err := commit.Output()
	if err != nil {
		t.Fatalf("commit-tree: %v", err)
	}

	cmd := exec.Command("git", "update-ref", "refs/heads/x", string(sha[:40]))
	cmd.Dir = repo
	cmd.Env = env
	if err := StartIn(repo, cmd); err != nil {
		t.Fatal(err)
	}
	pgid := cmd.Process.Pid
	t.Cleanup(func() { _ = syscall.Kill(-pgid, syscall.SIGKILL); _ = Wait(cmd) })

	eventually(t, "the hook to park inside the transaction", func() bool {
		_, err := os.Stat(ready)
		return err == nil
	})
	if _, err := os.Stat(filepath.Join(repo, "refs", "heads", "x.lock")); err != nil {
		t.Fatalf("the parked transaction holds no ref lock: %v", err)
	}
	if err := tryExclusive(t, repo); !errors.Is(err, ErrLeaseBusy) {
		t.Fatalf("TryExclusive while the tree lives = %v, want ErrLeaseBusy", err)
	}

	// The lease belongs to the tree, not to its leader: kill git, and the
	// hook that git left behind keeps it.
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = Wait(cmd)
	if err := tryExclusive(t, repo); !errors.Is(err, ErrLeaseBusy) {
		t.Fatalf("TryExclusive after only the leader died = %v, want ErrLeaseBusy", err)
	}

	if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the lease to end with the tree", func() bool { return tryExclusive(t, repo) == nil })
	if _, err := os.Stat(filepath.Join(repo, "refs", "heads", "x.lock")); err != nil {
		t.Fatalf("the killed transaction left no stale lock to clean: %v", err)
	}
}

// While a holder has the lease exclusively, a start waits, and then runs.
// Nothing starts under the exclusive lease, so a cleanup that holds it sees
// no new git child.
func TestStartInWaitsForAnExclusiveHolder(t *testing.T) {
	repo := t.TempDir()
	held, err := TryExclusive(repo)
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "ran")
	done := make(chan error, 1)
	go func() {
		cmd := exec.Command("sh", "-c", ": >"+marker)
		done <- RunIn(repo, cmd)
	}()
	select {
	case err := <-done:
		t.Fatalf("RunIn returned while the lease was held exclusively: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("the child ran under the exclusive lease")
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunIn after the release: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("RunIn did not start after the exclusive lease ended")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("the child did not run: %v", err)
	}
}

// The In forms close the parent's copy: the lease ends when the child exits,
// and the file stays, readable by the owner only.
func TestLeaseEndsWithTheChildAndTheFileStays(t *testing.T) {
	repo := t.TempDir()
	if out, err := OutputIn(repo, exec.Command("sh", "-c", "echo out")); err != nil || string(out) != "out\n" {
		t.Fatalf("OutputIn = %q, %v", out, err)
	}
	if out, err := CombinedOutputIn(repo, exec.Command("sh", "-c", "echo out; echo err >&2")); err != nil || len(out) != len("out\nerr\n") {
		t.Fatalf("CombinedOutputIn = %q, %v", out, err)
	}
	if err := tryExclusive(t, repo); err != nil {
		t.Fatalf("TryExclusive after the children exited = %v", err)
	}
	fi, err := os.Stat(filepath.Join(repo, LeaseFile))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("lease file mode %v, want 0600", fi.Mode().Perm())
	}
}

// A start that fails leaves no lease behind.
func TestFailedStartReleasesTheLease(t *testing.T) {
	repo := t.TempDir()
	if err := RunIn(repo, exec.Command("/nonexistent/git")); err == nil {
		t.Fatal("RunIn of a missing program succeeded")
	}
	if err := tryExclusive(t, repo); err != nil {
		t.Fatalf("TryExclusive after a failed start = %v", err)
	}
}
