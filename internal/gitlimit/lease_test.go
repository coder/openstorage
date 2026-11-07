package gitlimit

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
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
// before it runs a hook. It runs git directly and behind the heap-cap shell
// wrapper, which must pass the descriptor on.
func TestLeaseLivesWithTheGitProcessTree(t *testing.T) {
	t.Run("direct", func(t *testing.T) {
		leaseLivesWithTheTree(t, func(args ...string) *exec.Cmd { return exec.Command("git", args...) })
	})
	t.Run("heap cap", func(t *testing.T) {
		if !HeapLimitSupported() {
			t.Skip("no heap cap on this platform")
		}
		leaseLivesWithTheTree(t, func(args ...string) *exec.Cmd { return Command(t.Context(), HeavyHeapBytes, args...) })
	})
}

func leaseLivesWithTheTree(t *testing.T, gitCmd func(args ...string) *exec.Cmd) {
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

	cmd := gitCmd("update-ref", "refs/heads/x", string(sha[:40]))
	cmd.Dir = repo
	cmd.Env = env
	if err := StartIn(t.Context(), repo, cmd); err != nil {
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
		done <- RunIn(t.Context(), repo, cmd)
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
	if out, err := OutputIn(t.Context(), repo, exec.Command("sh", "-c", "echo out")); err != nil || string(out) != "out\n" {
		t.Fatalf("OutputIn = %q, %v", out, err)
	}
	if out, err := CombinedOutputIn(t.Context(), repo, exec.Command("sh", "-c", "echo out; echo err >&2")); err != nil || len(out) != len("out\nerr\n") {
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
	if err := RunIn(t.Context(), repo, exec.Command("/nonexistent/git")); err == nil {
		t.Fatal("RunIn of a missing program succeeded")
	}
	if err := tryExclusive(t, repo); err != nil {
		t.Fatalf("TryExclusive after a failed start = %v", err)
	}
}

// The first process that opens a repository's lease creates its epoch, and
// no later open moves it: a ref lock older than the epoch comes from a git
// that held no lease.
func TestTheFirstOpenCreatesTheEpochAndNoLaterOpenMovesIt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		first func(repo string) error
	}{
		{"OpenLease", func(repo string) error {
			l, err := OpenLease(t.Context(), repo)
			if err == nil {
				_ = l.Close()
			}
			return err
		}},
		{"TryExclusive", func(repo string) error {
			l, err := TryExclusive(repo)
			if err == nil {
				_ = l.Close()
			}
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := t.TempDir()
			if _, err := LeaseEpoch(repo); !errors.Is(err, ErrNoEpoch) {
				t.Fatalf("LeaseEpoch before any open = %v, want ErrNoEpoch", err)
			}
			before := time.Now().Add(-2 * time.Second)
			if err := tc.first(repo); err != nil {
				t.Fatal(err)
			}
			epoch, err := LeaseEpoch(repo)
			if err != nil {
				t.Fatal(err)
			}
			if epoch.Before(before) || epoch.After(time.Now().Add(2*time.Second)) {
				t.Fatalf("epoch %v is not the time of the first open", epoch)
			}

			old := time.Now().Add(-48 * time.Hour).Truncate(time.Second)
			if err := os.Chtimes(filepath.Join(repo, EpochFile), old, old); err != nil {
				t.Fatal(err)
			}
			for _, open := range []func(string) error{
				func(repo string) error { l, err := OpenLease(t.Context(), repo); closeLease(l); return err },
				func(repo string) error { l, err := TryExclusive(repo); closeLease(l); return err },
			} {
				if err := open(repo); err != nil {
					t.Fatal(err)
				}
			}
			if got, err := LeaseEpoch(repo); err != nil || !got.Equal(old) {
				t.Fatalf("epoch after later opens = %v (%v), want it unchanged at %v", got, err, old)
			}
		})
	}
}

func closeLease(l *Lease) {
	if l != nil {
		_ = l.Close()
	}
}

// A lease epoch that is not a regular file is an error, not "no epoch": the
// repository was tampered with, and cleanup must not read a time from it.
func TestLeaseEpochRefusesAnythingButARegularFile(t *testing.T) {
	repo := t.TempDir()
	target := filepath.Join(t.TempDir(), "elsewhere")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(repo, EpochFile)); err != nil {
		t.Fatal(err)
	}
	if _, err := LeaseEpoch(repo); err == nil || errors.Is(err, ErrNoEpoch) {
		t.Fatalf("LeaseEpoch of a symlink = %v, want an error that is not ErrNoEpoch", err)
	}
}

// A start that waits for an exclusive holder honors its context: a canceled
// request or an expired budget ends the wait, and the child never starts.
func TestStartInStopsWaitingWhenItsContextEnds(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  func(t *testing.T) (context.Context, func())
		want error
	}{
		{"canceled", func(t *testing.T) (context.Context, func()) {
			ctx, cancel := context.WithCancel(t.Context())
			return ctx, func() { time.Sleep(50 * time.Millisecond); cancel() }
		}, context.Canceled},
		{"deadline", func(t *testing.T) (context.Context, func()) {
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			t.Cleanup(cancel)
			return ctx, func() {}
		}, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := t.TempDir()
			held, err := TryExclusive(repo)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			marker := filepath.Join(t.TempDir(), "ran")
			ctx, end := tc.ctx(t)
			done := make(chan error, 1)
			go func() { done <- RunIn(ctx, repo, exec.Command("sh", "-c", ": >"+marker)) }()
			end()
			select {
			case err := <-done:
				if !errors.Is(err, tc.want) {
					t.Fatalf("RunIn = %v, want %v", err, tc.want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("RunIn kept waiting for the lease after its context ended")
			}
			if _, err := os.Stat(marker); err == nil {
				t.Fatal("the child ran")
			}
		})
	}
}

// The lease sits on LeaseFD and the descriptors below it stay closed, because
// Git reads GIT_TRACE=3 to 9 as open trace descriptors and would write its
// trace into the lease file.
func TestTheLeaseIsOnlyOnLeaseFD(t *testing.T) {
	repo := t.TempDir()
	// A redirection to a closed descriptor ends the shell that runs it, so
	// each probe runs in a subshell. A shell redirects only the descriptors 0
	// to 9, so /proc shows the lease's.
	script := `for n in 3 4 5 6 7 8 9; do if ( true >&$n ) 2>/dev/null; then exit $n; fi; done`
	if runtime.GOOS == "linux" {
		script += "\ntest -e /proc/$$/fd/" + strconv.Itoa(LeaseFD) + " || exit 20"
	}
	if err := RunIn(t.Context(), repo, exec.Command("sh", "-c", script)); err != nil {
		t.Fatalf("descriptors in the child: %v", err)
	}
}

func TestGitTraceToADescriptorBelowTenNeverReachesTheLease(t *testing.T) {
	repo := bareRepo(t)
	leaseSize := func() int64 {
		fi, err := os.Stat(filepath.Join(repo, LeaseFile))
		if err != nil {
			t.Fatal(err)
		}
		return fi.Size()
	}
	traced := func() *exec.Cmd {
		cmd := exec.Command("git", "-C", repo, "rev-parse", "--git-dir")
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_TRACE=3")
		return cmd
	}

	// Control: the lease on descriptor 3 receives git's trace. Without it the
	// test would pass if this git wrote no trace at all.
	l, err := OpenLease(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	control := traced()
	control.ExtraFiles = []*os.File{l.f}
	if err := control.Run(); err != nil {
		t.Fatalf("control: %v", err)
	}
	_ = l.Close()
	if leaseSize() == 0 {
		t.Fatal("control: git wrote no trace into a lease on descriptor 3")
	}
	if err := os.Truncate(filepath.Join(repo, LeaseFile), 0); err != nil {
		t.Fatal(err)
	}

	if err := RunIn(t.Context(), repo, traced()); err != nil {
		t.Fatalf("git with GIT_TRACE=3: %v", err)
	}
	if got := leaseSize(); got != 0 {
		t.Fatalf("git wrote %d bytes of trace into the lease file", got)
	}
}
