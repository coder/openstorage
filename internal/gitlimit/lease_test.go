package gitlimit

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
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
// The test runs on every git that CI runs: git must not close the lease
// descriptor before it runs a hook. It runs git directly and behind the
// heap-cap shell wrapper, which must pass the descriptor on.
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

// CombinedOutputIn returns only the last 64 KiB of the output, with
// stdout and stderr in the order the child wrote them.
func TestCombinedOutputInKeepsTheLast64KiB(t *testing.T) {
	repo := t.TempDir()
	out, err := CombinedOutputIn(t.Context(), repo, exec.Command("sh", "-c", "head -c 1048576 /dev/zero | tr '\\0' x; echo; echo the end >&2"))
	if err != nil || len(out) != 64<<10 || !strings.HasSuffix(string(out), "x\nthe end\n") {
		t.Fatalf("CombinedOutputIn = %d bytes ending %q, %v; want the last %d bytes", len(out), out[max(len(out)-12, 0):], err, 64<<10)
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

// Two starts hold the lease at once: a lease is shared, and only an exclusive
// try sees it busy.
func TestSharedLeasesCoexist(t *testing.T) {
	repo := t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	first, err := OpenLease(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	second, err := OpenLease(ctx, repo)
	if err != nil {
		t.Fatalf("a second shared lease while the first is held: %v", err)
	}
	if err := tryExclusive(t, repo); !errors.Is(err, ErrLeaseBusy) {
		t.Fatalf("TryExclusive under two shared leases = %v, want ErrLeaseBusy", err)
	}
	_ = first.Close()
	if err := tryExclusive(t, repo); !errors.Is(err, ErrLeaseBusy) {
		t.Fatalf("TryExclusive under one shared lease = %v, want ErrLeaseBusy", err)
	}
	_ = second.Close()
	if err := tryExclusive(t, repo); err != nil {
		t.Fatalf("TryExclusive with no lease held: %v", err)
	}
}

// The lease file belongs to the serve user alone.
func TestLeaseFileIsPrivate(t *testing.T) {
	repo := t.TempDir()
	// With a strict umask the test would pass for a looser mode too.
	defer syscall.Umask(syscall.Umask(0))
	l, err := OpenLease(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	fi, err := os.Stat(filepath.Join(repo, LeaseFile))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("lease file mode = %v, want 0600", fi.Mode().Perm())
	}
}

// Once StopChildren began, a start that waits for an exclusive holder gives up
// with ErrStopping at once, and does not wait out the lease bound.
func TestOpenLeaseStopsWaitingWhenChildrenAreStopping(t *testing.T) {
	repo := t.TempDir()
	held, err := TryExclusive(repo)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	children.mu.Lock()
	children.stopped = true
	children.mu.Unlock()
	t.Cleanup(func() {
		children.mu.Lock()
		children.stopped = false
		children.mu.Unlock()
	})
	done := make(chan error, 1)
	go func() {
		l, err := OpenLease(t.Context(), repo)
		if err == nil {
			_ = l.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrStopping) {
			t.Fatalf("OpenLease while stopping = %v, want ErrStopping", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OpenLease kept waiting although StopChildren began")
	}
}

// A start does not wait for an exclusive holder for ever.
func TestOpenLeaseGivesUpAfterTheLeaseWait(t *testing.T) {
	repo := t.TempDir()
	held, err := TryExclusive(repo)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	old := leaseWait
	leaseWait = 100 * time.Millisecond
	t.Cleanup(func() { leaseWait = old })
	done := make(chan error, 1)
	go func() {
		l, err := OpenLease(t.Context(), repo)
		if err == nil {
			_ = l.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrLeaseBusy) {
			t.Fatalf("OpenLease behind a hung holder = %v, want ErrLeaseBusy", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OpenLease waited far past the lease bound")
	}
}

// Only a shared lease can go to a child: an exclusive one would lock every
// later start out for the life of the child.
func TestAttachRefusesAnExclusiveLease(t *testing.T) {
	l, err := TryExclusive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	defer func() {
		if recover() == nil {
			t.Fatal("Attach of an exclusive lease did not panic")
		}
	}()
	l.Attach(exec.Command("true"))
}

// A start that fails before the child runs, because the lease wait ended or
// StopChildren began, closes the pipes that the caller made with StdinPipe and
// StdoutPipe: exec.Cmd closes them only in a Start that fails, and a
// descriptor leak per refused request would otherwise last until a garbage
// collection.
func TestStartInClosesThePipesOfAStartThatFails(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, repo string) context.Context
		want  error
	}{
		{"the lease wait ends", func(t *testing.T, repo string) context.Context {
			held, err := TryExclusive(repo)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = held.Close() })
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			t.Cleanup(cancel)
			return ctx
		}, context.DeadlineExceeded},
		{"the children are stopping", func(t *testing.T, repo string) context.Context {
			children.mu.Lock()
			children.stopped = true
			children.mu.Unlock()
			t.Cleanup(func() {
				children.mu.Lock()
				children.stopped = false
				children.mu.Unlock()
			})
			return t.Context()
		}, ErrStopping},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := t.TempDir()
			ctx := tc.setup(t, repo)
			cmd := exec.Command("cat")
			stdin, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := StartIn(ctx, repo, cmd); !errors.Is(err, tc.want) {
				t.Fatalf("StartIn = %v, want %v", err, tc.want)
			}
			if _, err := stdin.Write([]byte("x")); !errors.Is(err, os.ErrClosed) {
				t.Errorf("write to the stdin pipe of a start that failed = %v, want a closed file", err)
			}
			read := make(chan error, 1)
			go func() { _, err := stdout.Read(make([]byte, 1)); read <- err }()
			select {
			case err := <-read:
				if !errors.Is(err, os.ErrClosed) {
					t.Errorf("read from the stdout pipe of a start that failed = %v, want a closed file", err)
				}
			case <-time.After(2 * time.Second):
				t.Error("read from the stdout pipe of a start that failed blocks: the pipe is still open")
			}
		})
	}
}

// readOnlyFilesystem makes OpenLease see a read-only filesystem: every open
// that could write or create fails with EROFS, as on a read-only mount, and
// a read-only open works.
func readOnlyFilesystem(t *testing.T) {
	t.Helper()
	prev := openLeaseFile
	openLeaseFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE) != 0 {
			return nil, &os.PathError{Op: "open", Path: name, Err: syscall.EROFS}
		}
		return prev(name, flag, perm)
	}
	t.Cleanup(func() { openLeaseFile = prev })
}

// On a read-only filesystem nothing can write a ref lock, and reads there
// worked before the lease existed. With a lease file, the lease is a real
// one: the file is opened read-only, which flock allows.
func TestReadOnlyFilesystemWithALeaseFileHoldsARealLease(t *testing.T) {
	repo := t.TempDir()
	if err := tryExclusive(t, repo); err != nil {
		t.Fatal(err)
	}
	readOnlyFilesystem(t)
	l, err := OpenLease(t.Context(), repo)
	if err != nil {
		t.Fatalf("OpenLease on a read-only filesystem with a lease file = %v", err)
	}
	defer l.Close()
	if l.f == nil {
		t.Fatal("the lease holds nothing although the lease file exists")
	}
	probe, err := os.Open(filepath.Join(repo, LeaseFile))
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	if err := syscall.Flock(int(probe.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); !errors.Is(err, syscall.EWOULDBLOCK) {
		t.Fatalf("exclusive flock under the lease = %v, want EWOULDBLOCK", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(probe.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("exclusive flock after the lease closed = %v", err)
	}
}

// With no lease file the lease holds nothing, and a start there runs without
// a descriptor LeaseFD and makes no file.
func TestReadOnlyFilesystemWithoutALeaseFileStillRuns(t *testing.T) {
	repo := t.TempDir()
	readOnlyFilesystem(t)
	out, err := OutputIn(t.Context(), repo, exec.Command("sh", "-c", "echo out; test ! -e /proc/self/fd/10 || echo leased"))
	if err != nil || string(out) != "out\n" {
		t.Fatalf("OutputIn on a read-only filesystem = %q, %v, want out and no descriptor %d", out, err, LeaseFD)
	}
	if _, err := os.Lstat(filepath.Join(repo, LeaseFile)); err == nil {
		t.Fatal("a lease file appeared on a read-only filesystem")
	}
}

// Only EROFS selects the fallback. A lease file that exists but cannot be
// opened is an error, and so is any other failure: there a git can take ref
// locks, so a lease that holds nothing would hide it from TryExclusive.
func TestOpenLeaseFailsClosedOutsideAReadOnlyFilesystem(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file and directory permissions")
	}
	missing := filepath.Join(t.TempDir(), "gone")
	if l, err := OpenLease(t.Context(), missing); err == nil {
		_ = l.Close()
		t.Fatal("OpenLease of a missing directory succeeded")
	}

	// A directory that serve cannot write has no lease file to create, and
	// its refs/ can still be writable: a git there needs a real lease.
	nowrite := t.TempDir()
	if err := os.Mkdir(filepath.Join(nowrite, "refs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(nowrite, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(nowrite, 0o700) })
	marker := filepath.Join(nowrite, "refs", "ran")
	if err := StartIn(t.Context(), nowrite, exec.Command("sh", "-c", ": >"+marker)); err == nil {
		t.Fatal("StartIn in a directory without a lease file that serve cannot create succeeded")
	}
	if _, err := os.Lstat(marker); err == nil {
		t.Fatal("the child ran without a lease")
	}

	// A lease file that nobody may open.
	locked := t.TempDir()
	if err := tryExclusive(t, locked); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(locked, LeaseFile), 0); err != nil {
		t.Fatal(err)
	}
	if l, err := OpenLease(t.Context(), locked); err == nil {
		_ = l.Close()
		t.Fatal("OpenLease of a lease file with mode 0 succeeded")
	}
}

// On a read-only filesystem, a lease file that cannot even be read is an
// error, not a lease that holds nothing: only a missing file means that no
// git of serve can hold the lease.
func TestReadOnlyFilesystemWithAnUnreadableLeaseFileFails(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	repo := t.TempDir()
	if err := tryExclusive(t, repo); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(repo, LeaseFile), 0); err != nil {
		t.Fatal(err)
	}
	readOnlyFilesystem(t)
	if l, err := OpenLease(t.Context(), repo); err == nil {
		_ = l.Close()
		t.Fatal("OpenLease succeeded with a lease file that cannot be read")
	}
}

// Attach panics on a command that already has ExtraFiles: the lease must be
// the only extra descriptor, or LeaseFD would not hold it.
func TestAttachRefusesACommandWithExtraFiles(t *testing.T) {
	l, err := OpenLease(t.Context(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	cmd := exec.Command("true")
	cmd.ExtraFiles = []*os.File{os.Stdin}
	defer func() {
		if recover() == nil {
			t.Fatal("Attach on a command with ExtraFiles did not panic")
		}
	}()
	l.Attach(cmd)
}
