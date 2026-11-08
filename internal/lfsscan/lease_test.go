package lfsscan

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/gitlimit"
)

// fakeGitPath puts a `git` first on PATH that exits after a moment and
// leaves a sleeper behind. The sleeper is a child of the git, so it holds
// whatever descriptors the git got, and it outlives the scan: the lease is
// visible after ReachableLFSOIDs returned, one stage at a time. It returns
// the directory that holds one file per git run, with the sleeper's process
// id inside.
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

func busy(t *testing.T, repo string) bool {
	t.Helper()
	l, err := gitlimit.TryExclusive(repo)
	if err == nil {
		_ = l.Close()
		return false
	}
	if !errors.Is(err, gitlimit.ErrLeaseBusy) {
		t.Fatal(err)
	}
	return true
}

// All three stages of the scan are git children in the cache repository, and
// each holds the lease on its own. The fake git of every stage leaves a
// sleeper, and each sleeper must hold descriptor LeaseFD on the lease file:
// a stage that missed the lease shows here whatever the other stages hold.
// After the scan returned, the lease stays busy until the last sleeper is
// gone, and then the scan's own copy is gone too: a lease that the scan kept
// shows there.
func TestReachableLFSOIDsLeaseFollowsEveryStage(t *testing.T) {
	if _, err := os.Stat("/proc/self/fd"); err != nil {
		t.Skip("no /proc/self/fd")
	}
	repo := t.TempDir()
	pids := fakeGitPath(t)
	if _, _, err := ReachableLFSOIDs(t.Context(), repo, 1<<20); err != nil {
		t.Fatal(err)
	}
	stages := sleepers(t, pids)
	if len(stages) != 3 {
		t.Fatalf("the fake git ran %d times, want 3 (rev-list and two cat-file)", len(stages))
	}
	leaseFile, err := os.Stat(filepath.Join(repo, gitlimit.LeaseFile))
	if err != nil {
		t.Fatal(err)
	}
	for _, pid := range stages {
		fd, err := os.Stat("/proc/" + strconv.Itoa(pid) + "/fd/" + strconv.Itoa(gitlimit.LeaseFD))
		if err != nil || !os.SameFile(fd, leaseFile) {
			t.Fatalf("the child %d of a stage does not hold the lease file as descriptor %d: %v", pid, gitlimit.LeaseFD, err)
		}
	}
	for i, pid := range stages {
		if !busy(t, repo) {
			t.Fatalf("the lease is free with the children of %d stage(s) still alive", len(stages)-i)
		}
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
		// A killed child leaves its descriptor at once, but give the kernel
		// a moment to reap it before the next look.
		time.Sleep(50 * time.Millisecond)
	}
	deadline := time.Now().Add(5 * time.Second)
	for busy(t, repo) {
		if time.Now().After(deadline) {
			t.Fatal("the lease is still busy after the last stage's child exited: the scan kept its copy")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A scan that cannot take the lease leaves no pipe open. The stages' pipes
// are made after the lease, because exec.Cmd closes them only in a Start that
// runs, and a refused scan would otherwise leak ten descriptors until a
// garbage collection.
func TestReachableLFSOIDsLeavesNoPipeOpenWhenTheLeaseFails(t *testing.T) {
	if _, err := os.Stat("/proc/self/fd"); err != nil {
		t.Skip("no /proc/self/fd")
	}
	openFDs := func() int {
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		return len(entries)
	}
	repo := t.TempDir()
	held, err := gitlimit.TryExclusive(repo)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	defer debug.SetGCPercent(debug.SetGCPercent(-1)) // a collection would close leaked pipes
	before := openFDs()
	if _, _, err := ReachableLFSOIDs(ctx, repo, 1<<20); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ReachableLFSOIDs while the lease is held = %v, want the deadline error", err)
	}
	if after := openFDs(); after != before {
		t.Fatalf("open descriptors: %d before, %d after a scan that failed on the lease", before, after)
	}
}
