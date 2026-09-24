package api

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fakeSlowGit puts a `git` on PATH that records its pid and then execs
// sleep, so the pid is the process TopoRanks must kill and reap.
func fakeSlowGit(t *testing.T) (pidFile string) {
	t.Helper()
	bin := t.TempDir()
	pidFile = filepath.Join(bin, "pid")
	// Write-then-rename so a reader never sees a partial pid.
	script := "#!/bin/sh\necho $$ > " + pidFile + ".tmp && mv " + pidFile + ".tmp " + pidFile + "\nexec sleep 30\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	return pidFile
}

// requireReaped asserts the fake git's pid no longer exists: killed AND
// waited for, not a zombie (a zombie still answers kill(pid, 0)).
func requireReaped(t *testing.T, pidFile string) {
	t.Helper()
	raw, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("fake git never started: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 0 {
		t.Fatalf("bad pid %q", raw)
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("git pid %d still exists after ranks returned (kill 0: %v)", pid, err)
	}
}

// skipWithoutOrderWalk skips a test that needs a 200 from files/metadata
// on platforms where the order walk fails closed (no enforceable heap cap).
func skipWithoutOrderWalk(t *testing.T) {
	t.Helper()
	if !topoHeapLimitSupported() {
		t.Skip("files/metadata fails closed (503) where the walk's heap cap cannot be enforced")
	}
}

func requireFree(t *testing.T, b *topoWalkBudget) {
	t.Helper()
	if n := len(b.slots); n != 0 {
		t.Fatalf("%d walk slots still held", n)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if n := len(b.active); n != 0 {
		t.Fatalf("%d repositories still marked walking: %v", n, b.active)
	}
}

const topoTestSHA = "1111111111111111111111111111111111111111"

func TestTopoWalkBudgetSaturatedRejectsWithoutGit(t *testing.T) {
	b := newTopoWalkBudget(2, time.Minute, 0)
	b.slots <- struct{}{}
	b.slots <- struct{}{}
	// A nonexistent repo path proves no git process is started.
	_, err := b.ranks(context.Background(), "repo_a", "/nonexistent/repo", topoTestSHA, []string{topoTestSHA})
	if !errors.Is(err, errTopoWalkBusy) {
		t.Fatalf("err = %v, want errTopoWalkBusy", err)
	}
	if len(b.slots) != 2 {
		t.Fatalf("rejected call changed held slots to %d", len(b.slots))
	}
}

func TestTopoWalkBudgetDeadlineKillsAndReapsGit(t *testing.T) {
	pidFile := fakeSlowGit(t)
	b := newTopoWalkBudget(1, 300*time.Millisecond, 0)
	start := time.Now()
	_, err := b.ranks(context.Background(), "repo_a", t.TempDir(), topoTestSHA, []string{topoTestSHA})
	if !errors.Is(err, errTopoWalkTimeout) {
		t.Fatalf("err = %v, want errTopoWalkTimeout", err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("ranks returned after %v; the deadline did not stop git", d)
	}
	requireReaped(t, pidFile)
	requireFree(t, b)
}

func TestTopoWalkBudgetCallerCancelKillsAndReapsGit(t *testing.T) {
	pidFile := fakeSlowGit(t)
	b := newTopoWalkBudget(1, time.Minute, 0)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			if _, err := os.Stat(pidFile); err == nil {
				cancel()
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	_, err := b.ranks(ctx, "repo_a", t.TempDir(), topoTestSHA, []string{topoTestSHA})
	if err == nil || errors.Is(err, errTopoWalkTimeout) || !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("err = %v, want a cancellation error that is not errTopoWalkTimeout", err)
	}
	requireReaped(t, pidFile)
	requireFree(t, b)
}

func TestTopoWalkBudgetSuccessReleasesSlot(t *testing.T) {
	dir := t.TempDir()
	coreGit(t, dir, "init", "-q", "-b", "main", ".")
	coreGit(t, dir, "commit", "-q", "--allow-empty", "-m", "one")
	coreGit(t, dir, "commit", "-q", "--allow-empty", "-m", "two")
	tip, parent := coreRevParse(t, dir, "HEAD"), coreRevParse(t, dir, "HEAD~1")
	b := newTopoWalkBudget(1, time.Minute, 0)
	ranks, err := b.ranks(context.Background(), "repo_a", dir, tip, []string{parent, tip})
	if err != nil || len(ranks) != 2 || ranks[tip] != 0 || ranks[parent] != 1 {
		t.Fatalf("ranks = %v, err = %v", ranks, err)
	}
	requireFree(t, b)
}

// TestFilesMetadataTopoWalkBusy503 saturates the process-wide budget and
// checks that files/metadata answers a prompt 503 with Retry-After (never a
// 200 without `order`), then succeeds once capacity returns.
func TestFilesMetadataTopoWalkBusy503(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := coreSeedFixture(t, e, e.newOrg(t))
		for range cap(topoWalks.slots) {
			topoWalks.slots <- struct{}{}
		}
		drained := false
		drain := func() {
			if drained {
				return
			}
			drained = true
			for range cap(topoWalks.slots) {
				<-topoWalks.slots
			}
		}
		t.Cleanup(drain)

		resp, body := coreGet(t, e, "GET", "/api/v1/repos/files/metadata", fx.token, nil)
		coreRequireStatus(t, resp, body, http.StatusServiceUnavailable)
		if resp.Header.Get("Retry-After") != "1" {
			t.Errorf("Retry-After = %q, want 1", resp.Header.Get("Retry-After"))
		}
		var env errorEnvelope
		coreDecode(t, body, &env)
		if env.Error == "" {
			t.Errorf("503 body %q has no error message", body)
		}

		drain()
		skipWithoutOrderWalk(t)
		resp, body = coreGet(t, e, "GET", "/api/v1/repos/files/metadata", fx.token, nil)
		coreRequireStatus(t, resp, body, http.StatusOK)
	})
}

// TestFilesMetadataTopoWalkTooLarge503: when the walk exceeds the heap
// limit, files/metadata answers 503 WITHOUT Retry-After (retrying the same
// commit cannot succeed) and never a 200 without `order`.
func TestFilesMetadataTopoWalkTooLarge503(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the heap limit is enforced on Linux only")
	}
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := coreSeedFixture(t, e, e.newOrg(t))
		saved := topoWalks.heapLimitBytes
		topoWalks.heapLimitBytes = 64 << 10 // far below what git needs to start
		t.Cleanup(func() { topoWalks.heapLimitBytes = saved })

		resp, body := coreGet(t, e, "GET", "/api/v1/repos/files/metadata", fx.token, nil)
		coreRequireStatus(t, resp, body, http.StatusServiceUnavailable)
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			t.Errorf("Retry-After = %q, want none", ra)
		}
		requireFree(t, topoWalks)

		topoWalks.heapLimitBytes = saved
		resp, body = coreGet(t, e, "GET", "/api/v1/repos/files/metadata", fx.token, nil)
		coreRequireStatus(t, resp, body, http.StatusOK)
	})
}

// TestFilesMetadataTopoWalkUnsupportedPlatform503: where the heap cap
// cannot be enforced (non-Linux), files/metadata fails closed with 503 and
// no Retry-After, starting no git and holding no slot.
func TestFilesMetadataTopoWalkUnsupportedPlatform503(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		fx := coreSeedFixture(t, e, e.newOrg(t))
		saved := topoHeapLimitSupported
		topoHeapLimitSupported = func() bool { return false }
		t.Cleanup(func() { topoHeapLimitSupported = saved })

		resp, body := coreGet(t, e, "GET", "/api/v1/repos/files/metadata", fx.token, nil)
		coreRequireStatus(t, resp, body, http.StatusServiceUnavailable)
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			t.Errorf("Retry-After = %q, want none", ra)
		}
		requireFree(t, topoWalks)

		topoHeapLimitSupported = saved
		if !saved() {
			return // real non-Linux platform: the endpoint stays 503
		}
		resp, body = coreGet(t, e, "GET", "/api/v1/repos/files/metadata", fx.token, nil)
		coreRequireStatus(t, resp, body, http.StatusOK)
	})
}

// TestTopoWalkBudgetOneWalkPerRepo: one repository can hold at most one
// walk slot, so it cannot starve other repositories (security review
// round 4). A second walk for the same repository is busy without taking a
// global slot or starting git; another repository still walks; the
// repository is free again once its walk returns.
func TestTopoWalkBudgetOneWalkPerRepo(t *testing.T) {
	pidFile := fakeSlowGit(t)
	b := newTopoWalkBudget(4, time.Minute, 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := b.ranks(ctx, "repo_a", t.TempDir(), topoTestSHA, []string{topoTestSHA})
		done <- err
	}()
	for {
		if _, err := os.Stat(pidFile); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if _, err := b.ranks(context.Background(), "repo_a", "/nonexistent/repo", topoTestSHA, []string{topoTestSHA}); !errors.Is(err, errTopoWalkBusy) {
		t.Fatalf("second walk for repo_a: err = %v, want errTopoWalkBusy", err)
	}
	if n := len(b.slots); n != 1 {
		t.Fatalf("busy same-repo call left %d slots held, want 1", n)
	}

	// Another repository is admitted while repo_a walks: it starts its own
	// (fake, slow) git, which its short caller deadline then stops.
	ctxB, cancelB := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancelB()
	if _, err := b.ranks(ctxB, "repo_b", t.TempDir(), topoTestSHA, []string{topoTestSHA}); err == nil || errors.Is(err, errTopoWalkBusy) {
		t.Fatalf("repo_b: err = %v, want a non-busy walk error", err)
	}
	if n := len(b.slots); n != 1 {
		t.Fatalf("after repo_b returned, %d slots held, want 1 (repo_a)", n)
	}

	cancel()
	if err := <-done; err == nil {
		t.Fatal("cancelled repo_a walk returned no error")
	}
	requireReaped(t, pidFile)
	requireFree(t, b)
}
