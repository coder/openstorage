package gitread

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// childPIDs lists live (non-zombie) children of this test process. The
// budget tests are sequential, so no other test's git is running.
func childPIDs(t *testing.T) []int {
	t.Helper()
	ents, err := os.ReadDir("/proc")
	if err != nil {
		t.Skipf("no /proc: %v", err)
	}
	self := os.Getpid()
	var pids []int
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		raw, err := os.ReadFile(filepath.Join("/proc", e.Name(), "stat"))
		if err != nil {
			continue // exited meanwhile
		}
		// Fields after the ")" that ends comm: state ppid ...
		rest := strings.Fields(string(raw[strings.LastIndexByte(string(raw), ')')+1:]))
		if len(rest) < 2 || rest[0] == "Z" {
			continue
		}
		if ppid, _ := strconv.Atoi(rest[1]); ppid == self {
			pids = append(pids, pid)
		}
	}
	return pids
}

func assertNoChildren(t *testing.T) {
	t.Helper()
	if pids := childPIDs(t); len(pids) > 0 {
		t.Fatalf("git children still running after the call returned: %v", pids)
	}
}

// fakeGit puts a `git` shell script first on PATH for this test.
func fakeGit(t *testing.T, script string) {
	t.Helper()
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte("#!/bin/sh\n"+script+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// deepHistory builds a linear history of n commits with fast-import. Every
// commit rewrites hot.txt; cold.txt exists only in the root commit.
func deepHistory(t *testing.T, n int) (dir, tip, root string) {
	t.Helper()
	dir = t.TempDir()
	mustGit(t, dir, "init", "-q", "--bare")
	var in strings.Builder
	for i := range n {
		fmt.Fprintf(&in, "commit refs/heads/main\nmark :%d\ncommitter B <b@example.com> %d +0000\ndata 2\nc\n", i+1, 1700000000+i)
		if i > 0 {
			fmt.Fprintf(&in, "from :%d\n", i)
		} else {
			in.WriteString("M 100644 inline cold.txt\ndata 5\ncold\n")
		}
		fmt.Fprintf(&in, "M 100644 inline hot.txt\ndata %d\n%d\n", len(fmt.Sprint(i))+1, i)
	}
	cmd := exec.Command("git", "fast-import", "--quiet")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), gitEnv...)
	cmd.Stdin = strings.NewReader(in.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fast-import: %v\n%s", err, out)
	}
	tip = strings.TrimSpace(mustGit(t, dir, "rev-parse", "main"))
	root = strings.TrimSpace(mustGit(t, dir, "rev-list", "--max-parents=0", "main"))
	return dir, tip, root
}

func testBudget(slots *Slots, timeout time.Duration, output int64) *Budget {
	return NewBudget(slots, "repo", timeout, output)
}

// A deep history makes the per-file walk for a file from the root commit
// allocate in proportion to history length (the #313 attack shape).
func TestRunHeapCapStopsDeepPathWalk(t *testing.T) {
	if !HeapLimitSupported() {
		t.Skip("heap cap is Linux-only")
	}
	dir, tip, _ := deepHistory(t, 20000)
	b := testBudget(NewSlots(1, 1, 0), time.Minute, DefaultOutputBytes).
		WithLimits(Limits{HeavyHeap: 4 << 20, LightHeap: LightHeapBytes, MaxBlob: MaxBlobBytes})
	ctx := WithBudget(context.Background(), b)
	_, err := Log(ctx, dir, tip, LogOptions{Path: "cold.txt", MaxCount: 1})
	if !errors.Is(err, ErrMemoryLimit) || !IsBudgetError(err) {
		t.Fatalf("err = %v, want ErrMemoryLimit", err)
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		t.Fatalf("limit error wraps *exec.ExitError (%v): exit-status checks could misread it", exitErr)
	}
	b.Close()
	assertNoChildren(t)
	// The same walk fits under the default cap.
	b2 := testBudget(NewSlots(1, 1, 0), time.Minute, DefaultOutputBytes)
	defer b2.Close()
	commits, err := Log(WithBudget(context.Background(), b2), dir, tip, LogOptions{Path: "cold.txt", MaxCount: 1})
	if err != nil || len(commits) != 1 {
		t.Fatalf("default cap: commits = %d, err = %v", len(commits), err)
	}
}

func TestRunAppliesClassHeapCapsBeforeExec(t *testing.T) {
	if !HeapLimitSupported() {
		t.Skip("heap cap is Linux-only")
	}
	out := filepath.Join(t.TempDir(), "limit")
	fakeGit(t, "ulimit -d >> "+out)
	dir := t.TempDir()
	b := testBudget(NewSlots(1, 1, 0), time.Minute, DefaultOutputBytes)
	ctx := WithBudget(context.Background(), b)
	if _, err := Run(ctx, Cmd{Class: Heavy, Dir: dir, Args: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, Cmd{Class: Light, Dir: dir, Args: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	b.Close()
	// Without a Budget (write endpoints) Run applies no limit.
	if _, err := Run(context.Background(), Cmd{Class: Heavy, Dir: dir, Args: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	want := strconv.FormatInt(HeavyHeapBytes>>10, 10) + "\n" + strconv.FormatInt(LightHeapBytes>>10, 10) + "\nunlimited\n"
	if string(got) != want {
		t.Fatalf("git inherited RLIMIT_DATA %q KiB, want %q", got, want)
	}
}

// childEnvOf runs a fake `git` that prints its environment and returns the
// effective variables it saw.
func childEnvOf(t *testing.T, ctx context.Context, env []string) map[string]string {
	t.Helper()
	out, err := Run(ctx, Cmd{Class: Heavy, Dir: t.TempDir(), Env: env, Args: []string{"x"}})
	if err != nil {
		t.Fatal(err)
	}
	vars := map[string]string{}
	for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		if _, dup := vars[k]; dup {
			t.Fatalf("git saw %s twice", k)
		}
		vars[k] = v
	}
	return vars
}

// heapLimitFailure matches git's English diagnostics, so a translated
// locale must not reach git's messages (#339). Only LC_MESSAGES is
// pinned: LC_CTYPE/LC_COLLATE drive git grep's regex matching.
func TestRunPinsGitMessagesToCLocale(t *testing.T) {
	fakeGit(t, "env")
	t.Setenv("LANG", "de_DE.UTF-8")
	t.Setenv("LANGUAGE", "de")
	t.Setenv("LC_MESSAGES", "de_DE.UTF-8")
	t.Setenv("LC_CTYPE", "fr_FR.UTF-8")
	t.Setenv("LC_ALL", "de_DE.UTF-8")
	check := func(name string, got map[string]string, want map[string]string) {
		t.Helper()
		if v, ok := got["LC_ALL"]; ok {
			t.Errorf("%s: git saw LC_ALL=%q; it overrides LC_MESSAGES", name, v)
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("%s: git saw %s=%q, want %q", name, k, got[k], v)
			}
		}
	}
	// The inherited LC_ALL keeps its value on every other category.
	inherited := map[string]string{"LC_MESSAGES": "C", "LC_CTYPE": "de_DE.UTF-8", "LC_COLLATE": "de_DE.UTF-8", "LC_TIME": "de_DE.UTF-8", "LANG": "de_DE.UTF-8"}
	check("no budget", childEnvOf(t, context.Background(), nil), inherited)
	b := testBudget(NewSlots(1, 1, 0), time.Minute, DefaultOutputBytes)
	defer b.Close()
	ctx := WithBudget(context.Background(), b) // Linux: behind the heap-cap shell
	check("budget", childEnvOf(t, ctx, nil), inherited)

	// A caller's own Env is kept, not replaced by the server's.
	env := []string{"PATH=" + os.Getenv("PATH"), "MARKER=kept", "LC_ALL=pt_BR.UTF-8", "LC_MESSAGES=pt_BR.UTF-8"}
	orig := append([]string(nil), env...)
	for name, c := range map[string]context.Context{"caller env": context.Background(), "caller env, budget": ctx} {
		got := childEnvOf(t, c, env)
		check(name, got, map[string]string{"MARKER": "kept", "LC_MESSAGES": "C", "LC_CTYPE": "pt_BR.UTF-8"})
		if _, ok := got["LANG"]; ok {
			t.Errorf("%s: git saw the server's LANG with a caller Env", name)
		}
	}
	if strings.Join(env, "\n") != strings.Join(orig, "\n") {
		t.Fatalf("Run modified the caller's Env: %q", env)
	}

	// Without LC_ALL the other categories are untouched.
	got := childEnvOf(t, context.Background(), []string{"PATH=" + os.Getenv("PATH"), "LANG=de_DE.UTF-8"})
	check("no LC_ALL", got, map[string]string{"LANG": "de_DE.UTF-8", "LC_MESSAGES": "C"})
	if v, ok := got["LC_CTYPE"]; ok {
		t.Errorf("no LC_ALL: git saw LC_CTYPE=%q, want it unset", v)
	}
}

func TestRunOutputLimitKillsGit(t *testing.T) {
	dir, tip, _ := deepHistory(t, 5000)
	b := testBudget(NewSlots(1, 1, 0), time.Minute, 64<<10)
	ctx := WithBudget(context.Background(), b)
	start := time.Now()
	_, err := Log(ctx, dir, tip, LogOptions{})
	if !errors.Is(err, ErrOutputLimit) {
		t.Fatalf("err = %v, want ErrOutputLimit", err)
	}
	assertNoChildren(t)
	// The allowance is per request: a second child is refused too.
	if _, err := Log(ctx, dir, tip, LogOptions{MaxCount: 1000}); !errors.Is(err, ErrOutputLimit) {
		t.Fatalf("second call: err = %v, want ErrOutputLimit", err)
	}
	b.Close()
	t.Logf("overflow detected after %v", time.Since(start))
}

func TestRunTimeBudgetKillsAndReapsGit(t *testing.T) {
	fakeGit(t, "exec sleep 30")
	b := testBudget(NewSlots(1, 1, 0), 200*time.Millisecond, DefaultOutputBytes)
	ctx := WithBudget(context.Background(), b)
	start := time.Now()
	_, err := Run(ctx, Cmd{Class: Heavy, Dir: t.TempDir(), Args: []string{"log"}})
	if !errors.Is(err, ErrTimeBudget) {
		t.Fatalf("err = %v, want ErrTimeBudget", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("time budget of 200ms took %v to fire", d)
	}
	assertNoChildren(t)
	// The budget is spent: the next child is refused without starting.
	marker := filepath.Join(t.TempDir(), "ran")
	fakeGit(t, "touch "+marker)
	if _, err := Run(ctx, Cmd{Class: Light, Dir: t.TempDir(), Args: []string{"x"}}); !errors.Is(err, ErrTimeBudget) {
		t.Fatalf("after deadline: err = %v, want ErrTimeBudget", err)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("git started after the time budget ran out")
	}
	b.Close()
}

func TestRunBusyFailsBeforeStartingGit(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	fakeGit(t, "echo x >> "+marker)
	slots := NewSlots(1, 1, 50*time.Millisecond)
	holder := testBudget(slots, time.Minute, DefaultOutputBytes)
	hctx := WithBudget(context.Background(), holder)
	if _, err := Run(hctx, Cmd{Class: Heavy, Dir: t.TempDir(), Args: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	if slots.InUse() != 1 {
		t.Fatalf("slots in use = %d, want 1 (held until the request ends)", slots.InUse())
	}
	// A second Heavy child of the same request reuses its slot.
	if _, err := Run(hctx, Cmd{Class: Heavy, Dir: t.TempDir(), Args: []string{"x"}}); err != nil {
		t.Fatal(err)
	}

	other := testBudget(slots, time.Minute, DefaultOutputBytes)
	octx := WithBudget(context.Background(), other)
	if _, err := Run(octx, Cmd{Class: Heavy, Dir: t.TempDir(), Args: []string{"x"}}); !errors.Is(err, ErrBusy) {
		t.Fatalf("err = %v, want ErrBusy", err)
	}
	// Light-only requests need the slot too: they retain output as well.
	if _, err := Run(octx, Cmd{Class: Light, Dir: t.TempDir(), Args: []string{"x"}}); !errors.Is(err, ErrBusy) {
		t.Fatalf("light child: err = %v, want ErrBusy", err)
	}
	raw, _ := os.ReadFile(marker)
	if n := strings.Count(string(raw), "x"); n != 2 {
		t.Fatalf("git ran %d times, want 2 (busy children must not start)", n)
	}
	holder.Close()
	if _, err := Run(octx, Cmd{Class: Heavy, Dir: t.TempDir(), Args: []string{"x"}}); err != nil {
		t.Fatalf("after the holder closed: %v", err)
	}
	other.Close()
	if slots.InUse() != 0 {
		t.Fatalf("slots in use after Close = %d", slots.InUse())
	}
}

// Limit outcomes must never be reported as "not found" or "no match".
func TestLimitErrorsAreNotMaskedAsMissing(t *testing.T) {
	if !HeapLimitSupported() {
		t.Skip("heap cap is Linux-only")
	}
	f := newFixture(t)
	tiny := Limits{HeavyHeap: 64 << 10, LightHeap: 64 << 10, MaxBlob: MaxBlobBytes}
	ctx := WithBudget(context.Background(), testBudget(NewSlots(1, 1, 0), time.Minute, DefaultOutputBytes).WithLimits(tiny))
	if _, err := ResolveRef(ctx, f.dir, "main", false); !errors.Is(err, ErrMemoryLimit) || errors.Is(err, ErrNotFound) {
		t.Fatalf("ResolveRef: err = %v, want ErrMemoryLimit and not ErrNotFound", err)
	}
	if _, err := Grep(ctx, f.dir, f.c2, GrepOptions{Pattern: "no-such-text"}); !errors.Is(err, ErrMemoryLimit) {
		t.Fatalf("Grep: err = %v, want ErrMemoryLimit, not an empty result", err)
	}
	if _, err := Note(ctx, f.dir, "refs/notes/commits", f.c2); !errors.Is(err, ErrMemoryLimit) || errors.Is(err, ErrNotFound) {
		t.Fatalf("Note: err = %v, want ErrMemoryLimit and not ErrNotFound", err)
	}
	assertNoChildren(t)
}

// git grep defaults to one thread per CPU, and RLIMIT_DATA counts each
// thread's stack: unpinned, it cannot start under the heavy cap on a large
// machine. grep.threads=1 keeps it far below.
func TestGrepRunsUnderHeavyCap(t *testing.T) {
	f := newFixture(t)
	b := testBudget(NewSlots(1, 1, 0), time.Minute, DefaultOutputBytes).
		WithLimits(Limits{HeavyHeap: 16 << 20, LightHeap: LightHeapBytes, MaxBlob: MaxBlobBytes})
	files, err := Grep(WithBudget(context.Background(), b), f.dir, f.c2, GrepOptions{Pattern: "line"})
	if err != nil || len(files) == 0 {
		t.Fatalf("files = %v, err = %v", files, err)
	}
	b.Close()
}

func TestPrepareArchiveRejectsOversizedBlobUpFront(t *testing.T) {
	f := newFixture(t)
	b := testBudget(NewSlots(1, 1, 0), time.Minute, DefaultOutputBytes).
		WithLimits(Limits{HeavyHeap: HeavyHeapBytes, LightHeap: LightHeapBytes, MaxBlob: 10})
	ctx := WithBudget(context.Background(), b)
	_, err := PrepareArchive(ctx, f.dir, f.c2, ArchiveOptions{})
	if !errors.Is(err, ErrBlobTooLarge) {
		t.Fatalf("err = %v, want ErrBlobTooLarge", err)
	}
	// Filtering the large blobs out (max_blob_size) makes it valid again.
	if _, err := PrepareArchive(ctx, f.dir, f.c2, ArchiveOptions{MaxBlobSize: 10}); err != nil {
		t.Fatalf("with max_blob_size: %v", err)
	}
	b.Close()
}

// A history walk read incrementally (the shape of a `rev-list
// --topo-order` rank walk) runs through Run as a Heavy streaming child:
// the consumer ends git early with ErrStop, Run reaps it and reports no
// error, and the request's slot and deadline applied throughout.
func TestRunStreamingWalkStopsEarly(t *testing.T) {
	dir, tip, _ := deepHistory(t, 5000)
	slots := NewSlots(1, 1, 0)
	b := testBudget(slots, time.Minute, 1) // 1 byte: streaming output is not buffered or counted
	ctx := WithBudget(context.Background(), b)
	pr, pw := io.Pipe()
	seen := make(chan int, 1)
	go func() {
		sc := bufio.NewScanner(pr)
		n := 0
		for sc.Scan() {
			if n++; n == 100 {
				break
			}
		}
		seen <- n
		_ = pr.CloseWithError(ErrStop) // the next Write returns ErrStop
	}()
	_, err := Run(ctx, Cmd{Class: Heavy, Dir: dir, Stdout: pw, Args: []string{"rev-list", "--topo-order", tip}})
	_ = pw.Close()
	if err != nil {
		t.Fatalf("stopped walk: err = %v, want nil", err)
	}
	if n := <-seen; n != 100 {
		t.Fatalf("consumer saw %d commits, want 100", n)
	}
	if slots.InUse() != 1 {
		t.Fatal("the walk did not hold its request's slot")
	}
	assertNoChildren(t)
	b.Close()
}

// Non-Linux policy (ADR-049), exercised on Linux: where the heap cap
// cannot be enforced, Run runs git without it instead of refusing.
func TestRunWithoutHeapCapSupportStillRuns(t *testing.T) {
	saved := heapLimitAvailable
	heapLimitAvailable = func() bool { return false }
	t.Cleanup(func() { heapLimitAvailable = saved })
	out := filepath.Join(t.TempDir(), "limit")
	fakeGit(t, "ulimit -d > "+out)
	b := testBudget(NewSlots(1, 1, 0), time.Minute, DefaultOutputBytes)
	if _, err := Run(WithBudget(context.Background(), b), Cmd{Class: Heavy, Dir: t.TempDir(), Args: []string{"x"}}); err != nil {
		t.Fatalf("err = %v, want the read to run", err)
	}
	b.Close()
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(got)) != "unlimited" {
		t.Fatalf("git ran under RLIMIT_DATA %q, want no cap", got)
	}
}

// blockingWriter blocks every Write until release is closed.
type blockingWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}

// A client that stops reading an archive keeps no git process alive:
// Stream spools each blob before writing it. (The request keeps its slot
// until the HTTP layer's write deadlines end it.)
func TestArchiveStreamStalledReaderKeepsNoGitProcess(t *testing.T) {
	f := newFixture(t)
	slots := NewSlots(1, 1, 0)
	b := testBudget(slots, time.Minute, DefaultOutputBytes)
	ctx := WithBudget(context.Background(), b)
	prepared, err := PrepareArchive(ctx, f.dir, f.c2, ArchiveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	w := &blockingWriter{entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- prepared.Stream(ctx, w) }()
	select {
	case <-w.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("Stream never wrote")
	}
	assertNoChildren(t)
	if n := slots.InUse(); n != 1 {
		t.Fatalf("slots in use = %d, want 1 (held until the request ends)", n)
	}
	close(w.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	b.Close()
}

// A request can put a value of 100 KB into a git argument, and the error
// name of Run goes to the log: it must not carry the value.
func TestRunErrorNamesLongArgumentsBriefly(t *testing.T) {
	fakeGit(t, "echo boom >&2; exit 3")
	long := strings.Repeat("a", 100_000)
	_, err := Run(context.Background(), Cmd{Class: Light, Dir: t.TempDir(), Args: []string{"rev-parse", "--verify", long}})
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 3 {
		t.Fatalf("err = %v, want git's exit status 3", err)
	}
	msg := err.Error()
	if len(msg) > 1024 {
		t.Fatalf("the error is %d bytes, want it bounded: %.200s", len(msg), msg)
	}
	for _, want := range []string{"git rev-parse --verify aaaa", "...(100000 bytes)", "boom"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %.300q lacks %q", msg, want)
		}
	}
}
