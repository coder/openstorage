// Commit-graph refresh tests (issue #307, ADR-47): a full hydrate writes a
// split commit-graph, a failed or slow write never fails the hydrate, the
// child Git is isolated from inherited GIT_* state, and the time budget
// falls back to a graph without Bloom filters. The fake-Git tests drive
// the process lifecycle deterministically; the rest use real Git.
package githttp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/blob"
)

// seedHistory appends n commits to refs/heads/main in the bare repo at
// repoPath with fast-import (continuing from the current tip when one
// exists) and returns the new tip. Every commit rewrites hot.txt and one of
// 20 files under dir/, so path-limited walks have real tree diffs.
func seedHistory(tb testing.TB, repoPath string, n int) string {
	tb.Helper()
	var in strings.Builder
	parent, err := tryGit(repoPath, "rev-parse", "--verify", "-q", "refs/heads/main")
	parent = strings.TrimSpace(parent)
	for i := range n {
		fmt.Fprintf(&in, "commit refs/heads/main\ncommitter B <b@example.com> %d +0000\ndata 2\nc\n", 1700000000+i)
		if i == 0 && err == nil && parent != "" {
			fmt.Fprintf(&in, "from %s\n", parent)
		}
		fmt.Fprintf(&in, "M 100644 inline hot.txt\ndata %d\n%d\n", len(fmt.Sprint(i))+1, i)
		fmt.Fprintf(&in, "M 100644 inline dir/f%d.txt\ndata %d\n%d\n", i%20, len(fmt.Sprint(i))+1, i)
	}
	cmd := exec.Command("git", "fast-import", "--quiet")
	cmd.Dir = repoPath
	cmd.Stdin = strings.NewReader(in.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		tb.Fatalf("fast-import: %v\n%s", err, out)
	}
	out, err := tryGit(repoPath, "rev-parse", "refs/heads/main")
	if err != nil {
		tb.Fatalf("rev-parse: %v\n%s", err, out)
	}
	return strings.TrimSpace(out)
}

// repoTips returns the distinct SHAs the repo's refs point at, sorted.
func repoTips(tb testing.TB, repoPath string) []string {
	tb.Helper()
	out, err := tryGit(repoPath, "for-each-ref", "--format=%(objectname)")
	if err != nil {
		tb.Fatalf("for-each-ref: %v\n%s", err, out)
	}
	set := map[string]struct{}{}
	for f := range strings.FieldsSeq(out) {
		set[f] = struct{}{}
	}
	return slices.Sorted(maps.Keys(set))
}

// tipsOf builds the refTips a sync of these SHAs would collect.
func tipsOf(shas ...string) *refTips {
	t := newRefTips()
	for _, s := range shas {
		t.add(s)
	}
	return t
}

// commitGraphChain returns the split chain's layer hashes (nil when absent).
func commitGraphChain(t testing.TB, repoPath string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoPath, "objects", "info", "commit-graphs", "commit-graph-chain"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Fields(string(b))
}

// layerHasBloom reports whether a split layer carries changed-path Bloom
// filter data (the BDAT chunk id sits in the chunk table at the file head).
func layerHasBloom(t testing.TB, repoPath, layer string) bool {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoPath, "objects", "info", "commit-graphs", "graph-"+layer+".graph"))
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Contains(b[:min(len(b), 4096)], []byte("BDAT"))
}

// setCommitGraphBudgets overrides the refresh budgets for one test.
func setCommitGraphBudgets(t *testing.T, deadline, first, kill time.Duration) {
	t.Helper()
	d, f, k := commitGraphDeadline, commitGraphFirstBudget, commitGraphKillDelay
	commitGraphDeadline, commitGraphFirstBudget, commitGraphKillDelay = deadline, first, kill
	t.Cleanup(func() { commitGraphDeadline, commitGraphFirstBudget, commitGraphKillDelay = d, f, k })
}

// hydrateWithHistory prepares a cache repo holding n commits on main, with
// the fake store advertising that tip at generation 8, so the next Hydrate
// takes the full path.
func hydrateWithHistory(t *testing.T, n int) (st *countingStore, bl blob.BlobStore, cacheDir, repo, repoPath string) {
	t.Helper()
	st, bl, cacheDir = hydrateEnv(t)
	repo = "t-commit-graph"
	repoPath, err := EnsureCacheRepo(cacheDir, repo)
	if err != nil {
		t.Fatal(err)
	}
	tip := seedHistory(t, repoPath, n)
	st.mu.Lock()
	st.refs["refs/heads/main"] = tip
	st.mu.Unlock()
	st.setGen(8)
	return st, bl, cacheDir, repo, repoPath
}

func TestHydrate_WritesAndRefreshesCommitGraph(t *testing.T) {
	st, bl, cacheDir := hydrateEnv(t)
	const repo = "t-commit-graph-refresh"
	repoPath, err := EnsureCacheRepo(cacheDir, repo)
	if err != nil {
		t.Fatal(err)
	}
	tip := seedHistory(t, repoPath, 50)
	st.mu.Lock()
	st.refs["refs/heads/main"] = tip
	st.mu.Unlock()
	st.setGen(8)

	mustHydrate(t, st, bl, cacheDir, repo)
	chain := commitGraphChain(t, repoPath)
	if len(chain) == 0 {
		t.Fatal("full hydrate wrote no commit-graph chain")
	}
	if !layerHasBloom(t, repoPath, chain[0]) {
		t.Fatal("first commit-graph layer has no changed-path Bloom filters")
	}
	mustGit(t, repoPath, "commit-graph", "verify")

	// A push lands (new commits, generation bump): the next full hydrate
	// adds a layer, which inherits the Bloom mode.
	tip2 := seedHistory(t, repoPath, 5)
	st.mu.Lock()
	st.refs["refs/heads/main"] = tip2
	st.mu.Unlock()
	st.setGen(9)
	mustHydrate(t, st, bl, cacheDir, repo)
	chain2 := commitGraphChain(t, repoPath)
	if strings.Join(chain2, " ") == strings.Join(chain, " ") {
		t.Fatalf("hydrate after new commits left the chain unchanged: %v", chain2)
	}
	for _, layer := range chain2 {
		if !layerHasBloom(t, repoPath, layer) {
			t.Fatalf("layer %s lost the inherited Bloom mode", layer)
		}
	}
	mustGit(t, repoPath, "commit-graph", "verify")

	// The fast path stays fork-free and leaves the graph alone.
	var spawned []string
	obs := func(dir string, args []string) { spawned = append(spawned, strings.Join(args, " ")) }
	gitSpawnObserver.Store(&obs)
	defer gitSpawnObserver.Store(nil)
	mustHydrate(t, st, bl, cacheDir, repo)
	gitSpawnObserver.Store(nil)
	if len(spawned) != 0 {
		t.Fatalf("fast-path hydrate spawned git: %v", spawned)
	}
	if got := commitGraphChain(t, repoPath); strings.Join(got, " ") != strings.Join(chain2, " ") {
		t.Fatalf("fast path changed the chain: %v -> %v", chain2, got)
	}
}

// A pre-existing Git lock is never removed (its owner cannot be proven),
// and the failed write does not fail the hydrate or its marker.
func TestHydrate_CommitGraphLockPreservedAndHydrateSucceeds(t *testing.T) {
	st, bl, cacheDir, repo, repoPath := hydrateWithHistory(t, 20)
	graphs := filepath.Join(repoPath, "objects", "info", "commit-graphs")
	if err := os.MkdirAll(graphs, 0o755); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(graphs, "commit-graph-chain.lock")
	if err := os.WriteFile(lock, []byte("foreign writer\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(lock)
	if err != nil {
		t.Fatal(err)
	}

	mustHydrate(t, st, bl, cacheDir, repo)
	if got := markerContent(t, repoPath); got != "8 refs/heads/main" {
		t.Fatalf("marker = %q, want \"8 refs/heads/main\"", got)
	}
	after, err := os.Stat(lock)
	if err != nil {
		t.Fatalf("pre-existing lock removed: %v", err)
	}
	if b, _ := os.ReadFile(lock); string(b) != "foreign writer\n" || !os.SameFile(before, after) {
		t.Fatalf("pre-existing lock rewritten: %q", b)
	}
	if chain := commitGraphChain(t, repoPath); chain != nil {
		t.Fatalf("graph written despite a held lock: %v", chain)
	}
}

// The child Git ignores inherited GIT_* state and host config: a hostile
// GIT_DIR, object directory, or config injection must neither redirect the
// write nor disable it.
func TestWriteCommitGraph_IsolatesInheritedGitEnv(t *testing.T) {
	repoPath := t.TempDir()
	mustGit(t, repoPath, "init", "-q", "--bare")
	seedHistory(t, repoPath, 10)
	tips := repoTips(t, repoPath)
	decoy := t.TempDir()
	mustGit(t, decoy, "init", "-q", "--bare")
	globalCfg := filepath.Join(t.TempDir(), "gitconfig")
	if err := os.WriteFile(globalCfg, []byte("[core]\n\tcommitGraph = false\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_DIR", decoy)
	t.Setenv("GIT_OBJECT_DIRECTORY", filepath.Join(decoy, "objects"))
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "core.commitGraph")
	t.Setenv("GIT_CONFIG_VALUE_0", "false")
	t.Setenv("GIT_CONFIG_GLOBAL", globalCfg)

	if _, err := writeCommitGraph(t.Context(), repoPath, tips); err != nil {
		t.Fatalf("writeCommitGraph: %v", err)
	}
	if chain := commitGraphChain(t, repoPath); len(chain) == 0 {
		t.Fatal("no graph in the cache repo: inherited GIT_* state redirected or disabled the write")
	}
	if _, err := os.Stat(filepath.Join(decoy, "objects", "info", "commit-graphs")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("graph written into the decoy repo (stat err %v)", err)
	}
}

func TestCommitGraphEnv(t *testing.T) {
	got := commitGraphEnv([]string{"PATH=/bin", "GIT_DIR=/x", "HOME=/h", "GIT_CONFIG_COUNT=1", "GITX=keep"})
	want := []string{"PATH=/bin", "HOME=/h", "GITX=keep", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_CONFIG_NOSYSTEM=1"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("commitGraphEnv = %q, want %q", got, want)
	}
}

// fakeCommitGraphGit installs a shell script as the commit-graph binary.
// Every run appends "start <pid> <args>" to the returned log. The first
// attempt (no --no-changed-paths) records its pid and then hangs until
// SIGTERM ("term <pid>"), or exits 1 in mode "fail". The plain retry logs
// whether the first attempt's process is already gone ("prev-dead") and
// then execs the real Git, hangs too in mode "hang-all", or exits 1 in
// mode "hang-then-fail".
func fakeCommitGraphGit(t *testing.T, mode string) (logPath string) {
	t.Helper()
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	logPath = filepath.Join(dir, "log")
	script := filepath.Join(dir, "fake-git")
	const body = `#!/bin/sh
log="$CG_FAKE_LOG"
echo "start $$ $*" >> "$log"
case " $* " in
*" --no-changed-paths "*)
	if [ -f "$log.pid1" ] && kill -0 "$(cat "$log.pid1")" 2>/dev/null; then echo prev-alive >> "$log"; else echo prev-dead >> "$log"; fi
	if [ "$CG_FAKE_MODE" = hang-then-fail ]; then echo boom2 >&2; exit 1; fi
	[ "$CG_FAKE_MODE" = hang-all ] || exec "$CG_REAL_GIT" "$@" ;;
*)
	echo $$ > "$log.pid1"
	if [ "$CG_FAKE_MODE" = fail ]; then echo boom >&2; exit 1; fi ;;
esac
trap 'echo "term $$" >> "$log"; kill $sp 2>/dev/null; exit 143' TERM
sleep 30 &
sp=$!
wait $sp
echo "woke $$" >> "$log"
`
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CG_FAKE_LOG", logPath)
	t.Setenv("CG_FAKE_MODE", mode)
	t.Setenv("CG_REAL_GIT", realGit)
	old := commitGraphGit
	commitGraphGit = script
	t.Cleanup(func() { commitGraphGit = old })
	return logPath
}

func readFakeLog(t *testing.T, logPath string) []string {
	t.Helper()
	b, err := os.ReadFile(logPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

func countPrefix(lines []string, prefix string) int {
	n := 0
	for _, l := range lines {
		if strings.HasPrefix(l, prefix) {
			n++
		}
	}
	return n
}

// The first attempt running out of its own budget is terminated and reaped
// before the plain retry starts, and the retry leaves a usable graph
// without Bloom filters.
func TestWriteCommitGraph_TimeoutFallsBackToPlainGraph(t *testing.T) {
	repoPath := t.TempDir()
	mustGit(t, repoPath, "init", "-q", "--bare")
	seedHistory(t, repoPath, 10)
	logPath := fakeCommitGraphGit(t, "hang")
	setCommitGraphBudgets(t, 10*time.Second, 500*time.Millisecond, time.Second)

	if _, err := writeCommitGraph(t.Context(), repoPath, repoTips(t, repoPath)); err != nil {
		t.Fatalf("writeCommitGraph = %v, want the plain retry to succeed", err)
	}
	lines := readFakeLog(t, logPath)
	if countPrefix(lines, "start ") != 2 || countPrefix(lines, "term ") != 1 || countPrefix(lines, "woke ") != 0 {
		t.Fatalf("fake git log = %q, want two starts and one SIGTERM", lines)
	}
	if !strings.Contains(lines[0], "--changed-paths") {
		t.Fatalf("cold first attempt = %q, want --changed-paths", lines[0])
	}
	if countPrefix(lines, "prev-dead") != 1 {
		t.Fatalf("plain retry started before the first attempt was reaped: %q", lines)
	}
	chain := commitGraphChain(t, repoPath)
	if len(chain) != 1 || layerHasBloom(t, repoPath, chain[0]) {
		t.Fatalf("fallback chain %v: want one layer without Bloom filters", chain)
	}
	mustGit(t, repoPath, "commit-graph", "verify")
}

// A cancelled request stops the write and suppresses the retry.
func TestWriteCommitGraph_CancellationSuppressesFallback(t *testing.T) {
	repoPath := t.TempDir()
	mustGit(t, repoPath, "init", "-q", "--bare")
	seedHistory(t, repoPath, 5)
	tips := repoTips(t, repoPath)
	logPath := fakeCommitGraphGit(t, "hang")
	setCommitGraphBudgets(t, 10*time.Second, 5*time.Second, time.Second)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 500 {
			if countPrefix(readFakeLog(t, logPath), "start ") > 0 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		cancel()
	})
	_, err := writeCommitGraph(ctx, repoPath, tips)
	wg.Wait()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("writeCommitGraph = %v, want context.Canceled", err)
	}
	if lines := readFakeLog(t, logPath); countPrefix(lines, "start ") != 1 {
		t.Fatalf("fake git log = %q, want exactly one attempt", lines)
	}
}

// No retry starts once the shared deadline has passed, and a plain
// failure is not retried either.
func TestWriteCommitGraph_NoRetryAfterDeadlineOrFailure(t *testing.T) {
	repoPath := t.TempDir()
	mustGit(t, repoPath, "init", "-q", "--bare")
	seedHistory(t, repoPath, 5)

	logPath := fakeCommitGraphGit(t, "hang")
	setCommitGraphBudgets(t, 300*time.Millisecond, 5*time.Second, time.Second)
	if _, err := writeCommitGraph(t.Context(), repoPath, repoTips(t, repoPath)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline case = %v, want context.DeadlineExceeded", err)
	}
	if lines := readFakeLog(t, logPath); countPrefix(lines, "start ") != 1 {
		t.Fatalf("deadline case log = %q, want one attempt", lines)
	}

	logPath = fakeCommitGraphGit(t, "fail")
	setCommitGraphBudgets(t, 10*time.Second, 2*time.Second, time.Second)
	_, err := writeCommitGraph(t.Context(), repoPath, repoTips(t, repoPath))
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("failure case = %v, want the git stderr", err)
	}
	if lines := readFakeLog(t, logPath); countPrefix(lines, "start ") != 1 {
		t.Fatalf("failure case log = %q, want one attempt", lines)
	}
}

// Both attempts timing out still leave a successful hydrate with its
// marker, within the documented bound.
func TestHydrate_CommitGraphTimeoutsKeepHydration(t *testing.T) {
	st, bl, cacheDir, repo, repoPath := hydrateWithHistory(t, 5)
	logPath := fakeCommitGraphGit(t, "hang-all")
	setCommitGraphBudgets(t, 600*time.Millisecond, 200*time.Millisecond, 200*time.Millisecond)

	start := time.Now()
	mustHydrate(t, st, bl, cacheDir, repo)
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("hydrate took %v with a 600ms graph deadline", d)
	}
	if got := markerContent(t, repoPath); got != "8 refs/heads/main" {
		t.Fatalf("marker = %q, want \"8 refs/heads/main\"", got)
	}
	lines := readFakeLog(t, logPath)
	if countPrefix(lines, "start ") != 2 || countPrefix(lines, "prev-dead") != 1 || countPrefix(lines, "woke ") != 0 {
		t.Fatalf("fake git log = %q, want two terminated attempts", lines)
	}
}

// Readers never fail while full hydrates keep adding layers underneath
// them (the split-layer expiry race degrades to a warning, not an error).
func TestHydrate_CommitGraphRefreshUnderConcurrentReaders(t *testing.T) {
	st, bl, cacheDir := hydrateEnv(t)
	const repo = "t-commit-graph-readers"
	repoPath, err := EnsureCacheRepo(cacheDir, repo)
	if err != nil {
		t.Fatal(err)
	}
	advance := func(n int, gen int64) {
		tip := seedHistory(t, repoPath, n)
		st.mu.Lock()
		st.refs["refs/heads/main"] = tip
		st.mu.Unlock()
		st.setGen(gen)
	}
	advance(200, 8)
	mustHydrate(t, st, bl, cacheDir, repo)

	ctx, stop := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			for ctx.Err() == nil {
				for _, args := range [][]string{
					{"log", "-1", "--format=%H", "refs/heads/main", "--", "dir/f3.txt"},
					{"rev-list", "--topo-order", "--count", "refs/heads/main"},
				} {
					if out, err := tryGit(repoPath, args...); err != nil || strings.TrimSpace(out) == "" {
						t.Errorf("git %v during refresh: %v\n%s", args, err, out)
						return
					}
				}
			}
		})
	}
	for g := range 12 {
		advance(7, int64(9+g))
		mustHydrate(t, st, bl, cacheDir, repo)
	}
	stop()
	wg.Wait()
	mustGit(t, repoPath, "commit-graph", "verify")
}

// seedWideHistory appends n commits numbered from start, each rewriting 3
// of ~1,000 files under 50 directories (the BenchmarkLogPathCommitGraph
// shape), continuing from the current main tip when there is one.
func seedWideHistory(tb testing.TB, repoPath string, start, n int) {
	tb.Helper()
	var in strings.Builder
	if start > 0 {
		in.WriteString("reset refs/heads/main\nfrom refs/heads/main^0\n\n")
	}
	for i := start; i < start+n; i++ {
		fmt.Fprintf(&in, "commit refs/heads/main\ncommitter B <b@example.com> %d +0000\ndata 2\nc\n", 1700000000+i)
		for k := range 3 {
			content := fmt.Sprintf("%d %d\n", i, k)
			fmt.Fprintf(&in, "M 100644 inline d%d/f%d.txt\ndata %d\n%s", (i+k)%50, (i*3+k)%997, len(content), content)
		}
	}
	cmd := exec.Command("git", "fast-import", "--quiet")
	cmd.Dir = repoPath
	cmd.Stdin = strings.NewReader(in.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		tb.Fatalf("fast-import: %v\n%s", err, out)
	}
}

// BenchmarkCommitGraphRefresh measures the write cost ADR-47 charges to a
// full hydrate on a 20,000-commit history. "cold-bloom" and "cold-plain"
// time the two first-write variants as bare Git runs, so the numbers do
// not depend on the refresh budgets; the push and noop cases run the
// production writer on a chain that inherited Bloom filters, as a small or
// medium repo's graph does after its first write.
func BenchmarkCommitGraphRefresh(b *testing.B) {
	const n = 20000
	repoPath := b.TempDir()
	if out, err := tryGit(repoPath, "init", "-q", "--bare"); err != nil {
		b.Fatalf("init: %v\n%s", err, out)
	}
	seedWideHistory(b, repoPath, 0, n)
	ctx := context.Background()
	graphs := filepath.Join(repoPath, "objects", "info", "commit-graphs")
	cold := func(name string, flag string) {
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				b.StopTimer()
				if err := os.RemoveAll(graphs); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				if out, err := tryGit(repoPath, "commit-graph", "write", "--reachable", "--split", "--no-progress", flag); err != nil {
					b.Fatalf("commit-graph write: %v\n%s", err, out)
				}
			}
		})
	}
	cold("cold-plain", "--no-changed-paths")
	cold("cold-bloom", "--changed-paths")
	next := n
	for _, push := range []int{10, 100} {
		b.Run(fmt.Sprintf("push-%d", push), func(b *testing.B) {
			for b.Loop() {
				b.StopTimer()
				seedWideHistory(b, repoPath, next, push)
				next += push
				tips := repoTips(b, repoPath)
				b.StartTimer()
				if _, err := writeCommitGraph(ctx, repoPath, tips); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("noop", func(b *testing.B) {
		tips := repoTips(b, repoPath)
		for b.Loop() {
			if _, err := writeCommitGraph(ctx, repoPath, tips); err != nil {
				b.Fatal(err)
			}
		}
	})
	for _, layer := range commitGraphChain(b, repoPath) {
		if !layerHasBloom(b, repoPath, layer) {
			b.Fatalf("layer %s has no Bloom filters: a timed write fell back, so the push numbers are not the Bloom cost", layer)
		}
	}
}

// The refresh walks only the distinct tips the sync applied: same-tip ref
// fanout collapses to one tip and still writes, more distinct tips than
// commitGraphMaxTips skip the write (the hydrate and its marker still
// succeed), and refs a concurrent writer adds after the sync are not
// walked.
func TestHydrate_CommitGraphWalksOnlySyncedTips(t *testing.T) {
	st, bl, cacheDir, repo, repoPath := hydrateWithHistory(t, 5)
	st.mu.Lock()
	tip := st.refs["refs/heads/main"]
	for i := range 50 {
		st.refs[fmt.Sprintf("refs/heads/fan%d", i)] = tip
	}
	st.mu.Unlock()
	old := commitGraphMaxTips
	t.Cleanup(func() { commitGraphMaxTips = old })

	var args [][]string
	obs := func(_ string, a []string) {
		if len(a) > 0 && a[0] == "commit-graph" {
			args = append(args, a)
		}
	}
	gitSpawnObserver.Store(&obs)
	defer gitSpawnObserver.Store(nil)

	commitGraphMaxTips = 1 // 51 refs, one distinct tip
	mustHydrate(t, st, bl, cacheDir, repo)
	if len(args) != 1 || !slices.Contains(args[0], "--stdin-commits") || slices.Contains(args[0], "--reachable") {
		t.Fatalf("same-tip fanout: commit-graph runs %q, want one --stdin-commits write", args)
	}

	// Two distinct tips over a cap of 1: skipped, hydrate still clean.
	parent := strings.TrimSpace(mustGit(t, repoPath, "rev-parse", tip+"~1"))
	st.mu.Lock()
	st.refs["refs/heads/old"] = parent
	st.mu.Unlock()
	st.setGen(9)
	mustHydrate(t, st, bl, cacheDir, repo)
	if len(args) != 1 {
		t.Fatalf("over the tip cap: commit-graph runs %q, want no new write", args)
	}
	if got := markerContent(t, repoPath); got != "9 refs/heads/main" {
		t.Fatalf("marker = %q, want \"9 refs/heads/main\"", got)
	}
}

// A ref a concurrent writer (receive-pack) adds after the sync is not
// walked: the command gets exactly the synced tips on stdin.
func TestWriteCommitGraph_FeedsOnlyGivenTips(t *testing.T) {
	repoPath := t.TempDir()
	mustGit(t, repoPath, "init", "-q", "--bare")
	tip := seedHistory(t, repoPath, 5)
	synced := []string{tip}
	late := seedHistory(t, repoPath, 3) // moves main past the synced tip
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	stdinLog := filepath.Join(dir, "stdin")
	script := filepath.Join(dir, "record-git")
	body := "#!/bin/sh\ncat > \"" + stdinLog + "\"\nexec \"" + realGit + "\" \"$@\" < \"" + stdinLog + "\"\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	old := commitGraphGit
	commitGraphGit = script
	t.Cleanup(func() { commitGraphGit = old })

	if _, err := writeCommitGraph(t.Context(), repoPath, synced); err != nil {
		t.Fatalf("writeCommitGraph: %v", err)
	}
	if b, _ := os.ReadFile(stdinLog); strings.TrimSpace(string(b)) != tip {
		t.Fatalf("stdin = %q, want only the synced tip %s (not the late tip %s)", b, tip, late)
	}
	mustGit(t, repoPath, "commit-graph", "verify")
}

// With every process-wide slot busy the refresh is skipped without
// spawning Git; once a slot frees up it writes again.
func TestRefreshCommitGraph_SkipsWhenSlotsBusy(t *testing.T) {
	repoPath := t.TempDir()
	mustGit(t, repoPath, "init", "-q", "--bare")
	seedHistory(t, repoPath, 5)
	var spawns int
	obs := func(_ string, _ []string) { spawns++ }
	gitSpawnObserver.Store(&obs)
	defer gitSpawnObserver.Store(nil)

	for range cap(commitGraphSlots) {
		commitGraphSlots <- struct{}{}
	}
	tips := tipsOf(repoTips(t, repoPath)...)
	refreshCommitGraph(t.Context(), repoPath, tips)
	for range cap(commitGraphSlots) {
		<-commitGraphSlots
	}
	if spawns != 0 || commitGraphChain(t, repoPath) != nil {
		t.Fatalf("refresh ran with every slot busy (%d spawns)", spawns)
	}
	refreshCommitGraph(t.Context(), repoPath, tips)
	if spawns != 1 || len(commitGraphChain(t, repoPath)) == 0 {
		t.Fatalf("refresh with a free slot: %d spawns, want one write", spawns)
	}
	if len(commitGraphSlots) != 0 {
		t.Fatalf("refresh leaked a slot: %d held", len(commitGraphSlots))
	}
}

// A relative cache directory (an accepted --cache-dir value) yields
// relative repo paths; the refresh must work with them, not panic.
func TestHydrate_CommitGraphWithRelativeCacheDir(t *testing.T) {
	st, bl, cacheDir, repo, repoPath := hydrateWithHistory(t, 5)
	t.Chdir(filepath.Dir(cacheDir))
	rel := filepath.Base(cacheDir)
	got, refsErr, err := Hydrate(t.Context(), st, bl, rel, repo, "main")
	if err != nil || refsErr != nil {
		t.Fatalf("Hydrate(relative cache dir) = (refsErr %v, err %v)", refsErr, err)
	}
	if filepath.IsAbs(got) {
		t.Fatalf("repo path %q is absolute; the test no longer covers relative paths", got)
	}
	if len(commitGraphChain(t, repoPath)) == 0 {
		t.Fatal("no commit-graph written through a relative repo path")
	}
}
