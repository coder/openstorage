// Log contract for commit-graph refreshes (issue #322): every refresh
// ends in exactly one outcome from a fixed set, and every outcome except
// "written" is logged as one line with a stable prefix. Operators alert on
// these lines (docs/how-to/deploy-production.md, "Commit-graph refresh"),
// so the format and the outcome set are pinned here.
package githttp

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/gitlimit"
	"github.com/coder/openstorage/internal/metrics"
)

// captureLog redirects the standard logger for one test and returns the
// buffer it writes to.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	oldOut, oldFlags := log.Writer(), log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(oldOut); log.SetFlags(oldFlags) })
	return &buf
}

// refreshLines returns the commit-graph refresh lines in buf.
func refreshLines(buf *bytes.Buffer) []string {
	var out []string
	for l := range strings.Lines(buf.String()) {
		if strings.HasPrefix(l, "commit-graph refresh ") {
			out = append(out, strings.TrimRight(l, "\n"))
		}
	}
	return out
}

// assertOneOutcome checks buf holds exactly one refresh line, for outcome
// and repoPath.
func assertOneOutcome(t *testing.T, buf *bytes.Buffer, outcome commitGraphOutcome, repoPath string) {
	t.Helper()
	if outcome != outcomeSkippedBusy {
		assertNoAdmissionHeld(t)
	}
	lines := refreshLines(buf)
	want := fmt.Sprintf("commit-graph refresh outcome=%s repo=%s", outcome, repoPath)
	if len(lines) != 1 || (lines[0] != want && !strings.HasPrefix(lines[0], want+": ")) {
		t.Fatalf("refresh log lines = %q, want exactly one starting %q", lines, want)
	}
}

func newGraphRepo(t *testing.T, commits int) (repoPath string, tips *refTips) {
	t.Helper()
	repoPath = t.TempDir()
	mustGit(t, repoPath, "init", "-q", "--bare")
	seedHistory(t, repoPath, commits)
	return repoPath, tipsOf(repoTips(t, repoPath)...)
}

func TestCommitGraphOutcomes_FixedSet(t *testing.T) {
	want := []commitGraphOutcome{
		outcomeWritten, outcomeWrittenPlain, outcomeSkippedTipCap, outcomeSkippedBusy,
		outcomeLockHeld, outcomeMemoryLimit, outcomeTimedOut, outcomeCanceled, outcomeFailed,
	}
	if fmt.Sprint(commitGraphOutcomes) != fmt.Sprint(want) {
		t.Fatalf("commitGraphOutcomes = %v, want %v (a new outcome must be documented in deploy-production.md)", commitGraphOutcomes, want)
	}
	for _, o := range want {
		if strings.ContainsAny(string(o), " =:") || o == "" {
			t.Fatalf("outcome %q is not a bare label", o)
		}
	}
}

// The ops guide documents every outcome by name.
func TestCommitGraphOutcomes_Documented(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "how-to", "deploy-production.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range commitGraphOutcomes {
		if !strings.Contains(string(doc), "`outcome="+string(o)+"`") {
			t.Errorf("deploy-production.md does not document `outcome=%s`", o)
		}
	}
}

func TestRefreshCommitGraph_WrittenIsSilent(t *testing.T) {
	repoPath, tips := newGraphRepo(t, 5)
	buf := captureLog(t)
	if got := refreshCommitGraph(t.Context(), repoPath, testSlotKey, tips); got != outcomeWritten {
		t.Fatalf("outcome = %s, want written", got)
	}
	if lines := refreshLines(buf); len(lines) != 0 {
		t.Fatalf("successful refresh logged %q", lines)
	}
}

// A full hydrate of a repository with no refs is still one refresh: it
// counts once as written (nothing to graph is "already current") and,
// like every written, logs nothing.
func TestRefreshCommitGraph_EmptyRepoCountsWritten(t *testing.T) {
	buf := captureLog(t)
	labels := map[string]string{"outcome": string(outcomeWritten)}
	before := metrics.Value("openstorage_commit_graph_refresh_total", labels)
	if got := refreshCommitGraph(t.Context(), t.TempDir(), testSlotKey, newRefTips()); got != outcomeWritten {
		t.Fatalf("outcome = %s, want written", got)
	}
	if got := metrics.Value("openstorage_commit_graph_refresh_total", labels) - before; got != 1 {
		t.Fatalf("written counter delta = %v, want 1", got)
	}
	if lines := refreshLines(buf); len(lines) != 0 {
		t.Fatalf("empty-repo refresh logged %q", lines)
	}
}

func TestRefreshCommitGraph_SkippedTipCapLogged(t *testing.T) {
	repoPath, _ := newGraphRepo(t, 5)
	old := commitGraphMaxTips
	commitGraphMaxTips = 1
	t.Cleanup(func() { commitGraphMaxTips = old })
	tips := tipsOf(repoTips(t, repoPath)[0], strings.Repeat("a", 40))
	buf := captureLog(t)
	if got := refreshCommitGraph(t.Context(), repoPath, testSlotKey, tips); got != outcomeSkippedTipCap {
		t.Fatalf("outcome = %s, want skipped_tip_cap", got)
	}
	assertOneOutcome(t, buf, outcomeSkippedTipCap, repoPath)
}

func TestRefreshCommitGraph_SkippedBusyLogged(t *testing.T) {
	repoPath, tips := newGraphRepo(t, 5)
	holdMaintenance(t, gitlimit.MaintenanceShare)
	buf := captureLog(t)
	if got := refreshCommitGraph(t.Context(), repoPath, testSlotKey, tips); got != outcomeSkippedBusy {
		t.Fatalf("outcome = %s, want skipped_busy", got)
	}
	assertOneOutcome(t, buf, outcomeSkippedBusy, repoPath)
}

func TestRefreshCommitGraph_LockHeldLogged(t *testing.T) {
	repoPath, tips := newGraphRepo(t, 5)
	graphs := filepath.Join(repoPath, "objects", "info", "commit-graphs")
	if err := os.MkdirAll(graphs, 0o755); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(graphs, "commit-graph-chain.lock")
	if err := os.WriteFile(lock, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	buf := captureLog(t)
	if got := refreshCommitGraph(t.Context(), repoPath, testSlotKey, tips); got != outcomeLockHeld {
		t.Fatalf("outcome = %s, want lock_held", got)
	}
	assertOneOutcome(t, buf, outcomeLockHeld, repoPath)
	if _, err := os.Stat(lock); err != nil {
		t.Fatalf("lock removed: %v", err)
	}
}

func TestRefreshCommitGraph_WrittenPlainLogged(t *testing.T) {
	repoPath, tips := newGraphRepo(t, 5)
	fakeCommitGraphGit(t, "hang")
	setCommitGraphBudgets(t, 10*time.Second, 300*time.Millisecond, time.Second)
	buf := captureLog(t)
	if got := refreshCommitGraph(t.Context(), repoPath, testSlotKey, tips); got != outcomeWrittenPlain {
		t.Fatalf("outcome = %s, want written_plain", got)
	}
	assertOneOutcome(t, buf, outcomeWrittenPlain, repoPath)
}

func TestRefreshCommitGraph_TimedOutLogged(t *testing.T) {
	repoPath, tips := newGraphRepo(t, 5)
	fakeCommitGraphGit(t, "hang-all")
	setCommitGraphBudgets(t, 500*time.Millisecond, 200*time.Millisecond, 200*time.Millisecond)
	buf := captureLog(t)
	if got := refreshCommitGraph(t.Context(), repoPath, testSlotKey, tips); got != outcomeTimedOut {
		t.Fatalf("outcome = %s, want timed_out", got)
	}
	assertOneOutcome(t, buf, outcomeTimedOut, repoPath)
}

func TestRefreshCommitGraph_CanceledLogged(t *testing.T) {
	repoPath, tips := newGraphRepo(t, 5)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	buf := captureLog(t)
	if got := refreshCommitGraph(ctx, repoPath, testSlotKey, tips); got != outcomeCanceled {
		t.Fatalf("outcome = %s, want canceled", got)
	}
	assertOneOutcome(t, buf, outcomeCanceled, repoPath)
}

func TestRefreshCommitGraph_FailedLogged(t *testing.T) {
	repoPath, tips := newGraphRepo(t, 5)
	fakeCommitGraphGit(t, "fail")
	buf := captureLog(t)
	if got := refreshCommitGraph(t.Context(), repoPath, testSlotKey, tips); got != outcomeFailed {
		t.Fatalf("outcome = %s, want failed", got)
	}
	assertOneOutcome(t, buf, outcomeFailed, repoPath)
	if l := refreshLines(buf)[0]; !strings.Contains(l, "boom") {
		t.Fatalf("failed line %q lacks git's stderr", l)
	}
}

// The outcome follows what ended the refresh: a first-attempt timeout
// followed by an unrelated retry failure is failed, not timed_out.
func TestRefreshCommitGraph_ClassifiesTerminalFailure(t *testing.T) {
	repoPath, tips := newGraphRepo(t, 5)
	fakeCommitGraphGit(t, "hang-then-fail")
	setCommitGraphBudgets(t, 10*time.Second, 300*time.Millisecond, time.Second)
	buf := captureLog(t)
	if got := refreshCommitGraph(t.Context(), repoPath, testSlotKey, tips); got != outcomeFailed {
		t.Fatalf("outcome = %s, want failed", got)
	}
	assertOneOutcome(t, buf, outcomeFailed, repoPath)
	if l := refreshLines(buf)[0]; !strings.Contains(l, "boom2") {
		t.Fatalf("failed line %q lacks the retry's stderr", l)
	}
}

// Every refresh runs under the read API's Heavy heap cap, also on hydrates
// no read budget covers (upload-pack): the fake git reports the
// RLIMIT_DATA it was started with.
func TestRefreshCommitGraph_RunsUnderHeapCap(t *testing.T) {
	if !gitlimit.HeapLimitSupported() {
		t.Skip("no heap cap on this platform")
	}
	repoPath, tips := newGraphRepo(t, 5)
	realGit := realGitPath(t)
	dir := t.TempDir()
	report := filepath.Join(dir, "limit")
	body := "#!/bin/sh\n[ \"$1\" = commit-graph ] || exec \"" + realGit + "\" \"$@\"\nulimit -S -d > \"" + report + "\"\nexec \"" + realGit + "\" \"$@\"\n"
	if err := os.WriteFile(filepath.Join(dir, "git"), []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	if got := refreshCommitGraph(t.Context(), repoPath, testSlotKey, tips); got != outcomeWritten {
		t.Fatalf("outcome = %s, want written", got)
	}
	b, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.TrimSpace(string(b)), fmt.Sprint(gitlimit.HeavyHeapBytes/1024); got != want {
		t.Fatalf("commit-graph ran with RLIMIT_DATA %s KiB, want %s", got, want)
	}
	assertNoAdmissionHeld(t)
}

// A git that dies of its heap cap is reported as memory_limit.
func TestRefreshCommitGraph_MemoryLimitLogged(t *testing.T) {
	if !gitlimit.HeapLimitSupported() {
		t.Skip("no heap cap on this platform")
	}
	repoPath, tips := newGraphRepo(t, 5)
	realGit := realGitPath(t)
	dir := t.TempDir()
	body := "#!/bin/sh\n[ \"$1\" = commit-graph ] || exec \"" + realGit + "\" \"$@\"\necho 'fatal: Out of memory, malloc failed (tried to allocate 1048576 bytes)' >&2\nexit 128\n"
	if err := os.WriteFile(filepath.Join(dir, "git"), []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	buf := captureLog(t)
	if got := refreshCommitGraph(t.Context(), repoPath, testSlotKey, tips); got != outcomeMemoryLimit {
		t.Fatalf("outcome = %s, want memory_limit", got)
	}
	assertOneOutcome(t, buf, outcomeMemoryLimit, repoPath)
}

// Every outcome, including the silent written, counts once in
// openstorage_commit_graph_refresh_total under a label equal to its name.
func TestRecordCommitGraphOutcome_CountsMetric(t *testing.T) {
	captureLog(t)
	for _, o := range commitGraphOutcomes {
		labels := map[string]string{"outcome": string(o)}
		before := metrics.Value("openstorage_commit_graph_refresh_total", labels)
		recordCommitGraphOutcome("repo.git", o, "")
		if got := metrics.Value("openstorage_commit_graph_refresh_total", labels) - before; got != 1 {
			t.Errorf("outcome %s: counter delta = %v, want 1", o, got)
		}
	}
}

// A multi-line git stderr still yields exactly one log line: embedded
// newlines are escaped, so every record keeps its outcome= and repo=.
func TestRecordCommitGraphOutcome_OneLineWithMultiLineDetail(t *testing.T) {
	buf := captureLog(t)
	recordCommitGraphOutcome("repo.git", outcomeFailed, "warning: a\nfatal: b\r\nfatal: c")
	out := strings.TrimRight(buf.String(), "\n")
	if strings.Count(out, "\n") != 0 || !strings.HasPrefix(out, "commit-graph refresh outcome=failed repo=repo.git: ") {
		t.Fatalf("log output = %q, want one line with the stable prefix", out)
	}
	if !strings.Contains(out, `warning: a\nfatal: b\r\nfatal: c`) {
		t.Fatalf("log output = %q, want the newlines escaped, not dropped", out)
	}
}

// The child runs in the C locale: lock and heap failures are classified
// from Git's English messages, which a translated locale would change.
func TestRefreshCommitGraph_ChildRunsInCLocale(t *testing.T) {
	repoPath, tips := newGraphRepo(t, 5)
	realGit := realGitPath(t)
	dir := t.TempDir()
	report := filepath.Join(dir, "locale")
	body := "#!/bin/sh\n[ \"$1\" = commit-graph ] || exec \"" + realGit + "\" \"$@\"\necho \"LC_ALL=$LC_ALL\" > \"" + report + "\"\nexec \"" + realGit + "\" \"$@\"\n"
	if err := os.WriteFile(filepath.Join(dir, "git"), []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("LC_ALL", "de_DE.UTF-8")
	t.Setenv("LANG", "de_DE.UTF-8")
	if got := refreshCommitGraph(t.Context(), repoPath, testSlotKey, tips); got != outcomeWritten {
		t.Fatalf("outcome = %s, want written", got)
	}
	if b, err := os.ReadFile(report); err != nil || strings.TrimSpace(string(b)) != "LC_ALL=C" {
		t.Fatalf("child locale = %q (err %v), want LC_ALL=C", b, err)
	}
}
