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
		outcomeLockHeld, outcomeTimedOut, outcomeCanceled, outcomeFailed,
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
	if got := refreshCommitGraph(t.Context(), repoPath, tips); got != outcomeWritten {
		t.Fatalf("outcome = %s, want written", got)
	}
	if lines := refreshLines(buf); len(lines) != 0 {
		t.Fatalf("successful refresh logged %q", lines)
	}
}

func TestRefreshCommitGraph_SkippedTipCapLogged(t *testing.T) {
	repoPath, _ := newGraphRepo(t, 5)
	old := commitGraphMaxTips
	commitGraphMaxTips = 1
	t.Cleanup(func() { commitGraphMaxTips = old })
	tips := tipsOf(repoTips(t, repoPath)[0], strings.Repeat("a", 40))
	buf := captureLog(t)
	if got := refreshCommitGraph(t.Context(), repoPath, tips); got != outcomeSkippedTipCap {
		t.Fatalf("outcome = %s, want skipped_tip_cap", got)
	}
	assertOneOutcome(t, buf, outcomeSkippedTipCap, repoPath)
}

func TestRefreshCommitGraph_SkippedBusyLogged(t *testing.T) {
	repoPath, tips := newGraphRepo(t, 5)
	for range cap(commitGraphSlots) {
		commitGraphSlots <- struct{}{}
	}
	t.Cleanup(func() {
		for range cap(commitGraphSlots) {
			<-commitGraphSlots
		}
	})
	buf := captureLog(t)
	if got := refreshCommitGraph(t.Context(), repoPath, tips); got != outcomeSkippedBusy {
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
	if got := refreshCommitGraph(t.Context(), repoPath, tips); got != outcomeLockHeld {
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
	if got := refreshCommitGraph(t.Context(), repoPath, tips); got != outcomeWrittenPlain {
		t.Fatalf("outcome = %s, want written_plain", got)
	}
	assertOneOutcome(t, buf, outcomeWrittenPlain, repoPath)
}

func TestRefreshCommitGraph_TimedOutLogged(t *testing.T) {
	repoPath, tips := newGraphRepo(t, 5)
	fakeCommitGraphGit(t, "hang-all")
	setCommitGraphBudgets(t, 500*time.Millisecond, 200*time.Millisecond, 200*time.Millisecond)
	buf := captureLog(t)
	if got := refreshCommitGraph(t.Context(), repoPath, tips); got != outcomeTimedOut {
		t.Fatalf("outcome = %s, want timed_out", got)
	}
	assertOneOutcome(t, buf, outcomeTimedOut, repoPath)
}

func TestRefreshCommitGraph_CanceledLogged(t *testing.T) {
	repoPath, tips := newGraphRepo(t, 5)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	buf := captureLog(t)
	if got := refreshCommitGraph(ctx, repoPath, tips); got != outcomeCanceled {
		t.Fatalf("outcome = %s, want canceled", got)
	}
	assertOneOutcome(t, buf, outcomeCanceled, repoPath)
}

func TestRefreshCommitGraph_FailedLogged(t *testing.T) {
	repoPath, tips := newGraphRepo(t, 5)
	fakeCommitGraphGit(t, "fail")
	buf := captureLog(t)
	if got := refreshCommitGraph(t.Context(), repoPath, tips); got != outcomeFailed {
		t.Fatalf("outcome = %s, want failed", got)
	}
	assertOneOutcome(t, buf, outcomeFailed, repoPath)
	if l := refreshLines(buf)[0]; !strings.Contains(l, "boom") {
		t.Fatalf("failed line %q lacks git's stderr", l)
	}
}
