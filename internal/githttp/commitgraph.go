// commitgraph.go refreshes the split commit-graph of a cache repo after a
// full hydrate (issue #307, ADR-47). The graph is rebuildable cache state:
// read-side history walks (`git log -- <path>`, `--topo-order`) use it when
// present and fall back to parsing commit objects for anything it does not
// cover, so every failure here is logged and dropped, never returned to a
// request.
package githttp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/coder/openstorage/internal/gitlimit"
	"github.com/coder/openstorage/internal/metrics"
	"github.com/coder/openstorage/internal/ref"
)

// Refresh budgets (ADR-47 "Budgets and the no-Bloom fallback"). The whole
// refresh shares commitGraphDeadline; the first attempt, in the inherited
// Bloom mode, gets at most commitGraphFirstBudget of it, and the plain
// retry gets whatever remains. A terminated attempt gets SIGTERM (Git's
// tempfile handlers then remove its lock) and SIGKILL after
// commitGraphKillDelay, so the hydrate lock is held for at most about
// commitGraphDeadline + 2*commitGraphKillDelay. Variables, not constants,
// so tests can shrink them; nothing else writes them.
var (
	commitGraphDeadline    = 5 * time.Second
	commitGraphFirstBudget = 3 * time.Second
	commitGraphKillDelay   = time.Second
)

// commitGraphMaxTips caps the distinct commit tips one refresh may walk.
// The refresh never enumerates the live ref namespace (`--reachable`
// resolves every ref, and ref fanout is attacker-scaled: 500,000 refs
// took about 24 s); it feeds `--stdin-commits` the distinct tips the sync
// that just ran applied, collected while the sync streamed them. Above the
// cap the refresh is skipped; an existing graph stays in place and stays
// correct, only staler.
var commitGraphMaxTips = 10000

// refTips collects the distinct SHAs a ref sync streams, up to
// commitGraphMaxTips; past the cap it drops the set and marks itself over,
// so its memory stays bounded however many refs the repo has.
type refTips struct {
	set  map[string]struct{}
	over bool
}

func newRefTips() *refTips { return &refTips{set: map[string]struct{}{}} }

func (t *refTips) add(sha string) {
	if t.over {
		return
	}
	if _, ok := t.set[sha]; ok {
		return
	}
	if len(t.set) >= commitGraphMaxTips {
		t.set, t.over = nil, true
		return
	}
	t.set[sha] = struct{}{}
}

// tipCollectingRefSeq records every SHA inner yields into tips.
type tipCollectingRefSeq struct {
	inner refSeq
	tips  *refTips
}

func (c *tipCollectingRefSeq) next() (string, string, bool, error) {
	name, sha, ok, err := c.inner.next()
	if ok && err == nil {
		c.tips.add(sha)
	}
	return name, sha, ok, err
}

// errCommitGraphBudget is the cancellation cause of an attempt that ran out
// of its own budget (as opposed to the request or the shared deadline).
var errCommitGraphBudget = errors.New("commit-graph attempt budget expired")

// commitGraphOutcome is how one refresh ended. The set is fixed: it is
// the log contract in docs/how-to/deploy-production.md ("Commit-graph
// refresh") and the label set of openstorage_commit_graph_refresh_total,
// and it never carries the repo.
type commitGraphOutcome string

const (
	outcomeWritten       commitGraphOutcome = "written"         // graph extended (or nothing new); not logged
	outcomeWrittenPlain  commitGraphOutcome = "written_plain"   // first attempt timed out; plain retry succeeded (possibly with nothing new to write)
	outcomeSkippedTipCap commitGraphOutcome = "skipped_tip_cap" // more than commitGraphMaxTips distinct tips
	outcomeSkippedBusy   commitGraphOutcome = "skipped_busy"    // no maintenance admission (gitlimit.TryAdmitMaintenance)
	outcomeLockHeld      commitGraphOutcome = "lock_held"       // commit-graph-chain.lock already exists
	outcomeMemoryLimit   commitGraphOutcome = "memory_limit"    // git hit its heap cap (gitlimit.HeavyHeapBytes)
	outcomeTimedOut      commitGraphOutcome = "timed_out"       // the refresh deadline expired
	outcomeCanceled      commitGraphOutcome = "canceled"        // the request went away
	outcomeFailed        commitGraphOutcome = "failed"          // any other git failure
)

// commitGraphOutcomes lists every outcome, in documentation order.
var commitGraphOutcomes = []commitGraphOutcome{
	outcomeWritten, outcomeWrittenPlain, outcomeSkippedTipCap, outcomeSkippedBusy,
	outcomeLockHeld, outcomeMemoryLimit, outcomeTimedOut, outcomeCanceled, outcomeFailed,
}

// errCommitGraphLockHeld marks a write that failed because Git found its
// lock file already present (a concurrent or dead writer's).
var errCommitGraphLockHeld = errors.New("commit-graph lock file already exists")

// commitGraphMetric maps each outcome to its metrics label enum.
var commitGraphMetric = map[commitGraphOutcome]metrics.CommitGraphOutcome{
	outcomeWritten:       metrics.CommitGraphWritten,
	outcomeWrittenPlain:  metrics.CommitGraphWrittenPlain,
	outcomeSkippedTipCap: metrics.CommitGraphSkippedTipCap,
	outcomeSkippedBusy:   metrics.CommitGraphSkippedBusy,
	outcomeLockHeld:      metrics.CommitGraphLockHeld,
	outcomeMemoryLimit:   metrics.CommitGraphMemoryLimit,
	outcomeTimedOut:      metrics.CommitGraphTimedOut,
	outcomeCanceled:      metrics.CommitGraphCanceled,
	outcomeFailed:        metrics.CommitGraphFailed,
}

// logLineEscaper keeps a log record on one line.
var logLineEscaper = strings.NewReplacer("\r", `\r`, "\n", `\n`)

// recordCommitGraphOutcome is the single reporting point for a refresh's
// outcome. Every outcome counts once in
// openstorage_commit_graph_refresh_total{outcome}; every outcome except
// written is also logged as one line:
//
//	commit-graph refresh outcome=<outcome> repo=<repoPath>[: <detail>]
func recordCommitGraphOutcome(repoPath string, o commitGraphOutcome, detail string) commitGraphOutcome {
	m, ok := commitGraphMetric[o]
	assertf(ok && slices.Contains(commitGraphOutcomes, o), "recordCommitGraphOutcome: unknown outcome %q", o)
	metrics.CommitGraphRefresh(m)
	if o == outcomeWritten {
		return o
	}
	line := fmt.Sprintf("commit-graph refresh outcome=%s repo=%s", o, repoPath)
	if detail != "" {
		line += ": " + detail
	}
	// Exactly one line per record: git's stderr can span lines, so escape
	// embedded line breaks rather than letting them split the record.
	log.Print(logLineEscaper.Replace(line))
	return o
}

// refreshCommitGraph adds the commits reachable from tips (the distinct
// SHAs the sync that just completed applied) to repoPath's split
// commit-graph and reports how it ended (recordCommitGraphOutcome). A
// repo with no refs records nothing. The caller must hold the repo's
// hydrate lock: it serializes every commit-graph writer for the repo on
// this node. Never fails the caller: the outcome is informational.
func refreshCommitGraph(ctx context.Context, repoPath, slotKey string, tips *refTips) commitGraphOutcome {
	assertf(tips != nil, "refreshCommitGraph: nil tips")
	assertf(slotKey != "", "refreshCommitGraph: empty slot key")
	if tips.over {
		return recordCommitGraphOutcome(repoPath, outcomeSkippedTipCap,
			fmt.Sprintf("more than %d distinct ref tips (reads stay correct, only slower)", commitGraphMaxTips))
	}
	if len(tips.set) == 0 {
		return outcomeWritten // no refs, nothing to graph
	}
	// Join the shared git admission pool (#313's, gitlimit.ReadSlots) as
	// maintenance: try-only, because the caller holds the hydrate lock, and
	// at most gitlimit.MaintenanceShare refreshes at once, so reads keep
	// the rest of the pool. The permit is released after every attempt
	// has been reaped (writeCommitGraph returns only then).
	release, ok := gitlimit.TryAdmitMaintenance(slotKey)
	if !ok {
		return recordCommitGraphOutcome(repoPath, outcomeSkippedBusy,
			fmt.Sprintf("no admission: the shared git read pool or the %d-refresh maintenance share is full (the next full hydrate retries)", gitlimit.MaintenanceShare))
	}
	defer release()
	plain, err := writeCommitGraph(ctx, repoPath, slices.Sorted(maps.Keys(tips.set)))
	switch {
	case err == nil && plain:
		return recordCommitGraphOutcome(repoPath, outcomeWrittenPlain,
			fmt.Sprintf("first attempt exceeded %v; wrote a layer without Bloom filters, later writes on this node inherit that", commitGraphFirstBudget))
	case err == nil:
		return recordCommitGraphOutcome(repoPath, outcomeWritten, "")
	case errors.Is(err, context.Canceled):
		return recordCommitGraphOutcome(repoPath, outcomeCanceled, err.Error())
	case errors.Is(err, gitlimit.ErrMemoryLimit):
		return recordCommitGraphOutcome(repoPath, outcomeMemoryLimit, err.Error()+" (reads stay correct, only slower)")
	case errors.Is(err, errCommitGraphLockHeld):
		return recordCommitGraphOutcome(repoPath, outcomeLockHeld, err.Error()+" (see the stale-lock runbook; reads stay correct, only slower)")
	case errors.Is(err, context.DeadlineExceeded):
		return recordCommitGraphOutcome(repoPath, outcomeTimedOut, err.Error()+" (reads stay correct, only slower)")
	default:
		return recordCommitGraphOutcome(repoPath, outcomeFailed, err.Error()+" (reads stay correct, only slower)")
	}
}

// writeCommitGraph runs `git commit-graph write --stdin-commits --split` in
// the repo, walking only from tips (so its work is bounded by the tip
// count and the new history, never by the live ref namespace, which a
// concurrent receive-pack may be growing). Bloom filters (--changed-paths)
// are requested only when no graph exists yet; later writes inherit the
// choice from the existing chain. If the first attempt runs out of its own
// budget while the request and the shared deadline are still live, one
// retry writes a layer without Bloom filters (--no-changed-paths), which
// later writes then inherit; plain reports that the retry wrote it.
func writeCommitGraph(ctx context.Context, repoPath string, tips []string) (plain bool, err error) {
	assertf(repoPath != "", "writeCommitGraph: empty repo path")
	assertf(len(tips) > 0 && len(tips) <= commitGraphMaxTips, "writeCommitGraph: %d tips (cap %d)", len(tips), commitGraphMaxTips)
	for _, tip := range tips {
		assertf(ref.ValidSHA(tip), "writeCommitGraph: invalid tip %q", tip)
	}
	assertf(commitGraphDeadline > 0 && commitGraphFirstBudget > 0 && commitGraphKillDelay > 0,
		"writeCommitGraph: non-positive budget (deadline %v, first %v, kill %v)",
		commitGraphDeadline, commitGraphFirstBudget, commitGraphKillDelay)
	ctx, cancel := context.WithTimeout(ctx, commitGraphDeadline)
	defer cancel()
	stdin := strings.Join(tips, "\n") + "\n"
	args := []string{"commit-graph", "write", "--stdin-commits", "--split", "--no-progress"}
	first := args
	if !hasCommitGraph(repoPath) {
		first = append(slices.Clip(args), "--changed-paths")
	}
	err = runCommitGraphAttempt(ctx, repoPath, commitGraphFirstBudget, first, stdin)
	if !errors.Is(err, errCommitGraphBudget) {
		return false, err
	}
	// Only the terminal failure is wrapped (%w), so outcome classification
	// follows what ended the refresh, not the first attempt's timeout.
	if cerr := ctx.Err(); cerr != nil {
		return false, fmt.Errorf("%v; no retry: %w", err, cerr)
	}
	if rerr := runCommitGraphAttempt(ctx, repoPath, 0, append(slices.Clip(args), "--no-changed-paths"), stdin); rerr != nil {
		return false, fmt.Errorf("%v; plain retry: %w", err, rerr)
	}
	return true, nil
}

// hasCommitGraph reports whether repoPath already has a commit-graph (split
// chain or single file). An unexpected stat error counts as present, so
// the refresh inherits the existing mode instead of forcing Bloom filters.
func hasCommitGraph(repoPath string) bool {
	info := filepath.Join(repoPath, "objects", "info")
	for _, p := range []string{filepath.Join(info, "commit-graphs", "commit-graph-chain"), filepath.Join(info, "commit-graph")} {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			return true
		}
	}
	return false
}

// runCommitGraphAttempt runs one commit-graph write. budget > 0 bounds the
// attempt with errCommitGraphBudget as the cause; 0 runs it under ctx
// alone. cmd.Run returns only after the child is reaped, so the caller
// never releases the hydrate lock (or starts a retry) while it still runs.
func runCommitGraphAttempt(ctx context.Context, repoPath string, budget time.Duration, args []string, stdin string) error {
	assertf(budget >= 0, "runCommitGraphAttempt: negative budget %v", budget)
	actx := ctx
	if budget > 0 {
		var cancel context.CancelFunc
		actx, cancel = context.WithTimeoutCause(ctx, budget, errCommitGraphBudget)
		defer cancel()
	}
	// The same heap cap as the read API's Heavy children (#313), applied
	// on every path, including hydrates that no read budget covers
	// (upload-pack). No cap where the platform cannot enforce one.
	var heap int64
	if gitlimit.HeapLimitSupported() {
		heap = gitlimit.HeavyHeapBytes
	}
	observeGitSpawn(repoPath, args)
	cmd := gitlimit.Command(actx, heap, args...)
	cmd.Dir = repoPath
	cmd.Stdin = strings.NewReader(stdin)
	cmd.Env = commitGraphEnv(os.Environ())
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = commitGraphKillDelay
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		return nil
	}
	line := strings.Join(args, " ")
	if ctx.Err() == nil && errors.Is(context.Cause(actx), errCommitGraphBudget) {
		return fmt.Errorf("git %s: %w after %v", line, errCommitGraphBudget, budget)
	}
	if cerr := ctx.Err(); cerr != nil {
		err = fmt.Errorf("%w: %w", cerr, err)
	} else if heap > 0 && gitlimit.HeapFailure(err, stderr.String()) {
		err = fmt.Errorf("%w (%d bytes): %w", gitlimit.ErrMemoryLimit, heap, err)
	} else if strings.Contains(stderr.String(), ".lock': File exists") {
		// Git's lockfile message; the lock is never removed here (its
		// owner cannot be proven, ADR-47).
		err = fmt.Errorf("%w: %w", errCommitGraphLockHeld, err)
	}
	return fmt.Errorf("git %s: %w: %s", line, err, strings.TrimSpace(stderr.String()))
}

// commitGraphEnv is the child's environment: environ without any inherited
// GIT_* variable (GIT_DIR, GIT_OBJECT_DIRECTORY, GIT_CONFIG_COUNT/KEY/VALUE
// and friends could redirect or disable the write), with global and system
// config pinned to /dev/null like the API and sync Git subprocesses, and
// the C locale.
func commitGraphEnv(environ []string) []string {
	env := make([]string, 0, len(environ)+3)
	for _, kv := range environ {
		if !strings.HasPrefix(kv, "GIT_") {
			env = append(env, kv)
		}
	}
	// LC_ALL=C last (exec keeps the last value of a duplicate key): lock
	// and heap failures are classified from Git's English messages, which
	// an inherited translated locale would change.
	return append(env, "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "LC_ALL=C")
}
