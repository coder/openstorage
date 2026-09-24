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
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
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

// commitGraphMaxRefs caps the refs a refresh may walk. Ref fanout is
// attacker-scaled (a push can mint many same-tip refs cheaply), and
// `--reachable` resolves every ref on every write: 10,000 refs cost about
// 155 ms, 500,000 refs about 24 s. Above the cap the refresh is skipped;
// an existing graph stays in place and stays correct, only staler.
var commitGraphMaxRefs = 10000

// commitGraphSlots bounds concurrent refreshes per process, so writes across
// many repos can occupy at most cap(commitGraphSlots) CPUs. A refresh that
// finds every slot busy is skipped, not queued: the next full hydrate of
// that repo retries, and a stale graph only costs speed.
var commitGraphSlots = make(chan struct{}, 2)

// commitGraphGit is the binary the refresh runs. Test seam for driving the
// timeout, retry, and reaping paths with a scripted fake.
var commitGraphGit = "git"

// errCommitGraphBudget is the cancellation cause of an attempt that ran out
// of its own budget (as opposed to the request or the shared deadline).
var errCommitGraphBudget = errors.New("commit-graph attempt budget expired")

// refreshCommitGraph writes or extends repoPath's split commit-graph and
// logs any failure. refs is the repo's ref count from the sync that just
// completed. The caller must hold the repo's hydrate lock: it serializes
// every commit-graph writer for the repo on this node.
func refreshCommitGraph(ctx context.Context, repoPath string, refs int) {
	assertf(refs >= 0, "refreshCommitGraph: negative ref count %d", refs)
	if refs > commitGraphMaxRefs {
		log.Printf("commit-graph %s: skipped, %d refs exceed the %d-ref cap (reads stay correct, only slower)", repoPath, refs, commitGraphMaxRefs)
		return
	}
	select {
	case commitGraphSlots <- struct{}{}:
		defer func() { <-commitGraphSlots }()
	default:
		log.Printf("commit-graph %s: skipped, all %d refresh slots busy (the next full hydrate retries)", repoPath, cap(commitGraphSlots))
		return
	}
	if err := writeCommitGraph(ctx, repoPath); err != nil {
		log.Printf("commit-graph %s: %v (reads stay correct, only slower)", repoPath, err)
	}
}

// writeCommitGraph runs `git commit-graph write --reachable --split` in the
// repo. Bloom filters (--changed-paths) are requested only when no graph
// exists yet; later writes inherit the choice from the existing chain. If
// the first attempt runs out of its own budget while the request and the
// shared deadline are still live, one retry writes a layer without Bloom
// filters (--no-changed-paths), which later writes then inherit.
func writeCommitGraph(ctx context.Context, repoPath string) error {
	assertf(filepath.IsAbs(repoPath), "writeCommitGraph: relative repo path %q", repoPath)
	assertf(commitGraphDeadline > 0 && commitGraphFirstBudget > 0 && commitGraphKillDelay > 0,
		"writeCommitGraph: non-positive budget (deadline %v, first %v, kill %v)",
		commitGraphDeadline, commitGraphFirstBudget, commitGraphKillDelay)
	ctx, cancel := context.WithTimeout(ctx, commitGraphDeadline)
	defer cancel()
	args := []string{"commit-graph", "write", "--reachable", "--split", "--no-progress"}
	first := args
	if !hasCommitGraph(repoPath) {
		first = append(slices.Clip(args), "--changed-paths")
	}
	err := runCommitGraphAttempt(ctx, repoPath, commitGraphFirstBudget, first)
	if !errors.Is(err, errCommitGraphBudget) {
		return err
	}
	if cerr := ctx.Err(); cerr != nil {
		return fmt.Errorf("%w; no retry: %w", err, cerr)
	}
	if rerr := runCommitGraphAttempt(ctx, repoPath, 0, append(slices.Clip(args), "--no-changed-paths")); rerr != nil {
		return fmt.Errorf("%w; plain retry: %w", err, rerr)
	}
	log.Printf("commit-graph %s: first attempt exceeded %v; wrote a layer without Bloom filters, later writes on this node inherit that", repoPath, commitGraphFirstBudget)
	return nil
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
func runCommitGraphAttempt(ctx context.Context, repoPath string, budget time.Duration, args []string) error {
	assertf(budget >= 0, "runCommitGraphAttempt: negative budget %v", budget)
	actx := ctx
	if budget > 0 {
		var cancel context.CancelFunc
		actx, cancel = context.WithTimeoutCause(ctx, budget, errCommitGraphBudget)
		defer cancel()
	}
	observeGitSpawn(repoPath, args)
	cmd := exec.CommandContext(actx, commitGraphGit, args...)
	cmd.Dir = repoPath
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
	}
	return fmt.Errorf("git %s: %w: %s", line, err, strings.TrimSpace(stderr.String()))
}

// commitGraphEnv is the child's environment: environ without any inherited
// GIT_* variable (GIT_DIR, GIT_OBJECT_DIRECTORY, GIT_CONFIG_COUNT/KEY/VALUE
// and friends could redirect or disable the write), with global and system
// config pinned to /dev/null like the API and sync Git subprocesses.
func commitGraphEnv(environ []string) []string {
	env := make([]string, 0, len(environ)+3)
	for _, kv := range environ {
		if !strings.HasPrefix(kv, "GIT_") {
			env = append(env, kv)
		}
	}
	return append(env, "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
}
