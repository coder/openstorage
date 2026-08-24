// Package lfsscan is the leaf reachability layer of unreferenced-LFS-object
// reclamation (issue #73): a lenient Git LFS pointer-file decoder and a
// bounded `git rev-list`/`cat-file` walk collecting the OIDs reachable
// pointer files reference. It deliberately sits below internal/gitread —
// internal/jobs consumes it, and gitread's githttp dependency would cycle.
package lfsscan

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/coder/code-storage/internal/ref"
)

// assertf is a fail-fast invariant check for programming errors only.
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("lfsscan assertion failed: "+format, args...))
	}
}

// ErrLFSScanBudget marks a reachability walk aborted because it listed more
// objects than the caller's budget allows. The partial OID set is unusable
// for reclamation — an incomplete reachable set must never retire anything —
// so callers skip the repository and retry when budget allows.
var ErrLFSScanBudget = errors.New("lfs reachability scan budget exceeded")

// ReachableLFSOIDs walks every object reachable from the cache repo's refs
// (the DB-authority set Materialize just synced), decodes candidate blobs of
// at most LFSPointerMaxSize bytes as Git LFS pointer files, and returns the
// set of referenced sha256 OIDs plus the number of objects the walk listed.
//
// The walk streams three git subprocesses — rev-list --objects (with a blob
// size prefilter), cat-file --batch-check (type/size), cat-file --batch
// (candidate contents) — so memory stays O(pointers found), not O(objects).
// Aborting past budget kills the pipeline and returns ErrLFSScanBudget; any
// subprocess failure (including an object reported missing) fails the walk,
// because reclamation must only ever act on a provably complete scan.
func ReachableLFSOIDs(ctx context.Context, repoPath string, budget int64) (map[string]struct{}, int64, error) {
	assertf(repoPath != "", "ReachableLFSOIDs: empty repoPath")
	assertf(budget > 0, "ReachableLFSOIDs: non-positive budget %d", budget)

	g, gctx := errgroup.WithContext(ctx)

	revList := exec.CommandContext(gctx, "git", "rev-list", "--objects", "--no-object-names",
		fmt.Sprintf("--filter=blob:limit=%d", LFSPointerMaxSize+1), "--all")
	revList.Dir = repoPath
	var revErr bytes.Buffer
	revList.Stderr = &revErr
	revOut, err := revList.StdoutPipe()
	if err != nil {
		return nil, 0, fmt.Errorf("rev-list stdout: %w", err)
	}

	check := exec.CommandContext(gctx, "git", "cat-file", "--batch-check=%(objecttype) %(objectsize) %(objectname)", "--buffer")
	check.Dir = repoPath
	var checkErr bytes.Buffer
	check.Stderr = &checkErr
	checkIn, err := check.StdinPipe()
	if err != nil {
		return nil, 0, fmt.Errorf("batch-check stdin: %w", err)
	}
	checkOut, err := check.StdoutPipe()
	if err != nil {
		return nil, 0, fmt.Errorf("batch-check stdout: %w", err)
	}

	read := exec.CommandContext(gctx, "git", "cat-file", "--batch", "--buffer")
	read.Dir = repoPath
	var readErr bytes.Buffer
	read.Stderr = &readErr
	readIn, err := read.StdinPipe()
	if err != nil {
		return nil, 0, fmt.Errorf("batch stdin: %w", err)
	}
	readOut, err := read.StdoutPipe()
	if err != nil {
		return nil, 0, fmt.Errorf("batch stdout: %w", err)
	}

	for _, cmd := range []*exec.Cmd{revList, check, read} {
		if err := cmd.Start(); err != nil {
			return nil, 0, fmt.Errorf("start %s: %w", strings.Join(cmd.Args[:3], " "), err)
		}
		// Reap on every exit path; pump goroutines Wait first on success.
		defer func(c *exec.Cmd) { _ = c.Wait() }(cmd) //nolint:errcheck // best-effort reap after pipeline teardown
	}

	var walked atomic.Int64
	var exceeded atomic.Bool

	// Pump 1: reachable object names → batch-check, charging the budget.
	g.Go(func() error {
		defer checkIn.Close()
		sc := bufio.NewScanner(revOut)
		for sc.Scan() {
			sha := sc.Text()
			if !ref.ValidSHA(sha) {
				return fmt.Errorf("rev-list emitted %q", sha)
			}
			if walked.Add(1) > budget {
				exceeded.Store(true)
				return fmt.Errorf("%w (budget %d)", ErrLFSScanBudget, budget)
			}
			if _, err := io.WriteString(checkIn, sha+"\n"); err != nil {
				return fmt.Errorf("feed batch-check: %w", err)
			}
		}
		if err := sc.Err(); err != nil {
			return fmt.Errorf("read rev-list: %w", err)
		}
		if err := revList.Wait(); err != nil {
			return fmt.Errorf("rev-list: %w: %s", err, strings.TrimSpace(revErr.String()))
		}
		return nil
	})

	// Pump 2: pointer-sized blobs → batch content reader.
	g.Go(func() error {
		defer readIn.Close()
		sc := bufio.NewScanner(checkOut)
		for sc.Scan() {
			fields := strings.Fields(sc.Text())
			if len(fields) != 3 || fields[0] == "missing" || fields[1] == "missing" {
				return fmt.Errorf("batch-check emitted %q", sc.Text())
			}
			if fields[0] != "blob" {
				continue
			}
			size, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				return fmt.Errorf("batch-check emitted size %q", sc.Text())
			}
			if size <= 0 || size > LFSPointerMaxSize {
				continue
			}
			if _, err := io.WriteString(readIn, fields[2]+"\n"); err != nil {
				return fmt.Errorf("feed batch: %w", err)
			}
		}
		if err := sc.Err(); err != nil {
			return fmt.Errorf("read batch-check: %w", err)
		}
		if err := check.Wait(); err != nil {
			return fmt.Errorf("cat-file --batch-check: %w: %s", err, strings.TrimSpace(checkErr.String()))
		}
		return nil
	})

	// Pump 3: decode candidate blobs as pointer files.
	oids := make(map[string]struct{})
	g.Go(func() error {
		br := bufio.NewReader(readOut)
		buf := make([]byte, LFSPointerMaxSize+1)
		for {
			header, err := br.ReadString('\n')
			if errors.Is(err, io.EOF) && header == "" {
				break
			}
			if err != nil {
				return fmt.Errorf("read batch header: %w", err)
			}
			fields := strings.Fields(strings.TrimSuffix(header, "\n"))
			if len(fields) != 3 || fields[1] != "blob" {
				return fmt.Errorf("batch emitted header %q", header)
			}
			size, err := strconv.ParseInt(fields[2], 10, 64)
			if err != nil || size < 0 || size > LFSPointerMaxSize {
				return fmt.Errorf("batch emitted header %q", header)
			}
			content := buf[:size]
			if _, err := io.ReadFull(br, content); err != nil {
				return fmt.Errorf("read batch content: %w", err)
			}
			if nl, err := br.ReadByte(); err != nil || nl != '\n' {
				return fmt.Errorf("batch record for %s missing trailer", fields[0])
			}
			if oid, ok := ParseLFSPointer(content); ok {
				oids[oid] = struct{}{}
			}
		}
		if err := read.Wait(); err != nil {
			return fmt.Errorf("cat-file --batch: %w: %s", err, strings.TrimSpace(readErr.String()))
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		if exceeded.Load() {
			return nil, walked.Load(), fmt.Errorf("%w (budget %d)", ErrLFSScanBudget, budget)
		}
		return nil, walked.Load(), err
	}
	return oids, walked.Load(), nil
}
