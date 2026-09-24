package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/coder/openstorage/internal/gitread"
)

// Bounds for the files/metadata `order` walks (#292 security review). Each
// walk is one single-threaded `git rev-list --topo-order`. Without a
// commit-graph it reads the ref's whole history even when the page is
// cheap (#307 tracks commit-graphs), so the walks are capped per process
// and per request.
const (
	// topoWalkMaxConcurrent caps simultaneous walks in this process.
	// Excess requests fail fast (no queue) with 503 + Retry-After.
	topoWalkMaxConcurrent = 4
	// topoWalkTimeout caps one walk. A 300,000-commit history took about
	// 3.4 s in the security review's reproduction, so this admits
	// histories of roughly a million commits.
	topoWalkTimeout = 20 * time.Second
	// topoWalkMaxHeapBytes caps each walk's git heap (RLIMIT_DATA, Linux
	// only; see gitread.TopoRanks). Measured on git 2.54 (musl) and 2.55
	// (glibc): a 300,000-commit linear history fits in 128 MiB, while the
	// same walk fails under 64 MiB. Pack files are mapped read-only and
	// are not counted: they are shared page cache.
	topoWalkMaxHeapBytes = 128 << 20
)

var (
	// errTopoWalkBusy: every walk slot is taken; nothing was started.
	errTopoWalkBusy = errors.New("commit order walk capacity exhausted")
	// errTopoWalkTimeout: the walk hit topoWalkTimeout and git was killed.
	errTopoWalkTimeout = errors.New("commit order walk exceeded the server time budget")
	// errTopoWalkTooLarge: the walk needed more heap than
	// topoWalkMaxHeapBytes. Retrying the same commit cannot succeed.
	errTopoWalkTooLarge = errors.New("history too large to compute commit order within the server memory budget")
)

// topoWalks is the process-wide budget shared by every server instance.
var topoWalks = newTopoWalkBudget(topoWalkMaxConcurrent, topoWalkTimeout, topoWalkMaxHeapBytes)

// topoWalkBudget runs gitread.TopoRanks with a concurrency cap, a
// deadline, and a per-walk heap cap. A slot is released only after TopoRanks returns, and
// TopoRanks always waits for (reaps) git before it returns, so a held slot
// always means at most one live git process.
type topoWalkBudget struct {
	slots   chan struct{}
	timeout time.Duration
	// heapLimitBytes is passed to gitread.TopoRanks; 0 = no cap.
	heapLimitBytes int64
}

func newTopoWalkBudget(maxConcurrent int, timeout time.Duration, heapLimitBytes int64) *topoWalkBudget {
	assertf(maxConcurrent > 0, "newTopoWalkBudget: maxConcurrent %d", maxConcurrent)
	assertf(timeout > 0, "newTopoWalkBudget: timeout %v", timeout)
	assertf(heapLimitBytes >= 0, "newTopoWalkBudget: heapLimitBytes %d", heapLimitBytes)
	return &topoWalkBudget{slots: make(chan struct{}, maxConcurrent), timeout: timeout, heapLimitBytes: heapLimitBytes}
}

// ranks returns gitread.TopoRanks(ctx, repoPath, rev, want) under the
// budget. It returns errTopoWalkBusy without starting git when no slot is
// free, and an error wrapping errTopoWalkTimeout when the walk outlives the
// budget's deadline, and an error wrapping errTopoWalkTooLarge when git
// exceeds the heap cap. Caller cancellation surfaces as TopoRanks' own
// error.
func (b *topoWalkBudget) ranks(ctx context.Context, repoPath, rev string, want []string) (map[string]int, error) {
	if len(want) == 0 {
		return map[string]int{}, nil // no walk, so no slot
	}
	select {
	case b.slots <- struct{}{}:
	default:
		return nil, errTopoWalkBusy
	}
	defer func() { <-b.slots }()

	walkCtx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	ranks, err := gitread.TopoRanks(walkCtx, repoPath, rev, want, b.heapLimitBytes)
	if err != nil && ctx.Err() == nil && errors.Is(walkCtx.Err(), context.DeadlineExceeded) {
		return nil, fmt.Errorf("%w (%v): %w", errTopoWalkTimeout, b.timeout, err)
	}
	if errors.Is(err, gitread.ErrMemoryLimit) {
		return nil, fmt.Errorf("%w: %w", errTopoWalkTooLarge, err)
	}
	return ranks, err
}
