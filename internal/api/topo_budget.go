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
)

var (
	// errTopoWalkBusy: every walk slot is taken; nothing was started.
	errTopoWalkBusy = errors.New("commit order walk capacity exhausted")
	// errTopoWalkTimeout: the walk hit topoWalkTimeout and git was killed.
	errTopoWalkTimeout = errors.New("commit order walk exceeded the server time budget")
)

// topoWalks is the process-wide budget shared by every server instance.
var topoWalks = newTopoWalkBudget(topoWalkMaxConcurrent, topoWalkTimeout)

// topoWalkBudget runs gitread.TopoRanks with a concurrency cap and a
// deadline. A slot is released only after TopoRanks returns, and
// TopoRanks always waits for (reaps) git before it returns, so a held slot
// always means at most one live git process.
type topoWalkBudget struct {
	slots   chan struct{}
	timeout time.Duration
}

func newTopoWalkBudget(maxConcurrent int, timeout time.Duration) *topoWalkBudget {
	assertf(maxConcurrent > 0, "newTopoWalkBudget: maxConcurrent %d", maxConcurrent)
	assertf(timeout > 0, "newTopoWalkBudget: timeout %v", timeout)
	return &topoWalkBudget{slots: make(chan struct{}, maxConcurrent), timeout: timeout}
}

// ranks returns gitread.TopoRanks(ctx, repoPath, rev, want) under the
// budget. It returns errTopoWalkBusy without starting git when no slot is
// free, and an error wrapping errTopoWalkTimeout when the walk outlives the
// budget's deadline. Caller cancellation surfaces as TopoRanks' own error.
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
	ranks, err := gitread.TopoRanks(walkCtx, repoPath, rev, want)
	if err != nil && ctx.Err() == nil && errors.Is(walkCtx.Err(), context.DeadlineExceeded) {
		return nil, fmt.Errorf("%w (%v): %w", errTopoWalkTimeout, b.timeout, err)
	}
	return ranks, err
}
