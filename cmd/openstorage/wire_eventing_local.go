package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/coder/openstorage/internal/jobs"
	"github.com/coder/openstorage/internal/meta"
	upsync "github.com/coder/openstorage/internal/sync"
)

const (
	localPendingInterval = time.Second
	localPendingPageSize = 16
	localPendingTimeout  = 10 * time.Second
)

// recoverLocalPushClaims runs ONCE before the local queue/listener exists.
// Only then can a claim be attributed to the previous process. Never run this
// repair from the live consumer: active claims do not imply new requests.
func recoverLocalPushClaims(ctx context.Context, ms meta.EventingStore, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ids, err := ms.PendingPushRepoIDs(ctx)
	if err != nil {
		logger.Warn("pending push boot recovery failed", "err", err)
		return
	}
	for _, id := range ids {
		// MAX-coalescing preserves pending force; a steal also inherits
		// the orphan's persisted PushClaimForce, just as before.
		if err := ms.RequestPush(ctx, id, false); err != nil {
			logger.Warn("pending push boot recovery re-mark failed", "repo", id, "err", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// startLocalPendingSync delays consumption until the listener is serving,
// then observes one bounded page per tick. A full traversal wraps, so a full
// queue, a raced claim, or a new marker behind the cursor gets another turn.
// Work goes through the worker's coordinated observation path, not ordinary
// caller enqueue (which could reset a retry's backoff/budget).
func startLocalPendingSync(ctx context.Context, ms meta.LocalPendingStore, org string, q *jobs.Local, logger *slog.Logger) func() {
	assertf(ms != nil && org != "" && q != nil && logger != nil, "startLocalPendingSync: missing deps")
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		delay := time.NewTimer(pendingSyncRecoveryDelay)
		defer delay.Stop()
		select {
		case <-ctx.Done():
			return
		case <-delay.C:
		}
		ticker := time.NewTicker(localPendingInterval)
		defer ticker.Stop()
		after := ""
		for ctx.Err() == nil {
			after = scanLocalPendingSync(ctx, ms, org, q, logger, after)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func scanLocalPendingSync(ctx context.Context, ms meta.LocalPendingStore, org string, q *jobs.Local, logger *slog.Logger, after string) string {
	ctx, cancel := context.WithTimeout(ctx, localPendingTimeout)
	defer cancel()
	work, err := ms.ListPendingSyncWork(ctx, org, after, localPendingPageSize)
	if err != nil {
		logger.Warn("local pending sync scan failed", "err", err)
		return after
	}
	assertf(len(work) <= localPendingPageSize, "unbounded local pending page: %d", len(work))
	if len(work) == 0 {
		return ""
	}
	for _, row := range work {
		assertf(row.RepoID > after && (row.Pull || row.Push), "invalid local pending row: %+v after %q", row, after)
		if ctx.Err() != nil {
			return after
		}
		job := upsync.Job{Org: org, RepoID: row.RepoID}
		for lane, pending := range []bool{row.Pull, row.Push} {
			if !pending {
				continue
			}
			enqueue := q.EnqueuePendingSync
			if lane == 1 {
				enqueue = q.EnqueuePendingPush
			}
			if err := enqueue(ctx, job); err != nil && !errors.Is(err, upsync.ErrPendingQueueFull) {
				logger.Warn("local pending sync enqueue failed", "repo", row.RepoID, "err", err)
			}
		}
		after = row.RepoID
	}
	return after
}
