package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/githttp"
	"github.com/coder/openstorage/internal/jobs"
	"github.com/coder/openstorage/internal/meta"
	upsync "github.com/coder/openstorage/internal/sync"
)

// localSweepInterval is how often the SQLite local schedule turns due rows
// (backoffs that ended, expired claims, intent recorded while a run held
// the lane) into runs (issue #266, ADR-50).
const localSweepInterval = time.Second

// buildLocalSyncQueue builds the non-Postgres jobs queue and its cleanup.
// SQLite provides the durable local schedule: runs claim through the row,
// a sweep replaces boot recovery and the process-local retry timers, and
// explicit enqueues record their request before offering a run. S3WAL
// keeps the in-process queue with boot recovery (issue #335); it fails
// when the boot re-mark of orphaned pull claims fails (issue #424).
func buildLocalSyncQueue(ms meta.Store, org, cacheDir string, worker *upsync.Worker, deliverer *events.Deliverer, repack *jobs.Repacker, logger *slog.Logger) (jobs.Queue, func(), error) {
	if lanes, ok := upsync.MetaLocalLanes(ms); ok {
		assertf(cacheDir != "", "buildLocalSyncQueue: the publication wait needs the cache directory")
		worker.Local = lanes
		// Git-surface hooks record intent after the authority CAS, before
		// receive-pack publishes the cache ref: runs wait for publication.
		worker.WaitPublication = func(ctx context.Context, repoID string) error {
			return githttp.WaitForPushPublication(ctx, cacheDir, repoID)
		}
		sched := upsync.NewLocalScheduler(lanes, org, worker.Run, worker.RunPush, logger)
		queue := jobs.NewLocalScheduled(deliverer, sched, repack, logger)
		// The first pass waits for the loopback listener, like the boot
		// recovery it replaces; claims left by a dead process become due
		// when their lease expires.
		sched.Start(pendingSyncRecoveryDelay, localSweepInterval)
		return queue, queue.Close, nil
	}
	// Before the queue exists, so no claim of this process can be seen.
	if err := remarkOrphanedSyncClaims(ms); err != nil {
		return nil, nil, err
	}
	queue := jobs.NewLocal(deliverer, worker.Run, worker.RunPush, repack, logger)
	// Pending pull-upstream requests that arrive while a run is in
	// flight are re-enqueued by the finishing run (issue #79); the
	// in-process queue never deduplicates against a running run, so
	// a plain re-enqueue is sound here. The push lane mirrors it and
	// additionally drives the transient-retry backoff timers (#90).
	worker.Requeue = queue.EnqueueSync
	worker.RequeuePush = queue.EnqueuePush
	// Boot recovery (issue #86): queued local work and deferred retry
	// timers die with the process, but the SyncPending/PushPending
	// markers are durable — re-enqueue marked repos so accepted
	// pull-upstream and push-upstream (#90) requests survive restarts
	// (in particular the restart that applies
	// OPENSTORAGE_SYNC_STEAL_LEGACY). Delayed so the loopback
	// listener is serving before the first recovered run pushes; a
	// run that still meets a held claim re-arms the deferred retry.
	recovery := time.AfterFunc(pendingSyncRecoveryDelay, func() {
		rctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		ids, err := ms.PendingSyncRepoIDs(rctx)
		if err != nil {
			logger.Warn("pending sync boot recovery failed", "err", err)
		} else {
			for _, id := range ids {
				if err := queue.EnqueueSync(rctx, upsync.Job{Org: org, RepoID: id}); err != nil {
					logger.Warn("pending sync boot recovery enqueue failed", "repo", id, "err", err)
				}
			}
		}
		pushIDs, err := ms.PendingPushRepoIDs(rctx)
		if err != nil {
			logger.Warn("pending push boot recovery failed", "err", err)
			return
		}
		for _, id := range pushIDs {
			// Re-mark BEFORE enqueueing: the listing includes repos
			// whose previous process died AFTER its claim consumed
			// the marker (orphaned push_syncing rows — always dead
			// here, local mode is single-process). Their recovered
			// run meets the unexpired dead lease and skips; only a
			// set marker arms the deferred retry that steals it
			// after expiry. MAX-coalescing keeps any existing level
			// (never downgrades force), and the dead claim's own
			// force mandate is inherited at the steal regardless
			// (push_claim_force).
			if err := ms.RequestPush(rctx, id, false); err != nil {
				logger.Warn("pending push boot recovery re-mark failed", "repo", id, "err", err)
				continue
			}
			if err := queue.EnqueuePush(rctx, upsync.Job{Org: org, RepoID: id}); err != nil {
				logger.Warn("pending push boot recovery enqueue failed", "repo", id, "err", err)
			}
		}
	})
	return queue, func() {
		recovery.Stop()
		queue.Close()
	}, nil
}

// remarkOrphanedSyncTimeout bounds each boot re-mark call.
const remarkOrphanedSyncTimeout = 30 * time.Second

// remarkOrphanedSyncClaims re-marks sync_pending on every repo the boot
// scan lists (issue #424). A pull that crashed while it held its claim
// consumed the marker, so without the re-mark its recovered run meets
// the dead, unexpired lease, skips, and arms no deferred retry. The
// caller runs it before this process can claim anything: S3-WAL serve
// holds the instance lease by now, so every claim seen here is dead. If
// an operator disabled the instance lease and a second serve holds a
// live claim, the re-mark costs one follow-up run after that run
// finishes, never a concurrent one: TryBeginSync refuses every claim
// until its lease expires. An error fails the boot, because a later
// retry would race this process's own claims; nothing is consumed, so
// the next boot retries.
func remarkOrphanedSyncClaims(ms meta.Store) error {
	ctx, cancel := context.WithTimeout(context.Background(), remarkOrphanedSyncTimeout)
	ids, err := ms.PendingSyncRepoIDs(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("re-mark orphaned sync claims: %w", err)
	}
	for _, id := range ids {
		ctx, cancel := context.WithTimeout(context.Background(), remarkOrphanedSyncTimeout)
		err := ms.RequestSync(ctx, id)
		cancel()
		if err != nil && !errors.Is(err, meta.ErrNotFound) { // detached meanwhile: nothing owed
			return fmt.Errorf("re-mark orphaned sync claim of repo %s: %w", id, err)
		}
	}
	return nil
}
