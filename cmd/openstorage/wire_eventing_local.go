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

// localSweepInterval is how often the local schedule turns due rows
// (backoffs that ended, expired claims, intent recorded while a run held
// the lane) into runs (issue #266, ADR-50).
const localSweepInterval = time.Second

// buildLocalSyncQueue builds the non-Postgres jobs queue and its cleanup.
// SQLite and S3WAL provide the durable local schedule (ADR-50, ADR-51):
// runs claim through the row, a sweep replaces boot recovery and
// process-local retry timers, and explicit enqueues record their request
// before offering a run.
func buildLocalSyncQueue(ms meta.Store, org, cacheDir string, worker *upsync.Worker, deliverer *events.Deliverer, repack *jobs.Repacker, logger *slog.Logger) (jobs.Queue, func(), error) {
	lanes, ok := upsync.MetaLocalLanes(ms)
	if !ok {
		return nil, nil, fmt.Errorf("metadata store %T has no local schedule", ms)
	}
	assertf(cacheDir != "", "buildLocalSyncQueue: the publication wait needs the cache directory")
	worker.Local = lanes
	// Git-surface hooks record intent before the authority CAS, and
	// receive-pack publishes the cache ref after it: runs wait for
	// publication.
	worker.WaitPublication = func(ctx context.Context, repoID string) error {
		return githttp.WaitForPushPublication(ctx, cacheDir, repoID)
	}
	sched := upsync.NewLocalScheduler(lanes, org, worker.Run, worker.RunPush, logger)
	queue := jobs.NewLocalScheduled(deliverer, sched, repack, logger)
	// The first pass waits for the loopback listener. Claims left by a
	// dead process become due when their lease expires.
	sched.Start(pendingSyncRecoveryDelay, localSweepInterval)
	return queue, queue.Close, nil
}

// restIntentBeforeCAS is the serve decorator's HeldBeforeCAS on the local
// schedule: a REST write to a forward_on_push repository by a caller that
// may forward holds the ref in the cache, then records the forward intent
// (RequestPush), before its authority CAS (ADR-50 amendment), as the update
// hook does. Serve dying after the CAS, or a failed poke after it, then
// cannot leave an accepted write that no run forwards, and a run that
// claims the intent waits in its publication wait until the CAS returned
// and the hold ended. A failed read, hold or intent write refuses the
// write. The pull poke stays after the CAS (enqueueSyncOrForward).
func restIntentBeforeCAS(ms meta.EventingStore, cacheDir string, forwardDenied func(ctx context.Context) bool) func(ctx context.Context, repoID, refName string) (func(), error) {
	assertf(ms != nil && cacheDir != "" && forwardDenied != nil, "restIntentBeforeCAS: missing deps")
	noop := func() {}
	return func(ctx context.Context, repoID, refName string) (func(), error) {
		up, err := ms.GetRepoUpstream(ctx, repoID)
		if errors.Is(err, meta.ErrNotFound) {
			return noop, nil // no upstream link: nothing to forward
		}
		if err != nil {
			return nil, err
		}
		if !up.ForwardOnPush || forwardDenied(ctx) {
			return noop, nil // the same gates as the forward poke
		}
		release, err := githttp.HoldRef(ctx, cacheDir, repoID, refName)
		if err != nil {
			return nil, fmt.Errorf("hold %s: %w", refName, err)
		}
		if err := ms.RequestPush(ctx, repoID, false); err != nil && !errors.Is(err, meta.ErrNotFound) {
			release()
			return nil, err
		}
		return release, nil
	}
}
