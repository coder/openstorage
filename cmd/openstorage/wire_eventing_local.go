package main

import (
	"context"
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
	// Git-surface hooks record intent after the authority CAS, before
	// receive-pack publishes the cache ref: runs wait for publication.
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
