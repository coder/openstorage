package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"

	"github.com/coder/openstorage/internal/meta"
)

// River's lane scan (PostgreSQL). Every upstream sync and push request
// lives on through its unique-by-repo River job, whose completion gate
// schedules what follows. Without a live job, a lane's durable work (a
// pending request, or an expired claim that holds a consumed one) waits for
// an unrelated later trigger. River's rescuer discards a job after
// syncRescueAttempts crashed attempts, an operator can delete job rows, and
// a process can die or a job insert can fail after a request's mark
// (pull-upstream, push-upstream and the forward poke mark first).
// The scan lists the lanes whose claim guard holds and inserts their job
// again. River's uniqueness turns that insert into a no-op while a live job
// covers the lane, so every replica scans on its own. It inserts only the
// existing job kinds with the args EnqueueSync and EnqueuePush use, so
// replicas of an older binary run its jobs.

// laneScanPage is how many lanes one scan query returns (a var for tests).
// No index covers the lane guards, so each query costs time in proportion
// to the number of linked repositories whatever the page size, and a large
// page keeps a mass requeue to few queries.
var laneScanPage = 1000

// laneScanTimeout bounds one scan pass (a var for tests). A pass that runs
// out of time keeps its cursor, and the next pass continues after the last
// lane it handled.
var laneScanTimeout = 30 * time.Second

// errLaneCovered rolls back a scan insert that the lane does not need.
var errLaneCovered = errors.New("lane covered")

// StartLaneScan scans org's lanes (scanLanes) after delay and then every
// interval, until DrainSyncRuns or Close stops it.
func (q *River) StartLaneScan(org string, delay, interval time.Duration) {
	assertf(q.started && org != "" && delay >= 0 && interval > 0,
		"StartLaneScan: started=%v org %q delay %v interval %v", q.started, org, delay, interval)
	ctx, cancel := context.WithCancel(context.Background())
	q.scanMu.Lock()
	assertf(q.scanStop == nil, "StartLaneScan: started twice or after the scan stopped")
	q.scanStop = cancel
	q.scanMu.Unlock()
	q.scanning.Go(func() {
		wait := delay
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			_, err := q.scanLanes(ctx, org)
			switch {
			case ctx.Err() != nil:
			case errors.Is(err, context.DeadlineExceeded):
				// Expected while many lanes need a job: the next pass
				// continues from the cursor.
				q.logger.Info("lane scan: pass ran out of time", "timeout", laneScanTimeout)
			case err != nil:
				q.logger.Warn("lane scan failed", "err", err)
			}
			wait = interval
		}
	})
}

// stopLaneScan stops the scan and waits for a running pass. A later
// StartLaneScan fails its assertion.
func (q *River) stopLaneScan() {
	q.scanMu.Lock()
	stop := q.scanStop
	q.scanStop = func() {}
	q.scanMu.Unlock()
	if stop != nil {
		stop()
	}
	q.scanning.Wait()
}

// scanLanes runs one pass over org's lanes, starting after q.scanCursor:
// it lists pages of lanes with work (meta.PGListLaneWork) and requeues
// each lane's job (requeueLane). It moves q.scanCursor past each lane it
// handled, back to "" after a full traversal, and returns how many jobs it
// inserted. A pass that ran out of time returns context.DeadlineExceeded.
func (q *River) scanLanes(ctx context.Context, org string) (int, error) {
	assertf(org != "", "scanLanes: empty org")
	ctx, cancel := context.WithTimeout(ctx, laneScanTimeout)
	defer cancel()
	requeued := 0
	for {
		var page []meta.LocalWork
		if err := pgx.BeginFunc(ctx, q.pool, func(tx pgx.Tx) error {
			var err error
			page, err = meta.PGListLaneWork(ctx, tx, org, q.scanCursor, laneScanPage)
			return err
		}); err != nil {
			if ctx.Err() != nil {
				return requeued, ctx.Err()
			}
			return requeued, err
		}
		for _, w := range page {
			for _, push := range []bool{true, false} {
				if (push && !w.Push) || (!push && !w.Pull) {
					continue
				}
				inserted, err := q.requeueLane(ctx, org, w.RepoID, push)
				if ctx.Err() != nil {
					return requeued, ctx.Err()
				}
				if err != nil {
					// The next traversal tries this lane again; the
					// lanes after it need not wait for it.
					q.logger.Warn("lane scan: requeue failed", "err", err)
				}
				if inserted {
					requeued++
				}
			}
			q.scanCursor = w.RepoID
		}
		if len(page) < laneScanPage {
			q.scanCursor = ""
			return requeued, nil
		}
	}
}

// requeueLane inserts the job of repoID's push or pull lane and reports
// whether River added a job. A duplicate of a live job is rolled back.
//
// A new job is kept only if the lane still has work after the insert, in
// the same transaction: the list read it without locks, and a run may have
// consumed it since. Only a run of a live job claims, and after an insert
// that was not a duplicate the new job is the lane's only live job, which
// cannot run before this transaction commits. An insert that met a job
// whose completion transaction held its row waited for that commit. So the
// re-check sees every claim that consumed the listed work, and a lane
// without work gets no extra run.
func (q *River) requeueLane(ctx context.Context, org, repoID string, push bool) (bool, error) {
	lane := "pull"
	var args river.JobArgs = syncArgs{Org: org, RepoID: repoID}
	if push {
		lane, args = "push", pushArgs{Org: org, RepoID: repoID}
	}
	err := pgx.BeginFunc(ctx, q.pool, func(tx pgx.Tx) error {
		res, err := q.client.InsertTx(ctx, tx, args, nil)
		if err != nil {
			return err
		}
		if res.UniqueSkippedAsDuplicate {
			return errLaneCovered // a live job covers the lane
		}
		w, err := meta.PGLaneWork(ctx, tx, repoID)
		if err != nil {
			return err
		}
		if (push && !w.Push) || (!push && !w.Pull) {
			return errLaneCovered
		}
		return nil
	})
	if errors.Is(err, errLaneCovered) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("requeue %s lane of repo %s: %w", lane, repoID, err)
	}
	q.logger.Warn("lane scan: requeued a lane whose work had no job",
		"org", org, "repo", repoID, "lane", lane)
	return true, nil
}
