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
// an unrelated later trigger: River's rescuer discards a job after
// syncRescueAttempts crashed attempts, and an operator can delete job rows.
// The scan lists the lanes whose claim guard holds and inserts their job
// again. River's uniqueness turns that insert into a no-op while a live job
// covers the lane, so every replica scans on its own. It inserts only the
// existing job kinds with the args EnqueueSync and EnqueuePush use, so
// replicas of an older binary run its jobs.

// laneScanPage is how many lanes one scan query reads (a var for tests).
var laneScanPage = 100

// laneScanTimeout bounds one scan pass. A pass that runs out of time keeps
// its cursor, and the next pass continues after the last lane it handled.
const laneScanTimeout = 30 * time.Second

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
		cursor, wait := "", delay
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			var err error
			if cursor, _, err = q.scanLanes(ctx, org, cursor); err != nil && ctx.Err() == nil {
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

// scanLanes runs one pass over org's lanes, starting after cursor: it lists
// pages of lanes with work (meta.PGListLaneWork) and requeues each lane's
// job (requeueLane). It returns the cursor for the next pass ("" after a
// full traversal) and how many jobs it inserted.
func (q *River) scanLanes(ctx context.Context, org, cursor string) (string, int, error) {
	assertf(org != "", "scanLanes: empty org")
	ctx, cancel := context.WithTimeout(ctx, laneScanTimeout)
	defer cancel()
	requeued := 0
	for {
		var page []meta.LocalWork
		if err := pgx.BeginFunc(ctx, q.pool, func(tx pgx.Tx) error {
			var err error
			page, err = meta.PGListLaneWork(ctx, tx, org, cursor, laneScanPage)
			return err
		}); err != nil {
			return cursor, requeued, err
		}
		for _, w := range page {
			for _, push := range []bool{true, false} {
				if (push && !w.Push) || (!push && !w.Pull) {
					continue
				}
				inserted, err := q.requeueLane(ctx, org, w.RepoID, push)
				if ctx.Err() != nil {
					return cursor, requeued, ctx.Err()
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
			cursor = w.RepoID
		}
		if len(page) < laneScanPage {
			return "", requeued, nil
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
