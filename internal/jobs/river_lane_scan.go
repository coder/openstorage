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

// laneRequeueTimeout bounds one lane's requeue. An insert that waits on a
// locked job row then fails, and the pass goes on to the next lane.
const laneRequeueTimeout = 5 * time.Second

// errLaneCovered rolls back a scan insert that the lane does not need.
var errLaneCovered = errors.New("lane covered")

// errScanOutOfTime ends a pass that handled lanes until laneScanTimeout.
var errScanOutOfTime = errors.New("lane scan pass ran out of time")

// StartLaneScan scans org's lanes (scanLanes) after delay and then every
// interval, until DrainSyncRuns or Close stops it.
func (q *River) StartLaneScan(org string, delay, interval time.Duration) {
	assertf(q.started && org != "" && delay >= 0 && interval > 0,
		"StartLaneScan: started=%v org %q delay %v interval %v", q.started, org, delay, interval)
	ctx, cancel := context.WithCancel(context.Background())
	// The lock covers scanning.Go, so a stopLaneScan waits for the pass it
	// starts, and a failed assertion does not leave the lock held.
	q.scanMu.Lock()
	defer q.scanMu.Unlock()
	assertf(q.scanStop == nil, "StartLaneScan: started twice or after the scan stopped")
	q.scanStop = cancel
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
			case errors.Is(err, errScanOutOfTime):
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
// inserted. A pass that handled lanes until laneScanTimeout returns
// errScanOutOfTime. One that handled none in that time returns an error
// that wraps context.DeadlineExceeded: the database or its list query is
// too slow.
func (q *River) scanLanes(parent context.Context, org string) (int, error) {
	assertf(org != "", "scanLanes: empty org")
	ctx, cancel := context.WithTimeout(parent, laneScanTimeout)
	defer cancel()
	requeued, moved := 0, false
	outOfTime := func() (int, error) {
		switch {
		case parent.Err() != nil:
			return requeued, parent.Err()
		case moved:
			return requeued, errScanOutOfTime
		default:
			return requeued, fmt.Errorf("lane scan handled no lane in %v: %w", laneScanTimeout, ctx.Err())
		}
	}
	for {
		var page []meta.LocalWork
		if err := pgx.BeginFunc(ctx, q.pool, func(tx pgx.Tx) error {
			var err error
			page, err = meta.PGListLaneWork(ctx, tx, org, q.scanCursor, laneScanPage)
			return err
		}); err != nil {
			if ctx.Err() != nil {
				return outOfTime()
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
					return outOfTime()
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
			q.scanCursor, moved = w.RepoID, true
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
	ctx, cancel := context.WithTimeout(ctx, laneRequeueTimeout)
	defer cancel()
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
