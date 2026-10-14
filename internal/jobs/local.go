package jobs

import (
	"context"
	"errors"
	"log/slog"
	stdsync "sync"
	"time"

	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/sync"
)

// Local is the in-process queue for local/sqlite mode. Deliveries run on
// their own goroutines through events.Deliverer.Deliver (which owns the
// re-sign-per-attempt retry loop); sync runs go through
// sync.InProcessQueue (per-repo coalescing). Queued work is lost on
// process restart — the documented local-mode ADR trade-off.
type Local struct {
	deliverer *events.Deliverer
	syncq     localSyncQueue
	repacker  *Repacker // nil: repack disabled on this queue
	logger    *slog.Logger

	mu        stdsync.Mutex
	closed    bool
	repacking map[string]bool // repos with an in-flight repack goroutine
	// repackPending remembers at most ONE follow-up per repo for triggers
	// that fired while a fold was running: the running fold works a row
	// snapshot from its start, so later registrations survive it and would
	// otherwise strand above the threshold until the next write (mirrors
	// River's queued-states-only uniqueness).
	repackPending map[string]bool
	// repackSem bounds CONCURRENT fold execution across repos (river caps
	// its workers at 8; without a cap here, git:write callers crossing the
	// threshold in many repos/namespaces would multiply the per-fold
	// budgets by the repo count). Goroutines beyond the cap park cheaply
	// on the semaphore; closing stops them via repackDone.
	repackSem  chan struct{}
	repackDone chan struct{} // closed by Close: unblocks semaphore waiters
	wg         stdsync.WaitGroup
}

// localRepackWorkers caps concurrently EXECUTING local folds (the per-repo
// goroutines beyond it wait): aggregate fold resources stay bounded by
// cap × per-fold budgets instead of scaling with attacker-creatable repos.
const localRepackWorkers = 4

var (
	_ Queue                   = (*Local)(nil)
	_ sync.FirstSyncScheduler = (*Local)(nil)
)

// NewLocal builds the in-process queue. deliverer must be configured with
// the webhook lookup; runSync is normally sync.(*Worker).Run and runPush
// sync.(*Worker).RunPush (issue #90). repacker may be nil (repack disabled
// or not wired, e.g. hook-subprocess queues that must never fold);
// EnqueueRepack is then refused.
func NewLocal(deliverer *events.Deliverer, runSync, runPush SyncRunner, repacker *Repacker, logger *slog.Logger) *Local {
	assertf(runSync != nil, "NewLocal: nil sync runner")
	assertf(runPush != nil, "NewLocal: nil push runner")
	assertf(logger != nil, "NewLocal: nil logger")
	return newLocal(deliverer, sync.NewInProcessQueue(runSync, runPush, logger), repacker, logger)
}

// localSyncQueue is the sync/push half of the in-process queue:
// sync.InProcessQueue, or sync.LocalScheduler on SQLite (ADR-50).
type localSyncQueue interface {
	EnqueueSync(ctx context.Context, job sync.Job) error
	EnqueuePush(ctx context.Context, job sync.Job) error
	Close()
}

// NewLocalScheduled is NewLocal with sync and push runs driven by the
// durable SQLite schedule (issue #266). Close closes sched.
func NewLocalScheduled(deliverer *events.Deliverer, sched *sync.LocalScheduler, repacker *Repacker, logger *slog.Logger) *Local {
	assertf(sched != nil, "NewLocalScheduled: nil scheduler")
	return newLocal(deliverer, sched, repacker, logger)
}

func newLocal(deliverer *events.Deliverer, syncq localSyncQueue, repacker *Repacker, logger *slog.Logger) *Local {
	assertf(deliverer != nil, "NewLocal: nil deliverer")
	assertf(logger != nil, "NewLocal: nil logger")
	return &Local{
		deliverer:     deliverer,
		syncq:         syncq,
		repacker:      repacker,
		logger:        logger,
		repacking:     make(map[string]bool),
		repackPending: make(map[string]bool),
		repackSem:     make(chan struct{}, localRepackWorkers),
		repackDone:    make(chan struct{}),
	}
}

// EnqueueWebhookDelivery starts the delivery's retry loop on a background
// goroutine with a fresh context: it must outlive the HTTP request (or CAS
// call) that enqueued it. Terminal failures are logged by the Deliverer.
func (q *Local) EnqueueWebhookDelivery(_ context.Context, d events.Delivery) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return errors.New("jobs queue is closed")
	}
	q.wg.Go(func() {
		// Deliver logs retries and the final discard itself.
		_ = q.deliverer.Deliver(context.Background(), d)
	})
	return nil
}

// EnqueueSync delegates to the coalescing in-process sync queue.
func (q *Local) EnqueueSync(ctx context.Context, job sync.Job) error {
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return errors.New("jobs queue is closed")
	}
	return q.syncq.EnqueueSync(ctx, job)
}

// OfferSync offers a pull run for a request the caller already recorded
// (API pull-upstream). On the SQLite schedule it does not record the
// request again; the in-process queue has no such distinction and
// enqueues.
func (q *Local) OfferSync(ctx context.Context, job sync.Job) error {
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return errors.New("jobs queue is closed")
	}
	if o, ok := q.syncq.(interface {
		OfferSync(ctx context.Context, job sync.Job) error
	}); ok {
		return o.OfferSync(ctx, job)
	}
	return q.syncq.EnqueueSync(ctx, job)
}

// TracksCreateState is true only on the local schedule (SQLite, S3WAL);
// the in-process queue never completes a create.
func (q *Local) TracksCreateState() bool {
	_, ok := q.syncq.(sync.FirstSyncScheduler)
	return ok
}

// CompleteSyncCreate delegates to the SQLite schedule (issue #388).
func (q *Local) CompleteSyncCreate(ctx context.Context, org, repoID string) (meta.CreateOutcome, error) {
	fs, ok := q.syncq.(sync.FirstSyncScheduler) // the scheduler refuses after Close
	assertf(ok, "CompleteSyncCreate on a queue that tracks no create state")
	return fs.CompleteSyncCreate(ctx, org, repoID)
}

// EnqueuePush delegates to the in-process queue's outbound lane (issue #90).
func (q *Local) EnqueuePush(ctx context.Context, job sync.Job) error {
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return errors.New("jobs queue is closed")
	}
	return q.syncq.EnqueuePush(ctx, job)
}

// EnqueueRepack runs one fold attempt on a background goroutine with a
// fresh context (it must outlive the HTTP request that triggered it). At
// most one repack per repo is in flight; a trigger landing while one runs
// marks ONE pending rerun that starts when the current fold finishes —
// mirroring River's unique-by-repo jobs, whose uniqueness spans only the
// queued states so a mid-run trigger queues exactly one follow-up. Queued
// work is lost on process restart (the documented local-mode trade-off).
func (q *Local) EnqueueRepack(_ context.Context, repo string) error {
	assertf(repo != "", "EnqueueRepack: empty repo")
	if q.repacker == nil {
		return errors.New("jobs: repack not configured on this queue")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return errors.New("jobs queue is closed")
	}
	if q.repacking[repo] {
		q.repackPending[repo] = true // one follow-up after the in-flight fold
		return nil
	}
	q.repacking[repo] = true
	q.wg.Add(1)
	go q.runRepack(repo)
	return nil
}

// runRepack executes fold attempts for repo until neither a pending trigger
// nor the follow-up threshold demands another one (or the queue is closing
// — pending local work dies with the process, the documented trade-off).
// Reruns require the previous attempt to have (possibly) folded: rerunning
// after a clean lost race would be the eager retry the design rejects, and
// rerunning after a budget no-op would loop pointlessly. The NeedsFollowUp
// recheck mirrors the river worker: a budgeted fold of a large backlog
// (e.g. rows accumulated while local queued work was lost to a restart)
// must keep folding until the configured bound is restored, not stop after
// one slice because no concurrent trigger happened to land.
func (q *Local) runRepack(repo string) {
	defer q.wg.Done()
	for {
		// Execution slot: bounds aggregate fold work across repos. A
		// waiter unblocks on Close and exits without folding (queued
		// local work dies with the process, the documented trade-off).
		// Go picks RANDOMLY among ready select cases, so a free slot can
		// win over the closed done channel — recheck closure after
		// acquiring, or shutdown could sit through full fold batches it
		// meant to discard.
		exit := func() {
			q.mu.Lock()
			delete(q.repacking, repo)
			delete(q.repackPending, repo)
			q.mu.Unlock()
		}
		select {
		case q.repackSem <- struct{}{}:
			q.mu.Lock()
			closed := q.closed
			q.mu.Unlock()
			if closed {
				<-q.repackSem
				exit()
				return
			}
		case <-q.repackDone:
			exit()
			return
		}
		folded, err := q.repacker.Repack(context.Background(), repo)
		<-q.repackSem
		if err != nil {
			q.logger.Warn("repack failed", "repo", repo, "err", err)
		}
		// The follow-up recheck does I/O, so it runs outside the lock. A
		// read failure defers to the next trigger (log-only): unlike the
		// river worker there is no durable job to preserve here, and the
		// pending-flag consumption below still covers racing triggers.
		_, due, ferr := q.repacker.FollowUpDue(context.Background(), repo)
		if ferr != nil {
			q.logger.Warn("repack follow-up recheck failed", "repo", repo, "err", ferr)
		}
		needMore := folded && due
		// Consume the pending flag in the SAME critical section that
		// decides exit: a trigger landing after the recheck above but
		// before this lock must either be observed here (we loop) or
		// arrive after we cleared repacking (EnqueueRepack starts a fresh
		// worker) — there is no in-between where it could be deleted
		// unseen. A consumed trigger always earns one more fold attempt,
		// regardless of this attempt's outcome: it fired on a real
		// threshold crossing, an extra attempt is at worst a cheap no-op,
		// and consumption bounds the loop by trigger volume.
		q.mu.Lock()
		pending := q.repackPending[repo]
		delete(q.repackPending, repo)
		if q.closed || (!pending && !needMore) {
			delete(q.repacking, repo)
			q.mu.Unlock()
			return
		}
		q.mu.Unlock()
	}
}

// DrainSyncRuns runs the scheduled sync queue's shutdown drain (issue #342,
// sync.LocalScheduler.Drain) while serve's listener still takes loopback
// pushes. Deliveries and repacks keep running until Close. It does nothing
// for the in-process queue.
func (q *Local) DrainSyncRuns(ctx context.Context, wait time.Duration) {
	if d, ok := q.syncq.(interface {
		Drain(context.Context, time.Duration)
	}); ok {
		d.Drain(ctx, wait)
	}
}

// Close stops accepting work and waits for in-flight deliveries and syncs.
func (q *Local) Close() {
	q.mu.Lock()
	alreadyClosed := q.closed
	q.closed = true
	q.mu.Unlock()
	if !alreadyClosed {
		close(q.repackDone) // unblock repack goroutines parked on the semaphore
	}
	// A scheduled sync queue (the SQLite sweep, ADR-50) would keep starting
	// runs while deliveries and repacks drain: stop it first.
	if st, ok := q.syncq.(interface{ Stop() }); ok {
		st.Stop()
	}
	q.wg.Wait()
	q.syncq.Close()
}
