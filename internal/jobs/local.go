package jobs

import (
	"context"
	"errors"
	"log/slog"
	stdsync "sync"

	"codestorage/internal/events"
	"codestorage/internal/sync"
)

// Local is the in-process queue for local/sqlite mode. Deliveries run on
// their own goroutines through events.Deliverer.Deliver (which owns the
// re-sign-per-attempt retry loop); sync runs go through
// sync.InProcessQueue (per-repo coalescing). Queued work is lost on
// process restart — the documented local-mode ADR trade-off.
type Local struct {
	deliverer *events.Deliverer
	syncq     *sync.InProcessQueue
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
	wg            stdsync.WaitGroup
}

var _ Queue = (*Local)(nil)

// NewLocal builds the in-process queue. deliverer must be configured with
// the webhook lookup; runSync is normally sync.(*Worker).Run. repacker may
// be nil (repack disabled or not wired, e.g. hook-subprocess queues that
// must never fold); EnqueueRepack is then refused.
func NewLocal(deliverer *events.Deliverer, runSync SyncRunner, repacker *Repacker, logger *slog.Logger) *Local {
	assertf(deliverer != nil, "NewLocal: nil deliverer")
	assertf(runSync != nil, "NewLocal: nil sync runner")
	assertf(logger != nil, "NewLocal: nil logger")
	return &Local{
		deliverer:     deliverer,
		syncq:         sync.NewInProcessQueue(runSync, logger),
		repacker:      repacker,
		logger:        logger,
		repacking:     make(map[string]bool),
		repackPending: make(map[string]bool),
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
	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		// Deliver logs retries and the final discard itself.
		_ = q.deliverer.Deliver(context.Background(), d)
	}()
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
		folded, err := q.repacker.Repack(context.Background(), repo)
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

// Close stops accepting work and waits for in-flight deliveries and syncs.
func (q *Local) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.wg.Wait()
	q.syncq.Close()
}
