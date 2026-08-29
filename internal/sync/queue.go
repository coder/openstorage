package sync

import (
	"context"
	"errors"
	"log/slog"
	stdsync "sync"
)

// InProcessQueue runs sync jobs on goroutines with per-repo coalescing: a
// job enqueued while one for the same repo is still waiting is dropped —
// sound, because the waiting run's upstream fetch is still in the future,
// so it observes the coalesced request's state. A job enqueued while a run
// is already CLAIMED skips at BeginSync instead; the claim holder's
// FinishSync then reports the pending request (RequestSync marker) and
// Worker.Requeue schedules the follow-up run (issue #79). This is the
// local-mode jobs story — queued work is lost on process restart
// (foundation-slice ADR); the Postgres deployment substitutes the durable
// jobs seam.
type InProcessQueue struct {
	run     func(ctx context.Context, job Job) error
	runPush func(ctx context.Context, job Job) error
	logger  *slog.Logger
	mu      stdsync.Mutex
	pending map[string]bool // lane-prefixed repoID -> queued (not yet running)
	closed  bool
	wg      stdsync.WaitGroup
}

var _ Queue = (*InProcessQueue)(nil)

// NewInProcessQueue builds a queue driving run (normally (*Worker).Run) and
// runPush ((*Worker).RunPush, issue #90). The two lanes coalesce
// independently: a queued pull never swallows a push request or vice versa.
func NewInProcessQueue(run, runPush func(ctx context.Context, job Job) error, logger *slog.Logger) *InProcessQueue {
	assertf(run != nil, "NewInProcessQueue: nil run func")
	assertf(runPush != nil, "NewInProcessQueue: nil runPush func")
	assertf(logger != nil, "NewInProcessQueue: nil logger")
	return &InProcessQueue{run: run, runPush: runPush, logger: logger, pending: make(map[string]bool)}
}

// EnqueueSync schedules one sync run. The run executes on its own goroutine
// with a fresh background context — it must outlive the enqueueing HTTP
// request. Overlap with an in-flight run is handled by BeginSync
// (ErrSyncInProgress -> silent skip), matching invariant 11; callers that
// must not lose the request mark it first (UpstreamStore.RequestSync), and
// the in-flight run's finisher requeues it (Worker.Requeue).
func (q *InProcessQueue) EnqueueSync(_ context.Context, job Job) error {
	return q.enqueue("pull:", q.run, "sync run finished with error", job)
}

// EnqueuePush schedules one outbound push-upstream run (issue #90) with the
// same coalescing/overlap semantics as EnqueueSync on its own lane; callers
// that must not lose the request mark it first (UpstreamStore.RequestPush).
func (q *InProcessQueue) EnqueuePush(_ context.Context, job Job) error {
	return q.enqueue("push:", q.runPush, "push run finished with error", job)
}

func (q *InProcessQueue) enqueue(lane string, run func(ctx context.Context, job Job) error, failMsg string, job Job) error {
	assertf(job.Org != "" && job.RepoID != "", "enqueue: incomplete job %+v", job)
	key := lane + job.RepoID
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return errors.New("sync queue is closed")
	}
	if q.pending[key] {
		return nil // coalesced with the queued run
	}
	q.pending[key] = true
	q.wg.Go(func() {
		q.mu.Lock()
		delete(q.pending, key)
		q.mu.Unlock()
		if err := run(context.Background(), job); err != nil {
			q.logger.Warn(failMsg, "org", job.Org, "repo", job.RepoID, "err", err)
		}
	})
	return nil
}

// Close stops accepting jobs and waits for in-flight runs to finish.
func (q *InProcessQueue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.wg.Wait()
}
