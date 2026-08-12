package sync

import (
	"context"
	"errors"
	"log/slog"
	stdsync "sync"
)

// InProcessQueue runs sync jobs on goroutines with per-repo coalescing: a
// job enqueued while one for the same repo is still waiting is dropped (the
// waiting run will pick up the same upstream state). This is the local-mode
// jobs story — queued work is lost on process restart (foundation-slice
// ADR); the Postgres deployment substitutes the durable jobs seam.
type InProcessQueue struct {
	run     func(ctx context.Context, job Job) error
	logger  *slog.Logger
	mu      stdsync.Mutex
	pending map[string]bool // repoID -> queued (not yet running)
	closed  bool
	wg      stdsync.WaitGroup
}

var _ Queue = (*InProcessQueue)(nil)

// NewInProcessQueue builds a queue driving run (normally (*Worker).Run).
func NewInProcessQueue(run func(ctx context.Context, job Job) error, logger *slog.Logger) *InProcessQueue {
	assertf(run != nil, "NewInProcessQueue: nil run func")
	assertf(logger != nil, "NewInProcessQueue: nil logger")
	return &InProcessQueue{run: run, logger: logger, pending: make(map[string]bool)}
}

// EnqueueSync schedules one sync run. The run executes on its own goroutine
// with a fresh background context — it must outlive the enqueueing HTTP
// request. Overlap with an in-flight run is handled by BeginSync
// (ErrSyncInProgress -> silent skip), matching invariant 11.
func (q *InProcessQueue) EnqueueSync(_ context.Context, job Job) error {
	assertf(job.Org != "" && job.RepoID != "", "EnqueueSync: incomplete job %+v", job)
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return errors.New("sync queue is closed")
	}
	if q.pending[job.RepoID] {
		return nil // coalesced with the queued run
	}
	q.pending[job.RepoID] = true
	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		q.mu.Lock()
		delete(q.pending, job.RepoID)
		q.mu.Unlock()
		if err := q.run(context.Background(), job); err != nil {
			q.logger.Warn("sync run finished with error",
				"org", job.Org, "repo", job.RepoID, "err", err)
		}
	}()
	return nil
}

// Close stops accepting jobs and waits for in-flight runs to finish.
func (q *InProcessQueue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.wg.Wait()
}
