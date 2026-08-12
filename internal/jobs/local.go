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
	logger    *slog.Logger

	mu     stdsync.Mutex
	closed bool
	wg     stdsync.WaitGroup
}

var _ Queue = (*Local)(nil)

// NewLocal builds the in-process queue. deliverer must be configured with
// the webhook lookup; runSync is normally sync.(*Worker).Run.
func NewLocal(deliverer *events.Deliverer, runSync SyncRunner, logger *slog.Logger) *Local {
	assertf(deliverer != nil, "NewLocal: nil deliverer")
	assertf(runSync != nil, "NewLocal: nil sync runner")
	assertf(logger != nil, "NewLocal: nil logger")
	return &Local{
		deliverer: deliverer,
		syncq:     sync.NewInProcessQueue(runSync, logger),
		logger:    logger,
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

// Close stops accepting work and waits for in-flight deliveries and syncs.
func (q *Local) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.wg.Wait()
	q.syncq.Close()
}
