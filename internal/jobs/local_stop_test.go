package jobs

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/sync"
)

// stoppableSyncQueue records Stop and Close.
type stoppableSyncQueue struct {
	stopped, closed atomic.Bool
}

func (q *stoppableSyncQueue) EnqueueSync(context.Context, sync.Job) error { return nil }
func (q *stoppableSyncQueue) EnqueuePush(context.Context, sync.Job) error { return nil }
func (q *stoppableSyncQueue) Stop()                                       { q.stopped.Store(true) }
func (q *stoppableSyncQueue) Close()                                      { q.closed.Store(true) }

// A scheduled sync queue (the SQLite sweep) stops starting runs as soon as
// Close begins, not after in-flight deliveries and repacks drain (review
// of #341).
func TestLocalCloseStopsScheduledSyncBeforeDraining(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	syncq := &stoppableSyncQueue{}
	q := newLocal(&events.Deliverer{Logger: logger}, syncq, nil, logger)
	q.wg.Add(1) // a delivery still retrying
	done := make(chan struct{})
	go func() { q.Close(); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for !syncq.stopped.Load() {
		if time.Now().After(deadline) {
			t.Fatal("scheduled sync queue still running while Close drains deliveries")
		}
		time.Sleep(time.Millisecond)
	}
	if syncq.closed.Load() {
		t.Fatal("sync queue closed before deliveries drained")
	}
	q.wg.Done()
	<-done
	if !syncq.closed.Load() {
		t.Fatal("sync queue not closed")
	}
}
