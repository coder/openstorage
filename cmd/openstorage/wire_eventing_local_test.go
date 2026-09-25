package main

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/jobs"
	"github.com/coder/openstorage/internal/meta"
	upsync "github.com/coder/openstorage/internal/sync"
)

// SQLite serves get the durable local schedule (issue #266): the worker
// claims through the row, and the queue is the scheduler, not the
// in-process queue with boot recovery and retry timers.
func TestBuildLocalSyncQueueUsesDurableScheduleOnSQLite(t *testing.T) {
	ms, err := meta.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "meta.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ms.Close)
	if err := ms.InitSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.DiscardHandler)
	worker := &upsync.Worker{}
	deliverer := &events.Deliverer{Webhooks: events.MetaWebhookStore{Store: ms}, Logger: logger}
	queue, cleanup := buildLocalSyncQueue(ms, "org", worker, deliverer, nil, logger)
	defer cleanup()
	if worker.Local == nil {
		t.Fatal("SQLite worker has no local schedule")
	}
	if _, ok := queue.(*jobs.Local); !ok {
		t.Fatalf("queue is %T, want *jobs.Local", queue)
	}
	if worker.Requeue != nil || worker.RequeuePush != nil {
		t.Fatal("SQLite worker still wires process-local requeue")
	}
	// The API's explicit triggers reach the schedule through these
	// optional extensions: pull-upstream offers without re-marking, and
	// push-upstream records with RequestPushNow.
	if _, ok := queue.(interface {
		OfferSync(ctx context.Context, job upsync.Job) error
	}); !ok {
		t.Fatalf("queue %T cannot offer an already recorded pull", queue)
	}
	_, ups := upsync.MetaStores(ms)
	if _, ok := ups.(interface {
		RequestPushNow(ctx context.Context, repoID string, force bool) error
	}); !ok {
		t.Fatalf("upstream store %T has no RequestPushNow", ups)
	}
}
