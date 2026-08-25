// Queue conformance: the local queue is exercised end-to-end in-process;
// the River queue gets a real-Postgres roundtrip smoke test (testcontainers)
// covering delivery with retry, terminal webhook-gone drops, and
// unique-by-repo sync jobs.
package jobs

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/coder/code-storage/internal/events"
	"github.com/coder/code-storage/internal/meta"
	"github.com/coder/code-storage/internal/sync"
)

// stubWebhooks is a fixed-registration events.WebhookLookup.
type stubWebhooks map[string]events.Webhook

func (s stubWebhooks) GetWebhook(_ context.Context, id string) (events.Webhook, error) {
	wh, ok := s[id]
	if !ok {
		return events.Webhook{}, fmt.Errorf("webhook %s: %w", id, meta.ErrNotFound)
	}
	return wh, nil
}

type receiver struct {
	mu    stdsync.Mutex
	got   []string // signature headers, one per delivered POST
	fails int32    // remaining forced 500s
}

func (rc *receiver) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&rc.fails, -1) >= 0 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		rc.mu.Lock()
		rc.got = append(rc.got, r.Header.Get(events.HeaderSignature))
		rc.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}
}

func (rc *receiver) count() int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return len(rc.got)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// runQueue exercises one Queue implementation end-to-end against a live
// receiver: a failing-then-succeeding delivery (retry with fresh signature)
// and a sync job reaching the runner.
func runQueue(t *testing.T, q Queue, rc *receiver, ranSync *atomic.Int32) {
	t.Helper()
	ctx := context.Background()

	if err := q.EnqueueWebhookDelivery(ctx, events.Delivery{
		WebhookID: "wh_test", Event: events.EventPush, Payload: []byte(`{"k":"v"}`),
	}); err != nil {
		t.Fatalf("enqueue delivery: %v", err)
	}
	waitFor(t, "delivery after retry", func() bool { return rc.count() >= 1 })

	// A delivery for a deleted webhook is dropped terminally: the receiver
	// must never see it.
	if err := q.EnqueueWebhookDelivery(ctx, events.Delivery{
		WebhookID: "wh_gone", Event: events.EventPush, Payload: []byte(`{}`),
	}); err != nil {
		t.Fatalf("enqueue gone delivery: %v", err)
	}

	if err := q.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: "repo_1"}); err != nil {
		t.Fatalf("enqueue sync: %v", err)
	}
	waitFor(t, "sync run", func() bool { return ranSync.Load() >= 1 })

	if got := rc.count(); got != 1 {
		t.Fatalf("receiver saw %d deliveries, want 1 (webhook-gone must be dropped)", got)
	}
}

func TestLocalQueueRoundtrip(t *testing.T) {
	rc := &receiver{fails: 1} // first attempt 500s, retry succeeds re-signed
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	deliverer := &events.Deliverer{
		Webhooks:     stubWebhooks{"wh_test": {ID: "wh_test", URL: srv.URL, Secret: "s3cret"}},
		AllowPrivate: true, // httptest receiver is loopback (issue #53)
		Backoff:      func(int) time.Duration { return 10 * time.Millisecond },
		Logger:       slog.New(slog.DiscardHandler),
	}
	var ranSync atomic.Int32
	q := NewLocal(deliverer, func(context.Context, sync.Job) error {
		ranSync.Add(1)
		return nil
	}, func(context.Context, sync.Job) error { return nil }, nil, slog.New(slog.DiscardHandler))

	runQueue(t, q, rc, &ranSync)
	q.Close()

	if err := q.EnqueueSync(context.Background(), sync.Job{Org: "o", RepoID: "r"}); err == nil {
		t.Fatal("EnqueueSync after Close must fail")
	}
}

func TestRiverQueueRoundtrip(t *testing.T) {
	if testing.Short() {
		t.Skip("river roundtrip needs docker")
	}
	ctx := context.Background()
	pgC, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("postgres"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	defer func() { _ = pgC.Terminate(ctx) }()
	pgURL, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	rc := &receiver{fails: 1}
	srv := httptest.NewServer(rc.handler())
	defer srv.Close()

	deliverer := &events.Deliverer{
		Webhooks:     stubWebhooks{"wh_test": {ID: "wh_test", URL: srv.URL, Secret: "s3cret"}},
		AllowPrivate: true, // httptest receiver is loopback (issue #53)
		Logger:       slog.New(slog.DiscardHandler),
	}
	var ranSync atomic.Int32
	q, err := NewRiver(ctx, pgURL, deliverer, func(context.Context, sync.Job) error {
		ranSync.Add(1)
		return nil
	}, func(context.Context, sync.Job) error { return nil }, sync.NewMemStore(), nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	defer q.Close()

	// Insert-only client (the hook-subprocess path) shares the database:
	// jobs it inserts are worked by the full client's workers.
	ins, err := NewRiverInsertOnly(ctx, pgURL)
	if err != nil {
		t.Fatalf("NewRiverInsertOnly: %v", err)
	}
	defer ins.Close()

	runQueue(t, ins, rc, &ranSync)

	// Unique-by-repo: rapid duplicate enqueues for one repo must not pile
	// up one run per enqueue (a strict count is racy — the first run may
	// finish before the second insert — but 10 inserts must coalesce).
	before := ranSync.Load()
	for range 10 {
		if err := q.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: "repo_uniq"}); err != nil {
			t.Fatalf("enqueue sync: %v", err)
		}
	}
	waitFor(t, "unique sync run", func() bool { return ranSync.Load() > before })
	time.Sleep(500 * time.Millisecond) // allow any (unexpected) duplicates to run
	if got := ranSync.Load() - before; got > 2 {
		t.Fatalf("10 duplicate enqueues produced %d runs, want coalescing (<=2)", got)
	}
}
