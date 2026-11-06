package jobs

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/sync"
)

// blockingRestore parks the quarantine restore copy of a pack key until
// release closes, ignoring its context, like a restore in the sweep's
// detached recovery step.
type blockingRestore struct {
	blob.BlobStore
	entered chan struct{}
	release chan struct{}
	lists   atomic.Int32 // listings started: one per sweep pass at least
}

func (b *blockingRestore) ListPages(ctx context.Context, prefix string, pageSize int, fn func([]blob.ListedObject) error) error {
	b.lists.Add(1)
	return b.BlobStore.ListPages(ctx, prefix, pageSize, fn)
}

func (b *blockingRestore) Copy(ctx context.Context, dst, src string) error {
	if !strings.HasPrefix(dst, quarantinePrefix) {
		select {
		case b.entered <- struct{}{}:
		default:
		}
		<-b.release
	}
	return b.BlobStore.Copy(ctx, dst, src)
}

// sweepInRecovery builds a sweeper that deletes one orphan, finds it
// registered by the post-delete recheck, and parks in the restore copy.
func sweepInRecovery(t *testing.T) (s *Sweeper, f *sweepFixture, key string, entered, release chan struct{}) {
	t.Helper()
	f = newSweepFixture(t)
	key = blob.PackKey("r1", "recovering")
	f.upload(key, "recovering-bytes")
	f.backdate(key, 25*time.Hour)
	entered, release = make(chan struct{}, 1), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	// Calls 1 and 2 see the pack unregistered; call 3, the recheck, registered.
	s = NewSweeper(&blockingRestore{BlobStore: f.blobs, entered: entered, release: release}, &registeredAfterCall{after: 2},
		SweepConfig{Threshold: 24 * time.Hour, Delete: true}, slog.New(slog.DiscardHandler))
	return s, f, key, entered, release
}

// A sweep still in its detached recovery step must not hold its job open:
// Work returns shortly after its context ends, and the recovery goes on.
func TestSweepWorkerReturnsWhileTheSweepIsStillRecovering(t *testing.T) {
	s, f, key, entered, release := sweepInRecovery(t)
	w := &sweepWorker{schedule: &SweepSchedule{Sweeper: s, Interval: time.Hour}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := make(chan error, 1)
	go func() { returned <- w.Work(ctx, nil) }()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the sweep never reached its restore copy")
	}
	cancel()
	select {
	case err := <-returned:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Work = %v, want an error that wraps the job's cancellation", err)
		}
	case <-time.After(sweepReturnGrace + 10*time.Second):
		t.Fatal("Work still waits for the recovery step after its job ended")
	}
	close(release)
	waitFor(t, "the recovery to restore the pack after Work returned", func() bool { return f.exists(key) })
}

// panicLister panics when the sweep lists, like a blob driver with a defect.
type panicLister struct{ blob.BlobStore }

func (panicLister) ListPages(context.Context, string, int, func([]blob.ListedObject) error) error {
	panic("simulated blob driver defect")
}

// The pass runs in its own goroutine, outside River's recovery boundary. A
// panic in it comes back to Work's goroutine, where River records it as a
// failed attempt, instead of ending the process. The gate is released first,
// so the next tick is not turned away for the life of the process.
func TestSweepWorkerRaisesAPanicOfItsPassInItsOwnGoroutine(t *testing.T) {
	f := newSweepFixture(t)
	s := NewSweeper(panicLister{f.blobs}, f.store, SweepConfig{Threshold: 24 * time.Hour}, slog.New(slog.DiscardHandler))
	w := &sweepWorker{schedule: &SweepSchedule{Sweeper: s, Interval: time.Hour}}

	got := func() (v any) {
		defer func() { v = recover() }()
		_ = w.Work(context.Background(), nil)
		return nil
	}()
	if got != "simulated blob driver defect" {
		t.Fatalf("Work raised %v, want the panic of the pass", got)
	}
	if w.running.Load() {
		t.Fatal("the gate is still held after the pass panicked")
	}
}

// A pass that Work gave up on keeps running, and a tick that arrives meanwhile
// does nothing: abandoned passes do not pile up in one process. The next tick
// after the pass ended runs.
func TestSweepWorkerDoesNothingWhileAnAbandonedPassStillRuns(t *testing.T) {
	s, f, key, entered, release := sweepInRecovery(t)
	lists := &s.blobs.(*blockingRestore).lists
	w := &sweepWorker{schedule: &SweepSchedule{Sweeper: s, Interval: time.Hour}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() { first <- w.Work(ctx, nil) }()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the sweep never reached its restore copy")
	}
	cancel()
	select {
	case err := <-first:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("first Work = %v, want an error that wraps the job's cancellation", err)
		}
	case <-time.After(sweepReturnGrace + 10*time.Second):
		t.Fatal("Work still waits for the recovery step after its job ended")
	}

	started := lists.Load()
	tick := make(chan error, 1)
	go func() { tick <- w.Work(context.Background(), nil) }()
	select {
	case err := <-tick:
		if err != nil {
			t.Fatalf("a tick while the abandoned pass runs = %v, want it to do nothing", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a tick while the abandoned pass runs started a pass of its own")
	}
	if got := lists.Load(); got != started {
		t.Fatalf("a tick while the abandoned pass runs listed %d times, want no new pass", got-started)
	}

	close(release)
	waitFor(t, "the abandoned pass to end and restore the pack", func() bool { return f.exists(key) && !w.running.Load() })
	if err := w.Work(context.Background(), nil); err != nil {
		t.Fatalf("a tick after the pass ended: %v", err)
	}
	if lists.Load() == started {
		t.Fatal("a tick after the abandoned pass ended did not run a pass")
	}
}

// River.Close must not leave the sweep's job row running when it gives up on a
// sweep in recovery: the next process's start-up sweep deduplicates against a
// running row until River rescues it.
func TestRiverCloseDoesNotLeaveTheSweepRowRunning(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)
	s, _, _, entered, _ := sweepInRecovery(t)
	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	noop := func(context.Context, sync.Job) error { return nil }
	q, err := NewRiver(ctx, pgURL, deliverer, noop, noop, sync.NewMemStore(),
		&SweepSchedule{Sweeper: s, Interval: time.Hour}, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(60 * time.Second):
		t.Fatal("the periodic sweep never reached its restore copy")
	}

	start := time.Now()
	q.Close()
	if took := time.Since(start); took > closeTimeout+2*time.Second {
		t.Errorf("Close took %v, want it bounded by %v", took, closeTimeout)
	}

	conn, err := pgx.Connect(ctx, pgURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	var state string
	if err := conn.QueryRow(ctx, `SELECT state FROM river_job WHERE kind = 'orphan_pack_sweep' ORDER BY id DESC LIMIT 1`).Scan(&state); err != nil {
		t.Fatalf("read the sweep job: %v", err)
	}
	if state == "running" {
		t.Errorf("sweep job is still running after Close gave up on its recovery")
	}
	t.Logf("sweep job state after Close: %s", state)
}
