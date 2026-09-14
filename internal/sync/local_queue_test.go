package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"testing/synctest"
)

func TestLocalPendingQueueBoundsBothLanes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan string, 65)
		release := make(chan struct{})
		run := func(_ context.Context, job Job) error {
			started <- job.RepoID
			<-release
			return nil
		}
		q := NewInProcessQueue(run, run, slog.Default())
		defer q.Close()
		defer close(release)
		accepted := 0
		for i := range 64 {
			enqueue := q.EnqueuePendingSync
			if i%2 == 0 {
				enqueue = q.EnqueuePendingPush
			}
			err := enqueue(t.Context(), Job{Org: "acme", RepoID: fmt.Sprint(i)})
			if err == nil {
				accepted++
			} else if !errors.Is(err, ErrPendingQueueFull) {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		if accepted != 4 || len(started) != 4 {
			t.Fatalf("accepted=%d started=%d, want 4 bounded observations", accepted, len(started))
		}
		// Saturation must not change existing explicit caller semantics.
		if err := q.EnqueuePush(t.Context(), Job{Org: "acme", RepoID: "explicit"}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if len(started) != 5 {
			t.Fatalf("ordinary caller blocked by observation cap: %d", len(started))
		}
	})
}

func TestLocalPendingQueueClosed(t *testing.T) {
	run := func(context.Context, Job) error { t.Error("closed queue ran observation"); return nil }
	q := NewInProcessQueue(run, run, slog.Default())
	q.Close()
	for _, enqueue := range []func(context.Context, Job) error{q.EnqueuePendingSync, q.EnqueuePendingPush} {
		if err := enqueue(t.Context(), Job{Org: "acme", RepoID: "r"}); err == nil {
			t.Fatal("closed queue accepted observation")
		}
	}
}
