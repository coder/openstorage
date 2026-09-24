package sync

import (
	"context"
	"errors"
	"fmt"
	stdsync "sync"
	"testing"
	"time"
)

// Hold exactly one metadata operation open. No Git transfer or arbitrary
// sleep is needed to expose whether it owns the worker-wide timer mutex.
type blockedAdmissionStore struct {
	UpstreamStore
	read func()
}

func (s blockedAdmissionStore) GetUpstream(context.Context, string) (Upstream, error) {
	s.read()
	return Upstream{SyncPending: true, PushPending: PushPendingPush}, nil
}

func TestLocalAdmissionDoesNotBlockOtherRepositories(t *testing.T) {
	for _, push := range []bool{false, true} {
		for _, operation := range []string{"read", "claim"} {
			t.Run(fmt.Sprintf("push=%t/%s", push, operation), func(t *testing.T) {
				entered, release := make(chan struct{}), make(chan struct{})
				unblock := stdsync.OnceFunc(func() { close(release) })
				defer unblock()
				hold := func() { close(entered); <-release }
				w := &Worker{
					Requeue:     func(context.Context, Job) error { return nil },
					RequeuePush: func(context.Context, Job) error { return nil },
				}
				defer w.CloseLocal()
				w.Upstreams = blockedAdmissionStore{read: func() {
					if operation == "read" {
						hold()
					}
				}}
				job := Job{Org: "acme", RepoID: "slow", pendingOnly: true}
				finish, ok := w.startLocal(job, push)
				if !ok {
					t.Fatal("slow run was not admitted")
				}
				slowDone := make(chan error, 1)
				go func() {
					err := w.admitLocal(t.Context(), job, push, func() error {
						if operation == "claim" {
							hold()
						}
						return nil
					})
					finish()
					slowDone <- err
				}()
				<-entered
				otherDone := make(chan error, 1)
				go func() {
					other := Job{Org: "acme", RepoID: "fast"}
					finish, ok := w.startLocal(other, push)
					if !ok {
						otherDone <- errors.New("unrelated run was not admitted")
						return
					}
					defer finish()
					if err := w.admitLocal(t.Context(), other, push, func() error { return nil }); err != nil {
						otherDone <- err
						return
					}
					w.scheduleLocalRetry("fast", time.Hour, other, false)
					w.CloseLocal()
					otherDone <- nil
				}()
				select {
				case err := <-otherDone:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(2 * time.Second):
					t.Error("one repository's metadata I/O blocked unrelated admission/timer/shutdown")
					unblock()
					if err := <-otherDone; err != nil {
						t.Error(err)
					}
				}
				unblock()
				if err := <-slowDone; err != nil && !errors.Is(err, errNoPending) {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestLocalAdmissionPreservesNewRetryReservation(t *testing.T) {
	for _, push := range []bool{false, true} {
		t.Run(fmt.Sprintf("push=%t", push), func(t *testing.T) {
			w := &Worker{
				Requeue:     func(context.Context, Job) error { return nil },
				RequeuePush: func(context.Context, Job) error { return nil },
			}
			defer w.CloseLocal()
			job := Job{Org: "acme", RepoID: "repo_retry"}
			key := job.RepoID
			if push {
				key = "push-retry:" + job.RepoID
			}
			finish, ok := w.startLocal(job, push)
			if !ok {
				t.Fatal("run was not admitted")
			}
			defer finish()
			if err := w.admitLocal(t.Context(), job, push, func() error {
				// A preceding run can finish and register its successor while
				// the store claim is returning. This claim must not erase it.
				w.scheduleLocalRetry(key, time.Hour, job, push)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if w.deferredRetry[key] == nil {
				t.Fatal("claim erased a later retry reservation")
			}
			if err := w.admitLocal(t.Context(), job, push, func() error { return nil }); err != nil {
				t.Fatal(err)
			}
			if w.deferredRetry[key] != nil {
				t.Fatal("later claim did not cover the existing reservation")
			}
		})
	}
}
