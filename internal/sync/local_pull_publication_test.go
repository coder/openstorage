package sync

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/openstorage/internal/meta"
)

// localPullWorker is a non-forwarding pull fixture over the durable metadata
// implementations: the upstream has one commit, the loopback cache has no
// published ref yet, and the hook-style SyncPending marker is set.
func localPullWorker(t *testing.T, backend string) (*workerFixture, meta.Backend, Job, string, string) {
	t.Helper()
	st := localIntentStore(t, backend)
	r, err := st.CreateRepo(t.Context(), "acme", "https://example.com/pull-publication.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	f := newWorkerFixture(t)
	f.worker.Credentials, f.worker.Upstreams = MetaStores(st)
	upHead := seedUpstream(t, f.upstreamDir, "own", "proj")
	loop := f.addLoopbackRepo(t, r.RepoID)
	if _, err := st.SetRepoUpstream(t.Context(), meta.RepoUpstream{
		RepoID: r.RepoID, Provider: "gitea", Owner: "own", Name: "proj",
		UpstreamHost: "file://" + f.upstreamDir,
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.RequestSync(t.Context(), r.RepoID); err != nil {
		t.Fatal(err)
	}
	return f, st, Job{Org: "acme", RepoID: r.RepoID}, upHead, loop
}

// A hook CAS marks SyncPending before receive-pack publishes the accepted ref
// in the cache. The claimed pull must wait for that publication, or its
// loopback push compares against the stale cache and upstream-wins is lost.
func TestLocalPullWaitsForCachePublication(t *testing.T) {
	for _, backend := range []string{"sqlite", "s3wal"} {
		t.Run(backend, func(t *testing.T) {
			f, st, job, upHead, loop := localPullWorker(t, backend)
			q := NewInProcessQueue(f.worker.Run, f.worker.RunPush, discardTestLogger())
			defer q.Close()
			defer f.worker.CloseLocal()
			f.worker.Requeue = q.EnqueueSync
			work := t.TempDir()
			testGit(t, work, "init", "-b", "main")
			testGit(t, work, "commit", "--allow-empty", "-m", "accepted local write")
			accepted := testGit(t, work, "rev-parse", "HEAD")
			waits := 0
			f.worker.WaitPushPublication = func(ctx context.Context, id string) error {
				waits++
				up, err := st.GetRepoUpstream(ctx, id)
				if err != nil || id != job.RepoID || !up.Syncing || up.SyncPending {
					t.Fatalf("publication wait must follow the actual pull claim: %+v %v", up, err)
				}
				if got := loopbackMain(t, loop); got != "" {
					t.Fatalf("pull wrote the cache before publication: %s", got)
				}
				// Receive-pack publishes the accepted ref while the pull waits.
				testGit(t, work, "push", "file://"+loop, "HEAD:refs/heads/main")
				return nil
			}
			if err := f.worker.Run(t.Context(), job); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if waits != 1 {
				t.Fatalf("pull ran %d publication waits, want 1 before its snapshot", waits)
			}
			if got := loopbackMain(t, loop); got != upHead {
				t.Fatalf("cache main=%s after pull, want upstream %s over published %s", got, upHead, accepted)
			}
			up, err := st.GetRepoUpstream(t.Context(), job.RepoID)
			if err != nil || up.RunCount != 1 || up.SuccessCount != 1 || up.Syncing || up.SyncPending {
				t.Fatalf("pull did not settle: %+v err=%v", up, err)
			}
			assertEventTypes(t, f.rec.list(), EventSyncStarted, EventSyncSucceeded)
		})
	}
}

// A failed wait took no snapshot: it must restore the consumed pull intent
// durably and own one delayed local retry that live polling cannot replace.
func TestLocalPullPublicationWaitFailureKeepsIntent(t *testing.T) {
	for _, backend := range []string{"sqlite", "s3wal"} {
		for _, failure := range []string{"read", "timeout"} {
			t.Run(backend+"/"+failure, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					f, st, job, upHead, loop := localPullWorker(t, backend)
					q := NewInProcessQueue(f.worker.Run, f.worker.RunPush, discardTestLogger())
					defer q.Close()
					defer f.worker.CloseLocal()
					var requeues atomic.Int32
					f.worker.Requeue = func(ctx context.Context, retry Job) error {
						requeues.Add(1)
						return q.EnqueueSync(ctx, retry)
					}
					var waits atomic.Int32
					f.worker.WaitPushPublication = func(ctx context.Context, id string) error {
						up, err := st.GetRepoUpstream(ctx, id)
						if err != nil || !up.Syncing || up.SyncPending {
							t.Errorf("publication wait must follow the actual pull claim: %+v %v", up, err)
						}
						if waits.Add(1) > 1 {
							return nil
						}
						if failure == "timeout" {
							<-ctx.Done()
							return ctx.Err()
						}
						return errors.New("temporary hold read failure")
					}
					err := f.worker.Run(t.Context(), job)
					if err == nil || failureCode(err) != FailInternal {
						t.Fatalf("wait failure must fail the run as internal: %v", err)
					}
					assertState := func(runs, successes int, pending bool) {
						t.Helper()
						up, err := st.GetRepoUpstream(t.Context(), job.RepoID)
						if err != nil || up.RunCount != runs || up.SuccessCount != successes || up.SyncPending != pending || up.Syncing {
							t.Fatalf("pull state %+v, want runs=%d successes=%d pending=%t err=%v", up, runs, successes, pending, err)
						}
					}
					assertState(1, 0, true) // durable intent restored
					if got := loopbackMain(t, loop); got != "" {
						t.Fatalf("failed wait still transferred: %s", got)
					}
					events := f.rec.list()
					assertEventTypes(t, events, EventSyncStarted, EventSyncFailed)
					if events[1].Error != FailInternal {
						t.Fatalf("failed event error=%q, want %q", events[1].Error, FailInternal)
					}
					// Live polling observes the restored marker but cannot
					// replace the reserved retry with an immediate run.
					if err := q.EnqueuePendingSync(t.Context(), Job{Org: job.Org, RepoID: job.RepoID}); err != nil {
						t.Fatal(err)
					}
					synctest.Wait()
					assertState(1, 0, true)
					delay := PushRetryDelay(1)
					time.Sleep(delay - time.Second)
					synctest.Wait()
					assertState(1, 0, true)
					time.Sleep(time.Second)
					synctest.Wait()
					assertState(2, 1, false)
					if got := loopbackMain(t, loop); got != upHead {
						t.Fatalf("retry cache main=%q, want upstream %s", got, upHead)
					}
					if requeues.Load() != 1 || waits.Load() != 2 {
						t.Fatalf("requeues=%d waits=%d, want 1/2", requeues.Load(), waits.Load())
					}
				})
			})
		}
	}
}
