package sync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref/s3wal"
)

// Exercise the worker against the durable metadata implementations, not the
// MemStore double. File Git endpoints keep the transfer ordering observable.
func localIntentStore(t *testing.T, backend string) meta.Backend {
	t.Helper()
	var st meta.Backend
	if backend == "sqlite" {
		s, err := meta.OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "meta.db"))
		if err != nil {
			t.Fatal(err)
		}
		st = s
	} else {
		st = meta.NewS3WAL(s3wal.NewMemCAS())
	}
	t.Cleanup(st.Close)
	if err := st.InitSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
	key, err := auth.GenerateKey("ES256")
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.MarshalPublicKeyPEM(key.Public())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertOrgWithKeys(t.Context(), "acme", []string{string(pub)}); err != nil {
		t.Fatal(err)
	}
	return st
}

// afterSnapshotCredentials inserts a request at the credential lookup, which
// occurs after the outbound worker has fetched its local snapshot. All actual
// credential operations still go through the real metadata adapter.
type afterSnapshotCredentials struct {
	CredentialStore
	afterSnapshot func()
}

func (s *afterSnapshotCredentials) CredentialForRepo(ctx context.Context, id string) (Credential, error) {
	if s.afterSnapshot != nil {
		f := s.afterSnapshot
		s.afterSnapshot = nil
		f()
	}
	return s.CredentialStore.CredentialForRepo(ctx, id)
}

// afterRetryMark pauses the worker after its retry intent reaches the actual
// metadata store, but before schedulePushRetry can register its local timer.
type afterRetryMark struct {
	UpstreamStore
	afterMark func()
}

func (s *afterRetryMark) RequestPush(ctx context.Context, id string, force bool) error {
	if err := s.UpstreamStore.RequestPush(ctx, id, force); err != nil {
		return err
	}
	if s.afterMark != nil {
		f := s.afterMark
		s.afterMark = nil
		f()
	}
	return nil
}

// This feasibility test models one pending-only LIVE scan, never boot rescue.
// A durable pending bit can be a delayed worker retry rather than a new hook
// request. Polling must not reset that retry's cadence or retry budget.
func TestLocalIntentPollDoesNotStealRetry(t *testing.T) {
	for _, backend := range []string{"sqlite", "s3wal"} {
		for _, poll := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/poll=%t", backend, poll), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx := t.Context()
					st := localIntentStore(t, backend)
					r, err := st.CreateRepo(ctx, "acme", "https://example.com/retry.git", "main")
					if err != nil {
						t.Fatal(err)
					}
					f := newWorkerFixture(t)
					creds, ups := MetaStores(st)
					f.worker.Credentials, f.worker.Upstreams = creds, ups
					seedLocalRepo(t, f, r.RepoID)
					if _, err := st.SetRepoUpstream(ctx, meta.RepoUpstream{
						RepoID: r.RepoID, Provider: "gitea", Owner: "own", Name: "proj",
						UpstreamHost: "http://127.0.0.1:1", ForwardOnPush: true,
					}); err != nil {
						t.Fatal(err)
					}
					if err := st.RequestPush(ctx, r.RepoID, true); err != nil {
						t.Fatal(err)
					}
					retried := make(chan Job, 2)
					f.worker.RequeuePush = func(_ context.Context, job Job) error {
						retried <- job
						return nil
					}
					f.worker.Upstreams = &afterRetryMark{UpstreamStore: ups, afterMark: func() {
						if !poll {
							return
						}
						up, err := st.GetRepoUpstream(ctx, r.RepoID)
						if err != nil || up.PushPending != 2 || up.PushSyncing {
							t.Fatalf("retry must be marked, forced and unclaimed: %+v err=%v", up, err)
						}
						// Even an advisory check of the existing worker timer map
						// cannot identify retry intent in this registration window.
						f.worker.deferMu.Lock()
						deferred := len(f.worker.deferredRetry) != 0
						f.worker.deferMu.Unlock()
						if deferred {
							t.Fatal("fixture missed the retry registration window")
						}
						q := NewInProcessQueue(f.worker.Run, f.worker.RunPush, discardTestLogger())
						if err := q.EnqueuePendingPush(ctx, Job{Org: "acme", RepoID: r.RepoID}); err != nil {
							t.Fatal(err)
						}
						q.Close() // force the admitted poll job to finish in this window
					}}
					start := time.Now()
					if err := f.worker.RunPush(ctx, Job{Org: "acme", RepoID: r.RepoID}); !TransientPushFailure(err) {
						t.Fatalf("want real transient transfer failure, got %v", err)
					}
					up, err := st.GetRepoUpstream(ctx, r.RepoID)
					if err != nil || up.PushPending != 2 || up.PushSyncing {
						t.Fatalf("retry lost force or failed to settle: %+v err=%v", up, err)
					}
					if elapsed := time.Since(start); elapsed >= PushRetryDelay(1) {
						t.Fatalf("fixture exceeded first retry delay: %v", elapsed)
					}
					if up.PushRunCount != 1 {
						t.Errorf("without a new request, live poll started %d runs before first retry delay; want 1", up.PushRunCount)
					}
					select {
					case job := <-retried:
						t.Fatalf("scheduled retry fired early: %+v", job)
					default:
					}
					// Drain the real timer under fake time; no background retry is
					// left holding test metadata after the fixture is closed.
					time.Sleep(PushRetryDelay(1) + time.Second)
					synctest.Wait()
					select {
					case job := <-retried:
						if job.Retry != 1 || len(retried) != 0 {
							t.Fatalf("delayed successor=%+v remaining=%d, want one retry=1", job, len(retried))
						}
					default:
						t.Fatal("missing delayed successor")
					}
				})
			})
		}
	}
}

func localIntentWorker(t *testing.T, backend, host string) (*workerFixture, meta.Backend, Job) {
	t.Helper()
	st := localIntentStore(t, backend)
	r, err := st.CreateRepo(t.Context(), "acme", "https://example.com/admission.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	f := newWorkerFixture(t)
	f.worker.Credentials, f.worker.Upstreams = MetaStores(st)
	seedLocalRepo(t, f, r.RepoID)
	if host == "" {
		addEmptyUpstream(t, f.upstreamDir, "own", "proj")
		host = "file://" + f.upstreamDir
	}
	if _, err := st.SetRepoUpstream(t.Context(), meta.RepoUpstream{
		RepoID: r.RepoID, Provider: "gitea", Owner: "own", Name: "proj",
		UpstreamHost: host, ForwardOnPush: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.RequestPush(t.Context(), r.RepoID, true); err != nil {
		t.Fatal(err)
	}
	return f, st, Job{Org: "acme", RepoID: r.RepoID}
}

func TestLocalPendingPushStaleQueuedObservation(t *testing.T) {
	for _, backend := range []string{"sqlite", "s3wal"} {
		t.Run(backend, func(t *testing.T) {
			f, st, job := localIntentWorker(t, backend, "")
			arrived, release := make(chan struct{}), make(chan struct{})
			q := NewInProcessQueue(f.worker.Run, func(ctx context.Context, job Job) error {
				if job.pendingOnly {
					close(arrived)
					<-release
				}
				return f.worker.RunPush(ctx, job)
			}, discardTestLogger())
			defer q.Close()
			releaseWorker := stdsync.OnceFunc(func() { close(release) })
			defer releaseWorker()
			f.worker.RequeuePush = q.EnqueuePush
			if err := q.EnqueuePendingPush(t.Context(), job); err != nil {
				t.Fatal(err)
			}
			<-arrived
			// An explicit caller consumes the observed marker before the
			// queued observation reaches the worker. Its stale job is no work.
			if err := f.worker.RunPush(t.Context(), job); err != nil {
				t.Fatal(err)
			}
			releaseWorker()
			q.Close()
			up, err := st.GetRepoUpstream(t.Context(), job.RepoID)
			if err != nil || up.PushRunCount != 1 || up.PushPending != 0 {
				t.Fatalf("stale observation created a run: %+v err=%v", up, err)
			}
		})
	}
}

func TestLocalPendingPushRetryHandoff(t *testing.T) {
	for _, backend := range []string{"sqlite", "s3wal"} {
		t.Run(backend, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, st, job := localIntentWorker(t, backend, "http://127.0.0.1:1")
				q := NewInProcessQueue(f.worker.Run, f.worker.RunPush, discardTestLogger())
				defer q.Close()
				atEnqueue, release := make(chan struct{}), make(chan struct{})
				defer close(release)
				f.worker.RequeuePush = func(ctx context.Context, retry Job) error {
					if retry.Retry == 1 {
						atEnqueue <- struct{}{}
						<-release
					}
					return q.EnqueuePush(ctx, retry)
				}
				if err := f.worker.RunPush(t.Context(), job); !TransientPushFailure(err) {
					t.Fatalf("want transient failure: %v", err)
				}
				time.Sleep(PushRetryDelay(1))
				<-atEnqueue
				if err := q.EnqueuePendingPush(t.Context(), job); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				up, err := st.GetRepoUpstream(t.Context(), job.RepoID)
				if err != nil || up.PushRunCount != 1 || up.PushPending != 2 {
					t.Errorf("poll stole timer-to-queue handoff: runs=%d pending=%d err=%v", up.PushRunCount, up.PushPending, err)
				}
				// End the fixture's durable request before releasing the delayed
				// enqueue, so any later timer has no repository left to retry.
				if err := st.DeleteRepoUpstream(t.Context(), job.RepoID); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestLocalPendingPushQueuedRetry(t *testing.T) {
	for _, backend := range []string{"sqlite", "s3wal"} {
		t.Run(backend, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, st, job := localIntentWorker(t, backend, "http://127.0.0.1:1")
				arrived, release := make(chan struct{}), make(chan struct{})
				q := NewInProcessQueue(f.worker.Run, func(ctx context.Context, retry Job) error {
					if retry.Retry == 1 {
						close(arrived)
						<-release
					}
					return f.worker.RunPush(ctx, retry)
				}, discardTestLogger())
				defer q.Close()
				defer f.worker.CloseLocal()
				releaseWorker := stdsync.OnceFunc(func() { close(release) })
				defer releaseWorker()
				f.worker.RequeuePush = q.EnqueuePush
				if err := f.worker.RunPush(t.Context(), job); !TransientPushFailure(err) {
					t.Fatalf("want transient failure: %v", err)
				}
				time.Sleep(PushRetryDelay(1))
				<-arrived
				if err := q.EnqueuePendingPush(t.Context(), job); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				up, err := st.GetRepoUpstream(t.Context(), job.RepoID)
				if err != nil || up.PushRunCount != 1 {
					t.Fatalf("poll overtook queued retry: runs=%d err=%v", up.PushRunCount, err)
				}
				releaseWorker()
				synctest.Wait()
				up, err = st.GetRepoUpstream(t.Context(), job.RepoID)
				if err != nil || up.PushRunCount != 2 || up.PushPending != 2 {
					t.Fatalf("queued retry failed to retain force: %+v err=%v", up, err)
				}
			})
		})
	}
}

// Inject an upstream endpoint outage without replacing durable claim behavior.
type unavailablePushEndpoint struct {
	UpstreamStore
	offline bool
}

func (s *unavailablePushEndpoint) BeginPush(ctx context.Context, id, holder string, lease time.Duration) (Upstream, bool, string, error) {
	up, force, stolen, err := s.UpstreamStore.BeginPush(ctx, id, holder, lease)
	if s.offline {
		up.UpstreamHost = "http://127.0.0.1:1"
	}
	return up, force, stolen, err
}

// An explicit run may cover delayed work. Its old timer (or queued retry)
// must neither delay the next hook marker nor consume that marker later.
func TestLocalPendingPushCoveredRetry(t *testing.T) {
	for _, backend := range []string{"sqlite", "s3wal"} {
		for _, phase := range []string{"backoff", "deferred", "queued"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx := t.Context()
					f, st, job := localIntentWorker(t, backend, "")
					arrived, release := make(chan struct{}), make(chan struct{})
					q := NewInProcessQueue(f.worker.Run, func(ctx context.Context, retry Job) error {
						if phase == "queued" && retry.Retry == 1 {
							close(arrived)
							<-release
						}
						return f.worker.RunPush(ctx, retry)
					}, discardTestLogger())
					defer q.Close()
					defer f.worker.CloseLocal()
					releaseWorker := stdsync.OnceFunc(func() { close(release) })
					defer releaseWorker()
					f.worker.RequeuePush = q.EnqueuePush
					if phase == "deferred" {
						if _, _, _, err := f.worker.Upstreams.BeginPush(ctx, job.RepoID, "other", 2*time.Minute); err != nil {
							t.Fatal(err)
						}
						if err := st.RequestPush(ctx, job.RepoID, false); err != nil {
							t.Fatal(err)
						}
						if err := f.worker.RunPush(ctx, job); err != nil {
							t.Fatal(err)
						}
						if _, err := f.worker.Upstreams.FinishPush(ctx, job.RepoID, "other", true); err != nil {
							t.Fatal(err)
						}
					} else {
						endpoint := &unavailablePushEndpoint{UpstreamStore: f.worker.Upstreams, offline: true}
						f.worker.Upstreams = endpoint
						if err := f.worker.RunPush(ctx, job); !TransientPushFailure(err) {
							t.Fatalf("want real transient failure: %v", err)
						}
						endpoint.offline = false
						if phase == "queued" {
							time.Sleep(PushRetryDelay(1))
							<-arrived
						}
					}
					if err := f.worker.RunPush(ctx, job); err != nil {
						t.Fatalf("explicit run must cover the pending work: %v", err)
					}
					if err := st.RequestPush(ctx, job.RepoID, false); err != nil {
						t.Fatal(err)
					}
					if err := q.EnqueuePendingPush(ctx, job); err != nil {
						t.Fatal(err)
					}
					synctest.Wait()
					up, err := st.GetRepoUpstream(ctx, job.RepoID)
					if err != nil || up.PushRunCount != 3 || up.PushPending != 0 {
						t.Errorf("obsolete retry blocked a fresh hook: runs=%d pending=%d err=%v", up.PushRunCount, up.PushPending, err)
					}
					releaseWorker()
					time.Sleep(3 * time.Minute)
					synctest.Wait()
					up, err = st.GetRepoUpstream(ctx, job.RepoID)
					if err != nil || up.PushRunCount != 3 || up.PushPending != 0 {
						t.Fatalf("obsolete retry ran after its intent was covered: %+v err=%v", up, err)
					}
				})
			})
		}
	}
}

type localAdmissionFailure struct {
	UpstreamStore
	read, claim atomic.Bool
}

func (s *localAdmissionFailure) GetUpstream(ctx context.Context, id string) (Upstream, error) {
	if s.read.CompareAndSwap(true, false) {
		return Upstream{}, fmt.Errorf("temporary metadata read failure")
	}
	return s.UpstreamStore.GetUpstream(ctx, id)
}

func (s *localAdmissionFailure) BeginPush(ctx context.Context, id, holder string, lease time.Duration) (Upstream, bool, string, error) {
	if s.claim.CompareAndSwap(true, false) {
		return Upstream{}, false, "", fmt.Errorf("temporary metadata claim failure")
	}
	return s.UpstreamStore.BeginPush(ctx, id, holder, lease)
}

func TestLocalPendingPushPreclaimFailureKeepsBackoff(t *testing.T) {
	for _, backend := range []string{"sqlite", "s3wal"} {
		for _, phase := range []string{"lookup", "claim", "timer_read", "enqueue"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx := t.Context()
					f, st, job := localIntentWorker(t, backend, "http://127.0.0.1:1")
					fault := &localAdmissionFailure{UpstreamStore: f.worker.Upstreams}
					f.worker.Upstreams = fault
					var lookupFail, enqueueFail atomic.Bool
					lookup := f.worker.Repos
					f.worker.Repos = func(ctx context.Context, org, id string) (RepoInfo, error) {
						if lookupFail.CompareAndSwap(true, false) {
							return RepoInfo{}, fmt.Errorf("temporary repo lookup failure")
						}
						return lookup(ctx, org, id)
					}
					q := NewInProcessQueue(f.worker.Run, f.worker.RunPush, discardTestLogger())
					defer q.Close()
					defer f.worker.CloseLocal()
					f.worker.RequeuePush = func(ctx context.Context, retry Job) error {
						if enqueueFail.CompareAndSwap(true, false) {
							return fmt.Errorf("temporary enqueue failure")
						}
						return q.EnqueuePush(ctx, retry)
					}
					job.Retry = 3 // Exercise a late backoff position and the remaining budget.
					if err := f.worker.RunPush(ctx, job); !TransientPushFailure(err) {
						t.Fatalf("want real transient transfer failure: %v", err)
					}
					switch phase {
					case "lookup":
						lookupFail.Store(true)
					case "claim":
						fault.claim.Store(true)
					case "timer_read":
						fault.read.Store(true)
					case "enqueue":
						enqueueFail.Store(true)
					}
					time.Sleep(PushRetryDelay(4))
					synctest.Wait()
					if lookupFail.Load() || fault.claim.Load() || fault.read.Load() || enqueueFail.Load() {
						t.Fatal("fixture did not reach the selected failure")
					}
					assertRuns := func(want, pending int) {
						t.Helper()
						up, err := st.GetRepoUpstream(ctx, job.RepoID)
						if err != nil || up.PushRunCount != want || up.PushPending != pending {
							t.Fatalf("runs=%d pending=%d, want %d/%d: %v", up.PushRunCount, up.PushPending, want, pending, err)
						}
					}
					// No new caller intent: polling cannot take over the failed
					// pre-claim handoff as Retry=0 or run at a shorter cadence.
					if err := q.EnqueuePendingPush(ctx, Job{Org: job.Org, RepoID: job.RepoID}); err != nil {
						t.Fatal(err)
					}
					synctest.Wait()
					assertRuns(1, 2)
					time.Sleep(PushRetryDelay(4) - time.Second)
					synctest.Wait()
					assertRuns(1, 2)
					time.Sleep(time.Second)
					synctest.Wait()
					assertRuns(2, 2)
					time.Sleep(PushRetryDelay(5))
					synctest.Wait()
					assertRuns(3, 0) // budget exhausted, not reset by the failed handoff
				})
			})
		}
	}
}

func TestLocalPendingPushBackoffAndBudget(t *testing.T) {
	for _, backend := range []string{"sqlite", "s3wal"} {
		for _, fresh := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fresh=%t", backend, fresh), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					f, st, job := localIntentWorker(t, backend, "http://127.0.0.1:1")
					q := NewInProcessQueue(f.worker.Run, f.worker.RunPush, discardTestLogger())
					defer q.Close()
					defer f.worker.CloseLocal()
					f.worker.RequeuePush = q.EnqueuePush
					if err := f.worker.RunPush(t.Context(), job); !TransientPushFailure(err) {
						t.Fatalf("want transient failure: %v", err)
					}
					wantRuns := 1
					if fresh {
						// Preserve explicit caller behavior: a fresh enqueue may run
						// immediately during backoff. Its plain mark cannot downgrade
						// the already durable forced retry.
						if err := st.RequestPush(t.Context(), job.RepoID, false); err != nil {
							t.Fatal(err)
						}
						if err := q.EnqueuePush(t.Context(), job); err != nil {
							t.Fatal(err)
						}
						synctest.Wait()
						wantRuns++
					}
					for retry := 1; retry <= MaxPushTransientRetries; retry++ {
						if err := q.EnqueuePendingPush(t.Context(), job); err != nil {
							t.Fatal(err)
						}
						synctest.Wait()
						up, err := st.GetRepoUpstream(t.Context(), job.RepoID)
						if err != nil || up.PushRunCount != wantRuns || up.PushPending != 2 {
							t.Fatalf("poll changed backoff/force before retry%d: runs=%d want=%d pending=%d err=%v", retry, up.PushRunCount, wantRuns, up.PushPending, err)
						}
						time.Sleep(PushRetryDelay(retry) - time.Second)
						synctest.Wait()
						up, err = st.GetRepoUpstream(t.Context(), job.RepoID)
						if err != nil || up.PushRunCount != wantRuns {
							t.Fatalf("retry%d fired early: runs=%d want=%d err=%v", retry, up.PushRunCount, wantRuns, err)
						}
						time.Sleep(time.Second)
						synctest.Wait()
						wantRuns++
					}
					if err := q.EnqueuePendingPush(t.Context(), job); err != nil {
						t.Fatal(err)
					}
					synctest.Wait()
					up, err := st.GetRepoUpstream(t.Context(), job.RepoID)
					if err != nil || up.PushPending != 0 || up.PushRunCount != wantRuns {
						t.Fatalf("poll reset exhausted retry budget: %+v want runs=%d err=%v", up, wantRuns, err)
					}
					for _, event := range f.rec.list() {
						if !event.Forced {
							t.Fatalf("retry downgraded force: %+v", event)
						}
					}
				})
			})
		}
	}
}

type pauseRetryRead struct {
	UpstreamStore
	enabled atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (s *pauseRetryRead) GetUpstream(ctx context.Context, id string) (Upstream, error) {
	up, err := s.UpstreamStore.GetUpstream(ctx, id)
	if s.enabled.CompareAndSwap(true, false) {
		close(s.entered)
		<-s.release
	}
	return up, err
}

func TestLocalPendingPushShutdown(t *testing.T) {
	for _, backend := range []string{"sqlite", "s3wal"} {
		t.Run(backend, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, st, job := localIntentWorker(t, backend, "http://127.0.0.1:1")
				q := NewInProcessQueue(f.worker.Run, f.worker.RunPush, discardTestLogger())
				defer q.Close()
				f.worker.RequeuePush = q.EnqueuePush
				paused := &pauseRetryRead{UpstreamStore: f.worker.Upstreams, entered: make(chan struct{}), release: make(chan struct{})}
				f.worker.Upstreams = paused
				if err := f.worker.RunPush(t.Context(), job); !TransientPushFailure(err) {
					t.Fatalf("want transient failure: %v", err)
				}
				paused.enabled.Store(true)
				time.Sleep(PushRetryDelay(1))
				<-paused.entered
				closed := make(chan struct{})
				go func() { f.worker.CloseLocal(); close(closed) }()
				synctest.Wait()
				select {
				case <-closed:
					t.Fatal("CloseLocal failed to join an already firing timer")
				default:
				}
				close(paused.release)
				<-closed
				if err := q.EnqueuePendingPush(t.Context(), job); err != nil {
					t.Fatal(err)
				}
				if err := q.EnqueuePush(t.Context(), job); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				time.Sleep(PushRetryDelay(MaxPushTransientRetries))
				synctest.Wait()
				up, err := st.GetRepoUpstream(t.Context(), job.RepoID)
				if err != nil || up.PushRunCount != 1 || up.PushPending != 2 || up.PushSyncing {
					t.Fatalf("shutdown resurrected work or lost durable intent: %+v err=%v", up, err)
				}
			})
		})
	}
}

func TestLocalIntentWorkerInterleaving(t *testing.T) {
	for _, backend := range []string{"sqlite", "s3wal"} {
		for _, request := range []string{"none", "plain", "force_then_plain"} {
			t.Run(backend+"/"+request, func(t *testing.T) {
				ctx := t.Context()
				st := localIntentStore(t, backend)
				r, err := st.CreateRepo(ctx, "acme", "https://example.com/intent.git", "main")
				if err != nil {
					t.Fatal(err)
				}
				f := newWorkerFixture(t)
				creds, ups := MetaStores(st)
				f.worker.Credentials, f.worker.Upstreams = creds, ups
				first := seedLocalRepo(t, f, r.RepoID)
				bare := addEmptyUpstream(t, f.upstreamDir, "own", "proj")
				if _, err := st.SetRepoUpstream(ctx, meta.RepoUpstream{
					RepoID: r.RepoID, Provider: "gitea", Owner: "own", Name: "proj",
					UpstreamHost: "file://" + f.upstreamDir, ForwardOnPush: true,
				}); err != nil {
					t.Fatal(err)
				}
				if err := st.RequestPush(ctx, r.RepoID, false); err != nil {
					t.Fatal(err)
				}
				var followups []Job
				f.worker.RequeuePush = func(_ context.Context, job Job) error {
					followups = append(followups, job)
					return nil
				}
				want := first
				f.worker.Credentials = &afterSnapshotCredentials{CredentialStore: creds, afterSnapshot: func() {
					up, err := st.GetRepoUpstream(ctx, r.RepoID)
					if err != nil || !up.PushSyncing || up.PushPending != 0 {
						t.Fatalf("snapshot must hold a claim with consumed pending: %+v err=%v", up, err)
					}
					// An overlapping worker with no fresh pending must skip without
					// manufacturing a marker or a follow-up.
					if err := f.worker.RunPush(ctx, Job{Org: "acme", RepoID: r.RepoID}); err != nil {
						t.Fatal(err)
					}
					up, err = st.GetRepoUpstream(ctx, r.RepoID)
					if err != nil || up.PushPending != 0 || len(followups) != 0 {
						t.Fatalf("active claim manufactured intent: pending=%d followups=%v err=%v", up.PushPending, followups, err)
					}
					if request == "none" {
						return
					}
					work := t.TempDir()
					local := filepath.Join(f.loopbackDir, r.RepoID+".git")
					testGit(t, work, "clone", "file://"+local, ".")
					if err := os.WriteFile(filepath.Join(work, "new.txt"), []byte("accepted during forward\n"), 0o600); err != nil {
						t.Fatal(err)
					}
					testGit(t, work, "add", ".")
					testGit(t, work, "commit", "-m", "accepted during forward")
					testGit(t, work, "push", "origin", "main")
					want = testGit(t, work, "rev-parse", "HEAD")
					if err := st.RequestPush(ctx, r.RepoID, request == "force_then_plain"); err != nil {
						t.Fatal(err)
					}
					if err := st.RequestPush(ctx, r.RepoID, false); err != nil {
						t.Fatal(err)
					}
				}}
				// The production local queue permits the mirror-back goroutine to
				// finish before the push finisher enqueues its follow-up. Execute
				// that order deterministically through the real pull worker.
				f.worker.AfterForward = f.worker.Run
				if err := f.worker.RunPush(ctx, Job{Org: "acme", RepoID: r.RepoID}); err != nil {
					t.Fatal(err)
				}
				wantFollowups := 0
				if request != "none" {
					wantFollowups = 1
				}
				if len(followups) != wantFollowups {
					t.Fatalf("followups=%v, want %d", followups, wantFollowups)
				}
				up, err := st.GetRepoUpstream(ctx, r.RepoID)
				if err != nil {
					t.Fatal(err)
				}
				wantPending := wantFollowups
				if request == "force_then_plain" {
					wantPending = 2
				}
				if up.PushPending != wantPending {
					t.Fatalf("pending=%d, want %d", up.PushPending, wantPending)
				}
				if len(followups) == 1 {
					if err := f.worker.RunPush(ctx, followups[0]); err != nil {
						t.Fatal(err)
					}
				}
				up, err = st.GetRepoUpstream(ctx, r.RepoID)
				if err != nil || up.PushPending != 0 || up.PushSyncing || up.PushRunCount != 1+wantFollowups {
					t.Fatalf("settled worker state=%+v err=%v", up, err)
				}
				if got := testGit(t, bare, "rev-parse", "refs/heads/main"); got != want {
					t.Errorf("upstream lost accepted mid-run commit: got %s want %s (initial %s)", got, want, first)
				}
				if got := testGit(t, f.loopbackDir, "--git-dir="+filepath.Join(f.loopbackDir, r.RepoID+".git"), "rev-parse", "refs/heads/main"); got != want {
					t.Errorf("mirror-back overwrote accepted mid-run commit: got %s want %s", got, want)
				}
			})
		}
	}
}
