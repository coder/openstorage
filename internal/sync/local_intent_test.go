package sync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
						if err := q.EnqueuePush(ctx, Job{Org: "acme", RepoID: r.RepoID}); err != nil {
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
