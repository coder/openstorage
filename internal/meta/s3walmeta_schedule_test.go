// S3WAL-only local schedule tests (ADR-51). The shared transition table
// and model check run on S3WAL through runSchedBackends
// (sqlite_schedule_test.go); these cover what only S3WAL has: the sync
// fence write-through, ambiguous-PUT recovery, and the create-state
// completion.

package meta

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/ref"
	"github.com/coder/openstorage/internal/ref/s3wal"
)

// Serve gets the durable schedule through LocalSchedule on SQLite and
// S3WAL. The hook subprocess still asserts LocalScheduleStore to decide
// whether it records intent, so *S3WAL itself must not satisfy it until
// S3WAL hooks record intent (issue #335).
func TestLocalScheduleBackends(t *testing.T) {
	if _, ok := any(&S3WAL{}).(LocalScheduleStore); ok {
		t.Fatal("*S3WAL satisfies LocalScheduleStore: S3WAL hooks would record intent")
	}
	s3 := &S3WAL{}
	if ls, ok := LocalSchedule(s3); !ok || ls != (s3walLocalSchedule{s3}) {
		t.Fatalf("LocalSchedule(*S3WAL) = (%v, %v), want the S3WAL adapter", ls, ok)
	}
	lite := &SQLite{}
	if ls, ok := LocalSchedule(lite); !ok || ls != LocalScheduleStore(lite) {
		t.Fatalf("LocalSchedule(*SQLite) = (%v, %v), want the store itself", ls, ok)
	}
	if _, ok := LocalSchedule(&PG{}); ok {
		t.Fatal("LocalSchedule(*PG) is ok: PostgreSQL schedules through River")
	}
}

// ClaimDueSync installs the manifest fence token like TryBeginSync, and
// FinishSyncOutcome revokes it before it ends the run.
func TestS3WALClaimDueSyncWritesThrough(t *testing.T) {
	ctx := context.Background()
	cas, metaStore, _ := newWriteThroughStores(t)
	ls := s3walLocalSchedule{metaStore}
	repoID := seedS3WALUpstream(t, metaStore)
	if c, err := ls.ClaimDueSync(ctx, repoID, "h1", time.Minute); err != nil || c.Claimed || c.Busy {
		t.Fatalf("claim without a request = %+v, %v; want refused, not busy", c, err)
	}
	if got := manifestSyncClaim(t, cas, repoID); got != "" {
		t.Fatalf("refused claim left manifest token %q", got)
	}
	if err := metaStore.RequestSync(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	if c, err := ls.ClaimDueSync(ctx, repoID, "h1", time.Minute); err != nil || !c.Claimed {
		t.Fatalf("claim = %+v, %v", c, err)
	}
	if got := manifestSyncClaim(t, cas, repoID); got != "h1" {
		t.Fatalf("manifest token = %q, want h1", got)
	}
	if c, err := ls.ClaimDueSync(ctx, repoID, "h2", time.Minute); err != nil || c.Claimed || !c.Busy {
		t.Fatalf("second claim = %+v, %v; want busy", c, err)
	}
	if _, err := ls.FinishSyncOutcome(ctx, repoID, "h1", SyncOutcome{Kind: OutcomeRestore, Delay: time.Second}); err != nil {
		t.Fatal(err)
	}
	if got := manifestSyncClaim(t, cas, repoID); got != "" {
		t.Fatalf("manifest token after restore = %q, want cleared", got)
	}
}

// A restore counts no run, so its ambiguous PUT is recognized by the
// released lane with its request re-marked, also when a competing write
// superseded our landed one.
func TestS3WALFinishOutcomeAmbiguousRestore(t *testing.T) {
	for _, f := range []s3wal.PutFault{s3wal.FaultAmbiguousApplied, s3wal.FaultAmbiguousDropped} {
		cas, a, b := newS3WALMetaPair(t)
		ls := s3walLocalSchedule{a}
		ctx := context.Background()
		repoID := seedS3WALUpstream(t, a)
		if err := a.RequestPush(ctx, repoID, true); err != nil {
			t.Fatal(err)
		}
		if err := a.RequestSync(ctx, repoID); err != nil {
			t.Fatal(err)
		}
		if c, err := ls.ClaimDuePush(ctx, repoID, "h", time.Minute); err != nil || !c.Claimed {
			t.Fatalf("push claim = %+v, %v", c, err)
		}
		if c, err := ls.ClaimDueSync(ctx, repoID, "h", time.Minute); err != nil || !c.Claimed {
			t.Fatalf("sync claim = %+v, %v", c, err)
		}
		// The competitor's write changes nothing the outcome reads.
		bump := func() {
			if _, err := b.UpsertOrgWithKeys(ctx, "other", []string{"pem"}); err != nil {
				t.Errorf("competing write: %v", err)
			}
		}
		armAmbiguousPut(t, cas, f, bump)
		if _, err := ls.FinishPushOutcome(ctx, repoID, "h", PushOutcome{Kind: OutcomeRestore, Force: true, Delay: time.Minute}); err != nil {
			t.Fatalf("fault %d: push restore = %v", f, err)
		}
		cas.Hooks = s3wal.MemCASHooks{}
		armAmbiguousPut(t, cas, f, bump)
		if _, err := ls.FinishSyncOutcome(ctx, repoID, "h", SyncOutcome{Kind: OutcomeRestore, Delay: time.Minute}); err != nil {
			t.Fatalf("fault %d: sync restore = %v", f, err)
		}
		cas.Hooks = s3wal.MemCASHooks{}
		up, err := a.GetRepoUpstream(ctx, repoID)
		if err != nil || up.PushSyncing || up.PushPending != 2 || up.Syncing || !up.SyncPending || up.PushRunCount != 0 || up.RunCount != 0 {
			t.Fatalf("fault %d: lane after restores = %+v, %v", f, up, err)
		}
	}
}

func TestS3WALCompleteSyncRepoCreate(t *testing.T) {
	ctx := context.Background()
	st := NewS3WAL(s3wal.NewMemCAS())
	t.Cleanup(st.Close)
	ls := s3walLocalSchedule{st}
	org := seedOrg(t, st)
	pending := func(name string) Repo {
		t.Helper()
		r, err := st.CreateRepoWithState(ctx, org, name, "main", CreateStatePending)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	linked := pending("linked")
	if _, err := st.SetRepoUpstream(ctx, RepoUpstream{RepoID: linked.RepoID, Provider: "github", Owner: "o", Name: "n", UpstreamHost: "github.com"}); err != nil {
		t.Fatal(err)
	}
	if out, err := ls.CompleteSyncRepoCreate(ctx, org, linked.RepoID); err != nil || out != CreateCompleted {
		t.Fatalf("complete = %v, %v", out, err)
	}
	if up, err := st.GetRepoUpstream(ctx, linked.RepoID); err != nil || !up.SyncPending {
		t.Fatalf("first sync not requested: %+v, %v", up, err)
	}
	if out, err := ls.CompleteSyncRepoCreate(ctx, org, linked.RepoID); err != nil || out != CreateAlreadyComplete {
		t.Fatalf("second complete = %v, %v", out, err)
	}
	// No upstream: nothing changes, and the create stays pending.
	bare := pending("bare")
	if _, err := ls.CompleteSyncRepoCreate(ctx, org, bare.RepoID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("complete without upstream = %v, want ErrNotFound", err)
	}
	if r, err := st.GetRepoByID(ctx, org, bare.RepoID); err != nil || r.CreateState != CreateStatePending {
		t.Fatalf("after failed complete: %+v, %v; want pending", r, err)
	}
}

// A detaching row is not work, and a due claim on it answers ErrNotFound
// without touching the ref manifest, as the up-front check in beginSync.
func TestS3WALClaimDueSyncDetaching(t *testing.T) {
	ctx := context.Background()
	cas, metaStore, _ := newWriteThroughStores(t)
	ls := s3walLocalSchedule{metaStore}
	repoID := seedS3WALUpstream(t, metaStore)
	if err := metaStore.RequestSync(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	if work, err := ls.ListLocalWork(ctx, "org", "", 10); err != nil || len(work) != 1 || !work[0].Pull {
		t.Fatalf("ListLocalWork before detaching = %+v, %v; want the pull", work, err)
	}
	if err := metaStore.mutate(ctx, s3walMetaMutation{op: "test: mark detaching", fn: func(doc *s3walMetaDoc) (bool, error) {
		doc.Upstreams[repoID].Detaching = true
		return true, nil
	}}); err != nil {
		t.Fatal(err)
	}
	if work, err := ls.ListLocalWork(ctx, "org", "", 10); err != nil || len(work) != 0 {
		t.Fatalf("ListLocalWork = %+v, %v; want no work", work, err)
	}
	if c, err := ls.ClaimDueSync(ctx, repoID, "h1", time.Minute); !errors.Is(err, ErrNotFound) || c.Claimed {
		t.Fatalf("ClaimDueSync on a detaching row = %+v, %v; want ErrNotFound", c, err)
	}
	if got := manifestSyncClaim(t, cas, repoID); got != "" {
		t.Fatalf("manifest token = %q, want none", got)
	}
}

// A dropped ambiguous finish whose claim a thief stole and finished with a
// different result is not reported as ours: the counters and the attempt
// must match our outcome, not only the run count.
func TestS3WALFinishOutcomeAmbiguousThief(t *testing.T) {
	for _, tc := range []struct {
		name        string
		ours, thief PushOutcome
	}{
		{"done ok vs thief done failed", PushOutcome{Kind: OutcomeDone, Success: true}, PushOutcome{Kind: OutcomeDone}},
		{"done vs thief retry", PushOutcome{Kind: OutcomeDone, Success: true}, PushOutcome{Kind: OutcomeRetry, Delay: time.Minute}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Unix(1700000000, 0).UTC()
			clock := WithNowFunc(func() time.Time { return now })
			cas := s3wal.NewMemCAS()
			a, b := NewS3WAL(cas, clock), NewS3WAL(cas, clock)
			t.Cleanup(func() { a.Close(); b.Close() })
			ours, thief := s3walLocalSchedule{a}, s3walLocalSchedule{b}
			repoID := seedS3WALUpstream(t, a)
			if err := a.RequestPush(ctx, repoID, false); err != nil {
				t.Fatal(err)
			}
			if c, err := ours.ClaimDuePush(ctx, repoID, "h", time.Minute); err != nil || !c.Claimed {
				t.Fatalf("claim = %+v, %v", c, err)
			}
			armAmbiguousPut(t, cas, s3wal.FaultAmbiguousDropped, func() {
				now = now.Add(2 * time.Minute) // our lease expires
				if c, err := thief.ClaimDuePush(ctx, repoID, "thief", time.Minute); err != nil || !c.Claimed {
					t.Errorf("thief claim = %+v, %v", c, err)
				}
				if _, err := thief.FinishPushOutcome(ctx, repoID, "thief", tc.thief); err != nil {
					t.Errorf("thief finish: %v", err)
				}
			})
			if _, err := ours.FinishPushOutcome(ctx, repoID, "h", tc.ours); !errors.Is(err, ErrNotFound) {
				t.Fatalf("our finish = %v, want ErrNotFound (the thief finished the run)", err)
			}
		})
	}
}

// detachOnFenceSink marks the row detaching right after the fence write,
// as a DeleteRepoUpstream that lands while a claim installs its token.
type detachOnFenceSink struct {
	SyncClaimSink
	meta *S3WAL
}

func (d detachOnFenceSink) SetSyncClaim(ctx context.Context, repoID, holder string) error {
	if err := d.SyncClaimSink.SetSyncClaim(ctx, repoID, holder); err != nil {
		return err
	}
	return d.meta.mutate(ctx, s3walMetaMutation{op: "test: mark detaching", fn: func(doc *s3walMetaDoc) (bool, error) {
		doc.Upstreams[repoID].Detaching = true
		return true, nil
	}})
}

// A row that starts detaching during the fence install: a due claim gets
// ErrNotFound and its token is cleared; TryBeginSync keeps its refusal.
func TestS3WALClaimDueSyncDetachingDuringFence(t *testing.T) {
	for _, due := range []bool{true, false} {
		ctx := context.Background()
		cas := s3wal.NewMemCAS()
		refStore := ref.NewS3WAL(cas, nil)
		plain := NewS3WAL(cas)
		metaStore := NewS3WAL(cas, WithSyncClaimSink(detachOnFenceSink{SyncClaimSink: refStore, meta: plain}))
		t.Cleanup(func() { metaStore.Close(); plain.Close(); refStore.Close() })
		repoID := seedS3WALUpstream(t, metaStore)
		if err := metaStore.RequestSync(ctx, repoID); err != nil {
			t.Fatal(err)
		}
		var claimed bool
		var err error
		if due {
			var c LocalClaim
			c, err = s3walLocalSchedule{metaStore}.ClaimDueSync(ctx, repoID, "h1", time.Minute)
			claimed = c.Claimed
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("ClaimDueSync = %+v, %v; want ErrNotFound", c, err)
			}
		} else {
			_, claimed, _, err = metaStore.TryBeginSync(ctx, repoID, "h1", time.Minute)
			if err != nil {
				t.Fatalf("TryBeginSync = %v, want a plain refusal", err)
			}
		}
		if claimed {
			t.Fatalf("due=%v: claim handed out on a detaching row", due)
		}
		if got := manifestSyncClaim(t, cas, repoID); got != "" {
			t.Fatalf("due=%v: manifest token = %q, want cleared", due, got)
		}
	}
}
