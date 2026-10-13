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

	"github.com/coder/openstorage/internal/ref/s3wal"
)

// Serve chooses the durable schedule by asserting LocalScheduleStore, and
// the S3WAL worker is wired separately: until then *S3WAL must not
// satisfy the interface.
func TestS3WALIsNotYetALocalScheduleStore(t *testing.T) {
	if _, ok := any(&S3WAL{}).(LocalScheduleStore); ok {
		t.Fatal("*S3WAL satisfies LocalScheduleStore: serve would switch S3WAL to the durable schedule")
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
