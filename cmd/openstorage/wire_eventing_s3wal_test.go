package main

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/jobs"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref/s3wal"
	upsync "github.com/coder/openstorage/internal/sync"
)

// s3walClaimedRepo returns an S3-WAL store whose clock the test moves,
// holding one linked repo with an accepted request claimed by holder.
func s3walClaimedRepo(t *testing.T, cas *s3wal.MemCAS, nowNanos *atomic.Int64, holder string) (*meta.S3WAL, string) {
	t.Helper()
	ctx := t.Context()
	nowNanos.Store(time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC).UnixNano())
	st := meta.NewS3WAL(cas, meta.WithNowFunc(func() time.Time { return time.Unix(0, nowNanos.Load()) }))
	if _, err := st.UpsertOrgWithKeys(ctx, "org", []string{"pem"}); err != nil {
		t.Fatal(err)
	}
	repo, err := st.CreateLinkedRepo(ctx, "org", "https://example.com/acme/demo.git", "main",
		meta.RepoUpstream{Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.RequestSync(ctx, repo.RepoID); err != nil {
		t.Fatal(err)
	}
	if _, claimed, _, err := st.TryBeginSync(ctx, repo.RepoID, holder, time.Minute); err != nil || !claimed {
		t.Fatalf("claim = (%v, %v), want claimed", claimed, err)
	}
	return st, repo.RepoID
}

// bootS3WALQueue builds the S3-WAL serve queue the way a booting process does.
func bootS3WALQueue(t *testing.T, ms meta.Store) error {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)
	deliverer := &events.Deliverer{Webhooks: events.MetaWebhookStore{Store: ms}, Logger: logger}
	queue, cleanup, err := buildLocalSyncQueue(ms, "org", t.TempDir(), &upsync.Worker{}, deliverer, nil, logger)
	if err != nil {
		return err
	}
	t.Cleanup(cleanup)
	if _, ok := queue.(*jobs.Local); !ok {
		t.Fatalf("queue is %T, want the in-process *jobs.Local with boot recovery", queue)
	}
	return nil
}

// A pull that crashes while it holds its claim leaves syncing=true with
// sync_pending consumed by the claim (issue #424). The next S3-WAL boot
// must re-mark it: the boot scan then enqueues it, and the run's
// claim-skip arms the deferred retry that steals the dead lease once it
// expires. The lease itself still refuses every claim until it expires.
func TestS3WALBootRestartsPullThatCrashedHoldingClaim(t *testing.T) {
	ctx := t.Context()
	cas := s3wal.NewMemCAS()
	var nowNanos atomic.Int64
	// First process: claim the run, then crash before FinishSync. Close
	// only stops background goroutines.
	crashed, repoID := s3walClaimedRepo(t, cas, &nowNanos, "holder-crashed")
	crashed.Close()

	// Second process boots on the same bucket and builds its queue.
	booted := meta.NewS3WAL(cas, meta.WithNowFunc(func() time.Time { return time.Unix(0, nowNanos.Load()) }))
	t.Cleanup(booted.Close)
	if err := bootS3WALQueue(t, booted); err != nil {
		t.Fatal(err)
	}
	ids, err := booted.PendingSyncRepoIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(ids, repoID) {
		t.Fatalf("boot scan lists %v, want the crashed pull %s", ids, repoID)
	}
	row, err := booted.GetRepoUpstream(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if !row.SyncPending || !row.Syncing || row.SyncClaimHolder != "holder-crashed" {
		t.Fatalf("booted row = %+v, want the dead claim kept and sync_pending re-marked", row)
	}

	// The unexpired lease refuses the recovered run: no claim runs twice.
	if _, claimed, _, err := booted.TryBeginSync(ctx, repoID, "holder-recovered", time.Minute); err != nil || claimed {
		t.Fatalf("claim under unexpired lease = (%v, %v), want refused", claimed, err)
	}
	// The deferred retry fires after expiry: it steals the dead claim and
	// consumes the re-marked request.
	nowNanos.Add(int64(time.Minute + time.Second))
	up, claimed, stolenFrom, err := booted.TryBeginSync(ctx, repoID, "holder-recovered", time.Minute)
	if err != nil || !claimed || stolenFrom != "holder-crashed" || up.SyncPending {
		t.Fatalf("claim after expiry = (%+v, %v, %q, %v), want the dead claim stolen", up, claimed, stolenFrom, err)
	}
}

// A boot that meets a LIVE claim (a second serve with the instance lease
// disabled) re-marks it too. That must cost at most one follow-up run
// after the live run finishes, never a concurrent run.
func TestS3WALBootRemarkNeverDoubleRunsLiveClaim(t *testing.T) {
	ctx := t.Context()
	var nowNanos atomic.Int64
	live, repoID := s3walClaimedRepo(t, s3wal.NewMemCAS(), &nowNanos, "holder-live")
	t.Cleanup(live.Close)
	if err := bootS3WALQueue(t, live); err != nil {
		t.Fatal(err)
	}
	if _, claimed, _, err := live.TryBeginSync(ctx, repoID, "holder-second", time.Minute); err != nil || claimed {
		t.Fatalf("claim beside the live run = (%v, %v), want refused", claimed, err)
	}
	fin, err := live.FinishSync(ctx, repoID, "holder-live", true)
	if err != nil || !fin.SyncPending {
		t.Fatalf("live finish = (%+v, %v), want success with one follow-up owed", fin, err)
	}
	up, claimed, stolenFrom, err := live.TryBeginSync(ctx, repoID, "holder-followup", time.Minute)
	if err != nil || !claimed || stolenFrom != "" || up.SyncPending {
		t.Fatalf("follow-up claim = (%+v, %v, %q, %v), want a plain claim", up, claimed, stolenFrom, err)
	}
}

// failingRemark fails every RequestSync, as an unreachable meta store would.
type failingRemark struct{ *meta.S3WAL }

func (failingRemark) RequestSync(context.Context, string) error { return errors.New("meta store down") }

// A failed re-mark fails the boot instead of stranding the crashed pull.
func TestS3WALBootFailsWhenOrphanRemarkFails(t *testing.T) {
	var nowNanos atomic.Int64
	st, _ := s3walClaimedRepo(t, s3wal.NewMemCAS(), &nowNanos, "holder-crashed")
	t.Cleanup(st.Close)
	if err := bootS3WALQueue(t, failingRemark{st}); err == nil {
		t.Fatal("boot succeeded with an orphaned claim left unmarked")
	}
}
