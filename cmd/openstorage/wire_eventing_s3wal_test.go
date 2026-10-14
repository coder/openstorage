package main

import (
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/events"
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

// bootS3WALQueue builds the S3-WAL serve queue the way a booting process
// does and returns the worker's local schedule.
func bootS3WALQueue(t *testing.T, ms meta.Store) upsync.LocalLanes {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)
	deliverer := &events.Deliverer{Webhooks: events.MetaWebhookStore{Store: ms}, Logger: logger}
	worker := &upsync.Worker{}
	_, cleanup, err := buildLocalSyncQueue(ms, "org", t.TempDir(), worker, deliverer, nil, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)
	if worker.Local == nil {
		t.Fatal("S3-WAL worker has no local schedule")
	}
	return worker.Local
}

// A pull that crashes while it holds its claim leaves syncing=true with
// sync_pending consumed by the claim (issue #424). On the durable schedule
// the next process needs no boot re-mark: the sweep lists the expired
// claim as work, and the claim steals it. The lease still refuses every
// claim until it expires.
func TestS3WALSweepRestartsPullThatCrashedHoldingClaim(t *testing.T) {
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
	lanes := bootS3WALQueue(t, booted)

	// The unexpired lease refuses the recovered run: no claim runs twice.
	if work, err := lanes.ListLocalWork(ctx, "org", "", 10); err != nil || len(work) != 0 {
		t.Fatalf("work under unexpired lease = (%+v, %v), want none", work, err)
	}
	if c, err := lanes.ClaimDueSync(ctx, repoID, "holder-recovered", time.Minute); err != nil || c.Claimed || !c.Busy {
		t.Fatalf("claim under unexpired lease = (%+v, %v), want busy", c, err)
	}
	// After expiry the sweep sees the dead claim, and the claim steals it.
	nowNanos.Add(int64(time.Minute + time.Second))
	work, err := lanes.ListLocalWork(ctx, "org", "", 10)
	if err != nil || len(work) != 1 || work[0].RepoID != repoID || !work[0].Pull {
		t.Fatalf("work after expiry = (%+v, %v), want the crashed pull", work, err)
	}
	c, err := lanes.ClaimDueSync(ctx, repoID, "holder-recovered", time.Minute)
	if err != nil || !c.Claimed || c.StolenFrom != "holder-crashed" {
		t.Fatalf("claim after expiry = (%+v, %v), want the dead claim stolen", c, err)
	}
}

// wrappedS3WAL hides *meta.S3WAL behind another type, as a store wrapper would.
type wrappedS3WAL struct{ *meta.S3WAL }

// A store without a local schedule fails the boot: no serve runs without a
// sync schedule.
func TestBuildLocalSyncQueueRefusesStoreWithoutSchedule(t *testing.T) {
	st := meta.NewS3WAL(s3wal.NewMemCAS())
	t.Cleanup(st.Close)
	logger := slog.New(slog.DiscardHandler)
	deliverer := &events.Deliverer{Webhooks: events.MetaWebhookStore{Store: st}, Logger: logger}
	if _, _, err := buildLocalSyncQueue(wrappedS3WAL{st}, "org", t.TempDir(), &upsync.Worker{}, deliverer, nil, logger); err == nil {
		t.Fatal("boot succeeded without a local schedule")
	}
}
