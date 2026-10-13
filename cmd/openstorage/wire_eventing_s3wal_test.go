package main

import (
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

// A pull that crashes while it holds its claim leaves syncing=true with
// sync_pending consumed by the claim (issue #424). The next S3-WAL boot
// must re-mark it: the boot scan then enqueues it, and the run's
// claim-skip arms the deferred retry that steals the dead lease once it
// expires. The lease itself still refuses every claim until it expires.
func TestS3WALBootRestartsPullThatCrashedHoldingClaim(t *testing.T) {
	ctx := t.Context()
	cas := s3wal.NewMemCAS()
	var nowNanos atomic.Int64
	nowNanos.Store(time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC).UnixNano())
	clock := meta.WithNowFunc(func() time.Time { return time.Unix(0, nowNanos.Load()) })

	// First process: accept a pull request, claim the run, then crash
	// before FinishSync. Close only stops background goroutines.
	crashed := meta.NewS3WAL(cas, clock)
	if _, err := crashed.UpsertOrgWithKeys(ctx, "org", []string{"pem"}); err != nil {
		t.Fatal(err)
	}
	repo, err := crashed.CreateLinkedRepo(ctx, "org", "https://example.com/acme/demo.git", "main",
		meta.RepoUpstream{Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := crashed.RequestSync(ctx, repo.RepoID); err != nil {
		t.Fatal(err)
	}
	if _, claimed, _, err := crashed.TryBeginSync(ctx, repo.RepoID, "holder-crashed", time.Minute); err != nil || !claimed {
		t.Fatalf("crashed run claim = (%v, %v), want claimed", claimed, err)
	}
	crashed.Close()

	// Second process boots on the same bucket and builds its queue.
	booted := meta.NewS3WAL(cas, clock)
	t.Cleanup(booted.Close)
	logger := slog.New(slog.DiscardHandler)
	deliverer := &events.Deliverer{Webhooks: events.MetaWebhookStore{Store: booted}, Logger: logger}
	queue, cleanup := buildLocalSyncQueue(booted, "org", t.TempDir(), &upsync.Worker{}, deliverer, nil, logger)
	defer cleanup()
	if _, ok := queue.(*jobs.Local); !ok {
		t.Fatalf("queue is %T, want the in-process *jobs.Local with boot recovery", queue)
	}
	ids, err := booted.PendingSyncRepoIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(ids, repo.RepoID) {
		t.Fatalf("boot scan lists %v, want the crashed pull %s", ids, repo.RepoID)
	}
	row, err := booted.GetRepoUpstream(ctx, repo.RepoID)
	if err != nil {
		t.Fatal(err)
	}
	if !row.SyncPending || !row.Syncing || row.SyncClaimHolder != "holder-crashed" {
		t.Fatalf("booted row = %+v, want the dead claim kept and sync_pending re-marked", row)
	}

	// The unexpired lease refuses the recovered run: no claim runs twice.
	if _, claimed, _, err := booted.TryBeginSync(ctx, repo.RepoID, "holder-recovered", time.Minute); err != nil || claimed {
		t.Fatalf("claim under unexpired lease = (%v, %v), want refused", claimed, err)
	}
	// The deferred retry fires after expiry: it steals the dead claim and
	// consumes the re-marked request.
	nowNanos.Add(int64(time.Minute + time.Second))
	up, claimed, stolenFrom, err := booted.TryBeginSync(ctx, repo.RepoID, "holder-recovered", time.Minute)
	if err != nil || !claimed || stolenFrom != "holder-crashed" || up.SyncPending {
		t.Fatalf("claim after expiry = (%+v, %v, %q, %v), want the dead claim stolen", up, claimed, stolenFrom, err)
	}
}
