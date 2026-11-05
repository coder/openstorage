package jobs

// Issue #79 liveness on the River lane: syncArgs uniqueness spans the
// running state (river requires it), so a pull-upstream enqueue while a
// sync job runs is silently deduplicated against it — even though that
// run's upstream fetch may already have happened. The producer marks the
// request durable first (RequestSync); the sync worker's gated
// transactional completion must then schedule the successor run.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/sync"
	"github.com/coder/openstorage/internal/testutil/testimages"
)

// TestSyncSnoozeDelayLegacyBackoff: a lapsed LEGACY claim may be
// deliberately non-stealable (opt-in unset) — the durable job must pace
// its re-runs at legacy cadence instead of spinning every few seconds.
func TestSyncSnoozeDelayLegacyBackoff(t *testing.T) {
	if d := syncSnoozeDelay(sync.Upstream{}); d != syncFollowUpSnooze {
		t.Errorf("no-claim delay = %v, want %v", d, syncFollowUpSnooze)
	}
	lapsed := sync.Upstream{Syncing: true, SyncClaimHolder: "h-dead", SyncClaimExpiresAt: time.Now().Add(-time.Hour)}
	if d := syncSnoozeDelay(lapsed); d != syncFollowUpSnooze {
		t.Errorf("lapsed lease-aware delay = %v, want %v", d, syncFollowUpSnooze)
	}
	lapsedLegacy := sync.Upstream{Syncing: true, SyncClaimHolder: sync.LegacyClaimHolder, SyncClaimExpiresAt: time.Now().Add(-time.Hour)}
	if d := syncSnoozeDelay(lapsedLegacy); d != legacyClaimSnooze {
		t.Errorf("lapsed legacy delay = %v, want %v", d, legacyClaimSnooze)
	}
	frozen := sync.Upstream{Syncing: true, SyncClaimHolder: "h-live", SyncClaimExpiresAt: time.Now().Add(2 * time.Hour)}
	if d := syncSnoozeDelay(frozen); d != 30*time.Minute {
		t.Errorf("far-future delay = %v, want the 30m cap", d)
	}
}

// livenessPG starts one postgres container for a river liveness test.
func livenessPG(t *testing.T, ctx context.Context) string {
	t.Helper()
	if testing.Short() {
		t.Skip("river liveness tests need docker")
	}
	pgC, err := tcpostgres.Run(ctx, testimages.Postgres,
		tcpostgres.WithDatabase("postgres"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = pgC.Terminate(context.Background()) })
	pgURL, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return pgURL
}

func TestRiverSyncRequestDuringRunningJobSchedulesSuccessor(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)

	store := sync.NewMemStore()
	if _, err := store.PutUpstream(ctx, sync.Upstream{
		RepoID: "repo_live", Provider: "gitea", Owner: "o", Name: "n", UpstreamHost: "example.test",
	}); err != nil {
		t.Fatalf("PutUpstream: %v", err)
	}

	// The runner mimics sync.Worker.Run's claim lifecycle (invariant 11)
	// without git transfers: claim or skip, hold the FIRST claimed run
	// until released, finish.
	entered := make(chan struct{})
	release := make(chan struct{})
	var once stdsync.Once
	var runSeq, claimedRuns atomic.Int32
	runner := func(ctx context.Context, job sync.Job) error {
		holder := fmt.Sprintf("test-%d", runSeq.Add(1))
		if _, _, err := store.BeginSync(ctx, job.RepoID, holder, time.Minute); err != nil {
			if errors.Is(err, sync.ErrSyncInProgress) {
				return nil // overlapping run: skip, like the real worker
			}
			return err
		}
		claimedRuns.Add(1)
		once.Do(func() {
			close(entered)
			<-release
		})
		_, err := store.FinishSync(ctx, job.RepoID, holder, true)
		return err
	}

	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	q, err := NewRiver(ctx, pgURL, deliverer, runner, func(context.Context, sync.Job) error { return nil }, store, nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	defer q.Close()

	if err := q.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: "repo_live"}); err != nil {
		t.Fatalf("enqueue first sync: %v", err)
	}
	<-entered // job 1 claimed and is now held mid-run

	// The pull-upstream 202 path: the request is marked durable, then the
	// enqueue deduplicates against job 1's running row and is dropped.
	if err := store.RequestSync(ctx, "repo_live"); err != nil {
		t.Fatalf("RequestSync: %v", err)
	}
	if err := q.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: "repo_live"}); err != nil {
		t.Fatalf("enqueue during running job: %v", err)
	}
	close(release) // job 1 finishes; its gated completion owns the follow-up

	// Liveness: the marked request must be observed by a successor run.
	waitFor(t, "successor sync run", func() bool { return claimedRuns.Load() >= 2 })
	up, err := store.GetUpstream(ctx, "repo_live")
	if err != nil {
		t.Fatalf("GetUpstream: %v", err)
	}
	if up.SyncPending {
		t.Errorf("SyncPending still set after the successor run: %+v", up)
	}
}

// TestRiverPushCompletionRescuesStrandedMirrorBack (Codex round 6 on #90):
// RunPush marks the mirror-back durable (RequestSync) before its direct
// enqueue, but pg has no boot scan of pending markers — if that direct
// insert fails while no pull is in flight, only the push job's
// transactional completion can give the marker a vehicle. The push runner
// below mimics exactly that shape: successful forward, marker set, no
// direct enqueue.
func TestRiverPushCompletionRescuesStrandedMirrorBack(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)

	store := sync.NewMemStore()
	if _, err := store.PutUpstream(ctx, sync.Upstream{
		RepoID: "repo_mb", Provider: "gitea", Owner: "o", Name: "n",
		UpstreamHost: "example.test", ForwardOnPush: true,
	}); err != nil {
		t.Fatalf("PutUpstream: %v", err)
	}

	var pullSeq, pullClaims atomic.Int32
	pullRunner := func(ctx context.Context, job sync.Job) error {
		holder := fmt.Sprintf("pull-%d", pullSeq.Add(1))
		if _, _, err := store.BeginSync(ctx, job.RepoID, holder, time.Minute); err != nil {
			if errors.Is(err, sync.ErrSyncInProgress) {
				return nil
			}
			return err
		}
		pullClaims.Add(1)
		_, err := store.FinishSync(ctx, job.RepoID, holder, true)
		return err
	}
	pushRunner := func(ctx context.Context, job sync.Job) error {
		holder := "push-h"
		if _, _, _, err := store.BeginPush(ctx, job.RepoID, holder, time.Minute); err != nil {
			return err
		}
		if _, err := store.FinishPush(ctx, job.RepoID, holder, true); err != nil {
			return err
		}
		// Mirror-back marked durable; the direct enqueue "fails" (the
		// real worker logs and moves on), stranding the marker unless
		// the push completion gate covers it.
		return store.RequestSync(ctx, job.RepoID)
	}

	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	q, err := NewRiver(ctx, pgURL, deliverer, pullRunner, pushRunner, store, nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	defer q.Close()

	if err := q.EnqueuePush(ctx, sync.Job{Org: "org", RepoID: "repo_mb"}); err != nil {
		t.Fatalf("enqueue push: %v", err)
	}

	waitFor(t, "rescued mirror-back pull", func() bool { return pullClaims.Load() >= 1 })
	waitFor(t, "sync_pending consumed", func() bool {
		up, err := store.GetUpstream(ctx, "repo_mb")
		return err == nil && !up.SyncPending
	})
}

// A run that fails BEFORE claiming (transient repo-lookup/store error)
// must not strand a pending request: no run counted, no claim holder —
// the worker snoozes the job so the retry claims and consumes the marker
// (Codex round-2 finding on #79).
func TestRiverSyncPendingSurvivesPreClaimFailure(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)

	store := sync.NewMemStore()
	if _, err := store.PutUpstream(ctx, sync.Upstream{
		RepoID: "repo_flaky", Provider: "gitea", Owner: "o", Name: "n", UpstreamHost: "example.test",
	}); err != nil {
		t.Fatalf("PutUpstream: %v", err)
	}

	var attempts, claimedRuns atomic.Int32
	runner := func(ctx context.Context, job sync.Job) error {
		n := attempts.Add(1)
		if n == 1 {
			return errors.New("transient lookup failure before any claim")
		}
		holder := fmt.Sprintf("test-%d", n)
		if _, _, err := store.BeginSync(ctx, job.RepoID, holder, time.Minute); err != nil {
			return err
		}
		claimedRuns.Add(1)
		_, err := store.FinishSync(ctx, job.RepoID, holder, true)
		return err
	}

	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	q, err := NewRiver(ctx, pgURL, deliverer, runner, func(context.Context, sync.Job) error { return nil }, store, nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	defer q.Close()

	// The 202 path: marked durable, then enqueued; the job's first run
	// dies before claiming.
	if err := store.RequestSync(ctx, "repo_flaky"); err != nil {
		t.Fatalf("RequestSync: %v", err)
	}
	if err := q.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: "repo_flaky"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// The snoozed retry (syncFollowUpSnooze) must claim and consume the
	// pending marker instead of completing the only job and stranding it.
	waitFor(t, "retried claimed run", func() bool { return claimedRuns.Load() >= 1 })
	up, err := store.GetUpstream(ctx, "repo_flaky")
	if err != nil {
		t.Fatalf("GetUpstream: %v", err)
	}
	if up.SyncPending {
		t.Errorf("SyncPending still set after the retried run: %+v", up)
	}
}

// A push run whose publication wait fails restores its claimed request
// (sync.Worker.RunPush: RestorePush, then an error). The job must snooze,
// and its re-run must claim the request again.
func TestRiverPushRestoredRunSnoozesForRetry(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)

	store := sync.NewMemStore()
	if _, err := store.PutUpstream(ctx, sync.Upstream{
		RepoID: "repo_restored", Provider: "gitea", Owner: "o", Name: "n", UpstreamHost: "example.test", ForwardOnPush: true,
	}); err != nil {
		t.Fatalf("PutUpstream: %v", err)
	}

	var attempts, finished atomic.Int32
	pushRunner := func(ctx context.Context, job sync.Job) error {
		holder := fmt.Sprintf("push-%d", attempts.Add(1))
		if _, _, _, err := store.BeginPush(ctx, job.RepoID, holder, time.Minute); err != nil {
			return err
		}
		if attempts.Load() == 1 {
			if _, err := store.RestorePush(ctx, job.RepoID, holder, false); err != nil {
				return err
			}
			return errors.New("publication wait failed")
		}
		finished.Add(1)
		_, err := store.FinishPush(ctx, job.RepoID, holder, true)
		return err
	}

	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	q, err := NewRiver(ctx, pgURL, deliverer, func(context.Context, sync.Job) error { return nil }, pushRunner, store, nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	defer q.Close()

	if err := store.RequestPush(ctx, "repo_restored", false); err != nil {
		t.Fatalf("RequestPush: %v", err)
	}
	if err := q.EnqueuePush(ctx, sync.Job{Org: "org", RepoID: "repo_restored"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	waitFor(t, "re-run after the restored run", func() bool { return finished.Load() >= 1 })
	up, err := store.GetUpstream(ctx, "repo_restored")
	if err != nil {
		t.Fatalf("GetUpstream: %v", err)
	}
	if up.PushPending != sync.PushPendingNone || up.PushRunCount != 1 {
		t.Errorf("restored request not consumed by one counted run: %+v", up)
	}
}

// A run that SKIPS because a dead holder's unexpired lease refuses the
// steal must not complete its only job over the pending request: the
// gated completion snoozes until just past the observed expiry, and the
// re-run steals and syncs (issue #86, rescued-orphan shape).
func TestRiverSyncSkipWithPendingSnoozesUntilSteal(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)

	store := sync.NewMemStore()
	if _, err := store.PutUpstream(ctx, sync.Upstream{
		RepoID: "repo_snz", Provider: "gitea", Owner: "o", Name: "n", UpstreamHost: "example.test",
	}); err != nil {
		t.Fatalf("PutUpstream: %v", err)
	}
	// Crashed holder: claim held, never finished, lease still live.
	if _, _, err := store.BeginSync(ctx, "repo_snz", "dead-holder", 3*time.Second); err != nil {
		t.Fatalf("dead-holder claim: %v", err)
	}

	var runSeq, claimedRuns atomic.Int32
	runner := func(ctx context.Context, job sync.Job) error {
		holder := fmt.Sprintf("test-%d", runSeq.Add(1))
		if _, _, err := store.BeginSync(ctx, job.RepoID, holder, time.Minute); err != nil {
			if errors.Is(err, sync.ErrSyncInProgress) {
				return nil // unexpired lease refused the steal: skip
			}
			return err
		}
		claimedRuns.Add(1)
		_, err := store.FinishSync(ctx, job.RepoID, holder, true)
		return err
	}

	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	q, err := NewRiver(ctx, pgURL, deliverer, runner, func(context.Context, sync.Job) error { return nil }, store, nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	defer q.Close()

	// The 202 path against the wedged row.
	if err := store.RequestSync(ctx, "repo_snz"); err != nil {
		t.Fatalf("RequestSync: %v", err)
	}
	if err := q.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: "repo_snz"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// First run skips; the snoozed re-run (past the 3s lease) steals.
	waitFor(t, "post-expiry stolen run", func() bool { return claimedRuns.Load() >= 1 })
	up, err := store.GetUpstream(ctx, "repo_snz")
	if err != nil {
		t.Fatalf("GetUpstream: %v", err)
	}
	if up.SyncPending || up.Syncing {
		t.Errorf("row after snoozed steal = %+v, want released claim with consumed marker", up)
	}
}
