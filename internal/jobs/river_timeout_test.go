package jobs

import (
	"context"
	"log/slog"
	"math"
	"testing"
	"time"

	"github.com/riverqueue/river"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/ref"
	"github.com/coder/openstorage/internal/sync"
)

func timeLeft(ctx context.Context) time.Duration {
	if d, ok := ctx.Deadline(); ok {
		return time.Until(d)
	}
	return math.MaxInt64
}

func startTimeoutRiver(t *testing.T, pgURL string, runSync SyncRunner, absence *AbsenceReconcileSchedule) *River {
	t.Helper()
	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	noop := func(context.Context, sync.Job) error { return nil }
	q, err := NewRiver(context.Background(), pgURL, deliverer, runSync, noop, sync.NewMemStore(), nil, nil, absence, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	return q
}

func migrateRiver(t *testing.T, pgURL string) {
	t.Helper()
	startTimeoutRiver(t, pgURL, func(context.Context, sync.Job) error { return nil }, nil).Close()
}

func orphanJob(t *testing.T, ins *River, args river.JobArgs, age time.Duration) int64 {
	t.Helper()
	ctx := context.Background()
	res, err := ins.client.Insert(ctx, args, nil)
	if err != nil {
		t.Fatalf("insert %s: %v", args.Kind(), err)
	}
	if _, err := ins.pool.Exec(ctx, `UPDATE river_job
		SET state = 'running', attempt = 1, attempted_at = now() - make_interval(secs => $2), attempted_by = ARRAY['crashed-client']
		WHERE id = $1`, res.Job.ID, age.Seconds()); err != nil {
		t.Fatalf("orphan %s: %v", args.Kind(), err)
	}
	return res.Job.ID
}

func receiveLeft(t *testing.T, what string, left <-chan time.Duration) time.Duration {
	t.Helper()
	select {
	case d := <-left:
		return d
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return 0
	}
}

func TestRiverSyncRunKeepsItsOwnBudget(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)

	left := make(chan time.Duration, 1)
	q := startTimeoutRiver(t, pgURL, func(ctx context.Context, _ sync.Job) error {
		select {
		case left <- timeLeft(ctx):
		default:
		}
		return nil
	}, nil)
	defer q.Close()

	if err := q.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: "repo_budget"}); err != nil {
		t.Fatalf("enqueue sync: %v", err)
	}
	if got := receiveLeft(t, "sync run", left); got <= 11*time.Minute {
		t.Errorf("sync run context has %v left, want more than its 10m run budget plus two 30s detached steps", got)
	}
}

type firstReadDeadlineRegistry struct {
	AbsenceRegistry
	left chan time.Duration
}

func (d *firstReadDeadlineRegistry) GetJobScanState(ctx context.Context, job, scope string) (ref.JobScanState, bool, error) {
	select {
	case d.left <- timeLeft(ctx):
	default:
	}
	return d.AbsenceRegistry.GetJobScanState(ctx, job, scope)
}

func TestRiverAbsencePassKeepsItsHourCap(t *testing.T) {
	pgURL := livenessPG(t, context.Background())
	f := newSweepFixture(t)

	reg := &firstReadDeadlineRegistry{AbsenceRegistry: absenceRegistry(t, f), left: make(chan time.Duration, 1)}
	coverTTL := 4 * absencePassTimeout
	rec := NewAbsenceReconciler(reg, f.blobs, false, MinAbsenceReconcileBudget, coverTTL, time.Hour, slog.New(slog.DiscardHandler))
	q := startTimeoutRiver(t, pgURL, func(context.Context, sync.Job) error { return nil },
		&AbsenceReconcileSchedule{Reconciler: rec, Interval: time.Hour})
	defer q.Close()

	if got := receiveLeft(t, "absence pass", reg.left); got < absencePassTimeout-time.Minute {
		t.Errorf("absence pass context has %v left, want its own %v pass cap", got, absencePassTimeout)
	}
}

type firstListDeadlineBlobs struct {
	blob.BlobStore
	left chan time.Duration
}

func (b *firstListDeadlineBlobs) ListPages(ctx context.Context, prefix string, pageSize int, fn func([]blob.ListedObject) error) error {
	select {
	case b.left <- timeLeft(ctx):
	default:
	}
	return b.BlobStore.ListPages(ctx, prefix, pageSize, fn)
}

func TestRiverSweepPassEndsBeforeItsRescue(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)
	f := newSweepFixture(t)

	blobs := &firstListDeadlineBlobs{BlobStore: f.blobs, left: make(chan time.Duration, 1)}
	sweeper := NewSweeper(blobs, f.store, SweepConfig{Threshold: 24 * time.Hour}, slog.New(slog.DiscardHandler))
	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	noop := func(context.Context, sync.Job) error { return nil }
	q, err := NewRiver(ctx, pgURL, deliverer, noop, noop, sync.NewMemStore(),
		&SweepSchedule{Sweeper: sweeper, Interval: time.Hour}, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	defer q.Close()

	if got := receiveLeft(t, "sweep pass", blobs.left); got > passJobTimeout || got < passJobTimeout-time.Minute {
		t.Errorf("sweep pass context has %v left, want it to end %v after the start", got, passJobTimeout)
	}
}

func TestRiverRescuesOnlyOrphansPastTheirKindsDeadline(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)
	migrateRiver(t, pgURL)

	ins, err := NewRiverInsertOnly(ctx, pgURL)
	if err != nil {
		t.Fatalf("NewRiverInsertOnly: %v", err)
	}
	defer ins.Close()
	stillRunning := map[string]int64{
		"sync 29m after its start":           orphanJob(t, ins, syncArgs{Org: "org", RepoID: "repo_live"}, 29*time.Minute),
		"push 29m after its start":           orphanJob(t, ins, pushArgs{Org: "org", RepoID: "repo_live"}, 29*time.Minute),
		"absence pass 1h10m after its start": orphanJob(t, ins, absenceReconcileArgs{}, time.Hour+10*time.Minute),
		"LFS reclaim 1h10m after its start":  orphanJob(t, ins, lfsReclaimArgs{}, time.Hour+10*time.Minute),
		"sweep 2h10m after its start":        orphanJob(t, ins, sweepArgs{}, 2*time.Hour+10*time.Minute),
	}
	orphanJob(t, ins, syncArgs{Org: "org", RepoID: "repo_crashed"}, 40*time.Minute)
	crashedPush := orphanJob(t, ins, pushArgs{Org: "org", RepoID: "repo_crashed"}, 40*time.Minute)

	ran := make(chan string, 4)
	leaderWithPassesOff := startTimeoutRiver(t, pgURL, func(_ context.Context, job sync.Job) error {
		select {
		case ran <- job.RepoID:
		default:
		}
		return nil
	}, nil)
	defer leaderWithPassesOff.Close()

	// Rescue runs on the elected leader every 30s, then the retry waits
	// about 1s and the scheduler promotes it within 5s.
	deadline := time.After(90 * time.Second)
	for crashedRan := false; !crashedRan; {
		select {
		case repo := <-ran:
			if repo == "repo_live" {
				t.Errorf("repo_live re-ran 29 minutes after its start, while its crashed run can still hold the claim")
			}
			crashedRan = repo == "repo_crashed"
		case <-deadline:
			t.Fatal("repo_crashed's sync orphaned 40 minutes ago was never rescued")
		}
	}
	jobState := func(id int64) (state string, attempt int) {
		t.Helper()
		if err := ins.pool.QueryRow(ctx, `SELECT state, attempt FROM river_job WHERE id = $1`, id).Scan(&state, &attempt); err != nil {
			t.Fatalf("read job %d: %v", id, err)
		}
		return state, attempt
	}
	for what, id := range stillRunning {
		if state, attempt := jobState(id); state != "running" || attempt != 1 {
			t.Errorf("%s = %s at attempt %d, want still running at attempt 1", what, state, attempt)
		}
	}
	if state, attempt := jobState(crashedPush); state == "running" && attempt == 1 {
		t.Errorf("repo_crashed's push orphaned 40 minutes ago was not rescued with its sync")
	}
}
