package jobs

import (
	"context"
	"log/slog"
	"math"
	"testing"
	"time"

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

func orphanSyncJob(t *testing.T, ins *River, repo string, age time.Duration) {
	t.Helper()
	ctx := context.Background()
	if err := ins.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: repo}); err != nil {
		t.Fatalf("enqueue %s: %v", repo, err)
	}
	if _, err := ins.pool.Exec(ctx, `UPDATE river_job
		SET state = 'running', attempt = 1, attempted_at = now() - make_interval(secs => $2), attempted_by = ARRAY['crashed-client']
		WHERE kind = 'upstream_sync' AND args->>'repo_id' = $1`, repo, age.Seconds()); err != nil {
		t.Fatalf("orphan %s: %v", repo, err)
	}
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

func TestRiverRescuesOnlyOrphansPastTheJobDeadline(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)
	migrateRiver(t, pgURL)

	ins, err := NewRiverInsertOnly(ctx, pgURL)
	if err != nil {
		t.Fatalf("NewRiverInsertOnly: %v", err)
	}
	defer ins.Close()
	orphanSyncJob(t, ins, "repo_recent", 65*time.Minute)
	orphanSyncJob(t, ins, "repo_stale", 24*time.Hour)

	ran := make(chan string, 4)
	q := startTimeoutRiver(t, pgURL, func(_ context.Context, job sync.Job) error {
		select {
		case ran <- job.RepoID:
		default:
		}
		return nil
	}, nil)
	defer q.Close()

	// Rescue runs on the elected leader every 30s, then the retry waits
	// about 1s and the scheduler promotes it within 5s.
	deadline := time.After(90 * time.Second)
	for staleRan := false; !staleRan; {
		select {
		case repo := <-ran:
			if repo == "repo_recent" {
				t.Errorf("repo_recent re-ran 65 minutes after its start, inside the job deadline")
			}
			staleRan = repo == "repo_stale"
		case <-deadline:
			t.Fatal("repo_stale's orphaned job was never rescued")
		}
	}
	var state string
	var attempt int
	if err := ins.pool.QueryRow(ctx, `SELECT state, attempt FROM river_job WHERE kind = 'upstream_sync' AND args->>'repo_id' = 'repo_recent'`).Scan(&state, &attempt); err != nil {
		t.Fatalf("read repo_recent job: %v", err)
	}
	if state != "running" || attempt != 1 {
		t.Errorf("repo_recent job = %s at attempt %d, want still running at attempt 1", state, attempt)
	}
}
