package jobs

// Serve shutdown on the River lane (issue #343): DrainSyncRuns soft-stops
// River, waits for running jobs, then cancels sync and push runs with
// sync.ErrShutdown. A run cancelled that way restores its request, and the
// unchanged completion gate snoozes its job, so the next process runs the
// request exactly once.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/sync"
)

// drainRecorder records which repos each process's runners started.
type drainRecorder struct {
	mu      stdsync.Mutex
	started []string
	causes  []error
}

func (r *drainRecorder) start(repo string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.started = append(r.started, repo)
}

func (r *drainRecorder) cause(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.causes = append(r.causes, err)
}

func (r *drainRecorder) count(repo string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, s := range r.started {
		if s == repo {
			n++
		}
	}
	return n
}

// riverJob reads one repo's only job of kind: its state and args retry.
func riverJob(t *testing.T, pgURL, kind, repo string) (state string, retry int) {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), pgURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(t.Context()) }()
	var n int
	if err := conn.QueryRow(t.Context(),
		`SELECT count(*), min(state::text), coalesce(min((args->>'retry')::int), 0) FROM river_job WHERE kind = $1 AND args->>'repo_id' = $2`,
		kind, repo).Scan(&n, &state, &retry); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("%s jobs for %s: %d, want exactly 1", kind, repo, n)
	}
	return state, retry
}

func TestRiverDrainSyncRunsKeepsRequests(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)
	store := sync.NewMemStore()
	for _, id := range []string{"repo_pull", "repo_push", "repo_late"} {
		if _, err := store.PutUpstream(ctx, sync.Upstream{
			RepoID: id, Provider: "gitea", Owner: "o", Name: id, UpstreamHost: "example.test",
		}); err != nil {
			t.Fatal(err)
		}
	}
	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}

	// Process 1: runs claim, then hang until cancelled; a shutdown cause
	// restores, as sync.Worker does on PostgreSQL.
	var seq atomic.Int32
	rec1 := &drainRecorder{}
	hang := func(push bool) SyncRunner {
		return func(ctx context.Context, job sync.Job) error {
			holder := fmt.Sprintf("p1-%d", seq.Add(1))
			var force bool
			if push {
				_, f, _, err := store.BeginPush(ctx, job.RepoID, holder, time.Minute)
				if err != nil {
					return err
				}
				force = f
			} else if _, _, err := store.BeginSync(ctx, job.RepoID, holder, time.Minute); err != nil {
				return err
			}
			rec1.start(job.RepoID)
			<-ctx.Done()
			rec1.cause(context.Cause(ctx))
			if !errors.Is(context.Cause(ctx), sync.ErrShutdown) {
				return ctx.Err()
			}
			var err error
			if push {
				_, err = store.RestorePush(context.WithoutCancel(ctx), job.RepoID, holder, force)
			} else {
				_, err = store.RestoreSync(context.WithoutCancel(ctx), job.RepoID, holder)
			}
			return errors.Join(err, sync.ErrShutdown)
		}
	}
	q1, err := NewRiver(ctx, pgURL, deliverer, hang(false), hang(true), store, nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	if err := store.RequestSync(ctx, "repo_pull"); err != nil {
		t.Fatal(err)
	}
	if err := q1.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: "repo_pull"}); err != nil {
		t.Fatal(err)
	}
	if err := store.RequestPush(ctx, "repo_push", true); err != nil {
		t.Fatal(err)
	}
	if err := q1.EnqueuePush(ctx, sync.Job{Org: "org", RepoID: "repo_push", Retry: 2}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "both runs in flight", func() bool { return rec1.count("repo_pull") == 1 && rec1.count("repo_push") == 1 })

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		q1.DrainSyncRuns(dctx, 500*time.Millisecond)
	}()
	// A request recorded during the drain is inserted, but the stopping
	// client starts no new job.
	time.Sleep(100 * time.Millisecond)
	if err := store.RequestSync(ctx, "repo_late"); err != nil {
		t.Fatal(err)
	}
	if err := q1.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: "repo_late"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-drained:
	case <-time.After(30 * time.Second):
		t.Fatal("DrainSyncRuns did not return")
	}
	q1.Close()

	if n := rec1.count("repo_late"); n != 0 {
		t.Fatalf("a job started during the drain (%d runs)", n)
	}
	for _, c := range rec1.causes {
		if !errors.Is(c, sync.ErrShutdown) {
			t.Fatalf("run cancelled with cause %v, want sync.ErrShutdown", c)
		}
	}
	if up, _ := store.GetUpstream(ctx, "repo_pull"); up.RunCount != 0 || !up.SyncPending || up.Syncing {
		t.Fatalf("pull after the drain: %+v, want no run counted, the request pending, no claim", up)
	}
	if up, _ := store.GetUpstream(ctx, "repo_push"); up.PushRunCount != 0 || up.PushPending != sync.PushPendingForce || up.PushSyncing {
		t.Fatalf("push after the drain: %+v, want no run counted, force pending, no claim", up)
	}
	// The gate kept both jobs (snoozed: River stores a short snooze as
	// available at a later scheduled_at), with the push job's retry.
	kept := func(state string) bool { return state == "available" || state == "scheduled" }
	if state, _ := riverJob(t, pgURL, "upstream_sync", "repo_pull"); !kept(state) {
		t.Fatalf("pull job state %q, want it kept for the next process", state)
	}
	if state, retry := riverJob(t, pgURL, "upstream_push", "repo_push"); !kept(state) || retry != 2 {
		t.Fatalf("push job state %q retry %d, want it kept with retry 2", state, retry)
	}

	// Process 2 runs each kept request exactly once.
	rec2 := &drainRecorder{}
	finish := func(push bool) SyncRunner {
		return func(ctx context.Context, job sync.Job) error {
			holder := fmt.Sprintf("p2-%d", seq.Add(1))
			if push {
				_, force, _, err := store.BeginPush(ctx, job.RepoID, holder, time.Minute)
				if err != nil {
					return err
				}
				if !force || job.Retry != 2 {
					return fmt.Errorf("rerun lost its force (%v) or retry (%d)", force, job.Retry)
				}
				rec2.start(job.RepoID)
				_, err = store.FinishPush(ctx, job.RepoID, holder, true)
				return err
			}
			if _, _, err := store.BeginSync(ctx, job.RepoID, holder, time.Minute); err != nil {
				return err
			}
			rec2.start(job.RepoID)
			_, err := store.FinishSync(ctx, job.RepoID, holder, true)
			return err
		}
	}
	q2, err := NewRiver(ctx, pgURL, deliverer, finish(false), finish(true), store, nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	defer q2.Close()
	waitFor(t, "kept requests completed", func() bool {
		a, _ := store.GetUpstream(ctx, "repo_pull")
		b, _ := store.GetUpstream(ctx, "repo_push")
		c, _ := store.GetUpstream(ctx, "repo_late")
		return a.SuccessCount == 1 && b.PushSuccessCount == 1 && c.SuccessCount == 1
	})
	time.Sleep(syncFollowUpSnooze + time.Second) // room for any stray rerun
	for _, repo := range []string{"repo_pull", "repo_push", "repo_late"} {
		if n := rec2.count(repo); n != 1 {
			t.Fatalf("%s ran %d times after restart, want exactly 1", repo, n)
		}
	}
}

// closeExitBound is how long Close may take on an exit that skipped
// DrainSyncRuns: serve's whole signal drain gets 10 seconds.
const closeExitBound = 10 * time.Second

// closeWithin runs q.Close and fails the test when it does not return
// within closeExitBound.
func closeWithin(t *testing.T, q *River) {
	t.Helper()
	closed := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(closed)
		q.Close()
	}()
	select {
	case <-closed:
		t.Logf("Close returned after %v", time.Since(start).Round(time.Millisecond))
	case <-time.After(closeExitBound):
		t.Fatalf("Close did not return within %v", closeExitBound)
	}
}

// Serve's startup-failure and listener exits call Close without
// DrainSyncRuns (issue #636). Close cancels the running jobs and returns
// within the bound. The sync run keeps its request, and River makes the
// cancelled delivery available again without counting an attempt.
func TestRiverCloseWithoutDrainKeepsJobs(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)
	store := sync.NewMemStore()
	if _, err := store.PutUpstream(ctx, sync.Upstream{
		RepoID: "repo_pull", Provider: "gitea", Owner: "o", Name: "n", UpstreamHost: "example.test",
	}); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var posted atomic.Bool
	hook := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		posted.Store(true)
		select {
		case <-r.Context().Done():
		case <-stop:
		}
	}))
	defer hook.Close()
	defer close(stop)
	deliverer := &events.Deliverer{
		Webhooks: stubWebhooks{"wh_1": {ID: "wh_1", URL: hook.URL, Secret: "s"}},
		Client:   hook.Client(),
		Logger:   slog.New(slog.DiscardHandler),
	}
	var inFlight atomic.Bool
	hang := func(ctx context.Context, job sync.Job) error {
		if _, _, err := store.BeginSync(ctx, job.RepoID, "p1", time.Minute); err != nil {
			return err
		}
		inFlight.Store(true)
		<-ctx.Done()
		if !errors.Is(context.Cause(ctx), sync.ErrShutdown) {
			return ctx.Err()
		}
		_, err := store.RestoreSync(context.WithoutCancel(ctx), job.RepoID, "p1")
		return errors.Join(err, sync.ErrShutdown)
	}
	q, err := NewRiver(ctx, pgURL, deliverer, hang, func(context.Context, sync.Job) error { return nil }, store, nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	if err := store.RequestSync(ctx, "repo_pull"); err != nil {
		t.Fatal(err)
	}
	if err := q.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: "repo_pull"}); err != nil {
		t.Fatal(err)
	}
	if err := q.EnqueueWebhookDelivery(ctx, events.Delivery{WebhookID: "wh_1", Event: "repo.created", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "sync run and delivery in flight", func() bool { return inFlight.Load() && posted.Load() })

	closeWithin(t, q)

	if up, _ := store.GetUpstream(ctx, "repo_pull"); up.RunCount != 0 || !up.SyncPending || up.Syncing {
		t.Fatalf("pull after Close: %+v, want no run counted, the request pending, no claim", up)
	}
	if state, _ := riverJob(t, pgURL, "upstream_sync", "repo_pull"); state != "available" && state != "scheduled" {
		t.Fatalf("sync job state %q, want it kept for the next process", state)
	}
	conn, err := pgx.Connect(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var state string
	var attempt, errs int
	if err := conn.QueryRow(ctx,
		`SELECT state::text, attempt, coalesce(array_length(errors, 1), 0) FROM river_job WHERE kind = 'webhook_delivery'`,
	).Scan(&state, &attempt, &errs); err != nil {
		t.Fatal(err)
	}
	if state != "available" || attempt != 0 || errs != 0 {
		t.Fatalf("delivery job: state %q attempt %d errors %d, want available with no attempt counted", state, attempt, errs)
	}
}

// A job that outlives its cancellation, as the sweep's detached recovery
// can, does not hold Close past the bound. Its row stays running, as after
// a crash, until River rescues it (RescueStuckJobsAfter).
func TestRiverCloseBoundedWhenJobOutlivesCancel(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)
	store := sync.NewMemStore()
	if _, err := store.PutUpstream(ctx, sync.Upstream{
		RepoID: "repo_stuck", Provider: "gitea", Owner: "o", Name: "n", UpstreamHost: "example.test",
	}); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var inFlight atomic.Bool
	stuck := func(context.Context, sync.Job) error {
		inFlight.Store(true)
		<-release
		return nil
	}
	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	q, err := NewRiver(ctx, pgURL, deliverer, stuck, func(context.Context, sync.Job) error { return nil }, store, nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	if err := q.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: "repo_stuck"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "run in flight", inFlight.Load)

	closeWithin(t, q)

	if state, _ := riverJob(t, pgURL, "upstream_sync", "repo_stuck"); state != "running" {
		t.Fatalf("job state %q, want running until River rescues it", state)
	}
}

// A run that ends within the wait finishes normally: DrainSyncRuns cancels
// nothing while every job completes in time.
func TestRiverDrainSyncRunsWaitsForRunsThatFinish(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)
	store := sync.NewMemStore()
	if _, err := store.PutUpstream(ctx, sync.Upstream{
		RepoID: "repo_ok", Provider: "gitea", Owner: "o", Name: "n", UpstreamHost: "example.test",
	}); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var running atomic.Bool
	var cancelled atomic.Bool
	runner := func(ctx context.Context, job sync.Job) error {
		if _, _, err := store.BeginSync(ctx, job.RepoID, "h", time.Minute); err != nil {
			return err
		}
		running.Store(true)
		select {
		case <-release:
		case <-ctx.Done():
			cancelled.Store(true)
		}
		_, err := store.FinishSync(context.WithoutCancel(ctx), job.RepoID, "h", true)
		return err
	}
	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	q, err := NewRiver(ctx, pgURL, deliverer, runner, func(context.Context, sync.Job) error { return nil }, store, nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	defer q.Close()
	if err := q.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: "repo_ok"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "run in flight", running.Load)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		q.DrainSyncRuns(ctx, time.Minute)
	}()
	select {
	case <-drained:
		t.Fatal("DrainSyncRuns returned while a run was in flight")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	<-drained
	if cancelled.Load() {
		t.Fatal("a run that finished within the wait was cancelled")
	}
	if up, _ := store.GetUpstream(ctx, "repo_ok"); up.SuccessCount != 1 {
		t.Fatalf("row %+v, want one successful run", up)
	}
	if state, _ := riverJob(t, pgURL, "upstream_sync", "repo_ok"); state != "completed" {
		t.Fatalf("job state %q, want completed", state)
	}
}

// A run whose shutdown restore FAILED leaves its claim held and its request
// consumed. The job must still be kept: its next run steals the expired
// claim and does the work exactly once (issue #343 review, P1).
func TestRiverDrainKeepsJobWhenRestoreFails(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)
	store := sync.NewMemStore()
	if _, err := store.PutUpstream(ctx, sync.Upstream{
		RepoID: "repo_stuck", Provider: "gitea", Owner: "o", Name: "n", UpstreamHost: "example.test",
	}); err != nil {
		t.Fatal(err)
	}
	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	const lease = 2 * time.Second
	var inFlight atomic.Bool
	hang := func(ctx context.Context, job sync.Job) error {
		if _, _, err := store.BeginSync(ctx, job.RepoID, "p1", lease); err != nil {
			return err
		}
		inFlight.Store(true)
		<-ctx.Done()
		// Restore failed, as sync.Worker reports it: claim kept.
		return fmt.Errorf("restore sync: %w: store unavailable", sync.ErrShutdown)
	}
	q1, err := NewRiver(ctx, pgURL, deliverer, hang, func(context.Context, sync.Job) error { return nil }, store, nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	if err := store.RequestSync(ctx, "repo_stuck"); err != nil {
		t.Fatal(err)
	}
	if err := q1.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: "repo_stuck"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "run in flight", inFlight.Load)
	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	q1.DrainSyncRuns(dctx, 100*time.Millisecond)
	cancel()
	q1.Close()
	if up, _ := store.GetUpstream(ctx, "repo_stuck"); !up.Syncing || up.SyncPending {
		t.Fatalf("row %+v, want the failed restore's shape: claim held, request consumed", up)
	}
	if state, _ := riverJob(t, pgURL, "upstream_sync", "repo_stuck"); state != "available" && state != "scheduled" {
		t.Fatalf("job state %q, want it kept", state)
	}

	var runs atomic.Int32
	steal := func(ctx context.Context, job sync.Job) error {
		_, stolen, err := store.BeginSync(ctx, job.RepoID, "p2", time.Minute)
		if err != nil {
			return err
		}
		if stolen != "p1" {
			return fmt.Errorf("claim not stolen from p1 (%q)", stolen)
		}
		runs.Add(1)
		_, err = store.FinishSync(ctx, job.RepoID, "p2", true)
		return err
	}
	q2, err := NewRiver(ctx, pgURL, deliverer, steal, func(context.Context, sync.Job) error { return nil }, store, nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	defer q2.Close()
	waitFor(t, "the kept job to steal and run", func() bool { return runs.Load() == 1 })
	time.Sleep(syncFollowUpSnooze + time.Second)
	if n := runs.Load(); n != 1 {
		t.Fatalf("%d runs after restart, want exactly 1", n)
	}
	if up, _ := store.GetUpstream(ctx, "repo_stuck"); up.Syncing || up.SuccessCount != 1 {
		t.Fatalf("row %+v, want one successful run and no claim", up)
	}
}

// A run that already counted its failure when the drain cancelled it
// returns a plain error: its job completes through the gate and is not
// rerun (issue #343 review).
func TestRiverDrainCompletesCommittedFailure(t *testing.T) {
	ctx := context.Background()
	pgURL := livenessPG(t, ctx)
	store := sync.NewMemStore()
	if _, err := store.PutUpstream(ctx, sync.Upstream{
		RepoID: "repo_done", Provider: "gitea", Owner: "o", Name: "n", UpstreamHost: "example.test",
	}); err != nil {
		t.Fatal(err)
	}
	var counted atomic.Bool
	runner := func(ctx context.Context, job sync.Job) error {
		if _, _, err := store.BeginSync(ctx, job.RepoID, "h", time.Minute); err != nil {
			return err
		}
		if _, err := store.FinishSync(ctx, job.RepoID, "h", false); err != nil {
			return err
		}
		counted.Store(true)
		<-ctx.Done() // the drain arrives while the run emits its terminal event
		return errors.New("failed to clone repository")
	}
	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	q, err := NewRiver(ctx, pgURL, deliverer, runner, func(context.Context, sync.Job) error { return nil }, store, nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewRiver: %v", err)
	}
	if err := q.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: "repo_done"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "counted failure", counted.Load)
	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	q.DrainSyncRuns(dctx, 100*time.Millisecond)
	cancel()
	q.Close()
	if state, _ := riverJob(t, pgURL, "upstream_sync", "repo_done"); state != "completed" {
		t.Fatalf("job state %q, want completed (the failure was already counted)", state)
	}
}
