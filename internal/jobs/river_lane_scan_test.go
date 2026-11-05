package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/sync"
)

// laneScanEnv is a migrated PostgreSQL database with the orgs "org" and
// "other", for lane scan tests.
type laneScanEnv struct {
	pgURL string
	ms    meta.Backend
	conn  *pgx.Conn
}

func newLaneScanEnv(t *testing.T, ctx context.Context) laneScanEnv {
	t.Helper()
	pgURL := livenessPG(t, ctx)
	ms, err := meta.Open(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ms.Close)
	if err := ms.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}
	priv, err := auth.GenerateKey("ES256")
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.MarshalPublicKeyPEM(priv.Public())
	if err != nil {
		t.Fatal(err)
	}
	for _, org := range []string{"org", "other"} {
		if _, err := ms.UpsertOrgWithKeys(ctx, org, []string{string(pub)}); err != nil {
			t.Fatal(err)
		}
	}
	// Migrate River.
	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	noRun := func(context.Context, sync.Job) error { return nil }
	full, err := NewRiver(ctx, pgURL, deliverer, noRun, noRun, sync.NewMemStore(), nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	full.Close()
	conn, err := pgx.Connect(ctx, pgURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return laneScanEnv{pgURL: pgURL, ms: ms, conn: conn}
}

// linked creates a repo of org with an upstream link.
func (e laneScanEnv) linked(t *testing.T, ctx context.Context, org, name string) meta.Repo {
	t.Helper()
	repo, err := e.ms.CreateRepo(ctx, org, "https://example.com/"+name+".git", "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.ms.SetRepoUpstream(ctx, meta.RepoUpstream{
		RepoID: repo.RepoID, Provider: "gitea", Owner: "o", Name: name, UpstreamHost: "example.test",
	}); err != nil {
		t.Fatal(err)
	}
	return repo
}

func (e laneScanEnv) exec(t *testing.T, ctx context.Context, sql string, args ...any) {
	t.Helper()
	if _, err := e.conn.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// liveJobs counts the jobs of kind for repoID that River's uniqueness
// covers (syncArgs and pushArgs ByState).
func (e laneScanEnv) liveJobs(t *testing.T, ctx context.Context, kind, repoID string) int {
	t.Helper()
	var n int
	if err := e.conn.QueryRow(ctx, `SELECT count(*) FROM river_job WHERE kind = $1 AND args->>'repo_id' = $2
		AND state IN ('available', 'pending', 'retryable', 'running', 'scheduled')`, kind, repoID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestRiverLaneScanRequeuesLanesWithoutJob: one scan pass inserts a job
// for each lane of the org's live repos whose claim guard holds and that
// has no live job: a pending request whose job River discarded, or an
// expired claim. It leaves alone a lane with a live job or a live claim, a
// lapsed legacy claim without the steal opt-in, a deleted repo, another
// org's repo, and an idle lane.
func TestRiverLaneScanRequeuesLanesWithoutJob(t *testing.T) {
	ctx := context.Background()
	t.Setenv(meta.EnvSyncStealLegacy, "false")
	env := newLaneScanEnv(t, ctx)
	q, err := NewRiverInsertOnly(ctx, env.pgURL) // no workers: jobs stay put
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	q.logger = slog.New(slog.DiscardHandler)

	discardJobs := func(repoID string) {
		env.exec(t, ctx, `UPDATE river_job SET state = 'discarded', finalized_at = now() WHERE args->>'repo_id' = $1`, repoID)
	}
	type lanes struct{ push, pull int }
	cases := []struct {
		name  string
		org   string
		setup func(repoID string)
		want  lanes // live jobs after the pass
	}{
		{"push_pending_discarded", "org", func(id string) {
			if err := env.ms.RequestPush(ctx, id, false); err != nil {
				t.Fatal(err)
			}
			if err := q.EnqueuePush(ctx, sync.Job{Org: "org", RepoID: id}); err != nil {
				t.Fatal(err)
			}
			discardJobs(id)
		}, lanes{push: 1}},
		{"pull_pending_discarded", "org", func(id string) {
			if err := env.ms.RequestSync(ctx, id); err != nil {
				t.Fatal(err)
			}
			if err := q.EnqueueSync(ctx, sync.Job{Org: "org", RepoID: id}); err != nil {
				t.Fatal(err)
			}
			discardJobs(id)
		}, lanes{pull: 1}},
		{"push_claim_expired", "org", func(id string) {
			env.exec(t, ctx, `UPDATE repo_upstreams SET push_syncing = true, push_claim_holder = 'h-dead',
				push_claim_expires_at = now() - interval '1 minute' WHERE repo_id = $1`, id)
		}, lanes{push: 1}},
		{"pull_claim_expired", "org", func(id string) {
			env.exec(t, ctx, `UPDATE repo_upstreams SET syncing = true, sync_claim_holder = 'h-dead',
				sync_claim_expires_at = now() - interval '1 minute' WHERE repo_id = $1`, id)
		}, lanes{pull: 1}},
		{"claims_live", "org", func(id string) {
			env.exec(t, ctx, `UPDATE repo_upstreams SET push_syncing = true, push_pending = 1, push_claim_holder = 'h-live',
				push_claim_expires_at = now() + interval '1 hour', syncing = true, sync_pending = true,
				sync_claim_holder = 'h-live', sync_claim_expires_at = now() + interval '1 hour' WHERE repo_id = $1`, id)
		}, lanes{}},
		{"pull_legacy_lapsed", "org", func(id string) {
			env.exec(t, ctx, `UPDATE repo_upstreams SET syncing = true, sync_pending = true, sync_claim_holder = $2,
				sync_claim_expires_at = now() - interval '1 minute' WHERE repo_id = $1`, id, meta.LegacyClaimHolder)
		}, lanes{}},
		{"push_pending_live_job", "org", func(id string) {
			if err := env.ms.RequestPush(ctx, id, false); err != nil {
				t.Fatal(err)
			}
			if err := q.EnqueuePush(ctx, sync.Job{Org: "org", RepoID: id}); err != nil {
				t.Fatal(err)
			}
		}, lanes{push: 1}},
		{"deleted_repo", "org", func(id string) {
			if err := env.ms.RequestPush(ctx, id, false); err != nil {
				t.Fatal(err)
			}
			env.exec(t, ctx, `UPDATE repos SET deleted_at = now() WHERE repo_id = $1`, id)
		}, lanes{}},
		{"other_org", "other", func(id string) {
			if err := env.ms.RequestPush(ctx, id, false); err != nil {
				t.Fatal(err)
			}
		}, lanes{}},
		{"idle", "org", func(string) {}, lanes{}},
	}
	repos := make([]string, len(cases))
	for i, tc := range cases {
		repos[i] = env.linked(t, ctx, tc.org, tc.name).RepoID
		tc.setup(repos[i])
	}
	for i, tc := range cases {
		if tc.name == "push_pending_live_job" {
			continue
		}
		if n := env.liveJobs(t, ctx, "upstream_push", repos[i]) + env.liveJobs(t, ctx, "upstream_sync", repos[i]); n != 0 {
			t.Fatalf("%s: %d live jobs before the scan, want the stranded state", tc.name, n)
		}
	}

	prev := laneScanPage
	laneScanPage = 2 // several pages per pass
	t.Cleanup(func() { laneScanPage = prev })
	requeued, err := q.scanLanes(ctx, "org")
	if err != nil || q.scanCursor != "" || requeued != 4 {
		t.Fatalf("scanLanes = %d requeued, cursor %q, err %v; want a full pass that requeued 4", requeued, q.scanCursor, err)
	}
	for i, tc := range cases {
		got := lanes{push: env.liveJobs(t, ctx, "upstream_push", repos[i]), pull: env.liveJobs(t, ctx, "upstream_sync", repos[i])}
		if got != tc.want {
			t.Errorf("%s: live jobs %+v, want %+v", tc.name, got, tc.want)
		}
	}
	// The scan's jobs are the ones EnqueuePush and EnqueueSync insert.
	for i, tc := range cases {
		if tc.name != "push_pending_discarded" && tc.name != "pull_pending_discarded" {
			continue
		}
		var same bool
		if err := env.conn.QueryRow(ctx, `SELECT count(DISTINCT (kind, args, queue, max_attempts, priority)) = 1 AND count(*) = 2
			FROM river_job WHERE args->>'repo_id' = $1`, repos[i]).Scan(&same); err != nil {
			t.Fatal(err)
		}
		if !same {
			t.Errorf("%s: the scan's job differs from the discarded job that the enqueue inserted", tc.name)
		}
	}
	// Every lane with work now has a live job: the next pass adds none.
	if requeued, err := q.scanLanes(ctx, "org"); err != nil || requeued != 0 {
		t.Fatalf("second pass requeued %d (err %v), want 0", requeued, err)
	}
}

// TestRiverLaneScanRechecksAfterInsert: a lane that the list saw with work
// can lose it to a run before the scan inserts its job. The insert is then
// rolled back, so the lane gets no extra run.
func TestRiverLaneScanRechecksAfterInsert(t *testing.T) {
	ctx := context.Background()
	env := newLaneScanEnv(t, ctx)
	q, err := NewRiverInsertOnly(ctx, env.pgURL)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	q.logger = slog.New(slog.DiscardHandler)
	idle := env.linked(t, ctx, "org", "consumed").RepoID // as after the run's claim
	pullOnly := env.linked(t, ctx, "org", "pull-only").RepoID
	if err := env.ms.RequestSync(ctx, pullOnly); err != nil {
		t.Fatal(err)
	}
	pushOnly := env.linked(t, ctx, "org", "push-only").RepoID
	if err := env.ms.RequestPush(ctx, pushOnly, false); err != nil {
		t.Fatal(err)
	}
	deleted := env.linked(t, ctx, "org", "deleted").RepoID
	if err := env.ms.RequestPush(ctx, deleted, false); err != nil {
		t.Fatal(err)
	}
	env.exec(t, ctx, `UPDATE repos SET deleted_at = now() WHERE repo_id = $1`, deleted)
	unlinked, err := env.ms.CreateRepo(ctx, "org", "https://example.com/unlinked.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		repoID string
		push   bool
	}{
		{"idle push", idle, true}, {"idle pull", idle, false},
		{"push of a pull-only lane", pullOnly, true}, {"pull of a push-only lane", pushOnly, false},
		{"deleted repo", deleted, true}, {"unlinked repo", unlinked.RepoID, true},
	} {
		if inserted, err := q.requeueLane(ctx, "org", tc.repoID, tc.push); err != nil || inserted {
			t.Errorf("requeueLane(%s) = %v, %v; want no job", tc.name, inserted, err)
		}
	}
	var n int
	if err := env.conn.QueryRow(ctx, `SELECT count(*) FROM river_job`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("%d job rows for lanes without work, want 0", n)
	}
}

// laneTrio links three repos of "org" with a push request each and returns
// their IDs in the scan's order.
func (e laneScanEnv) laneTrio(t *testing.T, ctx context.Context) (a, b, c string) {
	t.Helper()
	var ids []string
	for _, name := range []string{"trio-1", "trio-2", "trio-3"} {
		id := e.linked(t, ctx, "org", name).RepoID
		if err := e.ms.RequestPush(ctx, id, false); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	rows, err := e.conn.Query(ctx, `SELECT id FROM unnest($1::text[]) AS id ORDER BY id`, ids)
	if err != nil {
		t.Fatal(err)
	}
	sorted, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || len(sorted) != 3 {
		t.Fatalf("sort repo ids: %v %v", sorted, err)
	}
	return sorted[0], sorted[1], sorted[2]
}

// lockJob holds a row lock on repoID's push job until the returned release
// runs, so a scan insert that meets the job waits.
func (e laneScanEnv) lockJob(t *testing.T, ctx context.Context, repoID string) (release func()) {
	t.Helper()
	tx, err := e.conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := tx.QueryRow(ctx, `WITH l AS (SELECT id FROM river_job WHERE kind = 'upstream_push' AND args->>'repo_id' = $1
		AND state = 'available' FOR UPDATE) SELECT count(*) FROM l`, repoID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("lock the push job of %s: %d rows, %v", repoID, n, err)
	}
	return func() { _ = tx.Rollback(ctx) }
}

// TestRiverLaneScanResumesAfterTimeout: a pass that runs out of time ends
// within laneScanTimeout and keeps its cursor, and the next pass continues
// after the last lane it handled instead of starting over.
func TestRiverLaneScanResumesAfterTimeout(t *testing.T) {
	ctx := context.Background()
	env := newLaneScanEnv(t, ctx)
	q, err := NewRiverInsertOnly(ctx, env.pgURL)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	q.logger = slog.New(slog.DiscardHandler)
	prev := laneScanTimeout
	laneScanTimeout = time.Second
	t.Cleanup(func() { laneScanTimeout = prev })

	a, b, c := env.laneTrio(t, ctx)
	if err := q.EnqueuePush(ctx, sync.Job{Org: "org", RepoID: b}); err != nil {
		t.Fatal(err)
	}
	conn2, err := pgx.Connect(ctx, env.pgURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn2.Close(context.Background()) })
	releaseB := env.lockJob(t, ctx, b) // b's insert waits: the pass times out there

	type pass struct {
		requeued int
		err      error
	}
	done := make(chan pass, 1)
	go func() {
		n, err := q.scanLanes(ctx, "org")
		done <- pass{n, err}
	}()
	var first pass
	select {
	case first = <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("the pass waited at b past laneScanTimeout")
	}
	if !errors.Is(first.err, context.DeadlineExceeded) || first.requeued != 1 || q.scanCursor != a {
		t.Fatalf("first pass = %d requeued, cursor %q, err %v; want a's job, then a timeout at b with the cursor at a",
			first.requeued, q.scanCursor, first.err)
	}
	// A pass that started over would now wait at a.
	releaseA := laneScanEnv{conn: conn2}.lockJob(t, ctx, a)
	defer releaseA()
	releaseB()
	requeued, err := q.scanLanes(ctx, "org")
	if err != nil || requeued != 1 || q.scanCursor != "" {
		t.Fatalf("second pass = %d requeued, cursor %q, err %v; want c's job and a full traversal", requeued, q.scanCursor, err)
	}
	if n := env.liveJobs(t, ctx, "upstream_push", c); n != 1 {
		t.Fatalf("c has %d live push jobs, want 1", n)
	}

	// A pass whose list fails keeps the cursor too.
	q.scanCursor = a
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := q.scanLanes(cancelled, "org"); err == nil || q.scanCursor != a {
		t.Fatalf("cancelled pass: err %v, cursor %q; want an error and the cursor kept at a", err, q.scanCursor)
	}
}

// TestRiverLaneScanSkipsFailingLane: a lane whose requeue fails does not
// stop the pass. The lanes after it still get their jobs.
func TestRiverLaneScanSkipsFailingLane(t *testing.T) {
	ctx := context.Background()
	env := newLaneScanEnv(t, ctx)
	// Every statement of this client gives up on a lock after half a second.
	q, err := NewRiverInsertOnly(ctx, env.pgURL+"&options=-c%20lock_timeout%3D500")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	q.logger = slog.New(slog.DiscardHandler)
	a, b, c := env.laneTrio(t, ctx)
	if err := q.EnqueuePush(ctx, sync.Job{Org: "org", RepoID: b}); err != nil {
		t.Fatal(err)
	}
	defer env.lockJob(t, ctx, b)() // b's requeue fails on the lock

	requeued, err := q.scanLanes(ctx, "org")
	if err != nil || requeued != 2 || q.scanCursor != "" {
		t.Fatalf("scanLanes = %d requeued, cursor %q, err %v; want a's and c's jobs and a full traversal", requeued, q.scanCursor, err)
	}
	for _, id := range []string{a, c} {
		if n := env.liveJobs(t, ctx, "upstream_push", id); n != 1 {
			t.Errorf("%s has %d live push jobs, want 1", id, n)
		}
	}
}

// TestRiverLaneScanRunsStrandedLanes: StartLaneScan passes periodically,
// and the job it inserts runs. A lane stranded after the first pass gets a
// run from a later one.
func TestRiverLaneScanRunsStrandedLanes(t *testing.T) {
	ctx := context.Background()
	env := newLaneScanEnv(t, ctx)
	_, ups := sync.MetaStores(env.ms)
	var runs atomic.Int32
	push := func(ctx context.Context, job sync.Job) error {
		holder := fmt.Sprintf("test-%d", runs.Add(1))
		if _, _, _, err := ups.BeginPush(ctx, job.RepoID, holder, time.Minute); err != nil {
			return err
		}
		_, err := ups.FinishPush(ctx, job.RepoID, holder, true)
		return err
	}
	noRun := func(context.Context, sync.Job) error { return nil }
	deliverer := &events.Deliverer{Webhooks: stubWebhooks{}, Logger: slog.New(slog.DiscardHandler)}
	q, err := NewRiver(ctx, env.pgURL, deliverer, noRun, push, ups, nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	q.StartLaneScan("org", 0, 100*time.Millisecond)

	for i, name := range []string{"first", "later"} {
		repo := env.linked(t, ctx, "org", name).RepoID
		if err := env.ms.RequestPush(ctx, repo, false); err != nil { // no job: stranded
			t.Fatal(err)
		}
		waitFor(t, name+" stranded push run", func() bool { return runs.Load() >= int32(i+1) })
		waitFor(t, name+" push request consumed", func() bool {
			up, err := ups.GetUpstream(ctx, repo)
			return err == nil && up.PushPending == 0 && !up.PushSyncing
		})
	}

	// DrainSyncRuns stops the scan, and so does Close of a River that never
	// drained.
	scanStops := func(what string, q *River) {
		t.Helper()
		stopped := make(chan struct{})
		go func() {
			q.scanning.Wait()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			t.Fatalf("%s left the lane scan running", what)
		}
	}
	q.DrainSyncRuns(ctx, time.Second)
	scanStops("DrainSyncRuns", q)
	closed, err := NewRiver(ctx, env.pgURL, deliverer, noRun, push, ups, nil, nil, nil, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	closed.StartLaneScan("org", time.Hour, time.Hour)
	closed.Close()
	scanStops("Close", closed)
}
