// Conformance and model tests for the SQLite local schedule (issue #266):
// the lane row owns pending level, due time, attempt, and claim, and every
// transition is one BEGIN IMMEDIATE transaction on that row. All time is
// store time (WithNowFunc), so no test sleeps.

package meta

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

const schedLease = 16 * time.Minute

// schedFixture is one SQLite store with a mutable injected clock and one
// upstream-linked repo in a served org.
type schedFixture struct {
	s    *SQLite
	now  time.Time
	org  string
	repo string
	url  string
}

func newSchedFixture(t *testing.T) *schedFixture {
	t.Helper()
	f := &schedFixture{now: time.Unix(1700000000, 0).UTC(), org: "sched-org"}
	s, err := OpenSQLite(t.Context(), "sqlite://"+filepath.Join(t.TempDir(), "meta.sqlite"),
		WithNowFunc(func() time.Time { return f.now }))
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(s.Close)
	if err := s.InitSchema(t.Context()); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	f.s = s
	f.repo, f.url = f.addRepo(t, f.org, "main")
	return f
}

// addRepo creates a repo with an upstream link in org and returns its ID and URL.
func (f *schedFixture) addRepo(t *testing.T, org, name string) (string, string) {
	t.Helper()
	ctx := t.Context()
	if _, err := f.s.UpsertOrgWithKeys(ctx, org, []string{"-----BEGIN PUBLIC KEY-----\ndummy\n-----END PUBLIC KEY-----\n"}); err != nil {
		t.Fatalf("seed org %q: %v", org, err)
	}
	url := "https://example.com/" + org + "/" + name + ".git"
	repo, err := f.s.CreateRepo(ctx, org, url, "main")
	if err != nil {
		t.Fatalf("CreateRepo: %v", err)
	}
	if _, err := f.s.SetRepoUpstream(ctx, RepoUpstream{RepoID: repo.RepoID, Provider: "github", Owner: "acme", Name: name, UpstreamHost: "github.com"}); err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}
	return repo.RepoID, url
}

// schedRow is the full durable lane state: the shared row plus the
// SQLite-only schedule columns.
type schedRow struct {
	Up    RepoUpstream
	Sched localSchedule
}

func (f *schedFixture) rowOf(t *testing.T, repoID string) schedRow {
	t.Helper()
	up, err := f.s.GetRepoUpstream(t.Context(), repoID)
	if err != nil {
		t.Fatalf("GetRepoUpstream: %v", err)
	}
	sched, err := f.s.readLocalSchedule(t.Context(), repoID)
	if err != nil {
		t.Fatalf("readLocalSchedule: %v", err)
	}
	return schedRow{Up: up, Sched: sched}
}

func (f *schedFixture) row(t *testing.T) schedRow { t.Helper(); return f.rowOf(t, f.repo) }

func (f *schedFixture) advance(d time.Duration) { f.now = f.now.Add(d) }

func (f *schedFixture) claimPush(t *testing.T, holder string) LocalClaim {
	t.Helper()
	c, err := f.s.ClaimDuePush(t.Context(), f.repo, holder, schedLease)
	if err != nil {
		t.Fatalf("ClaimDuePush(%s): %v", holder, err)
	}
	return c
}

func (f *schedFixture) claimSync(t *testing.T, holder string) LocalClaim {
	t.Helper()
	c, err := f.s.ClaimDueSync(t.Context(), f.repo, holder, schedLease)
	if err != nil {
		t.Fatalf("ClaimDueSync(%s): %v", holder, err)
	}
	return c
}

func (f *schedFixture) finishPush(t *testing.T, holder string, o PushOutcome) RepoUpstream {
	t.Helper()
	up, err := f.s.FinishPushOutcome(t.Context(), f.repo, holder, o)
	if err != nil {
		t.Fatalf("FinishPushOutcome(%s, %+v): %v", holder, o, err)
	}
	return up
}

// backoff leaves the push lane pending at the given level with a retry due
// after delay: one claimed run fails with OutcomeRetry.
func (f *schedFixture) backoff(t *testing.T, force bool, delay time.Duration) {
	t.Helper()
	if err := f.s.RequestPush(t.Context(), f.repo, force); err != nil {
		t.Fatalf("RequestPush: %v", err)
	}
	if c := f.claimPush(t, "setup"); !c.Claimed {
		t.Fatalf("setup claim refused: %+v", c)
	}
	f.finishPush(t, "setup", PushOutcome{Kind: OutcomeRetry, Force: force, Delay: delay})
}

func TestSQLiteScheduleUpgradeAddsColumns(t *testing.T) {
	f := newSchedFixture(t)
	ctx := t.Context()
	// Simulate a data dir that predates the schedule columns, with pending
	// work already recorded under the old schema.
	for _, col := range []string{"push_attempt", "push_due", "sync_due"} {
		if _, err := f.s.db.ExecContext(ctx, `ALTER TABLE repo_upstreams DROP COLUMN `+col); err != nil {
			t.Fatalf("drop %s: %v", col, err)
		}
	}
	if err := f.s.RequestPush(ctx, f.repo, true); err != nil {
		t.Fatalf("RequestPush on old schema: %v", err)
	}
	if err := f.s.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema upgrade: %v", err)
	}
	got := f.row(t)
	if got.Sched != (localSchedule{}) {
		t.Fatalf("upgraded schedule = %+v, want zero (first attempt, due now)", got.Sched)
	}
	c := f.claimPush(t, "h1")
	if !c.Claimed || !c.Force || c.Attempt != 0 {
		t.Fatalf("claim after upgrade = %+v, want claimed force attempt 0", c)
	}
	// A second InitSchema is a no-op on the upgraded schema.
	if err := f.s.InitSchema(ctx); err != nil {
		t.Fatalf("idempotent InitSchema: %v", err)
	}
}

func TestSQLiteClaimDuePush(t *testing.T) {
	t.Run("idle lane is not work", func(t *testing.T) {
		f := newSchedFixture(t)
		before := f.row(t)
		if c := f.claimPush(t, "h1"); c.Claimed || c.Busy {
			t.Fatalf("claim = %+v, want refused as no work", c)
		}
		if got := f.row(t); got != before {
			t.Fatalf("refused claim wrote the row:\n got %+v\nwant %+v", got, before)
		}
	})
	t.Run("backoff refuses until due", func(t *testing.T) {
		f := newSchedFixture(t)
		f.backoff(t, true, time.Minute)
		f.advance(time.Minute - time.Nanosecond)
		before := f.row(t)
		if c := f.claimPush(t, "h1"); c.Claimed || c.Busy {
			t.Fatalf("pre-due claim = %+v, want refused as not due", c)
		}
		if got := f.row(t); got != before {
			t.Fatalf("refused claim wrote the row")
		}
		f.advance(time.Nanosecond)
		c := f.claimPush(t, "h1")
		if !c.Claimed || !c.Force || c.Attempt != 1 || c.StolenFrom != "" {
			t.Fatalf("due claim = %+v, want claimed force attempt 1", c)
		}
		got := f.row(t)
		if got.Up.PushPending != 0 || !got.Up.PushSyncing || got.Up.PushClaimHolder != "h1" || !got.Up.PushClaimForce {
			t.Fatalf("claimed row = %+v", got.Up)
		}
		if got.Sched.pushAttempt != 1 {
			t.Fatalf("attempt must stay in the row while running: %+v", got.Sched)
		}
	})
	t.Run("live claim is busy", func(t *testing.T) {
		f := newSchedFixture(t)
		if err := f.s.RequestPush(t.Context(), f.repo, false); err != nil {
			t.Fatal(err)
		}
		f.claimPush(t, "h1")
		if err := f.s.RequestPush(t.Context(), f.repo, false); err != nil {
			t.Fatal(err)
		}
		if c := f.claimPush(t, "h2"); c.Claimed || !c.Busy {
			t.Fatalf("claim over live claim = %+v, want busy", c)
		}
	})
	t.Run("expired claim is stolen with attempt and force", func(t *testing.T) {
		f := newSchedFixture(t)
		f.backoff(t, true, time.Minute)
		f.advance(time.Minute)
		f.claimPush(t, "dead")
		f.advance(schedLease)
		c := f.claimPush(t, "thief")
		if !c.Claimed || c.StolenFrom != "dead" || !c.Force || c.Attempt != 1 {
			t.Fatalf("steal = %+v, want claimed from dead, force, attempt 1", c)
		}
	})
	t.Run("missing upstream", func(t *testing.T) {
		f := newSchedFixture(t)
		if _, err := f.s.ClaimDuePush(t.Context(), "repo_000000000000", "h1", schedLease); !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
}

func TestSQLiteRequestPushNow(t *testing.T) {
	f := newSchedFixture(t)
	f.backoff(t, false, 4*time.Minute)
	if err := f.s.RequestPushNow(t.Context(), f.repo, true); err != nil {
		t.Fatalf("RequestPushNow: %v", err)
	}
	got := f.row(t)
	if got.Up.PushPending != 2 || got.Sched.pushDue != 0 || got.Sched.pushAttempt != 1 {
		t.Fatalf("after RequestPushNow row=%+v sched=%+v, want level 2, due now, attempt kept", got.Up, got.Sched)
	}
	if c := f.claimPush(t, "api"); !c.Claimed || c.Attempt != 1 || !c.Force {
		t.Fatalf("expedited claim = %+v", c)
	}
	// A hook request does not expedite: it coalesces into the backoff.
	f2 := newSchedFixture(t)
	f2.backoff(t, false, 4*time.Minute)
	due := f2.row(t).Sched.pushDue
	if err := f2.s.RequestPush(t.Context(), f2.repo, false); err != nil {
		t.Fatal(err)
	}
	if got := f2.row(t).Sched.pushDue; got != due {
		t.Fatalf("RequestPush moved the due time %d -> %d", due, got)
	}
	if err := f.s.RequestPushNow(t.Context(), "repo_000000000000", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing upstream err = %v, want ErrNotFound", err)
	}
}

func TestSQLiteFinishPushOutcome(t *testing.T) {
	start := func(t *testing.T, force bool) *schedFixture {
		f := newSchedFixture(t)
		if err := f.s.RequestPush(t.Context(), f.repo, force); err != nil {
			t.Fatal(err)
		}
		if c := f.claimPush(t, "run"); !c.Claimed {
			t.Fatalf("claim: %+v", c)
		}
		return f
	}
	t.Run("done success with mirror-back and mid-run intent", func(t *testing.T) {
		f := start(t, false)
		if err := f.s.RequestPush(t.Context(), f.repo, false); err != nil {
			t.Fatal(err)
		}
		up := f.finishPush(t, "run", PushOutcome{Kind: OutcomeDone, Success: true, MirrorBack: true})
		got := f.row(t)
		if up != got.Up {
			t.Fatalf("returned row differs from stored row")
		}
		if got.Up.PushSyncing || got.Up.PushClaimHolder != "" || got.Up.PushRunCount != 1 || got.Up.PushSuccessCount != 1 ||
			got.Up.PushPending != 1 || !got.Up.SyncPending || got.Sched != (localSchedule{}) {
			t.Fatalf("done row = %+v sched = %+v", got.Up, got.Sched)
		}
	})
	t.Run("done failure resets position", func(t *testing.T) {
		f := newSchedFixture(t)
		f.backoff(t, false, time.Minute)
		f.advance(time.Minute)
		f.claimPush(t, "run")
		f.finishPush(t, "run", PushOutcome{Kind: OutcomeDone})
		got := f.row(t)
		if got.Up.PushRunCount != 2 || got.Up.PushSuccessCount != 0 || got.Up.SyncPending || got.Sched != (localSchedule{}) {
			t.Fatalf("done-failure row = %+v sched = %+v", got.Up, got.Sched)
		}
	})
	t.Run("retry advances attempt and due, keeps force", func(t *testing.T) {
		f := start(t, true)
		f.finishPush(t, "run", PushOutcome{Kind: OutcomeRetry, Force: true, Delay: time.Minute})
		got := f.row(t)
		wantDue := f.now.Add(time.Minute).UnixNano()
		if got.Up.PushSyncing || got.Up.PushPending != 2 || got.Up.PushRunCount != 1 || got.Up.PushClaimForce ||
			got.Sched.pushAttempt != 1 || got.Sched.pushDue != wantDue {
			t.Fatalf("retry row = %+v sched = %+v, want pending 2 attempt 1 due %d", got.Up, got.Sched, wantDue)
		}
	})
	t.Run("restore keeps attempt and counts no run", func(t *testing.T) {
		f := newSchedFixture(t)
		f.backoff(t, false, time.Minute)
		f.advance(time.Minute)
		f.claimPush(t, "run")
		f.finishPush(t, "run", PushOutcome{Kind: OutcomeRestore, Delay: 10 * time.Second})
		got := f.row(t)
		if got.Up.PushSyncing || got.Up.PushPending != 1 || got.Up.PushRunCount != 1 ||
			got.Sched.pushAttempt != 1 || got.Sched.pushDue != f.now.Add(10*time.Second).UnixNano() {
			t.Fatalf("restore row = %+v sched = %+v", got.Up, got.Sched)
		}
	})
	t.Run("explicit request during the run stays due after retry and restore", func(t *testing.T) {
		for _, kind := range []LaneOutcome{OutcomeRetry, OutcomeRestore} {
			f := start(t, false)
			if err := f.s.RequestPushNow(t.Context(), f.repo, false); err != nil {
				t.Fatal(err)
			}
			f.finishPush(t, "run", PushOutcome{Kind: kind, Delay: 4 * time.Minute})
			if got := f.row(t); got.Sched.pushDue != 0 || got.Up.PushPending != 1 {
				t.Fatalf("%v after mid-run RequestPushNow: row=%+v sched=%+v, want due now", kind, got.Up, got.Sched)
			}
			if c := f.claimPush(t, "api"); !c.Claimed {
				t.Fatalf("%v: explicit request postponed: %+v", kind, c)
			}
		}
		// A hook request during the run joins the backoff instead.
		f := start(t, false)
		if err := f.s.RequestPush(t.Context(), f.repo, false); err != nil {
			t.Fatal(err)
		}
		f.finishPush(t, "run", PushOutcome{Kind: OutcomeRetry, Delay: 4 * time.Minute})
		if got := f.row(t).Sched.pushDue; got != f.now.Add(4*time.Minute).UnixNano() {
			t.Fatalf("hook request during the run: due %d, want the backoff", got)
		}
	})
	t.Run("stolen claim is lost and untouched", func(t *testing.T) {
		f := start(t, false)
		f.advance(schedLease)
		f.claimPush(t, "thief")
		before := f.row(t)
		_, err := f.s.FinishPushOutcome(t.Context(), f.repo, "run", PushOutcome{Kind: OutcomeRetry, Delay: time.Minute})
		if !errors.Is(err, ErrClaimLost) {
			t.Fatalf("err = %v, want ErrClaimLost", err)
		}
		if got := f.row(t); got != before {
			t.Fatalf("lost finish wrote the row")
		}
	})
	t.Run("no run in flight", func(t *testing.T) {
		f := newSchedFixture(t)
		if _, err := f.s.FinishPushOutcome(t.Context(), f.repo, "run", PushOutcome{Kind: OutcomeDone}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
	t.Run("detached mid-run", func(t *testing.T) {
		f := start(t, false)
		if err := f.s.DeleteRepoUpstream(t.Context(), f.repo); err != nil {
			t.Fatal(err)
		}
		if _, err := f.s.FinishPushOutcome(t.Context(), f.repo, "run", PushOutcome{Kind: OutcomeDone}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
}

func TestSQLiteSyncLaneSchedule(t *testing.T) {
	t.Run("claim needs pending and due", func(t *testing.T) {
		f := newSchedFixture(t)
		if c := f.claimSync(t, "h1"); c.Claimed || c.Busy {
			t.Fatalf("idle claim = %+v, want no work", c)
		}
		if err := f.s.RequestSync(t.Context(), f.repo); err != nil {
			t.Fatal(err)
		}
		if c := f.claimSync(t, "h1"); !c.Claimed {
			t.Fatalf("pending claim = %+v", c)
		}
		if err := f.s.RequestSync(t.Context(), f.repo); err != nil {
			t.Fatal(err)
		}
		if c := f.claimSync(t, "h2"); c.Claimed || !c.Busy {
			t.Fatalf("claim over live claim = %+v, want busy", c)
		}
	})
	t.Run("restore re-marks and delays in one write", func(t *testing.T) {
		f := newSchedFixture(t)
		if err := f.s.RequestSync(t.Context(), f.repo); err != nil {
			t.Fatal(err)
		}
		f.claimSync(t, "run")
		if _, err := f.s.FinishSyncOutcome(t.Context(), f.repo, "run", SyncOutcome{Kind: OutcomeRestore, Delay: 10 * time.Second}); err != nil {
			t.Fatal(err)
		}
		got := f.row(t)
		if got.Up.Syncing || !got.Up.SyncPending || got.Up.RunCount != 0 || got.Sched.syncDue != f.now.Add(10*time.Second).UnixNano() {
			t.Fatalf("restore row = %+v sched = %+v", got.Up, got.Sched)
		}
		f.advance(10*time.Second - time.Nanosecond)
		if c := f.claimSync(t, "h2"); c.Claimed || c.Busy {
			t.Fatalf("pre-due claim = %+v", c)
		}
		f.advance(time.Nanosecond)
		if c := f.claimSync(t, "h2"); !c.Claimed {
			t.Fatalf("due claim = %+v", c)
		}
		if _, err := f.s.FinishSyncOutcome(t.Context(), f.repo, "h2", SyncOutcome{Kind: OutcomeDone, Success: true}); err != nil {
			t.Fatal(err)
		}
		got = f.row(t)
		if got.Up.Syncing || got.Up.RunCount != 1 || got.Up.SuccessCount != 1 || got.Sched.syncDue != 0 {
			t.Fatalf("done row = %+v sched = %+v", got.Up, got.Sched)
		}
	})
	t.Run("lost and missing finishes", func(t *testing.T) {
		f := newSchedFixture(t)
		if err := f.s.RequestSync(t.Context(), f.repo); err != nil {
			t.Fatal(err)
		}
		f.claimSync(t, "run")
		f.advance(schedLease)
		if err := f.s.RequestSync(t.Context(), f.repo); err != nil {
			t.Fatal(err)
		}
		if c := f.claimSync(t, "thief"); !c.Claimed || c.StolenFrom != "run" {
			t.Fatalf("steal = %+v", c)
		}
		if _, err := f.s.FinishSyncOutcome(t.Context(), f.repo, "run", SyncOutcome{Kind: OutcomeRestore, Delay: time.Second}); !errors.Is(err, ErrClaimLost) {
			t.Fatalf("err = %v, want ErrClaimLost", err)
		}
		if _, err := f.s.FinishSyncOutcome(t.Context(), f.repo, "thief", SyncOutcome{Kind: OutcomeDone}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.s.FinishSyncOutcome(t.Context(), f.repo, "thief", SyncOutcome{Kind: OutcomeDone}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("second finish err = %v, want ErrNotFound", err)
		}
	})
	t.Run("legacy claim keeps its adoption path", func(t *testing.T) {
		f := newSchedFixture(t)
		if _, err := f.s.db.ExecContext(t.Context(),
			`UPDATE repo_upstreams SET syncing = 1, sync_claim_holder = '', sync_claim_expires_at = 0 WHERE repo_id = ?`, f.repo); err != nil {
			t.Fatal(err)
		}
		if c := f.claimSync(t, "h1"); c.Claimed || !c.Busy {
			t.Fatalf("legacy claim = %+v, want adoption (busy)", c)
		}
		if got := f.row(t); got.Up.SyncClaimHolder != LegacyClaimHolder {
			t.Fatalf("holder = %q, want adoption", got.Up.SyncClaimHolder)
		}
	})
}

func TestSQLiteListLocalWork(t *testing.T) {
	f := newSchedFixture(t)
	ctx := t.Context()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	// f.repo: push due now.
	must(f.s.RequestPush(ctx, f.repo, false))
	backoffRepo, _ := f.addRepo(t, f.org, "backoff")
	pullRepo, _ := f.addRepo(t, f.org, "pull")
	deadRepo, _ := f.addRepo(t, f.org, "dead")
	liveRepo, _ := f.addRepo(t, f.org, "live")
	idleRepo, _ := f.addRepo(t, f.org, "idle")
	otherRepo, _ := f.addRepo(t, "other-org", "other")
	deletedRepo, deletedURL := f.addRepo(t, f.org, "deleted")

	withRepo := func(repo string, fn func()) { saved := f.repo; f.repo = repo; fn(); f.repo = saved }
	must(f.s.RequestSync(ctx, pullRepo))
	withRepo(deadRepo, func() {
		must(f.s.RequestPush(ctx, deadRepo, false))
		f.claimPush(t, "dead")
	})
	f.advance(schedLease) // dead's claim expires
	withRepo(backoffRepo, func() { f.backoff(t, false, time.Minute) })
	withRepo(liveRepo, func() {
		must(f.s.RequestPush(ctx, liveRepo, false))
		f.claimPush(t, "live")
		must(f.s.RequestPush(ctx, liveRepo, false))
	})
	must(f.s.RequestPush(ctx, otherRepo, false))
	must(f.s.RequestPush(ctx, deletedRepo, false))
	if _, err := f.s.SoftDeleteRepo(ctx, f.org, deletedURL); err != nil {
		t.Fatalf("SoftDeleteRepo: %v", err)
	}
	_ = idleRepo

	all := func() []LocalWork {
		t.Helper()
		var out []LocalWork
		after := ""
		for {
			page, err := f.s.ListLocalWork(ctx, f.org, after, 2)
			if err != nil {
				t.Fatalf("ListLocalWork: %v", err)
			}
			if len(page) == 0 {
				return out
			}
			if len(page) > 2 {
				t.Fatalf("page of %d exceeds limit 2", len(page))
			}
			out = append(out, page...)
			after = page[len(page)-1].RepoID
		}
	}
	want := []LocalWork{{RepoID: f.repo, Push: true}, {RepoID: pullRepo, Pull: true}, {RepoID: deadRepo, Push: true}}
	slices.SortFunc(want, func(a, b LocalWork) int {
		if a.RepoID < b.RepoID {
			return -1
		}
		return 1
	})
	if got := all(); !slices.Equal(got, want) {
		t.Fatalf("work = %+v\nwant %+v", got, want)
	}
	f.advance(time.Minute) // backoffRepo's retry becomes due
	got := all()
	if !slices.ContainsFunc(got, func(w LocalWork) bool { return w.RepoID == backoffRepo && w.Push }) {
		t.Fatalf("due backoff missing from %+v", got)
	}
	if work, err := f.s.ListLocalWork(ctx, "unknown-org", "", 16); err != nil || len(work) != 0 {
		t.Fatalf("unknown org = %+v, %v; want no work", work, err)
	}
}

// schedModel is a pure-Go reference for one lane row under the local
// transitions (C12). Times are unix nanoseconds of store time.
type schedModel struct {
	pushPending                   int
	pushSyncing, pushForce        bool
	pushHolder                    string
	pushExpires                   int64
	pushRun, pushSuccess, attempt int
	pushDue                       int64
	syncPending, syncing          bool
	syncHolder                    string
	syncExpires                   int64
	run, success                  int
	syncDue                       int64
}

func (m *schedModel) check(t *testing.T, step int, op string, got schedRow) {
	t.Helper()
	u, sc := got.Up, got.Sched
	mismatch := func(field string, g, w any) {
		t.Fatalf("step %d (%s): %s = %v, model %v", step, op, field, g, w)
	}
	switch {
	case u.PushPending != m.pushPending:
		mismatch("push_pending", u.PushPending, m.pushPending)
	case u.PushSyncing != m.pushSyncing:
		mismatch("push_syncing", u.PushSyncing, m.pushSyncing)
	case u.PushClaimHolder != m.pushHolder:
		mismatch("push_claim_holder", u.PushClaimHolder, m.pushHolder)
	case u.PushClaimExpiresAt.UnixNano() != m.pushExpires:
		mismatch("push_claim_expires_at", u.PushClaimExpiresAt.UnixNano(), m.pushExpires)
	case u.PushClaimForce != m.pushForce:
		mismatch("push_claim_force", u.PushClaimForce, m.pushForce)
	case u.PushRunCount != m.pushRun || u.PushSuccessCount != m.pushSuccess:
		mismatch("push counters", [2]int{u.PushRunCount, u.PushSuccessCount}, [2]int{m.pushRun, m.pushSuccess})
	case sc.pushAttempt != m.attempt:
		mismatch("push_attempt", sc.pushAttempt, m.attempt)
	case sc.pushDue != m.pushDue:
		mismatch("push_due", sc.pushDue, m.pushDue)
	case u.SyncPending != m.syncPending:
		mismatch("sync_pending", u.SyncPending, m.syncPending)
	case u.Syncing != m.syncing:
		mismatch("syncing", u.Syncing, m.syncing)
	case u.SyncClaimHolder != m.syncHolder:
		mismatch("sync_claim_holder", u.SyncClaimHolder, m.syncHolder)
	case u.SyncClaimExpiresAt.UnixNano() != m.syncExpires:
		mismatch("sync_claim_expires_at", u.SyncClaimExpiresAt.UnixNano(), m.syncExpires)
	case u.RunCount != m.run || u.SuccessCount != m.success:
		mismatch("sync counters", [2]int{u.RunCount, u.SuccessCount}, [2]int{m.run, m.success})
	case sc.syncDue != m.syncDue:
		mismatch("sync_due", sc.syncDue, m.syncDue)
	}
}

func TestSQLiteScheduleModel(t *testing.T) {
	for seed := range uint64(8) {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			f := newSchedFixture(t)
			rng := rand.New(rand.NewPCG(seed, 266))
			var m schedModel
			holders := []string{"h1", "h2", "h3"}
			for step := range 400 {
				now := f.now.UnixNano()
				holder := holders[rng.IntN(len(holders))]
				var op string
				switch rng.IntN(10) {
				case 0: // hook request
					force := rng.IntN(3) == 0
					op = fmt.Sprintf("RequestPush(%v)", force)
					if err := f.s.RequestPush(t.Context(), f.repo, force); err != nil {
						t.Fatalf("%s: %v", op, err)
					}
					m.pushPending = max(m.pushPending, level(force))
				case 1: // API request
					force := rng.IntN(3) == 0
					op = fmt.Sprintf("RequestPushNow(%v)", force)
					if err := f.s.RequestPushNow(t.Context(), f.repo, force); err != nil {
						t.Fatalf("%s: %v", op, err)
					}
					m.pushPending, m.pushDue = max(m.pushPending, level(force)), 0
				case 2:
					op = "RequestSync"
					if err := f.s.RequestSync(t.Context(), f.repo); err != nil {
						t.Fatalf("%s: %v", op, err)
					}
					m.syncPending = true
				case 3, 4:
					op = "ClaimDuePush(" + holder + ")"
					c := f.claimPush(t, holder)
					live := m.pushSyncing && m.pushExpires > now
					due := !m.pushSyncing && m.pushPending > 0 && m.pushDue <= now
					expired := m.pushSyncing && m.pushExpires <= now
					switch {
					case live:
						if c.Claimed || !c.Busy {
							t.Fatalf("step %d %s = %+v, model busy", step, op, c)
						}
					case due || expired:
						force := m.pushPending == 2 || (m.pushSyncing && m.pushForce)
						stolen := ""
						if expired {
							stolen = m.pushHolder
						}
						if !c.Claimed || c.Force != force || c.Attempt != m.attempt || c.StolenFrom != stolen {
							t.Fatalf("step %d %s = %+v, model claim force=%v attempt=%d stolen=%q", step, op, c, force, m.attempt, stolen)
						}
						m.pushPending, m.pushSyncing, m.pushHolder, m.pushExpires, m.pushForce = 0, true, holder, now+schedLease.Nanoseconds(), force
						m.pushDue = now // claim time marks "no explicit request during this run"
					default:
						if c.Claimed || c.Busy {
							t.Fatalf("step %d %s = %+v, model no work", step, op, c)
						}
					}
				case 5, 6:
					o := PushOutcome{Kind: LaneOutcome(1 + rng.IntN(3))}
					switch o.Kind {
					case OutcomeDone:
						o.Success = rng.IntN(2) == 0
						o.MirrorBack = o.Success && rng.IntN(2) == 0
					default:
						o.Force, o.Delay = rng.IntN(2) == 0, time.Duration(1+rng.IntN(20))*time.Minute
					}
					op = fmt.Sprintf("FinishPushOutcome(%s, %+v)", holder, o)
					_, err := f.s.FinishPushOutcome(t.Context(), f.repo, holder, o)
					switch {
					case !m.pushSyncing:
						if !errors.Is(err, ErrNotFound) {
							t.Fatalf("step %d %s err = %v, model ErrNotFound", step, op, err)
						}
					case m.pushHolder != holder:
						if !errors.Is(err, ErrClaimLost) {
							t.Fatalf("step %d %s err = %v, model ErrClaimLost", step, op, err)
						}
					default:
						if err != nil {
							t.Fatalf("step %d %s: %v", step, op, err)
						}
						m.pushSyncing, m.pushHolder, m.pushExpires, m.pushForce = false, "", 0, false
						switch o.Kind {
						case OutcomeDone:
							m.pushRun++
							if o.Success {
								m.pushSuccess++
							}
							if o.MirrorBack {
								m.syncPending = true
							}
							m.attempt, m.pushDue = 0, 0
						case OutcomeRetry:
							m.pushRun++
							m.pushPending = max(m.pushPending, level(o.Force))
							m.attempt++
							m.pushDue = retryDue(m.pushDue, now, o.Delay)
						case OutcomeRestore:
							m.pushPending = max(m.pushPending, level(o.Force))
							m.pushDue = retryDue(m.pushDue, now, o.Delay)
						}
					}
				case 7:
					op = "ClaimDueSync(" + holder + ")"
					c := f.claimSync(t, holder)
					live := m.syncing && m.syncExpires > now
					due := !m.syncing && m.syncPending && m.syncDue <= now
					expired := m.syncing && m.syncExpires <= now
					switch {
					case live:
						if c.Claimed || !c.Busy {
							t.Fatalf("step %d %s = %+v, model busy", step, op, c)
						}
					case due || expired:
						stolen := ""
						if expired {
							stolen = m.syncHolder
						}
						if !c.Claimed || c.StolenFrom != stolen {
							t.Fatalf("step %d %s = %+v, model claim stolen=%q", step, op, c, stolen)
						}
						m.syncPending, m.syncing, m.syncHolder, m.syncExpires = false, true, holder, now+schedLease.Nanoseconds()
					default:
						if c.Claimed || c.Busy {
							t.Fatalf("step %d %s = %+v, model no work", step, op, c)
						}
					}
				case 8:
					o := SyncOutcome{Kind: OutcomeDone, Success: rng.IntN(2) == 0}
					if rng.IntN(2) == 0 {
						o = SyncOutcome{Kind: OutcomeRestore, Delay: time.Duration(1+rng.IntN(30)) * time.Second}
					}
					op = fmt.Sprintf("FinishSyncOutcome(%s, %+v)", holder, o)
					_, err := f.s.FinishSyncOutcome(t.Context(), f.repo, holder, o)
					switch {
					case !m.syncing:
						if !errors.Is(err, ErrNotFound) {
							t.Fatalf("step %d %s err = %v, model ErrNotFound", step, op, err)
						}
					case m.syncHolder != holder:
						if !errors.Is(err, ErrClaimLost) {
							t.Fatalf("step %d %s err = %v, model ErrClaimLost", step, op, err)
						}
					default:
						if err != nil {
							t.Fatalf("step %d %s: %v", step, op, err)
						}
						m.syncing, m.syncHolder, m.syncExpires = false, "", 0
						if o.Kind == OutcomeDone {
							m.run++
							if o.Success {
								m.success++
							}
							m.syncDue = 0
						} else {
							m.syncPending, m.syncDue = true, now+o.Delay.Nanoseconds()
						}
					}
				case 9:
					d := time.Duration(rng.IntN(20)) * time.Minute
					op = fmt.Sprintf("advance(%v)", d)
					f.advance(d)
				}
				m.check(t, step, op, f.row(t))
			}
		})
	}
}

func level(force bool) int {
	if force {
		return 2
	}
	return 1
}

// retryDue is the model's due time after a retry or restore: an explicit
// request recorded during the run (due reset to 0) stays due now.
func retryDue(due, now int64, delay time.Duration) int64 {
	if due == 0 {
		return 0
	}
	return now + delay.Nanoseconds()
}
