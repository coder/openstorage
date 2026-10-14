package meta

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestCreateState(t *testing.T) { runBackends(t, testCreateState) }

// testCreateState: the create-state marker (issue #388). Only a pending
// row completes, a delete before completion wins, legacy rows are never
// completed, and concurrent completions have one winner. S3-WAL stores the
// state in its v2 document (ADR-51).
func testCreateState(t *testing.T, s Backend) {
	ctx := t.Context()
	org := seedOrg(t, s)
	state := func(t *testing.T, name string) CreateState {
		t.Helper()
		r, err := s.GetRepoByURL(ctx, org, name)
		if err != nil {
			t.Fatalf("GetRepoByURL(%s): %v", name, err)
		}
		return r.CreateState
	}
	complete := func(t *testing.T, repoID string, want CreateOutcome) {
		t.Helper()
		if got, err := s.CompleteRepoCreate(ctx, org, repoID); err != nil || got != want {
			t.Fatalf("CompleteRepoCreate(%s) = %v, %v; want %v", repoID, got, err, want)
		}
	}

	legacy, err := s.CreateRepo(ctx, org, "legacy", "main")
	if err != nil || legacy.CreateState != CreateStateLegacy || state(t, "legacy") != CreateStateLegacy {
		t.Fatalf("CreateRepo = %+v, %v; want a legacy row", legacy, err)
	}
	if got, err := s.CompleteRepoCreate(ctx, org, legacy.RepoID); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("CompleteRepoCreate(legacy) = %v, %v; want a not-pending error", got, err)
	}
	if got := state(t, "legacy"); got != CreateStateLegacy {
		t.Fatalf("legacy row state after complete = %d, want legacy", got)
	}

	done, err := s.CreateRepoWithState(ctx, org, "done", "main", CreateStateComplete)
	if err != nil || state(t, "done") != CreateStateComplete {
		t.Fatalf("CreateRepoWithState(complete) = %+v, %v; state %d", done, err, state(t, "done"))
	}
	complete(t, done.RepoID, CreateAlreadyComplete)

	pend, err := s.CreateRepoWithState(ctx, org, "pend", "main", CreateStatePending)
	if err != nil || state(t, "pend") != CreateStatePending {
		t.Fatalf("CreateRepoWithState(pending) = %+v, %v; state %d", pend, err, state(t, "pend"))
	}
	list, err := s.ListRepos(ctx, org, ListParams{Q: "pend", Limit: 10})
	if err != nil || len(list.Repos) != 1 || list.Repos[0].CreateState != CreateStatePending {
		t.Fatalf("ListRepos = %+v, %v; want the pending row", list, err)
	}
	complete(t, pend.RepoID, CreateCompleted)
	complete(t, pend.RepoID, CreateAlreadyComplete)
	if r, err := s.GetRepoByID(ctx, org, pend.RepoID); err != nil || r.CreateState != CreateStateComplete {
		t.Fatalf("GetRepoByID after complete = %+v, %v", r, err)
	}

	// A delete before completion wins: the create must not report success.
	for _, st := range []CreateState{CreateStatePending, CreateStateComplete} {
		name := "gone-" + map[CreateState]string{CreateStatePending: "pending", CreateStateComplete: "complete"}[st]
		r, err := s.CreateRepoWithState(ctx, org, name, "main", st)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.SoftDeleteRepo(ctx, org, name); err != nil {
			t.Fatal(err)
		}
		complete(t, r.RepoID, CreateDeleted)
		if _, err := s.GetRepoByURL(ctx, org, name); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s: deleted repo is live again: %v", name, err)
		}
	}

	if _, err := s.CompleteRepoCreate(ctx, org, "repo_000000000000"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CompleteRepoCreate(unknown) err = %v, want ErrNotFound", err)
	}
	linked, err := s.CreateLinkedRepo(ctx, org, "linked", "main",
		RepoUpstream{Provider: "github", Owner: "o", Name: "n", UpstreamHost: "github.com"}, nil)
	if err != nil || linked.CreateState != CreateStatePending || state(t, "linked") != CreateStatePending {
		t.Fatalf("CreateLinkedRepo = %+v, %v; state %d, want pending", linked, err, state(t, "linked"))
	}

	// Concurrent completions of one pending row: exactly one finishes it.
	race, err := s.CreateRepoWithState(ctx, org, "race", "main", CreateStatePending)
	if err != nil {
		t.Fatal(err)
	}
	const n = 8
	outcomes := make(chan CreateOutcome, n)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			got, err := s.CompleteRepoCreate(ctx, org, race.RepoID)
			if err != nil {
				t.Errorf("concurrent CompleteRepoCreate: %v", err)
			}
			outcomes <- got
		})
	}
	wg.Wait()
	close(outcomes)
	count := map[CreateOutcome]int{}
	for o := range outcomes {
		count[o]++
	}
	want := map[CreateOutcome]int{CreateCompleted: 1, CreateAlreadyComplete: n - 1}
	if len(count) != len(want) || count[CreateCompleted] != want[CreateCompleted] || count[CreateAlreadyComplete] != want[CreateAlreadyComplete] {
		t.Fatalf("concurrent outcomes = %v, want %v", count, want)
	}
}

// TestCreateStateUpgrade: a database that predates create_state gains the
// column on InitSchema, its rows read legacy (never complete), and a
// second InitSchema is a no-op.
func TestCreateStateUpgrade(t *testing.T) {
	t.Run("pg", func(t *testing.T) {
		pgBackend(t) // boots the shared container and sets pgTestURL
		ctx := t.Context()
		admin := pgStore
		if _, err := admin.pool.Exec(ctx, `CREATE DATABASE create_state_upgrade`); err != nil {
			t.Fatal(err)
		}
		u, err := url.Parse(pgTestURL)
		if err != nil {
			t.Fatal(err)
		}
		u.Path = "/create_state_upgrade"
		s, err := OpenPG(ctx, u.String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		upgradeCreateState(t, s, s.InitSchema, func(q string, args ...any) error {
			_, err := s.pool.Exec(ctx, q, args...)
			return err
		})
	})
	t.Run("sqlite", func(t *testing.T) {
		s, _ := openTestSQLite(t)
		upgradeCreateState(t, s, s.InitSchema, func(q string, args ...any) error {
			_, err := s.db.ExecContext(t.Context(), q, args...)
			return err
		})
	})
}

func upgradeCreateState(t *testing.T, s Backend, initSchema func(context.Context) error, exec func(string, ...any) error) {
	ctx := t.Context()
	if err := initSchema(ctx); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	org := seedOrg(t, s)
	if err := exec(`ALTER TABLE repos DROP COLUMN create_state`); err != nil {
		t.Fatalf("drop create_state: %v", err)
	}
	// A row written by an old binary, under the old schema.
	if err := exec(`INSERT INTO repos (repo_id, org_id, url, default_branch, created_at)
		SELECT 'repo_0123456789ab', id, 'old', 'main', ` + map[bool]string{true: "now()", false: "0"}[isPG(s)] + ` FROM orgs WHERE name = '` + org + `'`); err != nil {
		t.Fatalf("old-schema insert: %v", err)
	}
	for range 2 { // the second run checks idempotency
		if err := initSchema(ctx); err != nil {
			t.Fatalf("InitSchema upgrade: %v", err)
		}
	}
	r, err := s.GetRepoByURL(ctx, org, "old")
	if err != nil || r.CreateState != CreateStateLegacy {
		t.Fatalf("upgraded row = %+v, %v; want legacy", r, err)
	}
	if _, err := s.CreateRepoWithState(ctx, org, "new", "main", CreateStatePending); err != nil {
		t.Fatalf("pending create after upgrade: %v", err)
	}
}

func isPG(s Backend) bool { _, ok := s.(*PG); return ok }

func TestCompleteRepoCreateSchedules(t *testing.T) {
	runBackends(t, testCompleteRepoCreateSchedules)
}

// testCompleteRepoCreateSchedules: the first-sync request commits with the
// complete mark, or neither does (issue #388). PG runs the caller's
// schedule inside the locked transaction; SQLite records sync_pending.
// Unknown create_state values (a newer binary's) read legacy.
func testCompleteRepoCreateSchedules(t *testing.T, s Backend) {
	ctx := t.Context()
	org := seedOrg(t, s)
	up := RepoUpstream{Provider: "github", Owner: "o", Name: "n", UpstreamHost: "github.com"}
	// complete runs the backend's sync completion; calls counts schedule
	// steps (PG) or newly recorded first-sync requests (SQLite).
	var complete func(repoID string, fail bool) (CreateOutcome, int, error)
	var exec func(q string, args ...any)
	switch b := s.(type) {
	case *PG:
		exec = func(q string, args ...any) {
			if _, err := b.pool.Exec(ctx, strings.ReplaceAll(q, "?", "$1"), args...); err != nil {
				t.Fatal(err)
			}
		}
		complete = func(repoID string, fail bool) (out CreateOutcome, calls int, err error) {
			err = pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
				out, err = PGCompleteRepoCreate(ctx, tx, org, repoID, func(context.Context) error {
					calls++
					if fail {
						return errors.New("injected schedule failure")
					}
					return nil
				})
				return err
			})
			return out, calls, err
		}
	case *SQLite:
		exec = func(q string, args ...any) {
			if _, err := b.db.ExecContext(ctx, q, args...); err != nil {
				t.Fatal(err)
			}
		}
		complete = func(repoID string, fail bool) (CreateOutcome, int, error) {
			if fail { // a failing write: the upstream row is gone
				exec(`DELETE FROM repo_upstreams WHERE repo_id = ?`, repoID)
			}
			out, err := b.CompleteSyncRepoCreate(ctx, org, repoID)
			var n int
			_ = b.db.QueryRowContext(ctx, `SELECT count(*) FROM repo_upstreams WHERE repo_id = ? AND sync_pending = 1`, repoID).Scan(&n)
			return out, n, err
		}
	default:
		t.Fatalf("unexpected backend %T", s)
	}

	r, err := s.CreateLinkedRepo(ctx, org, "first", "main", up, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out, calls, err := complete(r.RepoID, false); err != nil || out != CreateCompleted || calls != 1 {
		t.Fatalf("pending: %v, %d calls, %v; want completed with one schedule", out, calls, err)
	}
	// The first sync ran; a repeat sees complete and schedules nothing.
	exec(`UPDATE repo_upstreams SET sync_pending = false WHERE repo_id = ?`, r.RepoID)
	if out, calls, err := complete(r.RepoID, false); err != nil || out != CreateAlreadyComplete || calls != 0 {
		t.Fatalf("complete: %v, %d calls, %v; want already complete, nothing scheduled", out, calls, err)
	}

	failing, err := s.CreateLinkedRepo(ctx, org, "failing", "main", up, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := complete(failing.RepoID, true); err == nil {
		t.Fatal("a failing schedule must fail the completion")
	}
	if got, err := s.GetRepoByID(ctx, org, failing.RepoID); err != nil || got.CreateState != CreateStatePending {
		t.Fatalf("after a failed schedule: %+v, %v; want still pending", got, err)
	}

	gone, err := s.CreateLinkedRepo(ctx, org, "gone", "main", up, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SoftDeleteRepo(ctx, org, "gone"); err != nil {
		t.Fatal(err)
	}
	if out, calls, err := complete(gone.RepoID, false); err != nil || out != CreateDeleted || calls != 0 {
		t.Fatalf("deleted: %v, %d calls, %v; want deleted, nothing scheduled", out, calls, err)
	}

	// A downgrade can leave a newer binary's state value: read it as legacy.
	exec(`UPDATE repos SET create_state = 7 WHERE repo_id = ?`, r.RepoID)
	if got, err := s.GetRepoByID(ctx, org, r.RepoID); err != nil || got.CreateState != CreateStateLegacy {
		t.Fatalf("unknown create_state read = %+v, %v; want legacy", got, err)
	}
}
