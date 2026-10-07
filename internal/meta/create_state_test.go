package meta

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"testing"
)

func TestCreateState(t *testing.T) { runBackends(t, testCreateState) }

// testCreateState: the create-state marker (issue #388). Only a pending
// row completes, a delete before completion wins, legacy rows are never
// completed, and concurrent completions have one winner. S3-WAL stores no
// state (frozen v1 format, issue #335): every row reads legacy, and
// CompleteRepoCreate reports only deleted or completed.
func testCreateState(t *testing.T, s Backend) {
	ctx := t.Context()
	org := seedOrg(t, s)
	_, untracked := s.(*S3WAL)
	tracked := func(st CreateState) CreateState {
		if untracked {
			return CreateStateLegacy
		}
		return st
	}
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
	if untracked {
		complete(t, legacy.RepoID, CreateCompleted)
	} else if got, err := s.CompleteRepoCreate(ctx, org, legacy.RepoID); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("CompleteRepoCreate(legacy) = %v, %v; want a not-pending error", got, err)
	}
	if got := state(t, "legacy"); got != CreateStateLegacy {
		t.Fatalf("legacy row state after complete = %d, want legacy", got)
	}

	done, err := s.CreateRepoWithState(ctx, org, "done", "main", CreateStateComplete)
	if err != nil || state(t, "done") != tracked(CreateStateComplete) {
		t.Fatalf("CreateRepoWithState(complete) = %+v, %v; state %d", done, err, state(t, "done"))
	}
	complete(t, done.RepoID, map[bool]CreateOutcome{false: CreateAlreadyComplete, true: CreateCompleted}[untracked])

	pend, err := s.CreateRepoWithState(ctx, org, "pend", "main", CreateStatePending)
	if err != nil || state(t, "pend") != tracked(CreateStatePending) {
		t.Fatalf("CreateRepoWithState(pending) = %+v, %v; state %d", pend, err, state(t, "pend"))
	}
	list, err := s.ListRepos(ctx, org, ListParams{Q: "pend", Limit: 10})
	if err != nil || len(list.Repos) != 1 || list.Repos[0].CreateState != tracked(CreateStatePending) {
		t.Fatalf("ListRepos = %+v, %v; want the pending row", list, err)
	}
	complete(t, pend.RepoID, CreateCompleted)
	complete(t, pend.RepoID, map[bool]CreateOutcome{false: CreateAlreadyComplete, true: CreateCompleted}[untracked])
	if r, err := s.GetRepoByID(ctx, org, pend.RepoID); err != nil || r.CreateState != tracked(CreateStateComplete) {
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
	if err != nil || state(t, "linked") != tracked(CreateStatePending) {
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
	if untracked {
		want = map[CreateOutcome]int{CreateCompleted: n}
	}
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
