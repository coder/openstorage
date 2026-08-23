// Repository-purge metadata seam tests (issue #52): eligibility listing,
// state reads, and the guarded completion delete must behave identically on
// both backends. Bodies fan out via runBackends.
package meta

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

// purgeMetaStore is the concrete-backend purge surface under test
// (jobs.RepoPurgeMeta mirrors this).
type purgeMetaStore interface {
	Backend
	PurgeableRepos(ctx context.Context, deletedBefore time.Time, limit int) ([]string, error)
	RepoPurgeState(ctx context.Context, repoID string) (PurgeState, bool, error)
	PurgeRepoMeta(ctx context.Context, repoID string) error
}

func TestRepoPurgeMetaSeams(t *testing.T) { runBackends(t, testRepoPurgeMetaSeams) }

func testRepoPurgeMetaSeams(t *testing.T, s Backend) {
	ps, ok := s.(purgeMetaStore)
	if !ok {
		t.Fatalf("store %T lacks purge seams", s)
	}
	ctx := context.Background()
	org := seedOrg(t, s)

	live, err := s.CreateRepo(ctx, org, "https://example.com/live.git", "main")
	if err != nil {
		t.Fatalf("CreateRepo(live): %v", err)
	}
	delA, err := s.CreateRepo(ctx, org, "https://example.com/del-a.git", "main")
	if err != nil {
		t.Fatalf("CreateRepo(del-a): %v", err)
	}
	delB, err := s.CreateRepo(ctx, org, "https://example.com/del-b.git", "main")
	if err != nil {
		t.Fatalf("CreateRepo(del-b): %v", err)
	}
	if _, err := s.SoftDeleteRepo(ctx, org, delA.URL); err != nil {
		t.Fatalf("SoftDeleteRepo(del-a): %v", err)
	}
	if _, err := s.SoftDeleteRepo(ctx, org, delB.URL); err != nil {
		t.Fatalf("SoftDeleteRepo(del-b): %v", err)
	}
	// Eventing rows the completion delete must take with it.
	if _, err := s.CreateGitCredential(ctx, GitCredential{RepoID: delA.RepoID, Username: "u", Password: "p"}); err != nil {
		t.Fatalf("CreateGitCredential: %v", err)
	}
	if _, err := s.SetRepoUpstream(ctx, RepoUpstream{RepoID: delA.RepoID, Provider: "github", Owner: "o", Name: "n", UpstreamHost: "github.com"}); err != nil {
		t.Fatalf("SetRepoUpstream: %v", err)
	}

	// PurgeableRepos: a future cutoff sees both soft-deleted repos (never
	// the live one); a past cutoff sees none (within-retention repos are
	// untouched); limit=1 returns exactly the oldest deletion.
	ids, err := ps.PurgeableRepos(ctx, time.Now().Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("PurgeableRepos(future): %v", err)
	}
	if !slices.Contains(ids, delA.RepoID) || !slices.Contains(ids, delB.RepoID) {
		t.Fatalf("PurgeableRepos(future) = %v, want both %s and %s", ids, delA.RepoID, delB.RepoID)
	}
	if slices.Contains(ids, live.RepoID) {
		t.Fatalf("PurgeableRepos listed the live repo: %v", ids)
	}
	if ids, err := ps.PurgeableRepos(ctx, time.Now().Add(-time.Hour), 10); err != nil || len(ids) != 0 {
		t.Fatalf("PurgeableRepos(past) = %v err=%v, want empty", ids, err)
	}
	if ids, err := ps.PurgeableRepos(ctx, time.Now().Add(time.Hour), 1); err != nil || len(ids) != 1 {
		t.Fatalf("PurgeableRepos(limit 1) = %v err=%v, want exactly one", ids, err)
	}

	// RepoPurgeState: live vs deleted vs absent.
	if s, found, err := ps.RepoPurgeState(ctx, live.RepoID); err != nil || !found || s.Repo.DeletedAt != nil || s.PurgedAt != nil {
		t.Fatalf("RepoPurgeState(live) = %+v found=%v err=%v", s, found, err)
	}
	if s, found, err := ps.RepoPurgeState(ctx, delA.RepoID); err != nil || !found || s.Repo.DeletedAt == nil || s.Repo.URL != delA.URL || s.PurgedAt != nil {
		t.Fatalf("RepoPurgeState(del-a) = %+v found=%v err=%v", s, found, err)
	}
	if _, found, err := ps.RepoPurgeState(ctx, "repo_nonexistent0"); err != nil || found {
		t.Fatalf("RepoPurgeState(absent): found=%v err=%v", found, err)
	}

	// PurgeRepoMeta refuses live repos and touches nothing.
	if err := ps.PurgeRepoMeta(ctx, live.RepoID); !errors.Is(err, ErrRepoNotDeleted) {
		t.Fatalf("PurgeRepoMeta(live) = %v, want ErrRepoNotDeleted", err)
	}
	if _, err := s.GetRepoByID(ctx, org, live.RepoID); err != nil {
		t.Fatalf("live repo damaged by refused purge: %v", err)
	}

	// The completion commit removes the eventing rows and stamps the
	// purge tombstone; the repos row is KEPT and the burned name is never
	// released (URL-scoped tokens may outlive any retention — Codex
	// security round on #105). Idempotent on rerun.
	if err := ps.PurgeRepoMeta(ctx, delA.RepoID); err != nil {
		t.Fatalf("PurgeRepoMeta(del-a): %v", err)
	}
	stateA, found, err := ps.RepoPurgeState(ctx, delA.RepoID)
	if err != nil || !found {
		t.Fatalf("repos tombstone row lost by purge: found=%v err=%v", found, err)
	}
	if stateA.PurgedAt == nil || stateA.Repo.DeletedAt == nil {
		t.Fatalf("purge tombstone not stamped: %+v", stateA)
	}
	if _, err := s.GetRepoUpstream(ctx, delA.RepoID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("repo_upstreams row survived purge: %v", err)
	}
	if _, err := s.CreateRepo(ctx, org, delA.URL, "main"); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("purge released the burned name: CreateRepo = %v, want ErrAlreadyExists", err)
	}
	if err := ps.PurgeRepoMeta(ctx, delA.RepoID); err != nil {
		t.Fatalf("PurgeRepoMeta rerun: %v", err)
	}
	// Purged rows leave the background queue permanently.
	ids, err = ps.PurgeableRepos(ctx, time.Now().Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("PurgeableRepos after purge: %v", err)
	}
	if slices.Contains(ids, delA.RepoID) {
		t.Fatalf("purged repo still purgeable: %v", ids)
	}
	// The other soft-deleted repo is untouched by delA's purge.
	if !slices.Contains(ids, delB.RepoID) {
		t.Fatalf("del-b lost from the purge queue: %v", ids)
	}
}
