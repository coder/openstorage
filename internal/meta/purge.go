// purge.go implements the repository-purge metadata seams (issue #52). The
// purge orchestrator (jobs.RepoPurger) permanently reclaims soft-deleted
// repositories; the repos row is the durable tombstone that drives crash
// resumption, so PurgeRepoMeta — which deletes it — is the purge's
// COMPLETION commit point and runs strictly last. Deleting the row also
// releases the burned (org, url) name (docs/explanation/domain-model.md):
// restore after purge is impossible by construction.
//
// Like the ref-store purge seams (ref/purge.go), these live on the
// concrete backends rather than meta.Store, whose test mirrors must not
// grow with purge-only surface.
package meta

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrRepoNotDeleted guards PurgeRepoMeta: the repos row still exists and is
// live, so purging its metadata would orphan a serving repository. Callers
// verified soft-deletion before starting — hitting this means the check was
// skipped or the repo was re-created mid-purge.
var ErrRepoNotDeleted = errors.New("repository is not soft-deleted")

// PurgeableRepos returns up to limit repo IDs soft-deleted at or before
// deletedBefore, oldest deletion first — the background purge pass's work
// queue. Limit bounds one pass; completed purges leave the eligible set, so
// the next tick continues where this one stopped.
func (s *PG) PurgeableRepos(ctx context.Context, deletedBefore time.Time, limit int) ([]string, error) {
	assertf(limit > 0, "PurgeableRepos: non-positive limit %d", limit)
	rows, err := s.pool.Query(ctx, `
SELECT repo_id FROM repos
WHERE deleted_at IS NOT NULL AND deleted_at <= $1
ORDER BY deleted_at ASC, repo_id ASC
LIMIT $2`, deletedBefore, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// RepoPurgeState returns the repo row for purge eligibility checks:
// found=false when no row exists (already purged or never created);
// repo.DeletedAt==nil means the repo is live and must not be purged.
// Keyed by repo_id alone — purging is a fleet-wide operator action, not an
// org-scoped API call.
func (s *PG) RepoPurgeState(ctx context.Context, repoID string) (repo Repo, found bool, err error) {
	assertf(repoID != "", "RepoPurgeState: empty repo ID")
	r, err := scanRepo(s.pool.QueryRow(ctx,
		`SELECT `+repoCols+` FROM repos WHERE repo_id = $1`, repoID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Repo{}, false, nil
	}
	if err != nil {
		return Repo{}, false, err
	}
	return r, true, nil
}

// PurgeRepoMeta deletes the repo's metadata rows — git_credentials,
// repo_upstreams, and the repos row itself — in one transaction, guarded on
// the row still being soft-deleted. A missing row is an idempotent no-op
// (a resumed purge that already completed); a live row aborts with
// ErrRepoNotDeleted before anything is touched.
func (s *PG) PurgeRepoMeta(ctx context.Context, repoID string) error {
	assertf(repoID != "", "PurgeRepoMeta: empty repo ID")
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var deletedAt *time.Time
		err := tx.QueryRow(ctx,
			`SELECT deleted_at FROM repos WHERE repo_id = $1 FOR UPDATE`, repoID).Scan(&deletedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // already purged: the completion commit is durable
		}
		if err != nil {
			return err
		}
		if deletedAt == nil {
			return ErrRepoNotDeleted
		}
		for _, stmt := range []string{
			`DELETE FROM git_credentials WHERE repo_id = $1`,
			`DELETE FROM repo_upstreams  WHERE repo_id = $1`,
			`DELETE FROM repos           WHERE repo_id = $1 AND deleted_at IS NOT NULL`,
		} {
			if _, err := tx.Exec(ctx, stmt, repoID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("purge repo %s metadata: %w", repoID, err)
	}
	return nil
}
