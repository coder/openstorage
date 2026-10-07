// purge.go implements the repository-purge metadata seams (issue #52). The
// purge orchestrator (jobs.RepoPurger) permanently reclaims soft-deleted
// repositories; the soft-deleted repos row is the durable marker that
// drives crash resumption, so PurgeRepoMeta — which stamps purged_at — is
// the purge's COMPLETION commit point and runs strictly last.
//
// The repos row itself is KEPT forever as a name tombstone: JWT repo
// claims are URL-scoped and may outlive any retention window (git tokens
// last up to a year), so releasing the burned (org, url) name would let a
// stale token authorize against a successor repository created at that
// URL (Codex security round on #105). Purging therefore reclaims storage
// and eventing rows but never un-burns the name, matching soft-delete's
// name semantics; restore after purge is impossible by construction.
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

// PurgeState is one repos row as the purge sees it: the row plus the
// purge-completion tombstone timestamp (nil until a purge completed).
type PurgeState struct {
	Repo     Repo
	PurgedAt *time.Time
}

// PurgeCandidate is one purge-queue entry: the repo plus the keyset
// position its deletion time defines. The zero value doubles as the
// start-of-queue cursor for PurgeableRepos.
type PurgeCandidate struct {
	RepoID    string
	DeletedAt time.Time
}

// purgeCursorArgs normalizes an `after` cursor for the keyset comparison:
// the zero cursor becomes (epoch, ""), strictly below every real deletion
// timestamp on both backends.
func purgeCursorArgs(after PurgeCandidate) (time.Time, string) {
	if after.DeletedAt.IsZero() {
		return time.Unix(0, 0).UTC(), ""
	}
	return after.DeletedAt, after.RepoID
}

// PurgeableRepos returns up to limit repos soft-deleted at or before
// deletedBefore and not yet purged, ordered by (deleted_at, repo_id)
// ascending and strictly after the `after` cursor — the background purge
// pass's rotating work queue (PR #105 round 9): a batch of persistently
// failing repos stays in the eligible set, so the caller carries the last
// attempted candidate as the cursor and wraps on exhaustion; without the
// cursor, oldest-first alone would let such a batch monopolize every pass
// and starve the rest of the fleet.
func (s *PG) PurgeableRepos(ctx context.Context, deletedBefore time.Time, after PurgeCandidate, limit int) ([]PurgeCandidate, error) {
	assertf(limit > 0, "PurgeableRepos: non-positive limit %d", limit)
	afterAt, afterID := purgeCursorArgs(after)
	rows, err := s.pool.Query(ctx, `
SELECT repo_id, deleted_at FROM repos
WHERE deleted_at IS NOT NULL AND deleted_at <= $1 AND purged_at IS NULL
  AND (deleted_at, repo_id) > ($2, $3)
ORDER BY deleted_at ASC, repo_id ASC
LIMIT $4`, deletedBefore, afterAt, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PurgeCandidate
	for rows.Next() {
		var c PurgeCandidate
		if err := rows.Scan(&c.RepoID, &c.DeletedAt); err != nil {
			return nil, err
		}
		c.DeletedAt = c.DeletedAt.UTC()
		out = append(out, c)
	}
	return out, rows.Err()
}

// RepoPurgeState returns the repo row for purge eligibility checks:
// found=false when no row exists; Repo.DeletedAt==nil means the repo is
// live and must not be purged; PurgedAt non-nil means a purge already
// completed. Keyed by repo_id alone — purging is a fleet-wide operator
// action, not an org-scoped API call.
func (s *PG) RepoPurgeState(ctx context.Context, repoID string) (state PurgeState, found bool, err error) {
	assertf(repoID != "", "RepoPurgeState: empty repo ID")
	row := s.pool.QueryRow(ctx,
		`SELECT `+repoCols+`, purged_at FROM repos WHERE repo_id = $1`, repoID)
	var r Repo
	var purgedAt *time.Time
	err = row.Scan(&r.RepoID, &r.OrgID, &r.URL, &r.DefaultBranch, &r.CreatedAt, &r.DeletedAt, &r.CreateState, &purgedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return PurgeState{}, false, nil
	}
	if err != nil {
		return PurgeState{}, false, err
	}
	// Timestamps normalized to UTC, matching scanRepo's Repo contract.
	r.CreatedAt = r.CreatedAt.UTC()
	if r.DeletedAt != nil {
		utc := r.DeletedAt.UTC()
		r.DeletedAt = &utc
	}
	if purgedAt != nil {
		utc := purgedAt.UTC()
		purgedAt = &utc
	}
	return PurgeState{Repo: r, PurgedAt: purgedAt}, true, nil
}

// PurgeRepoMeta finalizes a purge in one transaction: deletes the repo's
// git_credentials and repo_upstreams rows and stamps the repos row's
// purged_at tombstone, guarded on the row still being soft-deleted. The
// row itself is retained forever (see the package comment — the name stays
// burned). A missing or already-stamped row is an idempotent no-op; a live
// row aborts with ErrRepoNotDeleted before anything is touched.
func (s *PG) PurgeRepoMeta(ctx context.Context, repoID string) error {
	assertf(repoID != "", "PurgeRepoMeta: empty repo ID")
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var deletedAt *time.Time
		err := tx.QueryRow(ctx,
			`SELECT deleted_at FROM repos WHERE repo_id = $1 FOR UPDATE`, repoID).Scan(&deletedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // row gone (legacy purge): nothing left to finalize
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
			`UPDATE repos SET purged_at = now()
			  WHERE repo_id = $1 AND deleted_at IS NOT NULL AND purged_at IS NULL`,
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
