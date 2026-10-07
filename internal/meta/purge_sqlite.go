// purge_sqlite.go mirrors purge.go's repository-purge metadata seams for
// the SQLite backend (issue #52) — see purge.go for the rationale.
package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// PurgeableRepos — see the PG twin. deleted_at is stored as unixnano.
func (s *SQLite) PurgeableRepos(ctx context.Context, deletedBefore time.Time, after PurgeCandidate, limit int) ([]PurgeCandidate, error) {
	assertf(limit > 0, "PurgeableRepos: non-positive limit %d", limit)
	afterAt, afterID := purgeCursorArgs(after)
	rows, err := s.db.QueryContext(ctx, `
SELECT repo_id, deleted_at FROM repos
WHERE deleted_at IS NOT NULL AND deleted_at <= ? AND purged_at IS NULL
  AND (deleted_at, repo_id) > (?, ?)
ORDER BY deleted_at ASC, repo_id ASC
LIMIT ?`, deletedBefore.UnixNano(), afterAt.UnixNano(), afterID, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []PurgeCandidate
	for rows.Next() {
		var c PurgeCandidate
		var nanos int64
		if err := rows.Scan(&c.RepoID, &nanos); err != nil {
			return nil, err
		}
		c.DeletedAt = time.Unix(0, nanos).UTC()
		out = append(out, c)
	}
	return out, rows.Err()
}

// RepoPurgeState — see the PG twin.
func (s *SQLite) RepoPurgeState(ctx context.Context, repoID string) (state PurgeState, found bool, err error) {
	assertf(repoID != "", "RepoPurgeState: empty repo ID")
	row := s.db.QueryRowContext(ctx,
		`SELECT `+repoCols+`, purged_at FROM repos WHERE repo_id = ?`, repoID)
	var r Repo
	var createdAt int64
	var deletedAt, purgedAt sql.NullInt64
	err = row.Scan(&r.RepoID, &r.OrgID, &r.URL, &r.DefaultBranch, &createdAt, &deletedAt, &r.CreateState, &purgedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return PurgeState{}, false, nil
	}
	if err != nil {
		return PurgeState{}, false, err
	}
	// Unixnano → UTC time.Time, matching scanRepoSQLite's Repo contract.
	r.CreatedAt = time.Unix(0, createdAt).UTC()
	if deletedAt.Valid {
		utc := time.Unix(0, deletedAt.Int64).UTC()
		r.DeletedAt = &utc
	}
	state = PurgeState{Repo: r}
	if purgedAt.Valid {
		utc := time.Unix(0, purgedAt.Int64).UTC()
		state.PurgedAt = &utc
	}
	return state, true, nil
}

// PurgeRepoMeta — see the PG twin. The inTx write transaction (BEGIN
// IMMEDIATE) serializes the guard read with the deletes.
func (s *SQLite) PurgeRepoMeta(ctx context.Context, repoID string) error {
	assertf(repoID != "", "PurgeRepoMeta: empty repo ID")
	nanos, _ := s.nowNano()
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		var deletedAt sql.NullInt64
		err := tx.QueryRowContext(ctx,
			`SELECT deleted_at FROM repos WHERE repo_id = ?`, repoID).Scan(&deletedAt)
		if errors.Is(err, sql.ErrNoRows) {
			return nil // row gone (legacy purge): nothing left to finalize
		}
		if err != nil {
			return err
		}
		if !deletedAt.Valid {
			return ErrRepoNotDeleted
		}
		for _, stmt := range []string{
			`DELETE FROM git_credentials WHERE repo_id = ?`,
			`DELETE FROM repo_upstreams  WHERE repo_id = ?`,
		} {
			if _, err := tx.ExecContext(ctx, stmt, repoID); err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(ctx, `UPDATE repos SET purged_at = ?
		  WHERE repo_id = ? AND deleted_at IS NOT NULL AND purged_at IS NULL`, nanos, repoID)
		return err
	})
	if err != nil {
		return fmt.Errorf("purge repo %s metadata: %w", repoID, err)
	}
	return nil
}
