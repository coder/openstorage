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
func (s *SQLite) PurgeableRepos(ctx context.Context, deletedBefore time.Time, limit int) ([]string, error) {
	assertf(limit > 0, "PurgeableRepos: non-positive limit %d", limit)
	rows, err := s.db.QueryContext(ctx, `
SELECT repo_id FROM repos
WHERE deleted_at IS NOT NULL AND deleted_at <= ?
ORDER BY deleted_at ASC, repo_id ASC
LIMIT ?`, deletedBefore.UnixNano(), limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
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

// RepoPurgeState — see the PG twin.
func (s *SQLite) RepoPurgeState(ctx context.Context, repoID string) (repo Repo, found bool, err error) {
	assertf(repoID != "", "RepoPurgeState: empty repo ID")
	r, err := scanRepoSQLite(s.db.QueryRowContext(ctx,
		`SELECT `+repoCols+` FROM repos WHERE repo_id = ?`, repoID))
	if errors.Is(err, sql.ErrNoRows) {
		return Repo{}, false, nil
	}
	if err != nil {
		return Repo{}, false, err
	}
	return r, true, nil
}

// PurgeRepoMeta — see the PG twin. The inTx write transaction (BEGIN
// IMMEDIATE) serializes the guard read with the deletes.
func (s *SQLite) PurgeRepoMeta(ctx context.Context, repoID string) error {
	assertf(repoID != "", "PurgeRepoMeta: empty repo ID")
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		var deletedAt sql.NullInt64
		err := tx.QueryRowContext(ctx,
			`SELECT deleted_at FROM repos WHERE repo_id = ?`, repoID).Scan(&deletedAt)
		if errors.Is(err, sql.ErrNoRows) {
			return nil // already purged: the completion commit is durable
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
			`DELETE FROM repos           WHERE repo_id = ? AND deleted_at IS NOT NULL`,
		} {
			if _, err := tx.ExecContext(ctx, stmt, repoID); err != nil {
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
