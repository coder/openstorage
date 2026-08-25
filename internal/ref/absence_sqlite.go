// absence_sqlite.go mirrors absence.go's fleet-wide registry paging for
// the SQLite backend (issue #101) — see absence.go for the rationale.
package ref

import (
	"context"
	"fmt"
)

// ListFleetLFSObjects — see the PG twin.
func (s *SQLite) ListFleetLFSObjects(ctx context.Context, afterRepo, afterKey string, limit int) ([]AbsenceLFSRow, error) {
	validateAbsencePage(limit)
	rows, err := s.db.QueryContext(ctx, `
SELECT repo, key, bytes FROM lfs_objects
WHERE (repo, key) > (?, ?)
ORDER BY repo ASC, key ASC LIMIT ?`,
		afterRepo, afterKey, limit)
	if err != nil {
		return nil, fmt.Errorf("list fleet lfs objects: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []AbsenceLFSRow
	for rows.Next() {
		var r AbsenceLFSRow
		if err := rows.Scan(&r.Repo, &r.Key, &r.Bytes); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListFleetPacks — see the PG twin.
func (s *SQLite) ListFleetPacks(ctx context.Context, afterRepo, afterID string, limit int) ([]AbsencePackRow, error) {
	validateAbsencePage(limit)
	rows, err := s.db.QueryContext(ctx, `
SELECT repo, id, s3_key, bytes FROM packs
WHERE (repo, id) > (?, ?)
ORDER BY repo ASC, id ASC LIMIT ?`,
		afterRepo, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("list fleet packs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []AbsencePackRow
	for rows.Next() {
		var r AbsencePackRow
		if err := rows.Scan(&r.Repo, &r.ID, &r.Key, &r.Bytes); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
