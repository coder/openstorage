package ref

// Registry-vs-storage absence reconciliation rows (issue #101). The
// reconciliation job itself lives in internal/jobs (AbsenceReconciler);
// this file is the PG row surface it pages: flat fleet-wide keyset cursors
// over the two registries whose counted bytes a storage-layer loss can make
// physically absent — lfs_objects (PK repo, key) and packs (PK repo, id).
// The cursors are the tables' primary keys, so every page is one index
// range scan; issue #121's fleet-persistent cursor work can durably store
// the two (repo, key) pairs as-is.

import (
	"context"
	"fmt"
)

// AbsenceLFSRow is one registered LFS object as the absence reconciler
// sees it: the row key (canonical or v0.2.0 legacy spelling) and its
// counted bytes.
type AbsenceLFSRow struct {
	Repo  string
	Key   string
	Bytes int64
}

// AbsencePackRow is one registered pack as the absence reconciler sees it.
// ID is the keyset cursor component (the PK is (repo, id)); Key is the
// blob key the row's bytes live under. Bytes is 0 for rows registered
// before the packs.bytes migration (size unknown — reported, never
// covered).
type AbsencePackRow struct {
	Repo  string
	ID    string
	Key   string
	Bytes int64
}

func validateAbsencePage(limit int) {
	assertf(limit > 0 && limit <= lfsReclaimPageCap, "absence reconcile: bad page limit %d", limit)
}

// ListFleetLFSObjects pages registered LFS object rows fleet-wide, keyset
// on the (repo, key) primary key ascending — the absence reconciler's LFS
// work queue.
func (s *PG) ListFleetLFSObjects(ctx context.Context, afterRepo, afterKey string, limit int) ([]AbsenceLFSRow, error) {
	validateAbsencePage(limit)
	rows, err := s.pool.Query(ctx, `
SELECT repo, key, bytes FROM lfs_objects
WHERE (repo, key) > ($1, $2)
ORDER BY repo ASC, key ASC LIMIT $3`,
		afterRepo, afterKey, limit)
	if err != nil {
		return nil, fmt.Errorf("list fleet lfs objects: %w", err)
	}
	defer rows.Close()
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

// ListFleetPacks pages registered pack rows fleet-wide, keyset on the
// (repo, id) primary key ascending — the absence reconciler's pack work
// queue.
func (s *PG) ListFleetPacks(ctx context.Context, afterRepo, afterID string, limit int) ([]AbsencePackRow, error) {
	validateAbsencePage(limit)
	rows, err := s.pool.Query(ctx, `
SELECT repo, id, s3_key, bytes FROM packs
WHERE (repo, id) > ($1, $2)
ORDER BY repo ASC, id ASC LIMIT $3`,
		afterRepo, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("list fleet packs: %w", err)
	}
	defer rows.Close()
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
