// absence_sqlite.go mirrors absence.go's fleet-wide registry paging for
// the SQLite backend (issue #101) — see absence.go for the rationale.
package ref

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
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

// ListAbsenceCovers — see the PG twin. SQLite's default BINARY collation
// is already byte order, so the plain range scan matches the PG COLLATE
// "C" semantics.
func (s *SQLite) ListAbsenceCovers(ctx context.Context, afterRaw string, limit int) ([]AbsenceCoverRow, error) {
	validateAbsencePage(limit)
	if afterRaw == "" {
		afterRaw = AbsenceCoverPrefix // no real row key sorts at the bare prefix
	}
	// Foreign spellings are excluded in SQL — see the PG twin (Codex
	// round 3 on PR #132). substr with a negative start is the last-N
	// characters.
	rows, err := s.db.QueryContext(ctx, `
SELECT key, repo, bytes FROM pack_reservations
WHERE key > ? AND key < ?
  AND substr(key, -?) = ?
ORDER BY key ASC LIMIT ?`,
		afterRaw, absenceCoverRangeEnd, len(absenceCoverRawSuffix), absenceCoverRawSuffix, limit)
	if err != nil {
		return nil, fmt.Errorf("list absence covers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []AbsenceCoverRow
	for rows.Next() {
		var r AbsenceCoverRow
		if err := rows.Scan(&r.RawKey, &r.Base, &r.Bytes); err != nil {
			return nil, err
		}
		var ok bool
		if r.Key, ok = stripAbsenceCoverRow(r.RawKey); !ok {
			continue // foreign spelling: left to reservation expiry
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// AbsenceCoverHead — see the PG twin.
func (s *SQLite) AbsenceCoverHead(ctx context.Context, prefix string) (AbsenceCoverRow, bool, error) {
	assertf(strings.HasPrefix(prefix, AbsenceCoverPrefix), "AbsenceCoverHead(%q): not an absence cover prefix", prefix)
	var r AbsenceCoverRow
	err := s.db.QueryRowContext(ctx, `
SELECT key, repo, bytes FROM pack_reservations
WHERE key >= ? AND key < ?
  AND substr(key, -?) = ?
ORDER BY key DESC LIMIT 1`,
		prefix, coverRangeEnd(prefix), len(absenceCoverRawSuffix), absenceCoverRawSuffix).Scan(&r.RawKey, &r.Base, &r.Bytes)
	if errors.Is(err, sql.ErrNoRows) {
		return AbsenceCoverRow{}, false, nil
	}
	if err != nil {
		return AbsenceCoverRow{}, false, fmt.Errorf("absence cover head %s: %w", prefix, err)
	}
	var ok bool
	if r.Key, ok = stripAbsenceCoverRow(r.RawKey); !ok {
		return AbsenceCoverRow{}, false, nil // foreign spelling: not ours
	}
	return r, true, nil
}

// DeleteAbsenceCover — see the PG twin.
func (s *SQLite) DeleteAbsenceCover(ctx context.Context, key string) error {
	assertf(strings.HasPrefix(key, AbsenceCoverPrefix), "DeleteAbsenceCover(%q): not an absence cover key", key)
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM pack_reservations WHERE key = ?`, reservRowKey(key+RepairCoverSuffix))
	if err != nil {
		return fmt.Errorf("delete absence cover %s: %w", key, err)
	}
	return nil
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
