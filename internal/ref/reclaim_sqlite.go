package ref

// SQLite twins of the issue #73 reclamation row surface — see reclaim.go
// for the contracts. BEGIN IMMEDIATE (single writer) is the serialization
// everywhere the PG twin takes the base-ledger lock, and SQLite's
// synchronous per-row triggers make the generation fence exact: a ref
// write either committed (and bumped) before the verdict transaction or
// waits behind it.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// LFSReposWithObjects — see the PG twin.
func (s *SQLite) LFSReposWithObjects(ctx context.Context, after string, limit int) ([]string, error) {
	assertf(limit > 0 && limit <= lfsReclaimPageCap, "LFSReposWithObjects: bad page limit %d", limit)
	rows, err := s.db.QueryContext(ctx,
		`SELECT repo FROM lfs_object_counts WHERE count > 0 AND repo > ? ORDER BY repo ASC LIMIT ?`,
		after, limit)
	if err != nil {
		return nil, fmt.Errorf("lfs repos with objects: %w", err)
	}
	defer rows.Close()
	var repos []string
	for rows.Next() {
		var repo string
		if err := rows.Scan(&repo); err != nil {
			return nil, err
		}
		repos = append(repos, repo)
	}
	return repos, rows.Err()
}

// CountRefs — see the PG twin.
func (s *SQLite) CountRefs(ctx context.Context, repo string, limit int64) (int64, error) {
	assertf(repo != "", "CountRefs: empty repo")
	assertf(limit > 0, "CountRefs: non-positive limit %d", limit)
	var n int64
	if err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM (SELECT 1 FROM refs WHERE repo = ? LIMIT ?) capped`,
		repo, limit).Scan(&n); err != nil {
		return 0, fmt.Errorf("count refs %s: %w", repo, err)
	}
	return n, nil
}

// ListRepoLFSObjects — see the PG twin.
func (s *SQLite) ListRepoLFSObjects(ctx context.Context, repo, after string, limit int) ([]LFSObjectRow, error) {
	validateLFSReclaimPage(repo, limit)
	rows, err := s.db.QueryContext(ctx,
		`SELECT key, bytes, unreferenced_since FROM lfs_objects WHERE repo = ? AND key > ? ORDER BY key ASC LIMIT ?`,
		repo, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list lfs objects: %w", err)
	}
	defer rows.Close()
	var out []LFSObjectRow
	for rows.Next() {
		var r LFSObjectRow
		var since sql.NullInt64
		if err := rows.Scan(&r.Key, &r.Bytes, &since); err != nil {
			return nil, err
		}
		if since.Valid {
			r.UnreferencedSince = time.Unix(since.Int64, 0).UTC()
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// lfsKeyListArgs expands keys into an IN (...) placeholder list prefixed by
// repo — the SQLite spelling of the PG twins' `key = ANY($2)`.
func lfsKeyListArgs(repo string, keys []string) (placeholders string, args []any) {
	args = make([]any, 0, len(keys)+1)
	args = append(args, repo)
	for _, k := range keys {
		args = append(args, k)
	}
	return strings.TrimSuffix(strings.Repeat("?, ", len(keys)), ", "), args
}

// MarkLFSObjectsUnreferenced — see the PG twin.
func (s *SQLite) MarkLFSObjectsUnreferenced(ctx context.Context, repo string, keys []string) (int64, error) {
	validateLFSReclaimPage(repo, len(keys))
	placeholders, args := lfsKeyListArgs(repo, keys)
	res, err := s.db.ExecContext(ctx,
		`UPDATE lfs_objects SET unreferenced_since = unixepoch() WHERE repo = ? AND key IN (`+placeholders+`) AND unreferenced_since IS NULL`,
		args...)
	if err != nil {
		return 0, fmt.Errorf("mark lfs objects unreferenced: %w", err)
	}
	return res.RowsAffected()
}

// ClearLFSObjectsUnreferenced — see the PG twin.
func (s *SQLite) ClearLFSObjectsUnreferenced(ctx context.Context, repo string, keys []string) (int64, error) {
	validateLFSReclaimPage(repo, len(keys))
	placeholders, args := lfsKeyListArgs(repo, keys)
	res, err := s.db.ExecContext(ctx,
		`UPDATE lfs_objects SET unreferenced_since = NULL WHERE repo = ? AND key IN (`+placeholders+`) AND unreferenced_since IS NOT NULL`,
		args...)
	if err != nil {
		return 0, fmt.Errorf("clear lfs objects unreferenced: %w", err)
	}
	return res.RowsAffected()
}

// RetireUnreferencedLFSObject — see the PG twin for the full contract.
func (s *SQLite) RetireUnreferencedLFSObject(ctx context.Context, repo, key string, pinnedGens map[string]int64, grace, reapMaxAge time.Duration) (bool, error) {
	assertf(repo != "" && key != "", "RetireUnreferencedLFSObject: empty repo/key")
	assertf(len(pinnedGens) > 0, "RetireUnreferencedLFSObject: no pinned generations")
	assertf(grace >= 0, "RetireUnreferencedLFSObject: negative grace %v", grace)
	assertf(reapMaxAge > 0, "RetireUnreferencedLFSObject: non-positive reap watermark bound %v", reapMaxAge)
	tx, err := s.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE via _txlock
	if err != nil {
		return false, fmt.Errorf("retire lfs object begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	// Observed reap coupling (issue #120) — see the PG twin. The
	// watermark stamps updated_at in epoch MILLISECONDS.
	var reapPayload string
	switch err := tx.QueryRowContext(ctx, `
SELECT payload FROM job_scan_state
  WHERE job = ? AND scope = ?
    AND updated_at > `+sqliteEpochMS+` - ?`,
		ScanJobSweep, ScanScopeLFSDestructiveReap, reapMaxAge.Milliseconds()).Scan(&reapPayload); {
	case errors.Is(err, sql.ErrNoRows):
		return false, fmt.Errorf("retire lfs object %s: %w", key, ErrReapWatermarkStale)
	case err != nil:
		return false, err
	}
	// Legacy-spelled rows need a legacy-armed watermark — see the PG twin
	// (Codex round 17 on PR #132).
	if reapPayload != ReapWatermarkLegacy && !strings.Contains(key, "/o2/") {
		return false, fmt.Errorf("retire lfs object %s: %w", key, ErrLegacyReapUnarmed)
	}
	if fenced, err := repoPurgeFencedSQLite(ctx, tx, repo); err != nil {
		return false, err
	} else if fenced {
		return false, nil // the purge pipeline owns fenced repos' rows
	}
	for member, want := range pinnedGens {
		var gen int64
		err := tx.QueryRowContext(ctx,
			`SELECT generation FROM repo_generations WHERE repo = ?`, member).Scan(&gen)
		if errors.Is(err, sql.ErrNoRows) {
			gen = 0
		} else if err != nil {
			return false, err
		}
		if gen != want {
			return false, nil // refs moved since the scan: verdict is stale
		}
	}
	// A live per-key adoption claim fences retirement — see the PG twin
	// (Codex round 11). Claims stamp created_at in epoch MILLISECONDS.
	var claimed bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(SELECT 1 FROM pack_reservations
  WHERE key = ? AND created_at > `+sqliteEpochMS+` - ?)`,
		lfsClaimTwin(repo, key)+AdoptionClaimSuffix, AdoptionClaimTTL.Milliseconds()).Scan(&claimed); err != nil {
		return false, err
	}
	if claimed {
		return false, nil // an active materialization owns this key: spared
	}
	// SQLite stamps marks and evaluates "now" at whole-second resolution
	// (unixepoch()), and both FLOOR: a mark minted at T.999 reads as T, so
	// a naive elapsed check undercuts the window by up to a second — and
	// plain integer division would truncate a sub-second grace to zero
	// outright, retiring freshly marked rows immediately (Codex round 4).
	// Rounding the grace up and demanding one extra elapsed second makes
	// the real elapsed time strictly exceed the configured grace. Zero
	// stays zero: that caller explicitly waives the window.
	graceSecs := int64(0)
	if grace > 0 {
		graceSecs = (int64(grace)+int64(time.Second)-1)/int64(time.Second) + 1
	}
	var bytes int64
	switch err := tx.QueryRowContext(ctx, `
DELETE FROM lfs_objects WHERE repo = ? AND key = ?
  AND unreferenced_since IS NOT NULL
  AND unreferenced_since <= unixepoch() - ?
RETURNING bytes`, repo, key, graceSecs).Scan(&bytes); {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil // touched, re-registered, or already retired
	case err != nil:
		return false, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE lfs_object_counts SET count = MAX(count - 1, 0) WHERE repo = ?`, repo); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE pack_usage SET bytes = MAX(bytes - ?, 0) WHERE repo = ?`, bytes, repo); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO lfs_adoption_gens (repo, gen) VALUES (?, 1)
ON CONFLICT (repo) DO UPDATE SET gen = lfs_adoption_gens.gen + 1`, repo); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("retire lfs object commit: %w", err)
	}
	return true, nil
}

// TouchLFSObject — see RefStore.
func (s *SQLite) TouchLFSObject(ctx context.Context, repo, key string) (bool, error) {
	assertf(repo != "" && key != "", "TouchLFSObject: empty repo/key")
	res, err := s.db.ExecContext(ctx,
		`UPDATE lfs_objects SET unreferenced_since = NULL WHERE repo = ? AND key = ?`, repo, key)
	if err != nil {
		return false, fmt.Errorf("touch lfs object %s: %w", key, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}
