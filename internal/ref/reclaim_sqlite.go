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
	"encoding/json"
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
	legacyRow := !strings.Contains(key, "/o2/")
	if legacyRow && !DecodeReapWatermark(reapPayload).Legacy {
		return false, fmt.Errorf("retire lfs object %s: %w", key, ErrLegacyReapUnarmed)
	}
	// Pin the destructive-sweep epoch — see the PG twin (Codex round 33).
	// BEGIN IMMEDIATE serializes this read against BeginLFSReapEpoch's
	// single-writer increment.
	var reapEpoch int64
	switch err := tx.QueryRowContext(ctx,
		`SELECT generation FROM job_scan_state WHERE job = ? AND scope = ?`,
		ScanJobSweep, ScanScopeLFSReapEpoch).Scan(&reapEpoch); {
	case errors.Is(err, sql.ErrNoRows):
		reapEpoch = 0
	case err != nil:
		return false, err
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
	// Park the refund in the epoch bucket instead of refunding here —
	// see the PG twin (Codex round 33 on PR #132).
	scope := ScanScopeReclaimRefund(repo, reapEpoch)
	var bucket LFSRefundBucket
	var payload string
	switch err := tx.QueryRowContext(ctx,
		`SELECT payload FROM job_scan_state WHERE job = ? AND scope = ?`,
		ScanJobLFSReclaim, scope).Scan(&payload); {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return false, err
	default:
		if err := json.Unmarshal([]byte(payload), &bucket); err != nil {
			return false, fmt.Errorf("undecodable refund bucket %s: %w", scope, err)
		}
	}
	if legacyRow {
		bucket.LegacyRows, bucket.LegacyBytes = bucket.LegacyRows+1, bucket.LegacyBytes+bytes
	} else {
		bucket.Rows, bucket.Bytes = bucket.Rows+1, bucket.Bytes+bytes
	}
	merged, mErr := json.Marshal(bucket)
	assertf(mErr == nil, "retire lfs object: marshal refund bucket: %v", mErr)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO job_scan_state (job, scope, payload, generation, updated_at)
VALUES (?, ?, ?, ?, `+sqliteEpochMS+`)
ON CONFLICT (job, scope) DO UPDATE SET payload = excluded.payload, updated_at = `+sqliteEpochMS+``,
		ScanJobLFSReclaim, scope, string(merged), reapEpoch); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("retire lfs object commit: %w", err)
	}
	return true, nil
}

// SettleLFSRefunds — see the PG twin (Codex round 33 on PR #132).
func (s *SQLite) SettleLFSRefunds(ctx context.Context, repo string) (SettledLFSRefunds, error) {
	assertf(repo != "", "SettleLFSRefunds: empty repo")
	var out SettledLFSRefunds
	tx, err := s.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE via _txlock
	if err != nil {
		return out, fmt.Errorf("settle lfs refunds begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if fenced, err := repoPurgeFencedSQLite(ctx, tx, repo); err != nil {
		return out, err
	} else if fenced {
		return out, nil // the purge pipeline owns fenced repos' rows
	}
	epochFor := func(scope string) (int64, error) {
		var payload string
		switch err := tx.QueryRowContext(ctx,
			`SELECT payload FROM job_scan_state WHERE job = ? AND scope = ?`,
			ScanJobSweep, scope).Scan(&payload); {
		case errors.Is(err, sql.ErrNoRows):
			return 0, nil
		case err != nil:
			return 0, err
		}
		return DecodeReapWatermark(payload).Epoch, nil
	}
	canonEpoch, err := epochFor(ScanScopeLFSDestructiveReap)
	if err != nil {
		return out, err
	}
	legacyEpoch, err := epochFor(ScanScopeLFSDestructiveReapLegacy)
	if err != nil {
		return out, err
	}
	prefix := ScanScopeReclaimRefundPrefix(repo)
	type bucketRow struct {
		scope   string
		payload string
		epoch   int64
	}
	var refundRows, refundBytes int64
	// Keyset-page the WHOLE per-repo bucket keyspace (Codex round 35):
	// scopes are epoch-ordered, so a single fixed page could pin the scan
	// on old buckets whose legacy halves cannot settle while newer
	// canonical-only buckets waited past the limit forever. See the PG
	// twin for the bounds argument.
	for after := prefix; ; {
		var buckets []bucketRow
		rows, err := tx.QueryContext(ctx, `
SELECT scope, payload, generation FROM job_scan_state
WHERE job = ? AND substr(scope, 1, length(?)) = ? AND scope > ?
ORDER BY scope ASC LIMIT ?`,
			ScanJobLFSReclaim, prefix, prefix, after, lfsRefundBucketPage)
		if err != nil {
			return out, err
		}
		for rows.Next() {
			var b bucketRow
			if err := rows.Scan(&b.scope, &b.payload, &b.epoch); err != nil {
				rows.Close()
				return out, err
			}
			buckets = append(buckets, b)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return out, err
		}
		for _, b := range buckets {
			var bucket LFSRefundBucket
			if err := json.Unmarshal([]byte(b.payload), &bucket); err != nil {
				return out, fmt.Errorf("undecodable refund bucket %s: %w", b.scope, err)
			}
			settled := false
			if b.epoch < canonEpoch && (bucket.Rows > 0 || bucket.Bytes > 0) {
				refundRows, refundBytes = refundRows+bucket.Rows, refundBytes+bucket.Bytes
				bucket.Rows, bucket.Bytes = 0, 0
				settled = true
			}
			if b.epoch < legacyEpoch && (bucket.LegacyRows > 0 || bucket.LegacyBytes > 0) {
				refundRows, refundBytes = refundRows+bucket.LegacyRows, refundBytes+bucket.LegacyBytes
				bucket.LegacyRows, bucket.LegacyBytes = 0, 0
				settled = true
			}
			switch {
			case bucket == (LFSRefundBucket{}):
				if _, err := tx.ExecContext(ctx,
					`DELETE FROM job_scan_state WHERE job = ? AND scope = ?`,
					ScanJobLFSReclaim, b.scope); err != nil {
					return out, err
				}
			case settled:
				merged, mErr := json.Marshal(bucket)
				assertf(mErr == nil, "settle lfs refunds: marshal bucket: %v", mErr)
				if _, err := tx.ExecContext(ctx,
					`UPDATE job_scan_state SET payload = ?, updated_at = `+sqliteEpochMS+` WHERE job = ? AND scope = ?`,
					string(merged), ScanJobLFSReclaim, b.scope); err != nil {
					return out, err
				}
				out.Pending++
			default:
				out.Pending++
			}
		}
		if len(buckets) < lfsRefundBucketPage {
			break
		}
		after = buckets[len(buckets)-1].scope
	}
	if refundRows > 0 || refundBytes > 0 {
		if _, err := tx.ExecContext(ctx,
			`UPDATE lfs_object_counts SET count = MAX(count - ?, 0) WHERE repo = ?`, refundRows, repo); err != nil {
			return out, err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE pack_usage SET bytes = MAX(bytes - ?, 0) WHERE repo = ?`, refundBytes, repo); err != nil {
			return out, err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO lfs_adoption_gens (repo, gen) VALUES (?, 1)
ON CONFLICT (repo) DO UPDATE SET gen = lfs_adoption_gens.gen + 1`, repo); err != nil {
			return out, err
		}
	}
	if err := tx.Commit(); err != nil {
		return out, fmt.Errorf("settle lfs refunds commit: %w", err)
	}
	out.Rows, out.Bytes = refundRows, refundBytes
	return out, nil
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
