// SQLite half of the pack-quota ledger (see ledger.go for the design).
// Semantics mirror the PG implementation statement for statement; the
// single-writer BEGIN IMMEDIATE discipline replaces the per-base advisory
// lock, and reservation timestamps are INTEGER epoch milliseconds stamped
// by the DATABASE clock (sqliteEpochMS) so serve and hook processes agree.

package ref

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// sqliteEpochMS is the SQLite expression for "now" in epoch milliseconds.
// Millisecond resolution (unixepoch() is whole seconds) keeps short test
// TTLs meaningful while remaining a monotone DB-side clock.
const sqliteEpochMS = `CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)`

// ReservePackBytes implements the ledger.go admission check on SQLite.
func (s *SQLite) ReservePackBytes(ctx context.Context, group []string, packs []Pack, owner string, usageFloor, quotaBytes int64, ttl time.Duration) (ReserveDecision, error) {
	validateReserve(group, packs, owner, usageFloor, quotaBytes, ttl)
	tx, err := s.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE via _txlock
	if err != nil {
		return ReserveDecision{}, fmt.Errorf("reserve pack bytes begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	// Self-healing GC for expired rows — see the PG twin's rationale.
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM pack_reservations WHERE repo = ? AND created_at <= `+sqliteEpochMS+` - ?`,
		group[0], ttl.Milliseconds()); err != nil {
		return ReserveDecision{}, fmt.Errorf("reserve pack bytes prune: %w", err)
	}
	// One aggregate read; the IMMEDIATE transaction serializes every
	// writer, so this snapshot cannot interleave with a concurrent
	// reserve or register.
	groupArgs := make([]any, 0, len(group)+2*(len(packs)+2))
	for _, r := range group {
		groupArgs = append(groupArgs, r)
	}
	// Twice: the reserved-bytes SUM and the active-row COUNT repeat the
	// same predicate (repo, TTL window, excluding this push's keys).
	for range 2 {
		groupArgs = append(groupArgs, group[0], ttl.Milliseconds())
		for _, p := range packs {
			groupArgs = append(groupArgs, p.Key)
		}
	}
	var counter, reserved, activeRows int64
	if err := tx.QueryRowContext(ctx, `
SELECT
  COALESCE((SELECT SUM(bytes) FROM pack_usage WHERE repo IN (?`+placeholders(len(group)-1)+`)), 0),
  COALESCE((SELECT SUM(bytes) FROM pack_reservations
            WHERE repo = ?
              AND created_at > `+sqliteEpochMS+` - ?
              AND key NOT IN (?`+placeholders(len(packs)-1)+`)), 0),
  (SELECT COUNT(*) FROM pack_reservations
   WHERE repo = ?
     AND created_at > `+sqliteEpochMS+` - ?
     AND key NOT IN (?`+placeholders(len(packs)-1)+`))`,
		groupArgs...).Scan(&counter, &reserved, &activeRows); err != nil {
		return ReserveDecision{}, fmt.Errorf("reserve pack bytes read: %w", err)
	}
	dec := ReserveDecision{UsedBytes: max(counter, usageFloor), ReservedBytes: reserved}
	if activeRows+int64(len(packs)) > int64(maxActiveReservations) {
		dec.KeyCapExceeded = true
		return dec, nil // denied; the deferred rollback discards nothing
	}
	if !admitted(dec.UsedBytes, dec.ReservedBytes, incomingBytes(packs), quotaBytes) {
		return dec, nil // denied; the deferred rollback discards nothing
	}
	for _, p := range packs {
		// Same-key upsert (see the PG twin): retries refresh, never wedge;
		// the refresh KEEPS the existing owner (Codex round 5) and takes
		// the MAX of the declared byte values (Codex round 7).
		if _, err := tx.ExecContext(ctx, `
INSERT INTO pack_reservations (key, repo, bytes, owner, created_at)
VALUES (?, ?, ?, ?, `+sqliteEpochMS+`)
ON CONFLICT (key) DO UPDATE SET repo = excluded.repo,
                                bytes = MAX(pack_reservations.bytes, excluded.bytes),
                                created_at = excluded.created_at`,
			p.Key, group[0], p.Size, owner); err != nil {
			return ReserveDecision{}, fmt.Errorf("reserve pack bytes insert: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return ReserveDecision{}, fmt.Errorf("reserve pack bytes commit: %w", err)
	}
	dec.OK = true
	return dec, nil
}

// placeholders returns n comma-prefixed SQL placeholders (",?,?..."), for
// IN (?...) lists whose first placeholder the caller writes.
func placeholders(n int) string {
	assertf(n >= 0, "placeholders(%d)", n)
	out := make([]byte, 0, 2*n)
	for range n {
		out = append(out, ',', '?')
	}
	return string(out)
}

// LiveReservationKeys reports which keys hold unexpired reservation rows.
func (s *SQLite) LiveReservationKeys(ctx context.Context, keys []string, ttl time.Duration) ([]string, error) {
	assertf(len(keys) > 0 && ttl > 0, "LiveReservationKeys(%d keys, %v)", len(keys), ttl)
	args := make([]any, 0, len(keys)+1)
	for _, k := range keys {
		args = append(args, k)
	}
	args = append(args, ttl.Milliseconds())
	rows, err := s.db.QueryContext(ctx, `
SELECT key FROM pack_reservations
WHERE key IN (?`+placeholders(len(keys)-1)+`) AND created_at > `+sqliteEpochMS+` - ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var live []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		live = append(live, k)
	}
	return live, rows.Err()
}

// ReleasePackReservations deletes the owner's reservations for keys.
func (s *SQLite) ReleasePackReservations(ctx context.Context, keys []string, owner string) error {
	assertf(len(keys) > 0 && owner != "", "ReleasePackReservations(%v, %q)", keys, owner)
	args := make([]any, 0, len(keys)+1)
	args = append(args, owner)
	for _, k := range keys {
		args = append(args, k)
	}
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM pack_reservations WHERE owner = ? AND key IN (?`+placeholders(len(keys)-1)+`)`, args...)
	return err
}

// SweepExpiredPackReservations prunes reservation rows at least olderThan
// old.
func (s *SQLite) SweepExpiredPackReservations(ctx context.Context, olderThan time.Duration) (int64, error) {
	assertf(olderThan >= 0, "SweepExpiredPackReservations: negative age %v", olderThan)
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM pack_reservations WHERE created_at <= `+sqliteEpochMS+` - ?`,
		olderThan.Milliseconds())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// IsRetainedPack reports whether (repo, key) is a recorded superseded blob.
func (s *SQLite) IsRetainedPack(ctx context.Context, repo, key string) (bool, error) {
	assertf(repo != "" && key != "", "IsRetainedPack(%q, %q)", repo, key)
	var exists bool
	err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM pack_retained WHERE repo = ? AND key = ?)`, repo, key).Scan(&exists)
	return exists, err
}

// ReclaimRetainedPack removes the retained row and debits the usage counter.
func (s *SQLite) ReclaimRetainedPack(ctx context.Context, repo, key string) (int64, error) {
	assertf(repo != "" && key != "", "ReclaimRetainedPack(%q, %q)", repo, key)
	tx, err := s.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE via _txlock
	if err != nil {
		return 0, fmt.Errorf("reclaim retained begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var bytes int64
	switch err := tx.QueryRowContext(ctx,
		`DELETE FROM pack_retained WHERE repo = ? AND key = ? RETURNING bytes`,
		repo, key).Scan(&bytes); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil // no retained row: idempotent no-op
	case err != nil:
		return 0, err
	}
	// MAX clamp mirrors the PG GREATEST guard (see ledger.go).
	if _, err := tx.ExecContext(ctx,
		`UPDATE pack_usage SET bytes = MAX(bytes - ?, 0) WHERE repo = ?`, bytes, repo); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("reclaim retained commit: %w", err)
	}
	return bytes, nil
}

// registerLedgerSQLite mirrors registerLedgerPG for the SQLite transaction.
func registerLedgerSQLite(ctx context.Context, tx *sql.Tx, repo string, p Pack) (int64, error) {
	res, err := tx.ExecContext(ctx,
		`DELETE FROM pack_retained WHERE repo = ? AND key = ?`, repo, p.Key)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if n > 0 {
		return 0, nil // bytes already counted since before the fold
	}
	return p.Size, nil
}

// creditUsageSQLite adds bytes to repo's usage counter row (upsert).
func creditUsageSQLite(ctx context.Context, tx *sql.Tx, repo string, bytes int64) error {
	assertf(bytes >= 0, "creditUsageSQLite: negative credit %d for %s", bytes, repo)
	if bytes == 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO pack_usage (repo, bytes) VALUES (?, ?)
ON CONFLICT (repo) DO UPDATE SET bytes = pack_usage.bytes + excluded.bytes`, repo, bytes)
	return err
}

// RegisterLFSObject records one verified LFS object and settles its quota
// reservation (issue #51) — statement-for-statement mirror of the PG twin.
func (s *SQLite) RegisterLFSObject(ctx context.Context, repo, key string, bytes int64) error {
	validateRegisterLFS(repo, key, bytes)
	err := s.registerLFSObjectTx(ctx, repo, key, bytes)
	if errors.Is(err, ErrLFSObjectCapExceeded) {
		// Retire the cap loser's reservation — see the PG twin.
		if _, derr := s.db.ExecContext(ctx, `DELETE FROM pack_reservations WHERE key = ?`, key); derr != nil {
			return errors.Join(err, fmt.Errorf("retire cap-denied reservation %s: %w", key, derr))
		}
	}
	return err
}

func (s *SQLite) registerLFSObjectTx(ctx context.Context, repo, key string, bytes int64) error {
	tx, err := s.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE via _txlock
	if err != nil {
		return fmt.Errorf("register lfs object begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx,
		`INSERT INTO lfs_objects (repo, key, bytes) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`,
		repo, key, bytes)
	if err != nil {
		return err
	}
	inserted, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if inserted > 0 {
		// Atomic object-count cap via the maintained counter row (see the
		// PG twin; BEGIN IMMEDIATE is the serialization here — SQLite has
		// one writer at a time).
		var count int64
		if err := tx.QueryRowContext(ctx, `
INSERT INTO lfs_object_counts (repo, count) VALUES (?, 1)
ON CONFLICT (repo) DO UPDATE SET count = lfs_object_counts.count + 1
RETURNING count`, repo).Scan(&count); err != nil {
			return err
		}
		if count > MaxLFSObjectsPerRepo {
			return ErrLFSObjectCapExceeded // the deferred rollback discards insert + increment
		}
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM pack_reservations WHERE key = ?`, key); err != nil {
		return err
	}
	if inserted > 0 {
		if err := creditUsageSQLite(ctx, tx, repo, bytes); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("register lfs object commit: %w", err)
	}
	return nil
}

// CountLFSObjects returns repo's registered LFS object count — an O(1)
// counter-row read (see the PG twin).
func (s *SQLite) CountLFSObjects(ctx context.Context, repo string) (int64, error) {
	assertf(repo != "", "CountLFSObjects(%q)", repo)
	var n int64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE((SELECT count FROM lfs_object_counts WHERE repo = ?), 0)`, repo).Scan(&n)
	return n, err
}

// HasLFSObject reports whether (repo, key) is a registered LFS object.
func (s *SQLite) HasLFSObject(ctx context.Context, repo, key string) (bool, error) {
	assertf(repo != "" && key != "", "HasLFSObject(%q, %q)", repo, key)
	var exists bool
	err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM lfs_objects WHERE repo = ? AND key = ?)`, repo, key).Scan(&exists)
	return exists, err
}

// QuotaLedgerState reads the group's ledger state (see the PG twin).
func (s *SQLite) QuotaLedgerState(ctx context.Context, group []string, ttl time.Duration) (PackQuotaLedgerState, error) {
	assertf(len(group) > 0 && group[0] != "" && ttl > 0, "QuotaLedgerState(%v, %v)", group, ttl)
	args := make([]any, 0, 2*len(group)+2)
	for _, r := range group {
		args = append(args, r)
	}
	args = append(args, group[0], ttl.Milliseconds())
	for _, r := range group {
		args = append(args, r)
	}
	var st PackQuotaLedgerState
	err := s.db.QueryRowContext(ctx, `
SELECT
  COALESCE((SELECT SUM(bytes) FROM pack_usage WHERE repo IN (?`+placeholders(len(group)-1)+`)), 0),
  COALESCE((SELECT SUM(bytes) FROM pack_reservations
            WHERE repo = ? AND created_at > `+sqliteEpochMS+` - ?), 0),
  COALESCE((SELECT SUM(bytes) FROM pack_retained WHERE repo IN (?`+placeholders(len(group)-1)+`)), 0)`,
		args...).Scan(&st.UsageBytes, &st.ReservedBytes, &st.RetainedBytes)
	return st, err
}
