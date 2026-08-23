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
func (s *SQLite) ReservePackBytes(ctx context.Context, group []string, packs []Pack, owner string, usageFloor, floorGen, quotaBytes int64, ttl time.Duration) (ReserveDecision, error) {
	validateReserve(group, packs, owner, usageFloor, floorGen, quotaBytes, ttl)
	tx, err := s.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE via _txlock
	if err != nil {
		return ReserveDecision{}, fmt.Errorf("reserve pack bytes begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	// Purge fence — see the PG twin (issue #52, PR #105 round 9).
	if fenced, err := repoPurgeFencedSQLite(ctx, tx, group[0]); err != nil {
		return ReserveDecision{}, err
	} else if fenced {
		return ReserveDecision{}, fmt.Errorf("reserve pack bytes %s: %w", group[0], ErrRepoPurged)
	}
	// Self-healing GC for expired rows — see the PG twin's rationale.
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM pack_reservations WHERE repo = ? AND created_at <= `+sqliteEpochMS+` - ?
  AND key NOT LIKE '%/adopt-claim'`,
		group[0], ttl.Milliseconds()); err != nil {
		return ReserveDecision{}, fmt.Errorf("reserve pack bytes prune: %w", err)
	}
	// One aggregate read; the IMMEDIATE transaction serializes every
	// writer, so this snapshot cannot interleave with a concurrent
	// reserve or register. The byte SUM excludes BOTH era spellings of
	// this push's keys while the row-cap COUNT excludes only this
	// binary's spellings — see the PG twin and reservRowKeys.
	keys := make([]string, len(packs))
	for i, p := range packs {
		keys[i] = p.Key
	}
	excludeSum := reservKeySpellings(keys...)
	excludeCount := reservRowKeys(keys)
	groupArgs := make([]any, 0, len(group)+4+len(excludeSum)+len(excludeCount))
	for _, r := range group {
		groupArgs = append(groupArgs, r)
	}
	groupArgs = append(groupArgs, group[0], ttl.Milliseconds())
	for _, k := range excludeSum {
		groupArgs = append(groupArgs, k)
	}
	groupArgs = append(groupArgs, group[0], ttl.Milliseconds())
	for _, k := range excludeCount {
		groupArgs = append(groupArgs, k)
	}
	var counter, reserved, activeRows int64
	if err := tx.QueryRowContext(ctx, `
SELECT
  COALESCE((SELECT SUM(bytes) FROM pack_usage WHERE repo IN (?`+placeholders(len(group)-1)+`)), 0),
  COALESCE((SELECT SUM(bytes) FROM pack_reservations
            WHERE repo = ?
              AND created_at > `+sqliteEpochMS+` - ?
              AND key NOT IN (?`+placeholders(len(excludeSum)-1)+`)), 0),
  (SELECT COUNT(*) FROM pack_reservations
   WHERE repo = ?
     AND created_at > `+sqliteEpochMS+` - ?
     AND key NOT IN (?`+placeholders(len(excludeCount)-1)+`))`,
		groupArgs...).Scan(&counter, &reserved, &activeRows); err != nil {
		return ReserveDecision{}, fmt.Errorf("reserve pack bytes read: %w", err)
	}
	// SUM, not max — see the PG twin (Codex round 44).
	dec := ReserveDecision{UsedBytes: addSat(counter, usageFloor), ReservedBytes: reserved}
	// Transactional floor-generation recheck — see the PG twin (Codex
	// round 60, security). The IMMEDIATE transaction serializes against
	// every generation-bumping writer.
	if floorGen >= 0 {
		var gen int64
		if err := tx.QueryRowContext(ctx,
			`SELECT COALESCE((SELECT gen FROM lfs_adoption_gens WHERE repo = ?), 0)`,
			group[0]).Scan(&gen); err != nil {
			return ReserveDecision{}, fmt.Errorf("reserve pack bytes gen recheck: %w", err)
		}
		if gen != floorGen {
			dec.FloorStale = true
			return dec, nil // denied; the deferred rollback discards nothing
		}
	}
	if activeRows+int64(len(packs)) > int64(maxActiveReservations) {
		dec.KeyCapExceeded = true
		return dec, nil // denied; the deferred rollback discards nothing
	}
	if !admitted(dec.UsedBytes, dec.ReservedBytes, incomingBytes(packs), quotaBytes) {
		return dec, nil // denied; the deferred rollback discards nothing
	}
	for _, p := range packs {
		// Same-key upsert (see the PG twin): retries refresh, never
		// wedge; the row lives under this binary's era spelling (see
		// reservKeySuffix — the v0.1.0 fence), the refresh KEEPS gen
		// (round 33 — the row incarnation the retirement must present)
		// and takes the MAX of the declared byte values (Codex round 7).
		if _, err := tx.ExecContext(ctx, `
INSERT INTO pack_reservations (key, repo, bytes, owner, created_at, gen)
VALUES (?, ?, ?, ?, `+sqliteEpochMS+`, ?)
ON CONFLICT (key) DO UPDATE SET repo = excluded.repo,
                                bytes = MAX(pack_reservations.bytes, excluded.bytes),
                                owner = excluded.owner,
                                created_at = excluded.created_at,
                                attempts = pack_reservations.attempts + 1`,
			reservRowKey(p.Key), group[0], p.Size, owner, NewReservationOwner()); err != nil {
			return ReserveDecision{}, fmt.Errorf("reserve pack bytes insert: %w", err)
		}
		var gen string
		if err := tx.QueryRowContext(ctx,
			`SELECT gen FROM pack_reservations WHERE key = ?`, reservRowKey(p.Key)).Scan(&gen); err != nil {
			return ReserveDecision{}, fmt.Errorf("reserve pack bytes gen: %w", err)
		}
		if dec.Gens == nil {
			dec.Gens = make(map[string]string, len(packs))
		}
		dec.Gens[p.Key] = gen
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

// (ReleasePackReservations was removed in Codex round 22 on #51: same-key
// reservation rows are shared by every concurrent writer via refresh, so
// an owner-scoped failure release could free headroom another writer was
// still staging under. RetirePackAttempts replaced it in round 30 with
// per-attempt refcounts.)

// RetirePackAttempts — see the PG twin; BEGIN IMMEDIATE's single writer is
// the serialization.
func (s *SQLite) RetirePackAttempts(ctx context.Context, group []string, gens map[string]string) error {
	assertf(len(group) > 0 && group[0] != "" && len(gens) > 0, "RetirePackAttempts(%v, %v)", group, gens)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for key, gen := range gens {
		if _, err := tx.ExecContext(ctx,
			`UPDATE pack_reservations SET attempts = attempts - 1 WHERE repo = ? AND key = ? AND gen = ?`,
			group[0], reservRowKey(key), gen); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM pack_reservations WHERE repo = ? AND key = ? AND gen = ? AND attempts <= 0`,
			group[0], reservRowKey(key), gen); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// SweepExpiredPackReservations prunes reservation rows at least olderThan
// old.
func (s *SQLite) SweepExpiredPackReservations(ctx context.Context, olderThan time.Duration) (int64, error) {
	assertf(olderThan >= 0, "SweepExpiredPackReservations: negative age %v", olderThan)
	res, err := s.db.ExecContext(ctx, `
DELETE FROM pack_reservations
WHERE (key NOT LIKE '%/adopt-claim' AND created_at <= `+sqliteEpochMS+` - ?)
   OR (key LIKE '%/adopt-claim' AND created_at <= `+sqliteEpochMS+` - ?)`,
		olderThan.Milliseconds(), AdoptionClaimTTL.Milliseconds())
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
	// Floor-generation bump atomic with the debit (Codex round 59,
	// security) — see the PG twin's rationale.
	if bytes > 0 {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO lfs_adoption_gens (repo, gen) VALUES (?, 1)
ON CONFLICT (repo) DO UPDATE SET gen = lfs_adoption_gens.gen + 1`, baseRepoOf(repo)); err != nil {
			return 0, err
		}
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
	// Cap denial deletes no reservation rows — see the PG twin (Codex
	// round 37): the cap-denied caller retires its own attempt.
	return s.registerLFSObjectTx(ctx, repo, key, bytes, false)
}

// RegisterAdoptedLFSObject — see RefStore (Codex round 51): the adoption
// generation bump commits atomically with the registration.
func (s *SQLite) RegisterAdoptedLFSObject(ctx context.Context, repo, key string, bytes int64) error {
	validateRegisterLFS(repo, key, bytes)
	return s.registerLFSObjectTx(ctx, repo, key, bytes, true)
}

func (s *SQLite) registerLFSObjectTx(ctx context.Context, repo, key string, bytes int64, adopted bool) error {
	tx, err := s.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE via _txlock
	if err != nil {
		return fmt.Errorf("register lfs object begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	// Purge fence (issue #52): the single-writer transaction orders this
	// read against FenceRepoPurge's insert — see the PG twin.
	if fenced, err := repoPurgeFencedSQLite(ctx, tx, repo); err != nil {
		return err
	} else if fenced {
		return fmt.Errorf("register lfs object %s: %w", key, ErrRepoPurged)
	}
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
	// Settlement consumes BOTH era spellings — see the PG twin.
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM pack_reservations WHERE key IN (?, ?)`, reservRowKey(key), key); err != nil {
		return err
	}
	if inserted > 0 {
		if adopted {
			// Atomic with the insert+credit — see the PG twin (round 51).
			if _, err := tx.ExecContext(ctx, `
INSERT INTO lfs_adoption_gens (repo, gen) VALUES (?, 1)
ON CONFLICT (repo) DO UPDATE SET gen = lfs_adoption_gens.gen + 1`, repo); err != nil {
				return err
			}
		}
		if err := creditUsageSQLite(ctx, tx, repo, bytes); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("register lfs object commit: %w", err)
	}
	return nil
}

// BumpLFSAdoptionGen — see RefStore (Codex round 49). The purge-fence
// guard is inside the single upsert statement (issue #52, PR #105 round
// 10; see the PG twin's rationale): no row proposed for a fenced repo
// means no conflict and no update, and SQLite's single writer totally
// orders the statement against FenceRepoPurge's insert.
func (s *SQLite) BumpLFSAdoptionGen(ctx context.Context, repo string) error {
	assertf(repo != "", "BumpLFSAdoptionGen(%q)", repo)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO lfs_adoption_gens (repo, gen)
SELECT ?, 1 WHERE NOT EXISTS (SELECT 1 FROM purged_repos WHERE repo = ?)
ON CONFLICT (repo) DO UPDATE SET gen = lfs_adoption_gens.gen + 1`, repo, baseRepoOf(repo))
	return err
}

// LFSAdoptionGen — see RefStore (Codex round 49).
func (s *SQLite) LFSAdoptionGen(ctx context.Context, repo string) (int64, error) {
	assertf(repo != "", "LFSAdoptionGen(%q)", repo)
	var gen int64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE((SELECT gen FROM lfs_adoption_gens WHERE repo = ?), 0)`, repo).Scan(&gen)
	return gen, err
}

// GuardedLFSDelete — see the PG twin; BEGIN IMMEDIATE's single writer is
// the serialization here.
// TryAdoptionClaim — see RefStore (Codex security round 73). Local mode is
// single-process, but the shared-table claim keeps semantics identical to
// PG (and dedupes across the serve process and hook subprocesses).
func (s *SQLite) TryAdoptionClaim(ctx context.Context, repo, key string) (bool, string, error) {
	assertf(repo != "" && key != "", "TryAdoptionClaim(%q, %q)", repo, key)
	owner := NewReservationOwner()
	res, err := s.db.ExecContext(ctx, `
INSERT INTO pack_reservations (key, repo, bytes, owner, created_at, gen)
VALUES (?, ?, 1, ?, `+sqliteEpochMS+`, '')
ON CONFLICT (key) DO UPDATE SET repo = excluded.repo, owner = excluded.owner, created_at = excluded.created_at
WHERE pack_reservations.created_at <= `+sqliteEpochMS+` - ?`,
		key+AdoptionClaimSuffix, repo, owner, AdoptionClaimTTL.Milliseconds())
	if err != nil {
		return false, "", err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, "", err
	}
	if n == 0 {
		return false, "", nil
	}
	return true, owner, nil
}

// RenewAdoptionClaim — see RefStore.
func (s *SQLite) RenewAdoptionClaim(ctx context.Context, key, owner string) (bool, error) {
	assertf(key != "" && owner != "", "RenewAdoptionClaim(%q, %q)", key, owner)
	res, err := s.db.ExecContext(ctx, `
UPDATE pack_reservations SET created_at = `+sqliteEpochMS+`
WHERE key = ? AND owner = ? AND created_at > `+sqliteEpochMS+` - ?`,
		key+AdoptionClaimSuffix, owner, AdoptionClaimTTL.Milliseconds())
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// ReleaseAdoptionClaim — see RefStore.
func (s *SQLite) ReleaseAdoptionClaim(ctx context.Context, key, owner string) error {
	assertf(key != "" && owner != "", "ReleaseAdoptionClaim(%q, %q)", key, owner)
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM pack_reservations WHERE key = ? AND owner = ?`, key+AdoptionClaimSuffix, owner)
	return err
}

func (s *SQLite) GuardedLFSDelete(ctx context.Context, repo, key string, del func(context.Context) error) (bool, error) {
	assertf(repo != "" && key != "" && del != nil, "GuardedLFSDelete(%q, %q)", repo, key)
	tx, err := s.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE via _txlock
	if err != nil {
		return false, fmt.Errorf("guarded lfs delete begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var registered bool
	if err := tx.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM lfs_objects WHERE repo = ? AND key = ?)`, repo, key).Scan(&registered); err != nil {
		return false, err
	}
	if registered {
		return false, tx.Commit() // a verified upload won: nothing to delete
	}
	if err := del(ctx); err != nil {
		return false, err
	}
	return true, tx.Commit()
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
func (s *SQLite) QuotaLedgerState(ctx context.Context, group []string, ttl time.Duration, excludeKeys ...string) (PackQuotaLedgerState, error) {
	assertf(len(group) > 0 && group[0] != "" && ttl > 0, "QuotaLedgerState(%v, %v)", group, ttl)
	// Both era spellings per excluded key — see the PG twin.
	excludeKeys = reservKeySpellings(excludeKeys...)
	exclude := ""
	args := make([]any, 0, 2*len(group)+2+len(excludeKeys))
	for _, r := range group {
		args = append(args, r)
	}
	args = append(args, group[0], ttl.Milliseconds())
	if len(excludeKeys) > 0 {
		exclude = ` AND key NOT IN (?` + placeholders(len(excludeKeys)-1) + `)`
		for _, k := range excludeKeys {
			args = append(args, k)
		}
	}
	for _, r := range group {
		args = append(args, r)
	}
	var st PackQuotaLedgerState
	err := s.db.QueryRowContext(ctx, `
SELECT
  COALESCE((SELECT SUM(bytes) FROM pack_usage WHERE repo IN (?`+placeholders(len(group)-1)+`)), 0),
  COALESCE((SELECT SUM(bytes) FROM pack_reservations
            WHERE repo = ? AND created_at > `+sqliteEpochMS+` - ?`+exclude+`), 0),
  COALESCE((SELECT SUM(bytes) FROM pack_retained WHERE repo IN (?`+placeholders(len(group)-1)+`)), 0)`,
		args...).Scan(&st.UsageBytes, &st.ReservedBytes, &st.RetainedBytes)
	return st, err
}
