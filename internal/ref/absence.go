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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// AbsenceCoverPrefix is the synthetic blob-key namespace the absence
// reconciler's aggregate covers reserve under (jobs.AbsenceReconciler). No
// real blob key can collide: pack keys live under "packs/", LFS keys under
// "lfs/". The prefix is load-bearing for ListAbsenceCovers' range scan and
// must not contain "\n" (reservRowKey rejects it).
const AbsenceCoverPrefix = "absence-lost/"

// AbsenceCoverRow is one absence-cover reservation row as the reconciler's
// renewal phase sees it (issue #125): the durable record of a charged loss
// aggregate, re-derived at pass start instead of persisting the tracking
// set separately.
type AbsenceCoverRow struct {
	// RawKey is the reservation-row spelling — the opaque keyset cursor
	// for the next page.
	RawKey string
	// Key is the synthetic cover key ReserveRestoreCover was charged
	// with (reservation spellings stripped), the spelling a renewal
	// re-charges.
	Key string
	// Base is the base repository the reservation charges.
	Base string
	// Bytes is the row's reserved size (GREATEST-pinned across charges).
	Bytes int64
}

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

// absenceCoverRangeEnd is the exclusive upper bound of the cover-key range
// scan: the prefix with its final byte incremented ('/'+1).
var absenceCoverRangeEnd = AbsenceCoverPrefix[:len(AbsenceCoverPrefix)-1] + "0"

// absenceCoverRawSuffix is the reservation-row suffix every cover minted
// through ReserveRestoreCover carries. The queries filter on it in SQL
// (Codex round 3 on PR #132): a Go-side filter could return an EMPTY page
// for a stretch of foreign spellings under the prefix, which callers
// would misread as the end of the keyspace — silently skipping live
// covers whose renewals then lapse.
var absenceCoverRawSuffix = RepairCoverSuffix + reservKeySuffix

// stripAbsenceCoverRow maps one pack_reservations row in the absence-cover
// range back to the synthetic key it was charged under. The SQL filters
// guarantee the shape; ok=false is a defensive residual for rows that
// somehow carry the suffix without the prefix.
func stripAbsenceCoverRow(rawKey string) (key string, ok bool) {
	key, ok = strings.CutSuffix(rawKey, absenceCoverRawSuffix)
	if !ok || !strings.HasPrefix(key, AbsenceCoverPrefix) {
		return "", false
	}
	return key, true
}

// ListAbsenceCovers pages the live absence-cover reservation rows, keyset
// on the raw reservation key ascending (afterRaw "" starts the scan). The
// rows are the DURABLE tracking state of discovered losses: the renewal
// phase re-derives its obligations from this listing every pass, so any
// replica keeps every charged aggregate alive (issue #125). ttl is the
// liveness window admission consults (PackReservationTTL): rows older than
// it are EXPIRED — admission no longer counts them, so renewing or
// migrating one would resurrect a stale reservation (Codex round 6 on
// PR #132) — and are left to the ledger's expired-row cleanup.
func (s *PG) ListAbsenceCovers(ctx context.Context, afterRaw string, limit int, ttl time.Duration) ([]AbsenceCoverRow, error) {
	validateAbsencePage(limit)
	assertf(ttl > 0, "ListAbsenceCovers: non-positive ttl %v", ttl)
	if afterRaw == "" {
		afterRaw = AbsenceCoverPrefix // no real row key sorts at the bare prefix
	}
	// COLLATE "C" pins the range scan and cursor order to BYTE order
	// (reservation spellings embed "\n"; locale collations order it
	// unpredictably), served by the pack_reservations_key_c index —
	// the refs_repo_name_c pattern. Foreign spellings are excluded in
	// SQL so a page can never filter to empty while covers remain.
	rows, err := s.pool.Query(ctx, `
SELECT key, repo, bytes FROM pack_reservations
WHERE key COLLATE "C" > $1 AND key COLLATE "C" < $2
  AND right(key, $3) = $4
  AND created_at > now() - make_interval(secs => $5)
ORDER BY key COLLATE "C" ASC LIMIT $6`,
		afterRaw, absenceCoverRangeEnd, len(absenceCoverRawSuffix), absenceCoverRawSuffix, ttl.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("list absence covers: %w", err)
	}
	defer rows.Close()
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

// coverRangeEnd is the exclusive upper bound matching every raw row whose
// synthetic key starts with prefix (the appended reservation suffixes only
// extend the key, so the prefix range covers them all).
func coverRangeEnd(prefix string) string {
	assertf(prefix != "" && prefix[len(prefix)-1] < 0xff, "coverRangeEnd(%q)", prefix)
	return prefix[:len(prefix)-1] + string(prefix[len(prefix)-1]+1)
}

// AbsenceCoverHead returns the HIGHEST live cover row whose synthetic key
// starts with prefix (byte order; the reconciler's zero-padded incarnation
// spelling makes byte order numeric order). ok=false when none exists.
// This is the reconciler's point lookup for a base's current aggregate
// incarnation before charging into it (issue #125). ttl filters expired
// rows exactly like ListAbsenceCovers: charging into an expired higher
// incarnation would resurrect its stale GREATEST-pinned total.
func (s *PG) AbsenceCoverHead(ctx context.Context, prefix string, ttl time.Duration) (AbsenceCoverRow, bool, error) {
	assertf(strings.HasPrefix(prefix, AbsenceCoverPrefix), "AbsenceCoverHead(%q): not an absence cover prefix", prefix)
	assertf(ttl > 0, "AbsenceCoverHead: non-positive ttl %v", ttl)
	var r AbsenceCoverRow
	err := s.pool.QueryRow(ctx, `
SELECT key, repo, bytes FROM pack_reservations
WHERE key COLLATE "C" >= $1 AND key COLLATE "C" < $2
  AND right(key, $3) = $4
  AND created_at > now() - make_interval(secs => $5)
ORDER BY key COLLATE "C" DESC LIMIT 1`,
		prefix, coverRangeEnd(prefix), len(absenceCoverRawSuffix), absenceCoverRawSuffix, ttl.Seconds()).Scan(&r.RawKey, &r.Base, &r.Bytes)
	if errors.Is(err, pgx.ErrNoRows) {
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

// RenewAbsenceCovers re-stamps created_at on every LIVE deterministic
// absence-cover row in one DB-clock statement (Codex round 9 on PR #132):
// cover protection must never depend on the keyset renewal listing
// completing within the TTL — a large population or degraded ledger
// latency turns that listing into a multi-pass round robin, and a row
// aging past the TTL mid-cycle would be excluded as expired and silently
// never renewed. The strict TTL predicate is the round-6 invariant:
// already-expired rows stay dead. Only rows matching the reconciler's
// deterministic incarnation tail (`/<class>-e<19 digits>`) are touched —
// legacy nonce rows must keep decaying by TTL after their fold, and the
// keyset listing remains their (and the lifecycle's) precise mechanism.
// Returns the number of rows re-stamped.
func (s *PG) RenewAbsenceCovers(ctx context.Context, ttl time.Duration) (int64, error) {
	assertf(ttl > 0, "RenewAbsenceCovers: non-positive ttl %v", ttl)
	tag, err := s.pool.Exec(ctx, `
UPDATE pack_reservations SET created_at = now()
WHERE key COLLATE "C" > $1 AND key COLLATE "C" < $2
  AND key ~ $3
  AND created_at > now() - make_interval(secs => $4)`,
		AbsenceCoverPrefix, absenceCoverRangeEnd, absenceIncarnationTailRegexp, ttl.Seconds())
	if err != nil {
		return 0, fmt.Errorf("renew absence covers: %w", err)
	}
	return tag.RowsAffected(), nil
}

// absenceIncarnationTailRegexp matches the raw-row tail of a deterministic
// incarnation key: /<class>-e<19 zero-padded digits> followed by the
// repair-cover reservation suffix at end of string. The suffix contains no
// regex metacharacters (a literal newline matches itself; PG's $ anchors
// at end of string in default mode).
var absenceIncarnationTailRegexp = "/(lfs|packs)-e[0-9]{19}" + absenceCoverRawSuffix + "$"

// DeleteAbsenceCover removes one absence-cover reservation row by its
// synthetic key; deleting an absent row is a no-op. Only the absence
// reconciler mints and owns rows under AbsenceCoverPrefix, and River's
// unique periodic jobs serialize its passes fleet-wide, so the delete
// cannot race another writer of the row; an admission racing the delete
// sees the same outcome as the row's TTL expiry.
func (s *PG) DeleteAbsenceCover(ctx context.Context, key string) error {
	assertf(strings.HasPrefix(key, AbsenceCoverPrefix), "DeleteAbsenceCover(%q): not an absence cover key", key)
	_, err := s.pool.Exec(ctx,
		`DELETE FROM pack_reservations WHERE key = $1`, reservRowKey(key+RepairCoverSuffix))
	if err != nil {
		return fmt.Errorf("delete absence cover %s: %w", key, err)
	}
	return nil
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
