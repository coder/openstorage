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
	"strings"
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

// stripAbsenceCoverRow maps one pack_reservations row in the absence-cover
// range back to the synthetic key it was charged under. ok=false for
// foreign spellings (rows not minted through ReserveRestoreCover — nothing
// this binary wrote); such rows are skipped and left to reservation
// expiry.
func stripAbsenceCoverRow(rawKey string) (key string, ok bool) {
	key, ok = strings.CutSuffix(rawKey, RepairCoverSuffix+reservKeySuffix)
	if !ok || !strings.HasPrefix(key, AbsenceCoverPrefix) {
		return "", false
	}
	return key, true
}

// ListAbsenceCovers pages the live absence-cover reservation rows, keyset
// on the raw reservation key ascending (afterRaw "" starts the scan). The
// rows are the DURABLE tracking state of discovered losses: the renewal
// phase re-derives its obligations from this listing every pass, so any
// replica keeps every charged aggregate alive (issue #125).
func (s *PG) ListAbsenceCovers(ctx context.Context, afterRaw string, limit int) ([]AbsenceCoverRow, error) {
	validateAbsencePage(limit)
	if afterRaw == "" {
		afterRaw = AbsenceCoverPrefix // no real row key sorts at the bare prefix
	}
	// COLLATE "C" pins the range scan and cursor order to BYTE order
	// (reservation spellings embed "\n"; locale collations order it
	// unpredictably), served by the pack_reservations_key_c index —
	// the refs_repo_name_c pattern.
	rows, err := s.pool.Query(ctx, `
SELECT key, repo, bytes FROM pack_reservations
WHERE key COLLATE "C" > $1 AND key COLLATE "C" < $2
ORDER BY key COLLATE "C" ASC LIMIT $3`,
		afterRaw, absenceCoverRangeEnd, limit)
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
