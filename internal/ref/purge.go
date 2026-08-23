// purge.go implements the repository-purge row seams (issue #52). The
// purge orchestrator (jobs.RepoPurger) reclaims a soft-deleted repository
// by tombstoning its registry rows BEFORE deleting its blobs — the inverse
// of publish-before-CAS: a crash mid-purge must leave reapable tombstones
// (pack_retained rows, orphan-class blobs), never a committed ref pointing
// at deleted bytes.
//
// Every seam takes the repository's full accounting group (base repo plus
// its namespaces, blob.QuotaRepoGroup order — ref stays namespace-agnostic
// like ReservePackBytes) and is idempotent: a resumed purge re-runs each
// phase as a no-op. These methods live on the concrete stores rather than
// RefStore on purpose — the purge is wired against the raw backends, and
// RefStore's many test/simulation mirrors must not grow with it.
package ref

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// validatePurgeGroup asserts the shared purge-seam argument invariants.
func validatePurgeGroup(group []string) {
	assertf(len(group) > 0, "purge: empty repo group")
	for _, r := range group {
		assertf(r != "", "purge: empty repo key in group %v", group)
	}
}

// DropRepoRefs deletes every ref row of the group's repos (purge phase:
// refs die FIRST, so no later phase can strand a committed ref). Returns
// the rows deleted. The InitSchema triggers bump the repo generations as
// usual; the generation rows themselves fall to PurgeRepoLedger.
func (s *PG) DropRepoRefs(ctx context.Context, group []string) (int64, error) {
	validatePurgeGroup(group)
	ct, err := s.pool.Exec(ctx, `DELETE FROM refs WHERE repo = ANY($1)`, group)
	if err != nil {
		return 0, err
	}
	return ct.RowsAffected(), nil
}

// RetireRepoPacks tombstones every registered pack of the group's repos:
// in one transaction each packs row becomes a pack_retained row and is
// deleted. bytes is 0 for purge tombstones (packs rows store no size) —
// these rows are the sweeper-reapable CLASSIFICATION tombstone (issue #31
// retained class, provably DB-absent), not accounting: the usage counter
// stays credited until PurgeRepoLedger drops it, over-counting at worst
// (the safe direction; the group is unreachable anyway). An existing
// retained row (a real repack fold's, carrying real bytes) is kept as-is
// so its eventual reclaim still debits correctly. Returns the packs rows
// retired.
func (s *PG) RetireRepoPacks(ctx context.Context, group []string) (int64, error) {
	validatePurgeGroup(group)
	var retired int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
INSERT INTO pack_retained (repo, key, bytes)
SELECT repo, s3_key, 0 FROM packs WHERE repo = ANY($1)
ON CONFLICT (repo, key) DO NOTHING`, group); err != nil {
			return err
		}
		ct, err := tx.Exec(ctx, `DELETE FROM packs WHERE repo = ANY($1)`, group)
		if err != nil {
			return err
		}
		retired = ct.RowsAffected()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return retired, nil
}

// DropRepoLFSObjects deletes the group's LFS registry rows and their
// object-count rows (LFS rows key by the BASE repo, group[0]; the extra
// group keys match nothing and cost nothing). The blobs become
// unregistered and are reaped by the purge's own blob pass —
// GuardedLFSDelete recreates the count row it locks on, so dropping it
// here is safe. Returns the lfs_objects rows deleted.
func (s *PG) DropRepoLFSObjects(ctx context.Context, group []string) (int64, error) {
	validatePurgeGroup(group)
	var dropped int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		ct, err := tx.Exec(ctx, `DELETE FROM lfs_objects WHERE repo = ANY($1)`, group)
		if err != nil {
			return err
		}
		dropped = ct.RowsAffected()
		_, err = tx.Exec(ctx, `DELETE FROM lfs_object_counts WHERE repo = ANY($1)`, group)
		return err
	})
	if err != nil {
		return 0, err
	}
	return dropped, nil
}

// CountRepoRows counts the group's refs + packs + lfs_objects rows — the
// purge's pre-completion verification read: a non-zero count means a
// writer that resolved the repo before its soft-delete registered rows
// after the tombstoning phases, and the purge must defer (the next run
// re-tombstones) instead of deleting the repo's meta row over them.
func (s *PG) CountRepoRows(ctx context.Context, group []string) (int64, error) {
	validatePurgeGroup(group)
	var n int64
	err := s.pool.QueryRow(ctx, `
SELECT (SELECT count(*) FROM refs        WHERE repo = ANY($1))
     + (SELECT count(*) FROM packs       WHERE repo = ANY($1))
     + (SELECT count(*) FROM lfs_objects WHERE repo = ANY($1))`, group).Scan(&n)
	return n, err
}

// PurgeRepoLedger removes the group's remaining accounting and state rows
// (reservations, retained tombstones whose blobs are already gone, usage
// counters, LFS counts, adoption generations, repo generations). Runs
// strictly AFTER the blob pass verified the group's registry rows are gone
// — the retained rows must outlive their blobs, and the usage counter row
// is the accounting cleanup (no per-row debit: the whole unit dies).
// Returns the total rows deleted.
func (s *PG) PurgeRepoLedger(ctx context.Context, group []string) (int64, error) {
	validatePurgeGroup(group)
	var total int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		for _, stmt := range []string{
			`DELETE FROM pack_reservations WHERE repo = ANY($1)`,
			`DELETE FROM pack_retained     WHERE repo = ANY($1)`,
			`DELETE FROM pack_usage        WHERE repo = ANY($1)`,
			`DELETE FROM lfs_object_counts WHERE repo = ANY($1)`,
			`DELETE FROM lfs_adoption_gens WHERE repo = ANY($1)`,
			`DELETE FROM repo_generations  WHERE repo = ANY($1)`,
		} {
			ct, err := tx.Exec(ctx, stmt, group)
			if err != nil {
				return err
			}
			total += ct.RowsAffected()
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}
