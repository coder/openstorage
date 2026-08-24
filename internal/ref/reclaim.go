package ref

// Unreferenced-LFS-object reclamation rows (issue #73). The reachability
// scan itself lives in internal/jobs (LFSReclaimer) and internal/gitread
// (pointer walk); this file is the PG row surface it drives: candidate
// paging, the persistent unreferenced_since mark, and the guarded
// retirement that is the only row-deletion path outside repository purge
// and ADR-41 twin reconciliation.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

// LFSObjectRow is one registered LFS object as the reclamation scan sees it:
// the row key (canonical or v0.2.0 legacy spelling), its counted bytes, and
// the reclamation mark (zero while unmarked).
type LFSObjectRow struct {
	Key               string
	Bytes             int64
	UnreferencedSince time.Time
}

// lfsReclaimPageCap bounds one candidate page. Callers page; the cap only
// guards against a runaway limit.
const lfsReclaimPageCap = 10_000

func validateLFSReclaimPage(repo string, limit int) {
	assertf(repo != "", "lfs reclaim: empty repo")
	assertf(limit > 0 && limit <= lfsReclaimPageCap, "lfs reclaim: bad page limit %d", limit)
}

// LFSReposWithObjects pages base repos that hold at least one registered
// LFS object (keyset on repo, ascending) — the reclamation scan's work
// queue.
func (s *PG) LFSReposWithObjects(ctx context.Context, after string, limit int) ([]string, error) {
	assertf(limit > 0 && limit <= lfsReclaimPageCap, "LFSReposWithObjects: bad page limit %d", limit)
	rows, err := s.pool.Query(ctx,
		`SELECT repo FROM lfs_object_counts WHERE count > 0 AND repo > $1 ORDER BY repo ASC LIMIT $2`,
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

// CountRefs prices a namespace member's ref fanout for the reclamation
// budget without materializing the refs (an attacker can mint millions of
// same-tip refs; loading them just to count would balloon the worker's heap
// before the budget could reject the repo).
func (s *PG) CountRefs(ctx context.Context, repo string) (int64, error) {
	assertf(repo != "", "CountRefs: empty repo")
	var n int64
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM refs WHERE repo = $1`, repo).Scan(&n); err != nil {
		return 0, fmt.Errorf("count refs %s: %w", repo, err)
	}
	return n, nil
}

// ListRepoLFSObjects pages repo's registered LFS object rows (keyset on
// key, ascending).
func (s *PG) ListRepoLFSObjects(ctx context.Context, repo, after string, limit int) ([]LFSObjectRow, error) {
	validateLFSReclaimPage(repo, limit)
	rows, err := s.pool.Query(ctx,
		`SELECT key, bytes, unreferenced_since FROM lfs_objects WHERE repo = $1 AND key > $2 ORDER BY key ASC LIMIT $3`,
		repo, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list lfs objects: %w", err)
	}
	defer rows.Close()
	var out []LFSObjectRow
	for rows.Next() {
		var r LFSObjectRow
		var since *time.Time
		if err := rows.Scan(&r.Key, &r.Bytes, &since); err != nil {
			return nil, err
		}
		if since != nil {
			r.UnreferencedSince = *since
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// MarkLFSObjectsUnreferenced stamps unreferenced_since = now() on the given
// rows where no mark exists yet, and reports how many rows it stamped. An
// existing mark keeps its original clock — the grace window measures how
// long the object has CONTINUOUSLY been observed unreferenced.
func (s *PG) MarkLFSObjectsUnreferenced(ctx context.Context, repo string, keys []string) (int64, error) {
	validateLFSReclaimPage(repo, len(keys))
	ct, err := s.pool.Exec(ctx,
		`UPDATE lfs_objects SET unreferenced_since = now() WHERE repo = $1 AND key = ANY($2) AND unreferenced_since IS NULL`,
		repo, keys)
	if err != nil {
		return 0, fmt.Errorf("mark lfs objects unreferenced: %w", err)
	}
	return ct.RowsAffected(), nil
}

// ClearLFSObjectsUnreferenced removes the reclamation mark from the given
// rows (their OIDs were observed referenced by a reachable pointer file).
func (s *PG) ClearLFSObjectsUnreferenced(ctx context.Context, repo string, keys []string) (int64, error) {
	validateLFSReclaimPage(repo, len(keys))
	ct, err := s.pool.Exec(ctx,
		`UPDATE lfs_objects SET unreferenced_since = NULL WHERE repo = $1 AND key = ANY($2) AND unreferenced_since IS NOT NULL`,
		repo, keys)
	if err != nil {
		return 0, fmt.Errorf("clear lfs objects unreferenced: %w", err)
	}
	return ct.RowsAffected(), nil
}

// RetireUnreferencedLFSObject deletes one registered LFS object row whose
// reclamation mark has survived a full grace window, refunding its object
// count and usage bytes — the issue #73 GC verdict. It is the same guard
// class as GuardedLFSDelete: the whole verdict runs under the base-ledger
// lock RegisterLFSObject serializes on, and the delete predicate re-checks
// the mark IN the transaction, so an object re-uploaded or touched mid-scan
// (mark cleared, or a fresh mark younger than grace) is spared.
//
// pinnedGens fences the verdict against refs that moved since the
// reachability scan pinned them (repo generation per namespace-group
// member, 0 for never-mutated): each current generation is re-read under
// FOR UPDATE and any drift skips the retirement — a pointer file pushed
// mid-scan lives in a bumped generation by definition. The FOR UPDATE also
// forces a concurrent push's deferred generation-bump trigger to serialize
// against the verdict commit. Residual (documented in ADR-42): a push whose
// commit began before the recheck locked the rows can still land its bump
// after the verdict commits; only a raw-git pointer push referencing a
// grace-expired marked object inside that window is exposed, because
// git-lfs clients always touch via the batch API first.
//
// retired=false means the verdict lost — fence drift, purge fence, cleared
// or young mark, or a vanished row — and the caller moves on; the next scan
// re-derives everything from durable state.
func (s *PG) RetireUnreferencedLFSObject(ctx context.Context, repo, key string, pinnedGens map[string]int64, grace time.Duration) (bool, error) {
	assertf(repo != "" && key != "", "RetireUnreferencedLFSObject: empty repo/key")
	assertf(len(pinnedGens) > 0, "RetireUnreferencedLFSObject: no pinned generations")
	assertf(grace >= 0, "RetireUnreferencedLFSObject: negative grace %v", grace)
	retired := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockBaseLedger(ctx, tx, repo); err != nil {
			return err
		}
		// A purge-fenced repo's rows belong to the purge pipeline
		// (issue #52): it drops them wholesale with the counter row.
		if fenced, err := repoPurgeFencedPG(ctx, tx, repo); err != nil {
			return err
		} else if fenced {
			return nil
		}
		// Deterministic lock order across group members — concurrent
		// verdicts for one repo must not deadlock on the generation rows.
		members := make([]string, 0, len(pinnedGens))
		for member := range pinnedGens {
			members = append(members, member)
		}
		sort.Strings(members)
		for _, member := range members {
			var gen int64
			err := tx.QueryRow(ctx,
				`SELECT generation FROM repo_generations WHERE repo = $1 FOR UPDATE`, member).Scan(&gen)
			if errors.Is(err, pgx.ErrNoRows) {
				gen = 0
			} else if err != nil {
				return err
			}
			if gen != pinnedGens[member] {
				return nil // refs moved since the scan: verdict is stale
			}
		}
		var bytes int64
		switch err := tx.QueryRow(ctx, `
DELETE FROM lfs_objects WHERE repo = $1 AND key = $2
  AND unreferenced_since IS NOT NULL
  AND unreferenced_since <= now() - make_interval(secs => $3)
RETURNING bytes`, repo, key, grace.Seconds()).Scan(&bytes); {
		case errors.Is(err, pgx.ErrNoRows):
			return nil // touched, re-registered, or already retired
		case err != nil:
			return err
		}
		// Refund count and bytes and bump the adoption generation
		// atomically with the delete, mirroring the ADR-41 twin refund:
		// a floor cached while the row's bytes were counted must be
		// rejected once the counter shrinks.
		if _, err := tx.Exec(ctx,
			`UPDATE lfs_object_counts SET count = GREATEST(count - 1, 0) WHERE repo = $1`, repo); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE pack_usage SET bytes = GREATEST(bytes - $2, 0) WHERE repo = $1`, repo, bytes); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO lfs_adoption_gens (repo, gen) VALUES ($1, 1)
ON CONFLICT (repo) DO UPDATE SET gen = lfs_adoption_gens.gen + 1`, repo); err != nil {
			return err
		}
		retired = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("retire lfs object %s: %w", key, err)
	}
	return retired, nil
}

// TouchLFSObject — see RefStore.
func (s *PG) TouchLFSObject(ctx context.Context, repo, key string) (bool, error) {
	assertf(repo != "" && key != "", "TouchLFSObject: empty repo/key")
	ct, err := s.pool.Exec(ctx,
		`UPDATE lfs_objects SET unreferenced_since = NULL WHERE repo = $1 AND key = $2`, repo, key)
	if err != nil {
		return false, fmt.Errorf("touch lfs object %s: %w", key, err)
	}
	return ct.RowsAffected() > 0, nil
}
