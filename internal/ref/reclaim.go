package ref

// Unreferenced-LFS-object reclamation rows (issue #73). The reachability
// scan itself lives in internal/jobs (LFSReclaimer) and internal/gitread
// (pointer walk); this file is the PG row surface it drives: candidate
// paging, the persistent unreferenced_since mark, and the guarded
// retirement that is the only row-deletion path outside repository purge
// and ADR-41 twin reconciliation.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// lfsRefundBucketPage bounds one settlement PAGE of the bucket listing;
// settlement keyset-pages through the whole per-repo keyspace (Codex
// round 35 on PR #132). The population is sweep epochs with retirements
// between settlements — operator-bounded — and drained every reclaimer
// pass. A var so tests can exercise the paging without minting a
// thousand epochs.
var lfsRefundBucketPage = 1024

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
// before the budget could reject the repo). The count stops at limit (Codex
// round 12): even an index-only COUNT walks every matching entry, so an
// uncapped count on a fanout repo would do budget-scale index work before
// the budget could reject it — callers pass one past their remaining
// budget, and a saturated count yields the same over-budget verdict.
func (s *PG) CountRefs(ctx context.Context, repo string, limit int64) (int64, error) {
	assertf(repo != "", "CountRefs: empty repo")
	assertf(limit > 0, "CountRefs: non-positive limit %d", limit)
	var n int64
	if err := s.pool.QueryRow(ctx, `
SELECT COUNT(*) FROM (SELECT 1 FROM refs WHERE repo = $1 LIMIT $2) capped`,
		repo, limit).Scan(&n); err != nil {
		return 0, fmt.Errorf("count refs %s: %w", repo, err)
	}
	return n, nil
}

// lfsClaimTwin maps an lfs_objects row key to the canonical key its fleet
// adoption claims are minted under (the ADR-41 spellings: canonical
// `lfs/<repo>/o2/<oid>`, legacy `lfs/<repo>/<oid>`) — every server-side
// canonical materialization claims the canonical spelling, and retiring a
// legacy twin mid-region invalidates the same zero-growth verdict. An
// unparseable key maps to itself; the claim probe then finds nothing.
func lfsClaimTwin(repo, key string) string {
	oid, ok := strings.CutPrefix(key, "lfs/"+repo+"/")
	if !ok {
		return key
	}
	oid = strings.TrimPrefix(oid, "o2/")
	return "lfs/" + repo + "/o2/" + oid
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
// reclamation mark has survived a full grace window — the issue #73 GC
// verdict. The object-count and usage-byte refund is DEFERRED (Codex
// round 33 on PR #132): it parks in a per-epoch refund bucket and settles
// through SettleLFSRefunds only after a destructive sweep whose epoch
// strictly exceeds this verdict's — a sweep that provably listed blobs
// after this deletion, so the orphan was reaped (or the key re-registered
// with fresh accounting) before the capacity is handed back. It is the same guard
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
// reapMaxAge is the observed-coupling bound (issue #120): the verdict
// re-reads the sweeper's "last completed destructive LFS pass" watermark
// (job_scan_state, ScanJobSweep/ScanScopeLFSDestructiveReap) INSIDE the
// transaction and refuses with ErrReapWatermarkStale when the watermark is
// absent or older than the bound. Refunding an object-cap slot is only
// sound while the physical reap is demonstrably completing — a configured
// sweep cadence says nothing when deletes fail, a pass outruns the
// reclamation cycle, or the versioning guard demotes sweeps to list-only
// mid-run. The comparison runs on the database clock at both ends (the
// watermark is stamped with now()), so replica clock skew cannot widen the
// window.
//
// retired=false means the verdict lost — fence drift, purge fence, cleared
// or young mark, or a vanished row — and the caller moves on; the next scan
// re-derives everything from durable state.
func (s *PG) RetireUnreferencedLFSObject(ctx context.Context, repo, key string, pinnedGens map[string]int64, grace, reapMaxAge time.Duration) (bool, error) {
	assertf(repo != "" && key != "", "RetireUnreferencedLFSObject: empty repo/key")
	assertf(len(pinnedGens) > 0, "RetireUnreferencedLFSObject: no pinned generations")
	assertf(grace >= 0, "RetireUnreferencedLFSObject: negative grace %v", grace)
	assertf(reapMaxAge > 0, "RetireUnreferencedLFSObject: non-positive reap watermark bound %v", reapMaxAge)
	retired := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockBaseLedger(ctx, tx, repo); err != nil {
			return err
		}
		var reapPayload string
		switch err := tx.QueryRow(ctx, `
SELECT payload FROM job_scan_state
  WHERE job = $1 AND scope = $2
    AND updated_at > now() - make_interval(secs => $3)`,
			ScanJobSweep, ScanScopeLFSDestructiveReap, reapMaxAge.Seconds()).Scan(&reapPayload); {
		case errors.Is(err, pgx.ErrNoRows):
			return ErrReapWatermarkStale
		case err != nil:
			return err
		}
		// A legacy-spelled row's blob is only reaped by legacy-armed
		// sweeps (Codex round 17 on PR #132): the fresh watermark must
		// certify that class or the refund is unbacked.
		legacyRow := !strings.Contains(key, "/o2/")
		if legacyRow && !DecodeReapWatermark(reapPayload).Legacy {
			return ErrLegacyReapUnarmed
		}
		// Pin the destructive-sweep epoch this retirement runs under
		// (Codex round 33 on PR #132): FOR SHARE lets concurrent
		// retirements read together while forcing BeginLFSReapEpoch's
		// increment to wait for in-flight retire commits — so a sweep
		// with a strictly greater epoch provably listed blobs AFTER
		// this row's deletion became visible, the relation refund
		// settlement gates on. Absent row (no destructive sweep ever
		// started) pins epoch 0; the first completion carries epoch ≥ 1.
		var reapEpoch int64
		switch err := tx.QueryRow(ctx, `
SELECT generation FROM job_scan_state WHERE job = $1 AND scope = $2 FOR SHARE`,
			ScanJobSweep, ScanScopeLFSReapEpoch).Scan(&reapEpoch); {
		case errors.Is(err, pgx.ErrNoRows):
			reapEpoch = 0
		case err != nil:
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
		// A live per-key adoption claim fences retirement (Codex round
		// 11): every server-side canonical materialization — the PUT
		// publish/register region, adoption and compat copies — holds the
		// fleet claim with cadenced renewal, and its zero-growth verdict
		// assumes the row's slot and byte credit survive the region.
		// Retiring a claimed key would refund bytes another admission
		// could spend before the region's registration re-credits them
		// uncovered. Claim minting runs under the same base-ledger lock
		// as this transaction, so the check is totally ordered; sparing
		// is retryable — the next pass re-examines the row once the claim
		// releases or expires.
		var claimed bool
		if err := tx.QueryRow(ctx, `
SELECT EXISTS(SELECT 1 FROM pack_reservations
  WHERE key = $1 AND created_at > now() - make_interval(secs => $2))`,
			lfsClaimTwin(repo, key)+AdoptionClaimSuffix, AdoptionClaimTTL.Seconds()).Scan(&claimed); err != nil {
			return err
		}
		if claimed {
			return nil // an active materialization owns this key: spared
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
		// Park the refund in the epoch bucket instead of refunding here
		// (Codex round 33 on PR #132): the fresh completion watermark
		// only proves a sweep that ran BEFORE this deletion — it saw
		// the registry row and spared the blob, so an immediate refund
		// hands back capacity while the bytes still exist physically.
		// The count and usage stay charged until SettleLFSRefunds
		// observes a completion with a STRICTLY GREATER epoch. The
		// read-modify-write is serialized by the base-ledger lock every
		// bucket writer holds.
		scope := ScanScopeReclaimRefund(repo, reapEpoch)
		var bucket LFSRefundBucket
		var payload string
		switch err := tx.QueryRow(ctx,
			`SELECT payload FROM job_scan_state WHERE job = $1 AND scope = $2`,
			ScanJobLFSReclaim, scope).Scan(&payload); {
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil:
			return err
		default:
			if err := json.Unmarshal([]byte(payload), &bucket); err != nil {
				// Undecodable bucket (never written by this binary):
				// fail the verdict rather than silently dropping or
				// double-counting pending refunds.
				return fmt.Errorf("undecodable refund bucket %s: %w", scope, err)
			}
		}
		if legacyRow {
			bucket.LegacyRows, bucket.LegacyBytes = bucket.LegacyRows+1, bucket.LegacyBytes+bytes
		} else {
			bucket.Rows, bucket.Bytes = bucket.Rows+1, bucket.Bytes+bytes
		}
		merged, mErr := json.Marshal(bucket)
		assertf(mErr == nil, "retire lfs object: marshal refund bucket: %v", mErr)
		if _, err := tx.Exec(ctx, `
INSERT INTO job_scan_state (job, scope, payload, generation, updated_at)
VALUES ($1, $2, $3, $4, now())
ON CONFLICT (job, scope) DO UPDATE SET payload = EXCLUDED.payload, updated_at = now()`,
			ScanJobLFSReclaim, scope, string(merged), reapEpoch); err != nil {
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

// SettleLFSRefunds refunds this base's pending retirement buckets that a
// completed destructive sweep now covers (Codex round 33 on PR #132): a
// bucket's canonical part settles under any completion whose epoch
// strictly exceeds the bucket's, its legacy part only under a
// legacy-armed completion's epoch (canonical-only sweeps spare legacy
// orphans). The count/usage decrements and the adoption-generation bump
// (floor-cache invalidation, the ADR-41 refund pattern) run atomically
// with the bucket rewrite under the base-ledger lock. Purge-fenced repos
// settle nothing: the purge drops the whole ledger, buckets included.
func (s *PG) SettleLFSRefunds(ctx context.Context, repo string) (SettledLFSRefunds, error) {
	assertf(repo != "", "SettleLFSRefunds: empty repo")
	var out SettledLFSRefunds
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		out = SettledLFSRefunds{}
		if err := lockBaseLedger(ctx, tx, repo); err != nil {
			return err
		}
		if fenced, err := repoPurgeFencedPG(ctx, tx, repo); err != nil || fenced {
			return err
		}
		epochFor := func(scope string) (int64, error) {
			var payload string
			switch err := tx.QueryRow(ctx,
				`SELECT payload FROM job_scan_state WHERE job = $1 AND scope = $2`,
				ScanJobSweep, scope).Scan(&payload); {
			case errors.Is(err, pgx.ErrNoRows):
				return 0, nil
			case err != nil:
				return 0, err
			}
			return DecodeReapWatermark(payload).Epoch, nil
		}
		canonEpoch, err := epochFor(ScanScopeLFSDestructiveReap)
		if err != nil {
			return err
		}
		legacyEpoch, err := epochFor(ScanScopeLFSDestructiveReapLegacy)
		if err != nil {
			return err
		}
		prefix := ScanScopeReclaimRefundPrefix(repo)
		type bucketRow struct {
			scope   string
			payload string
			epoch   int64
		}
		var refundRows, refundBytes int64
		// Keyset-page the WHOLE per-repo bucket keyspace (Codex round 35):
		// scopes are epoch-ordered, so a single fixed page could pin the
		// scan on old buckets whose legacy halves cannot settle (legacy
		// reaping disabled after repeated failures) while newer
		// canonical-only buckets waited past the limit forever. Settled
		// buckets vanish and stuck ones accumulate at most one per
		// destructive-sweep epoch, so the walk stays operator-bounded.
		for after := prefix; ; {
			var buckets []bucketRow
			rows, err := tx.Query(ctx, `
SELECT scope, payload, generation FROM job_scan_state
WHERE job = $1 AND left(scope, length($2)) = $2 AND scope COLLATE "C" > $3
ORDER BY scope COLLATE "C" ASC LIMIT $4`,
				ScanJobLFSReclaim, prefix, after, lfsRefundBucketPage)
			if err != nil {
				return err
			}
			for rows.Next() {
				var b bucketRow
				if err := rows.Scan(&b.scope, &b.payload, &b.epoch); err != nil {
					rows.Close()
					return err
				}
				buckets = append(buckets, b)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
			for _, b := range buckets {
				var bucket LFSRefundBucket
				if err := json.Unmarshal([]byte(b.payload), &bucket); err != nil {
					return fmt.Errorf("undecodable refund bucket %s: %w", b.scope, err)
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
					if _, err := tx.Exec(ctx,
						`DELETE FROM job_scan_state WHERE job = $1 AND scope = $2`,
						ScanJobLFSReclaim, b.scope); err != nil {
						return err
					}
				case settled:
					merged, mErr := json.Marshal(bucket)
					assertf(mErr == nil, "settle lfs refunds: marshal bucket: %v", mErr)
					if _, err := tx.Exec(ctx,
						`UPDATE job_scan_state SET payload = $3, updated_at = now() WHERE job = $1 AND scope = $2`,
						ScanJobLFSReclaim, b.scope, string(merged)); err != nil {
						return err
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
			if _, err := tx.Exec(ctx,
				`UPDATE lfs_object_counts SET count = GREATEST(count - $2, 0) WHERE repo = $1`, repo, refundRows); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx,
				`UPDATE pack_usage SET bytes = GREATEST(bytes - $2, 0) WHERE repo = $1`, repo, refundBytes); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
INSERT INTO lfs_adoption_gens (repo, gen) VALUES ($1, 1)
ON CONFLICT (repo) DO UPDATE SET gen = lfs_adoption_gens.gen + 1`, repo); err != nil {
				return err
			}
		}
		out.Rows, out.Bytes = refundRows, refundBytes
		return nil
	})
	if err != nil {
		return SettledLFSRefunds{}, fmt.Errorf("settle lfs refunds %s: %w", repo, err)
	}
	return out, nil
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
