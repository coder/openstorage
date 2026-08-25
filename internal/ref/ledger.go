// Durable pack-quota ledger (issues #29 + #31): reservation, usage, and
// retained-byte state living in the ref-authority store so admission is
// atomic across nodes and replicas.
//
// Three tables (created by InitSchema on both backends):
//
//   - pack_reservations(key PK, repo, bytes, owner, created_at, attempts,
//     gen): one row per in-flight incoming pack, keyed by the
//     content-addressed blob key under this binary's era spelling
//     (reservRowKey — the v0.1.0 mixed-fleet fence). Inserted by
//     ReservePackBytes BEFORE the blob upload; consumed (deleted) by
//     RegisterPacks in the registration transaction; expired rows are
//     ignored by admission and pruned on the sweeper cadence. A crash
//     between reserve and register therefore over-reserves for at most
//     the TTL — admission control, never a byte-exact ledger.
//   - pack_usage(repo PK, bytes): the maintained per-repo usage counter.
//     RegisterPacks credits newly inserted rows' sizes; ReplacePacks
//     credits the fold's consolidated pack; ReclaimRetainedPack debits
//     superseded bytes when their blob actually leaves the pack prefix.
//   - pack_retained(repo+key PK, bytes): blob keys superseded by a
//     ReplacePacks fold (issue #31). Their rows left the registry but the
//     blobs linger under packs/ until a sweep reaps them, so their bytes
//     STAY in pack_usage (nothing is subtracted at fold time) and this
//     table records what to debit at reclaim — and identifies the
//     provably-folded class the sweeper may delete by default.
//
// Admission formula (ReservePackBytes, all in one transaction):
//
//	SUM(pack_usage over group) + usageFloor
//	  + SUM(active reservations for the base repo, excluding this push's keys)
//	  + incoming  <=  quota
//
// usageFloor is the caller's UNCOUNTED-bytes floor: its blob-listing
// measurement (blob.StorageUsageBytes) MINUS the usage counter read at
// that listing's scan start, clamped at zero. It is kept rather than
// retired (deviating from issue #29's "kill the listing" sketch) because
// it covers bytes no counter can know: repos whose packs predate the
// ledger, pre-registry LFS objects, and orphaned uploads from crashed
// pushes that never registered. The live counter covers everything
// registered — including settlements AFTER the floor's scan, which a
// max() of the two combined measurements hid whenever uncounted bytes
// coexisted with fresh settlements (Codex round 44). The sum counts a
// byte twice only when its blob was listed before its registration
// committed inside the scan-to-admission window — transient, and on the
// deny side.
//
// Serialization: reserve-vs-reserve for one base repo is serialized (PG: a
// per-base advisory xact lock in a keyspace disjoint from the schema-init
// locks; SQLite: the single-writer BEGIN IMMEDIATE), so two racing pushes
// cannot admit against the same snapshot — the issue-#29 exploit (two
// pushes that individually fit, together exceed) loses deterministically.
// Reserve-vs-register needs no lock: the usage/reservation read is ONE
// statement (one snapshot), and RegisterPacks moves a key's bytes from
// reservation to counter atomically in its own transaction, so any snapshot
// counts those bytes exactly once. Registrations only conserve or increase
// accounted bytes; the only decrease (ReclaimRetainedPack) racing a reserve
// can at worst deny a push that a moment later would fit — the safe side.
//
// Remaining overshoot windows, deliberately accepted and bounded:
//   - a reservation outliving its TTL while its upload is still in flight
//     frees headroom early (bounded by that pack's size; the TTL default
//     comfortably exceeds server-side upload times);
//   - orphaned uploads are invisible to the counter until the listing floor
//     sees them (immediately on the next push's fresh listing).
//
// The versioned-bucket LWW dedup race documented at the Stat/upload sites
// (hook.go, publish.go) is deliberately NOT closed by this ledger: refusing
// a second reservation for the same live key would wedge crash-window
// retries — a client whose push died between reserve and upload would be
// locked out for the full TTL, the exact wedge class issue #21's reviews
// eliminated. Same-key re-reservation instead refreshes the row (bytes
// counted once — identical keys are identical bytes) and counts the writer
// in the row's attempts refcount (Codex round 30): clean failures retire
// attempts token-bound (RetirePackAttempts), and the row ends at
// settlement, zero attempts, or TTL — never by any owner-scoped delete.

package ref

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// NewReservationOwner mints the random owner token stamped on one writer's
// reservation rows. The owner is pure provenance (Codex round 22): rows
// end by settlement (RegisterPacks/RegisterLFSObject consume by key),
// token-bound retirement (RetirePackAttempts), or TTL expiry — same-key
// rows are shared by every concurrent writer via refresh, so no
// owner-scoped delete can ever be sound. The mixed-fleet fence against
// v0.1.0's owner-scoped release lives in the row SPELLING, not in this
// column's value — see reservKeySuffix.
func NewReservationOwner() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("ref assertion failed: reservation owner entropy: %v", err))
	}
	return hex.EncodeToString(b[:])
}

// reservKeySuffix marks reservation rows minted by attempt-protocol
// binaries (Codex round 35). v0.1.0 replicas address rows by RAW blob key,
// and their statements are destructive to any row they reach: the legacy
// upsert CAPTURES a shared row (owner rotated to the legacy token, bytes
// clobbered — no GREATEST), and the legacy owner-scoped
// ReleasePackReservations then deletes it on a clean failure — freeing
// headroom every other same-key writer still stages under. Round 34's
// owner rotation fenced only the ordering where this binary's refresh ran
// last; no column VALUE survives a later legacy refresh. So this binary's
// rows are fenced by SPELLING instead: blob keys are single-line paths
// (server-generated IDs, hex digests, fixed segments — never a newline),
// making era rows unreachable by every legacy plain-key statement in
// EITHER order. Both eras still meet in the repo-scoped admission SUMs,
// so a cross-era same-key twin double-counts reserved bytes
// conservatively until the legacy row settles, releases, or expires
// (deny-safe, and only during the upgrade roll).
const reservKeySuffix = "\na2"

// reservRowKey maps a blob key to this binary's reservation-row spelling.
func reservRowKey(key string) string {
	assertf(key != "" && !strings.Contains(key, "\n"), "reservRowKey(%q)", key)
	return key + reservKeySuffix
}

// reservRowKeys maps blob keys to this binary's row spellings — the rows a
// reserve refreshes rather than inserts (the row-cap exclusion set; a
// legacy twin stays counted there because it is a physical row the upsert
// does NOT refresh).
func reservRowKeys(keys []string) []string {
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = reservRowKey(k)
	}
	return out
}

// reservKeySpellings returns every spelling a blob key's reservation may
// occupy — this binary's era row plus a legacy twin. Settlement consumes
// both (the key's bytes just moved to the usage counter, so either row is
// a stale double-count; v0.1.0's own settle consumes by raw key too), and
// admission byte-SUMs exclude both (the batch's incoming counts those
// bytes exactly once). Clean-failure retirement must NOT use this set:
// nothing was credited, and the legacy twin may still protect a live
// legacy writer — see RetirePackAttempts.
func reservKeySpellings(keys ...string) []string {
	out := make([]string, 0, 2*len(keys))
	for _, k := range keys {
		out = append(out, reservRowKey(k), k)
	}
	return out
}

// ReserveNoFloorGen skips ReservePackBytes' transactional floor-generation
// recheck (Codex round 60). Legitimate only where the reservation is not a
// floor-based admission: restore covers (unbounded quota — the floor input
// is a constant 0) and quota-disabled maintenance paths. Every real
// admission must pass the generation its floor was validated against.
const ReserveNoFloorGen int64 = -1

// AdoptionClaimSuffix distinguishes an adoption claim row (see
// RefStore.TryAdoptionClaim) from real reservations and covers: LFS keys
// end in the 64-hex oid, so the suffix never collides, and neither
// settlement (blob-key spellings), retirement (era rows), nor the
// admission GC (which excludes this suffix — Codex round 74: the GC runs
// with the ADMISSION ttl, as short as ~61s, while a legitimate hash runs
// 5m + 1s/MiB) ever touches it. A live claim contributes one byte to
// admission sums (deny-side, negligible); the sweeper prune reaps claims
// older than AdoptionClaimTTL.
const AdoptionClaimSuffix = "/adopt-claim"

// AdoptionClaimTTL is the lease length of one adoption claim: generously
// longer than the renewal cadence the hashing winner maintains (renewals
// restart the clock chunk-by-chunk), yet finite so a crashed winner's
// claim expires and the next write-scoped touch takes over.
const AdoptionClaimTTL = 30 * time.Minute

// TryAdoptionClaim — see RefStore.
func (s *PG) TryAdoptionClaim(ctx context.Context, repo, key string) (bool, string, error) {
	assertf(repo != "" && key != "", "TryAdoptionClaim(%q, %q)", repo, key)
	owner := NewReservationOwner()
	claimed := false
	// The claim runs under the base-ledger lock like every other
	// row-creating writer (issue #52, PR #105 rounds 13–14): a pre-delete
	// LFS touch must not mint a claim row behind a completed purge on
	// no-sweep deployments, and the shared lock totally orders the fence
	// check against FenceRepoPurge — a statement-snapshot predicate alone
	// could take its snapshot before the fence committed yet insert after
	// the ledger cleanup. Fenced repos get claimed=false: the caller
	// simply skips adoption.
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockBaseLedger(ctx, tx, baseRepoOf(repo)); err != nil {
			return err
		}
		if fenced, err := repoPurgeFencedPG(ctx, tx, repo); err != nil || fenced {
			return err
		}
		ct, err := tx.Exec(ctx, `
INSERT INTO pack_reservations (key, repo, bytes, owner, created_at, gen)
VALUES ($1, $2, 1, $3, now(), '')
ON CONFLICT (key) DO UPDATE SET repo = EXCLUDED.repo, owner = EXCLUDED.owner, created_at = now()
WHERE pack_reservations.created_at <= now() - make_interval(secs => $4)`,
			key+AdoptionClaimSuffix, repo, owner, AdoptionClaimTTL.Seconds())
		if err != nil {
			return err
		}
		claimed = ct.RowsAffected() > 0
		return nil
	})
	if err != nil {
		return false, "", err
	}
	if !claimed {
		return false, "", nil
	}
	return true, owner, nil
}

// RenewAdoptionClaim — see RefStore.
func (s *PG) RenewAdoptionClaim(ctx context.Context, key, owner string) (bool, error) {
	assertf(key != "" && owner != "", "RenewAdoptionClaim(%q, %q)", key, owner)
	ct, err := s.pool.Exec(ctx, `
UPDATE pack_reservations SET created_at = now()
WHERE key = $1 AND owner = $2 AND created_at > now() - make_interval(secs => $3)`,
		key+AdoptionClaimSuffix, owner, AdoptionClaimTTL.Seconds())
	if err != nil {
		return false, err
	}
	return ct.RowsAffected() > 0, nil
}

// ReleaseAdoptionClaim — see RefStore.
func (s *PG) ReleaseAdoptionClaim(ctx context.Context, key, owner string) error {
	assertf(key != "" && owner != "", "ReleaseAdoptionClaim(%q, %q)", key, owner)
	_, err := s.pool.Exec(ctx,
		`DELETE FROM pack_reservations WHERE key = $1 AND owner = $2`, key+AdoptionClaimSuffix, owner)
	return err
}

// lfsLegacyRowKey maps a canonical LFS object key — blob.LFSKey's
// lfs/<repo>/o2/<oid> layout (ADR-41) — to the spelling v0.2.0 stored in
// lfs_objects rows: lfs/<repo>/<oid>. ok is false for every other key
// shape. This is deliberately the ledger's only knowledge of the LFS key
// layout: registration uses it to migrate a legacy row in place (zero
// growth) instead of double-counting the object.
func lfsLegacyRowKey(key string) (string, bool) {
	if !strings.HasPrefix(key, "lfs/") {
		return "", false
	}
	// Split at the LAST "/o2/": the oid is the final slash-free segment
	// (blob.LFSKey asserts that), so this stays correct for any repo
	// segment content.
	i := strings.LastIndex(key, "/o2/")
	if i < len("lfs/") { // absent, or an empty repo segment
		return "", false
	}
	oid := key[i+len("/o2/"):]
	if oid == "" || strings.Contains(oid, "/") {
		return "", false
	}
	return key[:i] + "/" + oid, true
}

// RepairCoverSuffix distinguishes a restore's cover reservation key from
// the restored blob's settle key (Codex rounds 57+58): settlement consumes
// the blob key's own spellings, and the cover must instead outlive the
// request until the TTL collects it. Pack keys end in ".pack" and LFS keys
// in the 64-hex oid, while the ledger reserves "\n" for its era spellings,
// so this suffix never collides with a real reservation.
const RepairCoverSuffix = "/repair-cover"

// ReserveRestoreCover accounts bytes a writer-side restore is about to
// re-materialize under an already-registered key (packs and LFS alike,
// Codex rounds 57+58). The shared floor-generation bump after a restore
// only stops FUTURE floor serves: an admission already holding a floor
// scanned during the blob's absence (deflated by the missing object's
// size, hiding equally sized rowless legacy bytes) can commit after the
// bytes return and durably exceed quota. The cover makes those admissions
// see the returning bytes in active_reservations instead. It reserves
// under an unbounded quota — restores must never be quota-denied, or a
// registered row stays blobless — but the row cap still binds and any
// denial fails the restore (the retry converges); the TTL collects the
// row after the last stale floor can commit.
func ReserveRestoreCover(ctx context.Context, st RestoreCoverStore, group []string, key string, size int64, ttl time.Duration) error {
	assertf(size > 0, "ReserveRestoreCover(%q): size %d", key, size)
	dec, err := st.ReservePackBytes(ctx, group,
		[]Pack{{Key: key + RepairCoverSuffix, Size: size}}, NewReservationOwner(),
		0, ReserveNoFloorGen, math.MaxInt64, ttl)
	if err != nil {
		return err
	}
	if dec.KeyCapExceeded {
		return fmt.Errorf("restore cover for %s: too many in-flight reservations, retry", key)
	}
	if !dec.OK {
		// Unreachable with an unbounded quota (admission saturates);
		// fail closed rather than restore uncovered.
		return fmt.Errorf("restore cover for %s denied: %d used, %d reserved", key, dec.UsedBytes, dec.ReservedBytes)
	}
	return nil
}

// RestoreCoverStore is the single-method ledger slice ReserveRestoreCover
// needs, so every restore path — server handlers, the pre-receive hook,
// and the sweeper's narrow PackRegistry — can share the one cover
// implementation.
type RestoreCoverStore interface {
	ReservePackBytes(ctx context.Context, group []string, packs []Pack, owner string, usageFloor, floorGen, quotaBytes int64, ttl time.Duration) (ReserveDecision, error)
}

// ReserveDecision reports one admission decision. UsedBytes and
// ReservedBytes carry the arithmetic the caller folds into its denial
// message (the client-facing sideband line / 403 detail).
type ReserveDecision struct {
	// OK reports whether the reservation was admitted and inserted.
	OK bool
	// FloorStale denies because the caller's usage floor was validated
	// against a shared floor generation that moved before this
	// transaction (Codex round 60, security): a restore, adoption, or
	// retained-bytes debit changed what the floor measures between the
	// caller's generation check and this reserve — the classic
	// check-then-act window no post-hoc invalidation can reach. The
	// caller must re-derive a fresh floor (with its generation) and
	// retry; admitting against the stale integer could durably exceed
	// quota.
	FloorStale bool
	// UsedBytes is the usage the decision charged: the max of the ledger
	// counter and the caller's listing floor.
	UsedBytes int64
	// ReservedBytes is the active in-flight reservations charged on top of
	// UsedBytes (other pushes' keys only).
	ReservedBytes int64
	// Gens maps each reserved key to its row-incarnation gen (Codex round
	// 33): minted at INSERT, preserved across same-key refreshes.
	// RetirePackAttempts requires it, so a stale writer whose row expired
	// and was replaced can never retire the replacement. Within era rows
	// (reservKeySuffix) every refresh increments attempts, so gen plus the
	// attempts refcount fully identify a retirable row — round 33's extra
	// created_at comparison was dropped with the era fence (Codex round
	// 35: it retained reverse-order zero-attempt rows to the TTL, letting
	// failed tiny same-key upload pairs wedge the row cap). Set only when
	// OK.
	Gens map[string]string
	// RenewalDegraded denies because the absence reconciler's cover
	// renewal is in a degraded window (Codex round 26 on PR #132,
	// security): the TTL'd compensation covers this admission counts in
	// active_reservations may have lapsed un-renewed, deflating the
	// quota floor, so finite quota-gated admissions fail closed until a
	// complete recovery rotation re-covers every registered-but-missing
	// byte. Unbounded internal reservations (restores, covers — quota
	// math.MaxInt64) are exempt so recovery itself can proceed.
	// Transient from the client's perspective: retry later.
	RenewalDegraded bool
	// KeyCapExceeded reports that the denial was the per-repo cap on
	// ACTIVE reservation ROWS, not byte arithmetic (Codex security round
	// on #51): declared bytes bound row count at nothing — 1-byte LFS
	// batch entries could mint durable rows ~quota times over — so the
	// row population needs its own bound. Transient: rows expire with the
	// TTL, so callers should tell the client to retry later.
	KeyCapExceeded bool
}

// maxActiveReservations caps ACTIVE (non-expired) reservation rows per base
// repository, counted in the same admission snapshot as the byte sums. A
// compiled bound like the repack fold budgets: it exists to keep an abusive
// writer from growing the shared ref database (each row is ~100 bytes, so
// the cap bounds per-repo reservation state at ~1 MiB), not to tune
// concurrency — legitimate writers hold at most a few in-flight keys per
// push and up to one LFS batch (≤1000) at a time. A var so tests can
// exercise the cap without minting thousands of rows.
var maxActiveReservations = 10_000

// MaxLFSObjectsPerRepo caps a repository's REGISTERED LFS object count
// (issue #51, Codex security round 2): registered objects are permanent
// rows plus bucket keys, and the byte quota bounds their population at
// nothing (16M three-byte objects cost 48 MiB). Enforced ATOMICALLY inside
// RegisterLFSObject's serialized transaction (Codex round 6: an
// HTTP-layer read-then-register check admits concurrent overshoot); the
// LFS surface also pre-checks it for fast-fail UX. A var for tests.
var MaxLFSObjectsPerRepo = int64(100_000)

// ErrLFSObjectCapExceeded is RegisterLFSObject's typed denial: inserting
// this NEW object would take the repository past MaxLFSObjectsPerRepo.
// Re-registration of an existing key never returns it (zero growth).
var ErrLFSObjectCapExceeded = errors.New("repository LFS object count limit reached")

// lockBaseLedger takes the per-base advisory lock serializing quota
// admissions (reserveLockClass) inside tx. Every transaction that BUMPS
// the shared floor generation must take it BEFORE its first row work
// (Codex round 65): ReservePackBytes' transactional generation recheck is
// a plain snapshot read, so a bump committing between that read and the
// reserve's commit would evade it — under the shared lock the bump either
// commits before the reserve's recheck (which then sees the moved
// generation) or strictly after the reserve committed (whose admission
// used the pre-bump counter — the conservative side). Lock-first ordering
// also keeps bump transactions deadlock-free against reserves touching
// the same reservation rows.
func lockBaseLedger(ctx context.Context, tx pgx.Tx, base string) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), $2)`, base, reserveLockClass)
	return err
}

// reserveLockClass namespaces the per-base advisory locks ReservePackBytes
// takes ("PKQR"), in the two-int4 pg_advisory_xact_lock keyspace — disjoint
// from the single-int8 keys the schema-init paths use (META, SPIKE, RIVER).
const reserveLockClass = int32(0x504B5152)

// validateReserve asserts the ReservePackBytes argument invariants shared
// by both backends. group[0] is the BASE repository (the quota accounting
// unit reservations are keyed under); every other member must be one of its
// namespaces. packs carry the incoming (unpublished) blobs: distinct keys,
// positive sizes — an already-published key must not be re-reserved (it
// contributes zero growth and is dedup-skipped by the caller).
func validateReserve(group []string, packs []Pack, owner string, usageFloor, floorGen, quotaBytes int64, ttl time.Duration) {
	assertf(len(group) > 0 && group[0] != "", "ReservePackBytes: empty repo group")
	for _, r := range group[1:] {
		assertf(strings.HasPrefix(r, group[0]), "ReservePackBytes: group member %q not a namespace of base %q", r, group[0])
	}
	assertf(len(packs) > 0, "ReservePackBytes: no packs to reserve")
	seen := make(map[string]bool, len(packs))
	for _, p := range packs {
		assertf(p.Key != "" && p.Size > 0, "ReservePackBytes: pack needs key and positive size: %+v", p)
		assertf(!seen[p.Key], "ReservePackBytes: duplicate key %q", p.Key)
		seen[p.Key] = true
	}
	assertf(owner != "", "ReservePackBytes: empty owner")
	assertf(usageFloor >= 0, "ReservePackBytes: negative usage floor %d", usageFloor)
	assertf(floorGen >= ReserveNoFloorGen, "ReservePackBytes: invalid floor generation %d", floorGen)
	assertf(quotaBytes > 0, "ReservePackBytes: non-positive quota %d (0 disables the quota at the caller)", quotaBytes)
	assertf(ttl > 0, "ReservePackBytes: non-positive ttl %v", ttl)
}

// addSat returns a+b saturated at MaxInt64. Admission arithmetic sums
// CLIENT-DECLARED sizes (LFS batch entries are arbitrary int64s), so a
// naive sum can wrap negative and admit anything (Codex round 2 on #51);
// saturation keeps every overflowing total on the denying side of the
// quota comparison instead.
func addSat(a, b int64) int64 {
	assertf(a >= 0 && b >= 0, "addSat(%d, %d): negative addend", a, b)
	if a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

// incomingBytes sums the packs' sizes (validated positive), saturating at
// MaxInt64 — see addSat.
func incomingBytes(packs []Pack) int64 {
	var total int64
	for _, p := range packs {
		total = addSat(total, p.Size)
	}
	return total
}

// IncomingBytes is the exported saturating declared-size sum for admission
// callers. The LFS batch surface sums CLIENT-DECLARED sizes before its
// advisory Admitted check; a naive sum of two MaxInt64 entries wraps
// negative and panics Admitted's non-negative assertion instead of denying
// (Codex round 21), so the sum must use the ledger's own arithmetic.
func IncomingBytes(packs []Pack) int64 {
	return incomingBytes(packs)
}

// admitted reports whether incoming fits: used + reserved + incoming <=
// quota, computed with saturation so oversized declared totals deny
// instead of wrapping past the quota. A saturated total equal to MaxInt64
// is treated as over ANY quota — a genuine exactly-MaxInt64 admission is
// indistinguishable from an overflow and no real deployment stores 8 EiB.
func admitted(used, reserved, incoming, quota int64) bool {
	total := addSat(addSat(used, reserved), incoming)
	return total <= quota && total != math.MaxInt64
}

// Admitted is the exported, side-effect-free admission arithmetic — the
// same saturating comparison ReservePackBytes applies transactionally. The
// LFS batch surface uses it for its ADVISORY fast-fail check (issue #51,
// Codex round 17): batch-time durable holds proved rotatable (a fresh OID
// per TTL re-acquires the whole headroom without ever transferring), so
// batches insert nothing and only real transfer progress reserves.
func Admitted(used, reserved, incoming, quota int64) bool {
	return admitted(used, reserved, incoming, quota)
}

// AddSat is the exported saturating sum, for callers combining the usage
// counter with the uncounted-bytes floor outside the ledger (the LFS
// batch's advisory check) exactly as ReservePackBytes does (round 44).
func AddSat(a, b int64) int64 { return addSat(a, b) }

// ReservePackBytes implements the admission check documented atop this file
// on Postgres.
func (s *PG) ReservePackBytes(ctx context.Context, group []string, packs []Pack, owner string, usageFloor, floorGen, quotaBytes int64, ttl time.Duration) (ReserveDecision, error) {
	validateReserve(group, packs, owner, usageFloor, floorGen, quotaBytes, ttl)
	keys := make([]string, len(packs))
	for i, p := range packs {
		keys[i] = p.Key
	}
	incoming := incomingBytes(packs)
	var dec ReserveDecision
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Serialize reserve-vs-reserve per base repo (see the header).
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), $2)`, group[0], reserveLockClass); err != nil {
			return err
		}
		// Purge fence (issue #52, PR #105 round 9): the lock above is the
		// one FenceRepoPurge takes, so a reservation for a purge-fenced
		// repo can never be minted after the purge's ledger cleanup and
		// leak forever — the writer fails at admission instead.
		if fenced, err := repoPurgeFencedPG(ctx, tx, group[0]); err != nil {
			return err
		} else if fenced {
			return fmt.Errorf("reserve pack bytes %s: %w", group[0], ErrRepoPurged)
		}
		// Cover-renewal degraded fence (Codex round 26 on PR #132,
		// security): while armed, TTL'd absence covers may have lapsed
		// un-renewed and this transaction's strict-TTL reservation SUM
		// under-counts them — finite quota-gated admissions fail
		// closed (see ReserveDecision.RenewalDegraded); unbounded
		// internal reservations proceed so recovery can re-cover.
		if quotaBytes != math.MaxInt64 {
			var degraded bool
			if err := tx.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM job_scan_state WHERE job = $1 AND scope = $2)`,
				ScanJobAbsence, ScanScopeAbsenceAdmissionFence).Scan(&degraded); err != nil {
				return err
			}
			if degraded {
				dec = ReserveDecision{RenewalDegraded: true}
				return nil
			}
		}
		// Self-healing GC (Codex round 5 on #51): expired rows already
		// stopped gating admission, but they occupy physical rows the
		// sweeper may never prune (sweeping can be disabled). Pruning this
		// base repo's expired rows inside the serialized admission
		// transaction bounds the repo's PHYSICAL row population at the
		// active-row cap plus one TTL of churn, with no sweeper required.
		// Deleting an expired UN-FOLDED absence-cover row destroys the
		// physical lapse evidence the reconciler's first-health-stamp
		// probe arms on (Codex round 33 on PR #132) — the CTE records
		// the durable lapse-evidence marker in the same statement, so
		// the evidence survives its own consumption. Folded rows decay
		// deliberately and are never evidence.
		if _, err := tx.Exec(ctx, `
WITH doomed AS (
  DELETE FROM pack_reservations WHERE repo = $1 AND created_at <= now() - make_interval(secs => $2)
    AND key NOT LIKE '%/adopt-claim'
  RETURNING key, owner
)
INSERT INTO job_scan_state (job, scope, payload, generation, updated_at)
SELECT $3, $4, key, 0, now() FROM doomed
WHERE key COLLATE "C" > $5 AND key COLLATE "C" < $6
  AND right(key, $7) = $8 AND owner <> $9
LIMIT 1
ON CONFLICT (job, scope) DO UPDATE SET payload = EXCLUDED.payload, updated_at = now()`,
			group[0], ttl.Seconds(),
			ScanJobAbsence, ScanScopeAbsenceLapse,
			AbsenceCoverPrefix, absenceCoverRangeEnd,
			len(absenceCoverRawSuffix), absenceCoverRawSuffix, AbsenceFoldedOwner); err != nil {
			return err
		}
		// ONE statement = one snapshot: a concurrent registration moves a
		// key's bytes from reservation to counter atomically, so either
		// side of that move is counted here, never neither. The byte SUM
		// excludes BOTH spellings of this push's keys (a legacy twin's
		// bytes are this push's own incoming, counted once via `incoming`)
		// while the row-cap COUNT excludes only this binary's spellings —
		// see reservRowKeys.
		var counter, reserved, activeRows int64
		if err := tx.QueryRow(ctx, `
SELECT
  COALESCE((SELECT SUM(bytes) FROM pack_usage WHERE repo = ANY($1)), 0),
  COALESCE((SELECT SUM(bytes) FROM pack_reservations
            WHERE repo = $2
              AND created_at > now() - make_interval(secs => $3)
              AND key NOT LIKE '%`+AdoptionClaimSuffix+`'
              AND NOT (key = ANY($4))), 0),
  (SELECT COUNT(*) FROM pack_reservations
   WHERE repo = $2
     AND created_at > now() - make_interval(secs => $3)
     AND key NOT LIKE '%`+AdoptionClaimSuffix+`'
     AND NOT (key = ANY($5)))`,
			group, group[0], ttl.Seconds(), reservKeySpellings(keys...), reservRowKeys(keys)).Scan(&counter, &reserved, &activeRows); err != nil {
			return err
		}
		// SUM, not max (Codex round 44): the floor is the UNCOUNTED-bytes
		// snapshot (listing minus the counter at its scan), so bytes
		// registered after that scan appear exactly once — in the
		// counter. max() hid them whenever uncounted bytes coexisted
		// with fresh settlements.
		dec = ReserveDecision{UsedBytes: addSat(counter, usageFloor), ReservedBytes: reserved}
		// Transactional floor-generation recheck (Codex round 60,
		// security): the caller validated its floor against a generation
		// OUTSIDE this transaction, and a restore, adoption, or
		// retained-bytes debit may have moved it since — the floor
		// integer then hides bytes no post-hoc bump can protect. A moved
		// generation denies with FloorStale; the caller re-derives a
		// fresh floor and retries. Read under the same per-base
		// serialization lock as the snapshot above.
		if floorGen >= 0 {
			var gen int64
			if err := tx.QueryRow(ctx,
				`SELECT COALESCE((SELECT gen FROM lfs_adoption_gens WHERE repo = $1), 0)`,
				group[0]).Scan(&gen); err != nil {
				return err
			}
			if gen != floorGen {
				dec.FloorStale = true
				return nil // denied; dec.OK stays false
			}
		}
		if activeRows+int64(len(packs)) > int64(maxActiveReservations) {
			dec.KeyCapExceeded = true
			return nil // denied; dec.OK stays false
		}
		if !admitted(dec.UsedBytes, dec.ReservedBytes, incoming, quotaBytes) {
			return nil // denied; dec.OK stays false
		}
		for _, p := range packs {
			// Same-key upsert: a crash-window retry (or a concurrent
			// identical push) refreshes the row instead of conflicting, so
			// identical bytes are counted once and retries never wedge.
			// The row lives under this binary's era spelling (see
			// reservKeySuffix): no v0.1.0 statement can capture or delete
			// it. The refresh takes the MAXIMUM of the declared byte
			// values (Codex round 7): declared sizes are client input, so
			// a shrinking refresh would let a one-byte same-key PUT free
			// headroom a concurrent larger upload is still staging under.
			// Monotone growth until settlement is the conservative side —
			// settlement replaces the row with the verified actual size in
			// the usage counter, so an inflated declaration lives at most
			// until settle or TTL.
			// attempts counts the live same-key writers sharing this row
			// (Codex round 30): every reserve/refresh adds one, a clean
			// failure retires one (RetirePackAttempts), and the row ends
			// at zero attempts, settlement, or TTL. gen is the row's
			// incarnation (Codex round 33): a fresh INSERT mints one, a
			// refresh preserves it, and RETURNING hands the caller the
			// gen its retirement must present.
			var gen string
			if err := tx.QueryRow(ctx, `
INSERT INTO pack_reservations (key, repo, bytes, owner, gen) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (key) DO UPDATE SET repo = EXCLUDED.repo,
                                bytes = GREATEST(pack_reservations.bytes, EXCLUDED.bytes),
                                owner = EXCLUDED.owner,
                                created_at = now(),
                                attempts = pack_reservations.attempts + 1
RETURNING gen`,
				reservRowKey(p.Key), group[0], p.Size, owner, NewReservationOwner()).Scan(&gen); err != nil {
				return err
			}
			if dec.Gens == nil {
				dec.Gens = make(map[string]string, len(packs))
			}
			dec.Gens[p.Key] = gen
		}
		dec.OK = true
		return nil
	})
	if err != nil {
		return ReserveDecision{}, err
	}
	return dec, nil
}

// RetirePackAttempts — see RefStore. Serialized with ReservePackBytes per
// base repo (same advisory lock class): a retire must not interleave with
// a concurrent same-key refresh or the decrement could race the increment.
func (s *PG) RetirePackAttempts(ctx context.Context, group []string, gens map[string]string) error {
	assertf(len(group) > 0 && group[0] != "" && len(gens) > 0, "RetirePackAttempts(%v, %v)", group, gens)
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), $2)`, group[0], reserveLockClass); err != nil {
			return err
		}
		for key, gen := range gens {
			// The gen binds the decrement to the row incarnation the
			// caller actually reserved (Codex round 33): a replacement
			// row created after this caller's expired and was pruned
			// carries a different gen and is untouched. Within era rows
			// every refresh increments attempts, so a zero-attempt row
			// provably has no live writer and is deleted in ANY
			// retirement order (Codex round 35 — round 33's extra
			// created_at guard retained reverse-order zero-attempt rows
			// to the TTL, a row-cap wedge). Both statements address only
			// this binary's era spelling: retirement is a clean-FAILURE
			// path (nothing was credited), so a legacy twin row —
			// possibly protecting a live legacy writer — is left to its
			// own binary's lifecycle.
			if _, err := tx.Exec(ctx, `
UPDATE pack_reservations SET attempts = attempts - 1 WHERE repo = $1 AND key = $2 AND gen = $3`,
				group[0], reservRowKey(key), gen); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
DELETE FROM pack_reservations WHERE repo = $1 AND key = $2 AND gen = $3 AND attempts <= 0`,
				group[0], reservRowKey(key), gen); err != nil {
				return err
			}
		}
		return nil
	})
}

// SweepExpiredPackReservations prunes reservation rows at least olderThan
// old. Deleting an expired UN-FOLDED absence-cover row destroys the
// physical lapse evidence the reconciler's first-health-stamp probe arms
// on — the scheduled sweep is a second consumer of that evidence beside
// ReservePackBytes' admission-time GC (Codex rounds 33+34 on PR #132) —
// so the statement records the durable lapse-evidence marker atomically.
// The sweeper passes the admission TTL as olderThan, so every doomed
// cover row is exactly a lapsed cover; folded rows decay deliberately
// and never mark.
func (s *PG) SweepExpiredPackReservations(ctx context.Context, olderThan time.Duration) (int64, error) {
	assertf(olderThan >= 0, "SweepExpiredPackReservations: negative age %v", olderThan)
	var n int64
	err := s.pool.QueryRow(ctx, `
WITH doomed AS (
  DELETE FROM pack_reservations
  WHERE (key NOT LIKE '%/adopt-claim' AND created_at <= now() - make_interval(secs => $1))
     OR (key LIKE '%/adopt-claim' AND created_at <= now() - make_interval(secs => $2))
  RETURNING key, owner
), marked AS (
  INSERT INTO job_scan_state (job, scope, payload, generation, updated_at)
  SELECT $3, $4, key, 0, now() FROM doomed
  WHERE key COLLATE "C" > $5 AND key COLLATE "C" < $6
    AND right(key, $7) = $8 AND owner <> $9
  LIMIT 1
  ON CONFLICT (job, scope) DO UPDATE SET payload = EXCLUDED.payload, updated_at = now()
)
SELECT count(*) FROM doomed`,
		olderThan.Seconds(), AdoptionClaimTTL.Seconds(),
		ScanJobAbsence, ScanScopeAbsenceLapse,
		AbsenceCoverPrefix, absenceCoverRangeEnd,
		len(absenceCoverRawSuffix), absenceCoverRawSuffix, AbsenceFoldedOwner).Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// IsRetainedPack reports whether (repo, key) is a recorded superseded blob.
func (s *PG) IsRetainedPack(ctx context.Context, repo, key string) (bool, error) {
	assertf(repo != "" && key != "", "IsRetainedPack(%q, %q)", repo, key)
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pack_retained WHERE repo = $1 AND key = $2)`, repo, key).Scan(&exists)
	return exists, err
}

// ReclaimRetainedPack removes the retained row and debits the usage counter.
func (s *PG) ReclaimRetainedPack(ctx context.Context, repo, key string) (int64, error) {
	assertf(repo != "" && key != "", "ReclaimRetainedPack(%q, %q)", repo, key)
	var bytes int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// The debit bumps the floor generation below: serialize with the
		// admission recheck (Codex round 65, lockBaseLedger).
		if err := lockBaseLedger(ctx, tx, baseRepoOf(repo)); err != nil {
			return err
		}
		switch err := tx.QueryRow(ctx,
			`DELETE FROM pack_retained WHERE repo = $1 AND key = $2 RETURNING bytes`,
			repo, key).Scan(&bytes); {
		case errors.Is(err, pgx.ErrNoRows):
			bytes = 0
			return nil // no retained row: idempotent no-op
		case err != nil:
			return err
		}
		// GREATEST clamps defensively: the counter going negative would
		// mean a double-debit bug, and clamping keeps admission sane (a
		// too-small counter is still floored by the caller's listing).
		if _, err := tx.Exec(ctx,
			`UPDATE pack_usage SET bytes = GREATEST(bytes - $2, 0) WHERE repo = $1`, repo, bytes); err != nil {
			return err
		}
		if bytes == 0 {
			return nil
		}
		// The debit shrinks the counter AFTER floors snapshotted it
		// (Codex round 59, security): a floor scanned between the aged
		// blob's deletion and this debit cached max(0, listing−counter)
		// with the retained bytes still in the counter — deflated by up
		// to the retained size, hiding equally sized rowless legacy
		// bytes. Bump the shared floor generation atomically with the
		// debit (the counter-decrease twin of the adoption bump) so
		// every replica's floor from that window re-scans.
		_, err := tx.Exec(ctx, `
INSERT INTO lfs_adoption_gens (repo, gen) VALUES ($1, 1)
ON CONFLICT (repo) DO UPDATE SET gen = lfs_adoption_gens.gen + 1`, baseRepoOf(repo))
		return err
	})
	if err != nil {
		return 0, err
	}
	return bytes, nil
}

// registerLedgerPG folds one newly inserted pack row into the ledger inside
// the RegisterPacks transaction: the key stops being "retained" if it was
// (re-registered superseded bytes were already counted at their original
// registration and never debited), otherwise its size is credited to the
// usage counter. Returns the credit.
func registerLedgerPG(ctx context.Context, tx pgx.Tx, repo string, p Pack) (int64, error) {
	ct, err := tx.Exec(ctx,
		`DELETE FROM pack_retained WHERE repo = $1 AND key = $2`, repo, p.Key)
	if err != nil {
		return 0, err
	}
	if ct.RowsAffected() > 0 {
		return 0, nil // bytes already counted since before the fold
	}
	return p.Size, nil
}

// creditUsagePG adds bytes to repo's usage counter row (upsert).
func creditUsagePG(ctx context.Context, tx pgx.Tx, repo string, bytes int64) error {
	assertf(bytes >= 0, "creditUsagePG: negative credit %d for %s", bytes, repo)
	if bytes == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `
INSERT INTO pack_usage (repo, bytes) VALUES ($1, $2)
ON CONFLICT (repo) DO UPDATE SET bytes = pack_usage.bytes + EXCLUDED.bytes`, repo, bytes)
	return err
}

// validateRegisterLFS asserts the RegisterLFSObject argument invariants
// shared by both backends. bytes zero is legal (the well-known empty
// object) but negative is a caller bug.
func validateRegisterLFS(repo, key string, bytes int64) {
	assertf(repo != "" && key != "", "RegisterLFSObject(%q, %q)", repo, key)
	assertf(bytes >= 0, "RegisterLFSObject(%s, %s): negative bytes %d", repo, key, bytes)
}

// RegisterLFSObject records one verified LFS object and settles its quota
// reservation (issue #51) — see RefStore. Mirrors RegisterPacks' ledger
// discipline: the one-transaction insert + reservation consumption + credit
// is what lets ReservePackBytes read a single snapshot without locking
// against LFS settlement. The object-count cap is enforced HERE (Codex
// round 6) via the maintained counter row (Codex round 10: a COUNT(*)
// under lock made building to the cap quadratic): the counter upsert's row
// lock serializes same-repo registrations, so the RETURNING value is the
// authoritative post-insert population. Returns ErrLFSObjectCapExceeded
// (and commits nothing — insert and increment roll back together) when the
// NEW row would exceed the cap.
func (s *PG) RegisterLFSObject(ctx context.Context, repo, key string, bytes int64) error {
	validateRegisterLFS(repo, key, bytes)
	// A cap denial deletes NO reservation rows here (Codex round 37,
	// reversing round 13's key-wide retire): the shared era row may carry
	// a same-key peer's attempt that is still publishing — if that peer's
	// bytes commit durably but its registration never runs, the deleted
	// row was the only cover keeping the orphan inside quota until the
	// floor cache refreshes. Each cap-denied CALLER retires its own
	// attempt instead (gen-bound, on evidence of non-publication, like
	// every other clean failure), so the row still ends promptly at zero
	// attempts and a crashed loser leaves only the TTL behavior.
	return s.registerLFSObjectTx(ctx, repo, key, bytes, false)
}

// RegisterAdoptedLFSObject — see RefStore (Codex round 51): the adoption
// generation bump commits atomically with the registration.
func (s *PG) RegisterAdoptedLFSObject(ctx context.Context, repo, key string, bytes int64) error {
	validateRegisterLFS(repo, key, bytes)
	return s.registerLFSObjectTx(ctx, repo, key, bytes, true)
}

func (s *PG) registerLFSObjectTx(ctx context.Context, repo, key string, bytes int64, adopted bool) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Lock-first unconditionally, mirroring RegisterPacks: the
		// adoption bump must be serialized with the admission recheck
		// (Codex round 65), and the purge-fence check below must be
		// ordered against FenceRepoPurge's insert (issue #52). LFS repos
		// are always base repos (ADR-35).
		if err := lockBaseLedger(ctx, tx, repo); err != nil {
			return err
		}
		if fenced, err := repoPurgeFencedPG(ctx, tx, repo); err != nil {
			return err
		} else if fenced {
			return fmt.Errorf("register lfs object %s: %w", key, ErrRepoPurged)
		}
		// v0.2.0 registered rows at the single legacy spelling (ADR-41);
		// this binary registers only canonical `o2/` keys. Migrate a
		// same-object legacy row in place instead of inserting a
		// duplicate: its bytes and count are already credited, so this
		// registration must be zero-growth — a fresh insert would
		// double-count both ledgers. Guarded against a canonical twin so
		// the primary key cannot conflict.
		if legacy, ok := lfsLegacyRowKey(key); ok {
			// created_at is REFRESHED to the migration moment (Codex
			// round 89): the row's clock anchors the legacy reap grace —
			// downloads presign the legacy spelling right up to this
			// registration, and the old row's v0.2.0-era timestamp says
			// nothing about those URLs.
			if _, err := tx.Exec(ctx, `
UPDATE lfs_objects SET key = $1, created_at = now(), unreferenced_since = NULL
  WHERE repo = $2 AND key = $3
  AND NOT EXISTS (SELECT 1 FROM lfs_objects o2 WHERE o2.repo = $2 AND o2.key = $1)`,
				key, repo, legacy); err != nil {
				return err
			}
			// Reconcile a TWIN a racing v0.2.0 registration left behind
			// (Codex round 79): once both spellings have rows, the guard
			// above never migrates again — delete the legacy row and
			// refund its count and bytes, or the object stays
			// double-counted forever.
			if _, err := reconcileLegacyLFSRowPG(ctx, tx, repo, legacy, key); err != nil {
				return err
			}
		}
		ct, err := tx.Exec(ctx,
			`INSERT INTO lfs_objects (repo, key, bytes) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
			repo, key, bytes)
		if err != nil {
			return err
		}
		if ct.RowsAffected() > 0 {
			var count int64
			if err := tx.QueryRow(ctx, `
INSERT INTO lfs_object_counts (repo, count) VALUES ($1, 1)
ON CONFLICT (repo) DO UPDATE SET count = lfs_object_counts.count + 1
RETURNING count`, repo).Scan(&count); err != nil {
				return err
			}
			if count > MaxLFSObjectsPerRepo {
				return ErrLFSObjectCapExceeded // rolls back insert + increment
			}
		}
		// Settlement consumes BOTH era spellings (see reservKeySpellings):
		// the key's bytes are in the usage counter now, so a legacy twin
		// row would only double-count.
		if _, err := tx.Exec(ctx,
			`DELETE FROM pack_reservations WHERE key = ANY($1)`, reservKeySpellings(key)); err != nil {
			return err
		}
		if ct.RowsAffected() == 0 {
			// Already registered: bytes already counted. Re-registration
			// is the strongest liveness signal, so clear the reclamation
			// mark atomically with the settlement (issue #73) — a scan
			// verdict from before this upload must not retire the row.
			if _, err := tx.Exec(ctx,
				`UPDATE lfs_objects SET unreferenced_since = NULL WHERE repo = $1 AND key = $2`,
				repo, key); err != nil {
				return err
			}
			return nil
		}
		if adopted {
			// Atomic with the insert+credit (Codex round 51): the bump
			// must never be lost after the registration committed.
			if _, err := tx.Exec(ctx, `
INSERT INTO lfs_adoption_gens (repo, gen) VALUES ($1, 1)
ON CONFLICT (repo) DO UPDATE SET gen = lfs_adoption_gens.gen + 1`, repo); err != nil {
				return err
			}
		}
		return creditUsagePG(ctx, tx, repo, bytes)
	})
}

// reconcileLegacyLFSRowPG deletes repo's legacy-spelled lfs_objects row
// when its canonical twin exists, refunding the object count and usage
// bytes it double-charged (ADR-41, Codex round 79). Reports whether a row
// was reconciled.
func reconcileLegacyLFSRowPG(ctx context.Context, tx pgx.Tx, repo, legacy, canonical string) (bool, error) {
	// Cheap gate: the serialization below is taken only when a legacy row
	// actually exists — the hot path (every LFS registration) stays a
	// point read. A row appearing after this read is caught by the next
	// registration or the sweeper's janitor.
	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM lfs_objects WHERE repo = $1 AND key = $2)`, repo, legacy).Scan(&exists); err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	// The refund moves counted bytes OUT of the live counter (Codex round
	// 82, security): a floor cached while the twin double-counted
	// (listing 2S − counter 2S = 0) would still pass generation checks
	// after the refund and admit against understated usage. Serialize
	// with admissions on the base-ledger lock — the same lock the
	// round-60 floor-generation recheck holds — and bump the generation
	// ATOMICALLY with the refund, so every stale floor is rejected.
	// Reentrant under registerLFSObjectTx's adopted path (advisory xact
	// locks stack within one transaction).
	if err := lockBaseLedger(ctx, tx, repo); err != nil {
		return false, err
	}
	var legacyBytes int64
	switch err := tx.QueryRow(ctx, `
DELETE FROM lfs_objects WHERE repo = $1 AND key = $2
  AND EXISTS (SELECT 1 FROM lfs_objects c WHERE c.repo = $1 AND c.key = $3)
RETURNING bytes`, repo, legacy, canonical).Scan(&legacyBytes); {
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE lfs_object_counts SET count = GREATEST(count - 1, 0) WHERE repo = $1`, repo); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE pack_usage SET bytes = GREATEST(bytes - $2, 0) WHERE repo = $1`, repo, legacyBytes); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO lfs_adoption_gens (repo, gen) VALUES ($1, 1)
ON CONFLICT (repo) DO UPDATE SET gen = lfs_adoption_gens.gen + 1`, repo); err != nil {
		return false, err
	}
	return true, nil
}

// ReconcileLegacyLFSObjects — see RefStore.
func (s *PG) ReconcileLegacyLFSObjects(ctx context.Context) (int64, bool, error) {
	// Paged (Codex round 91): a mixed-version fleet can strand twins
	// across MANY repositories — no per-repo cap bounds the cross-repo
	// candidate set, and Heal runs this synchronously at startup when
	// sweeping is disabled, so materializing every candidate could stall
	// or OOM serving. Each reconcile DELETES its legacy row, so
	// re-running the bounded query converges without offsets; a
	// non-empty page that makes no progress fails loudly instead of
	// spinning. The invocation is page-capped (security round 127) so a
	// live twin-minting old replica cannot hold startup hostage.
	var n int64
	for page := 0; page < reconcileLegacyLFSMaxPages; page++ {
		rows, err := s.pool.Query(ctx, `
SELECT l.repo, l.key FROM lfs_objects l
WHERE strpos(l.key, '/o2/') = 0
  AND EXISTS (SELECT 1 FROM lfs_objects c WHERE c.repo = l.repo
    AND c.key = 'lfs/' || l.repo || '/o2/' || substr(l.key, char_length('lfs/' || l.repo || '/') + 1))
LIMIT $1`, reconcileLegacyLFSBatch)
		if err != nil {
			return n, false, err
		}
		type cand struct{ repo, key string }
		var cands []cand
		for rows.Next() {
			var c cand
			if err := rows.Scan(&c.repo, &c.key); err != nil {
				rows.Close()
				return n, false, err
			}
			cands = append(cands, c)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return n, false, err
		}
		var progress int64
		for _, c := range cands {
			canonical, ok := canonicalFromLegacyLFSRowKey(c.repo, c.key)
			if !ok {
				continue // defensive: candidate selection and Go parsing disagree
			}
			if err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
				done, err := reconcileLegacyLFSRowPG(ctx, tx, c.repo, c.key, canonical)
				if done {
					n++
					progress++
				}
				return err
			}); err != nil {
				return n, false, err
			}
		}
		if len(cands) < reconcileLegacyLFSBatch {
			return n, false, nil
		}
		if progress == 0 {
			return n, false, fmt.Errorf("reconcile legacy lfs rows: full page of %d candidates made no progress", len(cands))
		}
	}
	// Page cap exhausted (security round 127): clean PARTIAL success —
	// reported so callers can schedule bounded follow-up passes (Codex
	// round 128: post-roll with sweeping disabled, startup Heal is the
	// only pass, and silently stranding a >cap backlog double-charges
	// usage and object count until some restart).
	return n, true, nil
}

// reconcileLegacyLFSBatch bounds one ReconcileLegacyLFSObjects page
// (Codex round 91): candidates are re-selected after each page — every
// reconcile deletes its row — so the loop holds at most one page in
// memory regardless of fleet-wide twin count.
var reconcileLegacyLFSBatch = 512

// reconcileLegacyLFSMaxPages bounds one ReconcileLegacyLFSObjects
// INVOCATION (Codex security round 127): Heal runs the loop synchronously
// before serving, and an old replica continuously re-registering twins
// (small compat/adoption objects re-batched by a git:write client) keeps
// every page full and progressing — the loop otherwise never exits and
// startup is delayed indefinitely. The cap covers any realistic static
// backlog (512 pages × 512 rows ≈ 262k twins) while bounding adversarial
// livelock; a capped invocation returns cleanly and the remainder falls
// to the ticker, the next startup, or the next invocation.
var reconcileLegacyLFSMaxPages = 512

// canonicalFromLegacyLFSRowKey maps a legacy row key (lfs/<repo>/<oid>)
// back to its canonical spelling, the inverse of lfsLegacyRowKey.
func canonicalFromLegacyLFSRowKey(repo, legacy string) (string, bool) {
	oid, found := strings.CutPrefix(legacy, "lfs/"+repo+"/")
	if !found || oid == "" || strings.Contains(oid, "/") {
		return "", false
	}
	return "lfs/" + repo + "/o2/" + oid, true
}

// GuardedLFSDelete — see RefStore. The counter row lock is the
// serialization point: RegisterLFSObject's first-registration path upserts
// that row inside its transaction, so a registration either committed
// before this lock (the registry re-read sees it → abort) or blocks until
// this transaction commits (its writer-verify then republishes the bytes).
func (s *PG) GuardedLFSDelete(ctx context.Context, repo, key string, del func(context.Context) error) (bool, error) {
	assertf(repo != "" && key != "" && del != nil, "GuardedLFSDelete(%q, %q)", repo, key)
	deleted := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Lock-first, matching every other row-creating path (PR #105
		// round 15): the fence check below must be totally ordered
		// against FenceRepoPurge's insert, or a snapshot read taken just
		// before the fence commits could run the counter-row upsert after
		// the purge's ledger cleanup. LFS repos are base repos (ADR-35).
		if err := lockBaseLedger(ctx, tx, repo); err != nil {
			return err
		}
		// A purge-FENCED repo needs no writer guard at all: the
		// registrations this guard serializes against are themselves
		// fenced and can never commit, and the counter-row upsert below
		// would recreate accounting metadata the purge's ledger cleanup
		// removed (issue #52, PR #105 round 14). Delete directly.
		if fenced, err := repoPurgeFencedPG(ctx, tx, repo); err != nil {
			return err
		} else if fenced {
			if err := del(ctx); err != nil {
				return err
			}
			deleted = true
			return nil
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO lfs_object_counts (repo, count) VALUES ($1, 0) ON CONFLICT DO NOTHING`, repo); err != nil {
			return err
		}
		var n int64
		if err := tx.QueryRow(ctx,
			`SELECT count FROM lfs_object_counts WHERE repo = $1 FOR UPDATE`, repo).Scan(&n); err != nil {
			return err
		}
		var registered bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM lfs_objects WHERE repo = $1 AND key = $2)`, repo, key).Scan(&registered); err != nil {
			return err
		}
		if registered {
			return nil // a verified upload won: nothing to delete
		}
		if err := del(ctx); err != nil {
			return err
		}
		deleted = true
		return nil
	})
	return deleted, err
}

// baseRepoOf strips the optional namespace suffix (`+ephemeral`/`+import`)
// so the adoption generation is keyed by the BASE repository, matching the
// floor caches' quota accounting unit (mirrors githttp.BaseRepo, which ref
// cannot import).
func baseRepoOf(repo string) string {
	base, _, _ := strings.Cut(repo, "+")
	return base
}

// BumpLFSAdoptionGen — see RefStore (Codex round 49).
func (s *PG) BumpLFSAdoptionGen(ctx context.Context, repo string) error {
	assertf(repo != "", "BumpLFSAdoptionGen(%q)", repo)
	// Serialized with the admission recheck (Codex round 65,
	// lockBaseLedger) like every generation bump.
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockBaseLedger(ctx, tx, baseRepoOf(repo)); err != nil {
			return err
		}
		// Purge fence (issue #52, PR #105 round 10): a pre-delete
		// writer's post-registration repair path can reach this bump
		// after the purge's ledger cleanup; silently no-op instead of
		// recreating the row — no admission for a purged repo consults
		// floors, so there is nothing left to invalidate. The lock above
		// totally orders the check against FenceRepoPurge.
		if fenced, err := repoPurgeFencedPG(ctx, tx, repo); err != nil || fenced {
			return err
		}
		_, err := tx.Exec(ctx, `
INSERT INTO lfs_adoption_gens (repo, gen) VALUES ($1, 1)
ON CONFLICT (repo) DO UPDATE SET gen = lfs_adoption_gens.gen + 1`, repo)
		return err
	})
}

// RecordLFSMismatch — see RefStore (#104, Codex round 132).
func (s *PG) RecordLFSMismatch(ctx context.Context, repo, key, fingerprint string) error {
	assertf(repo != "" && key != "" && fingerprint != "", "RecordLFSMismatch(%q, %q)", repo, key)
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockBaseLedger(ctx, tx, baseRepoOf(repo)); err != nil {
			return err
		}
		// Purge fence: markers are row-creating writes; a purged repo's
		// legacy blobs are the sweeper's problem, not the bridge's.
		if fenced, err := repoPurgeFencedPG(ctx, tx, repo); err != nil || fenced {
			return err
		}
		_, err := tx.Exec(ctx, `
INSERT INTO lfs_mismatches (repo, key, fingerprint) VALUES ($1, $2, $3)
ON CONFLICT (repo, key) DO UPDATE SET fingerprint = EXCLUDED.fingerprint`, repo, key, fingerprint)
		return err
	})
}

// LFSMismatchFingerprint — see RefStore (#104, Codex round 132).
func (s *PG) LFSMismatchFingerprint(ctx context.Context, repo, key string) (string, bool, error) {
	assertf(repo != "" && key != "", "LFSMismatchFingerprint(%q, %q)", repo, key)
	var fp string
	err := s.pool.QueryRow(ctx,
		`SELECT fingerprint FROM lfs_mismatches WHERE repo = $1 AND key = $2`, repo, key).Scan(&fp)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	return fp, err == nil, err
}

// ClearLFSMismatch — see RefStore (#104, Codex round 132).
func (s *PG) ClearLFSMismatch(ctx context.Context, repo, key string) error {
	assertf(repo != "" && key != "", "ClearLFSMismatch(%q, %q)", repo, key)
	_, err := s.pool.Exec(ctx, `DELETE FROM lfs_mismatches WHERE repo = $1 AND key = $2`, repo, key)
	return err
}

// LFSAdoptionGen — see RefStore (Codex round 49).
func (s *PG) LFSAdoptionGen(ctx context.Context, repo string) (int64, error) {
	assertf(repo != "", "LFSAdoptionGen(%q)", repo)
	var gen int64
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE((SELECT gen FROM lfs_adoption_gens WHERE repo = $1), 0)`, repo).Scan(&gen)
	return gen, err
}

// CountLFSObjects returns repo's registered LFS object count — an O(1)
// counter-row read (issue #51; the LFS surface's fast-fail pre-checks call
// it per upload, so it must not scan the attacker-growable object table).
func (s *PG) CountLFSObjects(ctx context.Context, repo string) (int64, error) {
	assertf(repo != "", "CountLFSObjects(%q)", repo)
	var n int64
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE((SELECT count FROM lfs_object_counts WHERE repo = $1), 0)`, repo).Scan(&n)
	return n, err
}

// HasLFSObject reports whether (repo, key) is a registered LFS object.
func (s *PG) HasLFSObject(ctx context.Context, repo, key string) (bool, error) {
	assertf(repo != "" && key != "", "HasLFSObject(%q, %q)", repo, key)
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM lfs_objects WHERE repo = $1 AND key = $2)`, repo, key).Scan(&exists)
	return exists, err
}

// LFSObjectRegisteredAt — see RefStore (Codex round 89; bytes since #127).
func (s *PG) LFSObjectRegisteredAt(ctx context.Context, repo, key string) (time.Time, int64, bool, error) {
	assertf(repo != "" && key != "", "LFSObjectRegisteredAt(%q, %q)", repo, key)
	var at time.Time
	var bytes int64
	err := s.pool.QueryRow(ctx,
		`SELECT created_at, bytes FROM lfs_objects WHERE repo = $1 AND key = $2`, repo, key).Scan(&at, &bytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, 0, false, nil
	}
	return at, bytes, err == nil, err
}

// PackQuotaLedgerState is a point-in-time read of one accounting group's
// ledger, for tests, oracles, and operator inspection — NOT part of the
// admission path (ReservePackBytes reads its own single snapshot).
type PackQuotaLedgerState struct {
	UsageBytes    int64 // SUM(pack_usage) over the group
	ReservedBytes int64 // SUM(active reservations) for the base repo
	RetainedBytes int64 // SUM(pack_retained) over the group
}

// QuotaLedgerState reads the group's ledger state (see PackQuotaLedgerState).
func (s *PG) QuotaLedgerState(ctx context.Context, group []string, ttl time.Duration, excludeKeys ...string) (PackQuotaLedgerState, error) {
	assertf(len(group) > 0 && group[0] != "" && ttl > 0, "QuotaLedgerState(%v, %v)", group, ttl)
	// Both era spellings per excluded key — the advisory-admission mirror
	// of ReservePackBytes' byte-SUM exclusion (see reservKeySpellings).
	excludeKeys = reservKeySpellings(excludeKeys...)
	var st PackQuotaLedgerState
	err := s.pool.QueryRow(ctx, `
SELECT
  COALESCE((SELECT SUM(bytes) FROM pack_usage WHERE repo = ANY($1)), 0),
  COALESCE((SELECT SUM(bytes) FROM pack_reservations
            WHERE repo = $2 AND created_at > now() - make_interval(secs => $3)
              AND key NOT LIKE '%`+AdoptionClaimSuffix+`'
              AND NOT (key = ANY($4))), 0),
  COALESCE((SELECT SUM(bytes) FROM pack_retained WHERE repo = ANY($1)), 0)`,
		group, group[0], ttl.Seconds(), excludeKeys).Scan(&st.UsageBytes, &st.ReservedBytes, &st.RetainedBytes)
	return st, err
}
