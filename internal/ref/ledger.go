// Durable pack-quota ledger (issues #29 + #31): reservation, usage, and
// retained-byte state living in the ref-authority store so admission is
// atomic across nodes and replicas.
//
// Three tables (created by InitSchema on both backends):
//
//   - pack_reservations(key PK, repo, bytes, owner, created_at): one row
//     per in-flight incoming pack, keyed by the content-addressed blob key.
//     Inserted by ReservePackBytes BEFORE the blob upload; consumed
//     (deleted) by RegisterPacks in the registration transaction; expired
//     rows are ignored by admission and pruned on the sweeper cadence.
//     A crash between reserve and register therefore over-reserves for at
//     most the TTL — admission control, never a byte-exact ledger.
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
//	max(SUM(pack_usage over group), usageFloor)
//	  + SUM(active reservations for the base repo, excluding this push's keys)
//	  + incoming  <=  quota
//
// usageFloor is the caller's blob-listing measurement (blob.StorageUsageBytes).
// It is kept as a safety floor rather than retired (deviating from issue
// #29's "kill the listing" sketch) because it covers bytes no counter can
// know: repos whose packs predate the ledger, and orphaned uploads from
// crashed pushes that never registered. The counter covers what the listing
// cannot see in time: bytes admitted by concurrent pushes that registered
// after the caller listed. max() of two lower bounds of true usage is still
// a lower bound, so admission stays exactly as strict as the tighter one.
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
// counted once — identical keys are identical bytes), KEEPING the original
// owner (issue #51, Codex round 5): a refresh that took ownership would
// let the newcomer's failure cleanup free the one row backing the original
// writer's still-staging upload — an upload-duration-sized window for
// uncounted growth. The inverse hazard (the original's failure cleanup
// firing in the instants after a newcomer's refresh) is accepted as the
// lesser risk: failure and release are one code path, so that window is
// tiny, and the miss only leaves a TTL-bounded row.

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
// reservation rows. The owner is provenance only (Codex round 22): rows end
// exclusively by settlement (RegisterPacks/RegisterLFSObject consume by
// KEY) or TTL expiry — same-key rows are shared by every concurrent writer
// via refresh, so no owner-scoped delete can ever be sound: whichever
// writer's failure fired it would free headroom the others still stage
// under.
func NewReservationOwner() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("ref assertion failed: reservation owner entropy: %v", err))
	}
	return hex.EncodeToString(b[:])
}

// ReserveDecision reports one admission decision. UsedBytes and
// ReservedBytes carry the arithmetic the caller folds into its denial
// message (the client-facing sideband line / 403 detail).
type ReserveDecision struct {
	// OK reports whether the reservation was admitted and inserted.
	OK bool
	// UsedBytes is the usage the decision charged: the max of the ledger
	// counter and the caller's listing floor.
	UsedBytes int64
	// ReservedBytes is the active in-flight reservations charged on top of
	// UsedBytes (other pushes' keys only).
	ReservedBytes int64
	// Gens maps each reserved key to its row-incarnation token (Codex
	// round 33): minted at INSERT, preserved across same-key refreshes.
	// RetirePackAttempts requires it, so a stale writer whose row expired
	// and was replaced can never retire the replacement. Set only when OK.
	Gens map[string]ReservationToken
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
func validateReserve(group []string, packs []Pack, owner string, usageFloor, quotaBytes int64, ttl time.Duration) {
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
	assertf(quotaBytes > 0, "ReservePackBytes: non-positive quota %d (0 disables the quota at the caller)", quotaBytes)
	assertf(ttl > 0, "ReservePackBytes: non-positive ttl %v", ttl)
}

// ReservationToken identifies one reservation row incarnation as observed
// by the reserve that returned it (Codex round 33). Gen is the row's
// incarnation token (minted at INSERT, preserved across refreshes);
// CreatedAt is the DB-stamped refresh time this reserve itself wrote.
// Retirement deletes a row only when BOTH match its expectations: the gen
// proves the row is the same incarnation, and created_at ≤ CreatedAt
// proves no later writer — a pre-attempt binary's refresh included, which
// bumps created_at without incrementing attempts — refreshed the row after
// this reserve. Both comparisons are DB-clock-to-DB-clock, so app/database
// clock skew cannot misfire them; a blocked delete only leaves the row to
// the TTL.
type ReservationToken struct {
	Gen       string
	CreatedAt time.Time
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

// ReservePackBytes implements the admission check documented atop this file
// on Postgres.
func (s *PG) ReservePackBytes(ctx context.Context, group []string, packs []Pack, owner string, usageFloor, quotaBytes int64, ttl time.Duration) (ReserveDecision, error) {
	validateReserve(group, packs, owner, usageFloor, quotaBytes, ttl)
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
		// Self-healing GC (Codex round 5 on #51): expired rows already
		// stopped gating admission, but they occupy physical rows the
		// sweeper may never prune (sweeping can be disabled). Pruning this
		// base repo's expired rows inside the serialized admission
		// transaction bounds the repo's PHYSICAL row population at the
		// active-row cap plus one TTL of churn, with no sweeper required.
		if _, err := tx.Exec(ctx, `
DELETE FROM pack_reservations WHERE repo = $1 AND created_at <= now() - make_interval(secs => $2)`,
			group[0], ttl.Seconds()); err != nil {
			return err
		}
		// ONE statement = one snapshot: a concurrent registration moves a
		// key's bytes from reservation to counter atomically, so either
		// side of that move is counted here, never neither.
		var counter, reserved, activeRows int64
		if err := tx.QueryRow(ctx, `
SELECT
  COALESCE((SELECT SUM(bytes) FROM pack_usage WHERE repo = ANY($1)), 0),
  COALESCE((SELECT SUM(bytes) FROM pack_reservations
            WHERE repo = $2
              AND created_at > now() - make_interval(secs => $3)
              AND NOT (key = ANY($4))), 0),
  (SELECT COUNT(*) FROM pack_reservations
   WHERE repo = $2
     AND created_at > now() - make_interval(secs => $3)
     AND NOT (key = ANY($4)))`,
			group, group[0], ttl.Seconds(), keys).Scan(&counter, &reserved, &activeRows); err != nil {
			return err
		}
		dec = ReserveDecision{UsedBytes: max(counter, usageFloor), ReservedBytes: reserved}
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
			// The refresh deliberately KEEPS the existing owner (Codex
			// round 5 on #51; owner is now vestigial for LFS keys, whose
			// rows end only by settle or TTL) and takes the MAXIMUM of the
			// declared byte values (Codex round 7): declared sizes are
			// client input, so a shrinking refresh would let a one-byte
			// same-key PUT free headroom a concurrent larger upload is
			// still staging under. Monotone growth until settlement is the
			// conservative side — settlement replaces the row with the
			// verified actual size in the usage counter, so an inflated
			// declaration lives at most until settle or TTL.
			// attempts counts the live same-key writers sharing this row
			// (Codex round 30): every reserve/refresh adds one, a clean
			// failure retires one (RetirePackAttempts), and the row ends
			// at zero attempts, settlement, or TTL. gen is the row's
			// incarnation (Codex round 33): a fresh INSERT mints one, a
			// refresh preserves it, and RETURNING hands the caller the
			// token — gen plus this reserve's own DB-stamped created_at —
			// its retirement must present.
			var tok ReservationToken
			if err := tx.QueryRow(ctx, `
INSERT INTO pack_reservations (key, repo, bytes, owner, gen) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (key) DO UPDATE SET repo = EXCLUDED.repo,
                                bytes = GREATEST(pack_reservations.bytes, EXCLUDED.bytes),
                                created_at = now(),
                                attempts = pack_reservations.attempts + 1
RETURNING gen, created_at`,
				p.Key, group[0], p.Size, owner, NewReservationOwner()).Scan(&tok.Gen, &tok.CreatedAt); err != nil {
				return err
			}
			if dec.Gens == nil {
				dec.Gens = make(map[string]ReservationToken, len(packs))
			}
			dec.Gens[p.Key] = tok
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
func (s *PG) RetirePackAttempts(ctx context.Context, group []string, gens map[string]ReservationToken) error {
	assertf(len(group) > 0 && group[0] != "" && len(gens) > 0, "RetirePackAttempts(%v, %v)", group, gens)
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), $2)`, group[0], reserveLockClass); err != nil {
			return err
		}
		for key, tok := range gens {
			// The token binds the decrement to the row incarnation the
			// caller actually reserved (Codex round 33): a replacement
			// row created after this caller's expired and was pruned
			// carries a different gen and is untouched. The DELETE
			// additionally requires created_at unchanged since this
			// caller's own reserve — ANY later refresh, including a
			// pre-attempt binary's (which bumps created_at without
			// incrementing attempts, the mixed-fleet hazard), retains the
			// row for the TTL.
			if _, err := tx.Exec(ctx, `
UPDATE pack_reservations SET attempts = attempts - 1 WHERE repo = $1 AND key = $2 AND gen = $3`,
				group[0], key, tok.Gen); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
DELETE FROM pack_reservations WHERE repo = $1 AND key = $2 AND gen = $3 AND attempts <= 0 AND created_at <= $4`,
				group[0], key, tok.Gen, tok.CreatedAt); err != nil {
				return err
			}
		}
		return nil
	})
}

// SweepExpiredPackReservations prunes reservation rows at least olderThan
// old.
func (s *PG) SweepExpiredPackReservations(ctx context.Context, olderThan time.Duration) (int64, error) {
	assertf(olderThan >= 0, "SweepExpiredPackReservations: negative age %v", olderThan)
	ct, err := s.pool.Exec(ctx,
		`DELETE FROM pack_reservations WHERE created_at <= now() - make_interval(secs => $1)`,
		olderThan.Seconds())
	if err != nil {
		return 0, err
	}
	return ct.RowsAffected(), nil
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
		_, err := tx.Exec(ctx,
			`UPDATE pack_usage SET bytes = GREATEST(bytes - $2, 0) WHERE repo = $1`, repo, bytes)
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
	err := s.registerLFSObjectTx(ctx, repo, key, bytes)
	if errors.Is(err, ErrLFSObjectCapExceeded) {
		// Retire the loser's reservation (Codex round 13): the cap denial
		// is PERMANENT for a new key — every concurrent registration of
		// any new key is denied too — so no writer can ever settle this
		// row, and leaving it would gate unrelated admissions for the
		// full TTL. Safe against same-key peers still staging: durable
		// usage requires a fresh pre-publish admission plus a
		// lease-bounded registration, both of which re-run after this
		// delete. Best-effort — a failure just leaves TTL behavior.
		if _, derr := s.pool.Exec(ctx, `DELETE FROM pack_reservations WHERE key = $1`, key); derr != nil {
			return errors.Join(err, fmt.Errorf("retire cap-denied reservation %s: %w", key, derr))
		}
	}
	return err
}

func (s *PG) registerLFSObjectTx(ctx context.Context, repo, key string, bytes int64) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
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
		if _, err := tx.Exec(ctx,
			`DELETE FROM pack_reservations WHERE key = $1`, key); err != nil {
			return err
		}
		if ct.RowsAffected() == 0 {
			return nil // already registered: bytes already counted
		}
		return creditUsagePG(ctx, tx, repo, bytes)
	})
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
	if excludeKeys == nil {
		excludeKeys = []string{}
	}
	var st PackQuotaLedgerState
	err := s.pool.QueryRow(ctx, `
SELECT
  COALESCE((SELECT SUM(bytes) FROM pack_usage WHERE repo = ANY($1)), 0),
  COALESCE((SELECT SUM(bytes) FROM pack_reservations
            WHERE repo = $2 AND created_at > now() - make_interval(secs => $3)
              AND NOT (key = ANY($4))), 0),
  COALESCE((SELECT SUM(bytes) FROM pack_retained WHERE repo = ANY($1)), 0)`,
		group, group[0], ttl.Seconds(), excludeKeys).Scan(&st.UsageBytes, &st.ReservedBytes, &st.RetainedBytes)
	return st, err
}
