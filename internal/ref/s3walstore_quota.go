// S3WAL quota ledger: reservations, admission arithmetic, retained-byte
// accounting, and the reservation janitor over manifest CAS (issues
// #29/#31 semantics, s3wal mapping per docs/plans/s3wal-plan.md §1/§2).
//
// Reservations key on PLAIN blob keys: s3wal has no v0.1.0 legacy binaries,
// so the era-spelling fence (reservKeySuffix) does not exist here — an
// intentional, documented divergence from the SQL backends.
//
// Every TTL comparison follows the §2.4 manifest-clock rules: stamps are
// written with the manifest logical clock, elapsed time is
// min(localNow, clk_candidate) − stamp, and destructive expiry (pruning,
// steals) demotes to report-only under the fail-closed skew guard.
package ref

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/coder/code-storage/internal/ref/s3wal"
)

// s3walLapseEvidence reports whether a doomed reservation map key is an
// UN-FOLDED absence-cover row — physical lapse evidence the reconciler's
// first-health-stamp probe arms on (Codex rounds 32-34 on PR #132).
// Folded nonces decay deliberately; foreign spellings are never evidence.
func s3walLapseEvidence(mapKey, owner string) bool {
	_, ok := s3walCoverSynth(mapKey)
	return ok && owner != AbsenceFoldedOwner
}

// s3walSeedDoomsCover reports whether snapshot m holds a reservation row
// the caller's expiry pass would BOTH prune and record the fleet-global
// lapse-evidence marker for — the pre-CAS externalized write that makes a
// seed unfit (s3walMutation.seedUnfit). window is the caller's non-claim
// expiry window (covers are never claim rows, so the claim window is
// irrelevant here); the lapse-evidence shape check runs first so
// irrelevant rows never consult the skew-guarded clock arms.
func (s *S3WAL) s3walSeedDoomsCover(base string, m *s3wal.Manifest, window time.Duration) bool {
	for key, r := range m.Reservations {
		if s3walLapseEvidence(key, r.Owner) && s.s3walExpiryAllowed(base, m, r.CreatedAt, window, "seed fitness") {
			return true
		}
	}
	return false
}

// s3walRecordLapse durably records the lapse-evidence marker for one
// doomed cover row BEFORE the CAS that consumes it commits (the SQLite
// twin's ordering): a marker without a consumed row is a conservative
// residual the recovery rotation clears, while the reverse order could
// destroy the only evidence. payload is the fleet-uniform raw spelling.
// The marker is fleet-global and legitimately upserted by CONCURRENT
// receive-hook processes, so raced writes retry (scanMutateShared) —
// the single-writer failure policy would reject a valid push.
func (s *S3WAL) s3walRecordLapse(ctx context.Context, mapKey string) error {
	payload := s3walCoverRaw(mapKey)
	return s.scanMutateShared(ctx, "record lapse marker", ScanJobAbsence, ScanScopeAbsenceLapse, func(*s3walScanRow, time.Time) (*s3walScanRow, error) {
		return &s3walScanRow{Payload: payload}, nil
	})
}

// s3walReservationLive reports whether a reservation stamp is inside ttl
// under the §2.4 elapsed rule. Under the drift arm of the skew guard the
// verdict leans LIVE: a forward clock jump must not vanish live rows
// early from admission sums (quota would under-count), retire-sparing
// (the adopter's mutual exclusion would lapse), or renewals (owner fences
// still protect stolen claims). Rows persist, so a leaned verdict
// re-resolves plainly once clocks agree. Only the drift arm applies: a
// manifest clock AHEAD already leans live through the plain elapsed
// arithmetic, and tiny conformance windows chain the manifest clock
// milliseconds ahead via ClkNext — that must not resurrect deliberately
// expired rows (the strict-TTL round-6 invariant).
func (s *S3WAL) s3walReservationLive(m *s3wal.Manifest, stamp time.Time, ttl time.Duration) bool {
	if s3wal.Elapsed(s.now(), m.Clk, stamp) < ttl {
		return true
	}
	return ttl > 0 && (!s3wal.DriftOK(s.wallDrift(), ttl) || !s.s3walDateOK(ttl))
}

// s3walDateWitnessMaxAge bounds how stale the §2.4-3 store-Date witness
// may be for a TTL verdict: every verdict runs inside a mutate/view whose
// manifest read just produced an observation through the same client, so
// a live path's witness is microseconds old — 30s tolerates slow bodies
// and GC loops without ever accepting a witness from an unrelated
// earlier pass. Real (monotonic) age deliberately, never the injectable
// clock: freshness measures THIS PROCESS's observation pipeline, not the
// clock under suspicion.
const s3walDateWitnessMaxAge = 30 * time.Second

// s3walDateOK is the §2.4-3 store-clock witness arm (see s3wal.DateOK):
// the absolute cross-check that catches AT-BOOT-wrong local clocks,
// which report ~zero drift and read old manifest clocks as quiet repos.
// A missing or stale witness fails closed for active bounds — the
// manifest read preceding every verdict observes one, so absence means
// the store stopped serving Date headers, and destruction must not
// proceed on an unwitnessed clock.
func (s *S3WAL) s3walDateOK(ttl time.Duration) bool {
	if s3wal.DateSkewBound(ttl) == 0 {
		return true // degenerate window: arm disabled (DateSkewBound)
	}
	date, observedAt, ok := s.cas.StoreDate()
	if !ok || time.Since(observedAt) > s3walDateWitnessMaxAge {
		return false
	}
	return s3wal.DateOK(s.now(), date, ttl)
}

// s3walExpiryAllowed gates DESTRUCTIVE expiry verdicts (prune, steal,
// grace deletion): true only when the stamp's window elapsed AND the skew
// guard passes. ttl 0 waives the window explicitly (the test/recovery
// hatch) and skips the guard — the caller asked for unconditional expiry.
func (s *S3WAL) s3walExpiryAllowed(base string, m *s3wal.Manifest, stamp time.Time, ttl time.Duration, what string) bool {
	now := s.now()
	if ttl <= 0 {
		return true
	}
	if s3wal.Elapsed(now, m.Clk, stamp) < ttl {
		return false
	}
	if !s3wal.SkewOK(now, m.Clk, s.wallDrift(), ttl) || !s.s3walDateOK(ttl) {
		// Fail closed: over-retention is the only reachable failure
		// direction. Loud, then report-only (plan §2.4).
		s.warnSkew(base, m, ttl, what)
		return false
	}
	return true
}

// warnSkew logs the §2.4 skew-guard demotion loudly (expiry passes retry
// on their cadence, so verdicts stay report-only until clocks agree).
func (s *S3WAL) warnSkew(base string, m *s3wal.Manifest, ttl time.Duration, what string) {
	date, observedAt, _ := s.cas.StoreDate()
	slog.Warn("s3wal clock skew guard tripped: demoting expiry verdict to report-only",
		"base", base, "what", what, "manifest_clk", m.Clk, "local_now", s.now(),
		"wall_drift", s.wallDrift(), "store_date", date,
		"store_date_age", time.Since(observedAt), "ttl", ttl)
}

// s3walIsClaimKey reports whether a reservation map key is an adoption
// claim lease (never storage: excluded from admission byte sums and the
// row cap, reaped only on the AdoptionClaimTTL).
func s3walIsClaimKey(key string) bool {
	return len(key) >= len(AdoptionClaimSuffix) && key[len(key)-len(AdoptionClaimSuffix):] == AdoptionClaimSuffix
}

// ReservePackBytes implements the admission check documented atop
// ledger.go over one manifest CAS. Recovery class OPS (attempt increments
// are non-idempotent): ambiguity resolves via the writer nonce, then the
// op-ID's membership in the ops ring or on a touched reservation entry;
// both missing fails closed with a retryable error — never a blind
// re-apply.
func (s *S3WAL) ReservePackBytes(ctx context.Context, group []string, packs []Pack, owner string, usageFloor, floorGen, quotaBytes int64, ttl time.Duration) (ReserveDecision, error) {
	validateReserve(group, packs, owner, usageFloor, floorGen, quotaBytes, ttl)
	base := group[0]
	assertf(baseRepoOf(base) == base, "ReservePackBytes: group base %q is not a base repo", base)
	incomingKeys := make(map[string]bool, len(packs))
	for _, p := range packs {
		incomingKeys[p.Key] = true
	}
	incoming := incomingBytes(packs)
	opID := s3wal.NewOpID()
	// Finite quota-gated admissions hold the read side of the fence lock
	// across the fence check and the admission CAS (Codex rounds 26+35 on
	// PR #132) — the in-process fast path; the cross-process guarantee
	// is the post-commit re-read in s3walRevalidateFence below.
	finite := quotaBytes != math.MaxInt64
	if finite {
		s.absenceFenceMu.RLock()
		defer s.absenceFenceMu.RUnlock()
	}
	var dec ReserveDecision
	minted := false
	err := s.mutate(ctx, base, s3walMutation{
		op: "reserve pack bytes",
		// The self-healing GC below records the fleet-global lapse
		// marker BEFORE this CAS commits — an externalized write that
		// must never fire off an unvalidated seed (see s3walMutation).
		seedUnfit: func(m *s3wal.Manifest) bool { return s.s3walSeedDoomsCover(base, m, ttl) },
		// Growth is dynamic: only freshly MINTED rows add entries — a
		// same-key refresh mutates an existing row in place, and the
		// self-healing GC only shrinks.
		grows: func() bool { return minted },
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			dec = ReserveDecision{}
			minted = false
			if m.PurgedAt != nil {
				return false, fmt.Errorf("reserve pack bytes %s: %w", base, ErrRepoPurged)
			}
			// Cover-renewal degraded fence (Codex round 26 on PR #132,
			// security): while the marker is armed, TTL'd absence covers
			// may have lapsed un-renewed and this snapshot's strict-TTL
			// reservation sum under-counts them — finite admissions fail
			// closed; unbounded internal reservations proceed so recovery
			// can re-cover. A §2.10 side-object read inside the CAS loop,
			// re-checked on every lost-race re-run like the SQL twins
			// re-check per transaction.
			if finite {
				if _, armed, err := s.scanGet(ctx, ScanJobAbsence, ScanScopeAbsenceAdmissionFence); err != nil {
					return false, err
				} else if armed {
					dec = ReserveDecision{RenewalDegraded: true}
					return false, nil
				}
			}
			// Self-healing GC (Codex round 5 on #51): prune this group's
			// expired non-claim rows inside the admission CAS, bounding
			// physical rows with no sweeper required. Committed even on
			// denial, like the SQL transaction. Consuming an expired
			// UN-FOLDED absence-cover row records the durable
			// lapse-evidence marker BEFORE this CAS commits (Codex round
			// 33; ordering in s3walRecordLapse).
			changed := false
			doomedCover := ""
			for key, r := range m.Reservations {
				if s3walIsClaimKey(key) {
					continue
				}
				if s.s3walExpiryAllowed(base, m, r.CreatedAt, ttl, "reservation prune") {
					if doomedCover == "" && s3walLapseEvidence(key, r.Owner) {
						doomedCover = key
					}
					delete(m.Reservations, key)
					changed = true
				}
			}
			if doomedCover != "" {
				if err := s.s3walRecordLapse(ctx, doomedCover); err != nil {
					return false, fmt.Errorf("reserve pack bytes lapse marker: %w", err)
				}
			}
			// ONE snapshot: usage counter, live-reservation byte sum
			// excluding this push's keys, and the active-row count.
			var reserved, activeRows int64
			for key, r := range m.Reservations {
				if s3walIsClaimKey(key) || incomingKeys[key] {
					continue
				}
				if !s.s3walReservationLive(m, r.CreatedAt, ttl) {
					continue
				}
				reserved = addSat(reserved, r.Bytes)
				activeRows++
			}
			dec.UsedBytes = addSat(m.UsageBytes, usageFloor)
			dec.ReservedBytes = reserved
			// Transactional floor-generation recheck (Codex round 60).
			if floorGen >= 0 && m.LFS.AdoptionGen != floorGen {
				dec.FloorStale = true
				return changed, nil
			}
			if activeRows+int64(len(packs)) > int64(maxActiveReservations) {
				dec.KeyCapExceeded = true
				return changed, nil
			}
			if !admitted(dec.UsedBytes, dec.ReservedBytes, incoming, quotaBytes) {
				return changed, nil
			}
			if m.Reservations == nil {
				m.Reservations = make(map[string]*s3wal.Reservation)
			}
			dec.Gens = make(map[string]string, len(packs))
			for _, p := range packs {
				r := m.Reservations[p.Key]
				if r == nil {
					// Fresh insert mints the row-incarnation gen (Codex
					// round 33).
					r = &s3wal.Reservation{Bytes: p.Size, Gen: NewReservationOwner()}
					m.Reservations[p.Key] = r
					minted = true
				} else {
					// Same-key refresh: MAX bytes (Codex round 7), owner
					// rotates, clock restarts, attempts refcount grows
					// (Codex round 30), gen preserved.
					r.Bytes = max(r.Bytes, p.Size)
				}
				r.Owner = owner
				r.CreatedAt = clk
				r.Attempts++
				r.OpID = opID // the §2.3 per-entry ambiguity record
				dec.Gens[p.Key] = r.Gen
			}
			m.RecordOp(opID)
			dec.OK = true
			return true, nil
		},
		recover: func(fresh *s3wal.Manifest) s3walVerdict {
			if fresh.OpApplied(opID) {
				return s3walApplied // dec from our landed run stands
			}
			return s3walFailClosed
		},
	})
	if err != nil {
		return ReserveDecision{}, err
	}
	if finite && dec.OK {
		if err := s.s3walRevalidateFence(ctx, base, packs, owner, opID, &dec); err != nil {
			return ReserveDecision{}, err
		}
	}
	return dec, nil
}

// s3walRevalidateFence is the storage-coordinated half of the admission
// fence (Codex on PR #156, security). absenceFenceMu serializes the fence
// check and the admission CAS in-process only; git receive hooks run in
// FRESH processes with separate S3WAL instances (Phase 3), where the
// arming pass can land its marker between this admission's in-CAS
// pre-read and its commit — spending the exact deflated headroom the
// fence exists to close. The cross-process guarantee is this post-commit
// re-read: S3 reads are strongly consistent, so any arm completing
// before our CAS is visible to the GET. A pre-armed marker never reaches
// here — the in-CAS pre-read already denied — so marker PRESENCE is the
// entire changed-vs-pre-read signal.
//
// If armed, the admission CAS-undoes its own just-committed attempts,
// keyed by owner AND op-ID: entries re-reserved, registered, or swept in
// the meantime no longer match and are left alone, so no foreign attempts
// or op-IDs are refunded (the ops-ring record of our landed CAS stays —
// it is bounded ambiguity history, not accounting). Concurrent identical
// uploads SHARE one row (each admission increments Attempts while
// rotating Owner/OpID), so the undo decrements our attempt and deletes
// only at zero (Codex round 11 on PR #156, security) — deleting outright
// would strip the earlier in-flight pushes' quota cover and let a
// post-recovery admission over-commit the quota. The verdict then
// demotes to the armed fence's fail-closed deny. This restores the PG
// twin's guarantee shape: every finite admission either fails closed on
// fence evidence or commits early enough that the post-arm recovery pass
// observes its row — an arm+recover+disarm cycle completing INSIDE this
// window implies recovery either rewrote our base's manifest (whose CAS
// chain then re-read our committed row first) or never needed to, in
// which case this group's sums were never deflated.
//
// A failed re-read or revocation errors the whole admission: the caller
// must not treat the reservation as granted; the orphaned rows expire on
// the TTL like any crashed push's (over-count only — the safe
// direction).
func (s *S3WAL) s3walRevalidateFence(ctx context.Context, base string, packs []Pack, owner, opID string, dec *ReserveDecision) error {
	assertf(dec.OK, "s3walRevalidateFence: non-granted decision %+v", *dec)
	_, armed, err := s.scanGet(ctx, ScanJobAbsence, ScanScopeAbsenceAdmissionFence)
	if err != nil {
		return fmt.Errorf("reserve pack bytes %s: fence re-validation: %w", base, err)
	}
	if !armed {
		return nil
	}
	err = s.mutate(ctx, base, s3walMutation{
		op: "revoke fenced reservation",
		// ID class: the owner+op-ID predicate skips rows anyone else
		// touched since, and a surviving row's OpID rotates on the undo
		// so a CAS re-run of this revocation can never double-decrement
		// (the re-apply match must die with the first application).
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			changed := false
			for _, p := range packs {
				if r := m.Reservations[p.Key]; r != nil && r.Owner == owner && r.OpID == opID {
					r.Attempts--
					if r.Attempts <= 0 {
						delete(m.Reservations, p.Key)
					} else {
						r.OpID = s3wal.NewOpID()
					}
					changed = true
				}
			}
			return changed, nil
		},
	})
	if err != nil {
		return fmt.Errorf("reserve pack bytes %s: fence revocation: %w", base, err)
	}
	*dec = ReserveDecision{RenewalDegraded: true}
	return nil
}

// QuotaLedgerState reads the group's ledger state in one snapshot (see
// PackQuotaLedgerState). Usage is accounted at group scope in the manifest
// (namespace usage rolls up to the base — the quota accounting unit), so
// UsageBytes reports the group counter.
func (s *S3WAL) QuotaLedgerState(ctx context.Context, group []string, ttl time.Duration, excludeKeys ...string) (PackQuotaLedgerState, error) {
	assertf(len(group) > 0 && group[0] != "" && ttl > 0, "QuotaLedgerState(%v, %v)", group, ttl)
	m, err := s.viewManifest(ctx, baseRepoOf(group[0]))
	if err != nil {
		return PackQuotaLedgerState{}, err
	}
	excluded := make(map[string]bool, len(excludeKeys))
	for _, k := range excludeKeys {
		excluded[k] = true
	}
	// The admission-fence marker mirrors into the advisory bit exactly
	// like the SQL twins' EXISTS subquery (see PackQuotaLedgerState):
	// while armed, every finite admission WILL deny, and the adoption
	// preflight must reject BEFORE streaming and hashing the legacy
	// object (Codex round 8 on PR #156). Advisory only — the
	// authoritative check stays inside ReservePackBytes.
	_, armed, err := s.scanGet(ctx, ScanJobAbsence, ScanScopeAbsenceAdmissionFence)
	if err != nil {
		return PackQuotaLedgerState{}, err
	}
	st := PackQuotaLedgerState{UsageBytes: m.UsageBytes, RenewalDegraded: armed}
	for key, r := range m.Reservations {
		if s3walIsClaimKey(key) || excluded[key] {
			continue
		}
		if s.s3walReservationLive(m, r.CreatedAt, ttl) {
			st.ReservedBytes = addSat(st.ReservedBytes, r.Bytes)
		}
	}
	for _, repo := range group {
		if mem := m.Member(repo); mem != nil {
			for _, ret := range mem.Retained {
				st.RetainedBytes = addSat(st.RetainedBytes, ret.Bytes)
			}
		}
	}
	return st, nil
}

// RetirePackAttempts retires one same-key attempt per entry (see
// RefStore): gen-fenced decrement, delete at zero attempts. Recovery class
// OPS: nonce, then op-ID membership, else fail closed.
func (s *S3WAL) RetirePackAttempts(ctx context.Context, group []string, gens map[string]string) error {
	assertf(len(group) > 0 && group[0] != "" && len(gens) > 0, "RetirePackAttempts(%v, %v)", group, gens)
	opID := s3wal.NewOpID()
	return s.mutate(ctx, baseRepoOf(group[0]), s3walMutation{
		op: "retire pack attempts",
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			changed := false
			for key, gen := range gens {
				r := m.Reservations[key]
				if r == nil || r.Gen != gen {
					continue // absent or re-minted: no-op (round 33)
				}
				r.Attempts--
				r.OpID = opID
				if r.Attempts <= 0 {
					delete(m.Reservations, key)
				}
				changed = true
			}
			if changed {
				m.RecordOp(opID)
			}
			return changed, nil
		},
		recover: func(fresh *s3wal.Manifest) s3walVerdict {
			if fresh.OpApplied(opID) {
				return s3walApplied
			}
			return s3walFailClosed
		},
	})
}

// SweepExpiredPackReservations prunes reservation rows fleet-wide: one
// prefix walk, one CAS per group holding expired rows (L+M, idempotent).
// Non-claim rows expire at olderThan (0 = everything — the test/recovery
// hatch); claim leases expire only at AdoptionClaimTTL. Like the SQL
// twins (Codex round 34 on PR #132), consuming an expired UN-FOLDED
// absence-cover row records the durable lapse-evidence marker before the
// pruning CAS commits.
func (s *S3WAL) SweepExpiredPackReservations(ctx context.Context, olderThan time.Duration) (int64, error) {
	assertf(olderThan >= 0, "SweepExpiredPackReservations: negative age %v", olderThan)
	var total int64
	err := s.s3walEachBase(ctx, "", func(base string) error {
		var pruned int64
		err := s.mutate(ctx, base, s3walMutation{
			op: "sweep reservations",
			// Same externalized lapse marker as ReservePackBytes: never
			// derived from a seed (see s3walMutation.seedUnfit).
			seedUnfit: func(m *s3wal.Manifest) bool { return s.s3walSeedDoomsCover(base, m, olderThan) },
			fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
				pruned = 0
				doomedCover := ""
				for key, r := range m.Reservations {
					window := olderThan
					what := "reservation sweep"
					if s3walIsClaimKey(key) {
						window = AdoptionClaimTTL
						what = "claim sweep"
					}
					if s.s3walExpiryAllowed(base, m, r.CreatedAt, window, what) {
						if doomedCover == "" && s3walLapseEvidence(key, r.Owner) {
							doomedCover = key
						}
						delete(m.Reservations, key)
						pruned++
					}
				}
				if doomedCover != "" {
					if err := s.s3walRecordLapse(ctx, doomedCover); err != nil {
						return false, fmt.Errorf("sweep reservations lapse marker: %w", err)
					}
				}
				// s3wal-internal divergence from the SQL twins, which
				// have no W6 delete-lease protocol (Codex on PR #156):
				// a leaked lease — an ambiguous DELETE or a failed
				// post-delete clear leaves its entry, and registration
				// only ignores expired ones — is reaped once strictly
				// past the FULL blocking window, under the same guarded
				// clock rules as every destructive verdict. The
				// olderThan=0 test/recovery hatch never waives this
				// window: an in-window lease may fence an in-flight
				// DELETE. Reaped leases are not counted in the returned
				// reservation total.
				locksReaped := 0
				for key, lock := range m.DeleteLocks {
					if s.s3walExpiryAllowed(base, m, lock.CreatedAt, s3walDeleteLockWindow(), "delete lease reap") {
						delete(m.DeleteLocks, key)
						locksReaped++
					}
				}
				return pruned > 0 || locksReaped > 0, nil
			},
			// ID: re-applying deletes nothing twice; the count may
			// under-report on an ambiguous landed write, which only
			// defers rows to the next pass.
		})
		if err != nil {
			return err
		}
		total += pruned
		return nil
	})
	if err != nil {
		return total, err
	}
	return total, nil
}

// errS3WALStopWalk halts a fleet walk early: a callback returns it when
// its work budget is exhausted, and the walk stops issuing listing pages
// instead of visiting every remaining manifest (Codex round 11 on PR
// #156: a capped reconcile otherwise kept listing the whole fleet).
var errS3WALStopWalk = errors.New("s3wal: stop fleet walk")

// s3walEachBase walks every base group under the manifest prefix in byte
// order, strictly after the given base ("" = from the start), invoking fn
// per base. Listing pages are bounded; fn errors abort the walk, except
// errS3WALStopWalk which ends it cleanly.
func (s *S3WAL) s3walEachBase(ctx context.Context, afterBase string, fn func(base string) error) error {
	after := ""
	if afterBase != "" {
		after = s3wal.ManifestKey(afterBase)
	}
	const page = 512
	for {
		keys, err := s.cas.List(ctx, s3wal.RepoPrefix, after, page)
		if err != nil {
			return fmt.Errorf("s3wal fleet walk: %w", err)
		}
		for _, key := range keys {
			base, ok := s3wal.BaseFromManifestKey(key)
			if !ok {
				// Foreign object under the manifest prefix: crash-and-burn
				// (nothing else may live here — plan §2.1).
				return fmt.Errorf("s3wal fleet walk: foreign key %q under %s", key, s3wal.RepoPrefix)
			}
			if err := fn(base); err != nil {
				if errors.Is(err, errS3WALStopWalk) {
					return nil // budget exhausted: stop listing
				}
				return err
			}
		}
		if len(keys) < page {
			return nil
		}
		after = keys[len(keys)-1]
	}
}

// IsRetainedPack reports whether (repo, key) is a recorded superseded blob.
func (s *S3WAL) IsRetainedPack(ctx context.Context, repo, key string) (bool, error) {
	assertf(repo != "" && key != "", "IsRetainedPack(%q, %q)", repo, key)
	m, err := s.viewManifest(ctx, baseRepoOf(repo))
	if err != nil {
		return false, err
	}
	mem := m.Member(repo)
	if mem == nil {
		return false, nil
	}
	_, ok := mem.Retained[key]
	return ok, nil
}

// ReclaimRetainedPack removes the retained row, debits the usage counter
// (clamped at zero), and bumps the shared floor generation when bytes
// actually left (Codex round 59). Recovery class PRED/ID: re-applying on a
// fresh snapshot finds the row gone and reports 0 — no double debit.
func (s *S3WAL) ReclaimRetainedPack(ctx context.Context, repo, key string) (int64, error) {
	assertf(repo != "" && key != "", "ReclaimRetainedPack(%q, %q)", repo, key)
	var bytes int64
	err := s.mutate(ctx, baseRepoOf(repo), s3walMutation{
		op: "reclaim retained pack",
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			bytes = 0
			mem := m.Member(repo)
			if mem == nil {
				return false, nil
			}
			ret, ok := mem.Retained[key]
			if !ok {
				return false, nil // idempotent no-op
			}
			bytes = ret.Bytes
			delete(mem.Retained, key)
			m.UsageBytes = max(m.UsageBytes-bytes, 0)
			if bytes > 0 {
				m.LFS.AdoptionGen++ // atomic with the debit (round 59)
			}
			return true, nil
		},
	})
	if err != nil {
		return 0, err
	}
	return bytes, nil
}
