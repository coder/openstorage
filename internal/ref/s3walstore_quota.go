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
	"fmt"
	"log/slog"
	"time"

	"github.com/coder/code-storage/internal/ref/s3wal"
)

// s3walReservationLive reports whether a reservation stamp is inside ttl
// under the §2.4 elapsed rule.
func (s *S3WAL) s3walReservationLive(m *s3wal.Manifest, stamp time.Time, ttl time.Duration) bool {
	return s3wal.Elapsed(s.now(), m.Clk, stamp) < ttl
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
	if !s3wal.SkewOK(now, m.Clk, ttl) {
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
	slog.Warn("s3wal clock skew guard tripped: demoting expiry verdict to report-only",
		"base", base, "what", what, "manifest_clk", m.Clk, "local_now", s.now(), "ttl", ttl)
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
	var dec ReserveDecision
	err := s.mutate(ctx, base, s3walMutation{
		op:    "reserve pack bytes",
		grows: true,
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			dec = ReserveDecision{}
			if m.PurgedAt != nil {
				return false, fmt.Errorf("reserve pack bytes %s: %w", base, ErrRepoPurged)
			}
			// Self-healing GC (Codex round 5 on #51): prune this group's
			// expired non-claim rows inside the admission CAS, bounding
			// physical rows with no sweeper required. Committed even on
			// denial, like the SQL transaction.
			changed := false
			for key, r := range m.Reservations {
				if s3walIsClaimKey(key) {
					continue
				}
				if s.s3walExpiryAllowed(base, m, r.CreatedAt, ttl, "reservation prune") {
					delete(m.Reservations, key)
					changed = true
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
	return dec, nil
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
	st := PackQuotaLedgerState{UsageBytes: m.UsageBytes}
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
// hatch); claim leases expire only at AdoptionClaimTTL.
func (s *S3WAL) SweepExpiredPackReservations(ctx context.Context, olderThan time.Duration) (int64, error) {
	assertf(olderThan >= 0, "SweepExpiredPackReservations: negative age %v", olderThan)
	var total int64
	err := s.s3walEachBase(ctx, "", func(base string) error {
		var pruned int64
		err := s.mutate(ctx, base, s3walMutation{
			op: "sweep reservations",
			fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
				pruned = 0
				for key, r := range m.Reservations {
					window := olderThan
					what := "reservation sweep"
					if s3walIsClaimKey(key) {
						window = AdoptionClaimTTL
						what = "claim sweep"
					}
					if s.s3walExpiryAllowed(base, m, r.CreatedAt, window, what) {
						delete(m.Reservations, key)
						pruned++
					}
				}
				return pruned > 0, nil
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

// s3walEachBase walks every base group under the manifest prefix in byte
// order, strictly after the given base ("" = from the start), invoking fn
// per base. Listing pages are bounded; fn errors abort the walk.
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
