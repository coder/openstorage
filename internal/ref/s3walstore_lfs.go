// S3WAL LFS registry: verified-object registration with legacy-spelling
// migration and twin reconciliation (ADR-41), the object-count cap,
// adoption claims and generations, mismatch markers, and the §2.5 W6
// delete-lease GuardedLFSDelete (docs/plans/s3wal-plan.md).
//
// LFS state is keyed by the BASE repository (ADR-35): the manifest's
// single LFS block is the group's registry.
package ref

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/coder/open-storage/internal/ref/s3wal"
)

// errS3WALDeleteLockBlocked signals a registration path that a live delete
// lease fences its key: the caller polls (never an unconditional sleep to
// the lease horizon) and proceeds the moment the lock clears (§2.5 W6).
var errS3WALDeleteLockBlocked = errors.New("s3wal: key fenced by a live delete lease")

// s3walDeleteLockWindow is the FULL fencing window registrations honor:
// created_at + TTL + deleteIOTimeout + safety — a lock that missed its
// final renewal still fences the worst-case in-flight DELETE.
func s3walDeleteLockWindow() time.Duration {
	ttl, io, safety := s3walDeleteLockTTL, s3walDeleteIOTimeout, s3walDeleteLockSafety
	skew := min(15*time.Minute, ttl/4)
	assertf(ttl >= io+safety+skew,
		"s3wal delete-lease invariant violated: TTL %v < deleteIOTimeout %v + safety %v + skew %v", ttl, io, safety, skew)
	return ttl + io + safety
}

// s3walDeleteLockLive reports whether lock still fences key registrations.
// Treating a lock as expired permits destruction-adjacent behavior, so the
// verdict rides s3walExpiryAllowed (skew guard demotes to "still live").
func (s *S3WAL) s3walDeleteLockLive(base string, m *s3wal.Manifest, lock *s3wal.DeleteLock) bool {
	if lock == nil {
		return false
	}
	return !s.s3walExpiryAllowed(base, m, lock.CreatedAt, s3walDeleteLockWindow(), "delete lease expiry")
}

// RegisterLFSObject — see RefStore. One CAS: legacy migration / twin
// reconcile, idempotent insert, atomic cap check, reservation settlement,
// mark clearing, usage credit. Recovery class PRED/ID: re-applying on a
// fresh snapshot is zero-growth by construction.
func (s *S3WAL) RegisterLFSObject(ctx context.Context, repo, key string, bytes int64) error {
	validateRegisterLFS(repo, key, bytes)
	return s.registerLFSObject(ctx, repo, key, bytes, false)
}

// RegisterAdoptedLFSObject — see RefStore: the adoption-generation bump
// commits atomically with the registration (Codex round 51).
func (s *S3WAL) RegisterAdoptedLFSObject(ctx context.Context, repo, key string, bytes int64) error {
	validateRegisterLFS(repo, key, bytes)
	return s.registerLFSObject(ctx, repo, key, bytes, true)
}

func (s *S3WAL) registerLFSObject(ctx context.Context, repo, key string, bytes int64, adopted bool) error {
	base := baseRepoOf(repo)
	inserted := false
	for {
		err := s.mutate(ctx, base, s3walMutation{
			op: "register lfs object",
			// Growth is dynamic (Codex round 4 on PR #156): only a NEW
			// row insert grows the manifest. Re-registration of an
			// existing object — the strongest liveness signal, clearing
			// the unreferenced mark and consuming its reservation — and
			// the in-place legacy migration must pass even at the hard
			// cap, or the row stays reclamation-eligible while its
			// refresh is refused.
			grows: func() bool { return inserted },
			fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
				inserted = false
				if m.PurgedAt != nil {
					return false, fmt.Errorf("register lfs object %s: %w", key, ErrRepoPurged)
				}
				// A live delete lease fences this key (§2.5 W6): the guarded
				// delete's I/O may still be in flight, and registering over
				// it could leave a registered row pointing at deleted bytes.
				if s.s3walDeleteLockLive(base, m, m.DeleteLocks[key]) {
					return false, errS3WALDeleteLockBlocked
				}
				changed := false
				// An EXPIRED lease observed here is a leak (ambiguous
				// DELETE / failed post-delete clear — Codex on PR #156):
				// reap it opportunistically instead of ignoring it, or
				// the entry outlives every window until the sweep runs.
				if _, leaked := m.DeleteLocks[key]; leaked {
					delete(m.DeleteLocks, key)
					changed = true
				}
				// Legacy-spelling migration + twin reconcile (ADR-41).
				if legacy, ok := lfsLegacyRowKey(key); ok {
					if lrow := m.LFS.Objects[legacy]; lrow != nil {
						if m.LFS.Objects[key] == nil {
							// In-place migration: bytes stay counted;
							// created_at REFRESHES (Codex round 89) and the
							// mark clears.
							s3walEnsureLFSObjects(m)[key] = &s3wal.LFSObject{Bytes: lrow.Bytes, CreatedAt: clk}
							delete(m.LFS.Objects, legacy)
						} else {
							s3walReconcileLegacyRow(m, legacy)
						}
						changed = true
					}
				}
				row := m.LFS.Objects[key]
				if row == nil {
					s3walEnsureLFSObjects(m)[key] = &s3wal.LFSObject{Bytes: bytes, CreatedAt: clk}
					m.LFS.Count++
					if m.LFS.Count > MaxLFSObjectsPerRepo {
						// Abort the WHOLE CAS (nothing above is written) —
						// the SQL rollback twin.
						return false, ErrLFSObjectCapExceeded
					}
					inserted = true
					changed = true
				}
				if _, ok := m.Reservations[key]; ok {
					delete(m.Reservations, key) // settlement by key, any owner
					changed = true
				}
				if inserted {
					if adopted {
						m.LFS.AdoptionGen++ // atomic with insert+credit
					}
					m.UsageBytes += bytes
				} else if row.UnreferencedSince != nil {
					// Re-registration is the strongest liveness signal
					// (issue #73): clear the mark atomically, creditless.
					row.UnreferencedSince = nil
					changed = true
				}
				return changed, nil
			},
		})
		if errors.Is(err, errS3WALDeleteLockBlocked) {
			// Poll until the lease clears (or its window passes); proceed
			// immediately once it does — never an unconditional sleep.
			if err := s.sleepPoll(ctx); err != nil {
				return fmt.Errorf("register lfs object %s: %w", key, err)
			}
			continue
		}
		return err
	}
}

// sleepPoll waits one delete-lease poll interval, ctx-bounded.
func (s *S3WAL) sleepPoll(ctx context.Context) error {
	timer := time.NewTimer(s3walDeleteLockPoll)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// s3walEnsureLFSObjects returns the manifest's LFS object map, allocating
// it on first use.
func s3walEnsureLFSObjects(m *s3wal.Manifest) map[string]*s3wal.LFSObject {
	if m.LFS.Objects == nil {
		m.LFS.Objects = make(map[string]*s3wal.LFSObject)
	}
	return m.LFS.Objects
}

// s3walReconcileLegacyRow drops a legacy row whose canonical twin exists,
// refunding the count and bytes it double-charged and bumping the adoption
// generation atomically with the refund (ADR-41, Codex rounds 79+82).
// Caller has verified both rows exist.
func s3walReconcileLegacyRow(m *s3wal.Manifest, legacy string) {
	row := m.LFS.Objects[legacy]
	assertf(row != nil, "s3walReconcileLegacyRow: no legacy row %q", legacy)
	delete(m.LFS.Objects, legacy)
	m.LFS.Count = max(m.LFS.Count-1, 0)
	m.UsageBytes = max(m.UsageBytes-row.Bytes, 0)
	m.LFS.AdoptionGen++
}

// ReconcileLegacyLFSObjects — see RefStore: fleet walk, twin-drop + refund
// per group CAS (L+M, idempotent). Pages honor reconcileLegacyLFSBatch and
// the reconcileLegacyLFSMaxPages invocation cap (partial=true when capped).
func (s *S3WAL) ReconcileLegacyLFSObjects(ctx context.Context) (int64, bool, error) {
	var n int64
	pages := 0
	partial := false
	err := s.s3walEachBase(ctx, "", func(base string) error {
		for {
			if pages >= reconcileLegacyLFSMaxPages {
				partial = true // clean partial success (security round 127)
				// Stop the WALK too (Codex round 11 on PR #156): per-base
				// nil returns keep the fleet listing running unbounded
				// after the invocation cap was reached.
				return errS3WALStopWalk
			}
			var reconciled int
			err := s.mutate(ctx, base, s3walMutation{
				op: "reconcile legacy lfs",
				fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
					reconciled = 0
					var legacies []string
					for key := range m.LFS.Objects {
						canonical, ok := canonicalFromLegacyLFSRowKey(base, key)
						if !ok {
							continue // canonical or foreign-shaped key
						}
						if m.LFS.Objects[canonical] == nil {
							continue // no twin: the row is the object
						}
						legacies = append(legacies, key)
						if len(legacies) == reconcileLegacyLFSBatch {
							break
						}
					}
					for _, legacy := range legacies {
						s3walReconcileLegacyRow(m, legacy)
						reconciled++
					}
					return reconciled > 0, nil
				},
			})
			if err != nil {
				return err
			}
			if reconciled == 0 {
				return nil
			}
			n += int64(reconciled)
			pages++
			if reconciled < reconcileLegacyLFSBatch {
				return nil
			}
		}
	})
	if err != nil {
		return n, false, err
	}
	return n, partial, nil
}

// RecordLFSMismatch — see RefStore (#104): fleet-persistent upsert;
// purge-fenced repos silently no-op like every row-creating write.
func (s *S3WAL) RecordLFSMismatch(ctx context.Context, repo, key, fingerprint string) error {
	assertf(repo != "" && key != "" && fingerprint != "", "RecordLFSMismatch(%q, %q)", repo, key)
	freshKey := false
	return s.mutate(ctx, baseRepoOf(repo), s3walMutation{
		op: "record lfs mismatch",
		// Dynamic growth: overwriting an existing key's fingerprint is
		// an in-place upsert, not an insert.
		grows: func() bool { return freshKey },
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			freshKey = false
			if m.PurgedAt != nil {
				return false, nil
			}
			if _, exists := m.LFS.Mismatches[key]; !exists {
				freshKey = true
			}
			if m.LFS.Mismatches[key] == fingerprint {
				return false, nil
			}
			if m.LFS.Mismatches == nil {
				m.LFS.Mismatches = make(map[string]string)
			}
			m.LFS.Mismatches[key] = fingerprint
			return true, nil
		},
	})
}

// LFSMismatchFingerprint — see RefStore.
func (s *S3WAL) LFSMismatchFingerprint(ctx context.Context, repo, key string) (string, bool, error) {
	assertf(repo != "" && key != "", "LFSMismatchFingerprint(%q, %q)", repo, key)
	m, err := s.viewManifest(ctx, baseRepoOf(repo))
	if err != nil {
		return "", false, err
	}
	fp, ok := m.LFS.Mismatches[key]
	return fp, ok, nil
}

// ClearLFSMismatch — see RefStore.
func (s *S3WAL) ClearLFSMismatch(ctx context.Context, repo, key string) error {
	assertf(repo != "" && key != "", "ClearLFSMismatch(%q, %q)", repo, key)
	return s.mutate(ctx, baseRepoOf(repo), s3walMutation{
		op: "clear lfs mismatch",
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			if _, ok := m.LFS.Mismatches[key]; !ok {
				return false, nil
			}
			delete(m.LFS.Mismatches, key)
			return true, nil
		},
	})
}

// HasLFSObject reports whether (repo, key) is a registered LFS object.
func (s *S3WAL) HasLFSObject(ctx context.Context, repo, key string) (bool, error) {
	assertf(repo != "" && key != "", "HasLFSObject(%q, %q)", repo, key)
	m, err := s.viewManifest(ctx, baseRepoOf(repo))
	if err != nil {
		return false, err
	}
	return m.LFS.Objects[key] != nil, nil
}

// TouchLFSObject — see RefStore: clears the reclamation mark and reports
// whether the row exists (the zero-growth liveness handshake, issue #73).
func (s *S3WAL) TouchLFSObject(ctx context.Context, repo, key string) (bool, error) {
	assertf(repo != "" && key != "", "TouchLFSObject: empty repo/key")
	touched := false
	err := s.mutate(ctx, baseRepoOf(repo), s3walMutation{
		op: "touch lfs object",
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			row := m.LFS.Objects[key]
			touched = row != nil
			if row == nil || row.UnreferencedSince == nil {
				return false, nil
			}
			row.UnreferencedSince = nil
			return true, nil
		},
	})
	if err != nil {
		return false, fmt.Errorf("touch lfs object %s: %w", key, err)
	}
	return touched, nil
}

// LFSObjectRegisteredAt — see RefStore.
func (s *S3WAL) LFSObjectRegisteredAt(ctx context.Context, repo, key string) (time.Time, int64, bool, error) {
	assertf(repo != "" && key != "", "LFSObjectRegisteredAt(%q, %q)", repo, key)
	m, err := s.viewManifest(ctx, baseRepoOf(repo))
	if err != nil {
		return time.Time{}, 0, false, err
	}
	row := m.LFS.Objects[key]
	if row == nil {
		return time.Time{}, 0, false, nil
	}
	return row.CreatedAt, row.Bytes, true, nil
}

// BumpLFSAdoptionGen — see RefStore: purge-fenced repos silently no-op
// (nothing left to invalidate — PR #105 round 10).
func (s *S3WAL) BumpLFSAdoptionGen(ctx context.Context, repo string) error {
	assertf(repo != "", "BumpLFSAdoptionGen(%q)", repo)
	return s.mutate(ctx, baseRepoOf(repo), s3walMutation{
		op: "bump lfs adoption gen",
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			if m.PurgedAt != nil {
				return false, nil
			}
			m.LFS.AdoptionGen++
			return true, nil
		},
		// ID: an ambiguous re-apply over-bumps, which is a spurious cache
		// invalidation — safe (Appendix A).
	})
}

// LFSAdoptionGen — see RefStore. The generation is accounted at group
// scope (the floor caches' quota accounting unit is the base repository).
func (s *S3WAL) LFSAdoptionGen(ctx context.Context, repo string) (int64, error) {
	assertf(repo != "", "LFSAdoptionGen(%q)", repo)
	m, err := s.viewManifest(ctx, baseRepoOf(repo))
	if err != nil {
		return 0, err
	}
	return m.LFS.AdoptionGen, nil
}

// CountLFSObjects returns repo's registered LFS object count — an O(1)
// maintained-counter read.
func (s *S3WAL) CountLFSObjects(ctx context.Context, repo string) (int64, error) {
	assertf(repo != "", "CountLFSObjects(%q)", repo)
	m, err := s.viewManifest(ctx, baseRepoOf(repo))
	if err != nil {
		return 0, err
	}
	return m.LFS.Count, nil
}

// GuardedLFSDelete — see RefStore. SQL serializes delete-vs-register under
// the ledger lock; an object store cannot join a check and a blob DELETE
// in one atom, so s3wal PREVENTS the race with the §2.5 W6 delete lease:
// (i) CAS-insert the lock only while the key is unregistered and no live
// lock exists; (ii) run del under a hard deleteIOTimeout with a TTL/3
// renewal loop; (iii) CAS-clear the lock. Registrations block on live
// locks for TTL + deleteIOTimeout + safety, so even a lease that missed
// its final renewal fences the worst-case in-flight DELETE. Recovery class
// LEASE.
func (s *S3WAL) GuardedLFSDelete(ctx context.Context, repo, key string, del func(context.Context) error) (bool, error) {
	assertf(repo != "" && key != "" && del != nil, "GuardedLFSDelete(%q, %q)", repo, key)
	base := baseRepoOf(repo)
	owner := NewReservationOwner()
	opID := s3wal.NewOpID()
	// Phase (i): acquire the delete lease.
	for {
		var fencedDirect, registered, blocked bool
		err := s.mutate(ctx, base, s3walMutation{
			op: "lfs delete lease",
			// Deliberately cap-EXEMPT, fresh inserts included (Codex
			// round 11 on PR #156): the transient lease is deletion
			// machinery — an over-cap manifest (reachable through
			// zero-growth mutations) must still reap unregistered LFS
			// blobs, and the row is removed again by phase (iii) or
			// expiry, so it never contributes durable growth.
			fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
				fencedDirect, registered, blocked = false, false, false
				if m.PurgedAt != nil {
					// Purge-fenced repos need no writer guard: fenced
					// registrations can never commit, and lease rows would
					// recreate state the purge cleaned (PR #105 round 14).
					fencedDirect = true
					return false, nil
				}
				if m.LFS.Objects[key] != nil {
					registered = true // a verified upload won: never delete
					return false, nil
				}
				if s.s3walDeleteLockLive(base, m, m.DeleteLocks[key]) {
					blocked = true // another deleter holds the lease
					return false, nil
				}
				if m.DeleteLocks == nil {
					m.DeleteLocks = make(map[string]*s3wal.DeleteLock)
				}
				m.DeleteLocks[key] = &s3wal.DeleteLock{Owner: owner, OpID: opID, CreatedAt: clk}
				return true, nil
			},
			recover: func(fresh *s3wal.Manifest) s3walVerdict {
				if lock := fresh.DeleteLocks[key]; lock != nil && lock.Owner == owner {
					return s3walApplied
				}
				return s3walRerun
			},
		})
		if err != nil {
			return false, err
		}
		if fencedDirect {
			if err := del(ctx); err != nil {
				return false, err
			}
			return true, nil
		}
		if registered {
			return false, nil
		}
		if !blocked {
			break // lease held
		}
		if err := s.sleepPoll(ctx); err != nil {
			return false, fmt.Errorf("guarded lfs delete %s: %w", key, err)
		}
	}
	// Phase (ii): run del under the hard I/O timeout while a background
	// loop renews the lease every TTL/3. A renewal loss means the lease
	// was stolen or expired: before dispatch we abort; after dispatch the
	// caller may NOT treat the key as deleted (the blocking window fences
	// the in-flight DELETE).
	renewCtx, stopRenew := context.WithCancel(ctx)
	lost := make(chan struct{})
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(s3walDeleteLockTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
			}
			renewed := false
			var attemptClk time.Time
			err := s.mutate(renewCtx, base, s3walMutation{
				op: "lfs delete lease renew",
				fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
					renewed = false
					attemptClk = clk
					lock := m.DeleteLocks[key]
					if lock == nil || lock.Owner != owner {
						return false, nil // lease lost
					}
					lock.CreatedAt = clk
					renewed = true
					return true, nil
				},
				recover: func(fresh *s3wal.Manifest) s3walVerdict {
					// Same proof as RenewAdoptionClaim (Codex round 6 on
					// PR #156, security): a dropped renewal PUT leaves our
					// owner with the OLD stamp, and believing it succeeded
					// lets the lease silently expire under the in-flight
					// DELETE. Applied only when the stamp advanced to this
					// attempt's clock.
					if lock := fresh.DeleteLocks[key]; lock != nil && lock.Owner == owner && !lock.CreatedAt.Before(attemptClk) {
						renewed = true
						return s3walApplied
					}
					return s3walRerun
				},
			})
			if err != nil || !renewed {
				close(lost)
				return
			}
		}
	}()
	renewalLost := func() bool {
		select {
		case <-lost:
			return true
		default:
			return false
		}
	}
	// Pre-dispatch renewal-loss check: never send the DELETE without a
	// live lease.
	if renewalLost() {
		stopRenew()
		<-renewDone
		return false, fmt.Errorf("guarded lfs delete %s: delete lease lost before dispatch", key)
	}
	dctx, cancel := context.WithTimeout(ctx, s3walDeleteIOTimeout)
	derr := del(dctx)
	cancel()
	stopRenew()
	<-renewDone
	if renewalLost() {
		return false, fmt.Errorf("guarded lfs delete %s: delete lease lost while the delete was in flight — key may not be treated as deleted", key)
	}
	if derr != nil {
		// The DELETE may still complete server-side: leave the lease to
		// expire so the blocking window keeps fencing registrations.
		return false, derr
	}
	// Phase (iii): clear the lease. The delete succeeded either way; a
	// failed clear only blocks same-key registrations until expiry.
	err := s.mutate(ctx, base, s3walMutation{
		op: "lfs delete lease clear",
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			lock := m.DeleteLocks[key]
			if lock == nil || lock.Owner != owner {
				return false, nil
			}
			delete(m.DeleteLocks, key)
			return true, nil
		},
		recover: func(fresh *s3wal.Manifest) s3walVerdict {
			if lock := fresh.DeleteLocks[key]; lock == nil || lock.Owner != owner {
				return s3walApplied
			}
			return s3walRerun
		},
	})
	if err != nil {
		slogWarnLeaseClear(key, err)
	}
	return true, nil
}

// slogWarnLeaseClear logs a failed post-delete lease clear: the bytes are
// gone (deleted=true is honest); the stale lease expires on its window.
func slogWarnLeaseClear(key string, err error) {
	slog.Warn("s3wal delete lease clear failed; lease will expire on its window", "key", key, "err", err)
}

// TryAdoptionClaim — see RefStore: claims are reservation entries under
// the /adopt-claim key suffix (exactly the SQL spelling), stolen only past
// AdoptionClaimTTL under the §2.4 rules. Purge-fenced repos report
// claimed=false. Recovery class PRED: owner token == ours ⇒ applied.
func (s *S3WAL) TryAdoptionClaim(ctx context.Context, repo, key string) (bool, string, error) {
	assertf(repo != "" && key != "", "TryAdoptionClaim(%q, %q)", repo, key)
	base := baseRepoOf(repo)
	owner := NewReservationOwner()
	claimKey := key + AdoptionClaimSuffix
	claimed := false
	freshClaim := false
	err := s.mutate(ctx, base, s3walMutation{
		op: "try adoption claim",
		// Dynamic growth: stealing an expired claim rewrites its entry
		// in place — an over-cap manifest must not wedge the adoption
		// mutual exclusion on dead holders.
		grows: func() bool { return freshClaim },
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			claimed = false
			freshClaim = false
			if m.PurgedAt != nil {
				return false, nil
			}
			if r := m.Reservations[claimKey]; r != nil {
				if !s.s3walExpiryAllowed(base, m, r.CreatedAt, AdoptionClaimTTL, "adoption claim steal") {
					return false, nil // a live claim exists
				}
				// Expired: take it over (owner rotates, clock restarts).
				r.Owner = owner
				r.CreatedAt = clk
				claimed = true
				return true, nil
			}
			if m.Reservations == nil {
				m.Reservations = make(map[string]*s3wal.Reservation)
			}
			freshClaim = true
			m.Reservations[claimKey] = &s3wal.Reservation{Owner: owner, Bytes: 1, CreatedAt: clk}
			claimed = true
			return true, nil
		},
		recover: func(fresh *s3wal.Manifest) s3walVerdict {
			if r := fresh.Reservations[claimKey]; r != nil && r.Owner == owner {
				claimed = true
				return s3walApplied
			}
			return s3walRerun
		},
	})
	if err != nil {
		return false, "", err
	}
	if !claimed {
		return false, "", nil
	}
	return true, owner, nil
}

// RenewAdoptionClaim — see RefStore: restarts the TTL clock while the live
// claim still belongs to owner.
func (s *S3WAL) RenewAdoptionClaim(ctx context.Context, key, owner string) (bool, error) {
	assertf(key != "" && owner != "", "RenewAdoptionClaim(%q, %q)", key, owner)
	claimKey := key + AdoptionClaimSuffix
	base := s3walBaseOfLFSKey(key)
	renewed := false
	var attemptClk time.Time
	err := s.mutate(ctx, base, s3walMutation{
		op: "renew adoption claim",
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			renewed = false
			attemptClk = clk
			r := m.Reservations[claimKey]
			if r == nil || r.Owner != owner || !s.s3walReservationLive(m, r.CreatedAt, AdoptionClaimTTL) {
				return false, nil // lease lost: stop hashing
			}
			r.CreatedAt = clk
			renewed = true
			return true, nil
		},
		recover: func(fresh *s3wal.Manifest) s3walVerdict {
			// Owner match alone is NOT proof (Codex round 6 on PR #156,
			// security): renewal changes only CreatedAt, so a dropped PUT
			// leaves the same owner with the OLD stamp — reporting success
			// would let a near-expiry claim be stolen before the next tick
			// while the holder resumes work believing the TTL restarted.
			// Applied only when the stamp provably advanced to this
			// attempt's clock (only the token holder writes newer stamps
			// under our owner); an older stamp reruns until the renewal is
			// proven or fn reports the lease genuinely lost.
			if r := fresh.Reservations[claimKey]; r != nil && r.Owner == owner && !r.CreatedAt.Before(attemptClk) {
				renewed = true
				return s3walApplied
			}
			return s3walRerun
		},
	})
	if err != nil {
		return false, err
	}
	return renewed, nil
}

// ReleaseAdoptionClaim — see RefStore: owner-fenced delete (best-effort;
// a crashed winner's claim expires via AdoptionClaimTTL instead).
func (s *S3WAL) ReleaseAdoptionClaim(ctx context.Context, key, owner string) error {
	assertf(key != "" && owner != "", "ReleaseAdoptionClaim(%q, %q)", key, owner)
	claimKey := key + AdoptionClaimSuffix
	return s.mutate(ctx, s3walBaseOfLFSKey(key), s3walMutation{
		op: "release adoption claim",
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			r := m.Reservations[claimKey]
			if r == nil || r.Owner != owner {
				return false, nil
			}
			delete(m.Reservations, claimKey)
			return true, nil
		},
	})
}

// s3walBaseOfLFSKey derives the base repo group an adoption-claim key
// belongs to. Claims are minted on LFS blob keys (lfs/<repo>/[o2/]<oid>,
// blob.LFSKey), and the repo segment is the base repository (ADR-35) —
// Renew/Release receive no repo argument, so the key is the only routing
// input, exactly like the SQL backends' repo-less claim statements.
func s3walBaseOfLFSKey(key string) string {
	rest, ok := strings.CutPrefix(key, "lfs/")
	assertf(ok, "s3walBaseOfLFSKey: %q is not an LFS blob key", key)
	i := strings.LastIndex(rest, "/")
	assertf(i > 0, "s3walBaseOfLFSKey: %q has no repo segment", key)
	repo := rest[:i]
	repo = strings.TrimSuffix(repo, "/o2")
	assertf(repo != "", "s3walBaseOfLFSKey: %q has an empty repo segment", key)
	return baseRepoOf(repo)
}
