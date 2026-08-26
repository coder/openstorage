// S3WAL absence-cover surface (issues #101/#125, PR #132; plan §1/§2.10):
// covers are ordinary reservation-map entries in each base group's
// manifest (ReserveRestoreCover charges them like any admission), so the
// fleet listing/head/renew/CAS/delete surface is a `_wal/repo/` prefix
// walk plus per-manifest CAS — never a separate table.
//
// Raw-key spelling: the SQL backends expose RawKey as the reservation-row
// spelling (synthetic key + RepairCoverSuffix + reservKeySuffix). s3wal
// manifests key reservations on PLAIN blob keys (no era suffix — the
// documented divergence in s3walstore_quota.go), but this surface still
// EMITS the fleet-uniform decorated spelling and strips it back at the
// boundary: RawKey is a persisted cursor and fold-CAS identity (issue
// #121 stores it durably), so its observable shape must not depend on
// which backend minted it.
//
// Listing order: rows are emitted per base group in decorated-raw byte
// order, base groups in manifest byte order — a total order that matches
// the SQL backends' global raw-key byte order except when one base ID is
// a byte-prefix of another (the §2.9 family caveat: '/' vs '-' sort
// around the base boundary). Completeness and duplicate-freedom under
// cursor resume hold regardless, which is the property the renewal
// listing consumes.
package ref

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/coder/code-storage/internal/ref/s3wal"
)

// s3walAbsenceIncarnationTail matches a deterministic incarnation cover's
// MANIFEST map key tail: /<class>-e<19 digits> + RepairCoverSuffix at end
// (the plain-key twin of absenceIncarnationTailRegexp).
var s3walAbsenceIncarnationTail = regexp.MustCompile(`/(lfs|packs)-e[0-9]{19}` + regexp.QuoteMeta(RepairCoverSuffix) + `$`)

// s3walCoverSynth maps a reservation MAP key back to the synthetic cover
// key it was charged under; ok=false for every non-cover spelling
// (adoption claims, plain pack reservations, planted foreign rows).
func s3walCoverSynth(mapKey string) (string, bool) {
	synth, ok := strings.CutSuffix(mapKey, RepairCoverSuffix)
	if !ok || !strings.HasPrefix(synth, AbsenceCoverPrefix) {
		return "", false
	}
	return synth, true
}

// s3walCoverRaw decorates a cover's map key into the fleet-uniform raw
// spelling (see the header).
func s3walCoverRaw(mapKey string) string { return reservRowKey(mapKey) }

// s3walCoverBase extracts the base repo from a synthetic cover key. The
// reconciler mints every cover as <AbsenceCoverPrefix><base>/<tail> with a
// '/'-free tail (deterministic incarnations and legacy nonces alike), so
// the base is everything before the LAST '/' — asserted, because bases
// may themselves contain '/' in conformance runs.
func s3walCoverBase(synth string) string {
	rest := strings.TrimPrefix(synth, AbsenceCoverPrefix)
	assertf(rest != synth, "s3walCoverBase(%q): not an absence cover key", synth)
	i := strings.LastIndexByte(rest, '/')
	assertf(i > 0, "s3walCoverBase(%q): cover key lacks the <base>/<tail> shape", synth)
	return rest[:i]
}

// s3walCoverLowExtends reports whether base x extends base p with a byte
// that sorts BELOW '/' — the §2.9 alphabet admits exactly ',' (0x2C),
// '-' (0x2D), and '.' (0x2E) there — so x's DECORATED cover keys
// (<base>/<tail>…) sort BEFORE p's own even though x sorts after p in
// plain base order.
func s3walCoverLowExtends(x, p string) bool {
	return len(x) > len(p) && x[:len(p)] == p && x[len(p)] < '/'
}

// s3walCoverAncestors returns base's strict prefixes that base
// low-extends, outermost first — exactly the hold-back stack
// s3walEachBaseCoverOrder carries when it visits base, reconstructed from
// the cursor base's own bytes. A resumed walk must seed these: they sort
// AFTER base in decorated order but BEFORE it in the manifest listing,
// so a plain startAfter walk would never revisit them.
func s3walCoverAncestors(base string) []string {
	var chain []string
	for i := 1; i < len(base); i++ {
		if base[i] < '/' {
			chain = append(chain, base[:i])
		}
	}
	return chain
}

// s3walEachBaseCoverOrder walks bases in DECORATED (<base>+"/") byte
// order — the order cover RAW keys sort in — on top of the base-ordered
// manifest listing (Codex round 3 on PR #156). The two orders diverge
// exactly on prefix families: "abc-1" sorts AFTER "abc" as a base but its
// decorated keys sort BEFORE "abc/…" ('-' 0x2D < '/' 0x2F), so the plain
// walk would emit them too late and a raw-key cursor parked on an "abc"
// row would skip them forever. The transform is a monotonic hold-back
// stack: each listed base pops-and-emits every held base it does not
// low-extend, then is held itself until its own low-byte family drains;
// the stack at any base is exactly its low-extended prefix chain, each
// entry low-extending the one below, so popping top-first emits in
// strictly ascending decorated order. Bases nesting across a '/'
// boundary ("abc" and "abc/x" — impossible under production's [0-9a-z-]
// repo IDs, reachable only with conformance-test names) can interleave at
// the ROW level, which no per-base traversal can serialize; the caller's
// ascending-emission assert turns that into a loud failure instead of a
// silent loss.
func (s *S3WAL) s3walEachBaseCoverOrder(ctx context.Context, afterBase string, fn func(base string) error) error {
	held := s3walCoverAncestors(afterBase)
	flush := func(next string) error {
		for len(held) > 0 {
			top := held[len(held)-1]
			if next != "" && s3walCoverLowExtends(next, top) {
				return nil // next belongs to top's low family: keep holding
			}
			held = held[:len(held)-1]
			if err := fn(top); err != nil {
				return err
			}
		}
		return nil
	}
	err := s.s3walEachBase(ctx, afterBase, func(base string) error {
		if err := flush(base); err != nil {
			return err
		}
		held = append(held, base)
		return nil
	})
	if err != nil {
		return err
	}
	return flush("")
}

// ListAbsenceCovers — see the PG twin for the contract: rows ascend by
// RAW key across pages (the consumer's rotating renewal cursor wraps on
// that order). The manifest walk therefore runs in decorated cover-key
// order (s3walEachBaseCoverOrder), and cursor resume derives the cursor
// row's base group from its own spelling, re-emits that group's rows
// past the cursor, seeds the held prefix chain, then continues strictly
// after it. Every emitted row is asserted strictly above its
// predecessor — the advertised order made a crash-and-burn invariant.
func (s *S3WAL) ListAbsenceCovers(ctx context.Context, afterRaw string, limit int, ttl time.Duration) ([]AbsenceCoverRow, error) {
	validateAbsencePage(limit)
	assertf(ttl > 0, "ListAbsenceCovers: non-positive ttl %v", ttl)
	var rows []AbsenceCoverRow
	lastRaw := afterRaw
	emit := func(base string) error {
		if len(rows) == limit {
			return errS3WALWalkDone
		}
		m, err := s.viewManifest(ctx, base)
		if err != nil {
			return err
		}
		var page []AbsenceCoverRow
		for mapKey, r := range m.Reservations {
			synth, ok := s3walCoverSynth(mapKey)
			if !ok {
				continue // foreign spelling: left to reservation expiry
			}
			assertf(s3walCoverBase(synth) == base, "s3wal manifest %s holds foreign cover %q", base, synth)
			raw := s3walCoverRaw(mapKey)
			if raw <= afterRaw {
				continue
			}
			if !s.s3walReservationLive(m, r.CreatedAt, ttl) {
				continue // expired rows are invisible (Codex round 6)
			}
			page = append(page, AbsenceCoverRow{
				RawKey: raw,
				Key:    synth,
				Base:   base,
				Bytes:  r.Bytes,
				Owner:  r.Owner,
				Folded: r.Owner == AbsenceFoldedOwner,
			})
		}
		sort.Slice(page, func(i, j int) bool { return page[i].RawKey < page[j].RawKey })
		for _, row := range page {
			if len(rows) == limit {
				return errS3WALWalkDone
			}
			assertf(row.RawKey > lastRaw, "list absence covers: emission order inverted (%q after %q) — decorated walk failed to serialize base %q", row.RawKey, lastRaw, base)
			lastRaw = row.RawKey
			rows = append(rows, row)
		}
		return nil
	}
	var err error
	if afterRaw == "" {
		err = s.s3walEachBaseCoverOrder(ctx, "", emit)
	} else {
		mapKey, ok := strings.CutSuffix(afterRaw, reservKeySuffix)
		if !ok {
			return nil, fmt.Errorf("list absence covers: cursor %q is not a cover raw key", afterRaw)
		}
		synth, ok := s3walCoverSynth(mapKey)
		if !ok {
			return nil, fmt.Errorf("list absence covers: cursor %q is not a cover raw key", afterRaw)
		}
		startBase := s3walCoverBase(synth)
		if err = emit(startBase); err == nil {
			err = s.s3walEachBaseCoverOrder(ctx, startBase, emit)
		}
	}
	if err != nil && err != errS3WALWalkDone {
		return nil, err
	}
	return rows, nil
}

// AbsenceCoverHead — see the PG twin. The class prefix always spells
// <AbsenceCoverPrefix><base>/<class-tail>, so the head lookup is one
// manifest read, never a fleet walk.
func (s *S3WAL) AbsenceCoverHead(ctx context.Context, prefix string, ttl time.Duration) (AbsenceCoverRow, bool, error) {
	assertf(strings.HasPrefix(prefix, AbsenceCoverPrefix), "AbsenceCoverHead(%q): not an absence cover prefix", prefix)
	assertf(ttl > 0, "AbsenceCoverHead: non-positive ttl %v", ttl)
	base := s3walCoverBase(prefix)
	m, err := s.viewManifest(ctx, base)
	if err != nil {
		return AbsenceCoverRow{}, false, err
	}
	var head AbsenceCoverRow
	for mapKey, r := range m.Reservations {
		synth, ok := s3walCoverSynth(mapKey)
		if !ok {
			continue // foreign spelling: not ours
		}
		raw := s3walCoverRaw(mapKey)
		if !strings.HasPrefix(raw, prefix) {
			continue
		}
		if !s.s3walReservationLive(m, r.CreatedAt, ttl) {
			continue // charging into an expired incarnation resurrects it
		}
		if raw > head.RawKey {
			head = AbsenceCoverRow{RawKey: raw, Key: synth, Base: base, Bytes: r.Bytes}
		}
	}
	if head.RawKey == "" {
		return AbsenceCoverRow{}, false, nil
	}
	return head, true, nil
}

// RenewAbsenceCovers — see the PG twin: LIVE deterministic incarnation
// rows and LIVE un-folded legacy nonce rows are re-stamped with the
// write-time manifest clock; expired rows stay dead (the strict-TTL
// round-6 invariant) and folded nonces decay deliberately. One CAS per
// base holding matching rows; re-stamping again on a lost-race re-run is
// harmless (ID class).
func (s *S3WAL) RenewAbsenceCovers(ctx context.Context, ttl time.Duration) (int64, error) {
	assertf(ttl > 0, "RenewAbsenceCovers: non-positive ttl %v", ttl)
	var total int64
	err := s.s3walEachBase(ctx, "", func(base string) error {
		var touched int64
		err := s.mutate(ctx, base, s3walMutation{
			op: "renew absence covers",
			fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
				touched = 0
				for mapKey, r := range m.Reservations {
					if _, ok := s3walCoverSynth(mapKey); !ok {
						continue
					}
					if !s3walAbsenceIncarnationTail.MatchString(mapKey) && r.Owner == AbsenceFoldedOwner {
						continue // folded nonce: deliberate TTL decay
					}
					if !s.s3walReservationLive(m, r.CreatedAt, ttl) {
						continue // already expired: stays dead
					}
					r.CreatedAt = clk
					touched++
				}
				return touched > 0, nil
			},
		})
		if err != nil {
			return err
		}
		total += touched
		return nil
	})
	if err != nil {
		return total, fmt.Errorf("renew absence covers: %w", err)
	}
	return total, nil
}

// LapsedAbsenceCoverExists — see the PG twin: any physically present
// un-folded cover row aged past ttl is lapse evidence. Liveness leans
// LIVE under the §2.4 skew guard (s3walReservationLive): a clock jump
// must not mass-manufacture false lapse evidence, and rows persist, so
// genuine evidence resurfaces once clocks agree.
func (s *S3WAL) LapsedAbsenceCoverExists(ctx context.Context, ttl time.Duration) (bool, error) {
	assertf(ttl > 0, "LapsedAbsenceCoverExists: non-positive ttl %v", ttl)
	exists := false
	err := s.s3walEachBase(ctx, "", func(base string) error {
		m, err := s.viewManifest(ctx, base)
		if err != nil {
			return err
		}
		for mapKey, r := range m.Reservations {
			if _, ok := s3walCoverSynth(mapKey); !ok {
				continue
			}
			if r.Owner == AbsenceFoldedOwner {
				continue // deliberate retirement, not a lapse
			}
			if !s.s3walReservationLive(m, r.CreatedAt, ttl) {
				exists = true
				return errS3WALWalkDone
			}
		}
		return nil
	})
	if err != nil && err != errS3WALWalkDone {
		return false, fmt.Errorf("lapsed absence cover exists: %w", err)
	}
	return exists, nil
}

// CASAbsenceCoverOwner — see the PG twin: guarded on the LISTED (owner,
// bytes) pair so a concurrent re-charge always wins. Recovery class OPS
// (an owner rewrite is not idempotent against a re-charge): the op-ID on
// the touched entry resolves ambiguous PUTs exactly.
func (s *S3WAL) CASAbsenceCoverOwner(ctx context.Context, rawKey, expectedOwner string, expectedBytes int64, newOwner string) (bool, error) {
	assertf(strings.HasPrefix(rawKey, AbsenceCoverPrefix), "CASAbsenceCoverOwner(%q)", rawKey)
	assertf(newOwner != "", "CASAbsenceCoverOwner(%q): empty new owner", rawKey)
	mapKey, ok := strings.CutSuffix(rawKey, reservKeySuffix)
	if !ok {
		return false, nil // not a cover raw spelling: no such row (SQL: zero rows)
	}
	synth, ok := s3walCoverSynth(mapKey)
	if !ok {
		return false, nil
	}
	opID := s3wal.NewOpID()
	won := false
	err := s.mutate(ctx, s3walCoverBase(synth), s3walMutation{
		op: "cas absence cover owner",
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			won = false
			r := m.Reservations[mapKey]
			if r == nil || r.Owner != expectedOwner || r.Bytes != expectedBytes {
				return false, nil // vanished or drifted: nothing written
			}
			r.Owner = newOwner
			r.OpID = opID
			m.RecordOp(opID)
			won = true
			return true, nil
		},
		recover: func(fresh *s3wal.Manifest) s3walVerdict {
			if fresh.OpApplied(opID) {
				return s3walApplied
			}
			return s3walFailClosed
		},
	})
	if err != nil {
		return false, fmt.Errorf("cas absence cover owner %s: %w", rawKey, err)
	}
	return won, nil
}

// DeleteAbsenceCover — see the PG twin. Recovery class PRED/ID:
// re-applying on a fresh snapshot finds the row gone and no-ops.
func (s *S3WAL) DeleteAbsenceCover(ctx context.Context, key string) error {
	assertf(strings.HasPrefix(key, AbsenceCoverPrefix), "DeleteAbsenceCover(%q): not an absence cover key", key)
	mapKey := key + RepairCoverSuffix
	return s.mutate(ctx, s3walCoverBase(key), s3walMutation{
		op: "delete absence cover",
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			if _, ok := m.Reservations[mapKey]; !ok {
				return false, nil // deleting an absent row is a no-op
			}
			delete(m.Reservations, mapKey)
			return true, nil
		},
	})
}
