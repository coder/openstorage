// S3WAL fleet-persistent scan state (PR #132, plan §2.10): one CAS-advanced
// side object per (job, scope) under _wal/scan/<job>/<hex(scope)>.
//
// Mapping decisions, concrete and documented:
//
//   - Keys are extension-free (the §2.1 ordering argument: a ".json" suffix
//     corrupts listing order between prefix-related scopes) and the scope is
//     LOWERCASE-HEX encoded — the plan's "path-escaped" sketch is replaced
//     deliberately: %XX escaping reorders '%' (0x25) against literals, while
//     lowercase hex is a strictly monotone, fixed-width (2:1), reversible
//     injection, so bucket listing order over encoded keys IS byte order
//     over the unencoded scopes — exactly the ORDER BY scope COLLATE "C"
//     contract ListJobScanStateScopes promises on the SQL backends.
//   - The clock story: the SQL backends stamp updated_at with the DATABASE
//     clock so freshness verdicts never mix application clocks. s3wal side
//     objects have no manifest logical clock, but under the single-process
//     decree every scan-state reader and writer shares ONE process clock,
//     so the "database clock at both ends" contract collapses to the local
//     clock honestly. Stamps are ClkNext-monotone per object (a backward
//     local-clock jump can never regress updated_at); ages inside a small
//     future tolerance clamp to zero, and a stamp further ahead of the
//     local clock is UNVERIFIABLE — it reads as stale (fail closed), never
//     fresh (see scanRowAge).
//   - Single-writer discipline (§2.10): River's unique periodic jobs — and
//     here, the single-process decree — make each (job, scope) row
//     single-writer. In-process writers of one key serialize on a per-key
//     mutex, so a conditional-write conflict that still reaches the store
//     is evidence of a SECOND serving process: it fails loudly and is never
//     retried. The §2.11 instance lease (Phase 3) upgrades the loud error
//     to a process fail-stop. ONE deliberate exception: fleet-global
//     markers upserted by concurrent receive-hook processes (the
//     lapse-evidence marker) are multi-writer by design and retry
//     conflicts instead — see scanMutateShared.
//   - There is no cross-delete seq watermark for side objects: unlike
//     manifests, scan rows are legitimately deleted and re-created (bucket
//     drains, watermark resets), which resets Seq to 1. The per-key lock
//     plus the CAS conflict policy are the staleness defense instead; the
//     shared casobj DecodeSeq probe deliberately does not match this codec
//     (distinct field names), so the manifest watermark stays unarmed here.
package ref

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/coder/code-storage/internal/ref/s3wal"
)

var _ ScanStateStore = (*S3WAL)(nil)

// s3walScanPrefix is the side-object namespace scan rows live under —
// disjoint from s3wal.RepoPrefix, so fleet manifest walks never see them.
const s3walScanPrefix = "_wal/scan/"

// s3walScanVersion versions the side-object codec.
const s3walScanVersion = 1

// s3walScanRow is one durable scan-state side object. Field names are
// deliberately distinct from the manifest codec ("scan_v"/"scan_seq", not
// "v"/"seq") so s3wal.DecodeSeq rejects these bodies and the manifest
// stale-read watermark never arms on a key whose Seq legitimately resets
// across delete+recreate.
type s3walScanRow struct {
	ScanV int `json:"scan_v"`
	// Seq is strictly monotone per object LIFE (resets on recreate) —
	// distinct bytes per logical version, killing ETag-ABA like the
	// manifest's Seq.
	Seq int64 `json:"scan_seq"`
	// Writer is a fresh 128-bit nonce per write: the ambiguous-PUT
	// recovery identity (§2.3), which also makes non-idempotent
	// mutations (BeginLFSReapEpoch's increment) exactly-once.
	Writer     string    `json:"writer"`
	Payload    string    `json:"payload"`
	Generation int64     `json:"generation"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// s3walScanDir is one job's listing namespace. Jobs are code constants
// with a restricted alphabet (asserted — a '/' would corrupt the per-job
// listing namespace).
func s3walScanDir(job string) string {
	assertf(job != "", "s3walScanDir: empty job")
	for i := 0; i < len(job); i++ {
		b := job[i]
		ok := b == '-' || b == '_' ||
			(b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
		assertf(ok, "s3walScanDir: job %q carries byte %q outside [A-Za-z0-9_-]", job, b)
	}
	return s3walScanPrefix + job + "/"
}

// s3walScanKey builds the side-object key; scopes are arbitrary and
// hex-encoded (see the header).
func s3walScanKey(job, scope string) string {
	assertf(scope != "", "s3walScanKey(%q, %q)", job, scope)
	return s3walScanDir(job) + hex.EncodeToString([]byte(scope))
}

// lockScan serializes this process's writers of one scan-state key (see
// the header). It shares lockBase's mutex map under a NUL-prefixed name:
// 0x00 <= '+' is rejected by the s3wal base alphabet, so no base group can
// collide with a scan lock.
func (s *S3WAL) lockScan(key string) func() {
	return s.lockBase("\x00" + key)
}

// scanRead fetches and decodes one side object; ok=false when absent. A
// body that exists but does not decode as our codec is a foreign object in
// a namespace only this driver may write — loud error, never skipped.
func (s *S3WAL) scanRead(ctx context.Context, key string) (s3walScanRow, string, bool, error) {
	obj, err := s.cas.Get(ctx, key)
	if errors.Is(err, s3wal.ErrNotExist) {
		return s3walScanRow{}, "", false, nil
	}
	if err != nil {
		return s3walScanRow{}, "", false, fmt.Errorf("s3wal scan read %s: %w", key, err)
	}
	var row s3walScanRow
	if err := json.Unmarshal(obj.Body, &row); err != nil {
		return s3walScanRow{}, "", false, fmt.Errorf("s3wal scan read %s: undecodable side object: %w", key, err)
	}
	if row.ScanV != s3walScanVersion || row.Seq < 1 || row.Writer == "" || row.UpdatedAt.IsZero() {
		return s3walScanRow{}, "", false, fmt.Errorf("s3wal scan read %s: foreign or corrupt side object (v=%d seq=%d)", key, row.ScanV, row.Seq)
	}
	return row, obj.ETag, true, nil
}

// scanGet reads one (job, scope) row; ok=false when absent.
func (s *S3WAL) scanGet(ctx context.Context, job, scope string) (s3walScanRow, bool, error) {
	row, _, ok, err := s.scanRead(ctx, s3walScanKey(job, scope))
	return row, ok, err
}

// s3walScanFutureTolerance bounds how far AHEAD of the local clock a
// scan-state stamp may sit and still clamp to age-0 fresh: ClkNext chains
// stamps milliseconds ahead of a busy writer's clock, and sub-second
// offsets are noise against the freshness windows the gates use. A stamp
// further ahead is evidence the local clock is behind the clock that
// wrote the row — a restart after a backward step, or an at-boot-slow
// clock.
const s3walScanFutureTolerance = 2 * time.Second

// s3walScanAgeUnverifiable poisons a freshness verdict: every ">= bound"
// staleness gate reads it as stale.
const s3walScanAgeUnverifiable = time.Duration(math.MaxInt64)

// scanRowAge is the row's age on the local process clock (see the header's
// clock story). A stamp inside the future tolerance clamps to zero; one
// beyond it is UNVERIFIABLE and reads as stale, never fresh (Codex round
// 8 on PR #156, P1): the destructive-reap watermark gates registry
// retirement, and clamping a genuinely future stamp to age-0 would accept
// an actually-stale watermark as fresh, permitting retirement and
// quota/count refunds without a recent successful physical reap.
// Fail-closed direction only: over-retention until clocks agree.
func (s *S3WAL) scanRowAge(row s3walScanRow) time.Duration {
	age := s.now().Sub(row.UpdatedAt)
	if age < -s3walScanFutureTolerance {
		slog.Warn("s3wal scan-state stamp is ahead of the local clock: treating the row as unverifiable-stale",
			"updated_at", row.UpdatedAt, "local_now", s.now(), "ahead_by", -age)
		return s3walScanAgeUnverifiable
	}
	if age < 0 {
		return 0
	}
	return age
}

// scanMutate runs one read-mutate-CAS loop on a side object under the
// per-key lock. fn sees the current row (nil = absent) and the write
// stamp; returning nil writes nothing. Conflict policy per §2.10: a
// definitive 412 (lost race / duplicate create) is dual-serve evidence —
// loud error, no retry; an ambiguous PUT re-reads and resolves via the
// writer nonce, retrying only when the object provably did not change.
func (s *S3WAL) scanMutate(ctx context.Context, op, job, scope string, fn func(prev *s3walScanRow, stamp time.Time) (*s3walScanRow, error)) error {
	return s.scanMutateWith(ctx, op, job, scope, false, fn)
}

// scanMutateShared is scanMutate for the ONE multi-writer side-object
// family: fleet-global markers legitimately upserted by CONCURRENT
// receive-hook processes, each a fresh S3WAL instance the per-key mutex
// cannot serialize (Codex round 8 on PR #156: a raced lapse-marker upsert
// read a definitive 412 as dual-serve evidence and rejected an otherwise
// valid push). For these idempotent upserts a conditional conflict — or a
// foreign write observed during ambiguity recovery — is contention:
// re-read and re-apply under backoff, bounded by casAttempts.
func (s *S3WAL) scanMutateShared(ctx context.Context, op, job, scope string, fn func(prev *s3walScanRow, stamp time.Time) (*s3walScanRow, error)) error {
	return s.scanMutateWith(ctx, op, job, scope, true, fn)
}

func (s *S3WAL) scanMutateWith(ctx context.Context, op, job, scope string, shared bool, fn func(prev *s3walScanRow, stamp time.Time) (*s3walScanRow, error)) error {
	assertf(op != "" && fn != nil, "scanMutate: incomplete mutation %q", op)
	key := s3walScanKey(job, scope)
	unlock := s.lockScan(key)
	defer unlock()
	for attempt := 0; attempt < s.casAttempts; attempt++ {
		prev, etag, ok, err := s.scanRead(ctx, key)
		if err != nil {
			return err
		}
		var prevRow *s3walScanRow
		var prevStamp time.Time
		var prevSeq int64
		if ok {
			snapshot := prev
			prevRow, prevStamp, prevSeq = &snapshot, prev.UpdatedAt, prev.Seq
		}
		stamp := s3wal.ClkNext(s.now(), prevStamp)
		next, err := fn(prevRow, stamp)
		if err != nil {
			return err
		}
		if next == nil {
			return nil
		}
		next.ScanV = s3walScanVersion
		next.Seq = prevSeq + 1
		next.Writer = s3wal.NewOpID()
		next.UpdatedAt = stamp
		body, merr := json.Marshal(next)
		assertf(merr == nil, "s3wal %s %s: encode scan row: %v", op, key, merr)
		var werr error
		if !ok {
			_, werr = s.cas.Create(ctx, key, body)
		} else {
			_, werr = s.cas.Advance(ctx, key, body, etag)
		}
		if werr == nil {
			return nil
		}
		switch {
		case errors.Is(werr, s3wal.ErrExists), errors.Is(werr, s3wal.ErrLostRace):
			if shared {
				// Multi-writer marker: a lost race is contention, not
				// evidence — re-read and re-apply the idempotent upsert.
				if err := s.sleepJitter(ctx, attempt); err != nil {
					return fmt.Errorf("s3wal %s %s: %w", op, key, err)
				}
				continue
			}
			// A definitive conditional rejection: some OTHER writer
			// advanced this single-writer object — dual-serve evidence
			// (§2.10). Loud, permanent, never retried.
			return fmt.Errorf("s3wal %s %s: scan-state CAS conflict is dual-serve evidence under the single-writer decree: %w", op, key, werr)
		case errors.Is(werr, s3wal.ErrNotExecuted):
			continue // provably never sent — plain retry
		}
		if _, ok := errors.AsType[*s3wal.AmbiguousError](werr); !ok {
			return fmt.Errorf("s3wal %s %s: %w", op, key, werr)
		}
		// Ambiguous: the write MAY have landed. Re-read and resolve via
		// the writer nonce.
		fresh, _, fok, rerr := s.scanRead(ctx, key)
		if rerr != nil {
			return fmt.Errorf("s3wal %s %s: %w (recovery read failed: %v)", op, key, werr, rerr)
		}
		if fok && fresh.Writer == next.Writer {
			assertf(fresh.Seq == next.Seq, "s3wal %s %s: our nonce at foreign seq %d (wrote %d)", op, key, fresh.Seq, next.Seq)
			return nil // our write landed and is the latest
		}
		unchanged := (!ok && !fok) ||
			(ok && fok && fresh.Seq == prev.Seq && fresh.Writer == prev.Writer)
		if unchanged {
			continue // provably not landed: retry the same mutation
		}
		if shared {
			// A concurrent upserter advanced the marker while our PUT was
			// ambiguous: re-apply on the fresh state (idempotent).
			if err := s.sleepJitter(ctx, attempt); err != nil {
				return fmt.Errorf("s3wal %s %s: %w", op, key, err)
			}
			continue
		}
		return fmt.Errorf("s3wal %s %s: foreign write during ambiguity recovery is dual-serve evidence under the single-writer decree: %w", op, key, werr)
	}
	return fmt.Errorf("s3wal %s %s: %d attempts: %w", op, key, s.casAttempts, s3wal.ErrCASContention)
}

// scanPut upserts one row — the PutJobScanState primitive shared by the
// watermark, marker, and bucket writers.
func (s *S3WAL) scanPut(ctx context.Context, job, scope, payload string, generation int64) error {
	return s.scanMutate(ctx, "put scan state", job, scope, func(_ *s3walScanRow, _ time.Time) (*s3walScanRow, error) {
		return &s3walScanRow{Payload: payload, Generation: generation}, nil
	})
}

// scanDelete removes one row unconditionally (§2.7: deletes are never
// conditional; the single-writer decree owns the key). Idempotent.
func (s *S3WAL) scanDelete(ctx context.Context, job, scope string) error {
	key := s3walScanKey(job, scope)
	unlock := s.lockScan(key)
	defer unlock()
	if err := s.cas.Delete(ctx, key); err != nil {
		return fmt.Errorf("s3wal delete scan state %s: %w", key, err)
	}
	return nil
}

// GetJobScanState — see ScanStateStore.
func (s *S3WAL) GetJobScanState(ctx context.Context, job, scope string) (JobScanState, bool, error) {
	assertf(job != "" && scope != "", "GetJobScanState(%q, %q)", job, scope)
	row, ok, err := s.scanGet(ctx, job, scope)
	if err != nil || !ok {
		return JobScanState{}, false, err
	}
	return JobScanState{Payload: row.Payload, Generation: row.Generation, UpdatedAt: row.UpdatedAt}, true, nil
}

// JobScanStateAge — see ScanStateStore and the header's clock story.
func (s *S3WAL) JobScanStateAge(ctx context.Context, job, scope string) (time.Duration, bool, error) {
	assertf(job != "" && scope != "", "JobScanStateAge(%q, %q)", job, scope)
	row, ok, err := s.scanGet(ctx, job, scope)
	if err != nil || !ok {
		return 0, false, err
	}
	return s.scanRowAge(row), true, nil
}

// PutJobScanState — see ScanStateStore.
func (s *S3WAL) PutJobScanState(ctx context.Context, job, scope, payload string, generation int64) error {
	assertf(job != "" && scope != "", "PutJobScanState(%q, %q)", job, scope)
	return s.scanPut(ctx, job, scope, payload, generation)
}

// DeleteJobScanState — see ScanStateStore.
func (s *S3WAL) DeleteJobScanState(ctx context.Context, job, scope string) error {
	assertf(job != "" && scope != "", "DeleteJobScanState(%q, %q)", job, scope)
	return s.scanDelete(ctx, job, scope)
}

// ParkRepoJobScanState — see ScanStateStore. The purge fence and the park
// cannot share one atom across a manifest and a side object; the honest
// s3wal substitute is the per-base in-process lock BOTH this check and
// FenceRepoPurge's manifest CAS run under (the same serialization PG buys
// with the base-ledger advisory lock, sound under the single-process
// decree): a park ordered before the fence lands before the purge's
// ledger cleanup sweeps the row; a park ordered after sees the fence and
// skips.
func (s *S3WAL) ParkRepoJobScanState(ctx context.Context, job, scope, payload string, generation int64, base string) error {
	assertf(job != "" && scope != "" && base != "", "ParkRepoJobScanState(%q, %q, %q)", job, scope, base)
	assertf(baseRepoOf(base) == base, "ParkRepoJobScanState: %q is not a base repo", base)
	unlock := s.lockBase(base)
	defer unlock()
	m, _, err := s.readManifest(ctx, base)
	if err != nil {
		return err
	}
	if m.PurgedAt != nil {
		return nil // rows die with the purge: never recreate the cursor
	}
	return s.scanPut(ctx, job, scope, payload, generation)
}

// ListJobScanStateScopes — see ScanStateStore. Listing order over the
// hex-encoded keys IS byte order over the unencoded scopes (the header's
// monotone-injection argument), and both range ends are index bounds
// exactly like the SQL twins: keyset from max(after, prefix), exclusive
// upper bound coverRangeEnd(prefix).
func (s *S3WAL) ListJobScanStateScopes(ctx context.Context, job, prefix, after string, limit int) ([]string, error) {
	assertf(job != "" && prefix != "", "ListJobScanStateScopes(%q, %q)", job, prefix)
	assertf(limit > 0, "ListJobScanStateScopes: non-positive limit %d", limit)
	if after < prefix {
		after = prefix
	}
	dir := s3walScanDir(job)
	startAfter := dir + hex.EncodeToString([]byte(after))
	end := dir + hex.EncodeToString([]byte(coverRangeEnd(prefix)))
	var out []string
	for len(out) < limit {
		ask := limit - len(out)
		keys, err := s.cas.List(ctx, dir, startAfter, ask)
		if err != nil {
			return nil, fmt.Errorf("list job scan state scopes %s/%s: %w", job, prefix, err)
		}
		for _, key := range keys {
			if key >= end {
				return out, nil
			}
			scope, derr := hex.DecodeString(key[len(dir):])
			if derr != nil {
				// Foreign object under the scan prefix: crash-and-burn
				// (nothing else may live here).
				return nil, fmt.Errorf("list job scan state scopes %s/%s: foreign key %q under %s: %w", job, prefix, key, dir, derr)
			}
			out = append(out, string(scope))
		}
		if len(keys) < ask { // short page: keyspace exhausted
			break
		}
		startAfter = keys[len(keys)-1]
	}
	return out, nil
}

// BeginLFSReapEpoch — see ScanStateStore. The write side of reapEpochMu
// mirrors PG's row-lock serialization: retirements hold the read side
// from their epoch read through the retire CAS and refund park, so every
// retirement recorded under an older epoch committed before this sweep's
// listing began.
func (s *S3WAL) BeginLFSReapEpoch(ctx context.Context) (int64, error) {
	s.reapEpochMu.Lock()
	defer s.reapEpochMu.Unlock()
	var epoch int64
	err := s.scanMutate(ctx, "begin lfs reap epoch", ScanJobSweep, ScanScopeLFSReapEpoch,
		func(prev *s3walScanRow, _ time.Time) (*s3walScanRow, error) {
			if prev == nil {
				epoch = 1
				return &s3walScanRow{Payload: "", Generation: 1}, nil
			}
			epoch = prev.Generation + 1
			// The SQL upsert bumps generation and leaves payload as-is.
			return &s3walScanRow{Payload: prev.Payload, Generation: epoch}, nil
		})
	if err != nil {
		return 0, fmt.Errorf("begin lfs reap epoch: %w", err)
	}
	assertf(epoch > 0, "begin lfs reap epoch: non-positive epoch %d", epoch)
	return epoch, nil
}

// RecordLFSReapComplete — see the SQL twins.
func (s *S3WAL) RecordLFSReapComplete(ctx context.Context, epoch int64, legacyArmed bool) error {
	assertf(epoch >= 0, "RecordLFSReapComplete: negative epoch %d", epoch)
	payload, err := json.Marshal(ReapWatermark{Epoch: epoch, Legacy: legacyArmed})
	assertf(err == nil, "RecordLFSReapComplete: marshal watermark: %v", err)
	if err := s.scanPut(ctx, ScanJobSweep, ScanScopeLFSDestructiveReap, string(payload), epoch); err != nil {
		return err
	}
	if legacyArmed {
		// Written second: a crash in between leaves only the main
		// watermark — legacy settlement defers, strictly conservative.
		return s.scanPut(ctx, ScanJobSweep, ScanScopeLFSDestructiveReapLegacy, string(payload), epoch)
	}
	return nil
}

// ArmAbsenceAdmissionFence — see the PG twin for the ordering contract.
// The write side of absenceFenceMu mirrors PG's exclusive advisory lock:
// finite admissions hold the read side across their fence check and
// admission CAS (sound in-process under the single-process decree), so
// arming waits out in-flight admissions and every admission starting
// afterwards observes the marker and fails closed.
func (s *S3WAL) ArmAbsenceAdmissionFence(ctx context.Context, payload string) error {
	assertf(payload != "", "ArmAbsenceAdmissionFence: empty payload")
	s.absenceFenceMu.Lock()
	defer s.absenceFenceMu.Unlock()
	return s.scanPut(ctx, ScanJobAbsence, ScanScopeAbsenceAdmissionFence, payload, 0)
}
