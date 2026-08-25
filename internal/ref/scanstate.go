package ref

// Fleet-persistent background-job scan state (issues #120/#121/#125): the
// one durable cursor/watermark primitive every budget-bounded scan job
// shares. River may schedule each periodic job on any replica, and worker
// turnover resets process memory, so rotation cursors, per-repo settlement
// cursors, and the sweeper's destructive-reap watermark live here instead.
//
// Semantics, deliberately minimal:
//
//   - One row per (job, scope). Payload is an opaque string the owning job
//     encodes/decodes; Generation is an optional invalidation fence the
//     owning job compares (a mismatch means "state predates a fence bump:
//     discard, fresh scan") — the store never interprets either.
//   - Put is an atomic last-writer-wins upsert stamping updated_at with the
//     DATABASE clock, so cross-replica freshness comparisons (the sweep
//     watermark) never mix application clocks.
//   - River's unique periodic jobs serialize each job fleet-wide (one
//     running pass per job kind), so writers of a given row never race;
//     last-writer-wins is exact, not merely convergent. Rows are
//     bookkeeping, never authority: losing one restarts a scan from the
//     top, which every consumer tolerates by construction.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Scan-state row coordinates. Scopes are per-job namespaces; helpers below
// build the parameterized ones.
const (
	// ScanJobLFSReclaim owns the LFS reclamation scan's rotation cursor
	// and per-repo row-settlement cursors (issue #121).
	ScanJobLFSReclaim = "lfs_reclaim"
	// ScanScopeReclaimRotation is the reclaimer's rotating repo cursor.
	ScanScopeReclaimRotation = "rotation"
	// ScanJobAbsence owns the absence reconciler's registry rotation
	// cursors and cover-renewal cursor (issue #125).
	ScanJobAbsence = "absence_reconcile"
	// ScanScopeAbsenceLFS/Packs are the two registry rotation cursors;
	// ScanScopeAbsenceRenewal is the cover-renewal listing cursor;
	// ScanScopeAbsenceProofs carries per-base fenced class proofs
	// awaiting their counterpart class (Codex round 21 on PR #132).
	ScanScopeAbsenceLFS     = "lfs"
	ScanScopeAbsencePacks   = "packs"
	ScanScopeAbsenceRenewal = "renewal"
	ScanScopeAbsenceProofs  = "proofs"
	// ScanScopeAbsenceRenewalHealth is a DB-clock watermark stamped
	// after every SUCCESSFUL bulk cover renewal; its age is the proof
	// that TTL'd absence covers cannot have lapsed un-renewed.
	// ScanScopeAbsenceAdmissionFence, while present, fails finite
	// quota-gated admissions closed (Codex round 26 on PR #132,
	// security): it is armed when a renewal failure finds the health
	// watermark absent or older than the cover-lapse safety bound, and
	// cleared only after a healthy pass completes a full recovery
	// rotation of both registry classes.
	ScanScopeAbsenceRenewalHealth  = "renewal-health"
	ScanScopeAbsenceAdmissionFence = "admission-fence"
	// ScanScopeAbsenceLapse is the durable lapse-evidence marker (Codex
	// round 33 on PR #132): ReservePackBytes' expired-row GC records it
	// in the same transaction whenever it deletes an expired UN-FOLDED
	// absence-cover row — physical evidence the reconciler's
	// first-health-stamp probe would otherwise never see, because the
	// very writers the admission fence should block prune it first.
	// Consumed (deleted) when the fence settles after a full recovery
	// rotation.
	ScanScopeAbsenceLapse = "lapse-evidence"
	// ScanJobSweep owns the sweeper's watermarks. The destructive-reap
	// scope's updated_at is the "last completed destructive LFS pass"
	// watermark (issue #120): written only when a sweep ran with the LFS
	// delete class armed (not demoted by the delete guard) and finished
	// its keyspace with zero recorded errors; consulted transactionally
	// by RetireUnreferencedLFSObject before any slot refund.
	ScanJobSweep                = "sweep"
	ScanScopeLFSDestructiveReap = "lfs-destructive-reap"
	// ScanScopeLFSDestructiveReapLegacy mirrors the completion watermark
	// but is written ONLY by legacy-armed sweeps (Codex round 33 on PR
	// #132): a later canonical-only completion overwrites the main
	// scope's payload, and legacy refund settlement must not lose the
	// proof that a legacy-armed sweep completed at some epoch.
	ScanScopeLFSDestructiveReapLegacy = "lfs-destructive-reap-legacy"
	// ScanScopeLFSReapEpoch holds the monotone destructive-sweep epoch
	// in its GENERATION column (Codex round 33 on PR #132): every
	// destructive LFS sweep increments it at pass START, committed
	// before the blob listing begins. Retirement records the epoch it
	// ran under; a completion watermark whose epoch is STRICTLY GREATER
	// proves a full clean sweep began after the retirement committed —
	// the only sound "the orphan was visible to that sweep" relation
	// (wall-clock comparison is not: PG now() is transaction-start
	// time, and a sweep can start between a retire's now() and its
	// commit).
	ScanScopeLFSReapEpoch = "lfs-reap-epoch"
)

// ErrReapWatermarkStale reports that a retirement verdict refused to
// refund an object-cap slot because the sweeper's destructive-reap
// watermark is absent or older than the caller's bound — destructive LFS
// sweeps are not demonstrably completing, so refunds must wait (issue
// #120). The caller reports the deferral distinctly and retries on a later
// pass; nothing was written.
var ErrReapWatermarkStale = errors.New("destructive LFS sweep watermark is stale: slot refund deferred")

// ReapWatermark is the destructive-reap completion watermark's JSON
// payload. Epoch is the pass's ScanScopeLFSReapEpoch value, stamped at
// sweep START — refund settlement requires a completion whose epoch
// strictly exceeds a retirement's recorded epoch (Codex round 33 on PR
// #132). Legacy certifies the sweep also had the LEGACY LFS delete class
// armed (CODESTORAGE_SWEEP_DELETE_LFS_LEGACY): legacy-spelled registry
// rows may only be retired, and their refunds settled, under legacy-armed
// certification (Codex round 17) — with the legacy class off, the sweeper
// deliberately spares legacy-spelled orphan blobs, so refunding their
// slots would grow storage past the cap.
type ReapWatermark struct {
	Epoch  int64 `json:"epoch"`
	Legacy bool  `json:"legacy,omitempty"`
}

// DecodeReapWatermark parses a completion watermark payload. Undecodable
// payloads (never written by this binary) fail toward zero values —
// canonical-only certification at epoch 0, which defers every settlement
// and legacy retirement: strictly the conservative direction.
func DecodeReapWatermark(payload string) ReapWatermark {
	var wm ReapWatermark
	if err := json.Unmarshal([]byte(payload), &wm); err != nil || wm.Epoch < 0 {
		return ReapWatermark{}
	}
	return wm
}

// ErrLegacyReapUnarmed reports that a retirement verdict refused to retire
// a LEGACY-SPELLED registry row because the freshest destructive-reap
// watermark does not certify legacy reaping (see ReapWatermark). The
// caller defers the row; it retires once a legacy-armed sweep completes.
var ErrLegacyReapUnarmed = errors.New("legacy LFS reaping is not armed: legacy row retirement deferred")

// ScanScopeReclaimRepo is the reclaimer's per-repo row-settlement cursor
// scope for one base repository.
func ScanScopeReclaimRepo(base string) string {
	assertf(base != "", "ScanScopeReclaimRepo: empty base")
	return "repo/" + base
}

// ScanScopeReclaimRefundRoot prefixes every pending-refund bucket scope —
// the reclaimer's rotation pages it to settle bases that vanished from
// LFSReposWithObjects (all rows retired).
const ScanScopeReclaimRefundRoot = "refund/"

// ScanScopeReclaimRefundRotation is the reclaimer's durable cursor over
// the refund bucket keyspace ('-' sorts below '/': the cursor row never
// collides with the ScanScopeReclaimRefundRoot listing range).
const ScanScopeReclaimRefundRotation = "refund-rotation"

// ScanScopeReclaimRefundPrefix prefixes one base's pending-refund bucket
// scopes (Codex round 33 on PR #132): retirement no longer refunds
// count/usage in its own transaction — a completion watermark only proves
// a sweep that ran BEFORE the deletion, which necessarily spared the
// blob. Refunds park in per-epoch buckets and settle once a clean sweep
// with a STRICTLY GREATER epoch completes.
func ScanScopeReclaimRefundPrefix(base string) string {
	assertf(base != "", "ScanScopeReclaimRefundPrefix: empty base")
	return ScanScopeReclaimRefundRoot + base + "/e"
}

// ScanScopeReclaimRefund is the pending-refund bucket scope for one base
// and retirement epoch. Zero-padded so bucket scopes list in epoch order.
func ScanScopeReclaimRefund(base string, epoch int64) string {
	assertf(epoch >= 0, "ScanScopeReclaimRefund: negative epoch %d", epoch)
	return fmt.Sprintf("%s%019d", ScanScopeReclaimRefundPrefix(base), epoch)
}

// LFSRefundBucket is one pending-refund bucket's JSON payload: the
// canonical- and legacy-spelled row counts and byte totals retired under
// the bucket's epoch (the row's generation column). Legacy parts settle
// only under a legacy-armed completion — canonical-only sweeps spare
// legacy orphans.
type LFSRefundBucket struct {
	Rows        int64 `json:"rows,omitempty"`
	Bytes       int64 `json:"bytes,omitempty"`
	LegacyRows  int64 `json:"legacy_rows,omitempty"`
	LegacyBytes int64 `json:"legacy_bytes,omitempty"`
}

// SettledLFSRefunds is one SettleLFSRefunds call's outcome.
type SettledLFSRefunds struct {
	Rows  int64 // object-cap slots refunded
	Bytes int64 // usage bytes refunded
	// Pending counts buckets left unsettled (no covering completion yet).
	Pending int
}

// JobScanState is one durable scan-state row.
type JobScanState struct {
	Payload    string
	Generation int64
	// UpdatedAt is the row's last write time on the DATABASE clock
	// (PG now(); SQLite unix-ms), the clock freshness bounds compare on.
	UpdatedAt time.Time
}

// ScanStateStore is the surface background jobs persist scan state
// through. Both concrete backends (PG, SQLite) satisfy it.
type ScanStateStore interface {
	// GetJobScanState reads one row; ok=false when absent.
	GetJobScanState(ctx context.Context, job, scope string) (JobScanState, bool, error)
	// JobScanStateAge reports how long ago (job, scope) was last
	// written, measured entirely on the DATABASE clock — freshness
	// verdicts must never compare a DB-stamped updated_at against an
	// application clock (Codex round 10 on PR #132: a replica skewed
	// ahead by more than the bound would permanently misjudge a
	// DB-fresh watermark as stale). ok=false when the row is absent.
	JobScanStateAge(ctx context.Context, job, scope string) (time.Duration, bool, error)
	// PutJobScanState atomically upserts one row, stamping updated_at
	// with the database clock.
	PutJobScanState(ctx context.Context, job, scope, payload string, generation int64) error
	// DeleteJobScanState removes one row; deleting an absent row is a
	// no-op.
	DeleteJobScanState(ctx context.Context, job, scope string) error
	// ParkRepoJobScanState upserts one PER-REPO row exactly like
	// PutJobScanState, but consults base's purge fence in the same
	// transaction and silently skips the write when fenced (Codex
	// round 26 on PR #132): the purge's ledger cleanup deletes the
	// repo's scan state, and an unfenced park racing the purge would
	// recreate a row nothing ever removes — the purged repo is never
	// listed again. On PG the check holds the base-ledger advisory
	// lock, totally ordering it against FenceRepoPurge: a park ordered
	// before the fence commits before it, so the later ledger cleanup
	// still sweeps the row; a park ordered after sees the fence and
	// no-ops. On SQLite the conditional upsert is one single-writer
	// statement.
	ParkRepoJobScanState(ctx context.Context, job, scope, payload string, generation int64, base string) error
	// ListJobScanStateScopes pages one job's scopes in byte order,
	// keyset from max(after, prefix), bounded to scopes carrying the
	// prefix (Codex round 33 on PR #132): the seam refund-bucket and
	// candidacy rotations page durable populations through.
	ListJobScanStateScopes(ctx context.Context, job, prefix, after string, limit int) ([]string, error)
	// BeginLFSReapEpoch atomically increments and returns the monotone
	// destructive-sweep epoch (see ScanScopeLFSReapEpoch). Called at
	// destructive LFS sweep START, before any blob listing; the write's
	// row lock serializes against in-flight retirements' epoch reads,
	// so every retirement recorded under an older epoch committed
	// before this sweep's listing began.
	BeginLFSReapEpoch(ctx context.Context) (int64, error)
	// RecordLFSReapComplete writes the destructive-reap completion
	// watermark (see ReapWatermark) — and, when legacy-armed, the
	// separate legacy completion scope — after a clean destructive LFS
	// pass.
	RecordLFSReapComplete(ctx context.Context, epoch int64, legacyArmed bool) error
}

func (s *PG) GetJobScanState(ctx context.Context, job, scope string) (JobScanState, bool, error) {
	assertf(job != "" && scope != "", "GetJobScanState(%q, %q)", job, scope)
	var st JobScanState
	err := s.pool.QueryRow(ctx,
		`SELECT payload, generation, updated_at FROM job_scan_state WHERE job = $1 AND scope = $2`,
		job, scope).Scan(&st.Payload, &st.Generation, &st.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return JobScanState{}, false, nil
	}
	if err != nil {
		return JobScanState{}, false, err
	}
	return st, true, nil
}

// JobScanStateAge — see ScanStateStore.
func (s *PG) JobScanStateAge(ctx context.Context, job, scope string) (time.Duration, bool, error) {
	assertf(job != "" && scope != "", "JobScanStateAge(%q, %q)", job, scope)
	var secs float64
	err := s.pool.QueryRow(ctx,
		`SELECT EXTRACT(EPOCH FROM (now() - updated_at))::float8 FROM job_scan_state WHERE job = $1 AND scope = $2`,
		job, scope).Scan(&secs)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("job scan state age %s/%s: %w", job, scope, err)
	}
	return time.Duration(secs * float64(time.Second)), true, nil
}

func (s *PG) PutJobScanState(ctx context.Context, job, scope, payload string, generation int64) error {
	assertf(job != "" && scope != "", "PutJobScanState(%q, %q)", job, scope)
	_, err := s.pool.Exec(ctx, `
INSERT INTO job_scan_state (job, scope, payload, generation, updated_at)
VALUES ($1, $2, $3, $4, now())
ON CONFLICT (job, scope) DO UPDATE SET payload = EXCLUDED.payload,
                                       generation = EXCLUDED.generation,
                                       updated_at = now()`,
		job, scope, payload, generation)
	return err
}

// ListJobScanStateScopes — see ScanStateStore.
func (s *PG) ListJobScanStateScopes(ctx context.Context, job, prefix, after string, limit int) ([]string, error) {
	assertf(job != "" && prefix != "", "ListJobScanStateScopes(%q, %q)", job, prefix)
	assertf(limit > 0, "ListJobScanStateScopes: non-positive limit %d", limit)
	if after < prefix {
		after = prefix
	}
	rows, err := s.pool.Query(ctx, `
SELECT scope FROM job_scan_state
WHERE job = $1 AND scope COLLATE "C" > $2 AND left(scope, length($3)) = $3
ORDER BY scope COLLATE "C" ASC LIMIT $4`,
		job, after, prefix, limit)
	if err != nil {
		return nil, fmt.Errorf("list job scan state scopes %s/%s: %w", job, prefix, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var scope string
		if err := rows.Scan(&scope); err != nil {
			return nil, err
		}
		out = append(out, scope)
	}
	return out, rows.Err()
}

// BeginLFSReapEpoch — see ScanStateStore.
func (s *PG) BeginLFSReapEpoch(ctx context.Context) (int64, error) {
	var epoch int64
	err := s.pool.QueryRow(ctx, `
INSERT INTO job_scan_state (job, scope, payload, generation, updated_at)
VALUES ($1, $2, '', 1, now())
ON CONFLICT (job, scope) DO UPDATE SET generation = job_scan_state.generation + 1,
                                       updated_at = now()
RETURNING generation`,
		ScanJobSweep, ScanScopeLFSReapEpoch).Scan(&epoch)
	if err != nil {
		return 0, fmt.Errorf("begin lfs reap epoch: %w", err)
	}
	assertf(epoch > 0, "begin lfs reap epoch: non-positive epoch %d", epoch)
	return epoch, nil
}

// RecordLFSReapComplete — see ScanStateStore.
func (s *PG) RecordLFSReapComplete(ctx context.Context, epoch int64, legacyArmed bool) error {
	assertf(epoch >= 0, "RecordLFSReapComplete: negative epoch %d", epoch)
	payload, err := json.Marshal(ReapWatermark{Epoch: epoch, Legacy: legacyArmed})
	assertf(err == nil, "RecordLFSReapComplete: marshal watermark: %v", err)
	if err := s.PutJobScanState(ctx, ScanJobSweep, ScanScopeLFSDestructiveReap, string(payload), epoch); err != nil {
		return err
	}
	if legacyArmed {
		// Written second: a crash in between leaves only the main
		// watermark — legacy settlement defers until the next
		// legacy-armed completion, strictly the conservative direction.
		return s.PutJobScanState(ctx, ScanJobSweep, ScanScopeLFSDestructiveReapLegacy, string(payload), epoch)
	}
	return nil
}

func (s *PG) DeleteJobScanState(ctx context.Context, job, scope string) error {
	assertf(job != "" && scope != "", "DeleteJobScanState(%q, %q)", job, scope)
	_, err := s.pool.Exec(ctx,
		`DELETE FROM job_scan_state WHERE job = $1 AND scope = $2`, job, scope)
	return err
}

func (s *PG) ParkRepoJobScanState(ctx context.Context, job, scope, payload string, generation int64, base string) error {
	assertf(job != "" && scope != "" && base != "", "ParkRepoJobScanState(%q, %q, %q)", job, scope, base)
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockBaseLedger(ctx, tx, base); err != nil {
			return err
		}
		fenced, err := repoPurgeFencedPG(ctx, tx, base)
		if err != nil {
			return err
		}
		if fenced {
			return nil // rows die with the purge: never recreate the cursor
		}
		_, err = tx.Exec(ctx, `
INSERT INTO job_scan_state (job, scope, payload, generation, updated_at)
VALUES ($1, $2, $3, $4, now())
ON CONFLICT (job, scope) DO UPDATE SET payload = EXCLUDED.payload,
                                       generation = EXCLUDED.generation,
                                       updated_at = now()`,
			job, scope, payload, generation)
		return err
	})
}
