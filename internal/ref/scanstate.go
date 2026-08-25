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
	"errors"
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
	// ScanScopeAbsenceRenewal is the cover-renewal listing cursor.
	ScanScopeAbsenceLFS     = "lfs"
	ScanScopeAbsencePacks   = "packs"
	ScanScopeAbsenceRenewal = "renewal"
	// ScanJobSweep owns the sweeper's watermarks. The destructive-reap
	// scope's updated_at is the "last completed destructive LFS pass"
	// watermark (issue #120): written only when a sweep ran with the LFS
	// delete class armed (not demoted by the delete guard) and finished
	// its keyspace with zero recorded errors; consulted transactionally
	// by RetireUnreferencedLFSObject before any slot refund.
	ScanJobSweep                = "sweep"
	ScanScopeLFSDestructiveReap = "lfs-destructive-reap"
)

// ErrReapWatermarkStale reports that a retirement verdict refused to
// refund an object-cap slot because the sweeper's destructive-reap
// watermark is absent or older than the caller's bound — destructive LFS
// sweeps are not demonstrably completing, so refunds must wait (issue
// #120). The caller reports the deferral distinctly and retries on a later
// pass; nothing was written.
var ErrReapWatermarkStale = errors.New("destructive LFS sweep watermark is stale: slot refund deferred")

// ScanScopeReclaimRepo is the reclaimer's per-repo row-settlement cursor
// scope for one base repository.
func ScanScopeReclaimRepo(base string) string {
	assertf(base != "", "ScanScopeReclaimRepo: empty base")
	return "repo/" + base
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
	// PutJobScanState atomically upserts one row, stamping updated_at
	// with the database clock.
	PutJobScanState(ctx context.Context, job, scope, payload string, generation int64) error
	// DeleteJobScanState removes one row; deleting an absent row is a
	// no-op.
	DeleteJobScanState(ctx context.Context, job, scope string) error
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

func (s *PG) DeleteJobScanState(ctx context.Context, job, scope string) error {
	assertf(job != "" && scope != "", "DeleteJobScanState(%q, %q)", job, scope)
	_, err := s.pool.Exec(ctx,
		`DELETE FROM job_scan_state WHERE job = $1 AND scope = $2`, job, scope)
	return err
}
