package ref

// SQLite scan-state rows (see scanstate.go). Local mode is single-node, but
// the code path is shared: cursors survive process restarts through the
// same table, which is also what the dogfooding transcript exercises.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

func (s *SQLite) GetJobScanState(ctx context.Context, job, scope string) (JobScanState, bool, error) {
	assertf(job != "" && scope != "", "GetJobScanState(%q, %q)", job, scope)
	var st JobScanState
	var updatedMS int64
	err := s.db.QueryRowContext(ctx,
		`SELECT payload, generation, updated_at FROM job_scan_state WHERE job = ? AND scope = ?`,
		job, scope).Scan(&st.Payload, &st.Generation, &updatedMS)
	if errors.Is(err, sql.ErrNoRows) {
		return JobScanState{}, false, nil
	}
	if err != nil {
		return JobScanState{}, false, err
	}
	st.UpdatedAt = time.UnixMilli(updatedMS)
	return st, true, nil
}

// JobScanStateAge — see ScanStateStore. updated_at is epoch milliseconds
// at SECOND precision (strftime %s × 1000), so the age can over-read by up
// to a second; every freshness bound comparing it has a ≥1 minute floor
// (jobs.reapWatermarkMinFreshness).
func (s *SQLite) JobScanStateAge(ctx context.Context, job, scope string) (time.Duration, bool, error) {
	assertf(job != "" && scope != "", "JobScanStateAge(%q, %q)", job, scope)
	var ms int64
	err := s.db.QueryRowContext(ctx,
		`SELECT `+sqliteEpochMS+` - updated_at FROM job_scan_state WHERE job = ? AND scope = ?`,
		job, scope).Scan(&ms)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("job scan state age %s/%s: %w", job, scope, err)
	}
	return time.Duration(ms) * time.Millisecond, true, nil
}

func (s *SQLite) PutJobScanState(ctx context.Context, job, scope, payload string, generation int64) error {
	assertf(job != "" && scope != "", "PutJobScanState(%q, %q)", job, scope)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO job_scan_state (job, scope, payload, generation, updated_at)
VALUES (?, ?, ?, ?, strftime('%s','now') * 1000)
ON CONFLICT (job, scope) DO UPDATE SET payload = excluded.payload,
                                       generation = excluded.generation,
                                       updated_at = excluded.updated_at`,
		job, scope, payload, generation)
	return err
}

// ArmAbsenceAdmissionFence — see the PG twin for the ordering contract.
// SQLite needs no explicit lock: the upsert is one IMMEDIATE-mode write,
// and ReservePackBytes' whole check-and-reserve runs in a single-writer
// BEGIN IMMEDIATE transaction, so the pair cannot interleave.
func (s *SQLite) ArmAbsenceAdmissionFence(ctx context.Context, payload string) error {
	assertf(payload != "", "ArmAbsenceAdmissionFence: empty payload")
	return s.PutJobScanState(ctx, ScanJobAbsence, ScanScopeAbsenceAdmissionFence, payload, 0)
}

// ListJobScanStateScopes — see ScanStateStore. SQLite TEXT compares by
// byte order natively.
func (s *SQLite) ListJobScanStateScopes(ctx context.Context, job, prefix, after string, limit int) ([]string, error) {
	assertf(job != "" && prefix != "", "ListJobScanStateScopes(%q, %q)", job, prefix)
	assertf(limit > 0, "ListJobScanStateScopes: non-positive limit %d", limit)
	if after < prefix {
		after = prefix
	}
	// Exclusive-upper index bound, not a filter (Codex round 38); see
	// the PG twin.
	rows, err := s.db.QueryContext(ctx, `
SELECT scope FROM job_scan_state
WHERE job = ? AND scope > ? AND scope < ?
ORDER BY scope ASC LIMIT ?`,
		job, after, coverRangeEnd(prefix), limit)
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

// BeginLFSReapEpoch — see ScanStateStore. Single-writer statements make
// the increment atomic; retirements' epoch reads serialize behind it.
func (s *SQLite) BeginLFSReapEpoch(ctx context.Context) (int64, error) {
	var epoch int64
	err := s.db.QueryRowContext(ctx, `
INSERT INTO job_scan_state (job, scope, payload, generation, updated_at)
VALUES (?, ?, '', 1, `+sqliteEpochMS+`)
ON CONFLICT (job, scope) DO UPDATE SET generation = job_scan_state.generation + 1,
                                       updated_at = `+sqliteEpochMS+`
RETURNING generation`,
		ScanJobSweep, ScanScopeLFSReapEpoch).Scan(&epoch)
	if err != nil {
		return 0, fmt.Errorf("begin lfs reap epoch: %w", err)
	}
	assertf(epoch > 0, "begin lfs reap epoch: non-positive epoch %d", epoch)
	return epoch, nil
}

// RecordLFSReapComplete — see the PG twin.
func (s *SQLite) RecordLFSReapComplete(ctx context.Context, epoch int64, legacyArmed bool) error {
	assertf(epoch >= 0, "RecordLFSReapComplete: negative epoch %d", epoch)
	payload, err := json.Marshal(ReapWatermark{Epoch: epoch, Legacy: legacyArmed})
	assertf(err == nil, "RecordLFSReapComplete: marshal watermark: %v", err)
	if err := s.PutJobScanState(ctx, ScanJobSweep, ScanScopeLFSDestructiveReap, string(payload), epoch); err != nil {
		return err
	}
	if legacyArmed {
		return s.PutJobScanState(ctx, ScanJobSweep, ScanScopeLFSDestructiveReapLegacy, string(payload), epoch)
	}
	return nil
}

func (s *SQLite) DeleteJobScanState(ctx context.Context, job, scope string) error {
	assertf(job != "" && scope != "", "DeleteJobScanState(%q, %q)", job, scope)
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM job_scan_state WHERE job = ? AND scope = ?`, job, scope)
	return err
}

func (s *SQLite) ParkRepoJobScanState(ctx context.Context, job, scope, payload string, generation int64, base string) error {
	assertf(job != "" && scope != "" && base != "", "ParkRepoJobScanState(%q, %q, %q)", job, scope, base)
	// One single-writer statement: the fence check and the upsert are
	// atomic under SQLite's database lock (see the interface contract).
	_, err := s.db.ExecContext(ctx, `
INSERT INTO job_scan_state (job, scope, payload, generation, updated_at)
SELECT ?, ?, ?, ?, strftime('%s','now') * 1000
WHERE NOT EXISTS (SELECT 1 FROM purged_repos WHERE repo = ?)
ON CONFLICT (job, scope) DO UPDATE SET payload = excluded.payload,
                                       generation = excluded.generation,
                                       updated_at = excluded.updated_at`,
		job, scope, payload, generation, base)
	return err
}
