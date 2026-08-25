package ref

// SQLite scan-state rows (see scanstate.go). Local mode is single-node, but
// the code path is shared: cursors survive process restarts through the
// same table, which is also what the dogfooding transcript exercises.

import (
	"context"
	"database/sql"
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
