// purge_sqlite.go mirrors purge.go's repository-purge row seams for the
// SQLite backend (issue #52) — statement-for-statement twins; see purge.go
// for the phase rationale and invariants.
package ref

import (
	"context"
	"fmt"
)

// purgeGroupArgs converts the group to the []any placeholder args
// database/sql wants.
func purgeGroupArgs(group []string) []any {
	args := make([]any, len(group))
	for i, r := range group {
		args[i] = r
	}
	return args
}

// DropRepoRefs — see the PG twin.
func (s *SQLite) DropRepoRefs(ctx context.Context, group []string) (int64, error) {
	validatePurgeGroup(group)
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM refs WHERE repo IN (`+placeholders(len(group))+`)`,
		purgeGroupArgs(group)...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// RetireRepoPacks — see the PG twin.
func (s *SQLite) RetireRepoPacks(ctx context.Context, group []string) (int64, error) {
	validatePurgeGroup(group)
	tx, err := s.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE via _txlock
	if err != nil {
		return 0, fmt.Errorf("retire repo packs begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	args := purgeGroupArgs(group)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO pack_retained (repo, key, bytes)
SELECT repo, s3_key, 0 FROM packs WHERE repo IN (`+placeholders(len(group))+`)
ON CONFLICT (repo, key) DO NOTHING`, args...); err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx,
		`DELETE FROM packs WHERE repo IN (`+placeholders(len(group))+`)`, args...)
	if err != nil {
		return 0, err
	}
	retired, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("retire repo packs commit: %w", err)
	}
	return retired, nil
}

// DropRepoLFSObjects — see the PG twin.
func (s *SQLite) DropRepoLFSObjects(ctx context.Context, group []string) (int64, error) {
	validatePurgeGroup(group)
	tx, err := s.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE via _txlock
	if err != nil {
		return 0, fmt.Errorf("drop repo lfs begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	args := purgeGroupArgs(group)
	res, err := tx.ExecContext(ctx,
		`DELETE FROM lfs_objects WHERE repo IN (`+placeholders(len(group))+`)`, args...)
	if err != nil {
		return 0, err
	}
	dropped, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM lfs_object_counts WHERE repo IN (`+placeholders(len(group))+`)`, args...); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("drop repo lfs commit: %w", err)
	}
	return dropped, nil
}

// CountRepoRows — see the PG twin.
func (s *SQLite) CountRepoRows(ctx context.Context, group []string) (int64, error) {
	validatePurgeGroup(group)
	ph := placeholders(len(group))
	args := purgeGroupArgs(group)
	args = append(args, purgeGroupArgs(group)...)
	args = append(args, purgeGroupArgs(group)...)
	var n int64
	err := s.db.QueryRowContext(ctx, `
SELECT (SELECT count(*) FROM refs        WHERE repo IN (`+ph+`))
     + (SELECT count(*) FROM packs       WHERE repo IN (`+ph+`))
     + (SELECT count(*) FROM lfs_objects WHERE repo IN (`+ph+`))`, args...).Scan(&n)
	return n, err
}

// PurgeRepoLedger — see the PG twin.
func (s *SQLite) PurgeRepoLedger(ctx context.Context, group []string) (int64, error) {
	validatePurgeGroup(group)
	tx, err := s.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE via _txlock
	if err != nil {
		return 0, fmt.Errorf("purge repo ledger begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	ph := placeholders(len(group))
	args := purgeGroupArgs(group)
	var total int64
	for _, table := range []string{
		"pack_reservations", "pack_retained", "pack_usage",
		"lfs_object_counts", "lfs_adoption_gens", "repo_generations",
	} {
		res, err := tx.ExecContext(ctx,
			`DELETE FROM `+table+` WHERE repo IN (`+ph+`)`, args...)
		if err != nil {
			return 0, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		total += n
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("purge repo ledger commit: %w", err)
	}
	return total, nil
}
