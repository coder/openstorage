// Local schedule state for SQLite (issue #266, ADR-50).
//
// Invariant: the lane row owns the schedule. For each repository lane
// (push or pull), whether work is outstanding (pending level), when it may
// next start (push_due / sync_due), its retry position (push_attempt), and
// who is running it (claim holder and expiry) live only in the
// repo_upstreams row. Every transition below is one BEGIN IMMEDIATE
// transaction on that row, and every transition that ends a run is guarded
// by that run's claim holder. Callers keep no process-local retry timers:
// a sweep derives "due now" from the rows (ListLocalWork), and a claim
// (ClaimDue*) is the only admission gate.
//
// PG schedules through River. S3WAL implements the same transitions in
// s3walmeta_schedule.go (metadata v2, ADR-51).

package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// LocalScheduleStore is the local serve process's durable schedule. It is
// an optional interface (type-assert a Store): only *SQLite implements it
// on a Store today (the S3WAL transitions are not wired yet).
type LocalScheduleStore interface {
	// CompleteSyncRepoCreate marks a pending sync create complete and
	// records its first-sync request in one write (issue #388).
	CompleteSyncRepoCreate(ctx context.Context, org, repoID string) (CreateOutcome, error)
	// RequestPushNow is RequestPush for an explicit caller request (API
	// push-upstream): it also makes the pending work due now, keeping the
	// retry position. One write, so no request/expedite gap exists.
	RequestPushNow(ctx context.Context, repoID string, force bool) error
	// ClaimDuePush claims the push lane only when it has durable work: a
	// pending level whose due time has passed and no live claim, or an
	// expired claim (stolen with its force mandate). Otherwise it writes
	// nothing and reports Busy (a live claim holds the lane) or neither.
	ClaimDuePush(ctx context.Context, repoID, holder string, lease time.Duration) (LocalClaim, error)
	// ClaimDueSync is ClaimDuePush for the pull lane (sync_pending,
	// sync_due); legacy-claim adoption is unchanged from TryBeginSync.
	ClaimDueSync(ctx context.Context, repoID, holder string, lease time.Duration) (LocalClaim, error)
	// FinishPushOutcome ends holder's push run and schedules what follows
	// in the same write. ErrClaimLost if another holder owns the lane;
	// ErrNotFound if detached or no run is in flight.
	FinishPushOutcome(ctx context.Context, repoID, holder string, o PushOutcome) (RepoUpstream, error)
	// FinishSyncOutcome is FinishPushOutcome for the pull lane.
	FinishSyncOutcome(ctx context.Context, repoID, holder string, o SyncOutcome) (RepoUpstream, error)
	// ListLocalWork returns at most limit lanes of org's live repos whose
	// claim guard holds now, ordered by RepoID strictly after after. An
	// empty page ends a traversal; an unknown org has no work.
	ListLocalWork(ctx context.Context, org, after string, limit int) ([]LocalWork, error)
}

var _ LocalScheduleStore = (*SQLite)(nil)

// LocalClaim is the result of a conditional claim. Exactly one of Claimed,
// Busy, or neither (no durable work due) holds.
type LocalClaim struct {
	Up         RepoUpstream // the row after the claim, or the current row when refused
	Claimed    bool
	Busy       bool   // refused: a live claim (or legacy grace) holds the lane
	Force      bool   // push only: the run must force-push
	Attempt    int    // push only: the run's retry position (0 = first attempt)
	StolenFrom string // holder of the expired claim this claim replaced
}

// LaneOutcome is how a run ends.
type LaneOutcome int

const (
	// OutcomeDone ends the run: success, permanent failure, or retry
	// budget spent. Resets attempt and due time.
	OutcomeDone LaneOutcome = iota + 1
	// OutcomeRetry (push only) ends a transiently failed run: re-marks the
	// run's level, advances the attempt, and sets due = now + Delay, unless
	// RequestPushNow recorded an explicit request during the run: that
	// request stays due now.
	OutcomeRetry
	// OutcomeRestore ends a run that never started its transfer (failed
	// publication wait) or that the process's shutdown cancelled (issue
	// #342): re-marks, keeps the attempt, due = now + Delay (an explicit
	// request made during the run stays due now), and counts no run.
	OutcomeRestore
)

// PushOutcome describes how a push run ends.
type PushOutcome struct {
	Kind       LaneOutcome
	Success    bool          // Done: counts a successful run
	MirrorBack bool          // Done+Success: also sets sync_pending in the same write
	Force      bool          // Retry/Restore: the run's consumed level was force
	Delay      time.Duration // Retry/Restore: due = now + Delay
}

// SyncOutcome describes how a pull run ends (Done or Restore).
type SyncOutcome struct {
	Kind    LaneOutcome
	Success bool          // Done: counts a successful run
	Delay   time.Duration // Restore: due = now + Delay
}

// LocalWork is one repo whose push and/or pull lane is claimable now.
type LocalWork struct {
	RepoID string
	Push   bool
	Pull   bool
}

// localSchedule is the SQLite-only schedule columns of one row (unix ns).
type localSchedule struct {
	pushAttempt int
	pushDue     int64
	syncDue     int64
}

func readLocalScheduleTx(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, repoID string) (localSchedule, error) {
	var sc localSchedule
	err := q.QueryRowContext(ctx,
		`SELECT push_attempt, push_due, sync_due FROM repo_upstreams WHERE repo_id = ?`, repoID,
	).Scan(&sc.pushAttempt, &sc.pushDue, &sc.syncDue)
	if errors.Is(err, sql.ErrNoRows) {
		return localSchedule{}, ErrNotFound
	}
	if err != nil {
		return localSchedule{}, err
	}
	assertf(sc.pushAttempt >= 0 && sc.pushDue >= 0 && sc.syncDue >= 0, "corrupt schedule for %q: %+v", repoID, sc)
	return sc, nil
}

// readLocalSchedule reads one row's schedule columns (tests and diagnostics).
func (s *SQLite) readLocalSchedule(ctx context.Context, repoID string) (localSchedule, error) {
	return readLocalScheduleTx(ctx, s.db, repoID)
}

func (s *SQLite) RequestPushNow(ctx context.Context, repoID string, force bool) error {
	assertf(repoID != "", "RequestPushNow: empty repo id")
	level := 1
	if force {
		level = 2
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE repo_upstreams SET push_pending = MAX(push_pending, ?), push_due = 0 WHERE repo_id = ?`, level, repoID)
	if err != nil {
		return fmt.Errorf("request push now for repo %q: %w", repoID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("request push now for repo %q: %w", repoID, err)
	}
	if n == 0 {
		return fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
	}
	assertf(n == 1, "UPDATE by primary key affected %d rows", n)
	return nil
}

func (s *SQLite) ClaimDuePush(ctx context.Context, repoID, holder string, lease time.Duration) (LocalClaim, error) {
	return s.beginPush(ctx, repoID, holder, lease, true)
}

func (s *SQLite) ClaimDueSync(ctx context.Context, repoID, holder string, lease time.Duration) (LocalClaim, error) {
	return s.beginSync(ctx, repoID, holder, lease, true)
}

func (o PushOutcome) validate() {
	switch o.Kind {
	case OutcomeDone:
		assertf(o.Delay == 0 && !o.Force, "done push outcome carries retry fields: %+v", o)
		assertf(!o.MirrorBack || o.Success, "mirror-back without success: %+v", o)
	case OutcomeRetry, OutcomeRestore:
		assertf(o.Delay > 0 && !o.Success && !o.MirrorBack, "invalid push %v outcome: %+v", o.Kind, o)
	default:
		assertf(false, "unknown push outcome kind %d", o.Kind)
	}
}

func (o SyncOutcome) validate() {
	switch o.Kind {
	case OutcomeDone:
		assertf(o.Delay == 0, "done sync outcome carries a delay: %+v", o)
	case OutcomeRestore:
		assertf(o.Delay > 0 && !o.Success, "invalid sync restore outcome: %+v", o)
	default:
		assertf(false, "invalid sync outcome kind %d", o.Kind)
	}
}

func (s *SQLite) FinishPushOutcome(ctx context.Context, repoID, holder string, o PushOutcome) (RepoUpstream, error) {
	assertf(repoID != "", "FinishPushOutcome: empty repo id")
	assertf(holder != "", "FinishPushOutcome: empty holder")
	o.validate()
	nowNanos, _ := s.nowNano()
	const release = `push_syncing = 0, push_claim_holder = '', push_claim_expires_at = 0, push_claim_force = 0`
	var (
		stmt string
		args []any
	)
	level := 1
	if o.Force {
		level = 2
	}
	switch o.Kind {
	case OutcomeDone:
		success, mirror := 0, 0
		if o.Success {
			success = 1
		}
		if o.MirrorBack {
			mirror = 1
		}
		stmt = `UPDATE repo_upstreams SET ` + release + `,
			push_run_count = push_run_count + 1, push_success_count = push_success_count + ?,
			push_attempt = 0, push_due = 0, sync_pending = MAX(sync_pending, ?)`
		args = []any{success, mirror}
	case OutcomeRetry:
		stmt = `UPDATE repo_upstreams SET ` + release + `,
			push_run_count = push_run_count + 1, push_pending = MAX(push_pending, ?),
			push_attempt = push_attempt + 1, push_due = CASE WHEN push_due = 0 THEN 0 ELSE ? END`
		args = []any{level, nowNanos + o.Delay.Nanoseconds()}
	case OutcomeRestore:
		stmt = `UPDATE repo_upstreams SET ` + release + `,
			push_pending = MAX(push_pending, ?), push_due = CASE WHEN push_due = 0 THEN 0 ELSE ? END`
		args = []any{level, nowNanos + o.Delay.Nanoseconds()}
	}
	stmt += ` WHERE repo_id = ? AND push_syncing = 1 AND push_claim_holder = ?`
	args = append(args, repoID, holder)
	up, err := s.finishLane(ctx, repoID, holder, stmt, args, func(cur RepoUpstream) (bool, string) {
		return cur.PushSyncing, cur.PushClaimHolder
	})
	if err != nil {
		return RepoUpstream{}, fmt.Errorf("finish push for repo %q: %w", repoID, err)
	}
	return up, nil
}

func (s *SQLite) FinishSyncOutcome(ctx context.Context, repoID, holder string, o SyncOutcome) (RepoUpstream, error) {
	assertf(repoID != "", "FinishSyncOutcome: empty repo id")
	assertf(holder != "", "FinishSyncOutcome: empty holder")
	o.validate()
	nowNanos, _ := s.nowNano()
	const release = `syncing = 0, sync_claim_holder = '', sync_claim_expires_at = 0`
	var (
		stmt string
		args []any
	)
	switch o.Kind {
	case OutcomeDone:
		success := 0
		if o.Success {
			success = 1
		}
		stmt = `UPDATE repo_upstreams SET ` + release + `,
			run_count = run_count + 1, success_count = success_count + ?, sync_due = 0`
		args = []any{success}
	case OutcomeRestore:
		stmt = `UPDATE repo_upstreams SET ` + release + `, sync_pending = 1, sync_due = ?`
		args = []any{nowNanos + o.Delay.Nanoseconds()}
	}
	stmt += ` WHERE repo_id = ? AND syncing = 1 AND sync_claim_holder = ?`
	args = append(args, repoID, holder)
	up, err := s.finishLane(ctx, repoID, holder, stmt, args, func(cur RepoUpstream) (bool, string) {
		return cur.Syncing, cur.SyncClaimHolder
	})
	if err != nil {
		return RepoUpstream{}, fmt.Errorf("finish sync for repo %q: %w", repoID, err)
	}
	return up, nil
}

// finishLane runs one holder-guarded end-of-run UPDATE and classifies a
// guard miss inside the same transaction, like FinishPush/FinishSync:
// ErrClaimLost when another holder owns the lane, ErrNotFound otherwise.
func (s *SQLite) finishLane(ctx context.Context, repoID, holder, stmt string, args []any,
	claimOf func(RepoUpstream) (bool, string)) (RepoUpstream, error) {
	var up RepoUpstream
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, stmt, args...)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		row, err := scanRepoUpstreamSQLite(tx.QueryRowContext(ctx,
			`SELECT `+repoUpstreamCols+` FROM repo_upstreams WHERE repo_id = ?`, repoID))
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if n == 0 {
			if held, cur := claimOf(row); held && cur != holder {
				return fmt.Errorf("now held by %q: %w", cur, ErrClaimLost)
			}
			return ErrNotFound
		}
		assertf(n == 1, "finish for %q affected %d rows", repoID, n)
		held, _ := claimOf(row)
		assertf(!held, "finish for %q left the lane claimed", repoID)
		up = row
		return nil
	})
	return up, err
}

func (s *SQLite) ListLocalWork(ctx context.Context, org, after string, limit int) ([]LocalWork, error) {
	assertf(org != "" && limit > 0, "ListLocalWork: empty org %q or non-positive limit %d", org, limit)
	nowNanos, _ := s.nowNano()
	steal := 0
	if legacyStealEnabled() {
		steal = 1
	}
	// Each flag is the claim guard of ClaimDue* evaluated now: an
	// unclaimed lane with a due pending level, or an expired claim. A
	// lapsed legacy grace is not claimable without the operator's steal
	// assertion, so it is not work either (it would recur every pass).
	// Other orgs' and soft-deleted repos' rows are filtered before LIMIT.
	const push = `((u.push_syncing = 0 AND u.push_pending > 0 AND u.push_due <= :now)
		OR (u.push_syncing = 1 AND u.push_claim_expires_at <= :now))`
	const pull = `((u.syncing = 0 AND u.sync_pending = 1 AND u.sync_due <= :now)
		OR (u.syncing = 1 AND u.sync_claim_expires_at <= :now
		    AND (u.sync_claim_holder != :legacy OR :steal = 1)))`
	rows, err := s.db.QueryContext(ctx, `SELECT u.repo_id, `+push+`, `+pull+`
		FROM repo_upstreams u
		JOIN repos r ON r.repo_id = u.repo_id AND r.deleted_at IS NULL
		JOIN orgs o ON o.id = r.org_id AND o.name = :org
		WHERE u.repo_id > :after AND (`+push+` OR `+pull+`)
		ORDER BY u.repo_id LIMIT :limit`,
		sql.Named("now", nowNanos), sql.Named("legacy", LegacyClaimHolder), sql.Named("steal", steal),
		sql.Named("org", org), sql.Named("after", after), sql.Named("limit", limit))
	if err != nil {
		return nil, fmt.Errorf("list local work: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var work []LocalWork
	for rows.Next() {
		var w LocalWork
		if err := rows.Scan(&w.RepoID, &w.Push, &w.Pull); err != nil {
			return nil, fmt.Errorf("list local work: %w", err)
		}
		assertf(w.Push || w.Pull, "listed %q without due work", w.RepoID)
		work = append(work, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list local work: %w", err)
	}
	assertf(len(work) <= limit, "ListLocalWork returned %d > %d rows", len(work), limit)
	return work, nil
}
