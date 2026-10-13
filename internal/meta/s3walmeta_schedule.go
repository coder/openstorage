// Local schedule state for S3WAL (issue #335, ADR-50, ADR-51): the same
// row-owned schedule as sqlite_schedule.go, stored in the metadata v2
// fields push_attempt, push_due and sync_due. Every transition is one CAS
// of _wal/meta.json, and every transition that ends a run is guarded by
// that run's claim holder.
//
// The transitions live on s3walLocalSchedule, not on *S3WAL, so that
// *S3WAL does not satisfy LocalScheduleStore yet: serve type-asserts that
// interface to choose the durable schedule, and the S3WAL worker, sweep and
// shutdown restore are wired separately (ADR-51 PR 3).
//
// Ambiguous PUTs: like FinishSync, the finish transitions classify a
// superseded write by its effect. A thief that stole and finished an
// expired claim can move the counters exactly as ours did, so either
// verdict leaves the stored schedule correct; only the caller's report can
// differ (success, ErrClaimLost or ErrNotFound). No outcome is ever applied
// twice, because the holder guard fails once the claim is released.

package meta

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// s3walLocalSchedule is the S3WAL local schedule over s's document.
type s3walLocalSchedule struct{ *S3WAL }

var _ LocalScheduleStore = s3walLocalSchedule{}

// readLocalSchedule reads one lane's schedule fields (tests and diagnostics).
func (s s3walLocalSchedule) readLocalSchedule(ctx context.Context, repoID string) (localSchedule, error) {
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return localSchedule{}, err
	}
	u, err := lookupUpstream(doc, repoID)
	if err != nil {
		return localSchedule{}, ErrNotFound
	}
	sc := localSchedule{pushAttempt: u.PushAttempt, pushDue: u.PushDue, syncDue: u.SyncDue}
	assertf(sc.pushAttempt >= 0 && sc.pushDue >= 0 && sc.syncDue >= 0, "corrupt schedule for %q: %+v", repoID, sc)
	return sc, nil
}

// CompleteSyncRepoCreate is CompleteRepoCreate that also records the
// first-sync request (sync_pending) in the same CAS. A complete or deleted
// row records nothing; a pending row without an upstream is ErrNotFound and
// stays pending. Recovery class PRED, as CompleteRepoCreate.
func (s s3walLocalSchedule) CompleteSyncRepoCreate(ctx context.Context, org, repoID string) (CreateOutcome, error) {
	assertf(repoID != "", "CompleteSyncRepoCreate: empty repo id for org %q", org)
	var out CreateOutcome
	err := s.mutate(ctx, s3walMetaMutation{
		op: "complete sync repo create",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			out = 0
			orgRow, err := lookupOrg(doc, org)
			if err != nil {
				return false, err
			}
			r := doc.Repos[repoID]
			if r == nil || r.OrgID != orgRow.ID {
				return false, fmt.Errorf("repo id %q in org %q: %w", repoID, org, ErrNotFound)
			}
			if r.DeletedAt != 0 || CreateState(r.CreateState) != CreateStatePending {
				out, err = completeOutcome(r.toRepo(repoID))
				return false, err
			}
			u := doc.Upstreams[repoID]
			if u == nil || u.Detaching {
				return false, fmt.Errorf("request first sync for repo %q: %w", repoID, ErrNotFound)
			}
			u.SyncPending = true
			r.CreateState = int(CreateStateComplete)
			out = CreateCompleted
			return true, nil
		},
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

// RequestPushNow raises the pending level and makes it due now, keeping the
// attempt. Recovery class ID: the MAX-merge and the zero due time are
// idempotent.
func (s s3walLocalSchedule) RequestPushNow(ctx context.Context, repoID string, force bool) error {
	assertf(repoID != "", "RequestPushNow: empty repo id")
	level := 1
	if force {
		level = 2
	}
	err := s.mutate(ctx, s3walMetaMutation{
		op: "request push now",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			u, err := lookupUpstream(doc, repoID)
			if err != nil {
				return false, err
			}
			if u.PushPending >= level && u.PushDue == 0 {
				return false, nil
			}
			u.PushPending, u.PushDue = max(u.PushPending, level), 0
			return true, nil
		},
	})
	if err != nil {
		return fmt.Errorf("request push now for repo %q: %w", repoID, err)
	}
	return nil
}

func (s s3walLocalSchedule) ClaimDuePush(ctx context.Context, repoID, holder string, lease time.Duration) (LocalClaim, error) {
	return s.beginPush(ctx, repoID, holder, lease, true)
}

func (s s3walLocalSchedule) ClaimDueSync(ctx context.Context, repoID, holder string, lease time.Duration) (LocalClaim, error) {
	return s.beginSync(ctx, repoID, holder, lease, true)
}

// FinishPushOutcome ends holder's push run and schedules what follows in
// the same CAS, as on SQLite. Recovery class PRED: our claim still in place
// means the write never landed; otherwise a run-counting outcome landed iff
// the run and success counters and the attempt moved exactly as ours, and a
// restore landed iff the lane is released with its level re-marked. A restore superseded by a new claim
// reruns and reports ErrClaimLost or ErrNotFound, but its effect stands.
func (s s3walLocalSchedule) FinishPushOutcome(ctx context.Context, repoID, holder string, o PushOutcome) (RepoUpstream, error) {
	assertf(repoID != "", "FinishPushOutcome: empty repo id")
	assertf(holder != "", "FinishPushOutcome: empty holder")
	o.validate()
	nowNanos, _ := s.nowNano()
	level := 1
	if o.Force {
		level = 2
	}
	var up RepoUpstream
	var prevRun, prevSuccess, prevAttempt int
	err := s.mutate(ctx, s3walMetaMutation{
		op: "finish push outcome",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			u := doc.Upstreams[repoID]
			if err := laneHeld(repoID, u != nil && u.PushSyncing, func() string { return u.PushClaimHolder }, holder); err != nil {
				return false, err
			}
			prevRun, prevSuccess, prevAttempt = u.PushRunCount, u.PushSuccessCount, u.PushAttempt
			u.PushSyncing, u.PushClaimHolder, u.PushClaimExpiresAt, u.PushClaimForce = false, "", 0, false
			switch o.Kind {
			case OutcomeDone:
				u.PushRunCount++
				if o.Success {
					u.PushSuccessCount++
				}
				u.PushAttempt, u.PushDue = 0, 0
				u.SyncPending = u.SyncPending || o.MirrorBack
			case OutcomeRetry, OutcomeRestore:
				if o.Kind == OutcomeRetry {
					u.PushRunCount++
					u.PushAttempt++
				}
				u.PushPending = max(u.PushPending, level)
				if u.PushDue != 0 { // 0: an explicit request during the run stays due now
					u.PushDue = nowNanos + o.Delay.Nanoseconds()
				}
			}
			up = u.toRepoUpstream(repoID)
			return true, nil
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			u := doc.Upstreams[repoID]
			switch {
			case u == nil, u.PushSyncing && u.PushClaimHolder == holder:
				return s3walMetaRerun
			case o.Kind == OutcomeDone && u.PushRunCount == prevRun+1 && u.PushSuccessCount == prevSuccess+b2i(o.Success) && u.PushAttempt == 0,
				o.Kind == OutcomeRetry && u.PushRunCount == prevRun+1 && u.PushSuccessCount == prevSuccess && u.PushAttempt == prevAttempt+1,
				o.Kind == OutcomeRestore && !u.PushSyncing && u.PushPending >= level:
				return s3walMetaApplied
			}
			return s3walMetaRerun
		},
	})
	if err != nil {
		return RepoUpstream{}, fmt.Errorf("finish push for repo %q: %w", repoID, err)
	}
	return up, nil
}

// FinishSyncOutcome is FinishPushOutcome for the pull lane. Like FinishSync,
// it first revokes the run's fence token in the ref manifest.
func (s s3walLocalSchedule) FinishSyncOutcome(ctx context.Context, repoID, holder string, o SyncOutcome) (RepoUpstream, error) {
	assertf(repoID != "", "FinishSyncOutcome: empty repo id")
	assertf(holder != "", "FinishSyncOutcome: empty holder")
	o.validate()
	if s.sink != nil {
		if err := s.sink.ClearSyncClaim(ctx, repoID, holder); err != nil {
			return RepoUpstream{}, fmt.Errorf("finish sync for repo %q: clear sync-claim fence: %w", repoID, err)
		}
	}
	nowNanos, _ := s.nowNano()
	var up RepoUpstream
	var prevRun, prevSuccess int
	err := s.mutate(ctx, s3walMetaMutation{
		op: "finish sync outcome",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			u := doc.Upstreams[repoID]
			if err := laneHeld(repoID, u != nil && u.Syncing, func() string { return u.SyncClaimHolder }, holder); err != nil {
				return false, err
			}
			prevRun, prevSuccess = u.RunCount, u.SuccessCount
			u.Syncing, u.SyncClaimHolder, u.SyncClaimExpiresAt = false, "", 0
			switch o.Kind {
			case OutcomeDone:
				u.RunCount++
				if o.Success {
					u.SuccessCount++
				}
				u.SyncDue = 0
			case OutcomeRestore:
				u.SyncPending, u.SyncDue = true, nowNanos+o.Delay.Nanoseconds()
			}
			up = u.toRepoUpstream(repoID)
			return true, nil
		},
		recover: func(doc *s3walMetaDoc) s3walMetaVerdict {
			u := doc.Upstreams[repoID]
			switch {
			case u == nil, u.Syncing && u.SyncClaimHolder == holder:
				return s3walMetaRerun
			case o.Kind == OutcomeDone && u.RunCount == prevRun+1 && u.SuccessCount == prevSuccess+b2i(o.Success),
				o.Kind == OutcomeRestore && !u.Syncing && u.SyncPending:
				return s3walMetaApplied
			}
			return s3walMetaRerun
		},
	})
	if err != nil {
		return RepoUpstream{}, fmt.Errorf("finish sync for repo %q: %w", repoID, err)
	}
	return up, nil
}

// laneHeld classifies an end-of-run guard as SQLite's finishLane does: nil
// when holder owns the lane, ErrClaimLost when another holder does, and
// ErrNotFound otherwise (detached, or no run in flight).
func laneHeld(repoID string, held bool, cur func() string, holder string) error {
	switch {
	case held && cur() == holder:
		return nil
	case held:
		return fmt.Errorf("now held by %q: %w", cur(), ErrClaimLost)
	}
	return fmt.Errorf("upstream for repo %q: %w", repoID, ErrNotFound)
}

// ListLocalWork lists the claimable lanes of org's live repos, as on
// SQLite. A detaching row's pull lane is not work: ClaimDueSync refuses it.
func (s s3walLocalSchedule) ListLocalWork(ctx context.Context, org, after string, limit int) ([]LocalWork, error) {
	assertf(org != "" && limit > 0, "ListLocalWork: empty org %q or non-positive limit %d", org, limit)
	nowNanos, _ := s.nowNano()
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return nil, fmt.Errorf("list local work: %w", err)
	}
	orgRow := doc.Orgs[org]
	if orgRow == nil {
		return nil, nil
	}
	var work []LocalWork
	for id, u := range doc.Upstreams {
		r := doc.Repos[id]
		if id <= after || r == nil || r.OrgID != orgRow.ID || r.DeletedAt != 0 {
			continue
		}
		w := LocalWork{
			RepoID: id,
			Push: (!u.PushSyncing && u.PushPending > 0 && u.PushDue <= nowNanos) ||
				(u.PushSyncing && u.PushClaimExpiresAt <= nowNanos),
			Pull: !u.Detaching && ((!u.Syncing && u.SyncPending && u.SyncDue <= nowNanos) ||
				(u.Syncing && u.SyncClaimExpiresAt <= nowNanos)),
		}
		if w.Push || w.Pull {
			work = append(work, w)
		}
	}
	sort.Slice(work, func(i, j int) bool { return work[i].RepoID < work[j].RepoID })
	if len(work) > limit {
		work = work[:limit]
	}
	return work, nil
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
