package sync

import (
	"context"

	"github.com/coder/openstorage/internal/meta"
)

// FirstSyncScheduler is a Queue that schedules a sync create's first sync
// in the transaction that marks the create complete (issue #388).
type FirstSyncScheduler interface {
	// TracksCreateState is false when the queue cannot complete a create
	// (the in-process queue): callers then never call CompleteSyncCreate.
	TracksCreateState() bool
	// CompleteSyncCreate atomically schedules the first sync (no ref: the
	// worker mirrors all heads and tags) and marks the create complete,
	// only while pending. Complete or deleted repos schedule nothing.
	CompleteSyncCreate(ctx context.Context, org, repoID string) (meta.CreateOutcome, error)
}

// CompleteSyncCreate records the first-sync request and the complete mark
// in one SQLite write, then offers the run (the sweep runs it otherwise).
func (s *LocalScheduler) CompleteSyncCreate(ctx context.Context, org, repoID string) (meta.CreateOutcome, error) {
	assertf(org == s.org && repoID != "", "CompleteSyncCreate: org %q (scheduler %q), repo %q", org, s.org, repoID)
	done, err := s.enter()
	if err != nil {
		return 0, err
	}
	defer done()
	out, err := s.lanes.CompleteSyncRepoCreate(ctx, org, repoID)
	if err != nil || out != meta.CreateCompleted {
		return out, err
	}
	if err := s.offer(Job{Org: org, RepoID: repoID}, false, false); err != nil {
		s.logger.Warn("first sync offer failed; the sweep will run it", "repo_id", repoID, "err", err)
	}
	return out, nil
}

// TracksCreateState: every local schedule store (SQLite, S3WAL) stores
// create state.
func (s *LocalScheduler) TracksCreateState() bool { return true }

func (m metaLocalLanes) CompleteSyncRepoCreate(ctx context.Context, org, repoID string) (meta.CreateOutcome, error) {
	return m.ls.CompleteSyncRepoCreate(ctx, org, repoID)
}
