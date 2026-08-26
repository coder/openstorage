// s3walmeta_purge.go mirrors purge.go's repository-purge metadata seams
// for the s3wal backend (issue #52) — see purge.go for the rationale. The
// repos row is kept forever as a name tombstone (burned-name invariant);
// purge deletes the eventing rows and stamps purged_at in ONE document
// CAS, so the guard read and the deletes are same-atom by construction
// (the atomicity the SQL twins buy with transactions).
package meta

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// PurgeableRepos — see the PG twin. deleted_at is stored as unixnano;
// ordering and the keyset cursor match the SQL row-value comparison.
func (s *S3WAL) PurgeableRepos(ctx context.Context, deletedBefore time.Time, after PurgeCandidate, limit int) ([]PurgeCandidate, error) {
	assertf(limit > 0, "PurgeableRepos: non-positive limit %d", limit)
	afterAt, afterID := purgeCursorArgs(after)
	afterNanos := afterAt.UnixNano()
	cutoff := deletedBefore.UnixNano()
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return nil, err
	}
	var out []PurgeCandidate
	for id, r := range doc.Repos {
		if r.DeletedAt == 0 || r.DeletedAt > cutoff || r.PurgedAt != 0 {
			continue
		}
		// Keyset predicate: (deleted_at, repo_id) > (after).
		if r.DeletedAt < afterNanos || (r.DeletedAt == afterNanos && id <= afterID) {
			continue
		}
		out = append(out, PurgeCandidate{RepoID: id, DeletedAt: time.Unix(0, r.DeletedAt).UTC()})
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].DeletedAt.Equal(out[j].DeletedAt) {
			return out[i].DeletedAt.Before(out[j].DeletedAt)
		}
		return out[i].RepoID < out[j].RepoID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// RepoPurgeState — see the PG twin.
func (s *S3WAL) RepoPurgeState(ctx context.Context, repoID string) (PurgeState, bool, error) {
	assertf(repoID != "", "RepoPurgeState: empty repo ID")
	doc, err := s.viewDoc(ctx)
	if err != nil {
		return PurgeState{}, false, err
	}
	r := doc.Repos[repoID]
	if r == nil {
		return PurgeState{}, false, nil
	}
	state := PurgeState{Repo: r.toRepo(repoID)}
	if r.PurgedAt != 0 {
		utc := time.Unix(0, r.PurgedAt).UTC()
		state.PurgedAt = &utc
	}
	return state, true, nil
}

// PurgeRepoMeta — see the PG twin: deletes the repo's credential and
// upstream rows and stamps purged_at, guarded on the row still being
// soft-deleted; a missing or already-stamped row is an idempotent no-op;
// a live row aborts with ErrRepoNotDeleted before anything is touched.
// Recovery class ID: every effect is a guarded delete/stamp, so a re-run
// on a fresh snapshot converges on the same terminal state.
func (s *S3WAL) PurgeRepoMeta(ctx context.Context, repoID string) error {
	assertf(repoID != "", "PurgeRepoMeta: empty repo ID")
	nanos, _ := s.nowNano()
	err := s.mutate(ctx, s3walMetaMutation{
		op: "purge repo meta",
		fn: func(doc *s3walMetaDoc) (bool, error) {
			r := doc.Repos[repoID]
			if r == nil {
				return false, nil // row gone (legacy purge): nothing left to finalize
			}
			if r.DeletedAt == 0 {
				return false, ErrRepoNotDeleted
			}
			changed := false
			for id, c := range doc.Credentials {
				if c.RepoID == repoID {
					delete(doc.Credentials, id)
					changed = true
				}
			}
			if doc.Upstreams[repoID] != nil {
				delete(doc.Upstreams, repoID)
				changed = true
			}
			if r.PurgedAt == 0 {
				r.PurgedAt = nanos
				changed = true
			}
			return changed, nil
		},
	})
	if err != nil {
		return fmt.Errorf("purge repo %s metadata: %w", repoID, err)
	}
	return nil
}
