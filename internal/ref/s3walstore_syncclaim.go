// Phase 3 sync-claim write-through seam (plan §2.8, issue #133): the S3
// meta backend (internal/meta) mirrors every inbound sync claim into the
// base repo group's manifest so a fenced CASRef checks the holder in the
// SAME object it CASes — steal-vs-write serialization is the manifest CAS
// itself, strictly stronger than the SQL FOR-UPDATE argument. These two
// exported methods are the entire seam: the meta backend calls them
// through a small interface (meta.SyncClaimSink) so ref never imports
// meta; wiring happens at serve time.
//
// Like the SQL fence (`... AND sync_claim_holder = $4`), the manifest
// fence checks holder identity only — expiry arbitration stays in the
// meta store's claim lease, whose CAS decides steals before the thief
// rewrites this token. CreatedAt is the manifest write clock, recorded
// for observability, never compared.
package ref

import (
	"context"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/coder/openstorage/internal/ref/s3wal"
)

// SetSyncClaim installs holder as repoID's sync-claim fence token,
// overwriting any previous token: the caller won the meta claim lease
// (possibly by stealing an expired one), so the previous token — live or
// stale — is superseded by construction.
//
// Recovery class PRED: holder tokens are unique per sync run, so no
// same-holder entry can predate this call — a fresh snapshot carrying our
// holder proves OUR write landed (the Phase 1b renewal lesson about
// same-field rewrites does not bite: there is no pre-existing same-holder
// entry whose old stamp could alias a dropped PUT).
func (s *S3WAL) SetSyncClaim(ctx context.Context, repoID, holder string) error {
	assertf(repoID != "" && holder != "", "SetSyncClaim(%q, %q)", repoID, holder)
	if !utf8.ValidString(holder) {
		// Same codec-boundary rationale as ErrRefNameNotUTF8: an invalid
		// holder would be coerced to its U+FFFD spelling on encode and the
		// fence comparison (raw bytes) could never match it again.
		return fmt.Errorf("set sync claim %s: holder: %w", repoID, ErrRefNameNotUTF8)
	}
	freshInstall := false
	return s.mutate(ctx, baseRepoOf(repoID), s3walMutation{
		op: "set sync claim",
		// Rewriting an existing token is non-growing: an over-cap manifest
		// must not wedge claim rotation on a dead holder's stale token.
		grows: func() bool { return freshInstall },
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			freshInstall = false
			if m.PurgedAt != nil {
				// Purge is terminal (burned-name invariant): the fence is
				// moot — every fenced CASRef already fails on ErrRepoPurged
				// — so installing a token would only dirty a tombstone.
				return false, nil
			}
			if m.SyncClaim != nil && m.SyncClaim.Holder == holder {
				return false, nil // already ours (idempotent re-entry)
			}
			freshInstall = m.SyncClaim == nil
			m.SyncClaim = &s3wal.SyncClaim{Holder: holder, CreatedAt: clk}
			return true, nil
		},
		recover: func(fresh *s3wal.Manifest) s3walVerdict {
			if fresh.PurgedAt != nil {
				return s3walApplied // terminal no-op either way
			}
			if fresh.SyncClaim != nil && fresh.SyncClaim.Holder == holder {
				return s3walApplied
			}
			return s3walRerun
		},
	})
}

// ClearSyncClaim removes repoID's sync-claim fence token iff it still
// belongs to holder (owner-guarded: a thief's token is never touched —
// the late finisher of a stolen claim must leave the thief's fence
// intact, issue #86).
//
// Recovery class PRED on the semantic effect: the postcondition is
// "holder's token is not in the manifest", so a fresh snapshot with no
// claim or a foreign holder proves it regardless of whether our PUT or a
// competing overwrite produced it.
func (s *S3WAL) ClearSyncClaim(ctx context.Context, repoID, holder string) error {
	assertf(repoID != "" && holder != "", "ClearSyncClaim(%q, %q)", repoID, holder)
	if !utf8.ValidString(holder) {
		return nil // no manifest can contain such a token — clean no-op
	}
	return s.mutate(ctx, baseRepoOf(repoID), s3walMutation{
		op: "clear sync claim",
		fn: func(m *s3wal.Manifest, _ time.Time) (bool, error) {
			if m.PurgedAt != nil || m.SyncClaim == nil || m.SyncClaim.Holder != holder {
				return false, nil // owner guard: not ours to clear
			}
			m.SyncClaim = nil
			return true, nil
		},
		recover: func(fresh *s3wal.Manifest) s3walVerdict {
			if fresh.PurgedAt != nil || fresh.SyncClaim == nil || fresh.SyncClaim.Holder != holder {
				return s3walApplied
			}
			return s3walRerun
		},
	})
}
