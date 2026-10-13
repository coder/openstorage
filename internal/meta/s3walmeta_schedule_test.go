// S3WAL-only local schedule tests (ADR-51). The shared transition table
// and model check run on S3WAL through runSchedBackends
// (sqlite_schedule_test.go); these cover what only S3WAL has: the sync
// fence write-through.

package meta

import (
	"context"
	"testing"
	"time"
)

// Serve chooses the durable schedule by asserting LocalScheduleStore, and
// the S3WAL worker is wired separately: until then *S3WAL must not
// satisfy the interface.
func TestS3WALIsNotYetALocalScheduleStore(t *testing.T) {
	if _, ok := any(&S3WAL{}).(LocalScheduleStore); ok {
		t.Fatal("*S3WAL satisfies LocalScheduleStore: serve would switch S3WAL to the durable schedule")
	}
}

// ClaimDueSync installs the manifest fence token like TryBeginSync, and
// FinishSyncOutcome revokes it before it ends the run.
func TestS3WALClaimDueSyncWritesThrough(t *testing.T) {
	ctx := context.Background()
	cas, metaStore, _ := newWriteThroughStores(t)
	ls := s3walLocalSchedule{metaStore}
	repoID := seedS3WALUpstream(t, metaStore)
	if c, err := ls.ClaimDueSync(ctx, repoID, "h1", time.Minute); err != nil || c.Claimed || c.Busy {
		t.Fatalf("claim without a request = %+v, %v; want refused, not busy", c, err)
	}
	if got := manifestSyncClaim(t, cas, repoID); got != "" {
		t.Fatalf("refused claim left manifest token %q", got)
	}
	if err := metaStore.RequestSync(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	if c, err := ls.ClaimDueSync(ctx, repoID, "h1", time.Minute); err != nil || !c.Claimed {
		t.Fatalf("claim = %+v, %v", c, err)
	}
	if got := manifestSyncClaim(t, cas, repoID); got != "h1" {
		t.Fatalf("manifest token = %q, want h1", got)
	}
	if c, err := ls.ClaimDueSync(ctx, repoID, "h2", time.Minute); err != nil || c.Claimed || !c.Busy {
		t.Fatalf("second claim = %+v, %v; want busy", c, err)
	}
	if _, err := ls.FinishSyncOutcome(ctx, repoID, "h1", SyncOutcome{Kind: OutcomeRestore, Delay: time.Second}); err != nil {
		t.Fatal(err)
	}
	if got := manifestSyncClaim(t, cas, repoID); got != "" {
		t.Fatalf("manifest token after restore = %q, want cleared", got)
	}
}
