package ref

// Fenced CAS coverage (issue #86): under WithSyncClaimFence the swap
// applies only while the repo's sync claim is held by the fencing holder,
// evaluated atomically inside the CAS statement — a stolen run's stale
// force push loses at the ref store even if it slips past every earlier
// check. Relies on the deployment invariant that meta and ref share one
// database; the tests create a minimal repo_upstreams shape in the ref
// test database.

import (
	"context"
	"testing"
)

const fenceDDL = `CREATE TABLE IF NOT EXISTS repo_upstreams (
	repo_id TEXT PRIMARY KEY,
	sync_claim_holder TEXT NOT NULL DEFAULT ''
)`

func TestCASRefSyncClaimFence(t *testing.T) {
	t.Run("pg", func(t *testing.T) {
		s := pgTestStore(t)
		ctx := context.Background()
		exec := func(q string, args ...any) {
			t.Helper()
			if _, err := s.pool.Exec(ctx, q, args...); err != nil {
				t.Fatalf("exec %q: %v", q, err)
			}
		}
		exec(fenceDDL)
		testCASRefSyncClaimFence(t, s, func(holder string) {
			exec(`INSERT INTO repo_upstreams (repo_id, sync_claim_holder) VALUES ($1, $2)
			      ON CONFLICT (repo_id) DO UPDATE SET sync_claim_holder = $2`, "repo_fence_pg", holder)
		}, func() {
			exec(`DELETE FROM repo_upstreams WHERE repo_id = $1`, "repo_fence_pg")
		}, "repo_fence_pg")
	})
	t.Run("sqlite", func(t *testing.T) {
		s, _ := openTestSQLite(t)
		ctx := context.Background()
		exec := func(q string, args ...any) {
			t.Helper()
			if _, err := s.db.ExecContext(ctx, q, args...); err != nil {
				t.Fatalf("exec %q: %v", q, err)
			}
		}
		exec(fenceDDL)
		testCASRefSyncClaimFence(t, s, func(holder string) {
			exec(`INSERT INTO repo_upstreams (repo_id, sync_claim_holder) VALUES (?, ?)
			      ON CONFLICT (repo_id) DO UPDATE SET sync_claim_holder = ?`, "repo_fence_sq", holder, holder)
		}, func() {
			exec(`DELETE FROM repo_upstreams WHERE repo_id = ?`, "repo_fence_sq")
		}, "repo_fence_sq")
	})
}

func testCASRefSyncClaimFence(t *testing.T, s RefStore, setHolder func(string), dropRow func(), repo string) {
	ctx := context.Background()
	const (
		shaA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		shaB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	cas := func(fenceHolder, old, new string) bool {
		t.Helper()
		cctx := ctx
		if fenceHolder != "" {
			cctx = WithSyncClaimFence(cctx, repo, fenceHolder)
		}
		won, err := s.CASRef(cctx, repo, "refs/heads/main", old, new)
		if err != nil {
			t.Fatalf("CASRef(%q, %s -> %s): %v", fenceHolder, old[:4], new[:4], err)
		}
		return won
	}
	mustRef := func(want string) {
		t.Helper()
		got, err := s.GetRef(ctx, repo, "refs/heads/main")
		if want == "" {
			if err == nil {
				t.Fatalf("ref exists (%s), want absent", got[:8])
			}
			return
		}
		if err != nil || got != want {
			t.Fatalf("ref = %q (%v), want %s", got, err, want[:8])
		}
	}

	setHolder("h-live")
	// The live holder creates and updates through the fence.
	if !cas("h-live", ZeroSHA, shaA) {
		t.Fatal("fenced create by the live holder lost")
	}
	mustRef(shaA)
	// A stale holder's update is refused, ref untouched.
	if cas("h-stale", shaA, shaB) {
		t.Fatal("fenced update by a stale holder won")
	}
	mustRef(shaA)
	// The claim moves (steal): the previous holder loses, the thief wins.
	setHolder("h-thief")
	if cas("h-live", shaA, shaB) {
		t.Fatal("fenced update by the stolen-from holder won")
	}
	mustRef(shaA)
	if !cas("h-thief", shaA, shaB) {
		t.Fatal("fenced update by the thief lost")
	}
	mustRef(shaB)
	// Unfenced CAS is unchanged by the fence machinery.
	if !cas("", shaB, shaA) {
		t.Fatal("unfenced update lost")
	}
	mustRef(shaA)
	// Fenced delete: stale holder refused, live holder wins.
	if cas("h-stale", shaA, ZeroSHA) {
		t.Fatal("fenced delete by a stale holder won")
	}
	mustRef(shaA)
	if !cas("h-thief", shaA, ZeroSHA) {
		t.Fatal("fenced delete by the thief lost")
	}
	mustRef("")
	// No upstream row at all: the fence fails CLOSED.
	dropRow()
	if cas("h-thief", ZeroSHA, shaA) {
		t.Fatal("fenced create with no upstream row won, want fail-closed refusal")
	}
	mustRef("")
}
