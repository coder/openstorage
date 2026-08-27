package githttp

// End-to-end coverage for issue #170: a concurrent request's Hydrate must
// not full-sync a just-CAS-committed ref into the shared cache repo before
// the committing push's own receive-pack performs its local ref update —
// git would then reject the push ("cannot lock ref ... reference already
// exists") even though the authority accepted it.
//
// The race window (post-CAS, pre-local-update) is opened deterministically
// with the EnvUpdateBarrier hook seam.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

// TestPush_SurvivesConcurrentHydrateOfCommittedRef: a branch-creation push
// pauses after its update hook committed the authority CAS; a concurrent
// request on the same node then hydrates (full sync — the CAS bumped the
// repo generation); the released push must still succeed.
func TestPush_SurvivesConcurrentHydrateOfCommittedRef(t *testing.T) {
	repo := registerRepo(t, "t-hydrate-race")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	barrier := filepath.Join(t.TempDir(), "update-barrier")
	node := startNodeT(t, EnvUpdateBarrier+"="+barrier)

	type pushResult struct {
		out string
		err error
	}
	done := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/race")
		done <- pushResult{out, err}
	}()

	// The hook signals: authority CAS committed, local ref update pending.
	waitForFile(t, barrier+".ready", 30*time.Second)

	// Concurrent request on the same node. The CAS bumped the generation,
	// so this takes the full-sync path over the same cache repo the paused
	// receive-pack is about to update.
	mustGit(t, "", "ls-remote", remoteURL(node, repo, token))

	// Release the hook; receive-pack now performs its local ref update.
	if err := os.WriteFile(barrier+".release", nil, 0o644); err != nil {
		t.Fatalf("release barrier: %v", err)
	}

	res := <-done
	t.Logf("push racing a concurrent hydrate:\n%s", res.out)
	if res.err != nil {
		t.Fatalf("push spuriously rejected while racing a concurrent hydrate: %v\n%s", res.err, res.out)
	}
	if got := pgRefs(t, repo)["refs/heads/race"]; got != head {
		t.Fatalf("authority refs/heads/race = %q, want %q", got, head)
	}
	// The cache repo converges: once the push is done, an advertisement on
	// the same node serves the new branch.
	out := mustGit(t, "", "ls-remote", remoteURL(node, repo, token), "refs/heads/race")
	if !strings.Contains(out, head) {
		t.Fatalf("post-push advertisement lacks refs/heads/race@%s:\n%s", head, out)
	}
}

// TestPush_DFCreateWaitsForHeldNeighborDeletion (Codex P1 on #173): push A
// deletes refs/heads/a and pauses post-CAS (hold live, local deletion
// pending — hydration deliberately skips held refs, so the stale local `a`
// survives every sync). Push B then creates refs/heads/a/b: its update hook
// must wait for A's hold to clear before committing, or B's authority CAS
// would succeed while its local update fails against the surviving loose
// ref — the same authority-accepted-but-client-rejected wedge as the plain
// race.
func TestPush_DFCreateWaitsForHeldNeighborDeletion(t *testing.T) {
	repo := registerRepo(t, "t-hydrate-race-df")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main", "HEAD:refs/heads/a")
	head := revParse(t, work, "HEAD")

	barrier := filepath.Join(t.TempDir(), "update-barrier")
	node := startNodeT(t, EnvUpdateBarrier+"="+barrier)

	type pushResult struct {
		out string
		err error
	}
	// Push A: delete refs/heads/a; its update hook (the node's first)
	// consumes the one-shot barrier and pauses post-CAS.
	doneA := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", remoteURL(node, repo, token), ":refs/heads/a")
		doneA <- pushResult{out, err}
	}()
	waitForFile(t, barrier+".ready", 30*time.Second)

	// Push B: create the D/F child refs/heads/a/b while A's hold is live.
	doneB := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/a/b")
		doneB <- pushResult{out, err}
	}()
	// Give B time to reach its update hook's conflicting-hold wait, then
	// release A. (Timing slop only makes B trivially conflict-free; the
	// assertions below hold either way.)
	time.Sleep(1 * time.Second)
	if err := os.WriteFile(barrier+".release", nil, 0o644); err != nil {
		t.Fatalf("release barrier: %v", err)
	}

	resA := <-doneA
	t.Logf("push A (delete a):\n%s", resA.out)
	if resA.err != nil {
		t.Fatalf("deletion push failed: %v\n%s", resA.err, resA.out)
	}
	resB := <-doneB
	t.Logf("push B (create a/b):\n%s", resB.out)
	if resB.err != nil {
		t.Fatalf("D/F creation push spuriously rejected: %v\n%s", resB.err, resB.out)
	}
	refs := pgRefs(t, repo)
	if _, ok := refs["refs/heads/a"]; ok {
		t.Fatalf("refs/heads/a survived its deletion: %v", refs)
	}
	if refs["refs/heads/a/b"] != head {
		t.Fatalf("authority refs/heads/a/b = %q, want %q", refs["refs/heads/a/b"], head)
	}
}
