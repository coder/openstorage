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

// TestPush_DFChildOfExistingRefCleanlyDeclined (Codex round-4 P1 family):
// the authority CAS is name-keyed, so without the update hook's
// authority-side D/F probe a push creating refs/heads/a/b while
// refs/heads/a exists commits a/b into the authority even though no git
// ref store can hold both — permanently wedging every future hydration of
// the repo. The hook must decline the update per-ref BEFORE the CAS,
// keeping the authority representable and the repo serving.
func TestPush_DFChildOfExistingRefCleanlyDeclined(t *testing.T) {
	repo := registerRepo(t, "t-df-existing")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main", "HEAD:refs/heads/a")

	out, err := tryGit(work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/a/b")
	if err == nil {
		t.Fatalf("D/F-conflicting creation must be rejected:\n%s", out)
	}
	t.Logf("D/F-conflicting push (expected clean decline):\n%s", out)
	if !strings.Contains(out, "directory/file") {
		t.Fatalf("rejection is not the hook's clean D/F decline:\n%s", out)
	}
	refs := pgRefs(t, repo)
	if _, ok := refs["refs/heads/a/b"]; ok {
		t.Fatalf("authority accepted an unrepresentable D/F pair: %v", refs)
	}
	// The repo still serves: no committed-but-unmaterializable ref is
	// wedging hydration.
	mustGit(t, "", "ls-remote", remoteURL(nodeA, repo, token))
}

// TestPush_DeniedParentAllowedChildLands (Codex round-7 P2): a non-atomic
// push carrying a policy-DENIED creation of refs/heads/den and an allowed
// creation of refs/heads/den/ok must keep git's per-ref semantics — the
// denied parent never survives, so the allowed child lands; whole-push D/F
// denial is only correct for atomic pushes, where an already-committed
// authority CAS cannot be taken back when git aborts the local
// transaction.
func TestPush_DeniedParentAllowedChildLands(t *testing.T) {
	repo := registerRepo(t, "t-df-denied-parent")
	work := initWorkRepo(t)
	full := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, full), "HEAD:refs/heads/main")
	head := revParse(t, work, "HEAD")

	// A fresh commit so the allowed child genuinely needs its pack — a
	// stored-objects-only mixed push is refused whole by the deliberate
	// publication gate (issue #21), which is not the semantics under test.
	writeFile(t, work, "b.txt", "two\n")
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "two")
	head2 := revParse(t, work, "HEAD")

	restricted := mintTestToken(t, repo, "git:read,git:write", `[["refs/heads/den",["no-push"]],["*",[]]]`)
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, restricted), head+":refs/heads/den", "HEAD:refs/heads/den/ok")
	if err == nil {
		t.Fatalf("push must report the denied parent:\n%s", out)
	}
	t.Logf("mixed denied-parent push (child must land):\n%s", out)
	refs := pgRefs(t, repo)
	if _, ok := refs["refs/heads/den"]; ok {
		t.Fatalf("denied parent committed: %v", refs)
	}
	if refs["refs/heads/den/ok"] != head2 {
		t.Fatalf("allowed child rejected alongside its denied parent: %v", refs)
	}
}

// TestAtomicPush_DeniedWhenCreationDoomedByAuthorityDF (Codex round-8 P1):
// an atomic push mixing a healthy update with a creation whose D/F
// relative exists in the authority must be denied WHOLE in pre-receive —
// otherwise the healthy command's authority CAS commits before the doomed
// creation's hook declines, git aborts the local atomic transaction, and
// the client is told the whole push failed while the authority moved.
func TestAtomicPush_DeniedWhenCreationDoomedByAuthorityDF(t *testing.T) {
	repo := registerRepo(t, "t-df-atomic-doomed")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main", "HEAD:refs/heads/good", "HEAD:refs/heads/z")
	head := revParse(t, work, "HEAD")

	writeFile(t, work, "b.txt", "two\n")
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "two")

	out, err := tryGit(work, "push", "--atomic", remoteURL(nodeA, repo, token),
		"HEAD:refs/heads/good", "HEAD:refs/heads/z/child")
	if err == nil {
		t.Fatalf("atomic push with an authority-doomed creation must be denied:\n%s", out)
	}
	t.Logf("atomic doomed push (expected whole-push denial):\n%s", out)
	refs := pgRefs(t, repo)
	if refs["refs/heads/good"] != head {
		t.Fatalf("authority moved refs/heads/good under a whole-push failure report: %v", refs)
	}
	if _, ok := refs["refs/heads/z/child"]; ok {
		t.Fatalf("authority accepted the doomed creation: %v", refs)
	}
}

// TestPush_DeniedDeletionSurvivorBlocksDFChild (Codex round-4 P1, exact
// scenario): one non-atomic push carries a policy-DENIED deletion of
// refs/heads/a plus an allowed creation of refs/heads/a/b. The denied
// deletion means `a` survives, so the update hook's authority probe must
// decline a/b at commit time — pre-receive verdicts are advisory and must
// not be trusted to predict survivors.
func TestPush_DeniedDeletionSurvivorBlocksDFChild(t *testing.T) {
	repo := registerRepo(t, "t-df-denied-survivor")
	work := initWorkRepo(t)
	full := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, full), "HEAD:refs/heads/main", "HEAD:refs/heads/a")

	// A fresh commit so the allowed a/b creation genuinely needs its pack
	// (otherwise the publication gate already refuses the mixed push
	// before any hook runs — a valid but different denial).
	writeFile(t, work, "b.txt", "two\n")
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "two")

	// no-push on exactly refs/heads/a denies its deletion; everything
	// else is allowed by the catch-all.
	restricted := mintTestToken(t, repo, "git:read,git:write", `[["refs/heads/a",["no-push"]],["*",[]]]`)
	out, err := tryGit(work, "push", remoteURL(nodeA, repo, restricted), ":refs/heads/a", "HEAD:refs/heads/a/b")
	if err == nil {
		t.Fatalf("push with a denied-survivor D/F conflict must be rejected:\n%s", out)
	}
	t.Logf("denied-survivor push (expected per-ref declines):\n%s", out)
	refs := pgRefs(t, repo)
	if _, ok := refs["refs/heads/a"]; !ok {
		t.Fatalf("policy-denied deletion still removed refs/heads/a: %v", refs)
	}
	if _, ok := refs["refs/heads/a/b"]; ok {
		t.Fatalf("authority accepted a/b although its denied-survivor parent remains: %v", refs)
	}
	mustGit(t, "", "ls-remote", remoteURL(nodeA, repo, full))
}
