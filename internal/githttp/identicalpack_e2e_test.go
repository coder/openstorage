package githttp

// Identical concurrent packs (issue #187): two pushes whose quarantined
// packs are byte-identical resolve to the same content-addressed pack name.
// The blob upload and the RegisterPacks transaction are idempotent for that
// case by design, but git's quarantine migration was not: the pack's .keep
// sidecar records "receive-pack <pid> on <host>" — process-unique bytes —
// so the second push to migrate failed finalize_object_file's
// identical-content collision check on the .keep file and rejected the
// whole push with "unable to migrate objects to permanent storage".
// pre-receive now normalizes quarantined .keep contents to a fixed string,
// making the loser's migration the byte-identical no-op the rest of the
// publication path already treats it as.

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestPush_IdenticalConcurrentPacksBothSucceed pins the race
// deterministically: push A parks inside receive-pack via the update-hook
// barrier — its quarantine ALREADY migrated (git migrates right after
// pre-receive succeeds, before update hooks), so the shared cache repo
// holds A's pack trio plus its .keep — then push B sends the byte-identical
// pack end to end. Before the fix B's migration died on the .keep content
// mismatch; now both pushes must land, the pack must be registered once,
// and the blob store must hold exactly one copy.
func TestPush_IdenticalConcurrentPacksBothSucceed(t *testing.T) {
	repo := registerRepo(t, "t-identical-packs")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")

	// Two clones build the SAME commit: the shared -c identity plus fixed
	// dates make the empty commits byte-identical, so both pushes
	// quarantine byte-identical single-commit packs under the same
	// content-addressed name.
	cloneA := filepath.Join(t.TempDir(), "clone-a")
	cloneB := filepath.Join(t.TempDir(), "clone-b")
	mustGit(t, "", "clone", work, cloneA)
	mustGit(t, "", "clone", work, cloneB)
	dateEnv := []string{
		"GIT_AUTHOR_DATE=2026-01-02T03:04:05Z",
		"GIT_COMMITTER_DATE=2026-01-02T03:04:05Z",
	}
	mustGitEnv(t, cloneA, dateEnv, "commit", "--allow-empty", "-m", "identical")
	mustGitEnv(t, cloneB, dateEnv, "commit", "--allow-empty", "-m", "identical")
	sha := revParse(t, cloneA, "HEAD")
	if shaB := revParse(t, cloneB, "HEAD"); shaB != sha {
		t.Fatalf("clone commits diverged: %s vs %s", sha, shaB)
	}

	barrier := filepath.Join(t.TempDir(), "update-barrier")
	node := startNodeT(t, EnvUpdateBarrier+"="+barrier)

	type pushResult struct {
		out string
		err error
	}
	doneA := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(cloneA, "push", remoteURL(node, repo, token), "HEAD:refs/heads/ida")
		doneA <- pushResult{out, err}
	}()
	// A's update hook created <barrier>.ready and parked: publication,
	// registration, AND the quarantine migration are done; receive-pack A
	// is alive, so its migrated .keep still sits in objects/pack. B's
	// hooks pass the one-shot barrier untouched.
	waitForFile(t, barrier+".ready", 30*time.Second)

	outB, errB := tryGit(cloneB, "push", remoteURL(node, repo, token), "HEAD:refs/heads/idb")
	if errB != nil {
		t.Fatalf("identical concurrent push rejected (issue #187): %v\n%s", errB, outB)
	}

	if err := os.WriteFile(barrier+".release", nil, 0o644); err != nil {
		t.Fatalf("release barrier: %v", err)
	}
	resA := <-doneA
	if resA.err != nil {
		t.Fatalf("push A failed: %v\n%s", resA.err, resA.out)
	}

	refs := pgRefs(t, repo)
	if refs["refs/heads/ida"] != sha || refs["refs/heads/idb"] != sha {
		t.Fatalf("authority refs = %v, want ida and idb at %s", refs, sha)
	}
	// One seed pack (the main push) plus ONE shared identical pack: the
	// loser adopted the winner's row instead of double-registering, and
	// the content-addressed blob exists exactly once.
	if packs := pgPacks(t, repo); len(packs) != 2 {
		t.Fatalf("registered packs = %+v, want 2 (seed + one shared identical pack)", packs)
	}
	if objs := s3Packs(t, repo); len(objs) != 2 {
		t.Fatalf("blob store pack objects = %+v, want 2", objs)
	}
}
