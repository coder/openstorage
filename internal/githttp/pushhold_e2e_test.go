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
