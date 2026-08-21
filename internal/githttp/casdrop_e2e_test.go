package githttp

// End-to-end coverage for issue #2: a DB connection that dies after the
// update hook's CAS statement commits must not fail the push (when the
// recovery recheck can run) and must never wedge the client's retry.

import (
	"os"
	"path/filepath"
	"testing"
)

// TestPushRecoversFromPostCommitCASDrop: the CAS commits, the store reports
// a connection drop, but the recovery point-read still works — the push
// must succeed on the first attempt (ref.IdempotentCAS reconciles the
// outcome below the events decorator, so the push webhook is emitted too;
// emission itself is pinned by the internal/events tests).
func TestPushRecoversFromPostCommitCASDrop(t *testing.T) {
	repo := registerRepo(t, "t-casdrop-soft")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")

	marker := filepath.Join(t.TempDir(), "cas-drop-consumed")
	node := startNodeT(t, EnvCASDrop+"="+marker)

	writeFile(t, work, "b.txt", "two\n")
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "two")
	head := revParse(t, work, "HEAD")

	out := mustGit(t, work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	t.Logf("push under soft CAS drop:\n%s", out)

	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("fault knob was not consumed (update hook never saw it): %v", err)
	}
	if got := pgRefs(t, repo)["refs/heads/main"]; got != head {
		t.Fatalf("main = %q, want %q", got, head)
	}
}

// TestPushRetryAfterHardCASDrop: the CAS commits but the connection is truly
// gone (the recheck fails too), so the first push reports failure. The ref
// moved regardless; the client's retry must then succeed cleanly instead of
// surfacing a spurious "moved concurrently" conflict.
func TestPushRetryAfterHardCASDrop(t *testing.T) {
	repo := registerRepo(t, "t-casdrop-hard")
	work := initWorkRepo(t)
	token := mintTestToken(t, repo, "git:read,git:write", "")
	mustGit(t, work, "push", remoteURL(nodeA, repo, token), "HEAD:refs/heads/main")

	marker := filepath.Join(t.TempDir(), "cas-drop-consumed")
	node := startNodeT(t, EnvCASDropHard+"="+marker)

	writeFile(t, work, "b.txt", "two\n")
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "two")
	head := revParse(t, work, "HEAD")

	out, err := tryGit(work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	if err == nil {
		t.Fatalf("push should fail under the hard drop knob:\n%s", out)
	}
	t.Logf("push under hard CAS drop (expected failure):\n%s", out)

	// The statement committed before the injected drop: the ref moved even
	// though the client saw a failure.
	if got := pgRefs(t, repo)["refs/heads/main"]; got != head {
		t.Fatalf("main after failed push = %q, want committed %q", got, head)
	}

	// The retry re-negotiates, finds the ref already at the target, and
	// finishes cleanly (exit 0, no spurious rejection).
	out2 := mustGit(t, work, "push", remoteURL(node, repo, token), "HEAD:refs/heads/main")
	t.Logf("retry push:\n%s", out2)
	if got := pgRefs(t, repo)["refs/heads/main"]; got != head {
		t.Fatalf("main after retry = %q, want %q", got, head)
	}
}
