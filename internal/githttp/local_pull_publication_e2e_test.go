package githttp

import (
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/meta"
)

// A non-forwarding repo's hook marks SyncPending after the authority CAS but
// before receive-pack publishes the accepted ref in its cache. A live pull
// claiming that marker must not compare against the stale cache: after the
// publication it must still restore upstream-wins. S3WAL uses owned SeaweedFS.
func TestLocalHookPullBeforeCachePublication(t *testing.T) {
	if testBackend != backendLocal {
		t.Skip("SQLite hook scheduling (ADR-50); S3WAL hooks do not schedule (#335)")
	}
	root := t.TempDir()
	bare := filepath.Join(root, "up", "mirror.git")
	mustGit(t, "", "init", "--bare", "-b", "main", bare)
	backend := filepath.Join(strings.TrimSpace(mustGit(t, "", "--exec-path")), "git-http-backend")
	upstream := httptest.NewServer(&cgi.Handler{Path: backend, Env: []string{
		"GIT_PROJECT_ROOT=" + root, "GIT_HTTP_EXPORT_ALL=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
	}})
	defer upstream.Close()
	db, blobs := localSchedulingStorage(t)
	barrier := filepath.Join(t.TempDir(), "publication")
	// Disarm the one-shot barrier until the controlled push: the setup pull's
	// own loopback update must pass through.
	if err := os.WriteFile(barrier+".ready", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	node, err := startNode(t.TempDir(), "OPENSTORAGE_PG_URL="+db, "OPENSTORAGE_S3_URL="+blobs,
		"OPENSTORAGE_SYNC_ALLOW_PRIVATE=true", EnvUpdateBarrier+"="+barrier)
	if err != nil {
		t.Fatal(err)
	}
	defer node.stop()
	// Unpark before stopping the node, including any failing assertion.
	defer func() { _ = os.WriteFile(barrier+".release", nil, 0o644) }()
	ms, err := meta.Open(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()
	token := mintTestToken(t, "https://example.com/pull-publication.git", "repo:write,git:read,git:write", "")
	var created struct {
		RepoID string `json:"repo_id"`
	}
	if status := apiJSON(t, node, "POST", token, "/api/v1/repos", "{}", &created); status != 201 || created.RepoID == "" {
		t.Fatalf("create: status=%d id=%q", status, created.RepoID)
	}
	id := created.RepoID
	work := initWorkRepo(t)
	upHead := revParse(t, work, "HEAD")
	mustGit(t, work, "push", bare, "HEAD:refs/heads/main")
	if _, err := ms.SetRepoUpstream(t.Context(), meta.RepoUpstream{
		RepoID: id, Provider: "github", Owner: "up", Name: "mirror", UpstreamHost: upstream.URL,
	}); err != nil {
		t.Fatal(err)
	}
	get := func() meta.RepoUpstream {
		t.Helper()
		up, err := ms.GetRepoUpstream(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		return up
	}
	settled := func(up meta.RepoUpstream, successes int) bool {
		return up.SuccessCount >= successes && !up.Syncing && !up.SyncPending
	}
	// Setup: a real first pull through the live consumer, which also proves
	// the consumer is scanning before the controlled window opens.
	if err := ms.RequestSync(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	var up meta.RepoUpstream
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if up = get(); settled(up, 1) {
			break
		}
	}
	if !settled(up, 1) || up.SuccessCount != 1 {
		t.Fatalf("setup pull did not settle: %+v", up)
	}
	if out := mustGit(t, "", "ls-remote", remoteURL(node, id, token), "refs/heads/main"); !strings.HasPrefix(out, upHead) {
		t.Fatalf("setup pull did not mirror upstream %s: %s", upHead, out)
	}

	accepted := commitFile(t, work, "local.txt", "local only\n", "accepted local write", nil)
	if err := os.Remove(barrier + ".ready"); err != nil { // arm for this push only
		t.Fatal(err)
	}
	type pushResult struct {
		out string
		err error
	}
	done := make(chan pushResult, 1)
	go func() {
		out, err := tryGit(work, "push", remoteURL(node, id, token), "HEAD:refs/heads/main")
		done <- pushResult{out, err}
	}()
	waitForFile(t, barrier+".ready", 30*time.Second)
	// The hook's marker is consumed by the live pull inside the window.
	claimed := func(up meta.RepoUpstream) bool { return !up.SyncPending && (up.Syncing || up.RunCount > 1) }
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(25 * time.Millisecond) {
		if up = get(); claimed(up) {
			break
		}
	}
	if !claimed(up) {
		t.Fatalf("live pull did not claim the hook intent inside the publication window: %+v", up)
	}
	// Give a non-waiting pull time to finish against the stale cache.
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline) && up.Syncing; time.Sleep(25 * time.Millisecond) {
		up = get()
	}
	select {
	case res := <-done:
		t.Fatalf("push left the barrier before release: %v\n%s", res.err, res.out)
	default:
	}
	if !up.Syncing || up.RunCount != 1 {
		t.Errorf("pull finished before cache publication: %+v", up)
	}
	t.Logf("paused after authority CAS: syncing=%t run_count=%d success=%d pending=%t", up.Syncing, up.RunCount, up.SuccessCount, up.SyncPending)
	if err := os.WriteFile(barrier+".release", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	res := <-done
	if res.err != nil {
		t.Fatalf("real push failed: %v\n%s", res.err, res.out)
	}
	// Observe the actual cache ref, not only metadata: upstream wins over the
	// published local write.
	var got string
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		up = get()
		got = strings.TrimSpace(mustGit(t, "", "ls-remote", remoteURL(node, id, token), "refs/heads/main"))
		if settled(up, 2) && strings.HasPrefix(got, upHead) {
			break
		}
	}
	if !settled(up, 2) || !strings.HasPrefix(got, upHead) {
		t.Fatalf("after publication local main=%q, want upstream %s over accepted %s; metadata=%+v", got, upHead, accepted, up)
	}
	if out := strings.TrimSpace(mustGit(t, bare, "for-each-ref", "--format=%(objectname)", "refs/heads/main")); out != upHead {
		t.Fatalf("non-forwarding repo changed upstream: %s", out)
	}
	t.Logf("released publication of %s; pull restored upstream %s", accepted, upHead)
}
