package githttp

import (
	"context"
	"net/http/cgi"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref/s3wal/s3waltest"
)

// This fixture owns separate authority/blob storage: TestMain's node must not
// consume these markers under a different egress policy or repair their claims.
func localSchedulingStorage(t *testing.T) (string, string) {
	t.Helper()
	if testBackend == backendLocal {
		root := t.TempDir()
		return "sqlite://" + filepath.Join(root, "meta.db"), "file://" + filepath.Join(root, "blobs")
	}
	db, err := url.Parse(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	blobs, err := url.Parse(blobURL)
	if err != nil {
		t.Fatal(err)
	}
	// Own the container as well as the bucket: TestMain reserves volume slots
	// for its shared bucket and the backup drill, not an additional collection.
	// The helper's terminator retains this context; it must outlive t.Context's
	// cancellation before cleanup, or termination silently relies on Ryuk.
	addr, terminate := startSeaweedFS(context.WithoutCancel(t.Context()))
	t.Cleanup(terminate)
	for _, u := range []*url.URL{db, blobs} {
		u.Host = "local-hook-forwarding"
		query := u.Query()
		query.Set("endpoint", "http://"+addr)
		u.RawQuery = query.Encode()
	}
	raw := s3waltest.RawS3Client("http://"+addr, "us-east-1", s3AccessKey, s3SecretKey)
	if err := s3waltest.EnsureBucket(t.Context(), raw, db.Host); err != nil {
		t.Fatal(err)
	}
	return db.String(), blobs.String()
}

func TestLocalHookLiveForwardAndPendingRestart(t *testing.T) {
	if testBackend == backendPG {
		t.Skip("local hook scheduling contract")
	}
	root := t.TempDir()
	bare := filepath.Join(root, "up", "mirror.git")
	mustGit(t, "", "init", "--bare", "-b", "main", bare)
	mustGit(t, bare, "config", "http.receivepack", "true")
	backend := filepath.Join(strings.TrimSpace(mustGit(t, "", "--exec-path")), "git-http-backend")
	upstream := httptest.NewServer(&cgi.Handler{Path: backend, Env: []string{
		"GIT_PROJECT_ROOT=" + root, "GIT_HTTP_EXPORT_ALL=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
	}})
	defer upstream.Close()
	db, blobs := localSchedulingStorage(t)
	cache := t.TempDir()
	start := func() *testNode {
		n, err := startNode(cache, "OPENSTORAGE_PG_URL="+db, "OPENSTORAGE_S3_URL="+blobs, "OPENSTORAGE_SYNC_ALLOW_PRIVATE=true")
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	node := start()
	defer func() {
		if node != nil {
			node.stop()
		}
	}()
	ms, err := meta.Open(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()
	url := "https://example.com/owned-forward.git"
	token := mintTestToken(t, url, "repo:write,git:read,git:write", "")
	var created struct {
		RepoID string `json:"repo_id"`
	}
	if status := apiJSON(t, node, "POST", token, "/api/v1/repos", "{}", &created); status != 200 || created.RepoID == "" {
		t.Fatalf("create: status=%d id=%q", status, created.RepoID)
	}
	id := created.RepoID
	if _, err := ms.SetRepoUpstream(t.Context(), meta.RepoUpstream{
		RepoID: id, Provider: "github", Owner: "up", Name: "mirror", UpstreamHost: upstream.URL, ForwardOnPush: true,
	}); err != nil {
		t.Fatal(err)
	}
	waitSettled := func(wantRuns int) meta.RepoUpstream {
		t.Helper()
		deadline := time.Now().Add(90 * time.Second)
		var up meta.RepoUpstream
		for time.Now().Before(deadline) {
			var err error
			up, err = ms.GetRepoUpstream(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			if up.PushSuccessCount >= wantRuns && !up.PushSyncing && up.PushPending == 0 && !up.Syncing && !up.SyncPending && up.SuccessCount >= wantRuns {
				return up
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("forward + mirror-back did not settle: %+v", up)
		return up
	}
	work := initWorkRepo(t)
	first := revParse(t, work, "HEAD")
	mustGit(t, work, "push", remoteURL(node, id, token), "HEAD:refs/heads/main")
	settled := waitSettled(1)
	if got := revParse(t, bare, "refs/heads/main"); got != first {
		t.Fatalf("upstream=%s want accepted=%s", got, first)
	}
	// A write-only caller can update locally, but must trigger neither a full
	// forward nor a pull which would overwrite that newly accepted commit.
	second := commitFile(t, work, "second.txt", "local only\n", "write-only", nil)
	writeOnly := mintTestToken(t, url, "git:write", "")
	mustGit(t, work, "push", remoteURL(node, id, writeOnly), "HEAD:refs/heads/main")
	time.Sleep(3 * time.Second) // multiple live-consumer ticks after first-run settlement
	up, err := ms.GetRepoUpstream(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if up.PushRunCount != settled.PushRunCount || up.RunCount != settled.RunCount || up.PushPending != 0 || up.SyncPending || up.PushSyncing || up.Syncing {
		t.Fatalf("denied forwarding produced work: %+v", up)
	}
	if got := revParse(t, bare, "refs/heads/main"); got != first {
		t.Fatalf("write-only token exported %s", got)
	}
	if out := mustGit(t, "", "ls-remote", remoteURL(node, id, token), "refs/heads/main"); !strings.HasPrefix(out, second) {
		t.Fatalf("accepted denied-forward write was reverted: %s", out)
	}
	// Restart tests the existing durable-pending contract, not persistence of
	// retry timers: seed precisely that marker while no worker can consume it.
	node.stop()
	node = nil
	if err := ms.RequestPush(t.Context(), id, false); err != nil {
		t.Fatal(err)
	}
	node = start()
	waitSettled(2)
	if got := revParse(t, bare, "refs/heads/main"); got != second {
		t.Fatalf("restart did not consume durable pending work: %s", got)
	}
	t.Logf("forwarded %s, denied write stayed local at %s, restart consumed the existing pending marker", first, second)
}
