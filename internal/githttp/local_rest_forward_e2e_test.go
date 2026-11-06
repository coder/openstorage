package githttp

import (
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/meta"
)

// TestLocalRESTWriteForwards drives serve's REST ref writes on the local
// schedule end to end: a branch created and a tag created through the API
// on a forward_on_push repository reach the upstream, and the lane goes
// idle. Each write runs restIntentBeforeCAS (the ref's push hold, then the
// forward intent) inside the real handler before its CAS (ADR-50
// amendment).
func TestLocalRESTWriteForwards(t *testing.T) {
	skipUnlessLocalSchedule(t)
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
	node, err := startNode(t.TempDir(), "OPENSTORAGE_PG_URL="+db, "OPENSTORAGE_S3_URL="+blobs, "OPENSTORAGE_SYNC_ALLOW_PRIVATE=true")
	if err != nil {
		t.Fatal(err)
	}
	defer node.stop()
	ms, err := meta.Open(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()
	url := "https://example.com/rest-forward.git"
	token := mintTestToken(t, url, "repo:write,git:read,git:write", "")
	var created struct {
		RepoID string `json:"repo_id"`
	}
	if status := apiJSON(t, node, "POST", token, "/api/v1/repos", "{}", &created); status != http.StatusCreated || created.RepoID == "" {
		t.Fatalf("create: status=%d id=%q", status, created.RepoID)
	}
	id := created.RepoID
	if _, err := ms.SetRepoUpstream(t.Context(), meta.RepoUpstream{
		RepoID: id, Provider: "github", Owner: "up", Name: "mirror", UpstreamHost: upstream.URL, ForwardOnPush: true,
	}); err != nil {
		t.Fatal(err)
	}
	// upstreamHas waits until the upstream's ref equals want and the lane
	// is idle.
	upstreamHas := func(ref, want string) {
		t.Helper()
		deadline := time.Now().Add(90 * time.Second)
		var got string
		var up meta.RepoUpstream
		for time.Now().Before(deadline) {
			out, _ := tryGit("", "--git-dir", bare, "rev-parse", "--verify", "--quiet", ref) // fails while the ref is missing
			got = strings.TrimSpace(out)
			var err error
			if up, err = ms.GetRepoUpstream(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			if got == want && !up.PushSyncing && up.PushPending == 0 {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("upstream %s = %q, want %s; lane %+v", ref, got, want, up)
	}
	work := initWorkRepo(t)
	head := revParse(t, work, "HEAD")
	mustGit(t, work, "push", remoteURL(node, id, token), "HEAD:refs/heads/main")
	upstreamHas("refs/heads/main", head)

	if status := apiJSON(t, node, http.MethodPost, token, "/api/v1/repos/branches/create",
		`{"base_ref":"main","target_branch":"agent"}`, nil); status != http.StatusOK {
		t.Fatalf("branch create: status %d", status)
	}
	upstreamHas("refs/heads/agent", head)
	if status := apiJSON(t, node, http.MethodPost, token, "/api/v1/repos/tags",
		`{"name":"v1","ref":"main"}`, nil); status != http.StatusOK {
		t.Fatalf("tag create: status %d", status)
	}
	upstreamHas("refs/tags/v1", head)
}
